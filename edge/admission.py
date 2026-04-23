"""Admission control for edge tasks."""

import os 
import sys
import time
import threading
import logging
from collections import deque
from dataclasses import dataclass

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.constants import (
    QUEUE_SIZE,
    QUEUE_TTL_MS,
    WORKER_POOL_SIZE,
    HYSTERESIS_HIGH_PCT,
    HYSTERESIS_LOW_PCT,
    WORKER_MEMORY_LIMIT_MB,
) 

from core.task import TaskManifest, TaskState, TaskResult
from core.telemetry import TelemetryRecorder, AdmissionEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ember.admission")


@dataclass
class AdmissionResult:
    """Result of an admission attempt."""

    admitted: bool
    queued: bool
    rejected: bool
    reason: str
    task_id: str

class MemoryPressureMonitor:
    """Monitors memory usage while providing pressure status."""

    def __init__(
            self,
            high_threshold: float = HYSTERESIS_HIGH_PCT,
            low_threshold: float = HYSTERESIS_LOW_PCT,
            total_memory_mb: int = WORKER_MEMORY_LIMIT_MB * WORKER_POOL_SIZE,
    ) -> None:
        self.high_threshold = high_threshold
        self.low_threshold = low_threshold
        self.total_memory_mb = total_memory_mb
        self._throttled = False
        self._lock = threading.Lock()

    def update(self, used_memory_mb: float) -> bool:
        """Updated memory state."""
        usage_pct = used_memory_mb / self.total_memory_mb

        with self._lock:
            if self._throttled:
                if usage_pct < self.low_threshold:
                    self._throttled = False
                    logger.info(
                        "Memory pressure CLEARED: %.1f%% < %.1f%% low threshold",
                        usage_pct * 100,
                        self.low_threshold * 100,
                    )
            else:
                if usage_pct >= self.high_threshold:
                    self._throttled = True
                    logger.warning(
                        "Memory pressure THROTTLED: %.1f%% >= %.1f%% high threshold",
                        usage_pct * 100,
                        self.high_threshold * 100,
                    )
            return not self._throttled

    @property
    def is_throttled(self) -> bool:
        return self._throttled

class AdmissionGate:

    def __init__(
        self,
        max_concurrency: int = WORKER_POOL_SIZE,
        max_queue_depth: int = QUEUE_SIZE,
        queue_ttl_ms: int = QUEUE_TTL_MS,
    ) -> None:
        self.max_concurrency = max_concurrency
        self.max_queue_depth = max_queue_depth
        self.queue_ttl_ms = queue_ttl_ms

        self._semaphore = threading.BoundedSemaphore(value=max_concurrency)
        self._active_count = 0
        self._active_lock = threading.Lock()

        self._queue: deque[TaskManifest] = deque()
        self._queue_lock = threading.RLock()

               # The Deadband: memory pressure hysteresis
        self.memory_monitor = MemoryPressureMonitor()

        # Telemetry
        self.telemetry = TelemetryRecorder()

        # Stats
        self.total_admitted = 0
        self.total_queued = 0
        self.total_rejected = 0
        self.total_expired = 0

    @property
    def active_workers(self) -> int:
        with self._active_lock:
            return self._active_count

    @property
    def queue_depth(self) -> int:
        with self._queue_lock:
            return len(self._queue)

    def _evicted_expired(self) -> int:

        evicted = 0
        with self._queue_lock:
            while self._queue and self._queue[0].is_expired(self.queue_ttl_ms):
                expired_task = self._queue.popleft()
                expired_task.state = TaskState.EXPIRED
                self.total_expired += 1
                evicted += 1

                self.telemetry.record(
                    task_id=expired_task.task_id,
                    task_type=expired_task.task_type,
                    event=AdmissionEvent.EXPIRED,
                    queue_wait_ms=expired_task.queue_wait_ms(),
                    queue_depth=len(self._queue),
                    active_workers=self.active_workers,
                )

                logger.warning(
                    "EXPIRED | task_id=%s | waited=%.2fms | ttl=%dms",
                    expired_task.task_id,
                    expired_task.queue_wait_ms(),
                    self.queue_ttl_ms,
                )
        return evicted

    def submit(self, task: TaskManifest) -> AdmissionResult:

        self._evicted_expired()

        # Check memory pressure
        if self.memory_monitor.is_throttled:
            self.total_rejected += 1
            self.telemetry.record(
                task_id=task.task_id,
                task_type=task.task_type,
                event=AdmissionEvent.REJECTED,
                queue_depth=self.queue_depth,
                active_workers=self.active_workers,
            )
            logger.warning(
                "[REJECT] %s (Memory Pressure) -> 429",
                task.task_id[:8],
            )
            return AdmissionResult(
                admitted=False,
                queued=False,
                rejected=True,
                reason="429: memory pressure — hysteresis throttle active",
                task_id=task.task_id,
            )

        acquired = self._semaphore.acquire(blocking=False)

        if acquired:
            with self._active_lock:
                self._active_count += 1
            task.state = TaskState.ASSIGNED
            task.assigned_at_ms = time.perf_counter() * 1000
            self.total_admitted += 1

            self.telemetry.record(
                task_id=task.task_id,
                task_type=task.task_type,
                event=AdmissionEvent.ADMITTED,
                queue_wait_ms=0.0,
                queue_depth=self.queue_depth,
                active_workers=self.active_workers,
            )
            logger.info(
                "[ADMIT] %s -> Worker_%02d",
                task.task_id[:8],
                self.active_workers,
            )
            return AdmissionResult(
                admitted=True,
                queued=False,
                rejected=False,
                reason="admitted: worker slot acquired",
                task_id=task.task_id,
            )
        with self._queue_lock:
            if len(self._queue) < self.max_queue_depth:
                task.state = TaskState.QUEUED
                self._queue.append(task)
                self.total_queued += 1

                self.telemetry.record(
                    task_id=task.task_id,
                    task_type=task.task_type,
                    event=AdmissionEvent.QUEUED,
                    queue_wait_ms=0.0,
                    queue_depth=len(self._queue),
                    active_workers=self.active_workers,
                )
                logger.info(
                    "[QUEUE] %s (Size: %d/%d)",
                    task.task_id[:8],
                    len(self._queue),
                    self.max_queue_depth,
                )
                return AdmissionResult(
                    admitted=False,
                    queued=True,
                    rejected=False,
                    reason=f"queued: position {len(self._queue)}/{self.max_queue_depth}",
                    task_id=task.task_id,
                )

        self.total_rejected += 1
        self.telemetry.record(
            task_id=task.task_id,
            task_type=task.task_type,
            event=AdmissionEvent.REJECTED,
            queue_depth=self.queue_depth,
            active_workers=self.active_workers,
        )
        logger.warning(
            "[REJECT] %s (Queue Full) -> 429",
            task.task_id[:8],
        )
        return AdmissionResult(
            admitted=False,
            queued=False,
            rejected=True,
            reason=f"429: queue full ({self.max_queue_depth}/{self.max_queue_depth})",
            task_id=task.task_id,
        )

    def release(self, task: TaskManifest) -> None:
        """Release a worker slot after task completion."""
        task.state = TaskState.COMPLETED
        task.completed_at_ms = time.perf_counter() * 1000

        self.telemetry.record(
            task_id=task.task_id,
            task_type=task.task_type,
            event=AdmissionEvent.COMPLETED,
            queue_wait_ms=0.0,
            queue_depth=self.queue_depth,
            active_workers=self.active_workers,
        )

        with self._active_lock:
            self._active_count -= 1
        self._semaphore.release()

    def dequeue(self) -> TaskManifest | None:
        """Attempt to dequeue a task for execution."""

        self._evicted_expired()

        while True:
            with self._queue_lock:
                if not self._queue:
                    return None
                task = self._queue.popleft()


                if task.is_expired(self.queue_ttl_ms):
                    task.state = TaskState.EXPIRED
                    self.total_expired += 1
                    self.telemetry.record(
                        task_id=task.task_id,
                        task_type=task.task_type,
                        event=AdmissionEvent.EXPIRED,
                        queue_wait_ms=task.queue_wait_ms(),
                        queue_depth=len(self._queue),
                        active_workers=self.active_workers,
                    )
                    logger.warning(
                        "[EXPIRE] %s (Wait: %.1fms) -> Dropped",
                        task.task_id[:8],
                        task.queue_wait_ms(),
                    )
                    continue  # Try next task in queue

                wait_ms = task.queue_wait_ms()
                self.telemetry.record(
                    task_id=task.task_id,
                    task_type=task.task_type,
                    event=AdmissionEvent.DEQUEUED,
                    queue_wait_ms=wait_ms,
                    queue_depth=self.queue_depth,
                    active_workers=self.active_workers,
                )
                logger.info(
                    "[DEQUEUE] %s (Wait: %.1fms) -> Worker",
                    task.task_id[:8],
                    wait_ms,
                )
                return task

    def status(self) -> dict:
        """Return a snapshot of the admission gate state."""
        return {
            "active_workers": self.active_workers,
            "max_concurrency": self.max_concurrency,
            "queue_depth": self.queue_depth,
            "max_queue_depth": self.max_queue_depth,
            "memory_throttled": self.memory_monitor.is_throttled,
            "total_admitted": self.total_admitted,
            "total_queued": self.total_queued,
            "total_rejected": self.total_rejected,
            "total_expired": self.total_expired,
        }
