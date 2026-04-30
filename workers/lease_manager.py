"""
EMBER Task Lease Manager

The final safety net for the execution lifecycle. If a worker silently
hangs or becomes partitioned while holding a task, the lease manager
detects the expired lease and reclaims the task for retry.

The Admission Gate handles queue-level TTL (100ms Dead Letter).
The Lease Manager handles execution-level timeouts (450ms reclaim threshold).

Lease timeout is set to 450ms — slightly below the p99 target of 500ms.
This ensures a retry can be attempted before a client-side timeout fires.

State transitions on lease expiry:
    ASSIGNED/RUNNING → QUEUED  (if retries remain)
    ASSIGNED/RUNNING → FAILED  (if max_retries exceeded)
"""

import os
import sys
import time
import threading
import logging
from dataclasses import dataclass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.constants import TASK_LEASE_TIMEOUT_MS
from core.task import TaskManifest, TaskState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ember.lease_manager")


@dataclass
class LeaseEvent:
    """A recorded lease expiry event."""

    task_id: str
    task_type: str
    worker_id: int | None
    elapsed_ms: float
    retries_remaining: int
    action: str  # "requeued" or "failed"
    timestamp: float


class LeaseManager:
    """
    Enforces lease timeouts on tasks in ASSIGNED and RUNNING states.

    The controller calls check_leases() periodically. Any task whose
    lease has expired is reclaimed:
      - If retries remain → state moves to QUEUED for retry
      - If no retries left → state moves to FAILED (poison pill protection)

    The 450ms threshold is chosen to be:
      - Above most normal execution times (p50 = 80ms local)
      - Below the p99 SLO (500ms) so a retry has time to complete
    """

    def __init__(
        self,
        lease_timeout_ms: float = TASK_LEASE_TIMEOUT_MS,
    ) -> None:
        self.lease_timeout_ms = lease_timeout_ms
        self._active_leases: dict[str, TaskManifest] = {}
        self._lock = threading.Lock()
        self._events: list[LeaseEvent] = []

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active_leases)

    @property
    def events(self) -> list[LeaseEvent]:
        return list(self._events)

    def grant_lease(self, task: TaskManifest, worker_id: int) -> None:
        """
        Grant a lease when a task is assigned to a worker.

        Stamps the task with the assignment time and worker ID.
        """
        task.state = TaskState.ASSIGNED
        task.assigned_at_ms = time.perf_counter() * 1000
        task.assigned_worker_id = worker_id

        with self._lock:
            self._active_leases[task.task_id] = task

        logger.info(
            "[LEASE] Granted | task=%s | worker=%d | timeout=%dms",
            task.task_id[:8],
            worker_id,
            self.lease_timeout_ms,
        )

    def start_execution(self, task: TaskManifest) -> None:
        """
        Mark a task as RUNNING. The lease clock keeps ticking
        from the original assigned_at_ms — not reset.
        """
        task.state = TaskState.RUNNING
        task.started_at_ms = time.perf_counter() * 1000

        logger.info(
            "[LEASE] Running | task=%s | worker=%s",
            task.task_id[:8],
            task.assigned_worker_id,
        )

    def complete_lease(self, task: TaskManifest) -> None:
        """
        Release a lease when a task completes successfully.

        Removes the task from active tracking.
        """
        task.state = TaskState.COMPLETED
        task.completed_at_ms = time.perf_counter() * 1000

        with self._lock:
            self._active_leases.pop(task.task_id, None)

        elapsed = task.lease_elapsed_ms() or 0.0
        logger.info(
            "[LEASE] Completed | task=%s | elapsed=%.1fms",
            task.task_id[:8],
            elapsed,
        )

    def check_leases(self) -> list[TaskManifest]:
        """
        Scan all active leases and reclaim expired ones.

        Returns a list of tasks that were reclaimed (moved to QUEUED
        or FAILED). The caller is responsible for actually re-submitting
        the QUEUED tasks to the admission gate.
        """
        reclaimed: list[TaskManifest] = []
        now = time.perf_counter() * 1000

        with self._lock:
            expired_ids = []

            for task_id, task in self._active_leases.items():
                if task.is_lease_expired(self.lease_timeout_ms):
                    expired_ids.append(task_id)

            for task_id in expired_ids:
                task = self._active_leases.pop(task_id)
                elapsed = task.lease_elapsed_ms() or 0.0

                if task.retries_remaining > 0:
                    # Requeue for retry
                    task.retries_remaining -= 1
                    task.state = TaskState.QUEUED
                    task.assigned_at_ms = None
                    task.started_at_ms = None
                    task.assigned_worker_id = None
                    # Reset enqueue time for fresh queue TTL
                    task.enqueued_at_ms = now
                    action = "requeued"

                    logger.warning(
                        "[LEASE] EXPIRED → REQUEUE | task=%s | worker=%s | "
                        "elapsed=%.1fms | retries_left=%d",
                        task.task_id[:8],
                        task.assigned_worker_id,
                        elapsed,
                        task.retries_remaining,
                    )
                else:
                    # No retries left — poison pill protection
                    task.state = TaskState.FAILED
                    action = "failed"

                    logger.error(
                        "[LEASE] EXPIRED → FAILED | task=%s | worker=%s | "
                        "elapsed=%.1fms | no retries remaining",
                        task.task_id[:8],
                        task.assigned_worker_id,
                        elapsed,
                    )

                event = LeaseEvent(
                    task_id=task.task_id,
                    task_type=task.task_type,
                    worker_id=task.assigned_worker_id,
                    elapsed_ms=elapsed,
                    retries_remaining=task.retries_remaining,
                    action=action,
                    timestamp=time.monotonic(),
                )
                self._events.append(event)
                reclaimed.append(task)

        return reclaimed

    def status(self) -> dict:
        """Snapshot of lease manager state."""
        return {
            "active_leases": self.active_count,
            "lease_timeout_ms": self.lease_timeout_ms,
            "total_reclaimed": len(self._events),
            "total_requeued": sum(1 for e in self._events if e.action == "requeued"),
            "total_failed": sum(1 for e in self._events if e.action == "failed"),
        }