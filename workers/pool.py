
import os
import sys
import time
import logging 
import multiprocessing
from enum import Enum
from dataclasses import dataclass, field


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.constants import (
    WORKER_POOL_SIZE,
    WORKER_RECYCLE_AFTER_JOBS,
    HEARTBEAT_INTERVAL_S,
)
logger = logging.getLogger("ember.worker_pool")

_mp = multiprocessing.get_context("fork")


class WorkerState(Enum):

    BOOTING = "BOOTING"
    WAIT = "WAIT"
    EXECUTING = "EXECUTING"
    RECYCLING = "RECYCLING"
    DEAD = "DEAD"

@dataclass
class WorkerInfo:
    """Metadata tracked by the controller for each worker."""

    worker_id: int
    pid: int | None = None
    state: WorkerState = WorkerState.BOOTING
    jobs_completed: int = 0
    recycle_limit: int = WORKER_RECYCLE_AFTER_JOBS
    last_heartbeat: float = 0.0
    booted_at: float = 0.0

    def needs_recycle(self) -> float:
        return self.jobs_completed >= self.recycle_limit

    def heartbeat_age_s(self) -> float:
        if self.last_heartbeat == 0.0:
            return float("inf")
        return time.monotonic() - self.last_heartbeat

def _worker_process(
    worker_id: int,
    control_queue: multiprocessing.Queue,
    heartbeat_interval_s: float,
) -> None:
    """The worker process entry point."""

    control_queue.put({
        "type": "REGISTER",
        "worker_id": worker_id,
        "pid": os.getpid(),
        "timestamp": time.monotonic(),
    })
    time.sleep(heartbeat_interval_s)

class WorkerPool:
    """Fixed process pool with boot verification and recycling."""\

    def __init__(
        self,
        pool_size: int = WORKER_POOL_SIZE,
        recycle_limit: int = WORKER_RECYCLE_AFTER_JOBS,
        heartbeat_interval_s: float = HEARTBEAT_INTERVAL_S,
        ) -> None:
            self.pool_size = pool_size
            self.recycle_limit = recycle_limit
            self.heartbeat_interval_s = heartbeat_interval_s

            self._control_queue: multiprocessing.Queue = _mp.Queue()

            self._workers: dict[int, WorkerInfo] = {}
            self._processes: dict[int, multiprocessing.Process] = {}

            self._booted = False

    @property
    def active_count(self) -> int:
         return sum(
              1 for w in self._workers.values()
              if w.state in (WorkerState.WAIT, WorkerState.EXECUTING)
         )

    @property
    def all_in_wait(self) -> bool:
         return all(
              w.state == WorkerState.WAIT
              for w in self._workers.values()
         )

    def boot(self, timeout_s: float = 10.0) -> bool:
        """
        Boot the full worker pool and verify all registrations.

        Returns True if all workers registered within the timeout.
        """
        logger.info(
            "[POOL] Booting %d workers (recycle every %d jobs)...",
            self.pool_size,
            self.recycle_limit,
        )

        boot_start = time.monotonic()

        # Spawn all worker processes
        for worker_id in range(self.pool_size):
            self._spawn_worker(worker_id)

        # Collect registrations
        registered = 0
        deadline = boot_start + timeout_s

        while registered < self.pool_size and time.monotonic() < deadline:
            try:
                remaining = deadline - time.monotonic()
                msg = self._control_queue.get(timeout=max(0.1, remaining))

                if msg["type"] == "REGISTER":
                    wid = msg["worker_id"]
                    self._workers[wid].pid = msg["pid"]
                    self._workers[wid].state = WorkerState.WAIT
                    self._workers[wid].booted_at = msg["timestamp"]
                    self._workers[wid].last_heartbeat = msg["timestamp"]
                    registered += 1

                    if registered % 10 == 0 or registered == self.pool_size:
                        logger.info(
                            "[POOL] Registered %d/%d workers",
                            registered,
                            self.pool_size,
                        )

                elif msg["type"] == "HEARTBEAT":
                    wid = msg["worker_id"]
                    if wid in self._workers:
                        self._workers[wid].last_heartbeat = msg["timestamp"]
                        self._workers[wid].jobs_completed = msg["jobs_completed"]

            except Exception:
                continue

        boot_elapsed = time.monotonic() - boot_start

        if registered == self.pool_size:
            self._booted = True
            logger.info(
                "[POOL] All %d workers booted in %.2fs",
                self.pool_size,
                boot_elapsed,
            )
            return True
        else:
            logger.error(
                "[POOL] Boot FAILED: only %d/%d registered in %.2fs",
                registered,
                self.pool_size,
                boot_elapsed,
            )
            return False

    def _spawn_worker(self, worker_id: int) -> None:
        """Spawn a single worker process."""
        info = WorkerInfo(
            worker_id=worker_id,
            recycle_limit=self.recycle_limit,
        )
        self._workers[worker_id] = info

        p = _mp.Process(
            target=_worker_process,
            args=(worker_id, self._control_queue, self.heartbeat_interval_s),
            daemon=True,
        )
        p.start()
        self._processes[worker_id] = p # type: ignore
        info.pid = p.pid

    def drain_heartbeats(self, timeout_s: float = 0.5) -> int:
        """
        Read all pending messages from the control queue.

        Updates worker state with latest heartbeat timestamps.
        Returns the number of heartbeats processed.
        """
        count = 0
        deadline = time.monotonic() + timeout_s

        while time.monotonic() < deadline:
            try:
                msg = self._control_queue.get(timeout=0.05)
                if msg["type"] == "HEARTBEAT":
                    wid = msg["worker_id"]
                    if wid in self._workers:
                        self._workers[wid].last_heartbeat = msg["timestamp"]
                        self._workers[wid].jobs_completed = msg["jobs_completed"]
                        count += 1
            except Exception:
                break

        return count

    def get_worker_info(self, worker_id: int) -> WorkerInfo | None:
        return self._workers.get(worker_id)

    def shutdown(self, grace_s: float = 2.0) -> None:
        """Terminate all worker processes."""
        logger.info("[POOL] Shutting down %d workers...", len(self._processes))

        for wid, p in self._processes.items():
            if p.is_alive():
                p.terminate()

        # Wait for graceful exit
        deadline = time.monotonic() + grace_s
        for wid, p in self._processes.items():
            remaining = max(0.1, deadline - time.monotonic())
            p.join(timeout=remaining)

        # Force-kill any survivors
        for wid, p in self._processes.items():
            if p.is_alive():
                p.kill()
                p.join(timeout=1)

        for w in self._workers.values():
            w.state = WorkerState.DEAD

        logger.info("[POOL] All workers terminated")

    def status(self) -> dict:
        """Snapshot of pool state."""
        states = {}
        for w in self._workers.values():
            state_name = w.state.value
            states[state_name] = states.get(state_name, 0) + 1

        return {
            "pool_size": self.pool_size,
            "booted": self._booted,
            "active_count": self.active_count,
            "all_in_wait": self.all_in_wait,
            "state_distribution": states,
            "recycle_limit": self.recycle_limit,
        }