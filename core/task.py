"""
EMBER Task Contract

Defines the TaskManifest (what a task looks like), TaskState (what can
happen to it), and TaskResult (what comes back). This is the guest-host
interface — both the admission gate and workers depend on these types.
"""

import time
import uuid
from enum import Enum
from dataclasses import dataclass, field
from typing import Any


class TaskState(Enum):
    """
    Strict state machine for task lifecycle.

    QUEUED → ASSIGNED → RUNNING → COMPLETED → ACKNOWLEDGED
                          │
                          └──→ QUEUED         (retry, if retries remain)
                          └──→ FAILED         (max_retries exceeded)
                          └──→ EXPIRED        (TTL exceeded while queued)
                          └──→ LEASE_EXPIRED  (worker hung — lease reclaimed)
    """

    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    LEASE_EXPIRED = "LEASE_EXPIRED"


@dataclass
class TaskManifest:
    """
    The contract between client and engine.

    Tasks are typed functions — not arbitrary scripts. The task_type maps
    to a callable in the TASK_REGISTRY, and args are passed as kwargs.
    """

    task_type: str
    args: dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timeout_ms: int = 500
    max_retries: int = 2

    # Lifecycle tracking (set by the engine, not the client)
    state: TaskState = TaskState.QUEUED
    retries_remaining: int = field(init=False)
    enqueued_at_ms: float = field(init=False)
    assigned_at_ms: float | None = None
    started_at_ms: float | None = None
    completed_at_ms: float | None = None
    assigned_worker_id: int | None = None

    def __post_init__(self) -> None:
        self.retries_remaining = self.max_retries
        self.enqueued_at_ms = time.perf_counter() * 1000

    def queue_wait_ms(self) -> float:
        """How long this task has been waiting in the queue."""
        return (time.perf_counter() * 1000) - self.enqueued_at_ms

    def is_expired(self, ttl_ms: float) -> bool:
        """Check if this task has exceeded the queue TTL."""
        return self.queue_wait_ms() > ttl_ms

    def lease_elapsed_ms(self) -> float | None:
        """How long since this task was assigned to a worker."""
        if self.assigned_at_ms is None:
            return None
        return (time.perf_counter() * 1000) - self.assigned_at_ms

    def is_lease_expired(self, lease_timeout_ms: float) -> bool:
        """Check if the task's lease has exceeded the timeout."""
        elapsed = self.lease_elapsed_ms()
        if elapsed is None:
            return False
        return elapsed > lease_timeout_ms


@dataclass
class TaskResult:
    """What comes back from a completed task."""

    task_id: str
    success: bool
    result: Any = None
    error: str | None = None
    execution_ms: float = 0.0