import time 
import uuid 
from enum import Enum
from dataclasses import dataclass, field
from typing import Any 

class TaskState(Enum):
    """Acceptable states for a task in the system."""

    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"

@dataclass
class TaskManifest:
    """contract between admission and execution layers."""

    task_type: str
    args: dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timeout_ms: int = 500
    max_retries: int = 2

    state: TaskState = TaskState.QUEUED
    retries_remaining: int = field(init=False)
    enqueued_at_ms: float = field(init=False)
    assigned_at_ms: float | None = None
    completed_at_ms: float | None = None

    def __post_init__(self) -> None:
        self.retries_remaining = self.max_retries
        self.enqueued_at_ms = time.perf_counter() * 1000

    def queue_wait_ms(self) -> float:
        """Time spent in queue."""

        return (time.perf_counter() * 1000) - self.enqueued_at_ms

    def is_expired(self, ttl_ms: float) -> bool:
        """Time exceeding TTL in queue."""

        return self.queue_wait_ms() > ttl_ms

@dataclass
class TaskResult:
    """Result of task execution."""

    task_id: str
    success: bool
    result: Any = None
    error: str | None = None
    execution_ms: float = 0.0
