"""
Records telemetry data for task that pass through the admission gate.
"""

import time
import json
import logging
from dataclasses import dataclass, asdict
from enum import Enum


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ember.telemetry")


class AdmissionEvent(Enum):
    """Acceptable events the admission gate can record."""

    ADMITTED = "ADMITTED"
    QUEUED = "QUEUED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    DEQUEUED = "DEQUEUED"
    COMPLETED = "COMPLETED"

@dataclass
class QueueDriftRecord:
    """A queue drift measurement record."""

    task_id: str
    task_type: str
    event: str
    queue_wait_ms: float
    queue_depth: int
    active_workers: int
    timestamp_ms: float


class TelemetryRecorder:
    """ Records admission & queue metrics, Logs to structured JSON format."""

    def __init__(self) -> None:
        self.records: list[QueueDriftRecord] = []

    def record(
        self,
        task_id: str,
        task_type: str,
        event: AdmissionEvent,
        queue_wait_ms: float = 0.0,
        queue_depth: int = 0,
        active_workers: int = 0,

    )-> QueueDriftRecord:
        """Attaches an admission event with queue drift data."""
        rec = QueueDriftRecord(
            task_id=task_id,
            task_type=task_type,
            event=event.value,
            queue_wait_ms=round(queue_wait_ms, 3),
            queue_depth=queue_depth,
            active_workers=active_workers,
            timestamp_ms=round(time.perf_counter() * 1000, 3),
        )

        self.records.append(rec)
        logger.info("TELEMETRY | %s", json.dumps(asdict(rec)))
        return rec

    def get_records(self, event: AdmissionEvent | None = None) -> list[QueueDriftRecord]:
        """Returns data, optionally filtered by event type."""
        if event is None:
            return list(self.records)
        return [r for r in self.records if r.event == event.value]

    def clear(self) -> None:
        """Resets recorded metrics."""
        self.records.clear()
