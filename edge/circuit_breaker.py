



import os
import sys
import time
import json
import threading
import logging
from enum import IntEnum
from dataclasses import dataclass, field, asdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from core.constants import (
    CB_FAILURE_THRESHOLD,
    CB_OPEN_DURATION_S,
    CB_EXTENDED_OPEN_DURATION_S,
    ROUTING_TIMEOUT_MS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("ember.circuit_breaker")


class CircuitState(IntEnum):

    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


@dataclass
class CircuitEvent:

    from_state: str
    to_state: str
    reason: str
    consecutive_failures: int
    trip_count: int
    timestamp_s: float


class CircuitBreaker:

    def __init__(
        self,
        failure_threshold: int = CB_FAILURE_THRESHOLD,
        open_duration_s: float = CB_OPEN_DURATION_S,
        extended_open_duration_s: float = CB_EXTENDED_OPEN_DURATION_S,
        ) -> None:
        self.failure_threshold = failure_threshold
        self.open_duration_s = open_duration_s
        self.extended_open_duration_s = extended_open_duration_s

        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._trip_count = 0
        self._last_failure_time: float | None = None
        self._opened_at: float | None = None
        self._current_cooldown_s = open_duration_s
        self._lock = threading.Lock()


        self._events: list[CircuitEvent] = []

    @property
    def state(self) -> CircuitState:

        with self._lock:
            if self._state == CircuitState.OPEN and self._opened_at is not None:
                elapsed = time.monotonic() - self._opened_at
                if elapsed >= self._current_cooldown_s:
                    self._transition(CircuitState.HALF_OPEN, "cooling period expired")
            return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def trip_count(self) -> int:
        return self._trip_count

    @property
    def last_failure_time(self) -> float | None:
        return self._last_failure_time

    @property
    def events(self) -> list[CircuitEvent]:
        return list(self._events)

    def should_allow_request(self) -> bool:

        current = self.state
        return current != CircuitState.OPEN

    def record_success(self) -> None:

        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._transition(CircuitState.CLOSED, "probe succeeded")
                self._current_cooldown_s = self.open_duration_s
            self._consecutive_failures = 0

    def record_failure(self) -> None:

        with self._lock:
            self._consecutive_failures += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:

                self._current_cooldown_s = self.extended_open_duration_s
                self._transition(
                    CircuitState.OPEN,
                              f"half-open probe failed (extended cooldown: {self.extended_open_duration_s}s)",
                )

            elif self._state == CircuitState.CLOSED:
                if self._consecutive_failures >= self.failure_threshold:
                    self._transition(
                        CircuitState.OPEN,
                        f"{self._consecutive_failures} consecutive failures "
                        f"(threshold: {self.failure_threshold})",
                    )

    def _transition(self, to_state: CircuitState, reason: str) -> None:


        from_state = self._state
        self._state = to_state

        if to_state == CircuitState.OPEN:
            self._opened_at = time.monotonic()
            self._trip_count += 1

        event = CircuitEvent(
            from_state=from_state.name,
            to_state=to_state.name,
            reason=reason,
            consecutive_failures=self._consecutive_failures,
            trip_count=self._trip_count,
            timestamp_s=round(time.monotonic(), 3),
        )
        self._events.append(event)

        log_msg = (
            f"[CIRCUIT] {from_state.name} -> {to_state.name} | "
            f"reason={reason} | failures={self._consecutive_failures} | "
            f"trips={self._trip_count}"
        )

        if to_state == CircuitState.OPEN:
            logger.warning(log_msg)
        elif to_state == CircuitState.CLOSED:
            logger.info(log_msg)

    def force_reset(self) -> None:

        with self._lock:
            self._transition(CircuitState.CLOSED, "manual reset")
            self._consecutive_failures = 0
            self._current_cooldown_s = self.open_duration_s

    def status(self) -> dict:

        return {
            "circuit_state": self.state.value,
            "circuit_state_name": self.state.name,
            "consecutive_failures": self._consecutive_failures,
            "trip_count": self._trip_count,
            "last_failure_time": self._last_failure_time,
            "current_cooldown_s": self._current_cooldown_s,
        }




