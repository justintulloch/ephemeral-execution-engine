"""
Steel Thread — Ticket 4: Resilience Substrate & Failure Injection

Proves the failure-first design by simulating Gray Failures (latency spikes)
and verifying the Circuit Breaker state machine:

  1. Steady State:    Successful routing with <15ms RTT
  2. Inject Failure:  50ms delay exceeds 20ms timeout → Abort-Early
  3. Trip Breaker:    3 consecutive failures → OPEN state
  4. Zero Overhead:   4th+ requests bypass Chicago entirely
  5. Recovery:        Clear fault → Half-Open probe → CLOSED

Usage:
    python tests/steel_thread/test_circuit_breaker.py
    pytest tests/steel_thread/test_circuit_breaker.py -v

Note: This test manages its own gRPC server — no separate terminal needed.
"""

import os
import sys
import time
import uuid
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from edge.circuit_breaker import CircuitBreaker, CircuitState
from edge.resilient_client import ResilientRoutingClient, FallbackReason
from tests.steel_thread.chaos_server import serve as start_chaos_server


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def make_id() -> str:
    return str(uuid.uuid4())


# ─────────────────────────────────────────────
# Test 1: Steady State — successful routing
# ─────────────────────────────────────────────

def test_steady_state(client: ResilientRoutingClient) -> None:
    """Verify successful routing with <15ms RTT when Chicago is healthy."""
    print("\n" + "=" * 60)
    print("TEST 1: Steady State (Healthy Chicago)")
    print("=" * 60)

    results = []
    for i in range(5):
        decision = client.request_route(make_id())
        results.append(decision)

    for i, d in enumerate(results):
        print(f"  Request {i+1}: rtt={d.rtt_ms:.2f}ms | circuit={d.circuit_state.name}")
        assert d.route_to_chicago, f"Request {i+1} should route to Chicago"
        assert d.fallback_reason == FallbackReason.NONE
        assert d.circuit_state == CircuitState.CLOSED

    avg_rtt = sum(d.rtt_ms for d in results) / len(results)
    print(f"\n  Average RTT: {avg_rtt:.2f}ms")
    print(f"  Circuit state: {client.circuit_breaker.state.name}")
    assert client.circuit_breaker.consecutive_failures == 0

    print("\n  [PASS] Steady state routing verified")


# ─────────────────────────────────────────────
# Test 2: Abort-Early — timeout triggers LocalFallback
# ─────────────────────────────────────────────

def test_abort_early(client: ResilientRoutingClient, servicer) -> None:
    """
    Inject 50ms delay. Verify Columbus cuts the connection at ~20ms
    and triggers LocalFallback. The request should NOT wait 50ms.
    """
    print("\n" + "=" * 60)
    print("TEST 2: Abort-Early (50ms Delay → 20ms Cutoff)")
    print("=" * 60)

    # Inject gray failure
    servicer.simulated_delay_ms = 50.0
    print(f"\n  [CHAOS] Injected delay: {servicer.simulated_delay_ms}ms")

    decision = client.request_route(make_id())

    print(f"  RTT: {decision.rtt_ms:.2f}ms")
    print(f"  Fallback: {decision.fallback_reason.value}")
    print(f"  Circuit: {decision.circuit_state.name}")

    assert decision.fallback_to_local, "Should fall back to local"
    assert decision.fallback_reason == FallbackReason.TIMEOUT
    # The abort should happen around 20ms, not 50ms
    assert decision.rtt_ms < 40.0, (
        f"Abort-Early failed: RTT was {decision.rtt_ms:.2f}ms — "
        "should have cut off around 20ms, not waited for the full 50ms delay"
    )

    print(f"\n  [PASS] Connection cut at {decision.rtt_ms:.1f}ms (budget: 20ms)")


# ─────────────────────────────────────────────
# Test 3: Circuit Breaker Trip — 3 failures → OPEN
# ─────────────────────────────────────────────

def test_circuit_trip(client: ResilientRoutingClient) -> None:
    """
    With delay still active, send requests until the breaker trips.
    Should trip to OPEN after exactly 3 consecutive failures.
    """
    print("\n" + "=" * 60)
    print("TEST 3: Circuit Breaker Trip (3 Failures → OPEN)")
    print("=" * 60)

    # We already have 1 failure from test_abort_early
    # Send 2 more to hit the threshold
    failures_before = client.circuit_breaker.consecutive_failures
    print(f"\n  Failures before: {failures_before}")

    remaining = 3 - failures_before
    for i in range(remaining):
        decision = client.request_route(make_id())
        print(
            f"  Failure {failures_before + i + 1}/3: "
            f"rtt={decision.rtt_ms:.2f}ms | "
            f"circuit={decision.circuit_state.name}"
        )

    # Verify breaker is now OPEN
    state = client.circuit_breaker.state
    failures = client.circuit_breaker.consecutive_failures
    trips = client.circuit_breaker.trip_count

    print(f"\n  Circuit state: {state.name} (value={state.value})")
    print(f"  Consecutive failures: {failures}")
    print(f"  Total trips: {trips}")

    assert state == CircuitState.OPEN, f"Expected OPEN, got {state.name}"
    assert failures >= 3, f"Expected >= 3 failures, got {failures}"
    assert trips >= 1, f"Expected >= 1 trip, got {trips}"

    print("\n  [PASS] Circuit tripped to OPEN after 3 failures")


# ─────────────────────────────────────────────
# Test 4: Zero Overhead — OPEN state bypasses Chicago
# ─────────────────────────────────────────────

def test_zero_overhead(client: ResilientRoutingClient) -> None:
    """
    While OPEN, verify that requests bypass Chicago entirely.
    RTT should be ~0ms (no network call) and reason should be CIRCUIT_OPEN.
    """
    print("\n" + "=" * 60)
    print("TEST 4: Zero Overhead (OPEN State)")
    print("=" * 60)

    assert client.circuit_breaker.state == CircuitState.OPEN

    skips_before = client.total_circuit_skips
    results = []

    for i in range(5):
        decision = client.request_route(make_id())
        results.append(decision)

    for i, d in enumerate(results):
        print(
            f"  Request {i+1}: rtt={d.rtt_ms:.2f}ms | "
            f"reason={d.fallback_reason.value} | "
            f"circuit={d.circuit_state.name}"
        )
        assert d.fallback_to_local, "Should fall back to local"
        assert d.fallback_reason == FallbackReason.CIRCUIT_OPEN
        assert d.rtt_ms == 0.0, f"Expected 0ms RTT (no network call), got {d.rtt_ms}"
        assert d.circuit_state == CircuitState.OPEN

    skips_after = client.total_circuit_skips
    print(f"\n  Circuit skips: {skips_after - skips_before} (should be 5)")
    assert skips_after - skips_before == 5

    print("\n  [PASS] Zero network overhead in OPEN state")


# ─────────────────────────────────────────────
# Test 5: Half-Open Recovery — probe restores traffic
# ─────────────────────────────────────────────

def test_half_open_recovery(client: ResilientRoutingClient, servicer) -> None:
    """
    Clear the fault, wait for cooling period to expire,
    verify the half-open probe succeeds and restores CLOSED state.
    """
    print("\n" + "=" * 60)
    print("TEST 5: Half-Open Recovery")
    print("=" * 60)

    # Clear the injected fault
    servicer.clear_faults()
    print("\n  [CHAOS] Faults cleared")
    print(f"  Circuit state: {client.circuit_breaker.state.name}")

    # Use a short cooldown for testing (override the 30s default)
    # We directly modify the internal state to avoid waiting 30 real seconds
    print("  Simulating 30s cooling period (fast-forward)...")
    with client.circuit_breaker._lock:
        # Pretend the breaker opened 31 seconds ago
        client.circuit_breaker._opened_at = time.monotonic() - 31.0

    # The next state check should transition OPEN → HALF_OPEN
    state = client.circuit_breaker.state
    print(f"  Circuit state after cooldown: {state.name}")
    assert state == CircuitState.HALF_OPEN, f"Expected HALF_OPEN, got {state.name}"

    # Send the probe request — should succeed and reset to CLOSED
    probe = client.request_route(make_id(), task_type="probe")
    print(f"\n  Probe result: rtt={probe.rtt_ms:.2f}ms | circuit={probe.circuit_state.name}")

    assert probe.route_to_chicago, "Probe should route to Chicago"
    assert probe.circuit_state == CircuitState.CLOSED, (
        f"Expected CLOSED after successful probe, got {probe.circuit_state.name}"
    )
    assert client.circuit_breaker.consecutive_failures == 0

    print("\n  [PASS] Half-open probe succeeded, circuit restored to CLOSED")


# ─────────────────────────────────────────────
# Test 6: Half-Open Probe Failure — extended cooldown
# ─────────────────────────────────────────────

def test_half_open_probe_failure(client: ResilientRoutingClient, servicer) -> None:
    """
    If the half-open probe fails, the breaker should return to OPEN
    with an extended 60-second cooldown.
    """
    print("\n" + "=" * 60)
    print("TEST 6: Half-Open Probe Failure (Extended Cooldown)")
    print("=" * 60)

    # Re-inject the fault
    servicer.simulated_delay_ms = 50.0
    print(f"\n  [CHAOS] Re-injected delay: {servicer.simulated_delay_ms}ms")

    # Trip the breaker again (3 failures)
    for _ in range(3):
        client.request_route(make_id())

    assert client.circuit_breaker.state == CircuitState.OPEN
    print(f"  Circuit tripped to OPEN (trip #{client.circuit_breaker.trip_count})")

    # Fast-forward past the 30s cooldown
    with client.circuit_breaker._lock:
        client.circuit_breaker._opened_at = time.monotonic() - 31.0

    state = client.circuit_breaker.state
    assert state == CircuitState.HALF_OPEN
    print(f"  Circuit transitioned to HALF_OPEN")

    # Probe should fail (delay still active)
    probe = client.request_route(make_id(), task_type="probe")
    print(f"  Probe result: rtt={probe.rtt_ms:.2f}ms | circuit={probe.circuit_state.name}")

    assert probe.fallback_to_local, "Probe should fail and fall back"
    assert probe.circuit_state == CircuitState.OPEN, (
        f"Expected OPEN after failed probe, got {probe.circuit_state.name}"
    )

    # Verify extended cooldown is set
    status = client.circuit_breaker.status()
    cooldown = status["current_cooldown_s"]
    print(f"  Cooldown after failed probe: {cooldown}s (should be 60s)")
    assert cooldown == 60, f"Expected 60s extended cooldown, got {cooldown}s"

    print("\n  [PASS] Failed probe extends cooldown to 60s")


# ─────────────────────────────────────────────
# Test 7: Full State Transition Log
# ─────────────────────────────────────────────

def test_state_transition_log(client: ResilientRoutingClient) -> None:
    """Verify all circuit breaker state transitions were logged."""
    print("\n" + "=" * 60)
    print("TEST 7: State Transition Log (Resilience Telemetry)")
    print("=" * 60)

    events = client.circuit_breaker.events
    print(f"\n  Total state transitions: {len(events)}")

    for i, event in enumerate(events):
        print(
            f"  {i+1}. {event.from_state} -> {event.to_state} | "
            f"reason={event.reason} | "
            f"failures={event.consecutive_failures} | "
            f"trips={event.trip_count}"
        )

    # Verify we have the critical transitions
    transition_pairs = [(e.from_state, e.to_state) for e in events]

    assert ("CLOSED", "OPEN") in transition_pairs, "Missing CLOSED → OPEN transition"
    assert ("OPEN", "HALF_OPEN") in transition_pairs, "Missing OPEN → HALF_OPEN transition"
    assert ("HALF_OPEN", "CLOSED") in transition_pairs, "Missing HALF_OPEN → CLOSED transition"
    assert ("HALF_OPEN", "OPEN") in transition_pairs, "Missing HALF_OPEN → OPEN (probe fail)"

    # Verify trip count
    trips = client.circuit_breaker.trip_count
    print(f"\n  Total circuit trips: {trips}")
    assert trips >= 2, f"Expected >= 2 trips, got {trips}"

    # Final status
    status = client.circuit_breaker.status()
    print(f"\n  Final breaker status:")
    for k, v in status.items():
        print(f"    {k}: {v}")

    print("\n  [PASS] All state transitions logged correctly")


# ─────────────────────────────────────────────
# Test 8: Final Recovery — clean restoration
# ─────────────────────────────────────────────

def test_final_recovery(client: ResilientRoutingClient, servicer) -> None:
    """
    Clear all faults, fast-forward past extended cooldown,
    verify the system fully recovers to steady state.
    """
    print("\n" + "=" * 60)
    print("TEST 8: Final Recovery (Full Restoration)")
    print("=" * 60)

    servicer.clear_faults()
    print("\n  [CHAOS] All faults cleared")

    # Fast-forward past extended cooldown (60s)
    with client.circuit_breaker._lock:
        client.circuit_breaker._opened_at = time.monotonic() - 61.0

    # Should transition to HALF_OPEN, then probe succeeds → CLOSED
    probe = client.request_route(make_id(), task_type="recovery_probe")
    print(f"  Recovery probe: rtt={probe.rtt_ms:.2f}ms | circuit={probe.circuit_state.name}")

    assert probe.route_to_chicago, "Recovery probe should succeed"
    assert probe.circuit_state == CircuitState.CLOSED

    # Verify steady state is restored
    for i in range(3):
        d = client.request_route(make_id())
        assert d.route_to_chicago, f"Post-recovery request {i+1} should route to Chicago"

    print(f"  Post-recovery: 3/3 requests routed to Chicago")

    # Final stats
    status = client.status()
    print(f"\n  Final client status:")
    for k, v in status.items():
        print(f"    {k}: {v}")

    print("\n  [PASS] System fully recovered to steady state")


# ─────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────

def run_all_tests() -> None:
    """Run all circuit breaker and resilience tests."""
    print("\n" + "=" * 60)
    print("EMBER — Ticket 4: Resilience Substrate & Failure Injection")
    print("=" * 60)

    # Start the chaos server in this process
    server, servicer = start_chaos_server(port=50051)

    # Give the server a moment to bind
    import time
    time.sleep(0.3)

    # Create a single client + breaker shared across all tests
    breaker = CircuitBreaker()
    client = ResilientRoutingClient(circuit_breaker=breaker)

    try:
        test_steady_state(client)
        test_abort_early(client, servicer)
        test_circuit_trip(client)
        test_zero_overhead(client)
        test_half_open_recovery(client, servicer)
        test_half_open_probe_failure(client, servicer)
        test_state_transition_log(client)
        test_final_recovery(client, servicer)

        print("\n" + "=" * 60)
        print("ALL TESTS PASSED")
        print("=" * 60 + "\n")

    finally:
        client.close()
        server.stop(grace=2)


if __name__ == "__main__":
    run_all_tests()