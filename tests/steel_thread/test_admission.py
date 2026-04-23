import os
import sys
import time
import threading


# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from edge.admission import AdmissionGate, AdmissionResult
from core.task import TaskManifest, TaskState
from core.telemetry import AdmissionEvent


# ─────────────────────────────────────────────
# Test 1: Concurrency wall (C_max = 1)
# ─────────────────────────────────────────────

def test_concurrency_wall() -> None:
    """
    With C_max=1:
      - Request A should be admitted
      - Request B should be queued
      - Requests C-Z (23+) should be rejected with 429
    """
    print("\n" + "=" * 60)
    print("TEST 1: Concurrency Wall (C_max = 1)")
    print("=" * 60)

    gate = AdmissionGate(max_concurrency=1, max_queue_depth=22)

    # Request A: should be admitted
    task_a = TaskManifest(task_type="inference_v1", timeout_ms=400)
    result_a = gate.submit(task_a)

    print(f"\n  Request A: {result_a.reason}")
    assert result_a.admitted, f"Request A should be admitted, got: {result_a.reason}"
    assert gate.active_workers == 1, f"Expected 1 active worker, got {gate.active_workers}"

    # Request B: should be queued (worker slot taken)
    task_b = TaskManifest(task_type="inference_v1", timeout_ms=400)
    result_b = gate.submit(task_b)

    print(f"  Request B: {result_b.reason}")
    assert result_b.queued, f"Request B should be queued, got: {result_b.reason}"
    assert gate.queue_depth == 1, f"Expected queue depth 1, got {gate.queue_depth}"

    # Requests C through Z+: fill the queue (21 more), then overflow
    queued_count = 1  # B is already queued
    rejected_count = 0

    for i in range(23):
        task = TaskManifest(task_type="inference_v1")
        result = gate.submit(task)

        if result.queued:
            queued_count += 1
        elif result.rejected:
            rejected_count += 1

    print(f"\n  Queued:   {queued_count} (max {gate.max_queue_depth})")
    print(f"  Rejected: {rejected_count}")

    assert queued_count == 22, f"Expected 22 queued (full), got {queued_count}"
    assert rejected_count >= 2, f"Expected at least 2 rejected, got {rejected_count}"

    # Verify the last rejection reason contains 429
    last_task = TaskManifest(task_type="overflow_test")
    last_result = gate.submit(last_task)
    print(f"  Overflow:  {last_result.reason}")
    assert last_result.rejected, "Overflow request should be rejected"
    assert "429" in last_result.reason, f"Should contain '429', got: {last_result.reason}"

    # Status snapshot
    status = gate.status()
    print(f"\n  Gate Status: {status}")

    print("\n  [PASS] Concurrency wall enforced correctly")

# ─────────────────────────────────────────────
# Test 2: Worker release and dequeue
# ─────────────────────────────────────────────

def test_release_and_dequeue() -> None:
    """
    After releasing a worker slot, the next queued task
    should be available for dequeue.
    """
    print("\n" + "=" * 60)
    print("TEST 2: Worker Release & Dequeue")
    print("=" * 60)

    gate = AdmissionGate(max_concurrency=1, max_queue_depth=22)

    # Admit task A
    task_a = TaskManifest(task_type="inference_v1")
    gate.submit(task_a)

    # Queue task B
    task_b = TaskManifest(task_type="inference_v1")
    gate.submit(task_b)

    print(f"\n  Before release: active={gate.active_workers}, queue={gate.queue_depth}")
    assert gate.active_workers == 1
    assert gate.queue_depth == 1

    # Simulate task A completing
    gate.release(task_a)

    print(f"  After release:  active={gate.active_workers}, queue={gate.queue_depth}")

    # Dequeue task B
    dequeued = gate.dequeue()
    assert dequeued is not None, "Should have dequeued task B"
    assert dequeued.task_id == task_b.task_id, "Dequeued task should be task B"

    print(f"  Dequeued: task_id={dequeued.task_id}")
    print(f"  Queue wait: {dequeued.queue_wait_ms():.2f}ms")

    print("\n  [PASS] Release and dequeue working correctly")

# ─────────────────────────────────────────────
# Test 3: Dead Letter TTL
# ─────────────────────────────────────────────

def test_dead_letter_ttl() -> None:
    """
    Tasks waiting longer than 100ms in the queue should be evicted.
    """
    print("\n" + "=" * 60)
    print("TEST 3: Dead Letter TTL (100ms)")
    print("=" * 60)

    gate = AdmissionGate(max_concurrency=1, max_queue_depth=22, queue_ttl_ms=100)

    # Fill the worker slot
    task_a = TaskManifest(task_type="inference_v1")
    gate.submit(task_a)

    # Queue a task
    task_b = TaskManifest(task_type="inference_v1")
    gate.submit(task_b)

    print(f"\n  Queue depth before wait: {gate.queue_depth}")
    assert gate.queue_depth == 1

    # Wait for TTL to expire
    print("  Waiting 150ms for TTL expiry...")
    time.sleep(0.15)

    # Attempt to dequeue — should get None because task_b expired
    dequeued = gate.dequeue()
    print(f"  Queue depth after TTL:   {gate.queue_depth}")
    print(f"  Dequeued: {dequeued}")
    print(f"  Expired count: {gate.total_expired}")

    assert dequeued is None, "Expired task should have been evicted"
    assert gate.total_expired == 1, f"Expected 1 expired, got {gate.total_expired}"

    # Verify telemetry recorded the expiration
    expired_records = gate.telemetry.get_records(AdmissionEvent.EXPIRED)
    assert len(expired_records) >= 1, "Should have telemetry record for expiration"
    print(f"  Telemetry: {expired_records[0].queue_wait_ms:.2f}ms wait recorded")

    print("\n  [PASS] Dead Letter TTL enforced correctly")

# ─────────────────────────────────────────────
# Test 4: Queue drift telemetry
# ─────────────────────────────────────────────

def test_queue_drift_telemetry() -> None:
    """
    Verify that queue entry time is logged for queued tasks.
    The drift metric is t_exit - t_entry.
    """
    print("\n" + "=" * 60)
    print("TEST 4: Queue Drift Telemetry")
    print("=" * 60)

    gate = AdmissionGate(max_concurrency=1, max_queue_depth=22)

    # Admit task A
    task_a = TaskManifest(task_type="inference_v1")
    gate.submit(task_a)

    # Queue task B
    task_b = TaskManifest(task_type="inference_v1")
    gate.submit(task_b)

    # Wait a bit so drift is measurable
    time.sleep(0.02)  # 20ms

    # Release A, dequeue B
    gate.release(task_a)
    dequeued = gate.dequeue()

    # Check telemetry
    queued_records = gate.telemetry.get_records(AdmissionEvent.QUEUED)
    dequeued_records = gate.telemetry.get_records(AdmissionEvent.DEQUEUED)

    print(f"\n  Queued events:   {len(queued_records)}")
    print(f"  Dequeued events: {len(dequeued_records)}")

    assert len(queued_records) >= 1, "Should have QUEUED telemetry record"
    assert len(dequeued_records) >= 1, "Should have DEQUEUED telemetry record"

    # The dequeue record should show queue wait time
    drift = dequeued_records[0].queue_wait_ms
    print(f"  Queue drift (t_exit - t_entry): {drift:.2f}ms")
    assert drift > 0, "Queue drift should be positive"

    print("\n  [PASS] Queue drift telemetry recording correctly")

# ─────────────────────────────────────────────
# Test 5: Memory pressure hysteresis
# ─────────────────────────────────────────────

def test_memory_pressure_hysteresis() -> None:
    """
    Verify the 90%/75% hysteresis deadband:
      - At 90%: admission throttled
      - At 80%: still throttled (above 75%)
      - At 74%: un-throttled
    """
    print("\n" + "=" * 60)
    print("TEST 5: Memory Pressure Hysteresis (90%/75%)")
    print("=" * 60)

    gate = AdmissionGate(max_concurrency=10, max_queue_depth=22)
    monitor = gate.memory_monitor

    total = monitor.total_memory_mb

    # Below threshold — should allow
    allowed = monitor.update(total * 0.5)
    print(f"\n  50% usage: allowed={allowed}")
    assert allowed, "Should allow at 50%"

    # Hit 90% — should throttle
    allowed = monitor.update(total * 0.91)
    print(f"  91% usage: allowed={allowed}")
    assert not allowed, "Should throttle at 91%"

    # Drop to 80% — still throttled (hysteresis: need to drop below 75%)
    allowed = monitor.update(total * 0.80)
    print(f"  80% usage: allowed={allowed} (still throttled — above 75% low threshold)")
    assert not allowed, "Should still be throttled at 80%"

    # Submit a task — should be rejected due to memory pressure
    task = TaskManifest(task_type="inference_v1")
    result = gate.submit(task)
    print(f"  Submit during throttle: {result.reason}")
    assert result.rejected, "Should reject during memory pressure"
    assert "429" in result.reason, "Should contain 429"

    # Drop below 75% — un-throttle
    allowed = monitor.update(total * 0.74)
    print(f"  74% usage: allowed={allowed} (cleared)")
    assert allowed, "Should un-throttle below 75%"

    # Now admission should work again
    task2 = TaskManifest(task_type="inference_v1")
    result2 = gate.submit(task2)
    print(f"  Submit after clear: {result2.reason}")
    assert result2.admitted, "Should admit after pressure clears"

    print("\n  [PASS] Memory pressure hysteresis working correctly")

# ─────────────────────────────────────────────
# Test 6: Concurrent simulation (A=executing, B=queued, C-Z=rejected)
# ─────────────────────────────────────────────

def test_concurrent_simulation() -> None:
    """
    Full simulation with threading:
      - Task A: 400ms simulated execution
      - Task B: submitted while A is running → queued
      - Tasks C-Z: overflow → 429
      - After A completes, B should be dequeued
    """
    print("\n" + "=" * 60)
    print("TEST 6: Full Concurrent Simulation")
    print("=" * 60)

    gate = AdmissionGate(max_concurrency=1, max_queue_depth=22)
    results: dict[str, AdmissionResult] = {}
    completed = threading.Event()

    def execute_task(task: TaskManifest, delay_s: float) -> None:
        """Simulate a worker executing a task."""
        time.sleep(delay_s)
        gate.release(task)
        completed.set()

    # Request A: 400ms simulated execution
    task_a = TaskManifest(task_type="inference_v1", args={"delay": 400})
    result_a = gate.submit(task_a)
    results["A"] = result_a
    print(f"\n  Request A: {result_a.reason}")

    # Start execution in background
    worker = threading.Thread(target=execute_task, args=(task_a, 0.4))
    worker.start()

    # Request B: submitted immediately → should queue
    task_b = TaskManifest(task_type="inference_v1", args={"delay": 100})
    result_b = gate.submit(task_b)
    results["B"] = result_b
    print(f"  Request B: {result_b.reason}")

    # Requests C through Z+: fill queue then overflow
    overflow_tasks = []
    for i in range(23):
        task = TaskManifest(task_type="inference_v1")
        result = gate.submit(task)
        overflow_tasks.append((chr(67 + i), result))

    queued = sum(1 for _, r in overflow_tasks if r.queued)
    rejected = sum(1 for _, r in overflow_tasks if r.rejected)
    print(f"\n  C-Z Results: {queued} queued, {rejected} rejected")

    # Wait for A to complete
    completed.wait(timeout=2.0)
    worker.join()

    print(f"\n  After A completes:")
    print(f"    Active workers: {gate.active_workers}")
    print(f"    Queue depth:    {gate.queue_depth}")

    # Final status
    status = gate.status()
    print(f"\n  Final Gate Status:")
    for k, v in status.items():
        print(f"    {k}: {v}")

    # Assertions
    assert result_a.admitted, "A should be admitted"
    assert result_b.queued, "B should be queued"
    assert rejected >= 2, f"Should have at least 2 rejections, got {rejected}"

    print("\n  [PASS] Concurrent simulation completed successfully")

# ─────────────────────────────────────────────
# Test 7: Empty Queue Success (no routing penalty)
# ─────────────────────────────────────────────

def test_empty_queue_success() -> None:
    """
    When Request A finishes, Request B should move from Queued
    to Executing WITHOUT an additional 20ms routing penalty.

    This validates that the local fallback doesn't accidentally
    trigger a Chicago lookup for a task already held in the local queue.
    """
    print("\n" + "=" * 60)
    print("TEST 7: Empty Queue Success (No Routing Penalty)")
    print("=" * 60)

    gate = AdmissionGate(max_concurrency=1, max_queue_depth=22)

    # Admit A, queue B
    task_a = TaskManifest(task_type="inference_v1")
    task_b = TaskManifest(task_type="inference_v1")
    gate.submit(task_a)
    gate.submit(task_b)

    assert gate.active_workers == 1
    assert gate.queue_depth == 1

    # Release A — measure how fast B becomes available
    gate.release(task_a)

    start = time.perf_counter()
    dequeued = gate.dequeue()
    dequeue_time_ms = (time.perf_counter() - start) * 1000

    assert dequeued is not None, "B should be dequeued"
    assert dequeued.task_id == task_b.task_id

    print(f"\n  Dequeue latency: {dequeue_time_ms:.3f}ms")
    print(f"  Routing timeout: 20ms")

    # The dequeue should be sub-millisecond — purely local, no routing
    assert dequeue_time_ms < 5.0, (
        f"Dequeue took {dequeue_time_ms:.3f}ms — should be <5ms. "
        "If >20ms, a Chicago routing lookup may have been triggered."
    )

    print(f"  Result: {dequeue_time_ms:.3f}ms << 20ms — no routing penalty")
    print("\n  [PASS] Queued task dispatched locally without routing overhead")

# ─────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────

def run_all_tests() -> None:
    """Run all admission gate tests."""
    print("\n" + "=" * 60)
    print("EMBER — Ticket 2: Admission Controller Steel Thread")
    print("=" * 60)

    test_concurrency_wall()
    test_release_and_dequeue()
    test_dead_letter_ttl()
    test_queue_drift_telemetry()
    test_memory_pressure_hysteresis()
    test_concurrent_simulation()
    test_empty_queue_success()

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_all_tests()
