"""
Steel Thread — Final Ticket: Local Execution Substrate

Validates three pillars of the execution fabric:

  Task 1: Worker Pool Boot — 43 workers boot to Wait state, recycle at 100
  Task 2: Shared Memory Scale-Up — 400MB mmap shared without RAM doubling
  Task 3: Task Leasing — expired leases reclaimed at 450ms threshold

Usage:
    python tests/steel_thread/test_local_substrate.py
    pytest tests/steel_thread/test_local_substrate.py -v
"""

import os
import sys
import time
import multiprocessing

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from core.constants import (
    WORKER_POOL_SIZE,
    WORKER_RECYCLE_AFTER_JOBS,
    WORKER_MEMORY_LIMIT_MB,
    TASK_LEASE_TIMEOUT_MS,
)
from core.task import TaskManifest, TaskState
from core.shared_memory import SharedModelWeights
from workers.pool import WorkerPool, WorkerState
from workers.lease_manager import LeaseManager

# Use fork context for Python 3.14
_mp = multiprocessing.get_context("fork")


# ─────────────────────────────────────────────
# Task 1: Worker Pool Boot
# ─────────────────────────────────────────────

def test_worker_pool_boot() -> None:
    """
    Boot 43 workers and verify all reach Wait state.
    Each worker must send REGISTER and begin emitting heartbeats.
    """
    print("\n" + "=" * 60)
    print("TASK 1: Worker Pool Boot (43 Workers → Wait State)")
    print("=" * 60)

    pool = WorkerPool(pool_size=WORKER_POOL_SIZE)

    try:
        boot_start = time.monotonic()
        success = pool.boot(timeout_s=15.0)
        boot_elapsed = time.monotonic() - boot_start

        print(f"\n  Boot time: {boot_elapsed:.2f}s")
        print(f"  Boot success: {success}")

        assert success, "Worker pool boot failed"

        # Verify all workers are in Wait state
        status = pool.status()
        print(f"  Pool size: {status['pool_size']}")
        print(f"  Active count: {status['active_count']}")
        print(f"  All in Wait: {status['all_in_wait']}")
        print(f"  State distribution: {status['state_distribution']}")

        assert status["pool_size"] == WORKER_POOL_SIZE, (
            f"Expected {WORKER_POOL_SIZE} workers, got {status['pool_size']}"
        )
        assert status["all_in_wait"], "Not all workers reached Wait state"
        assert status["state_distribution"].get("WAIT", 0) == WORKER_POOL_SIZE

        # Verify each worker has a PID and recycle limit
        for wid in range(WORKER_POOL_SIZE):
            info = pool.get_worker_info(wid)
            assert info is not None, f"Worker {wid} not found"
            assert info.pid is not None, f"Worker {wid} has no PID"
            assert info.state == WorkerState.WAIT, (
                f"Worker {wid} in {info.state.value}, expected WAIT"
            )
            assert info.recycle_limit == WORKER_RECYCLE_AFTER_JOBS

        print(f"\n  All {WORKER_POOL_SIZE} workers verified:")
        print(f"    - PID assigned: ✓")
        print(f"    - State = WAIT: ✓")
        print(f"    - Recycle at {WORKER_RECYCLE_AFTER_JOBS} jobs: ✓")

        # Verify heartbeats were received during boot
        # Workers send their first heartbeat immediately after REGISTER,
        # and those were consumed during boot(). We verify by checking
        # that each worker has a valid last_heartbeat timestamp.
        workers_with_heartbeat = sum(
            1 for wid in range(WORKER_POOL_SIZE)
            if pool.get_worker_info(wid).last_heartbeat # type: ignore
        )
        print(f"    - Workers with heartbeat: {workers_with_heartbeat}/{WORKER_POOL_SIZE}")
        assert workers_with_heartbeat == WORKER_POOL_SIZE, (
            f"Only {workers_with_heartbeat}/{WORKER_POOL_SIZE} workers have heartbeat timestamps"
        )

        print("\n  [PASS] Worker pool boot verified")

    finally:
        pool.shutdown()


# ─────────────────────────────────────────────
# Task 2: Shared Memory Scale-Up (400MB)
# ─────────────────────────────────────────────

def _reader_process(
    path: str,
    num_elements: int,
    ready_event: multiprocessing.Event, # type: ignore
    done_event: multiprocessing.Event, # type: ignore
    result_queue: multiprocessing.Queue,
) -> None:
    """Child process: attach to shared memory, verify integrity, then wait."""
    import numpy as np

    mmap = np.memmap(path, dtype="float32", mode="r", shape=(num_elements,))

    # Verify integrity: first 100 elements should be 0..99
    expected = np.arange(100, dtype="float32")
    actual = np.array(mmap[:100])
    integrity_ok = np.array_equal(expected, actual)

    # Force the OS to page in ALL the data
    _ = np.sum(mmap)

    result_queue.put({
        "integrity": integrity_ok,
        "pid": os.getpid(),
    })

    # Signal that we're ready, then wait so both readers hold pages simultaneously
    ready_event.set()
    done_event.wait(timeout=30)


def test_shared_memory_400mb() -> None:
    """
    Load a 400MB dummy file into shared memory.
    Two separate processes must read it without doubling RAM.

    We measure system-wide memory because per-process RSS counts
    shared mmap pages in each process — making it look like duplication
    even when the OS is sharing physical pages correctly.
    """
    print("\n" + "=" * 60)
    print("TASK 2: Shared Memory Scale-Up (400MB)")
    print("=" * 60)

    import psutil

    weights = SharedModelWeights(
        path="/tmp/ember_test_400mb.mmap",
        size_mb=WORKER_MEMORY_LIMIT_MB,
    )

    try:
        # Host creates the shared weights
        print(f"\n  Creating {WORKER_MEMORY_LIMIT_MB}MB shared weight file...")
        host_mmap = weights.create()

        file_size_mb = os.path.getsize(weights.path) / (1024 * 1024)
        print(f"  File size on disk: {file_size_mb:.1f}MB")
        assert abs(file_size_mb - WORKER_MEMORY_LIMIT_MB) < 1.0

        # Verify host can read correctly
        assert weights.verify_integrity(host_mmap), "Host integrity check failed"
        print(f"  Host integrity: verified")

        # Measure system memory BEFORE readers
        mem_before = psutil.virtual_memory().used / (1024 * 1024)
        print(f"\n  System memory before readers: {mem_before:.0f}MB")

        # Spawn two reader processes
        print(f"  Spawning 2 reader processes...")
        q1 = _mp.Queue()
        q2 = _mp.Queue()
        ready1 = _mp.Event()
        ready2 = _mp.Event()
        done = _mp.Event()

        p1 = _mp.Process(
            target=_reader_process,
            args=(weights.path, weights.num_elements, ready1, done, q1),
        )
        p2 = _mp.Process(
            target=_reader_process,
            args=(weights.path, weights.num_elements, ready2, done, q2),
        )

        p1.start()
        p2.start()

        # Wait for both readers to have all pages loaded
        ready1.wait(timeout=30)
        ready2.wait(timeout=30)

        # Both readers are now holding all 400MB of pages in memory
        # Measure system memory WITH both readers active
        time.sleep(0.5)  # Let OS settle
        mem_after = psutil.virtual_memory().used / (1024 * 1024)
        system_delta_mb = mem_after - mem_before

        # Release readers
        done.set()
        p1.join(timeout=10)
        p2.join(timeout=10)

        r1 = q1.get()
        r2 = q2.get()

        print(f"\n  Reader 1 (PID {r1['pid']}): integrity={'PASS' if r1['integrity'] else 'FAIL'}")
        print(f"  Reader 2 (PID {r2['pid']}): integrity={'PASS' if r2['integrity'] else 'FAIL'}")

        assert r1["integrity"], "Reader 1 integrity failed"
        assert r2["integrity"], "Reader 2 integrity failed"

        # The critical check: system-wide memory increase
        # If pages are shared: delta ≈ 400MB (one copy + process overhead)
        # If pages are duplicated: delta ≈ 800MB (two copies)
        # We use 600MB as the threshold — well above shared, well below duplicated
        shared_threshold_mb = WORKER_MEMORY_LIMIT_MB * 1.5  # 600MB
        duplicated_estimate = WORKER_MEMORY_LIMIT_MB * 2.0  # 800MB

        print(f"\n  System memory after readers: {mem_after:.0f}MB")
        print(f"  System delta: {system_delta_mb:.0f}MB")
        print(f"  Shared threshold: <{shared_threshold_mb:.0f}MB")
        print(f"  Duplicated estimate: ~{duplicated_estimate:.0f}MB")

        assert system_delta_mb < shared_threshold_mb, (
            f"System memory increased by {system_delta_mb:.0f}MB — "
            f"exceeds {shared_threshold_mb:.0f}MB threshold. "
            f"Pages may not be shared."
        )

        print(f"\n  Result: {system_delta_mb:.0f}MB << {duplicated_estimate:.0f}MB")
        print(f"  Pages are shared — not duplicated")

        print("\n  [PASS] 400MB shared memory verified — no RAM doubling")

    finally:
        weights.cleanup()


# ─────────────────────────────────────────────
# Task 3: Task Leasing
# ─────────────────────────────────────────────

def test_lease_normal_completion() -> None:
    """Tasks that complete within the lease timeout should succeed normally."""
    print("\n" + "=" * 60)
    print("TASK 3a: Lease — Normal Completion")
    print("=" * 60)

    lm = LeaseManager(lease_timeout_ms=TASK_LEASE_TIMEOUT_MS)

    task = TaskManifest(task_type="inference_v1")
    lm.grant_lease(task, worker_id=1)

    assert task.state == TaskState.ASSIGNED
    assert task.assigned_worker_id == 1
    print(f"\n  Lease granted: task={task.task_id[:8]}, worker=1")

    # Simulate execution
    lm.start_execution(task)
    assert task.state == TaskState.RUNNING
    print(f"  State: RUNNING")

    # Simulate completion within timeout
    time.sleep(0.05)  # 50ms — well within 450ms
    lm.complete_lease(task)

    assert task.state == TaskState.COMPLETED
    assert lm.active_count == 0
    elapsed = task.lease_elapsed_ms() or 0.0
    print(f"  Completed in ~{elapsed:.1f}ms (limit: {TASK_LEASE_TIMEOUT_MS}ms)")

    # Check leases — nothing should be reclaimed
    reclaimed = lm.check_leases()
    assert len(reclaimed) == 0, "Nothing should be reclaimed"

    print("\n  [PASS] Normal completion within lease timeout")


def test_lease_expiry_requeue() -> None:
    """Tasks exceeding the lease timeout should be reclaimed and requeued."""
    print("\n" + "=" * 60)
    print("TASK 3b: Lease — Expiry & Requeue (450ms Threshold)")
    print("=" * 60)

    # Use a short timeout for testing (100ms instead of 450ms)
    test_timeout_ms = 100.0
    lm = LeaseManager(lease_timeout_ms=test_timeout_ms)

    task = TaskManifest(task_type="inference_v1", max_retries=2)
    lm.grant_lease(task, worker_id=5)
    lm.start_execution(task)

    print(f"\n  Lease granted: task={task.task_id[:8]}, worker=5")
    print(f"  Lease timeout: {test_timeout_ms}ms")
    print(f"  Retries remaining: {task.retries_remaining}")

    # Simulate worker hanging
    print(f"  Simulating worker hang (waiting {test_timeout_ms + 50}ms)...")
    time.sleep((test_timeout_ms + 50) / 1000.0)

    # Check leases — task should be reclaimed
    reclaimed = lm.check_leases()

    assert len(reclaimed) == 1, f"Expected 1 reclaimed, got {len(reclaimed)}"
    reclaimed_task = reclaimed[0]

    print(f"\n  Reclaimed task: {reclaimed_task.task_id[:8]}")
    print(f"  New state: {reclaimed_task.state.value}")
    print(f"  Retries remaining: {reclaimed_task.retries_remaining}")

    assert reclaimed_task.state == TaskState.QUEUED, (
        f"Expected QUEUED, got {reclaimed_task.state.value}"
    )
    assert reclaimed_task.retries_remaining == 1, (
        f"Expected 1 retry remaining, got {reclaimed_task.retries_remaining}"
    )
    assert reclaimed_task.assigned_worker_id is None, "Worker ID should be cleared"
    assert reclaimed_task.assigned_at_ms is None, "Assigned time should be cleared"

    assert lm.active_count == 0, "No active leases should remain"

    # Check telemetry
    events = lm.events
    assert len(events) == 1
    assert events[0].action == "requeued"
    print(f"  Telemetry: action={events[0].action}, elapsed={events[0].elapsed_ms:.1f}ms")

    print("\n  [PASS] Expired lease reclaimed and requeued")


def test_lease_expiry_failed() -> None:
    """Tasks with no retries left should move to FAILED (poison pill protection)."""
    print("\n" + "=" * 60)
    print("TASK 3c: Lease — Expiry → Failed (No Retries)")
    print("=" * 60)

    test_timeout_ms = 100.0
    lm = LeaseManager(lease_timeout_ms=test_timeout_ms)

    task = TaskManifest(task_type="inference_v1", max_retries=0)
    lm.grant_lease(task, worker_id=7)
    lm.start_execution(task)

    print(f"\n  Lease granted: task={task.task_id[:8]}, worker=7")
    print(f"  Retries remaining: {task.retries_remaining} (zero — poison pill test)")

    # Simulate worker hanging
    time.sleep((test_timeout_ms + 50) / 1000.0)

    reclaimed = lm.check_leases()

    assert len(reclaimed) == 1
    assert reclaimed[0].state == TaskState.FAILED, (
        f"Expected FAILED, got {reclaimed[0].state.value}"
    )

    events = lm.events
    assert events[0].action == "failed"
    print(f"\n  State: {reclaimed[0].state.value}")
    print(f"  Telemetry: action={events[0].action}")

    print("\n  [PASS] No-retry task correctly moved to FAILED")


def test_lease_multiple_tasks() -> None:
    """Multiple tasks with mixed outcomes: some complete, some expire."""
    print("\n" + "=" * 60)
    print("TASK 3d: Lease — Mixed Outcomes (3 Tasks)")
    print("=" * 60)

    test_timeout_ms = 100.0
    lm = LeaseManager(lease_timeout_ms=test_timeout_ms)

    # Task A: will complete normally
    task_a = TaskManifest(task_type="inference_v1", max_retries=2)
    lm.grant_lease(task_a, worker_id=1)
    lm.start_execution(task_a)

    # Task B: will expire and requeue
    task_b = TaskManifest(task_type="inference_v2", max_retries=1)
    lm.grant_lease(task_b, worker_id=2)
    lm.start_execution(task_b)

    # Task C: will expire with no retries → FAILED
    task_c = TaskManifest(task_type="inference_v3", max_retries=0)
    lm.grant_lease(task_c, worker_id=3)
    lm.start_execution(task_c)

    print(f"\n  Active leases: {lm.active_count}")
    assert lm.active_count == 3

    # Complete task A quickly
    time.sleep(0.03)  # 30ms
    lm.complete_lease(task_a)
    print(f"  Task A completed normally")

    # Wait for B and C to expire
    time.sleep((test_timeout_ms + 50) / 1000.0)

    reclaimed = lm.check_leases()
    print(f"  Reclaimed: {len(reclaimed)} tasks")

    assert len(reclaimed) == 2, f"Expected 2 reclaimed, got {len(reclaimed)}"

    states = {t.task_id: t.state for t in reclaimed}
    print(f"\n  Results:")
    print(f"    Task A: {task_a.state.value} (completed)")
    for t in reclaimed:
        print(f"    Task {t.task_id[:8]}: {t.state.value}")

    assert task_a.state == TaskState.COMPLETED
    assert task_b.state == TaskState.QUEUED, f"Task B should be QUEUED, got {task_b.state.value}"
    assert task_c.state == TaskState.FAILED, f"Task C should be FAILED, got {task_c.state.value}"

    assert lm.active_count == 0

    status = lm.status()
    print(f"\n  Lease Manager Status:")
    for k, v in status.items():
        print(f"    {k}: {v}")

    print("\n  [PASS] Mixed outcomes handled correctly")


# ─────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────

def run_all_tests() -> None:
    """Run all local substrate tests."""
    print("\n" + "=" * 60)
    print("EMBER — Final Ticket: Local Execution Substrate")
    print("=" * 60)

    # Task 1: Worker Pool Boot
    test_worker_pool_boot()

    # Task 2: Shared Memory Scale-Up
    test_shared_memory_400mb()

    # Task 3: Task Leasing
    test_lease_normal_completion()
    test_lease_expiry_requeue()
    test_lease_expiry_failed()
    test_lease_multiple_tasks()

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_all_tests()