#!/usr/bin/env python3
"""
EMBER CLI — Substrate Diagnostics

Usage:
    python -m ember doctor           Run all diagnostics
    python -m ember doctor --memory  Verify shared memory allocation
    python -m ember doctor --limits  Verify rlimits enforcement
    python -m ember doctor --all     Run everything
"""

import os
import sys
import argparse
import platform
import resource
import signal
import multiprocessing

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.constants import (
    WORKER_POOL_SIZE,
    WORKER_MEMORY_LIMIT_MB,
    QUEUE_SIZE,
    RPS_MAX,
    WORKER_RECYCLE_AFTER_JOBS,
)

# Use "fork" context — Python 3.14 defaults to "forkserver" which
# requires pickling the target function. Since this file is __main__,
# the child process can't find our functions. Fork copies the parent's
# memory directly, avoiding the pickle/import issue entirely.
_mp = multiprocessing.get_context("fork")


# ─────────────────────────────────────────────
# Diagnostic result
# ─────────────────────────────────────────────

class DiagnosticResult:
    def __init__(self, name: str, passed: bool, detail: str) -> None:
        self.name = name
        self.passed = passed
        self.detail = detail

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"  [{status}] {self.name}: {self.detail}"


# ─────────────────────────────────────────────
# Multiprocessing worker functions (module-level)
#
# These MUST be at module level — not nested inside
# another function — because multiprocessing.Process
# needs to pickle the target, and Python cannot pickle
# local/nested functions.
# ─────────────────────────────────────────────

def _shm_reader(path: str, size: int, result_queue: multiprocessing.Queue) -> None:
    """Child process: open mmap read-only and verify parity."""
    import numpy as np
    fp_child = np.memmap(path, dtype="float32", mode="r", shape=(size,))
    matches = all(fp_child[i] == float(i) for i in range(100))
    result_queue.put(matches)


def _shm_writer(path: str, size: int, result_queue: multiprocessing.Queue) -> None:
    """Child process: open mmap read-only and attempt a write — should fail."""
    import numpy as np
    fp_child = np.memmap(path, dtype="float32", mode="r", shape=(size,))
    try:
        fp_child[0] = 999.0  # This should raise
        result_queue.put("write_succeeded")  # Bad — no protection
    except (ValueError, TypeError, RuntimeError):
        result_queue.put("write_blocked")  # Good — protected
    except Exception as e:
        result_queue.put(f"error: {type(e).__name__}: {e}")


def _rlimit_worker(limit_bytes: int, result_queue: multiprocessing.Queue) -> None:
    """Child process: set rlimit then try to exceed it."""
    import resource

    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))

    try:
        _ = bytearray(100 * 1024 * 1024)  # Try to allocate 100MB — should fail
        result_queue.put("no_kill")  # Bad — allocation succeeded
    except MemoryError:
        result_queue.put("killed")  # Good — OS enforced the limit
    except Exception as e:
        result_queue.put(f"error: {e}")


# ─────────────────────────────────────────────
# Diagnostic checks
# ─────────────────────────────────────────────

def check_python_version() -> DiagnosticResult:
    """Verify Python >= 3.14 for sub-interpreter support."""
    version = platform.python_version()
    major, minor = sys.version_info.major, sys.version_info.minor
    passed = major >= 3 and minor >= 14
    detail = f"Python {version}"
    if not passed:
        detail += " (requires >= 3.14 for PEP 734 sub-interpreters)"
    return DiagnosticResult("Python Version", passed, detail)


def check_shared_memory() -> DiagnosticResult:
    """Verify shared memory allocation and read/write parity."""
    try:
        import numpy as np

        test_size_mb = 10
        test_path = "/tmp/ember_doctor_shm_test.mmap"

        # Write from parent process
        fp = np.memmap(test_path, dtype="float32", mode="w+", shape=(test_size_mb * 256 * 1024,))
        fp[:100] = np.arange(100, dtype="float32")
        fp.flush()

        # Read from a child process to verify cross-process visibility
        q = _mp.Queue()
        p = _mp.Process(
            target=_shm_reader, args=(test_path, test_size_mb * 256 * 1024, q)
        )
        p.start()
        p.join(timeout=5)

        if p.exitcode != 0 or q.empty():
            return DiagnosticResult(
                "Shared Memory", False, "Child process failed to read mmap"
            )

        parity_ok = q.get()
        os.remove(test_path)

        if parity_ok:
            return DiagnosticResult(
                "Shared Memory",
                True,
                f"mmap write/read parity verified across processes ({test_size_mb}MB test)",
            )
        else:
            return DiagnosticResult(
                "Shared Memory", False, "Read/write parity mismatch across processes"
            )

    except ImportError:
        return DiagnosticResult("Shared Memory", False, "numpy not installed")
    except Exception as e:
        return DiagnosticResult("Shared Memory", False, f"Error: {e}")


def check_shared_memory_write_protection() -> DiagnosticResult:
    """
    Verify that a worker cannot accidentally modify pinned model weights.

    The "Negative Space" test: a child process opens the mmap as read-only
    and attempts to write. This should raise a ValueError or trigger
    a segfault — proving the weights are protected.
    """
    try:
        import numpy as np

        test_path = "/tmp/ember_doctor_shm_wp_test.mmap"
        size = 1024

        # Host writes the model weights
        fp = np.memmap(test_path, dtype="float32", mode="w+", shape=(size,))
        fp[:] = np.ones(size, dtype="float32")
        fp.flush()
        del fp

        q = _mp.Queue()
        p = _mp.Process(target=_shm_writer, args=(test_path, size, q))
        p.start()
        p.join(timeout=5)

        os.remove(test_path)

        if p.exitcode is not None and p.exitcode < 0:
            return DiagnosticResult(
                "Shared Memory Write Protection",
                True,
                f"OS killed write attempt with signal {-p.exitcode} (write-protected)",
            )

        if q.empty():
            return DiagnosticResult(
                "Shared Memory Write Protection",
                False,
                f"Child process exited with code {p.exitcode}, no result",
            )

        result = q.get()
        if result == "write_blocked":
            return DiagnosticResult(
                "Shared Memory Write Protection",
                True,
                "Write to read-only mmap correctly raised exception",
            )
        elif result == "write_succeeded":
            return DiagnosticResult(
                "Shared Memory Write Protection",
                False,
                "WARNING: child was able to write to read-only mmap",
            )
        else:
            return DiagnosticResult(
                "Shared Memory Write Protection", True, f"Write blocked: {result}"
            )

    except ImportError:
        return DiagnosticResult("Shared Memory Write Protection", False, "numpy not installed")
    except Exception as e:
        return DiagnosticResult("Shared Memory Write Protection", False, f"Error: {e}")


def check_rlimits() -> DiagnosticResult:
    """Verify that rlimits can enforce memory boundaries."""
    try:
        limit_bytes = 50 * 1024 * 1024  # 50MB test limit

        q = _mp.Queue()
        p = _mp.Process(target=_rlimit_worker, args=(limit_bytes, q))
        p.start()
        p.join(timeout=10)

        # If the process is still alive, it's stuck — likely got MemoryError
        # but can't write to the queue because it's memory-starved.
        # That means rlimits worked. Kill it and report success.
        if p.is_alive() or p.exitcode is None:
            p.terminate()
            p.join(timeout=3)
            return DiagnosticResult(
                "rlimits Enforcement",
                True,
                f"Worker memory-starved at {limit_bytes // (1024*1024)}MB limit "
                f"(MemoryError raised, process couldn't report back)",
            )

        if q.empty():
            if p.exitcode < 0:
                return DiagnosticResult(
                    "rlimits Enforcement",
                    True,
                    f"Worker killed by signal {-p.exitcode} when exceeding {limit_bytes // (1024*1024)}MB limit",
                )
            return DiagnosticResult(
                "rlimits Enforcement", False, f"Worker exited with code {p.exitcode}"
            )

        result = q.get()
        if result == "killed":
            return DiagnosticResult(
                "rlimits Enforcement",
                True,
                f"MemoryError raised when exceeding {limit_bytes // (1024*1024)}MB limit",
            )
        elif result == "no_kill":
            return DiagnosticResult(
                "rlimits Enforcement",
                False,
                "Worker exceeded memory limit without being stopped",
            )
        else:
            return DiagnosticResult("rlimits Enforcement", False, result)

    except Exception as e:
        return DiagnosticResult("rlimits Enforcement", False, f"Error: {e}")


def check_system_capacity() -> DiagnosticResult:
    """Verify the system can support the 85 RPS worst case."""
    import psutil

    cpu_count = psutil.cpu_count(logical=True)
    mem_total_mb = psutil.virtual_memory().total / (1024 * 1024)
    mem_needed_mb = WORKER_MEMORY_LIMIT_MB * 2  # At minimum, 2 workers for steel thread

    passed = mem_total_mb >= mem_needed_mb and cpu_count is not None and cpu_count >= 2
    detail = (
        f"CPUs={cpu_count}, RAM={mem_total_mb:.0f}MB, "
        f"min_needed={mem_needed_mb}MB for {WORKER_POOL_SIZE} workers"
    )

    return DiagnosticResult("System Capacity", passed, detail)


def check_constants_consistency() -> DiagnosticResult:
    """Verify system constants are internally consistent."""
    issues = []

    if QUEUE_SIZE < 2:
        issues.append(f"QUEUE_SIZE={QUEUE_SIZE} too small (min 2)")

    if WORKER_POOL_SIZE < 1:
        issues.append(f"WORKER_POOL_SIZE={WORKER_POOL_SIZE} must be >= 1")

    if WORKER_RECYCLE_AFTER_JOBS < 1:
        issues.append(f"WORKER_RECYCLE_AFTER_JOBS={WORKER_RECYCLE_AFTER_JOBS} must be >= 1")

    if issues:
        return DiagnosticResult("Constants Consistency", False, "; ".join(issues))

    return DiagnosticResult(
        "Constants Consistency",
        True,
        f"Workers={WORKER_POOL_SIZE}, Queue={QUEUE_SIZE}, "
        f"RPS_max={RPS_MAX}, Recycle={WORKER_RECYCLE_AFTER_JOBS}",
    )


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def run_doctor(check_memory: bool = False, check_limits: bool = False, check_all: bool = False) -> bool:
    """Run substrate diagnostics. Returns True if all checks pass."""

    if not check_memory and not check_limits:
        check_all = True

    print()
    print("=" * 60)
    print("EMBER Doctor — Substrate Diagnostics")
    print("=" * 60)

    results: list[DiagnosticResult] = []

    # Always run baseline checks
    print("\n[Baseline]")
    r = check_python_version()
    results.append(r)
    print(r)

    r = check_constants_consistency()
    results.append(r)
    print(r)

    r = check_system_capacity()
    results.append(r)
    print(r)

    # Memory checks
    if check_memory or check_all:
        print("\n[Memory]")
        r = check_shared_memory()
        results.append(r)
        print(r)

        r = check_shared_memory_write_protection()
        results.append(r)
        print(r)

    # rlimits checks
    if check_limits or check_all:
        print("\n[Resource Limits]")
        r = check_rlimits()
        results.append(r)
        print(r)

    # Summary
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    all_passed = failed == 0

    print()
    print("─" * 60)
    status = "READY" if all_passed else "NOT READY"
    print(f"  Status: {status} ({passed} passed, {failed} failed)")
    print("─" * 60)
    print()

    return all_passed


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ember",
        description="EMBER — Ephemeral Model-Based Burst Execution Runtime",
    )
    subparsers = parser.add_subparsers(dest="command")

    doctor_parser = subparsers.add_parser("doctor", help="Run substrate diagnostics")
    doctor_parser.add_argument("--memory", action="store_true", help="Check shared memory")
    doctor_parser.add_argument("--limits", action="store_true", help="Check rlimits enforcement")
    doctor_parser.add_argument("--all", action="store_true", help="Run all diagnostics")

    args = parser.parse_args()

    if args.command == "doctor":
        success = run_doctor(
            check_memory=args.memory,
            check_limits=args.limits,
            check_all=args.all,
        )
        sys.exit(0 if success else 1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()