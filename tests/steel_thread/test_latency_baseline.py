"""
Steel Thread — Ticket 1: Baseline Network Latency Test

Establishes a gRPC handshake between Columbus (Edge) and Chicago (Regional),
fires 1,000 mock RouteRequests, records latency for each, computes the
distribution (min, p50, p95, p99, max), and generates a histogram saved
to docs/latency_baseline.png.

Usage:
    1. In one terminal:   python regional/routing_server.py
    2. In another terminal: python tests/steel_thread/test_latency_baseline.py

Can also be run via pytest:
    pytest tests/steel_thread/test_latency_baseline.py -v
"""

import os
import sys
import time
import statistics

import numpy as np
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend — no display needed
import matplotlib.pyplot as plt

# Add project root to path so imports work from tests/ directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from edge.routing_client import RoutingClient
from core.constants import ROUTING_TIMEOUT_MS

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
TOTAL_REQUESTS = 1000
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../../docs")
GRAPH_PATH = os.path.join(OUTPUT_DIR, "latency_baseline.png")
REPORT_PATH = os.path.join(OUTPUT_DIR, "latency_baseline_report.txt")


def percentile(data: list[float], p: float) -> float:
    """Compute the p-th percentile of a list of values."""
    return float(np.percentile(data, p))


def run_baseline_test() -> dict:
    """
    Execute 1,000 RouteRequests and collect latency measurements.

    Returns a dict with latency stats and raw data.
    """
    latencies: list[float] = []
    successes = 0
    failures = 0

    print(f"\n{'='*60}")
    print("EMBER — Ticket 1: Baseline Network Latency Test")
    print(f"{'='*60}")
    print(f"Target: localhost:50051 (Chicago Regional Controller)")
    print(f"Requests: {TOTAL_REQUESTS}")
    print(f"Routing timeout budget: {ROUTING_TIMEOUT_MS}ms")
    print(f"{'='*60}\n")

    # ── Handshake: single request to verify connectivity ──
    print("[1/4] Establishing gRPC handshake...")
    with RoutingClient() as client:
        response, rtt = client.request_route(task_type="handshake")
        if response is None:
            print("  FAILED — Cannot reach Chicago Regional Controller.")
            print("  Is the server running? (python regional/routing_server.py)")
            sys.exit(1)
        print(f"  SUCCESS — Handshake completed in {rtt:.2f}ms")
        print(f"  Response: decision={response.decision}, reason='{response.reason}'")

    # ── Fire 1,000 requests ──
    print(f"\n[2/4] Sending {TOTAL_REQUESTS} RouteRequests...")
    with RoutingClient() as client:
        for i in range(TOTAL_REQUESTS):
            response, rtt = client.request_route(task_type="ping")

            if response is not None:
                latencies.append(rtt)
                successes += 1
            else:
                failures += 1

            # Progress indicator every 250 requests
            if (i + 1) % 250 == 0:
                print(f"  Sent {i + 1}/{TOTAL_REQUESTS}...")

    # ── Compute statistics ──
    print(f"\n[3/4] Computing latency distribution...")
    stats = {
        "total": TOTAL_REQUESTS,
        "successes": successes,
        "failures": failures,
        "min_ms": min(latencies),
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "max_ms": max(latencies),
        "mean_ms": statistics.mean(latencies),
        "stdev_ms": statistics.stdev(latencies) if len(latencies) > 1 else 0.0,
        "within_budget": percentile(latencies, 99) <= ROUTING_TIMEOUT_MS,
        "latencies": latencies,
    }

    # ── Print results ──
    print(f"\n{'─'*60}")
    print("RESULTS")
    print(f"{'─'*60}")
    print(f"  Successful:  {stats['successes']}/{stats['total']}")
    print(f"  Failed:      {stats['failures']}/{stats['total']}")
    print(f"{'─'*60}")
    print(f"  Min:         {stats['min_ms']:.3f} ms")
    print(f"  p50:         {stats['p50_ms']:.3f} ms")
    print(f"  p95:         {stats['p95_ms']:.3f} ms")
    print(f"  p99:         {stats['p99_ms']:.3f} ms")
    print(f"  Max:         {stats['max_ms']:.3f} ms")
    print(f"  Mean:        {stats['mean_ms']:.3f} ms")
    print(f"  Stdev:       {stats['stdev_ms']:.3f} ms")
    print(f"{'─'*60}")

    budget_status = "PASS" if stats["within_budget"] else "FAIL"
    print(f"  20ms Budget: {budget_status} (p99 = {stats['p99_ms']:.3f}ms vs {ROUTING_TIMEOUT_MS}ms)")
    print(f"{'─'*60}\n")

    return stats


def generate_graph(stats: dict) -> None:
    """Generate a latency distribution histogram and save to docs/."""
    print("[4/4] Generating latency distribution graph...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    latencies = stats["latencies"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("EMBER — Ticket 1: Baseline gRPC Latency (1,000 Requests)", fontsize=13)

    # ── Left: Histogram ──
    ax1.hist(latencies, bins=50, color="#2563eb", edgecolor="#1e40af", alpha=0.85)
    ax1.axvline(stats["p50_ms"], color="#16a34a", linestyle="--", linewidth=1.5, label=f"p50 = {stats['p50_ms']:.2f}ms")
    ax1.axvline(stats["p99_ms"], color="#dc2626", linestyle="--", linewidth=1.5, label=f"p99 = {stats['p99_ms']:.2f}ms")
    ax1.axvline(ROUTING_TIMEOUT_MS, color="#f59e0b", linestyle="-", linewidth=2, label=f"Budget = {ROUTING_TIMEOUT_MS}ms")
    ax1.set_xlabel("Round-Trip Time (ms)")
    ax1.set_ylabel("Frequency")
    ax1.set_title("Latency Distribution")
    ax1.legend(fontsize=9)

    # ── Right: Time series (latency per request) ──
    ax2.scatter(range(len(latencies)), latencies, s=3, alpha=0.5, color="#2563eb")
    ax2.axhline(ROUTING_TIMEOUT_MS, color="#f59e0b", linestyle="-", linewidth=2, label=f"Budget = {ROUTING_TIMEOUT_MS}ms")
    ax2.axhline(stats["p99_ms"], color="#dc2626", linestyle="--", linewidth=1.5, label=f"p99 = {stats['p99_ms']:.2f}ms")
    ax2.set_xlabel("Request Number")
    ax2.set_ylabel("Round-Trip Time (ms)")
    ax2.set_title("Latency Over Time")
    ax2.legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(GRAPH_PATH, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"  Graph saved to {os.path.abspath(GRAPH_PATH)}")


def save_report(stats: dict) -> None:
    """Save a plain-text report to docs/ for the PR."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    lines = [
        "EMBER — Ticket 1: Baseline Latency Report",
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Total Requests:  {stats['total']}",
        f"Successes:       {stats['successes']}",
        f"Failures:        {stats['failures']}",
        "",
        f"Min:    {stats['min_ms']:.3f} ms",
        f"p50:    {stats['p50_ms']:.3f} ms",
        f"p95:    {stats['p95_ms']:.3f} ms",
        f"p99:    {stats['p99_ms']:.3f} ms",
        f"Max:    {stats['max_ms']:.3f} ms",
        f"Mean:   {stats['mean_ms']:.3f} ms",
        f"Stdev:  {stats['stdev_ms']:.3f} ms",
        "",
        f"20ms Budget: {'PASS' if stats['within_budget'] else 'FAIL'}",
        f"  p99 ({stats['p99_ms']:.3f}ms) {'<=' if stats['within_budget'] else '>'} {ROUTING_TIMEOUT_MS}ms",
    ]

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"  Report saved to {os.path.abspath(REPORT_PATH)}")


# ─────────────────────────────────────────────
# Pytest entry point
# ─────────────────────────────────────────────

def test_baseline_latency_within_budget() -> None:
    """
    Pytest-compatible test that validates the 20ms routing budget.

    Run with: pytest tests/steel_thread/test_latency_baseline.py -v
    """
    stats = run_baseline_test()
    generate_graph(stats)
    save_report(stats)

    assert stats["failures"] == 0, f"{stats['failures']} requests failed"
    assert stats["within_budget"], (
        f"p99 latency ({stats['p99_ms']:.3f}ms) exceeds "
        f"the {ROUTING_TIMEOUT_MS}ms routing budget"
    )


# ─────────────────────────────────────────────
# Standalone entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    stats = run_baseline_test()
    generate_graph(stats)
    save_report(stats)

    if not stats["within_budget"]:
        print("WARNING: p99 exceeds 20ms budget. Flag for architectural review.")
        sys.exit(1)