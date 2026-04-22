import asyncio
import numpy as np
from edge.routing_client import run_ping_batch


if __name__ == "__main__":
    print("[Columbus Edge] Starting EMBER routing client...")
    latencies = asyncio.run(run_ping_batch())
    arr = np.array(latencies)
    print(f"\n  p50:  {np.percentile(arr, 50):.3f} ms")
    print(f"  p99:  {np.percentile(arr, 99):.3f} ms")
    print(f"  mean: {np.mean(arr):.3f} ms")
