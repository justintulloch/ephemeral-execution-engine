"""
EMBER System Constants

All architectural constraints and SLO targets
"""

# ──────────────────────────────────────────────
# Latency SLO (milliseconds)
# ──────────────────────────────────────────────
LOCAL_P50_MS = 80
LOCAL_P99_MS = 150
SPLIT_P50_MS = 220
SPLIT_P99_MS = 500
ROUTING_TIMEOUT_MS = 20

# ──────────────────────────────────────────────
# Worker Pool
# ──────────────────────────────────────────────
WORKER_POOL_SIZE = 43
WORKER_RECYCLE_AFTER_JOBS = 100
HEARTBEAT_INTERVAL_S = 5

# ──────────────────────────────────────────────
# Concurrency & Queue
# ──────────────────────────────────────────────
QUEUE_SIZE = 22
MAX_CONCURRENCY_PRACTICAL = 15
RPS_MAX = 85
T_AVG_MS = 175
SAFETY_COEFFICIENT = 0.85

# ──────────────────────────────────────────────
# Circuit Breaker
# ──────────────────────────────────────────────
CB_FAILURE_THRESHOLD = 3
CB_OPEN_DURATION_S = 30
CB_EXTENDED_OPEN_DURATION_S = 60

# ──────────────────────────────────────────────
# Energy SLO (Joules)
# ──────────────────────────────────────────────
ENERGY_P50_J = 2.8
ENERGY_P95_J = 4.0
ENERGY_P99_J = 5.5

# ──────────────────────────────────────────────
# Thermal Tiers (°C)
# ──────────────────────────────────────────────
THERMAL_GREEN_MAX = 65
THERMAL_YELLOW_MAX = 75
THERMAL_ORANGE_MAX = 82
# Red: >= THERMAL_ORANGE_MAX

# ──────────────────────────────────────────────
# Memory
# ──────────────────────────────────────────────
WORKER_MEMORY_LIMIT_MB = 400
HYSTERESIS_HIGH_PCT = 0.90
HYSTERESIS_LOW_PCT = 0.75

# ──────────────────────────────────────────────
# Queue
# ──────────────────────────────────────────────
QUEUE_TTL_MS = 100  # Dead letter: evict tasks waiting longer than 100ms

# ──────────────────────────────────────────────
# Task Contract
# ──────────────────────────────────────────────
DEFAULT_MAX_RETRIES = 2
