#!/usr/bin/env python3
"""
Generate synthetic oplog test data for polarwarp overlap tests.

Creates 6 files in ./test-data/:
  sequential-A.csv / sequential-B.csv   — no overlap (file B starts after A ends)
  partial-A.csv    / partial-B.csv      — ~50% overlap
  concurrent-A.csv / concurrent-B.csv  — ~99% overlap (fully concurrent)

Each file has 2000 operations (GET + PUT) spread across the time window.
"""

import csv
import datetime
import math
import os
import random

random.seed(42)
os.makedirs("test-data", exist_ok=True)

EPOCH = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
BYTES_SIZES = [0, 4096, 65536, 1048576, 16777216]   # zero, 4K, 64K, 1M, 16M
OPS         = ["GET", "GET", "GET", "PUT", "LIST"]   # weighted toward GET
ENDPOINTS   = ["http://node1:9000", "http://node2:9000"]
THREADS     = list(range(1, 9))
N_OPS       = 2000


def ts(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f000Z")


def gen_file(path: str, window_start_s: float, window_end_s: float) -> None:
    """Generate an oplog TSV file with N_OPS operations spread over [window_start_s, window_end_s]."""
    span = window_end_s - window_start_s
    rows = []
    for idx in range(N_OPS):
        op        = random.choice(OPS)
        thread    = random.choice(THREADS)
        client_id = f"client{random.randint(1, 2)}"
        nbytes    = 0 if op == "LIST" else random.choice(BYTES_SIZES[1:])
        endpoint  = random.choice(ENDPOINTS)
        # Spread start times uniformly across the window
        op_start_s = window_start_s + (idx / N_OPS) * span + random.uniform(0, span / N_OPS)
        # Latency: log-normal around 5ms for small ops, 50ms for large
        base_lat_s = 0.005 if nbytes < 1_000_000 else 0.050
        lat_s = max(0.0001, random.lognormvariate(math.log(base_lat_s), 0.5))
        op_end_s   = op_start_s + lat_s
        first_byte_s = op_start_s + lat_s * 0.1
        dur_ns     = int(lat_s * 1e9)

        start_dt = EPOCH + datetime.timedelta(seconds=op_start_s)
        end_dt   = EPOCH + datetime.timedelta(seconds=op_end_s)
        fb_dt    = EPOCH + datetime.timedelta(seconds=first_byte_s)

        rows.append([
            idx, thread, op, client_id, 1, nbytes,
            endpoint, f"obj-{idx:06d}", "",
            ts(start_dt), ts(fb_dt), ts(end_dt), dur_ns,
        ])

    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["idx", "thread", "op", "client_id", "n_objects", "bytes",
                    "endpoint", "file", "error", "start", "first_byte", "end", "duration_ns"])
        w.writerows(rows)
    print(f"Wrote {len(rows)} ops  →  {path}  (window {window_start_s:.0f}s – {window_end_s:.0f}s)")


# ── Scenario 1: Sequential ────────────────────────────────────────────────────
# File A: t=0..60s, File B: t=65..125s  → gap of 5s → 0% overlap
gen_file("test-data/sequential-A.csv", 0,   60)
gen_file("test-data/sequential-B.csv", 65, 125)

# ── Scenario 2: Partial overlap (~50%) ───────────────────────────────────────
# File A: t=0..60s, File B: t=30..90s
# Overlap = 30s, Union = 90s → Jaccard = 33%   (within 3–97% zone → warning)
gen_file("test-data/partial-A.csv",  0,  60)
gen_file("test-data/partial-B.csv", 30,  90)

# ── Scenario 3: Concurrent (~99% overlap) ────────────────────────────────────
# File A: t=0..60s, File B: t=0.5..60.5s
# Overlap = 59.5s, Union = 60.5s → Jaccard = 98.3%  → concurrent, no warning
gen_file("test-data/concurrent-A.csv",  0,   60)
gen_file("test-data/concurrent-B.csv",  0.5, 60.5)

print("\nDone.  Test files are in ./test-data/")
