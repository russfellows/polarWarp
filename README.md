# PolarWarp

[![Version](https://img.shields.io/badge/version-0.1.2-brightgreen.svg)](https://github.com/russfellows/polarWarp/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](python/)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](rust/)

High-performance tool for analyzing storage I/O operation logs (oplog files from sai3-bench, MinIO Warp, etc.).

## Features

- **Multi-format support**: TSV and CSV files, with automatic zstd decompression and separator detection
- **Size-bucketed analysis**: 9 size buckets (zero, 1B-8KiB, ... >2GiB)
- **Summary rows**: Aggregate statistics for META (LIST/HEAD/DELETE/STAT), GET, and PUT operations
- **Latency percentiles**: mean, median, p90, p95, p99, max (statistically valid)
- **Throughput metrics**: ops/sec and MiB/sec per bucket
- **Multi-file consolidation**: Combine results from multiple agents
- **Time skip**: Exclude warmup periods with `--skip` option

## Implementations

PolarWarp is available in two implementations with identical functionality and output format:

| Implementation | Speed | Best For |
|----------------|-------|----------|
| [**Rust**](rust/) | ~870K records/sec | Production use, large files, compiled binary |
| [**Python**](python/) | ~850K records/sec | Quick analysis, scripting, no compilation |

### Performance Comparison

Benchmark results processing a 1.16M operation mixed workload log (zstd compressed):

| Tool | Time | Speedup |
|------|------|--------|
| **PolarWarp (Rust)** | 1.34s | **8.4x faster** |
| **PolarWarp (Python)** | 1.36s | **8.2x faster** |
| MinIO `warp analyze` | 11.19s | baseline |

Both PolarWarp implementations provide **8x faster** analysis than MinIO's native `warp analyze` tool, while also providing more detailed per-bucket latency breakdowns.

### Multi-File Consolidation Performance

When analyzing multiple files (2 × 1.16M operations = 2.32M total), PolarWarp handles consolidation in a single command, while MinIO warp requires separate merge and analyze steps:

| Tool | Wall Time | Peak Memory | Notes |
|------|-----------|-------------|-------|
| **PolarWarp (Python)** | 2.62s | 1.83 GB | Single command |
| **PolarWarp (Rust)** | 3.03s | 1.20 GB | Single command |
| MinIO `warp merge` | 13.18s | 1.77 GB | Step 1 of 2 |
| MinIO `warp analyze` | 22.03s | 5.21 GB | Step 2 of 2 |
| **warp total** | **35.21s** | **5.21 GB** | Two commands required |

**Summary**: PolarWarp is **12x faster** and uses **3-4x less memory** than warp for multi-file analysis.

### Resource Scaling Analysis

Measured scaling factors (1 file → 2 files, each 1.16M operations):

| Tool | Time Scaling | Memory Scaling | Memory per Op |
|------|-------------|----------------|---------------|
| **PolarWarp (Rust)** | 2.2x (linear) | 1.97x (linear) | 0.52 KB/op |
| **PolarWarp (Python)** | 1.6x (sub-linear) | 1.54x (sub-linear) | 0.79 KB/op |
| MinIO warp | 2.0x (linear) | 2.28x (**super-linear**) | 2.24 KB/op |

### Projected Resource Usage at Scale

**Moderate scale: 2 × 15M operations (30M total)**

| Tool | Projected Time | Projected Memory |
|------|---------------|------------------|
| **PolarWarp (Rust)** | ~40s | ~16 GB |
| **PolarWarp (Python)** | ~35s | ~18 GB |
| MinIO warp (merge+analyze) | ~7.5 min | ~67 GB |

**Large scale: 8 × 15M operations (120M total)**

| Tool | Projected Time | Projected Memory | Feasibility |
|------|---------------|------------------|-------------|
| **PolarWarp (Rust)** | ~2.5 min | ~62 GB | ✅ Fits in 64 GB workstation |
| **PolarWarp (Python)** | ~2.3 min | ~72 GB | ⚠️ Needs 128 GB or swap |
| MinIO warp | ~30 min | **~270 GB** | ❌ Impractical |

*Projections based on measured scaling factors. warp's super-linear memory growth (2.28x per 2x data) makes it impractical for large-scale analysis, while PolarWarp's linear scaling remains manageable.*

### Quick Start - Rust

```bash
cd rust
cargo build --release
./target/release/polarwarp-rs oplog.tsv.zst
```

### Quick Start - Python

```bash
cd python
uv run ./polarwarp.py oplog.csv.zst
```

## Output Format

Both implementations produce identical output:

```
      op bytes_bucket bucket_# mean_lat_us med._lat_us 90%_lat_us 95%_lat_us 99%_lat_us max_lat_us avg_obj_KB ops_/_sec xput_MBps     count
    LIST         zero        0      533.98      533.98     533.98     533.98     533.98     533.98       0.00      0.20      0.00         1
     GET      1B-8KiB        1       76.18       71.97     114.27     128.50     160.82   1,173.53       4.00 47,394.46    185.13   236,971
```

## Size Buckets

Both implementations use identical bucket definitions (matching sai3-bench):

| Bucket # | Label | Size Range |
|----------|-------|------------|
| 0 | zero | 0 bytes (metadata ops) |
| 1 | 1B-8KiB | 1 B to 8 KiB |
| 2 | 8KiB-64KiB | 8 KiB to 64 KiB |
| 3 | 64KiB-512KiB | 64 KiB to 512 KiB |
| 4 | 512KiB-4MiB | 512 KiB to 4 MiB |
| 5 | 4MiB-32MiB | 4 MiB to 32 MiB |
| 6 | 32MiB-256MiB | 32 MiB to 256 MiB |
| 7 | 256MiB-2GiB | 256 MiB to 2 GiB |
| 8 | >2GiB | Greater than 2 GiB |

## Input File Format

Expected columns (sai3-bench oplog format):

```
idx  thread  op  client_id  n_objects  bytes  endpoint  file  error  start  first_byte  end  duration_ns
```

Also supports MinIO Warp CSV output format.

## Related Projects

- **[sai3-bench](https://github.com/russfellows/sai3-bench)** - Multi-protocol I/O benchmarking suite
- **[MinIO Warp](https://github.com/minio/warp)** - S3 benchmarking tool

## License

Licensed under the Apache License, Version 2.0
