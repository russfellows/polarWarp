# PolarWarp

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
