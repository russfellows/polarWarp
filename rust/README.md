# PolarWarp - Rust Implementation

[![Version](https://img.shields.io/badge/version-0.1.1-blue.svg)](Cargo.toml)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](../LICENSE)

A high-performance Rust implementation of PolarWarp for analyzing storage I/O operation logs.

## Overview

PolarWarp-rs processes oplog files (TSV/CSV format, optionally zstd compressed) and computes detailed performance metrics including latency percentiles, throughput, and ops/sec—all grouped by operation type and object size buckets.

Built with [Polars](https://pola.rs/) for blazing-fast DataFrame operations, polarwarp-rs can process **~830,000 records per second** in release mode.

## Features

- **Multi-format support**: TSV and CSV files, with automatic zstd decompression and separator detection
- **Size-bucketed analysis**: 9 size buckets matching sai3-bench (zero, 1B-8KiB, 8KiB-64KiB, ... >2GiB)
- **Summary rows**: Aggregate statistics for META (LIST/HEAD/DELETE/STAT), GET, and PUT operations
- **Per-client statistics**: Compare performance across multiple clients with `--per-client` option
- **Latency percentiles**: mean, median, p90, p95, p99, max (statistically valid, not averaged)
- **Throughput metrics**: ops/sec and MiB/sec per bucket
- **Multi-file consolidation**: Combine results from multiple agents/files
- **Time skip**: Exclude warmup periods with `--skip` option
- **Fast**: ~1.3s to process 1.16M records (release build)

## Installation

### From Source

```bash
# Build the release version (recommended)
cargo build --release

# The binary will be available at target/release/polarwarp-rs
```

## Usage

```bash
# Display help
polarwarp-rs --help

# Process a single file
polarwarp-rs oplog.tsv.zst

# Process multiple files (results are consolidated)
polarwarp-rs agent-1-oplog.tsv.zst agent-2-oplog.tsv.zst

# Skip first 2 minutes of warmup
polarwarp-rs --skip 2m oplog.tsv.zst

# Compare performance across multiple clients
polarwarp-rs --per-client multi_client_oplog.csv.zst

# Show basic file info only
polarwarp-rs --basic-stats oplog.tsv.zst
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `<FILES>...` | Input files to process (TSV/CSV, optionally zstd compressed) |
| `-s, --skip <TIME>` | Skip warmup time from start (e.g., "90s", "5m") |
| `--per-client` | Generate per-client statistics (in addition to overall stats) |
| `--basic-stats` | Show basic file info without full processing |
| `-h, --help` | Display help information |
| `-V, --version` | Display version information |

## Output Format

```
      op bytes_bucket bucket_# mean_lat_us med._lat_us 90%_lat_us 95%_lat_us 99%_lat_us max_lat_us avg_obj_KB ops_/_sec xput_MBps     count
    LIST         zero        0      533.98      533.98     533.98     533.98     533.98     533.98       0.00      0.20      0.00         1
     GET      1B-8KiB        1       76.18       71.97     114.27     128.50     160.82   1,173.53       4.00 47,394.46    185.13   236,971
```

### Size Buckets

Matching sai3-bench bucket definitions:

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

## Performance

### Speed Benchmarks

| Build | Time per 1.16M records | Records/sec |
|-------|----------------------|-------------|
| Debug | ~5-6s | ~200,000 |
| Release | ~1.11s | ~1,075,000 |

The release build is approximately **5x faster** than debug, thanks to:
- Link-Time Optimization (LTO)
- Single codegen unit
- Maximum optimization level (opt-level = 3)

### Resource Usage (2.32M records, 2 files consolidated)

| Metric | Value |
|--------|-------|
| **Wall clock time** | 2.36 seconds |
| **CPU utilization** | ~280% (~3 cores) |
| **Records/sec** | ~1,075,000 |
| **Peak memory (RSS)** | ~1,200 MB |
| **Page swaps** | 0 |
| **Major page faults** | 0 |

Zero page swaps and zero major page faults means all data is processed entirely in RAM with no disk paging—even when consolidating multiple large files.

## Oplog File Format

Expected TSV columns (matching sai3-bench oplog format):

```
idx  thread  op  client_id  n_objects  bytes  endpoint  file  error  start  first_byte  end  duration_ns
```

## Dependencies

- [Polars](https://pola.rs/) - Fast DataFrame library
- [Clap](https://clap.rs/) - Command-line argument parsing
- [Chrono](https://docs.rs/chrono/) - Date/time handling
- [zstd](https://docs.rs/zstd/) - Zstandard compression

## Related Projects

- **sai3-bench** - Multi-protocol I/O benchmarking suite
- **polarWarp** (Python) - Original Python implementation

## Future Enhancements

- Export to TSV/CSV/JSON formats
- Parallel file processing with Rayon
- Time-window analysis for detecting performance changes
- Comparative analysis between test runs

## License

Licensed under the Apache License, Version 2.0 