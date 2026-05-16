# PolarWarp - Rust Implementation

[![Version](https://img.shields.io/badge/version-0.2.1-blue.svg)](Cargo.toml)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](../LICENSE)
[![Tests](https://img.shields.io/badge/tests-28%20passing-brightgreen.svg)](src/main.rs)

A high-performance Rust implementation of PolarWarp for analyzing storage I/O operation logs.

## Overview

PolarWarp-rs processes oplog files (TSV/CSV format, optionally zstd compressed) and computes detailed performance metrics including latency percentiles, throughput, and ops/sec—all grouped by operation type and object size buckets.

Built with [Polars](https://pola.rs/) for blazing-fast DataFrame operations, polarwarp-rs can process **~830,000 records per second** in release mode.

## Features

- **Multi-format support**: TSV and CSV files, with automatic zstd decompression and separator detection
- **Dual file type support**: Per-operation trace files (`.trace.tsv.zst`) *and* aggregated summary files (`.summary.tsv.zst`) — type auto-detected from header columns
- **Size-bucketed analysis**: 9 size buckets matching sai3-bench (zero, 1B-8KiB, 8KiB-64KiB, ... >2GiB)
- **Summary rows**: Aggregate statistics for META (LIST/HEAD/DELETE/STAT), GET, and PUT operations
- **Per-client statistics**: Compare performance across multiple clients with `--per-client` option
- **Per-endpoint statistics**: Compare performance across storage endpoints with `--per-endpoint` option
- **Excel export**: Export results to a formatted `.xlsx` workbook with `--excel`
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

# Process a single trace file
polarwarp-rs oplog.trace.tsv.zst

# Process multiple files (results are consolidated)
polarwarp-rs agent-1.trace.tsv.zst agent-2.trace.tsv.zst

# Process a summary file (type is auto-detected)
polarwarp-rs run.summary.tsv.zst

# Skip first 2 minutes of warmup
polarwarp-rs --skip 2m oplog.trace.tsv.zst

# Compare performance across multiple clients
polarwarp-rs --per-client multi_client_oplog.trace.tsv.zst

# Trace file only → Results + Detail tabs (latency percentiles, throughput, op counts)
polarwarp-rs --excel=report.xlsx run.trace.tsv.zst

# Summary file only → Summary tab (raw per-second rows) + Charts tab (XY scatter)
polarwarp-rs --excel=report.xlsx run.summary.tsv.zst

# Both files together → all four tabs in one workbook (recommended)
polarwarp-rs --excel=report.xlsx run.trace.tsv.zst run.summary.tsv.zst

# Per-endpoint breakdown
polarwarp-rs --per-endpoint oplog.trace.tsv.zst

# Show basic file info only
polarwarp-rs --basic-stats oplog.trace.tsv.zst
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `<FILES>...` | Input files to process (TSV/CSV, optionally zstd compressed) |
| `-s, --skip <TIME>` | Skip warmup time from start (e.g., "90s", "5m") |
| `--per-client` | Generate per-client statistics (in addition to overall stats) |
| `--per-endpoint` | Generate per-endpoint statistics (in addition to overall stats) |
| `--excel [=FILE]` | Export results to an Excel `.xlsx` workbook |
| `--basic-stats` | Show basic file info without full processing |
| `-h, --help` | Display help information |
| `-V, --version` | Display version information |

## Output Format

```
      op bytes_bucket bucket_# mean_lat_us med._lat_us 90%_lat_us 95%_lat_us 99%_lat_us max_lat_us avg_obj_KB ops_/_sec xput_MBps     count max_threads runtime_s
    LIST         zero        0      533.98      533.98     533.98     533.98     533.98     533.98       0.00      0.20      0.00         1           1      5.00
     GET      1B-8KiB        1       76.18       71.97     114.27     128.50     160.82   1,173.53       4.00 47,394.46    185.13   236,971           8      5.00
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

## Testing

The Rust implementation has 28 unit tests embedded in `src/main.rs` (in the `#[cfg(test)] mod tests` block).

```bash
# Run all tests
cargo test

# Run tests with output visible
cargo test -- --nocapture

# Run a specific test by name
cargo test test_size_bucket
```

Tests cover: `parse_skip_time`, `format_with_commas`, `format_int_with_commas`,
`format_duration_ns`, `derive_short_name`, `derive_excel_path`, `make_tab_name`,
`FileType` equality, and `add_size_buckets`.

## Related Projects

- **[warp-replay](https://github.com/russfellows/warp-replay)** - Recommended S3 benchmarking and replay tool; the primary companion tool for PolarWarp. Produces the oplog trace and summary formats that PolarWarp is designed to analyze.
- **[sai3-bench](https://github.com/russfellows/sai3-bench)** - Multi-protocol I/O benchmarking suite
- **[MinIO Warp](https://github.com/minio/warp)** - The upstream S3 benchmarking tool. Has some compatibility with PolarWarp's trace format, but is limited by several bugs and inconsistent output formatting; warp-replay is strongly preferred.
- **[polarWarp](https://github.com/russfellows/polarWarp/tree/main/python)** (Python) - Python implementation of PolarWarp

## Future Enhancements

- Parallel file processing with Rayon
- Comparative analysis between test runs

## License

Licensed under the Apache License, Version 2.0 