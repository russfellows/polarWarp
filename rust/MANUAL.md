# PolarWarp-rs User Manual

A guide for using PolarWarp-rs - the high-performance Rust implementation for analyzing storage I/O operation logs.

## Table of Contents
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Command Line Options](#command-line-options)
- [Output Format](#output-format)
- [Excel Export](#excel-export)
- [File Formats](#file-formats)
- [Performance Expectations](#performance-expectations)
- [Troubleshooting](#troubleshooting)

## Installation

### From Binary (Recommended)
Download the latest binary release from our repository and add it to your path:

```bash
chmod +x polarwarp-rs
sudo mv polarwarp-rs /usr/local/bin/
```

### From Source
Build from source for optimal performance:

```bash
# Clone the repository
git clone https://github.com/russfellows/polarWarp.git
cd polarWarp/polarwarp-rs

# Build optimized release version
cargo build --release

# The binary will be available at
./target/release/polarwarp-rs
```

## Basic Usage

PolarWarp-rs processes oplog files in TSV or CSV format (raw or ZSTD compressed).

### Analyzing a Single File

```bash
# Full analysis with latency percentiles and throughput
polarwarp-rs oplog.tsv.zst

# View basic file information only
polarwarp-rs --basic-stats oplog.tsv.zst
```

### Processing Multiple Files

```bash
# Analyze multiple files with consolidated results
polarwarp-rs agent-1-oplog.tsv.zst agent-2-oplog.tsv.zst
```

### Skipping Warm-up Periods

```bash
# Skip the first 90 seconds of test data
polarwarp-rs --skip 90s oplog.tsv.zst

# Skip the first 2 minutes of test data
polarwarp-rs --skip 2m oplog.tsv.zst
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `<FILES>...` | Input files to process (required) |
| `-s, --skip <TIME>` | Skip initial warm-up period (e.g., "90s", "5m") |
| `--per-client` | Generate per-client statistics in addition to overall stats |
| `--per-endpoint` | Generate per-endpoint statistics in addition to overall stats |
| `--excel [=FILE]` | Export results to an Excel `.xlsx` workbook; FILE defaults to first input filename |
| `--basic-stats` | Show only basic file information without full processing |
| `-h, --help` | Display help information |
| `-V, --version` | Show version information |

## Output Format

PolarWarp-rs outputs a table with the following columns:

| Column | Description |
|--------|-------------|
| `op` | Operation type (GET, PUT, LIST, DELETE, etc.) |
| `bytes_bucket` | Size bucket label |
| `bucket_#` | Bucket number (0-8) |
| `mean_lat_us` | Mean latency in microseconds |
| `med._lat_us` | Median (p50) latency in microseconds |
| `90%_lat_us` | 90th percentile latency |
| `95%_lat_us` | 95th percentile latency |
| `99%_lat_us` | 99th percentile latency |
| `max_lat_us` | Maximum latency |
| `avg_obj_KB` | Average object size in KiB |
| `ops_/_sec` | Operations per second |
| `xput_MBps` | Throughput in MiB/sec |
| `count` | Number of operations |
| `max_threads` | Maximum distinct thread count observed in this bucket |
| `runtime_s` | Effective time window (seconds) used for throughput calculation |

### Size Buckets

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

## Excel Export

Use `--excel` to export results to an Excel `.xlsx` workbook. There are three usage modes depending on which file(s) you pass:

```bash
# Mode 1 — Trace file only
# Produces: Results + Detail tabs (latency percentiles, throughput, op counts)
polarwarp-rs --excel=report.xlsx run.trace.tsv.zst

# Mode 2 — Summary file only
# Produces: Summary tab (raw per-second rows) + Charts tab (XY scatter plots)
polarwarp-rs --excel=report.xlsx run.summary.tsv.zst

# Mode 3 — Both files together (recommended for a complete picture)
# Produces: all four tabs in one workbook
polarwarp-rs --excel=report.xlsx run.trace.tsv.zst run.summary.tsv.zst

# The trace and summary files from the same warp run share a base filename:
#   warp-mixed-2026-05-15[153937]-Gn1h.trace.tsv.zst
#   warp-mixed-2026-05-15[153937]-Gn1h.summary.tsv.zst

# Export to auto-named file (derived from first input filename)
polarwarp-rs --excel run.trace.tsv.zst

# Combine with other options
polarwarp-rs --skip 90s --per-endpoint --excel=report.xlsx run.trace.tsv.zst run.summary.tsv.zst
```

### Workbook Structure

Sheets produced depend on the input file type(s):

| Sheet | Source | Contents |
|-------|--------|----------|
| `<filename>-Results` | Trace | Full size-bucketed statistics table (latency, throughput, op counts) |
| `<filename>-Detail` | Trace | Per-endpoint or per-client breakdown |
| `Consolidated-Results` | Trace (multiple) | Merged results across all trace files |
| `Consolidated-Detail` | Trace (multiple) | Merged per-endpoint breakdown |
| `<filename>-Summary` | Summary | Raw per-second rows: op, start, end, GBps, ops/sec, errors |
| `<filename>-Charts` | Summary | Two XY scatter charts: Throughput (GB/s) and I/O Rate (ops/sec) over elapsed time, one series per op type (GET, PUT, etc.; TOTAL excluded) |

Detail tabs contain three sections:
- **Overall** — aggregate per-endpoint stats (all op types combined)
- **META Operations** — per-endpoint stats for LIST/HEAD/DELETE/STAT
- **GET Operations** — per-endpoint stats for GET
- **PUT Operations** — per-endpoint stats for PUT

### Worksheet Naming

Sheet names are derived from the input filename by stripping any warp `[timestamp]` suffix and truncating to 20 characters. When multiple files share the same 20-character prefix, a numeric suffix (`-1`, `-2`, …) is appended to ensure unique names.

## File Formats

PolarWarp-rs supports the following file formats:
- `.tsv` - Tab-separated values (default for sai3-bench oplogs)
- `.tsv.zst` - ZSTD compressed TSV files
- `.csv` - Comma-separated values
- `.csv.zst` - ZSTD compressed CSV files

The tool automatically detects the separator based on file extension. The **file type** (trace vs. summary) is auto-detected from the header columns.

### Trace file columns (per-operation oplog)

```
idx  thread  op  client_id  n_objects  bytes  endpoint  file  error  start  first_byte  end  duration_ns
```

### Summary file columns (aggregated time-series)

```
op  start  end  bps  ops_per_sec  errors
```

Each row represents one ~1-second time window per operation type (plus a `TOTAL` row). An optional `# cmdline …` comment before the header is ignored.

## Performance Expectations

PolarWarp-rs is designed for high performance:

| Metric | Debug Build | Release Build |
|--------|-------------|---------------|
| Processing speed | ~95K records/sec | ~780K records/sec |
| 230K record file | ~2.5 seconds | ~300 ms |
| Speedup | baseline | ~8x faster |

Release build optimizations:
- Link-Time Optimization (LTO)
- Single codegen unit
- Maximum optimization level (opt-level = 3)

## Troubleshooting

### Common Issues

1. **File Not Found**: Ensure the file path is correct and the file exists.

2. **Invalid Skip Format**: When using the `--skip` option, ensure it follows the format:
   - `Ns` for N seconds (e.g., "90s")
   - `Nm` for N minutes (e.g., "5m")

3. **Memory Issues**: For very large files, ensure you have adequate available RAM.

### Reporting Problems

If you encounter issues, please report them with:
- Command line used
- Error message received
- File size and type
- System specifications

## License

PolarWarp-rs is licensed under the Apache License, Version 2.0. 