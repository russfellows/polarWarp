# PolarWarp - Python Implementation

[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](../LICENSE)

Python implementation of PolarWarp using [Polars](https://pola.rs/) for fast DataFrame operations.

## Installation

### Using uv (Recommended)

```bash
cd python
uv venv --python 3.12
source .venv/bin/activate
uv pip install -e .
```

### Using pip

```bash
cd python
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Usage

```bash
# Display help
uv run ./polarwarp.py --help

# Process a single file
uv run ./polarwarp.py oplog.csv.zst

# Process multiple files (results are consolidated)
uv run ./polarwarp.py agent-1.csv.zst agent-2.csv.zst

# Skip first 2 minutes of warmup
uv run ./polarwarp.py --skip=2m oplog.csv.zst
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `<FILES>...` | Input files to process (CSV/TSV, optionally zstd compressed) |
| `--skip=<TIME>` | Skip warmup time from start (e.g., "90s", "5m") |
| `--help` | Display help information |

## Performance

On a Mac Studio M1 Ultra with 32 GB RAM:

| Metric | Value |
|--------|-------|
| Processing speed | ~55M lines in 40.7 seconds |
| vs MinIO warp tools | **37x faster** |
| Memory usage | ~18 GB peak for large files |

## Size Buckets

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
