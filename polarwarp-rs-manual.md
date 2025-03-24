# PolarWarp-rs User Manual

A lightweight guide for using PolarWarp-rs - the high-performance Rust implementation for analyzing MinIO Warp test results.

## Table of Contents
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Command Line Options](#command-line-options)
- [File Formats](#file-formats)
- [Performance Expectations](#performance-expectations)
- [Troubleshooting](#troubleshooting)

## Installation

### From Binary (Recommended)
Download the latest binary release from our repository and add it to your path:

```bash
chmod +x polarwarp-rs-bin
sudo mv polarwarp-rs-bin /usr/local/bin/polarwarp-rs-bin
```

### From Source
Build from source for the optimal performance or customization:

```bash
# Clone the repository
git clone https://github.com/yourusername/polarwarp-rs.git
cd polarwarp-rs

# Build optimized release version
cargo build --release

# The binary will be available at
./target/release/polarwarp-rs
# Copy to the root directory for easier access
cp ./target/release/polarwarp-rs ../polarwarp-rs-bin
```

## Basic Usage

PolarWarp-rs is designed to work with MinIO Warp output logs in CSV format (raw or ZSTD compressed).

### Analyzing a Single File

```bash
# View basic file information
polarwarp-rs-bin --basic-stats your_file.csv

# Process a compressed file
polarwarp-rs-bin your_file.csv.zst
```

### Processing Multiple Files

```bash
# Analyze multiple files with consolidated results
polarwarp-rs-bin file1.csv file2.csv file3.csv.zst
```

### Skipping Warm-up Periods

```bash
# Skip the first 90 seconds of test data
polarwarp-rs-bin --skip 90s your_file.csv

# Skip the first 5 minutes of test data
polarwarp-rs-bin --skip 5m your_file.csv.zst
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `--basic-stats` | Show only basic file information without full processing |
| `--skip <TIME>` | Skip initial warm-up period (e.g., "90s", "5m") |
| `--help` | Display help information |
| `--version` | Show version information |

## File Formats

PolarWarp-rs supports the following file formats:
- `.csv` - Standard CSV files with tab delimiters
- `.csv.zst` - ZSTD compressed CSV files

The tool automatically detects the format based on file extension.

## Performance Expectations

PolarWarp-rs is designed for high performance:

| Metric | Typical Value |
|--------|---------------|
| Processing speed | ~300ms for 41MB file (~20.5 MB/s) |
| Memory usage | ~756MB peak |
| CPU utilization | Efficiently uses multiple cores (>350% utilization) |
| Binary size | Compact 10MB executable |

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