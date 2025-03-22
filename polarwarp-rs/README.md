# PolarWarp-rs

A Rust implementation of polarWarp for processing MinIO Warp object testing output logs.

## Project Status

⚠️ **This project is in early development** ⚠️

Currently, PolarWarp-rs provides basic functionality to read and display MinIO Warp output files. More advanced features are planned - see the [ROADMAP.md](ROADMAP.md) file for details.

## Overview

PolarWarp-rs aims to be a high-performance tool for analyzing MinIO Warp object testing output logs. It is a Rust port of the Python-based polarWarp tool, designed to provide efficient processing of large output files.

The goal is to provide a compiled binary that can be distributed without sharing source code, while matching or exceeding the performance of the original Python implementation.

## Features

Current:
- Reading MinIO Warp output logs (CSV/ZSTD compressed)
- Basic file information display
- Command-line interface

Planned:
- Size-based object bucketing
- Statistical analysis (latency percentiles, throughput, ops/sec)
- Multi-file processing with result consolidation
- Time-skip option to exclude warmup periods
- Formatted output with human-readable numbers

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/polarwarp-rs.git
cd polarwarp-rs

# Build the release version
cargo build --release

# The binary will be available at target/release/polarwarp-rs
```

## Usage

```bash
# Get help
polarwarp-rs --help

# Process a file with basic stats
polarwarp-rs --basic-stats your_file.csv

# Process a compressed file
polarwarp-rs --basic-stats your_file.csv.zst
```

### Command Line Options

- `--basic-stats`: Show basic information about the file without processing
- `--skip <SKIP>`: Skip a specified amount of time from the start of each file (e.g., "90s", "5m") - *not yet implemented*
- `--help`: Display help information
- `--version`: Display version information

## Performance

The original Python polarWarp tool is already ~37x faster than MinIO's native tools. We aim to make PolarWarp-rs even faster and more memory-efficient.

## Contributing

Contributions are welcome! Please check the [ROADMAP.md](ROADMAP.md) file for areas where help is needed.

## License

Licensed under the Apache License, Version 2.0 