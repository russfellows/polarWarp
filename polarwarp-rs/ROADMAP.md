# PolarWarp-rs Development Roadmap

This document outlines the planned features and enhancements for the PolarWarp-rs project. The goal is to create a fully-featured, high-performance Rust implementation of the polarWarp tool for processing MinIO Warp output logs.

## Current Status

- ✅ Basic project structure
- ✅ Command-line argument parsing
- ✅ File reading (CSV, ZSTD compressed)
- ✅ Basic data display

## Phase 1: Core Functionality

- [ ] Implement datetime column parsing
  - Convert ISO 8601 timestamps to Rust datetime objects
  - Handle timezone information correctly
- [ ] Add skip time functionality
  - Filter data based on start time threshold
- [ ] Implement byte bucket categorization
  - Create size buckets for objects (None, 1-32k, 32k-128k, etc.)
- [ ] Implement basic statistics calculation by operation
  - Mean/median/percentile latencies
  - Throughput calculations
  - Operations per second

## Phase 2: Advanced Features

- [ ] Multi-file processing
  - Support for processing multiple files
  - Consolidated statistics across files
- [ ] Result formatting
  - Format numeric values with commas for readability
  - Aligned table output
- [ ] Add sorting options
  - Sort by bucket size, operation type, etc.
- [ ] Export results to different formats
  - CSV
  - JSON
  - Markdown tables

## Phase 3: Performance Optimizations

- [ ] Implement parallel processing of files
  - Use Rayon for multi-threading
- [ ] Memory usage optimization
  - Streaming processing for large files
  - Minimize allocation overhead
- [ ] Benchmark against Python implementation
  - Compare execution time
  - Compare memory usage

## Phase 4: Advanced Analysis

- [ ] Implement time-based analysis
  - Track performance over time
  - Detect performance degradation
- [ ] Add visualization options
  - Generate charts/graphs of performance data
  - Heatmaps for identifying hotspots
- [ ] Comparative analysis
  - Compare different test runs
  - Highlight significant differences

## Technical Challenges

During development of the initial prototype, we encountered several technical challenges:

1. **Polars API Compatibility**: The Polars Rust API differs significantly from the Python API, requiring careful adaptation of the data processing logic.

2. **ZSTD File Handling**: Working with compressed files required implementing proper decompression before processing.

3. **Datetime Handling**: Converting ISO 8601 timestamps to Rust datetime objects with proper timezone handling is complex.

4. **Error Handling**: Proper propagation of errors from different components while providing meaningful error messages.

5. **GroupBy Operations**: The approach to grouping and aggregating data is different in Rust Polars compared to Python Polars.

## Contribution Areas

If you're interested in contributing to PolarWarp-rs, here are some areas where help is needed:

- Implementing any of the features listed above
- Writing tests to ensure correctness
- Documentation improvements
- Performance optimization
- Cross-platform testing 