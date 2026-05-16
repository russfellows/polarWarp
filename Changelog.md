# Changelog

All notable changes to PolarWarp are documented here.

Both implementations (Rust `polarwarp-rs` and Python `polars-warp`) track the same version and share feature parity.

---

## [0.2.1] - 2026-05-16

### Changed

- **XY scatter charts** (Rust and Python) — The `{name}-Charts` Excel tab now uses XY scatter
  charts with straight lines and markers instead of plain line charts. Markers are sized at 4pt
  (roughly half the Excel default) so they sit just above the connecting line without dominating.

- **Auto-scaling throughput unit** (Rust and Python) — The throughput chart Y-axis and column
  headers now auto-select the unit based on peak bandwidth in the dataset:

  | Peak `bps` | Unit shown | Divisor |
  |-----------|-----------|---------|
  | ≥ 1 GB/s | GB/s | ÷ 1 000 000 000 |
  | < 1 GB/s | MB/s | ÷ 1 000 000 |

- **Auto-scaling ops/sec unit** (Rust and Python) — The I/O rate chart Y-axis and column
  headers similarly auto-select between `Kops/s` (≥ 1 000 ops/s) and `ops/s` (< 1 000 ops/s).

- **Raw per-second rows in Summary tab** (Rust and Python) — The `{name}-Summary` Excel tab
  now contains the raw per-second rows from the source file (`op`, `start`, `end`, throughput,
  `ops_per_sec`, `errors`), sorted by start time, rather than aggregate statistics. The aggregate
  statistics view is still available in the terminal output.

- **Chart tab column layout** — The Charts tab data columns are now organised as
  `{op}_time_s`, `{op}_{unit}`, `{op}_{ops_unit}` triplets (one triplet per op type),
  with TOTAL and zero-throughput ops excluded. Both the throughput and I/O-rate charts
  share the same per-op elapsed-time column as their X axis.

- **Documentation** — `README.md`, `rust/README.md`, and `rust/MANUAL.md` updated to
  clearly describe the three Excel input modes (trace only, summary only, both together)
  and the correct output tab contents.

### Fixed

- Rust: removed redundant `as u32` cast flagged by `cargo clippy`
- Rust: applied `cargo fmt` formatting to three locations in `main.rs`
- Python: applied `ruff format` reformat pass

---

## [0.2.0] - 2026-05-15

### New Features

- **Time-series performance charts** (Rust and Python) — When processing summary files
  (`.summary.tsv.zst`) with `--excel`, PolarWarp now generates a `{name}-Charts` Excel tab
  alongside the existing `{name}-Summary` tab. The Charts tab contains two embedded line charts:

  | Chart | X-axis | Y-axis | Series |
  |-------|--------|--------|--------|
  | Operations/sec over Time | Seconds from start | ops/sec | GET, PUT, META |
  | Throughput (MiB/s) over Time | Seconds from start | MiB/s | GET, PUT |

  The underlying time-series data (wide-format pivot table with one row per second) is written
  above the charts for further analysis in Excel. META is excluded from the throughput chart
  because metadata operations carry no meaningful byte payload.

---

## [0.1.7] - 2026-05-15

### New Features

- **Unit tests** — Both implementations now include comprehensive unit test suites:
  - **Rust** (`rust/src/main.rs`): 28 tests covering `parse_skip_time`,
    `format_with_commas`, `format_int_with_commas`, `format_duration_ns`,
    `derive_short_name`, `derive_excel_path`, `make_tab_name`, `FileType` equality,
    and `add_size_buckets`. Run with `cargo test`.
  - **Python** (`python/tests/test_polarwarp.py`): 44 tests covering `format_with_commas`,
    `_excel_derive_path`, short-name derivation, tab-name truncation, skip-pattern parsing,
    timedelta formatting, file-type detection, summary stats aggregation, and size bucket logic.
    Run with `uv run --group dev pytest tests/`.

- **Summary file parsing** (Rust and Python) — PolarWarp now accepts aggregated summary files (e.g. `.summary.tsv.zst`) in addition to per-operation trace files. The file type is auto-detected from the header: if `bps` and `ops_per_sec` columns are present it is treated as a summary; if `duration_ns` is present it is treated as a trace.

  Summary files contain one row per ~1-second time window per operation type (format: `op, start, end, bps, ops_per_sec, errors`), as produced by warp-replay and similar tools. An optional `# cmdline …` comment line before the header is silently skipped.

  For summary files, PolarWarp reports throughput variability across segments, grouped by `op`:

  | Column | Description |
  |--------|-------------|
  | `segments` | Number of 1-second windows observed |
  | `mean/p50/p90/p99/min/max MBps` | Throughput distribution across segments |
  | `stdev_MBps` | Sample standard deviation of throughput |
  | `mean/p50/p99 ops/s` | Operation-rate distribution across segments |
  | `total_errors` | Cumulative error count |

  The `TOTAL` row is always printed last. When only one segment is present, `stdev_MBps` is shown as `N/A`.

  With `--excel`, each summary file produces a dedicated `{name}-Summary` tab in the workbook.

  Mixing trace and summary files in a single invocation is allowed (each is processed independently); a warning is emitted and consolidation is skipped for the summary files.

---

## [0.1.6] - 2026-02-24

### Bug Fixes

- **#17 Overlap-aware multi-file consolidation** — When processing multiple oplog files, PolarWarp previously concatenated all rows regardless of whether the files' time ranges actually overlapped. This produced incorrect consolidated throughput and operation counts for sequential or partially overlapping test runs.

  The fix computes the **Jaccard overlap** (`overlap_duration / union_duration`) across all files and applies one of three strategies:

  | Jaccard | Behavior |
  |---------|----------|
  | < 3% | Files treated as **sequential runs** — consolidation is skipped and a warning is printed. Per-file results are still valid. |
  | 3–97% | **Partial overlap** — a warning is printed showing the exact Jaccard %, each file's data is filtered to the intersection window, then consolidated. |
  | ≥ 97% | **Fully concurrent** — data is filtered to the intersection window and consolidated without a warning. |

  In all non-sequential cases each file's rows are filtered to `start ∈ [overlap_start, overlap_end)` before concatenation, so counts and throughput are computed over a consistent time slice across files.

  **Also fixed (Python):** per-file `run_time_secs` was incorrectly using the running global overlap start instead of each file's own effective start time, causing subtly wrong per-file throughput when file start times differed.

### Notes

- Overlap thresholds (`OVERLAP_MIN_PCT = 3.0`, `OVERLAP_MAX_PCT = 97.0`) are defined as named constants near the top of both implementations and can be adjusted if needed.
- Each file's individual time range and duration is now printed during multi-file runs for transparency.

---

## [0.1.5] - 2025-01-23

### Bug Fixes

- **#14 Throughput calculation** — `xput_MBps` and `ops_/_sec` now use the per-operation effective time window derived from actual `start`/`end` timestamps in each bucket, rather than the total file run time. Fixes incorrect (artificially low) throughput on non-overlapping or sparse workloads.

- **#15 Per-endpoint statistics** — Per-endpoint stats were not correctly computed or displayed. Fixed aggregation and output for `--per-endpoint` mode.

- **#16 Thread concurrency column** — The `concurrency` column has been renamed to `max_threads` and now correctly reports the distinct thread count observed within each size/op bucket.

- **Python Excel validity errors** — Multiple issues caused Excel to report "found a problem with content" when opening Python-generated `.xlsx` files:
  - `http://` endpoint values were auto-converted to hyperlinks (broken URLs) — fixed with `{'strings_to_urls': False}` on Workbook creation.
  - Endpoint strings with trailing newlines caused corrupt cell values — fixed with `.strip()` in `write_string()`.
  - Section labels starting with `===` or `---` were parsed as formulas — fixed by using `write_string()` instead of `write()` for all label cells.
  - Detail tabs were missing the per-operation-type (META/GET/PUT) endpoint breakdown — added via new `_endpoint_pd_for_op()` helper.
  - Results tab had a duplicate column header row before summary rows — fixed by splitting header and data writes.

### New Features

- **`--excel [=FILE]` flag** (Rust and Python) — Exports analysis results to an `.xlsx` workbook. When `FILE` is omitted, the output file is derived from the first input filename. The workbook contains:
  - One **Results tab** per input file with full size-bucketed statistics
  - One **Detail tab** per input file with per-endpoint/per-client breakdowns, split by META, GET, and PUT operation types
  - A **Consolidated** tab (when multiple files are provided) with merged results

- **`--per-endpoint` flag** (Rust and Python) — Generates per-endpoint statistics in both console output and Excel Detail tabs. Each endpoint is shown with overall stats plus separate breakdowns by META, GET, and PUT operation type. Columns: `endpoint, mean_lat_us, med._lat_us, 99%_lat_us, ops_/_sec, xput_MBps, count`.

- **`runtime_s` column** — A new final output column showing the effective time window (in seconds) used for throughput calculation of each bucket row. Useful for verifying that the correct time window is being applied, especially when using `--skip`.

- **`max_threads` column** (renamed from `concurrency`) — Reports the maximum distinct thread count observed in each bucket. Moved to second-to-last column position.

- **Excel worksheet name deduplication** — When processing multiple files whose names share a common 20-character prefix (after timestamp stripping), worksheet names are disambiguated by appending `-1`, `-2`, etc.

### Breaking Changes

- Output column `concurrency` has been renamed to `max_threads`. Any downstream scripts parsing the column by name will need to be updated.

---

## [0.1.4] - 2025-01-10

### Initial Release

- Multi-format oplog support: TSV and CSV, with automatic zstd decompression and separator detection
- 9 size buckets matching sai3-bench definitions (zero through >2 GiB)
- Summary rows for META (LIST/HEAD/DELETE/STAT), GET, and PUT aggregate statistics
- Latency percentiles: mean, median, p90, p95, p99, max (statistically valid, not averaged)
- Throughput metrics: ops/sec and MiB/sec per bucket
- Multi-file consolidation: combine and aggregate results from multiple agent oplogs in a single command
- Time skip (`--skip`) to exclude warmup periods
- Per-client statistics (`--per-client`) for multi-client workload comparison
- Rust implementation (`polarwarp-rs`) achieving ~1,075K records/sec in release mode
- Python implementation (`polars-warp`) achieving ~558K records/sec
- Both implementations produce identical output format
