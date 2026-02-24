# Changelog

All notable changes to PolarWarp are documented here.

Both implementations (Rust `polarwarp-rs` and Python `polars-warp`) track the same version and share feature parity.

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
