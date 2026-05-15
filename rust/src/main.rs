// SPDX-FileCopyrightText: 2025 Russ Fellows <russ.fellows@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// polarwarp-rs: A Rust implementation of polarWarp for processing oplog files

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{ArgAction, Parser};
use num_format::{Locale, ToFormattedString};
use polars::prelude::*;
use regex::Regex;
use rust_xlsxwriter::{Chart, ChartLegendPosition, ChartType, Format, Workbook};

/// Rows for one Excel results tab and one detail tab: (results_rows, detail_rows)
type ExcelTabRows = (Vec<Vec<String>>, Vec<Vec<String>>);

/// Per-file Excel data collected before writing: (results_rows, detail_rows, file_path)
type FileExcelEntry = (Vec<Vec<String>>, Vec<Vec<String>>, String);

/// Detected format of an input file.
#[derive(Debug, Clone, Copy, PartialEq)]
enum FileType {
    /// Per-operation trace: columns include duration_ns, bytes, client_id, …
    Trace,
    /// Aggregated time-series summary: columns are op, start, end, bps, ops_per_sec, errors
    Summary,
}

/// Number of size buckets (matching sai3-bench)
const NUM_BUCKETS: usize = 9;

/// Size bucket boundaries (in bytes) - matching sai3-bench
const BUCKET_8K: i64 = 8 * 1024; // 8 KiB
const BUCKET_64K: i64 = 64 * 1024; // 64 KiB
const BUCKET_512K: i64 = 512 * 1024; // 512 KiB
const BUCKET_4M: i64 = 4 * 1024 * 1024; // 4 MiB
const BUCKET_32M: i64 = 32 * 1024 * 1024; // 32 MiB
const BUCKET_256M: i64 = 256 * 1024 * 1024; // 256 MiB
const BUCKET_2G: i64 = 2 * 1024 * 1024 * 1024; // 2 GiB

/// Size bucket labels (matching sai3-bench)
const BUCKET_LABELS: [&str; NUM_BUCKETS] = [
    "zero",         // 0 bytes (metadata ops)
    "1B-8KiB",      // Bucket 1
    "8KiB-64KiB",   // Bucket 2
    "64KiB-512KiB", // Bucket 3
    "512KiB-4MiB",  // Bucket 4
    "4MiB-32MiB",   // Bucket 5
    "32MiB-256MiB", // Bucket 6
    "256MiB-2GiB",  // Bucket 7
    ">2GiB",        // Bucket 8
];

/// Metadata operations that should be grouped together
const META_OPS: [&str; 4] = ["LIST", "HEAD", "DELETE", "STAT"];

/// Multi-file consolidation overlap thresholds (Jaccard = overlap / union).
/// Below MIN → files are sequential runs, consolidation is skipped.
/// Above MAX → files are treated as fully concurrent, no warning emitted.
/// Between MIN and MAX → partial overlap: warn, but still consolidate the intersection.
const OVERLAP_MIN_PCT: f64 = 3.0;
const OVERLAP_MAX_PCT: f64 = 97.0;

/// CLI arguments
#[derive(Parser)]
#[command(
    name = "polarwarp-rs",
    version = env!("CARGO_PKG_VERSION"),
    about = "A Rust implementation of polarWarp for processing oplog files",
    long_about = "PolarWarp-rs processes oplog files to provide performance metrics.\n\n\
                  It reads TSV-formatted operation logs (optionally zstd compressed) and \n\
                  computes latency percentiles, throughput, and ops/sec grouped by \n\
                  operation type and object size buckets."
)]
struct Args {
    /// Skip a specified amount of time from the start of each file
    /// e.g. "90s" for 90 seconds or "5m" for 5 minutes
    #[arg(short, long)]
    skip: Option<String>,

    /// Input files to process (TSV format, can be ZSTD compressed)
    #[arg(required = true, action = ArgAction::Append)]
    files: Vec<String>,

    /// Generate per-client statistics (in addition to overall stats)
    #[arg(long)]
    per_client: bool,

    /// Generate per-endpoint statistics (in addition to overall stats)
    #[arg(long)]
    per_endpoint: bool,

    /// Just print basic stats without full processing
    #[arg(long)]
    basic_stats: bool,

    /// Export results to Excel file. Optionally specify output path with =.
    /// --excel             derives name from input file (single) or polarwarp-results.xlsx
    /// --excel=path.xlsx   writes to the specified path
    #[arg(long, num_args = 0..=1, default_missing_value = "", require_equals = true,
          value_name = "FILE")]
    excel: Option<String>,
}

/// Parse skip time argument (e.g., "90s" or "5m") into nanoseconds
fn parse_skip_time(skip: &str) -> Result<i64> {
    let re = Regex::new(r"^(\d+)([sm])$")?;

    if let Some(caps) = re.captures(skip) {
        let value: i64 = caps.get(1).unwrap().as_str().parse()?;
        let unit = caps.get(2).unwrap().as_str();

        let nanos = match unit {
            "s" => value * 1_000_000_000,
            "m" => value * 60 * 1_000_000_000,
            _ => anyhow::bail!("Invalid time unit: {}", unit),
        };

        Ok(nanos)
    } else {
        anyhow::bail!(
            "Invalid skip format '{}'. Use format like '90s' or '5m'",
            skip
        )
    }
}

/// Detect the separator used in a file by examining the header line
/// Returns tab if tabs found, comma if commas found, defaults to tab
fn detect_separator(file_path: &str) -> Result<u8> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let file =
        File::open(file_path).with_context(|| format!("Failed to open file: {}", file_path))?;

    // Handle zstd compressed files
    let first_line = if file_path.ends_with(".zst") {
        let decoder = zstd::stream::read::Decoder::new(file)
            .with_context(|| format!("Failed to decompress zstd file: {}", file_path))?;
        let mut reader = BufReader::new(decoder);
        let mut line = String::new();
        reader.read_line(&mut line).with_context(|| {
            format!("Failed to read header from compressed file: {}", file_path)
        })?;
        line
    } else {
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .with_context(|| format!("Failed to read header from file: {}", file_path))?;
        line
    };

    if first_line.is_empty() {
        anyhow::bail!("File '{}' is empty or has no header", file_path);
    }

    // Count tabs vs commas in header line
    let tab_count = first_line.matches('\t').count();
    let comma_count = first_line.matches(',').count();

    // Use whichever delimiter appears more often
    // Note: warp program creates .csv files that are actually tab-separated!
    if tab_count > comma_count {
        if file_path.contains(".csv") && tab_count > 0 {
            eprintln!(
                "Note: File has .csv extension but uses tab separation (typical for warp output)"
            );
        }
        Ok(b'\t')
    } else if comma_count > 0 {
        Ok(b',')
    } else {
        // No clear delimiter found, might be invalid format
        if tab_count == 0 && comma_count == 0 {
            eprintln!(
                "Warning: No delimiter found in file '{}', defaulting to tab",
                file_path
            );
        }
        Ok(b'\t') // Default to tab
    }
}

/// Read the first non-empty, non-comment header line from a (possibly zstd) file.
fn read_header_line(file_path: &str) -> Result<String> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let file =
        File::open(file_path).with_context(|| format!("Failed to open file: {}", file_path))?;

    let read_first_non_comment = |reader: &mut dyn BufRead| -> Result<String> {
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 {
                anyhow::bail!("File '{}' has no header row", file_path);
            }
            let trimmed = line.trim().to_string();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                return Ok(trimmed);
            }
        }
    };

    if file_path.ends_with(".zst") {
        let decoder = zstd::stream::read::Decoder::new(file)
            .with_context(|| format!("Failed to decompress zstd file: {}", file_path))?;
        let mut reader = BufReader::new(decoder);
        read_first_non_comment(&mut reader)
    } else {
        let mut reader = BufReader::new(file);
        read_first_non_comment(&mut reader)
    }
}

/// Detect whether a file is a per-op trace or an aggregated summary,
/// by inspecting the header columns.
fn detect_file_type(file_path: &str) -> Result<FileType> {
    let header = read_header_line(file_path)?;
    let cols: Vec<&str> = header.split('\t').collect();
    if cols.contains(&"bps") && cols.contains(&"ops_per_sec") {
        Ok(FileType::Summary)
    } else if cols.contains(&"duration_ns") {
        Ok(FileType::Trace)
    } else {
        anyhow::bail!(
            "File '{}' has unrecognized header columns: {}\n\
             Expected either trace columns (duration_ns, …) or summary columns (bps, ops_per_sec, …)",
            file_path, header
        )
    }
}

/// Format a number with commas for readability
fn format_with_commas(value: f64) -> String {
    if value.is_nan() || value.is_infinite() {
        return format!("{:.2}", value);
    }

    let int_part = value.trunc() as i64;
    let frac_part = (value.fract() * 100.0).round() as i64;

    format!(
        "{}.{:02}",
        int_part.to_formatted_string(&Locale::en),
        frac_part.abs()
    )
}

/// Format integer with commas
fn format_int_with_commas(value: i64) -> String {
    value.to_formatted_string(&Locale::en)
}

fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Resolve Excel output path early (before processing files)
    let excel_path: Option<String> = args.excel.as_ref().map(|v| {
        if v.is_empty() {
            derive_excel_path(&args.files)
        } else {
            v.clone()
        }
    });

    // Parse skip time if provided
    let skip_nanos = if let Some(ref skip) = args.skip {
        let nanos = parse_skip_time(skip)?;
        println!("Using skip value of {}", skip);
        Some(nanos)
    } else {
        None
    };

    // Store results from each file for consolidation
    let mut all_dataframes: Vec<DataFrame> = Vec::new();
    // Overlap window: latest start / earliest end across all files
    let mut overlap_start_ns: Option<i64> = None;
    let mut overlap_end_ns: Option<i64> = None;
    // Union window: earliest start / latest end across all files
    let mut union_start_ns: Option<i64> = None;
    let mut union_end_ns: Option<i64> = None;
    // Per-file time ranges for diagnostic output
    let mut file_ranges: Vec<(String, i64, i64)> = Vec::new();

    // Excel: per-file collected rows (main_rows, detail_rows, file_path)
    let mut file_excel_data: Vec<FileExcelEntry> = Vec::new();

    // Summary-file Excel rows collected separately (one entry per file):
    // (stats_rows, raw_df_for_charts, file_path)
    let mut summary_excel_data: Vec<(Vec<Vec<String>>, DataFrame, String)> = Vec::new();
    let mut any_summary_file = false;
    let mut any_trace_file = false;

    // Process each file
    for file_path in &args.files {
        println!("\nProcessing file: {}", file_path);

        let start = Instant::now();

        // Detect file type and route to the appropriate handler
        let file_type = detect_file_type(file_path)?;

        match file_type {
            FileType::Summary => {
                any_summary_file = true;
                println!("Summary Statistics for: {}", file_path);
                let (df, _, _) = process_summary_file(file_path)?;
                if excel_path.is_some() {
                    let rows = collect_summary_excel_rows(&df)?;
                    summary_excel_data.push((rows, df.clone(), file_path.clone()));
                }
                let elapsed = start.elapsed();
                println!("Processed file in {:.2?}", elapsed);
                // Do not push to all_dataframes — summary rows are not per-op trace rows
                continue;
            }
            FileType::Trace => {
                any_trace_file = true;
            }
        }

        // Read and process the file
        let (df, file_start_ns, file_end_ns) = process_file(
            file_path,
            skip_nanos,
            args.basic_stats,
            args.per_client,
            args.per_endpoint,
        )?;

        // Update overlap (latest-start / earliest-end) and union (earliest-start / latest-end)
        if let (Some(fs), Some(fe)) = (file_start_ns, file_end_ns) {
            overlap_start_ns = Some(overlap_start_ns.map_or(fs, |v: i64| v.max(fs)));
            overlap_end_ns = Some(overlap_end_ns.map_or(fe, |v: i64| v.min(fe)));
            union_start_ns = Some(union_start_ns.map_or(fs, |v: i64| v.min(fs)));
            union_end_ns = Some(union_end_ns.map_or(fe, |v: i64| v.max(fe)));
            file_ranges.push((file_path.clone(), fs, fe));
        }

        // Store the processed dataframe for consolidation
        if !args.basic_stats {
            // Collect Excel rows if requested (must borrow df before moving it)
            if excel_path.is_some() {
                if let (Some(fs), Some(fe)) = (file_start_ns, file_end_ns) {
                    let run_secs = (fe - fs) as f64 / 1_000_000_000.0;
                    let (main_rows, detail_rows) =
                        collect_stats_rows(&df, run_secs, args.per_client, args.per_endpoint)?;
                    file_excel_data.push((main_rows, detail_rows, file_path.clone()));
                }
            }
            all_dataframes.push(df);
        }

        let elapsed = start.elapsed();
        println!("Processed file in {:.2?}", elapsed);
    }

    // Warn if the user mixed trace and summary files in one invocation
    if any_trace_file && any_summary_file {
        eprintln!(
            "WARNING: Mixed trace and summary files provided.  \
             Consolidation is skipped for summary files; each is reported independently."
        );
    }

    // Excel: consolidated tab data (populated below if multiple files)
    let mut consolidated_excel: Option<ExcelTabRows> = None;

    // If we have multiple files, consolidate results
    if args.files.len() > 1 && !args.basic_stats {
        println!("\nDone Processing Files... Consolidating Results");

        // Print per-file time ranges for transparency
        for (fp, fs, fe) in &file_ranges {
            println!(
                "  File time range: {}  (duration: {})",
                fp,
                format_duration_ns(fe - fs)
            );
        }

        match (
            overlap_start_ns,
            overlap_end_ns,
            union_start_ns,
            union_end_ns,
        ) {
            (Some(os), Some(oe), Some(us), Some(ue)) => {
                let overlap_dur = oe - os; // may be ≤ 0 if no intersection
                let union_dur = ue - us;

                // Jaccard in [0, 1]; clamp to 0 if no intersection
                let jaccard_pct = if overlap_dur <= 0 || union_dur <= 0 {
                    0.0_f64
                } else {
                    (overlap_dur as f64 / union_dur as f64) * 100.0
                };

                println!(
                    "  Overlap window: {} ns  Union window: {} ns",
                    overlap_dur.max(0),
                    union_dur
                );
                println!("  Jaccard overlap: {:.1}%  (overlap / union)", jaccard_pct);

                if jaccard_pct < OVERLAP_MIN_PCT {
                    // Files are sequential – consolidation would be meaningless
                    println!(
                        "WARNING: Files appear to be sequential runs ({:.1}% Jaccard overlap < {:.0}% threshold).",
                        jaccard_pct, OVERLAP_MIN_PCT
                    );
                    println!("  Skipping consolidation – per-file results above are still valid.");
                } else {
                    // Partial or full overlap – filter every dataframe to the intersection
                    // window so that counts and throughput are computed over the same time
                    // slice across all files.
                    if jaccard_pct < OVERLAP_MAX_PCT {
                        println!(
                            "WARNING: Partial overlap detected ({:.1}% Jaccard).  \
                             Filtering all files to overlap window before consolidating.",
                            jaccard_pct
                        );
                    } else {
                        println!("Files are concurrent runs ({:.1}% Jaccard).  Consolidating overlap window.",
                                 jaccard_pct);
                    }

                    let overlap_secs = overlap_dur as f64 / 1_000_000_000.0;
                    println!(
                        "  Overlap duration: {}  ({:.2} s)",
                        format_duration_ns(overlap_dur),
                        overlap_secs
                    );

                    // Filter each dataframe to operations whose start falls within
                    // [overlap_start, overlap_end) then concatenate
                    let filtered: Vec<DataFrame> = all_dataframes
                        .into_iter()
                        .map(|df| filter_to_window(df, os, oe))
                        .collect::<Result<_>>()?;

                    let consolidated_df = concat_dataframes(&filtered)?;

                    // Compute and display consolidated stats
                    compute_and_display_stats(
                        &consolidated_df,
                        overlap_secs,
                        "Consolidated Results:",
                        args.per_client,
                        args.per_endpoint,
                    )?;

                    // Collect consolidated Excel rows if requested
                    if excel_path.is_some() {
                        let (main_rows, detail_rows) = collect_stats_rows(
                            &consolidated_df,
                            overlap_secs,
                            args.per_client,
                            args.per_endpoint,
                        )?;
                        consolidated_excel = Some((main_rows, detail_rows));
                    }
                }
            }
            _ => {
                println!("No valid time data to consolidate.");
            }
        }
    }

    // Write Excel file if requested
    if let Some(ref path) = excel_path {
        let mut tabs: Vec<(String, Vec<Vec<String>>)> = Vec::new();
        let single = file_excel_data.len() == 1;

        // Pre-compute unique short names to avoid worksheet name collisions when
        // multiple files share the same prefix after truncation to 20 characters.
        let short_names: Vec<String> = if single {
            vec!["".to_string()]
        } else {
            let raw: Vec<String> = file_excel_data
                .iter()
                .map(|(_, _, fp)| derive_short_name(fp))
                .collect();
            // Count how many times each derived name appears.
            let mut tally: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for n in &raw {
                *tally.entry(n.clone()).or_insert(0) += 1;
            }
            // For duplicates, append a 1-based counter suffix (-1, -2, …).
            let mut seen: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            raw.iter()
                .map(|n| {
                    if tally[n] > 1 {
                        let counter = seen.entry(n.clone()).or_insert(1);
                        let unique = format!("{}-{}", n, counter);
                        *counter += 1;
                        unique
                    } else {
                        n.clone()
                    }
                })
                .collect()
        };

        for (i, (main_rows, detail_rows, _fp)) in file_excel_data.iter().enumerate() {
            let (results_tab, detail_tab) = if single {
                ("Results".to_string(), "Detail".to_string())
            } else {
                let short = &short_names[i];
                (
                    make_tab_name(short, "Results"),
                    make_tab_name(short, "Detail"),
                )
            };
            tabs.push((results_tab, main_rows.clone()));
            if !detail_rows.is_empty() {
                tabs.push((detail_tab, detail_rows.clone()));
            }
        }

        if let Some((main_rows, detail_rows)) = consolidated_excel {
            tabs.push(("Consolidated".to_string(), main_rows));
            if !detail_rows.is_empty() {
                tabs.push(("Consol-Detail".to_string(), detail_rows));
            }
        }

        // Add one tab per summary file (stats) and collect chart tab data
        let mut chart_tab_data: Vec<(DataFrame, String)> = Vec::new();
        for (rows, df, fp) in &summary_excel_data {
            let short = derive_short_name(fp);
            let tab_name = make_tab_name(&short, "Summary");
            tabs.push((tab_name, rows.clone()));
            chart_tab_data.push((df.clone(), make_tab_name(&short, "Charts")));
        }

        if !tabs.is_empty() || !chart_tab_data.is_empty() {
            write_excel_workbook_with_chart_tabs(path, &tabs, &chart_tab_data)?;
            println!("\nExcel file written: {}", path);
        }
    }

    Ok(())
}

/// Concatenate multiple dataframes
fn concat_dataframes(dfs: &[DataFrame]) -> Result<DataFrame> {
    if dfs.is_empty() {
        anyhow::bail!("No dataframes to concatenate");
    }

    if dfs.len() == 1 {
        return Ok(dfs[0].clone());
    }

    let lazy_frames: Vec<LazyFrame> = dfs.iter().map(|df| df.clone().lazy()).collect();
    let result = concat(lazy_frames, UnionArgs::default())?.collect()?;

    Ok(result)
}

/// Filter a dataframe to rows whose `start_ns` falls within [window_start, window_end).
/// Used to restrict each file's data to the shared overlap window before consolidating.
fn filter_to_window(df: DataFrame, window_start: i64, window_end: i64) -> Result<DataFrame> {
    let filtered = df
        .lazy()
        .filter(
            col("start_ns")
                .gt_eq(lit(window_start))
                .and(col("start_ns").lt(lit(window_end))),
        )
        .collect()?;
    Ok(filtered)
}

/// Format duration from nanoseconds to human-readable string (h:mm:ss.fraction)
fn format_duration_ns(nanos: i64) -> String {
    let total_secs = nanos as f64 / 1_000_000_000.0;
    let hours = (total_secs / 3600.0).floor() as i64;
    let minutes = ((total_secs % 3600.0) / 60.0).floor() as i64;
    let secs = total_secs % 60.0;

    format!("{}:{:02}:{:09.6}", hours, minutes, secs)
}

/// Process a single file and return the dataframe with computed metrics
fn process_file(
    file_path: &str,
    skip_nanos: Option<i64>,
    basic_stats_only: bool,
    per_client: bool,
    per_endpoint: bool,
) -> Result<(DataFrame, Option<i64>, Option<i64>)> {
    // Read the file
    let mut df = read_tsv_file(file_path)?;

    // Print basic statistics
    if basic_stats_only {
        print_basic_stats(&df);
        return Ok((df, None, None));
    }

    println!("Shape: {} rows × {} columns", df.height(), df.width());

    // Parse timestamps - convert string timestamps to datetime
    df = parse_timestamps(df)?;

    // Get start and end times (in nanoseconds since epoch)
    let (start_ns, end_ns) = get_time_range(&df)?;

    // Apply skip time if specified
    let effective_start_ns = if let Some(skip) = skip_nanos {
        let new_start = start_ns + skip;
        println!("Skipping rows with 'start' <= {} ns", new_start);

        // Filter out rows before skip threshold
        df = df
            .lazy()
            .filter(col("start_ns").gt(lit(new_start)))
            .collect()?;

        new_start
    } else {
        start_ns
    };

    let run_time_secs = (end_ns - effective_start_ns) as f64 / 1_000_000_000.0;
    let run_time_formatted = format_duration_ns(end_ns - effective_start_ns);
    println!(
        "The file run time is {}, time in seconds is: {:.2}",
        run_time_formatted, run_time_secs
    );

    // Add size buckets
    df = add_size_buckets(df)?;

    // Compute and display statistics
    compute_and_display_stats(&df, run_time_secs, "", per_client, per_endpoint)?;

    Ok((df, Some(effective_start_ns), Some(end_ns)))
}

/// Read a TSV or CSV file (optionally zstd compressed) into a DataFrame
fn read_tsv_file(file_path: &str) -> Result<DataFrame> {
    let path = Path::new(file_path);

    // Check if file exists
    if !path.exists() {
        anyhow::bail!("File not found: {}", file_path);
    }

    if !path.is_file() {
        anyhow::bail!("Not a file: {}", file_path);
    }

    // Detect separator by reading first line of the file
    let separator = detect_separator(file_path)
        .with_context(|| format!("Failed to detect separator in file: {}", file_path))?;

    let parse_options = CsvParseOptions::default()
        .with_separator(separator)
        .with_try_parse_dates(false)
        .with_missing_is_null(true)
        .with_truncate_ragged_lines(true);

    let read_options = CsvReadOptions::default()
        .with_parse_options(parse_options)
        .with_ignore_errors(true)
        .with_has_header(true);

    let df = read_options
        .try_into_reader_with_file_path(Some(path.to_path_buf()))?
        .finish()
        .with_context(|| {
            format!(
                "Failed to read file '{}'. Ensure it's a valid CSV/TSV file",
                file_path
            )
        })?;

    // Validate the dataframe has data
    if df.height() == 0 {
        anyhow::bail!("File '{}' contains no data rows", file_path);
    }

    // Validate required columns exist
    let required_columns = ["start", "end", "op", "bytes", "duration_ns"];
    let column_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let missing: Vec<&str> = required_columns
        .iter()
        .filter(|col| !column_names.contains(&col.to_string()))
        .copied()
        .collect();

    if !missing.is_empty() {
        anyhow::bail!(
            "File '{}' is missing required columns: {}",
            file_path,
            missing.join(", ")
        );
    }

    Ok(df)
}

/// Parse ISO 8601 timestamps and convert to nanoseconds since epoch
fn parse_timestamps(df: DataFrame) -> Result<DataFrame> {
    // Convert timestamp strings to nanoseconds since epoch
    // The timestamps are in format: 2025-12-02T23:16:43.054463723Z

    let result = df.lazy()
        .with_columns([
            // Parse start timestamp - replace Z with +00:00 for proper parsing
            col("start")
                .str().replace(lit("Z$"), lit("+00:00"), false)
                .str().to_datetime(
                    Some(TimeUnit::Nanoseconds),
                    None,
                    StrptimeOptions {
                        format: Some("%Y-%m-%dT%H:%M:%S%.f%z".into()),
                        strict: false,
                        exact: false,
                        cache: true,
                    },
                    lit("raise"),
                )
                .alias("start_dt"),
            // Parse end timestamp
            col("end")
                .str().replace(lit("Z$"), lit("+00:00"), false)
                .str().to_datetime(
                    Some(TimeUnit::Nanoseconds),
                    None,
                    StrptimeOptions {
                        format: Some("%Y-%m-%dT%H:%M:%S%.f%z".into()),
                        strict: false,
                        exact: false,
                        cache: true,
                    },
                    lit("raise"),
                )
                .alias("end_dt"),
        ])
        .with_columns([
            // Convert to nanoseconds since epoch for easier math
            col("start_dt").dt().timestamp(TimeUnit::Nanoseconds).alias("start_ns"),
            col("end_dt").dt().timestamp(TimeUnit::Nanoseconds).alias("end_ns"),
        ])
        .collect()
        .context("Failed to parse timestamps. Ensure timestamps are in ISO 8601 format (e.g., '2025-01-01T12:00:00Z')")?;

    Ok(result)
}

/// Get the time range (start and end) from the dataframe in nanoseconds
fn get_time_range(df: &DataFrame) -> Result<(i64, i64)> {
    let start_col = df.column("start_ns")?;
    let end_col = df.column("end_ns")?;

    // Get first non-null start time
    let start_ns = start_col
        .i64()?
        .into_iter()
        .flatten()
        .next()
        .context("Could not determine start time")?;

    // Get last non-null end time
    let end_ns = end_col
        .i64()?
        .into_iter()
        .flatten()
        .last()
        .context("Could not determine end time")?;

    Ok((start_ns, end_ns))
}

/// Add size bucket columns to the dataframe
fn add_size_buckets(df: DataFrame) -> Result<DataFrame> {
    let result = df
        .lazy()
        .with_columns([
            // Create bucket label (matching sai3-bench bucket boundaries)
            when(col("bytes").eq(lit(0)))
                .then(lit(BUCKET_LABELS[0])) // zero
                .when(
                    col("bytes")
                        .gt_eq(lit(1))
                        .and(col("bytes").lt(lit(BUCKET_8K))),
                )
                .then(lit(BUCKET_LABELS[1])) // 1B-8KiB
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_8K))
                        .and(col("bytes").lt(lit(BUCKET_64K))),
                )
                .then(lit(BUCKET_LABELS[2])) // 8KiB-64KiB
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_64K))
                        .and(col("bytes").lt(lit(BUCKET_512K))),
                )
                .then(lit(BUCKET_LABELS[3])) // 64KiB-512KiB
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_512K))
                        .and(col("bytes").lt(lit(BUCKET_4M))),
                )
                .then(lit(BUCKET_LABELS[4])) // 512KiB-4MiB
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_4M))
                        .and(col("bytes").lt(lit(BUCKET_32M))),
                )
                .then(lit(BUCKET_LABELS[5])) // 4MiB-32MiB
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_32M))
                        .and(col("bytes").lt(lit(BUCKET_256M))),
                )
                .then(lit(BUCKET_LABELS[6])) // 32MiB-256MiB
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_256M))
                        .and(col("bytes").lt(lit(BUCKET_2G))),
                )
                .then(lit(BUCKET_LABELS[7])) // 256MiB-2GiB
                .otherwise(lit(BUCKET_LABELS[8])) // >2GiB
                .alias("bytes_bucket"),
            // Create bucket number for sorting
            when(col("bytes").eq(lit(0)))
                .then(lit(0i32))
                .when(
                    col("bytes")
                        .gt_eq(lit(1))
                        .and(col("bytes").lt(lit(BUCKET_8K))),
                )
                .then(lit(1i32))
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_8K))
                        .and(col("bytes").lt(lit(BUCKET_64K))),
                )
                .then(lit(2i32))
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_64K))
                        .and(col("bytes").lt(lit(BUCKET_512K))),
                )
                .then(lit(3i32))
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_512K))
                        .and(col("bytes").lt(lit(BUCKET_4M))),
                )
                .then(lit(4i32))
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_4M))
                        .and(col("bytes").lt(lit(BUCKET_32M))),
                )
                .then(lit(5i32))
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_32M))
                        .and(col("bytes").lt(lit(BUCKET_256M))),
                )
                .then(lit(6i32))
                .when(
                    col("bytes")
                        .gt_eq(lit(BUCKET_256M))
                        .and(col("bytes").lt(lit(BUCKET_2G))),
                )
                .then(lit(7i32))
                .otherwise(lit(8i32))
                .alias("bucket_num"),
        ])
        .collect()?;

    Ok(result)
}

/// Compute the effective run time in seconds for a specific set of operations.
/// Uses min(start_ns) to max(end_ns) for that operation subset, which correctly
/// handles non-overlapping workloads (e.g., sequential PUT then GET phases).
/// Returns 0.0 if no matching operations are found.
fn compute_op_run_time(df: &DataFrame, ops: &[&str]) -> Result<f64> {
    let mut filter_expr = lit(false);
    for op in ops {
        filter_expr = filter_expr.or(col("op").eq(lit(*op)));
    }

    let filtered = df
        .clone()
        .lazy()
        .filter(filter_expr)
        .select([
            col("start_ns").min().alias("min_start"),
            col("end_ns").max().alias("max_end"),
        ])
        .collect()?;

    if filtered.height() == 0 {
        return Ok(0.0);
    }

    let min_start = filtered.column("min_start")?.i64()?.get(0);
    let max_end = filtered.column("max_end")?.i64()?.get(0);

    match (min_start, max_end) {
        (Some(s), Some(e)) if e > s => Ok((e - s) as f64 / 1_000_000_000.0),
        _ => Ok(0.0),
    }
}

/// Compute and display performance statistics
fn compute_and_display_stats(
    df: &DataFrame,
    run_time_secs: f64,
    title: &str,
    per_client: bool,
    per_endpoint: bool,
) -> Result<()> {
    // Pre-compute per-operation time ranges to correctly handle non-overlapping workloads
    // (e.g., when PUT and GET phases run sequentially, not concurrently - issue #14)
    let meta_time = compute_op_run_time(df, &META_OPS).unwrap_or(run_time_secs);
    let get_time = compute_op_run_time(df, &["GET"]).unwrap_or(run_time_secs);
    let put_time = compute_op_run_time(df, &["PUT"]).unwrap_or(run_time_secs);

    // Map op name to its effective run time (fall back to global if zero)
    let op_eff_time = |op: &str| -> f64 {
        let t = if META_OPS.contains(&op) {
            meta_time
        } else if op == "GET" {
            get_time
        } else if op == "PUT" {
            put_time
        } else {
            run_time_secs
        };
        if t > 0.0 {
            t
        } else {
            run_time_secs
        }
    };

    // Check if thread column exists for concurrency reporting (issue #16)
    let has_thread = df.column("thread").is_ok();

    // Build group_by aggregation expressions
    let mut agg_exprs: Vec<Expr> = vec![
        // Latency statistics (convert ns to µs)
        (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
        (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
        (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0))
            .alias("p90_lat_us"),
        (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0))
            .alias("p95_lat_us"),
        (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
            .alias("p99_lat_us"),
        (col("duration_ns").max() / lit(1000.0)).alias("max_lat_us"),
        // Size statistics
        (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
        // Raw count and bytes_sum — rates computed per-row using per-op time
        col("op").count().alias("count"),
        col("bytes")
            .sum()
            .cast(DataType::Float64)
            .alias("bytes_sum"),
    ];
    if has_thread {
        agg_exprs.push(col("thread").n_unique().alias("max_threads"));
    }

    let stats = df
        .clone()
        .lazy()
        .group_by([col("op"), col("bytes_bucket"), col("bucket_num")])
        .agg(agg_exprs)
        .sort(["bucket_num", "op"], SortMultipleOptions::default())
        .collect()?;

    // Print the results
    if !title.is_empty() {
        println!("\n{}", title);
    }

    // Print header
    println!("{:>8} {:>12} {:>8} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9} {:>11} {:>9}",
             "op", "bytes_bucket", "bucket_#",
             "mean_lat_us", "med._lat_us", "90%_lat_us",
             "95%_lat_us", "99%_lat_us", "max_lat_us", "avg_obj_KB", "ops_/_sec", "xput_MBps", "count", "max_threads", "runtime_s");

    // Extract columns
    let op_col = stats.column("op")?.str()?;
    let bucket_col = stats.column("bytes_bucket")?.str()?;
    let bucket_num_col = stats.column("bucket_num")?.i32()?;
    let mean_lat = stats.column("mean_lat_us")?.f64()?;
    let med_lat = stats.column("med_lat_us")?.f64()?;
    let p90_lat = stats.column("p90_lat_us")?.f64()?;
    let p95_lat = stats.column("p95_lat_us")?.f64()?;
    let p99_lat = stats.column("p99_lat_us")?.f64()?;
    let max_lat = stats.column("max_lat_us")?.f64()?;
    let avg_kb = stats.column("avg_obj_KB")?.f64()?;
    let count_col = stats.column("count")?.u32()?;
    let bytes_sum_col = stats.column("bytes_sum")?.f64()?;
    let concurrency_col = if has_thread {
        Some(stats.column("max_threads")?.u32()?)
    } else {
        None
    };

    for i in 0..stats.height() {
        let op = op_col.get(i).unwrap_or("?");
        let bucket = bucket_col.get(i).unwrap_or("?");
        let bucket_num = bucket_num_col.get(i).unwrap_or(0);
        let mean = mean_lat.get(i).unwrap_or(0.0);
        let med = med_lat.get(i).unwrap_or(0.0);
        let p90 = p90_lat.get(i).unwrap_or(0.0);
        let p95 = p95_lat.get(i).unwrap_or(0.0);
        let p99 = p99_lat.get(i).unwrap_or(0.0);
        let max = max_lat.get(i).unwrap_or(0.0);
        let avg = avg_kb.get(i).unwrap_or(0.0);
        let cnt = count_col.get(i).unwrap_or(0);
        let bsum = bytes_sum_col.get(i).unwrap_or(0.0);
        let conc = concurrency_col
            .as_ref()
            .map_or(0, |c| c.get(i).unwrap_or(0));

        // Skip rows with zero count (empty buckets or invalid data)
        if cnt == 0 {
            continue;
        }

        // Compute throughput using per-op time range (fix for non-overlapping workloads, issue #14)
        let eff_time = op_eff_time(op);
        let ops = cnt as f64 / eff_time;
        let xp = bsum / (1024.0 * 1024.0 * eff_time);

        println!("{:>8} {:>12} {:>8} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9} {:>11} {:>9}",
                 op, bucket, bucket_num,
                 format_with_commas(mean),
                 format_with_commas(med),
                 format_with_commas(p90),
                 format_with_commas(p95),
                 format_with_commas(p99),
                 format_with_commas(max),
                 format_with_commas(avg),
                 format_with_commas(ops),
                 format_with_commas(xp),
                 format_int_with_commas(cnt as i64),
                 format_int_with_commas(conc as i64),
                 format!("{:.1}", eff_time));
    }

    // Print summary rows for each operation category (META, GET, PUT)
    // These use statistically valid percentiles from ALL raw data for that op category
    println!(); // Separator line before summaries

    compute_and_print_summary_row(df, run_time_secs, meta_time, "META", &META_OPS, 97)?;
    compute_and_print_summary_row(df, run_time_secs, get_time, "GET", &["GET"], 98)?;
    compute_and_print_summary_row(df, run_time_secs, put_time, "PUT", &["PUT"], 99)?;

    // Print grand total
    let total_ops: u64 = count_col.into_iter().flatten().map(|x| x as u64).sum();
    let total_ops_sec: f64 = total_ops as f64 / run_time_secs;
    println!(
        "\nTotal operations: {}  ({:.2}/sec)",
        format_int_with_commas(total_ops as i64),
        total_ops_sec
    );

    // Print per-client statistics if requested
    if per_client {
        compute_and_print_per_client_stats(df, run_time_secs)?;
    }

    // Print per-endpoint statistics if requested (issue #15)
    if per_endpoint {
        compute_and_print_per_endpoint_stats(df, run_time_secs)?;
    }

    Ok(())
}

/// Compute and print per-client statistics to show variation across clients
fn compute_and_print_per_client_stats(df: &DataFrame, run_time_secs: f64) -> Result<()> {
    // Check if client_id column exists
    if df.column("client_id").is_err() {
        println!("\nWarning: client_id column not found, skipping per-client statistics.");
        return Ok(());
    }

    // Get unique client_ids
    let unique_clients = df.column("client_id")?.unique()?;
    let client_ids = unique_clients
        .str()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    if client_ids.len() <= 1 {
        println!("\nOnly one client detected, skipping per-client statistics.");
        return Ok(());
    }

    println!("\n{}", "=".repeat(80));
    println!(
        "Per-Client Statistics ({} clients detected)",
        client_ids.len()
    );
    println!("{}", "=".repeat(80));

    // Compute overall stats per client
    let client_stats = df
        .clone()
        .lazy()
        .group_by([col("client_id")])
        .agg([
            (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
            (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
            (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0))
                .alias("p90_lat_us"),
            (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0))
                .alias("p95_lat_us"),
            (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                .alias("p99_lat_us"),
            (col("duration_ns").max() / lit(1000.0)).alias("max_lat_us"),
            (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
            (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops_per_sec"),
            ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                / lit(run_time_secs))
            .alias("xput_MBps"),
            col("op").count().alias("count"),
        ])
        .sort(["client_id"], SortMultipleOptions::default())
        .collect()?;

    // Print header
    println!(
        "{:>15} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9}",
        "client_id",
        "mean_lat_us",
        "med._lat_us",
        "90%_lat_us",
        "95%_lat_us",
        "99%_lat_us",
        "max_lat_us",
        "avg_obj_KB",
        "ops_/_sec",
        "xput_MBps",
        "count"
    );

    // Print each client
    let client_col = client_stats.column("client_id")?.str()?;
    let mean_lat = client_stats.column("mean_lat_us")?.f64()?;
    let med_lat = client_stats.column("med_lat_us")?.f64()?;
    let p90_lat = client_stats.column("p90_lat_us")?.f64()?;
    let p95_lat = client_stats.column("p95_lat_us")?.f64()?;
    let p99_lat = client_stats.column("p99_lat_us")?.f64()?;
    let max_lat = client_stats.column("max_lat_us")?.f64()?;
    let avg_kb = client_stats.column("avg_obj_KB")?.f64()?;
    let ops_sec = client_stats.column("ops_per_sec")?.f64()?;
    let xput = client_stats.column("xput_MBps")?.f64()?;
    let count_col = client_stats.column("count")?.u32()?;

    for i in 0..client_stats.height() {
        let client = client_col.get(i).unwrap_or("?");
        let mean = mean_lat.get(i).unwrap_or(0.0);
        let med = med_lat.get(i).unwrap_or(0.0);
        let p90 = p90_lat.get(i).unwrap_or(0.0);
        let p95 = p95_lat.get(i).unwrap_or(0.0);
        let p99 = p99_lat.get(i).unwrap_or(0.0);
        let max = max_lat.get(i).unwrap_or(0.0);
        let avg = avg_kb.get(i).unwrap_or(0.0);
        let ops = ops_sec.get(i).unwrap_or(0.0);
        let xp = xput.get(i).unwrap_or(0.0);
        let cnt = count_col.get(i).unwrap_or(0);

        println!(
            "{:>15} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9}",
            client,
            format_with_commas(mean),
            format_with_commas(med),
            format_with_commas(p90),
            format_with_commas(p95),
            format_with_commas(p99),
            format_with_commas(max),
            format_with_commas(avg),
            format_with_commas(ops),
            format_with_commas(xp),
            format_int_with_commas(cnt as i64)
        );
    }

    // Print per-client stats by operation type
    println!("\nPer-Client Statistics by Operation Type:");
    println!("{}", "-".repeat(80));

    // Define operation categories
    let categories = vec![
        ("META", META_OPS.to_vec()),
        ("GET", vec!["GET"]),
        ("PUT", vec!["PUT"]),
    ];

    for (op_name, ops_list) in categories {
        // Build filter for this operation category
        let mut filter_expr = lit(false);
        for op in &ops_list {
            filter_expr = filter_expr.or(col("op").eq(lit(*op)));
        }

        let op_client_stats = df
            .clone()
            .lazy()
            .filter(filter_expr)
            .group_by([col("client_id")])
            .agg([
                (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
                (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
                (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                    .alias("p99_lat_us"),
                (col("op").count().cast(DataType::Float64) / lit(run_time_secs))
                    .alias("ops_per_sec"),
                ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                    / lit(run_time_secs))
                .alias("xput_MBps"),
                col("op").count().alias("count"),
            ])
            .sort(["client_id"], SortMultipleOptions::default())
            .collect()?;

        if op_client_stats.height() == 0 {
            continue;
        }

        println!("\n{} Operations:", op_name);
        println!(
            "{:>15} {:>11} {:>11} {:>10} {:>9} {:>9} {:>9}",
            "client_id",
            "mean_lat_us",
            "med._lat_us",
            "99%_lat_us",
            "ops_/_sec",
            "xput_MBps",
            "count"
        );

        let client_col = op_client_stats.column("client_id")?.str()?;
        let mean_lat = op_client_stats.column("mean_lat_us")?.f64()?;
        let med_lat = op_client_stats.column("med_lat_us")?.f64()?;
        let p99_lat = op_client_stats.column("p99_lat_us")?.f64()?;
        let ops_sec = op_client_stats.column("ops_per_sec")?.f64()?;
        let xput = op_client_stats.column("xput_MBps")?.f64()?;
        let count_col = op_client_stats.column("count")?.u32()?;

        for i in 0..op_client_stats.height() {
            let client = client_col.get(i).unwrap_or("?");
            let mean = mean_lat.get(i).unwrap_or(0.0);
            let med = med_lat.get(i).unwrap_or(0.0);
            let p99 = p99_lat.get(i).unwrap_or(0.0);
            let ops = ops_sec.get(i).unwrap_or(0.0);
            let xp = xput.get(i).unwrap_or(0.0);
            let cnt = count_col.get(i).unwrap_or(0);

            println!(
                "{:>15} {:>11} {:>11} {:>10} {:>9} {:>9} {:>9}",
                client,
                format_with_commas(mean),
                format_with_commas(med),
                format_with_commas(p99),
                format_with_commas(ops),
                format_with_commas(xp),
                format_int_with_commas(cnt as i64)
            );
        }
    }

    println!("\n{}\n", "=".repeat(80));

    Ok(())
}

/// Compute and print per-endpoint statistics to show variation across endpoints (issue #15)
fn compute_and_print_per_endpoint_stats(df: &DataFrame, run_time_secs: f64) -> Result<()> {
    // Check if endpoint column exists
    if df.column("endpoint").is_err() {
        println!("\nWarning: endpoint column not found, skipping per-endpoint statistics.");
        return Ok(());
    }

    // Get unique endpoints
    let unique_endpoints = df.column("endpoint")?.unique()?;
    let endpoints = unique_endpoints
        .str()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    if endpoints.len() <= 1 {
        println!("\nOnly one endpoint detected, skipping per-endpoint statistics.");
        return Ok(());
    }

    println!("\n{}", "=".repeat(80));
    println!(
        "Per-Endpoint Statistics ({} endpoints detected)",
        endpoints.len()
    );
    println!("{}", "=".repeat(80));

    // Compute overall stats per endpoint
    let endpoint_stats = df
        .clone()
        .lazy()
        .group_by([col("endpoint")])
        .agg([
            (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
            (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
            (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0))
                .alias("p90_lat_us"),
            (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0))
                .alias("p95_lat_us"),
            (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                .alias("p99_lat_us"),
            (col("duration_ns").max() / lit(1000.0)).alias("max_lat_us"),
            (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
            (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops_per_sec"),
            ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                / lit(run_time_secs))
            .alias("xput_MBps"),
            col("op").count().alias("count"),
        ])
        .sort(["endpoint"], SortMultipleOptions::default())
        .collect()?;

    // Print header
    println!(
        "{:>30} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9}",
        "endpoint",
        "mean_lat_us",
        "med._lat_us",
        "90%_lat_us",
        "95%_lat_us",
        "99%_lat_us",
        "max_lat_us",
        "avg_obj_KB",
        "ops_/_sec",
        "xput_MBps",
        "count"
    );

    let ep_col = endpoint_stats.column("endpoint")?.str()?;
    let mean_lat = endpoint_stats.column("mean_lat_us")?.f64()?;
    let med_lat = endpoint_stats.column("med_lat_us")?.f64()?;
    let p90_lat = endpoint_stats.column("p90_lat_us")?.f64()?;
    let p95_lat = endpoint_stats.column("p95_lat_us")?.f64()?;
    let p99_lat = endpoint_stats.column("p99_lat_us")?.f64()?;
    let max_lat = endpoint_stats.column("max_lat_us")?.f64()?;
    let avg_kb = endpoint_stats.column("avg_obj_KB")?.f64()?;
    let ops_sec = endpoint_stats.column("ops_per_sec")?.f64()?;
    let xput = endpoint_stats.column("xput_MBps")?.f64()?;
    let count_col = endpoint_stats.column("count")?.u32()?;

    for i in 0..endpoint_stats.height() {
        let ep = ep_col.get(i).unwrap_or("?");
        let mean = mean_lat.get(i).unwrap_or(0.0);
        let med = med_lat.get(i).unwrap_or(0.0);
        let p90 = p90_lat.get(i).unwrap_or(0.0);
        let p95 = p95_lat.get(i).unwrap_or(0.0);
        let p99 = p99_lat.get(i).unwrap_or(0.0);
        let max = max_lat.get(i).unwrap_or(0.0);
        let avg = avg_kb.get(i).unwrap_or(0.0);
        let ops = ops_sec.get(i).unwrap_or(0.0);
        let xp = xput.get(i).unwrap_or(0.0);
        let cnt = count_col.get(i).unwrap_or(0);

        // Skip null/unknown endpoints and zero-count rows
        if cnt == 0 || ep == "?" {
            continue;
        }

        println!(
            "{:>30} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9}",
            ep,
            format_with_commas(mean),
            format_with_commas(med),
            format_with_commas(p90),
            format_with_commas(p95),
            format_with_commas(p99),
            format_with_commas(max),
            format_with_commas(avg),
            format_with_commas(ops),
            format_with_commas(xp),
            format_int_with_commas(cnt as i64)
        );
    }

    // Print per-endpoint stats by operation type
    println!("\nPer-Endpoint Statistics by Operation Type:");
    println!("{}", "-".repeat(80));

    let categories = vec![
        ("META", META_OPS.to_vec()),
        ("GET", vec!["GET"]),
        ("PUT", vec!["PUT"]),
    ];

    for (op_name, ops_list) in categories {
        let mut filter_expr = lit(false);
        for op in &ops_list {
            filter_expr = filter_expr.or(col("op").eq(lit(*op)));
        }

        let op_ep_stats = df
            .clone()
            .lazy()
            .filter(filter_expr)
            .group_by([col("endpoint")])
            .agg([
                (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
                (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
                (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                    .alias("p99_lat_us"),
                (col("op").count().cast(DataType::Float64) / lit(run_time_secs))
                    .alias("ops_per_sec"),
                ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                    / lit(run_time_secs))
                .alias("xput_MBps"),
                col("op").count().alias("count"),
            ])
            .sort(["endpoint"], SortMultipleOptions::default())
            .collect()?;

        if op_ep_stats.height() == 0 {
            continue;
        }

        println!("\n{} Operations:", op_name);
        println!(
            "{:>30} {:>11} {:>11} {:>10} {:>9} {:>9} {:>9}",
            "endpoint",
            "mean_lat_us",
            "med._lat_us",
            "99%_lat_us",
            "ops_/_sec",
            "xput_MBps",
            "count"
        );

        let ep_col = op_ep_stats.column("endpoint")?.str()?;
        let mean_lat = op_ep_stats.column("mean_lat_us")?.f64()?;
        let med_lat = op_ep_stats.column("med_lat_us")?.f64()?;
        let p99_lat = op_ep_stats.column("p99_lat_us")?.f64()?;
        let ops_sec = op_ep_stats.column("ops_per_sec")?.f64()?;
        let xput = op_ep_stats.column("xput_MBps")?.f64()?;
        let count_col = op_ep_stats.column("count")?.u32()?;

        for i in 0..op_ep_stats.height() {
            let ep = ep_col.get(i).unwrap_or("?");
            let mean = mean_lat.get(i).unwrap_or(0.0);
            let med = med_lat.get(i).unwrap_or(0.0);
            let p99 = p99_lat.get(i).unwrap_or(0.0);
            let ops = ops_sec.get(i).unwrap_or(0.0);
            let xp = xput.get(i).unwrap_or(0.0);
            let cnt = count_col.get(i).unwrap_or(0);

            // Skip null/unknown endpoints and zero-count rows
            if cnt == 0 || ep == "?" {
                continue;
            }

            println!(
                "{:>30} {:>11} {:>11} {:>10} {:>9} {:>9} {:>9}",
                ep,
                format_with_commas(mean),
                format_with_commas(med),
                format_with_commas(p99),
                format_with_commas(ops),
                format_with_commas(xp),
                format_int_with_commas(cnt as i64)
            );
        }
    }

    println!("\n{}\n", "=".repeat(80));

    Ok(())
}

/// Compute and print a summary row for a category of operations.
/// Uses op_run_time for throughput (not the global run_time_secs) to correctly
/// handle non-overlapping workloads (issue #14). Includes concurrency (issue #16).
fn compute_and_print_summary_row(
    df: &DataFrame,
    run_time_secs: f64,
    op_run_time: f64,
    category: &str,
    ops: &[&str],
    bucket_idx: i32,
) -> Result<()> {
    // Build filter for matching operations
    let mut filter_expr = lit(false);
    for op in ops {
        filter_expr = filter_expr.or(col("op").eq(lit(*op)));
    }

    // Check if thread column exists for concurrency reporting (issue #16)
    let has_thread = df.column("thread").is_ok();

    // Build select expressions; compute raw counts/bytes — rates applied post-collect
    let mut select_exprs: Vec<Expr> = vec![
        (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
        (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
        (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0))
            .alias("p90_lat_us"),
        (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0))
            .alias("p95_lat_us"),
        (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
            .alias("p99_lat_us"),
        (col("duration_ns").max() / lit(1000.0)).alias("max_lat_us"),
        (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
        col("op").count().alias("count"),
        col("bytes")
            .sum()
            .cast(DataType::Float64)
            .alias("bytes_sum"),
    ];
    if has_thread {
        select_exprs.push(col("thread").n_unique().alias("max_threads"));
    }

    let category_stats = df
        .clone()
        .lazy()
        .filter(filter_expr)
        .select(select_exprs)
        .collect()?;

    // Check if we have any data for this category
    if category_stats.height() == 0 {
        return Ok(());
    }

    let count = category_stats.column("count")?.u32()?.get(0).unwrap_or(0);
    if count == 0 {
        return Ok(());
    }

    // Use per-op time for throughput (issue #14 fix: correct for non-overlapping workloads)
    let eff_time = if op_run_time > 0.0 {
        op_run_time
    } else {
        run_time_secs
    };

    let mean = category_stats
        .column("mean_lat_us")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let med = category_stats
        .column("med_lat_us")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let p90 = category_stats
        .column("p90_lat_us")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let p95 = category_stats
        .column("p95_lat_us")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let p99 = category_stats
        .column("p99_lat_us")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let max = category_stats
        .column("max_lat_us")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let avg = category_stats
        .column("avg_obj_KB")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let bsum = category_stats
        .column("bytes_sum")?
        .f64()?
        .get(0)
        .unwrap_or(0.0);
    let n_thr: u32 = if has_thread {
        category_stats
            .column("max_threads")?
            .u32()?
            .get(0)
            .unwrap_or(0)
    } else {
        0
    };

    let ops = count as f64 / eff_time;
    let xp = bsum / (1024.0 * 1024.0 * eff_time);

    println!("{:>8} {:>12} {:>8} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9} {:>11} {:>9}",
             category, "ALL", bucket_idx,
             format_with_commas(mean),
             format_with_commas(med),
             format_with_commas(p90),
             format_with_commas(p95),
             format_with_commas(p99),
             format_with_commas(max),
             format_with_commas(avg),
             format_with_commas(ops),
             format_with_commas(xp),
             format_int_with_commas(count as i64),
             format_int_with_commas(n_thr as i64),
             format!("{:.1}", eff_time));

    Ok(())
}

/// Print basic stats about the dataframe (used with --basic-stats flag)
fn print_basic_stats(df: &DataFrame) {
    println!("Shape: {} rows × {} columns", df.height(), df.width());

    // Print column names and types
    println!("\nColumns:");
    for name in df.get_column_names() {
        let dtype = df
            .column(name)
            .map(|s| s.dtype().clone())
            .unwrap_or(DataType::Null);
        println!("  - {}: {}", name, dtype);
    }

    // Print first few rows
    println!("\nSample data (first 5 rows):");
    let sample = df.head(Some(5));
    println!("{}", sample);
}

// ─────────────────────────── Summary file support ───────────────────────────

/// Read an aggregated summary file (.summary.tsv.zst) into a DataFrame.
/// Validates required columns, parses start/end timestamps to start_ns/end_ns.
fn read_summary_file(file_path: &str) -> Result<DataFrame> {
    let path = Path::new(file_path);
    if !path.exists() {
        anyhow::bail!("File not found: {}", file_path);
    }

    let parse_options = CsvParseOptions::default()
        .with_separator(b'\t')
        .with_try_parse_dates(false)
        .with_missing_is_null(true)
        .with_comment_prefix::<&str>(Some("#"))
        .with_truncate_ragged_lines(true);

    let read_options = CsvReadOptions::default()
        .with_parse_options(parse_options)
        .with_ignore_errors(true)
        .with_has_header(true);

    let df = read_options
        .try_into_reader_with_file_path(Some(path.to_path_buf()))?
        .finish()
        .with_context(|| format!("Failed to read summary file '{}'", file_path))?;

    if df.height() == 0 {
        anyhow::bail!("Summary file '{}' contains no data rows", file_path);
    }

    let required = ["op", "start", "end", "bps", "ops_per_sec", "errors"];
    let cols: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let missing: Vec<&str> = required
        .iter()
        .filter(|c| !cols.contains(&c.to_string()))
        .copied()
        .collect();
    if !missing.is_empty() {
        anyhow::bail!(
            "Summary file '{}' is missing required columns: {}",
            file_path,
            missing.join(", ")
        );
    }

    // Parse start/end to nanosecond timestamps for time-range computation
    let df = df
        .lazy()
        .with_columns([
            col("start")
                .str()
                .replace(lit("Z$"), lit("+00:00"), false)
                .str()
                .to_datetime(
                    Some(TimeUnit::Nanoseconds),
                    None,
                    StrptimeOptions {
                        format: Some("%Y-%m-%dT%H:%M:%S%.f%z".into()),
                        strict: false,
                        exact: false,
                        cache: true,
                    },
                    lit("raise"),
                )
                .dt()
                .timestamp(TimeUnit::Nanoseconds)
                .alias("start_ns"),
            col("end")
                .str()
                .replace(lit("Z$"), lit("+00:00"), false)
                .str()
                .to_datetime(
                    Some(TimeUnit::Nanoseconds),
                    None,
                    StrptimeOptions {
                        format: Some("%Y-%m-%dT%H:%M:%S%.f%z".into()),
                        strict: false,
                        exact: false,
                        cache: true,
                    },
                    lit("raise"),
                )
                .dt()
                .timestamp(TimeUnit::Nanoseconds)
                .alias("end_ns"),
            // Ensure bps is f64 (may be read as i64 if all values are whole numbers)
            col("bps").cast(DataType::Float64),
            col("ops_per_sec").cast(DataType::Float64),
        ])
        .collect()?;

    Ok(df)
}

/// Process a single summary file: read, display stats, return (df, start_ns, end_ns).
fn process_summary_file(file_path: &str) -> Result<(DataFrame, i64, i64)> {
    let df = read_summary_file(file_path)?;

    // Derive time range from start_ns / end_ns columns
    let start_ns = df
        .column("start_ns")?
        .i64()?
        .into_iter()
        .flatten()
        .next()
        .context("Could not determine start time from summary file")?;
    let end_ns = df
        .column("end_ns")?
        .i64()?
        .into_iter()
        .flatten()
        .last()
        .context("Could not determine end time from summary file")?;

    let seg_count = df.height();
    let duration_secs = (end_ns - start_ns) as f64 / 1_000_000_000.0;
    println!(
        "Time range: {} seconds  ({} segments)",
        duration_secs as i64, seg_count
    );

    compute_and_display_summary_stats(&df)?;

    Ok((df, start_ns, end_ns))
}

/// Compute and display per-op throughput variability statistics from a summary DataFrame.
/// Columns: op, bps, ops_per_sec, errors  (plus start_ns/end_ns)
fn compute_and_display_summary_stats(df: &DataFrame) -> Result<()> {
    let stats = df
        .clone()
        .lazy()
        .group_by([col("op")])
        .agg([
            col("bps").count().alias("segments"),
            (col("bps").mean() / lit(1_048_576.0)).alias("mean_MBps"),
            (col("bps").median() / lit(1_048_576.0)).alias("p50_MBps"),
            (col("bps").quantile(lit(0.90), QuantileMethod::Linear) / lit(1_048_576.0))
                .alias("p90_MBps"),
            (col("bps").quantile(lit(0.99), QuantileMethod::Linear) / lit(1_048_576.0))
                .alias("p99_MBps"),
            (col("bps").min() / lit(1_048_576.0)).alias("min_MBps"),
            (col("bps").max() / lit(1_048_576.0)).alias("max_MBps"),
            (col("bps").std(1) / lit(1_048_576.0)).alias("stdev_MBps"),
            col("ops_per_sec").mean().alias("mean_ops"),
            col("ops_per_sec").median().alias("p50_ops"),
            col("ops_per_sec")
                .quantile(lit(0.99), QuantileMethod::Linear)
                .alias("p99_ops"),
            col("errors")
                .cast(DataType::Int64)
                .sum()
                .alias("total_errors"),
        ])
        // Sort: TOTAL last, everything else alphabetical
        .sort(["op"], SortMultipleOptions::default())
        .collect()?;

    if stats.height() == 0 {
        println!("  (no data)");
        return Ok(());
    }

    println!(
        "{:>8} {:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>13}",
        "op",
        "segs",
        "mean_MBps",
        "p50_MBps",
        "p90_MBps",
        "p99_MBps",
        "min_MBps",
        "max_MBps",
        "stdev_MBps",
        "mean_ops/s",
        "p50_ops/s",
        "p99_ops/s",
        "total_errors"
    );

    let op_col = stats.column("op")?.str()?;
    let segs_col = stats.column("segments")?.u32()?;
    let mean_col = stats.column("mean_MBps")?.f64()?;
    let p50_col = stats.column("p50_MBps")?.f64()?;
    let p90_col = stats.column("p90_MBps")?.f64()?;
    let p99_col = stats.column("p99_MBps")?.f64()?;
    let min_col = stats.column("min_MBps")?.f64()?;
    let max_col = stats.column("max_MBps")?.f64()?;
    let stdev_col = stats.column("stdev_MBps")?.f64()?;
    let mops_col = stats.column("mean_ops")?.f64()?;
    let p50ops_col = stats.column("p50_ops")?.f64()?;
    let p99ops_col = stats.column("p99_ops")?.f64()?;
    let errs_col = stats.column("total_errors")?.i64()?;

    // Print non-TOTAL rows first, then TOTAL
    let height = stats.height();
    let print_row = |i: usize| {
        let op = op_col.get(i).unwrap_or("?");
        let segs = segs_col.get(i).unwrap_or(0);
        let mean = mean_col.get(i).unwrap_or(0.0);
        let p50 = p50_col.get(i).unwrap_or(0.0);
        let p90 = p90_col.get(i).unwrap_or(0.0);
        let p99 = p99_col.get(i).unwrap_or(0.0);
        let min = min_col.get(i).unwrap_or(0.0);
        let max = max_col.get(i).unwrap_or(0.0);
        let std = stdev_col.get(i).unwrap_or(0.0);
        let mops = mops_col.get(i).unwrap_or(0.0);
        let p50op = p50ops_col.get(i).unwrap_or(0.0);
        let p99op = p99ops_col.get(i).unwrap_or(0.0);
        let errs = errs_col.get(i).unwrap_or(0);
        println!(
            "{:>8} {:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>13}",
            op, segs,
            format_with_commas(mean), format_with_commas(p50),
            format_with_commas(p90),  format_with_commas(p99),
            format_with_commas(min),  format_with_commas(max),
            format_with_commas(std),
            format_with_commas(mops), format_with_commas(p50op), format_with_commas(p99op),
            format_int_with_commas(errs),
        );
    };

    for i in 0..height {
        if op_col.get(i).unwrap_or("") != "TOTAL" {
            print_row(i);
        }
    }
    // Print TOTAL row last
    for i in 0..height {
        if op_col.get(i).unwrap_or("") == "TOTAL" {
            print_row(i);
        }
    }

    Ok(())
}

/// Collect summary stats rows for Excel export (header + data rows, no commas in numbers).
fn collect_summary_excel_rows(df: &DataFrame) -> Result<Vec<Vec<String>>> {
    let stats = df
        .clone()
        .lazy()
        .group_by([col("op")])
        .agg([
            col("bps").count().alias("segments"),
            (col("bps").mean() / lit(1_048_576.0)).alias("mean_MBps"),
            (col("bps").median() / lit(1_048_576.0)).alias("p50_MBps"),
            (col("bps").quantile(lit(0.90), QuantileMethod::Linear) / lit(1_048_576.0))
                .alias("p90_MBps"),
            (col("bps").quantile(lit(0.99), QuantileMethod::Linear) / lit(1_048_576.0))
                .alias("p99_MBps"),
            (col("bps").min() / lit(1_048_576.0)).alias("min_MBps"),
            (col("bps").max() / lit(1_048_576.0)).alias("max_MBps"),
            (col("bps").std(1) / lit(1_048_576.0)).alias("stdev_MBps"),
            col("ops_per_sec").mean().alias("mean_ops"),
            col("ops_per_sec").median().alias("p50_ops"),
            col("ops_per_sec")
                .quantile(lit(0.99), QuantileMethod::Linear)
                .alias("p99_ops"),
            col("errors")
                .cast(DataType::Int64)
                .sum()
                .alias("total_errors"),
        ])
        .sort(["op"], SortMultipleOptions::default())
        .collect()?;

    let mut rows: Vec<Vec<String>> = vec![vec![
        "op".into(),
        "segments".into(),
        "mean_MBps".into(),
        "p50_MBps".into(),
        "p90_MBps".into(),
        "p99_MBps".into(),
        "min_MBps".into(),
        "max_MBps".into(),
        "stdev_MBps".into(),
        "mean_ops/s".into(),
        "p50_ops/s".into(),
        "p99_ops/s".into(),
        "total_errors".into(),
    ]];

    let op_col = stats.column("op")?.str()?;
    let segs_col = stats.column("segments")?.u32()?;
    let mean_col = stats.column("mean_MBps")?.f64()?;
    let p50_col = stats.column("p50_MBps")?.f64()?;
    let p90_col = stats.column("p90_MBps")?.f64()?;
    let p99_col = stats.column("p99_MBps")?.f64()?;
    let min_col = stats.column("min_MBps")?.f64()?;
    let max_col = stats.column("max_MBps")?.f64()?;
    let stdev_col = stats.column("stdev_MBps")?.f64()?;
    let mops_col = stats.column("mean_ops")?.f64()?;
    let p50ops_col = stats.column("p50_ops")?.f64()?;
    let p99ops_col = stats.column("p99_ops")?.f64()?;
    let errs_col = stats.column("total_errors")?.i64()?;

    // Non-TOTAL rows first, then TOTAL
    let height = stats.height();
    let push_row = |rows: &mut Vec<Vec<String>>, i: usize| {
        rows.push(vec![
            op_col.get(i).unwrap_or("?").to_string(),
            segs_col.get(i).unwrap_or(0).to_string(),
            format!("{:.2}", mean_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", p50_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", p90_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", p99_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", min_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", max_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", stdev_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", mops_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", p50ops_col.get(i).unwrap_or(0.0)),
            format!("{:.2}", p99ops_col.get(i).unwrap_or(0.0)),
            errs_col.get(i).unwrap_or(0).to_string(),
        ]);
    };
    for i in 0..height {
        if op_col.get(i).unwrap_or("") != "TOTAL" {
            push_row(&mut rows, i);
        }
    }
    for i in 0..height {
        if op_col.get(i).unwrap_or("") == "TOTAL" {
            push_row(&mut rows, i);
        }
    }

    Ok(rows)
}

/// Derive a short display name from a file path for use as an Excel tab prefix.
/// Strips path, extensions (.zst, .csv, .tsv), and warp timestamp brackets.
fn derive_short_name(file_path: &str) -> String {
    let path = Path::new(file_path);
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let name = name.strip_suffix(".zst").unwrap_or(name);
    let name = name
        .strip_suffix(".csv")
        .or_else(|| name.strip_suffix(".tsv"))
        .unwrap_or(name);
    // Strip warp-style [timestamp] and everything after
    let name = if let Some(idx) = name.find('[') {
        &name[..idx]
    } else {
        name
    };
    let name = name.trim_end_matches(['-', '_', '.']);
    // Truncate to 20 chars to leave room for "-Results" / "-Detail" suffix
    if name.len() > 20 { &name[..20] } else { name }.to_string()
}

/// Derive the Excel output path when --excel is given without an explicit name.
/// Single file: same directory as input, same stem, .xlsx extension.
/// Multiple files: "polarwarp-results.xlsx" in cwd.
fn derive_excel_path(files: &[String]) -> String {
    if files.len() == 1 {
        let path = Path::new(&files[0]);
        let parent = path.parent().unwrap_or(Path::new("."));
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("output");
        let name = name.strip_suffix(".zst").unwrap_or(name);
        let name = name
            .strip_suffix(".csv")
            .or_else(|| name.strip_suffix(".tsv"))
            .unwrap_or(name);
        parent
            .join(format!("{}.xlsx", name))
            .to_string_lossy()
            .into_owned()
    } else {
        "polarwarp-results.xlsx".to_string()
    }
}

/// Build a valid Excel tab name (max 31 chars) from a base and suffix.
fn make_tab_name(base: &str, suffix: &str) -> String {
    let full = format!("{}-{}", base, suffix);
    if full.len() <= 31 {
        full
    } else {
        let max_base = 31usize.saturating_sub(suffix.len() + 1);
        format!("{}-{}", &base[..max_base.min(base.len())], suffix)
    }
}

/// Write an Excel workbook that includes both regular string-based tabs and
/// time-series chart tabs for summary files.  Replaces a bare
/// `write_excel_workbook` call when there are summary files to chart.
fn write_excel_workbook_with_chart_tabs(
    path: &str,
    tabs: &[(String, Vec<Vec<String>>)],
    chart_tabs: &[(DataFrame, String)],
) -> Result<()> {
    let mut workbook = Workbook::new();
    let header_fmt = Format::new().set_bold().set_font_name("Aptos");
    let data_fmt = Format::new().set_font_name("Aptos");
    let section_fmt = Format::new()
        .set_bold()
        .set_font_name("Aptos")
        .set_font_size(11.0);

    // Write regular (string-based) tabs — identical to write_excel_workbook
    for (tab_name, rows) in tabs {
        if rows.is_empty() {
            continue;
        }
        let ws = workbook.add_worksheet();
        ws.set_name(tab_name)?;

        for (row_idx, row) in rows.iter().enumerate() {
            let is_section =
                row.len() == 1 && (row[0].starts_with("===") || row[0].starts_with("---"));

            for (col_idx, cell) in row.iter().enumerate() {
                let fmt = if row_idx == 0 || is_section {
                    if is_section {
                        &section_fmt
                    } else {
                        &header_fmt
                    }
                } else {
                    &data_fmt
                };
                if let Ok(num) = cell.parse::<f64>() {
                    ws.write_number_with_format(row_idx as u32, col_idx as u16, num, fmt)?;
                } else {
                    ws.write_string_with_format(row_idx as u32, col_idx as u16, cell, fmt)?;
                }
            }
        }

        if let Some(header) = rows.first() {
            for col_idx in 0..header.len() {
                let max_len = rows
                    .iter()
                    .map(|r| r.get(col_idx).map_or(0, |c| c.len()))
                    .max()
                    .unwrap_or(10);
                ws.set_column_width(col_idx as u16, (max_len as f64 + 2.0).min(35.0))?;
            }
        }
    }

    // Write time-series chart tabs for summary files
    for (df, tab_name) in chart_tabs {
        write_summary_chart_tab(&mut workbook, df, tab_name)?;
    }

    workbook
        .save(path)
        .with_context(|| format!("Failed to save Excel file: {}", path))?;
    Ok(())
}

/// Write a worksheet with per-second time-series data and two embedded line charts:
///   1. Operations/sec over Time (one series per non-TOTAL op)
///   2. Throughput MiB/s over Time (GET and PUT only — META has no byte payload)
///
/// Column layout: [seconds | <op>_ops … | <op>_MBps …]
/// Charts are inserted below the data table, side by side.
fn write_summary_chart_tab(workbook: &mut Workbook, df: &DataFrame, tab_name: &str) -> Result<()> {
    // Filter TOTAL rows; sort by time then op for deterministic ordering
    let df_filt = df
        .clone()
        .lazy()
        .filter(col("op").neq(lit("TOTAL")))
        .sort(["start_ns", "op"], SortMultipleOptions::default())
        .collect()?;

    if df_filt.height() == 0 {
        return Ok(());
    }

    let op_col = df_filt.column("op")?.str()?;
    let start_col = df_filt.column("start_ns")?.i64()?;
    let bps_col = df_filt.column("bps")?.f64()?;
    let ops_col = df_filt.column("ops_per_sec")?.f64()?;

    let min_ns = start_col.min().unwrap_or(0);

    // Collect unique ops (sorted) and unique second offsets (sorted)
    let mut ops_set: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut secs_set: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for i in 0..df_filt.height() {
        ops_set.insert(op_col.get(i).unwrap_or("").to_string());
        secs_set.insert((start_col.get(i).unwrap_or(0) - min_ns) / 1_000_000_000);
    }
    let mut all_ops: Vec<String> = ops_set.into_iter().collect();
    all_ops.sort();
    let mut all_secs: Vec<i64> = secs_set.into_iter().collect();
    all_secs.sort();

    let n_rows = all_secs.len();
    let n_ops = all_ops.len();

    // Build per-(op, seconds) lookup → (ops_per_sec, MBps)
    let mut lookup: std::collections::HashMap<(String, i64), (f64, f64)> =
        std::collections::HashMap::new();
    for i in 0..df_filt.height() {
        let op = op_col.get(i).unwrap_or("").to_string();
        let secs = (start_col.get(i).unwrap_or(0) - min_ns) / 1_000_000_000;
        let ops = ops_col.get(i).unwrap_or(0.0);
        let mbps = bps_col.get(i).unwrap_or(0.0) / 1_048_576.0;
        lookup.insert((op, secs), (ops, mbps));
    }

    // ops for throughput chart: GET and PUT only (META has no byte payload)
    let bw_ops: Vec<&String> = all_ops
        .iter()
        .filter(|o| o.as_str() == "GET" || o.as_str() == "PUT")
        .collect();

    // ── Write worksheet ───────────────────────────────────────────────────────
    let ws = workbook.add_worksheet();
    ws.set_name(tab_name)?;

    let header_fmt = Format::new().set_bold().set_font_name("Aptos");
    let data_fmt = Format::new().set_font_name("Aptos");

    // Header row: seconds | <op>_ops … | <op>_MBps …
    ws.write_string_with_format(0, 0, "seconds", &header_fmt)?;
    for (oi, op) in all_ops.iter().enumerate() {
        ws.write_string_with_format(0, (oi + 1) as u16, format!("{}_ops", op), &header_fmt)?;
    }
    for (bi, op) in bw_ops.iter().enumerate() {
        ws.write_string_with_format(
            0,
            (n_ops + 1 + bi) as u16,
            format!("{}_MBps", op),
            &header_fmt,
        )?;
    }

    // Data rows
    for (ri, &secs) in all_secs.iter().enumerate() {
        let row = (ri + 1) as u32;
        ws.write_number_with_format(row, 0, secs as f64, &data_fmt)?;
        for (oi, op) in all_ops.iter().enumerate() {
            if let Some(&(ops, _)) = lookup.get(&(op.clone(), secs)) {
                ws.write_number_with_format(row, (oi + 1) as u16, ops, &data_fmt)?;
            }
        }
        for (bi, op) in bw_ops.iter().enumerate() {
            if let Some(&(_, mbps)) = lookup.get(&((*op).clone(), secs)) {
                ws.write_number_with_format(row, (n_ops + 1 + bi) as u16, mbps, &data_fmt)?;
            }
        }
    }

    // Set column widths
    let n_cols = 1 + n_ops + bw_ops.len();
    for ci in 0..n_cols {
        ws.set_column_width(ci as u16, 13.0)?;
    }

    // ── Charts ────────────────────────────────────────────────────────────────
    let chart_row = (n_rows + 3) as u32;

    // Chart 1: ops/sec over time
    if n_ops > 0 {
        let mut chart_ops = Chart::new(ChartType::Line);
        chart_ops.title().set_name("Operations/sec over Time");
        chart_ops.x_axis().set_name("Seconds");
        chart_ops.y_axis().set_name("ops/sec");
        chart_ops.legend().set_position(ChartLegendPosition::Bottom);

        for (oi, op) in all_ops.iter().enumerate() {
            let data_col = (oi + 1) as u16;
            chart_ops
                .add_series()
                .set_name(op.as_str())
                .set_categories((tab_name, 1, 0, n_rows as u32, 0))
                .set_values((tab_name, 1, data_col, n_rows as u32, data_col));
        }
        ws.insert_chart(chart_row, 0, &chart_ops)?;
    }

    // Chart 2: throughput (MiB/s) — GET and PUT only
    if !bw_ops.is_empty() {
        let mut chart_bw = Chart::new(ChartType::Line);
        chart_bw.title().set_name("Throughput (MiB/s) over Time");
        chart_bw.x_axis().set_name("Seconds");
        chart_bw.y_axis().set_name("MiB/s");
        chart_bw.legend().set_position(ChartLegendPosition::Bottom);

        for (bi, op) in bw_ops.iter().enumerate() {
            let data_col = (n_ops + 1 + bi) as u16;
            chart_bw
                .add_series()
                .set_name(op.as_str())
                .set_categories((tab_name, 1, 0, n_rows as u32, 0))
                .set_values((tab_name, 1, data_col, n_rows as u32, data_col));
        }
        ws.insert_chart(chart_row, 8, &chart_bw)?;
    }

    Ok(())
}

// ─────────────────────────── Excel data collectors ───────────────────────────

/// Collect main stats + detail rows for Excel without re-reading the file.
/// Mirrors the query logic in `compute_and_display_stats` but returns rows instead of printing.
/// Numbers are stored as plain "1234.56" strings (no commas) so the Excel writer
/// can parse them as numeric values.
fn collect_stats_rows(
    df: &DataFrame,
    run_time_secs: f64,
    per_client: bool,
    per_endpoint: bool,
) -> Result<ExcelTabRows> {
    // Per-op effective time windows (issue #14)
    let meta_time = compute_op_run_time(df, &META_OPS).unwrap_or(run_time_secs);
    let get_time = compute_op_run_time(df, &["GET"]).unwrap_or(run_time_secs);
    let put_time = compute_op_run_time(df, &["PUT"]).unwrap_or(run_time_secs);
    let op_eff = |op: &str| -> f64 {
        let t = if META_OPS.contains(&op) {
            meta_time
        } else if op == "GET" {
            get_time
        } else if op == "PUT" {
            put_time
        } else {
            run_time_secs
        };
        if t > 0.0 {
            t
        } else {
            run_time_secs
        }
    };

    let has_thread = df.column("thread").is_ok();

    // Build per-bucket aggregation
    let mut agg: Vec<Expr> = vec![
        (col("duration_ns").mean() / lit(1000.0)).alias("mean"),
        (col("duration_ns").median() / lit(1000.0)).alias("med"),
        (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0)).alias("p90"),
        (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0)).alias("p95"),
        (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0)).alias("p99"),
        (col("duration_ns").max() / lit(1000.0)).alias("max"),
        (col("bytes").mean() / lit(1024.0)).alias("avg"),
        col("op").count().alias("count"),
        col("bytes").sum().cast(DataType::Float64).alias("bsum"),
    ];
    if has_thread {
        agg.push(col("thread").n_unique().alias("max_threads"));
    }

    let stats = df
        .clone()
        .lazy()
        .group_by([col("op"), col("bytes_bucket"), col("bucket_num")])
        .agg(agg)
        .sort(["bucket_num", "op"], SortMultipleOptions::default())
        .collect()?;

    let mut main_rows: Vec<Vec<String>> = Vec::new();

    // Header row
    main_rows.push(vec![
        "op".into(),
        "bytes_bucket".into(),
        "bucket_#".into(),
        "mean_lat_us".into(),
        "med._lat_us".into(),
        "90%_lat_us".into(),
        "95%_lat_us".into(),
        "99%_lat_us".into(),
        "max_lat_us".into(),
        "avg_obj_KB".into(),
        "ops_/_sec".into(),
        "xput_MBps".into(),
        "count".into(),
        "max_threads".into(),
        "runtime_s".into(),
    ]);

    let op_c = stats.column("op")?.str()?;
    let bkt_c = stats.column("bytes_bucket")?.str()?;
    let bnum_c = stats.column("bucket_num")?.i32()?;
    let mean_c = stats.column("mean")?.f64()?;
    let med_c = stats.column("med")?.f64()?;
    let p90_c = stats.column("p90")?.f64()?;
    let p95_c = stats.column("p95")?.f64()?;
    let p99_c = stats.column("p99")?.f64()?;
    let max_c = stats.column("max")?.f64()?;
    let avg_c = stats.column("avg")?.f64()?;
    let cnt_c = stats.column("count")?.u32()?;
    let bsum_c = stats.column("bsum")?.f64()?;
    let conc_c = if has_thread {
        Some(stats.column("max_threads")?.u32()?)
    } else {
        None
    };

    for i in 0..stats.height() {
        let cnt = cnt_c.get(i).unwrap_or(0);
        if cnt == 0 {
            continue;
        }
        let op = op_c.get(i).unwrap_or("?");
        let bsum = bsum_c.get(i).unwrap_or(0.0);
        let eff = op_eff(op);
        main_rows.push(vec![
            op.to_string(),
            bkt_c.get(i).unwrap_or("?").to_string(),
            bnum_c.get(i).unwrap_or(0).to_string(),
            format!("{:.2}", mean_c.get(i).unwrap_or(0.0)),
            format!("{:.2}", med_c.get(i).unwrap_or(0.0)),
            format!("{:.2}", p90_c.get(i).unwrap_or(0.0)),
            format!("{:.2}", p95_c.get(i).unwrap_or(0.0)),
            format!("{:.2}", p99_c.get(i).unwrap_or(0.0)),
            format!("{:.2}", max_c.get(i).unwrap_or(0.0)),
            format!("{:.2}", avg_c.get(i).unwrap_or(0.0)),
            format!("{:.2}", cnt as f64 / eff),
            format!("{:.2}", bsum / (1024.0 * 1024.0 * eff)),
            cnt.to_string(),
            conc_c
                .as_ref()
                .map_or(0, |c| c.get(i).unwrap_or(0))
                .to_string(),
            format!("{:.1}", eff),
        ]);
    }

    // Summary (ALL) rows for META / GET / PUT
    for (category, ops_list, bucket_idx) in [
        ("META", META_OPS.as_slice(), 97i32),
        ("GET", ["GET"].as_slice(), 98i32),
        ("PUT", ["PUT"].as_slice(), 99i32),
    ] {
        let mut filt = lit(false);
        for op in ops_list {
            filt = filt.or(col("op").eq(lit(*op)));
        }

        let mut sel: Vec<Expr> = vec![
            (col("duration_ns").mean() / lit(1000.0)).alias("mean"),
            (col("duration_ns").median() / lit(1000.0)).alias("med"),
            (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0))
                .alias("p90"),
            (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0))
                .alias("p95"),
            (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                .alias("p99"),
            (col("duration_ns").max() / lit(1000.0)).alias("max"),
            (col("bytes").mean() / lit(1024.0)).alias("avg"),
            col("op").count().alias("count"),
            col("bytes").sum().cast(DataType::Float64).alias("bsum"),
        ];
        if has_thread {
            sel.push(col("thread").n_unique().alias("n_thr"));
        }

        let cs = df.clone().lazy().filter(filt).select(sel).collect()?;
        let cnt = cs.column("count")?.u32()?.get(0).unwrap_or(0);
        if cnt == 0 {
            continue;
        }

        let op_time = match category {
            "META" => meta_time,
            "GET" => get_time,
            "PUT" => put_time,
            _ => run_time_secs,
        };
        let eff = if op_time > 0.0 {
            op_time
        } else {
            run_time_secs
        };
        let bsum = cs.column("bsum")?.f64()?.get(0).unwrap_or(0.0);
        let n_thr = if has_thread {
            cs.column("n_thr")?.u32()?.get(0).unwrap_or(0)
        } else {
            0
        };

        main_rows.push(vec![
            category.to_string(),
            "ALL".to_string(),
            bucket_idx.to_string(),
            format!("{:.2}", cs.column("mean")?.f64()?.get(0).unwrap_or(0.0)),
            format!("{:.2}", cs.column("med")?.f64()?.get(0).unwrap_or(0.0)),
            format!("{:.2}", cs.column("p90")?.f64()?.get(0).unwrap_or(0.0)),
            format!("{:.2}", cs.column("p95")?.f64()?.get(0).unwrap_or(0.0)),
            format!("{:.2}", cs.column("p99")?.f64()?.get(0).unwrap_or(0.0)),
            format!("{:.2}", cs.column("max")?.f64()?.get(0).unwrap_or(0.0)),
            format!("{:.2}", cs.column("avg")?.f64()?.get(0).unwrap_or(0.0)),
            format!("{:.2}", cnt as f64 / eff),
            format!("{:.2}", bsum / (1024.0 * 1024.0 * eff)),
            cnt.to_string(),
            n_thr.to_string(),
            format!("{:.1}", eff),
        ]);
    }

    // Collect detail rows
    let mut detail_rows: Vec<Vec<String>> = Vec::new();

    if per_client && df.column("client_id").is_ok() {
        let rows = collect_per_client_rows(df, run_time_secs)?;
        if !rows.is_empty() {
            detail_rows.extend(rows);
        }
    }

    if per_endpoint && df.column("endpoint").is_ok() {
        let rows = collect_per_endpoint_rows(df, run_time_secs)?;
        if !rows.is_empty() {
            if !detail_rows.is_empty() {
                detail_rows.push(vec![]);
            }
            detail_rows.extend(rows);
        }
    }

    Ok((main_rows, detail_rows))
}

/// Collect per-client statistics rows for Excel.
fn collect_per_client_rows(df: &DataFrame, run_time_secs: f64) -> Result<Vec<Vec<String>>> {
    if df.column("client_id").is_err() {
        return Ok(vec![]);
    }

    let cs = df
        .clone()
        .lazy()
        .group_by([col("client_id")])
        .agg([
            (col("duration_ns").mean() / lit(1000.0)).alias("mean"),
            (col("duration_ns").median() / lit(1000.0)).alias("med"),
            (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0))
                .alias("p90"),
            (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0))
                .alias("p95"),
            (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                .alias("p99"),
            (col("duration_ns").max() / lit(1000.0)).alias("max"),
            (col("bytes").mean() / lit(1024.0)).alias("avg"),
            (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops"),
            ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                / lit(run_time_secs))
            .alias("xp"),
            col("op").count().alias("count"),
        ])
        .sort(["client_id"], SortMultipleOptions::default())
        .collect()?;

    if cs.height() == 0 {
        return Ok(vec![]);
    }

    let mut rows: Vec<Vec<String>> = vec![
        vec!["=== Per-Client Statistics ===".into()],
        vec![
            "client_id".into(),
            "mean_lat_us".into(),
            "med._lat_us".into(),
            "90%_lat_us".into(),
            "95%_lat_us".into(),
            "99%_lat_us".into(),
            "max_lat_us".into(),
            "avg_obj_KB".into(),
            "ops_/_sec".into(),
            "xput_MBps".into(),
            "count".into(),
        ],
    ];

    let cid = cs.column("client_id")?.str()?;
    for i in 0..cs.height() {
        rows.push(vec![
            cid.get(i).unwrap_or("?").to_string(),
            format!("{:.2}", cs.column("mean")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("med")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("p90")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("p95")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("p99")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("max")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("avg")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("ops")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", cs.column("xp")?.f64()?.get(i).unwrap_or(0.0)),
            cs.column("count")?.u32()?.get(i).unwrap_or(0).to_string(),
        ]);
    }

    // Per-op breakdowns
    for (op_name, ops_list) in [
        ("META", META_OPS.as_slice()),
        ("GET", ["GET"].as_slice()),
        ("PUT", ["PUT"].as_slice()),
    ] {
        let mut filt = lit(false);
        for op in ops_list {
            filt = filt.or(col("op").eq(lit(*op)));
        }

        let os = df
            .clone()
            .lazy()
            .filter(filt)
            .group_by([col("client_id")])
            .agg([
                (col("duration_ns").mean() / lit(1000.0)).alias("mean"),
                (col("duration_ns").median() / lit(1000.0)).alias("med"),
                (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                    .alias("p99"),
                (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops"),
                ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                    / lit(run_time_secs))
                .alias("xp"),
                col("op").count().alias("count"),
            ])
            .sort(["client_id"], SortMultipleOptions::default())
            .collect()?;

        if os.height() == 0 {
            continue;
        }

        rows.push(vec![]);
        rows.push(vec![format!("--- {} Operations ---", op_name)]);
        rows.push(vec![
            "client_id".into(),
            "mean_lat_us".into(),
            "med._lat_us".into(),
            "99%_lat_us".into(),
            "ops_/_sec".into(),
            "xput_MBps".into(),
            "count".into(),
        ]);

        let cid = os.column("client_id")?.str()?;
        for i in 0..os.height() {
            rows.push(vec![
                cid.get(i).unwrap_or("?").to_string(),
                format!("{:.2}", os.column("mean")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("med")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("p99")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("ops")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("xp")?.f64()?.get(i).unwrap_or(0.0)),
                os.column("count")?.u32()?.get(i).unwrap_or(0).to_string(),
            ]);
        }
    }

    Ok(rows)
}

/// Collect per-endpoint statistics rows for Excel.
fn collect_per_endpoint_rows(df: &DataFrame, run_time_secs: f64) -> Result<Vec<Vec<String>>> {
    if df.column("endpoint").is_err() {
        return Ok(vec![]);
    }

    let es = df
        .clone()
        .lazy()
        .filter(col("endpoint").is_not_null())
        .group_by([col("endpoint")])
        .agg([
            (col("duration_ns").mean() / lit(1000.0)).alias("mean"),
            (col("duration_ns").median() / lit(1000.0)).alias("med"),
            (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0))
                .alias("p90"),
            (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0))
                .alias("p95"),
            (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                .alias("p99"),
            (col("duration_ns").max() / lit(1000.0)).alias("max"),
            (col("bytes").mean() / lit(1024.0)).alias("avg"),
            (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops"),
            ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                / lit(run_time_secs))
            .alias("xp"),
            col("op").count().alias("count"),
        ])
        .filter(col("count").gt(lit(0u32)))
        .sort(["endpoint"], SortMultipleOptions::default())
        .collect()?;

    if es.height() == 0 {
        return Ok(vec![]);
    }

    let mut rows: Vec<Vec<String>> = vec![
        vec!["=== Per-Endpoint Statistics ===".into()],
        vec![
            "endpoint".into(),
            "mean_lat_us".into(),
            "med._lat_us".into(),
            "90%_lat_us".into(),
            "95%_lat_us".into(),
            "99%_lat_us".into(),
            "max_lat_us".into(),
            "avg_obj_KB".into(),
            "ops_/_sec".into(),
            "xput_MBps".into(),
            "count".into(),
        ],
    ];

    let ep = es.column("endpoint")?.str()?;
    for i in 0..es.height() {
        rows.push(vec![
            ep.get(i).unwrap_or("?").to_string(),
            format!("{:.2}", es.column("mean")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("med")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("p90")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("p95")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("p99")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("max")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("avg")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("ops")?.f64()?.get(i).unwrap_or(0.0)),
            format!("{:.2}", es.column("xp")?.f64()?.get(i).unwrap_or(0.0)),
            es.column("count")?.u32()?.get(i).unwrap_or(0).to_string(),
        ]);
    }

    // Per-op breakdowns
    for (op_name, ops_list) in [
        ("META", META_OPS.as_slice()),
        ("GET", ["GET"].as_slice()),
        ("PUT", ["PUT"].as_slice()),
    ] {
        let mut filt = lit(false);
        for op in ops_list {
            filt = filt.or(col("op").eq(lit(*op)));
        }

        let os = df
            .clone()
            .lazy()
            .filter(filt.and(col("endpoint").is_not_null()))
            .group_by([col("endpoint")])
            .agg([
                (col("duration_ns").mean() / lit(1000.0)).alias("mean"),
                (col("duration_ns").median() / lit(1000.0)).alias("med"),
                (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0))
                    .alias("p99"),
                (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops"),
                ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0))
                    / lit(run_time_secs))
                .alias("xp"),
                col("op").count().alias("count"),
            ])
            .filter(col("count").gt(lit(0u32)))
            .sort(["endpoint"], SortMultipleOptions::default())
            .collect()?;

        if os.height() == 0 {
            continue;
        }

        rows.push(vec![]);
        rows.push(vec![format!("--- {} Operations ---", op_name)]);
        rows.push(vec![
            "endpoint".into(),
            "mean_lat_us".into(),
            "med._lat_us".into(),
            "99%_lat_us".into(),
            "ops_/_sec".into(),
            "xput_MBps".into(),
            "count".into(),
        ]);

        let ep = os.column("endpoint")?.str()?;
        for i in 0..os.height() {
            rows.push(vec![
                ep.get(i).unwrap_or("?").to_string(),
                format!("{:.2}", os.column("mean")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("med")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("p99")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("ops")?.f64()?.get(i).unwrap_or(0.0)),
                format!("{:.2}", os.column("xp")?.f64()?.get(i).unwrap_or(0.0)),
                os.column("count")?.u32()?.get(i).unwrap_or(0).to_string(),
            ]);
        }
    }

    Ok(rows)
}

// ─────────────────────────────── Unit tests ──────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_skip_time ─────────────────────────────────────────────────────

    #[test]
    fn test_parse_skip_time_seconds() {
        let nanos = parse_skip_time("90s").unwrap();
        assert_eq!(nanos, 90 * 1_000_000_000);
    }

    #[test]
    fn test_parse_skip_time_minutes() {
        let nanos = parse_skip_time("5m").unwrap();
        assert_eq!(nanos, 5 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_parse_skip_time_zero() {
        let nanos = parse_skip_time("0s").unwrap();
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_parse_skip_time_invalid_unit() {
        // "h" (hours) is not a recognised unit
        assert!(parse_skip_time("2h").is_err());
    }

    #[test]
    fn test_parse_skip_time_invalid_format() {
        assert!(parse_skip_time("abc").is_err());
        assert!(parse_skip_time("").is_err());
    }

    // ── format_with_commas ──────────────────────────────────────────────────

    #[test]
    fn test_format_with_commas_zero() {
        assert_eq!(format_with_commas(0.0), "0.00");
    }

    #[test]
    fn test_format_with_commas_thousands() {
        assert_eq!(format_with_commas(1_000.0), "1,000.00");
    }

    #[test]
    fn test_format_with_commas_large() {
        assert_eq!(format_with_commas(1_234_567.0), "1,234,567.00");
    }

    #[test]
    fn test_format_with_commas_fractional() {
        // 1024.75: integer part 1024 gets commas, fractional part is .75
        assert_eq!(format_with_commas(1_024.75), "1,024.75");
    }

    // ── format_int_with_commas ──────────────────────────────────────────────

    #[test]
    fn test_format_int_with_commas_small() {
        assert_eq!(format_int_with_commas(42), "42");
    }

    #[test]
    fn test_format_int_with_commas_million() {
        assert_eq!(format_int_with_commas(1_000_000), "1,000,000");
    }

    // ── format_duration_ns ──────────────────────────────────────────────────

    #[test]
    fn test_format_duration_ns_zero() {
        // 0 ns → 0 h, 0 m, 0.0 s  — seconds field uses {:09.6} = "00.000000"
        assert_eq!(format_duration_ns(0), "0:00:00.000000");
    }

    #[test]
    fn test_format_duration_ns_one_second() {
        assert_eq!(format_duration_ns(1_000_000_000), "0:00:01.000000");
    }

    #[test]
    fn test_format_duration_ns_one_minute() {
        assert_eq!(format_duration_ns(60_000_000_000), "0:01:00.000000");
    }

    #[test]
    fn test_format_duration_ns_one_hour() {
        assert_eq!(format_duration_ns(3_600_000_000_000), "1:00:00.000000");
    }

    #[test]
    fn test_format_duration_ns_90_seconds() {
        // 90 s = 1 min 30 s
        assert_eq!(format_duration_ns(90_000_000_000), "0:01:30.000000");
    }

    // ── derive_short_name ───────────────────────────────────────────────────

    #[test]
    fn test_derive_short_name_tsv_zst() {
        assert_eq!(derive_short_name("agent-1.tsv.zst"), "agent-1");
    }

    #[test]
    fn test_derive_short_name_csv_no_zst() {
        assert_eq!(derive_short_name("/tmp/results.csv"), "results");
    }

    #[test]
    fn test_derive_short_name_warp_bracket() {
        // Warp-style filename: everything from '[' onwards is stripped
        assert_eq!(
            derive_short_name("warp-run[20260101]-abc.tsv.zst"),
            "warp-run"
        );
    }

    #[test]
    fn test_derive_short_name_truncates_to_20() {
        // A base name longer than 20 chars must be truncated to at most 20
        let name = derive_short_name("this-is-a-very-long-filename-indeed.tsv.zst");
        assert!(
            name.len() <= 20,
            "expected ≤20 chars, got {}: {:?}",
            name.len(),
            name
        );
    }

    // ── derive_excel_path ───────────────────────────────────────────────────

    #[test]
    fn test_derive_excel_path_single_file() {
        // Single file with no directory component → same stem with .xlsx extension
        let path = derive_excel_path(&["run.tsv.zst".to_string()]);
        assert_eq!(path, "run.xlsx");
    }

    #[test]
    fn test_derive_excel_path_multiple_files() {
        let path =
            derive_excel_path(&["agent-1.tsv.zst".to_string(), "agent-2.tsv.zst".to_string()]);
        assert_eq!(path, "polarwarp-results.xlsx");
    }

    // ── make_tab_name ───────────────────────────────────────────────────────

    #[test]
    fn test_make_tab_name_short() {
        assert_eq!(make_tab_name("run", "Results"), "run-Results");
    }

    #[test]
    fn test_make_tab_name_enforces_31_char_limit() {
        // Excel worksheet names must be ≤ 31 characters
        let name = make_tab_name("this-is-a-very-long-base-name-here", "Results");
        assert!(
            name.len() <= 31,
            "expected ≤31 chars, got {}: {:?}",
            name.len(),
            name
        );
        assert!(
            name.ends_with("-Results"),
            "expected '-Results' suffix, got {:?}",
            name
        );
    }

    // ── FileType enum ───────────────────────────────────────────────────────

    #[test]
    fn test_file_type_equality() {
        assert_eq!(FileType::Trace, FileType::Trace);
        assert_eq!(FileType::Summary, FileType::Summary);
        assert_ne!(FileType::Trace, FileType::Summary);
    }

    // ── add_size_buckets (core bucketing logic) ─────────────────────────────

    #[test]
    fn test_size_bucket_zero_bytes() {
        let df = df!["bytes" => [0i64]].unwrap();
        let result = add_size_buckets(df).unwrap();
        let bucket = result
            .column("bytes_bucket")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(bucket, "zero");
    }

    #[test]
    fn test_size_bucket_boundaries() {
        // One value per bucket, including each exact lower boundary
        let df = df![
            "bytes" => [
                1i64,               // 1B-8KiB  (1 byte)
                8 * 1024 - 1,       // 1B-8KiB  (8191 bytes, just below 8 KiB)
                8 * 1024,           // 8KiB-64KiB  (8192 = BUCKET_8K)
                64 * 1024,          // 64KiB-512KiB
                512 * 1024,         // 512KiB-4MiB
                4 * 1024 * 1024,    // 4MiB-32MiB
                32 * 1024 * 1024,   // 32MiB-256MiB
                256 * 1024 * 1024,  // 256MiB-2GiB
                2i64 * 1024 * 1024 * 1024  // >2GiB
            ]
        ]
        .unwrap();

        let result = add_size_buckets(df).unwrap();
        let bc = result.column("bytes_bucket").unwrap().str().unwrap();

        assert_eq!(bc.get(0).unwrap(), "1B-8KiB", "1 byte");
        assert_eq!(bc.get(1).unwrap(), "1B-8KiB", "8191 bytes");
        assert_eq!(bc.get(2).unwrap(), "8KiB-64KiB", "8192 bytes");
        assert_eq!(bc.get(3).unwrap(), "64KiB-512KiB", "64 KiB");
        assert_eq!(bc.get(4).unwrap(), "512KiB-4MiB", "512 KiB");
        assert_eq!(bc.get(5).unwrap(), "4MiB-32MiB", "4 MiB");
        assert_eq!(bc.get(6).unwrap(), "32MiB-256MiB", "32 MiB");
        assert_eq!(bc.get(7).unwrap(), "256MiB-2GiB", "256 MiB");
        assert_eq!(bc.get(8).unwrap(), ">2GiB", "2 GiB");
    }

    #[test]
    fn test_size_bucket_num_matches_label() {
        // bucket_num and bytes_bucket must be consistent for every row
        let df = df![
            "bytes" => [0i64, 1i64, 8192i64, 65536i64, 524288i64,
                        4194304i64, 33554432i64, 268435456i64, 2147483648i64]
        ]
        .unwrap();
        let result = add_size_buckets(df).unwrap();
        let labels = result.column("bytes_bucket").unwrap().str().unwrap();
        let nums = result.column("bucket_num").unwrap().i32().unwrap();

        for i in 0..result.height() {
            let label = labels.get(i).unwrap();
            let num = nums.get(i).unwrap();
            assert_eq!(
                label, BUCKET_LABELS[num as usize],
                "row {}: bucket_num {} → label mismatch",
                i, num
            );
        }
    }
}
