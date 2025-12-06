// SPDX-FileCopyrightText: 2025 Russ Fellows <russ.fellows@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// polarwarp-rs: A Rust implementation of polarWarp for processing oplog files

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, ArgAction};
use num_format::{Locale, ToFormattedString};
use polars::prelude::*;
use regex::Regex;

/// Number of size buckets (matching sai3-bench)
const NUM_BUCKETS: usize = 9;

/// Size bucket boundaries (in bytes) - matching sai3-bench
const BUCKET_8K: i64 = 8 * 1024;              // 8 KiB
const BUCKET_64K: i64 = 64 * 1024;            // 64 KiB  
const BUCKET_512K: i64 = 512 * 1024;          // 512 KiB
const BUCKET_4M: i64 = 4 * 1024 * 1024;       // 4 MiB
const BUCKET_32M: i64 = 32 * 1024 * 1024;     // 32 MiB
const BUCKET_256M: i64 = 256 * 1024 * 1024;   // 256 MiB
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

    /// Just print basic stats without full processing
    #[arg(long)]
    basic_stats: bool,
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
        anyhow::bail!("Invalid skip format '{}'. Use format like '90s' or '5m'", skip)
    }
}

/// Detect the separator used in a file by examining the header line
/// Returns tab if tabs found, comma if commas found, defaults to tab
fn detect_separator(file_path: &str) -> Result<u8> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    
    let file = File::open(file_path)?;
    
    // Handle zstd compressed files
    let first_line = if file_path.ends_with(".zst") {
        let decoder = zstd::stream::read::Decoder::new(file)?;
        let mut reader = BufReader::new(decoder);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        line
    } else {
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        line
    };
    
    // Count tabs vs commas in header line
    let tab_count = first_line.matches('\t').count();
    let comma_count = first_line.matches(',').count();
    
    // Use whichever delimiter appears more often
    // This handles cases where .csv files are actually tab-separated (like warp output)
    if tab_count > comma_count {
        Ok(b'\t')
    } else if comma_count > 0 {
        Ok(b',')
    } else {
        Ok(b'\t')  // Default to tab
    }
}

/// Format a number with commas for readability
fn format_with_commas(value: f64) -> String {
    if value.is_nan() || value.is_infinite() {
        return format!("{:.2}", value);
    }
    
    let int_part = value.trunc() as i64;
    let frac_part = (value.fract() * 100.0).round() as i64;
    
    format!("{}.{:02}", int_part.to_formatted_string(&Locale::en), frac_part.abs())
}

/// Format integer with commas
fn format_int_with_commas(value: i64) -> String {
    value.to_formatted_string(&Locale::en)
}

fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

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
    let mut global_start_ns: Option<i64> = None;
    let mut global_end_ns: Option<i64> = None;

    // Process each file
    for file_path in &args.files {
        println!("\nProcessing file: {}", file_path);

        let start = Instant::now();

        // Read and process the file
        let (df, file_start_ns, file_end_ns) = process_file(
            file_path, 
            skip_nanos,
            args.basic_stats,
        )?;

        // Update global time range
        if let (Some(fs), Some(fe)) = (file_start_ns, file_end_ns) {
            match (global_start_ns, global_end_ns) {
                (None, None) => {
                    global_start_ns = Some(fs);
                    global_end_ns = Some(fe);
                }
                (Some(gs), Some(ge)) => {
                    global_start_ns = Some(gs.max(fs));
                    global_end_ns = Some(ge.min(fe));
                }
                _ => {}
            }
        }

        // Store the processed dataframe for consolidation
        if !args.basic_stats {
            all_dataframes.push(df);
        }

        let elapsed = start.elapsed();
        println!("Processed file in {:.2?}", elapsed);
    }

    // If we have multiple files, consolidate results
    if args.files.len() > 1 && !args.basic_stats {
        println!("\nDone Processing Files... Consolidating Results");
        
        if let (Some(gs), Some(ge)) = (global_start_ns, global_end_ns) {
            if gs >= ge {
                println!("No overlapping time range found between files, no Consolidated results are valid.");
                return Ok(());
            }
            
            let run_time_secs = (ge - gs) as f64 / 1_000_000_000.0;
            let run_time_formatted = format_duration_ns(ge - gs);
            println!("The consolidated running time is {}, time in seconds is: {:.2}", 
                     run_time_formatted, run_time_secs);

            // Concatenate all dataframes
            let consolidated_df = concat_dataframes(&all_dataframes)?;
            
            // Compute and display consolidated stats
            compute_and_display_stats(&consolidated_df, run_time_secs, "Consolidated Results:")?;
        } else {
            println!("No valid data to consolidate.");
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
    let result = concat(lazy_frames, UnionArgs::default())?
        .collect()?;
    
    Ok(result)
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
        df = df.lazy()
            .filter(col("start_ns").gt(lit(new_start)))
            .collect()?;
        
        new_start
    } else {
        start_ns
    };

    let run_time_secs = (end_ns - effective_start_ns) as f64 / 1_000_000_000.0;
    let run_time_formatted = format_duration_ns(end_ns - effective_start_ns);
    println!("The file run time is {}, time in seconds is: {:.2}", run_time_formatted, run_time_secs);

    // Add size buckets
    df = add_size_buckets(df)?;

    // Compute and display statistics
    compute_and_display_stats(&df, run_time_secs, "")?;

    Ok((df, Some(effective_start_ns), Some(end_ns)))
}

/// Read a TSV or CSV file (optionally zstd compressed) into a DataFrame
fn read_tsv_file(file_path: &str) -> Result<DataFrame> {
    let path = Path::new(file_path);
    
    // Detect separator by reading first line of the file
    let separator = detect_separator(file_path)?;

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
        .context("Failed to read TSV file")?;

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
        .collect()?;

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
    let result = df.lazy()
        .with_columns([
            // Create bucket label (matching sai3-bench bucket boundaries)
            when(col("bytes").eq(lit(0)))
                .then(lit(BUCKET_LABELS[0]))  // zero
            .when(col("bytes").gt_eq(lit(1)).and(col("bytes").lt(lit(BUCKET_8K))))
                .then(lit(BUCKET_LABELS[1]))  // 1B-8KiB
            .when(col("bytes").gt_eq(lit(BUCKET_8K)).and(col("bytes").lt(lit(BUCKET_64K))))
                .then(lit(BUCKET_LABELS[2]))  // 8KiB-64KiB
            .when(col("bytes").gt_eq(lit(BUCKET_64K)).and(col("bytes").lt(lit(BUCKET_512K))))
                .then(lit(BUCKET_LABELS[3]))  // 64KiB-512KiB
            .when(col("bytes").gt_eq(lit(BUCKET_512K)).and(col("bytes").lt(lit(BUCKET_4M))))
                .then(lit(BUCKET_LABELS[4]))  // 512KiB-4MiB
            .when(col("bytes").gt_eq(lit(BUCKET_4M)).and(col("bytes").lt(lit(BUCKET_32M))))
                .then(lit(BUCKET_LABELS[5]))  // 4MiB-32MiB
            .when(col("bytes").gt_eq(lit(BUCKET_32M)).and(col("bytes").lt(lit(BUCKET_256M))))
                .then(lit(BUCKET_LABELS[6]))  // 32MiB-256MiB
            .when(col("bytes").gt_eq(lit(BUCKET_256M)).and(col("bytes").lt(lit(BUCKET_2G))))
                .then(lit(BUCKET_LABELS[7]))  // 256MiB-2GiB
            .otherwise(lit(BUCKET_LABELS[8])) // >2GiB
                .alias("bytes_bucket"),
            // Create bucket number for sorting
            when(col("bytes").eq(lit(0)))
                .then(lit(0i32))
            .when(col("bytes").gt_eq(lit(1)).and(col("bytes").lt(lit(BUCKET_8K))))
                .then(lit(1i32))
            .when(col("bytes").gt_eq(lit(BUCKET_8K)).and(col("bytes").lt(lit(BUCKET_64K))))
                .then(lit(2i32))
            .when(col("bytes").gt_eq(lit(BUCKET_64K)).and(col("bytes").lt(lit(BUCKET_512K))))
                .then(lit(3i32))
            .when(col("bytes").gt_eq(lit(BUCKET_512K)).and(col("bytes").lt(lit(BUCKET_4M))))
                .then(lit(4i32))
            .when(col("bytes").gt_eq(lit(BUCKET_4M)).and(col("bytes").lt(lit(BUCKET_32M))))
                .then(lit(5i32))
            .when(col("bytes").gt_eq(lit(BUCKET_32M)).and(col("bytes").lt(lit(BUCKET_256M))))
                .then(lit(6i32))
            .when(col("bytes").gt_eq(lit(BUCKET_256M)).and(col("bytes").lt(lit(BUCKET_2G))))
                .then(lit(7i32))
            .otherwise(lit(8i32))
                .alias("bucket_num"),
        ])
        .collect()?;

    Ok(result)
}

/// Compute and display performance statistics
fn compute_and_display_stats(df: &DataFrame, run_time_secs: f64, title: &str) -> Result<()> {
    // Group by operation and bucket, compute statistics
    let stats = df.clone().lazy()
        .group_by([col("op"), col("bytes_bucket"), col("bucket_num")])
        .agg([
            // Latency statistics (convert ns to µs)
            (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
            (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
            (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0)).alias("p90_lat_us"),
            (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0)).alias("p95_lat_us"),
            (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0)).alias("p99_lat_us"),
            (col("duration_ns").max() / lit(1000.0)).alias("max_lat_us"),
            // Size statistics
            (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
            // Throughput
            (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops_per_sec"),
            ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0)) / lit(run_time_secs)).alias("xput_MBps"),
            col("op").count().alias("count"),
        ])
        .sort(
            ["bucket_num", "op"],
            SortMultipleOptions::default(),
        )
        .collect()?;

    // Print the results
    if !title.is_empty() {
        println!("\n{}", title);
    }
    
    // Print header (matching Python output format)
    println!("{:>8} {:>12} {:>8} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9}",
             "op", "bytes_bucket", "bucket_#", "mean_lat_us", "med._lat_us", "90%_lat_us", 
             "95%_lat_us", "99%_lat_us", "max_lat_us", "avg_obj_KB", "ops_/_sec", "xput_MBps", "count");

    // Print each row
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
    let ops_sec = stats.column("ops_per_sec")?.f64()?;
    let xput = stats.column("xput_MBps")?.f64()?;
    let count_col = stats.column("count")?.u32()?;

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
        let ops = ops_sec.get(i).unwrap_or(0.0);
        let xp = xput.get(i).unwrap_or(0.0);
        let cnt = count_col.get(i).unwrap_or(0);

        // Skip rows with zero count (empty buckets or invalid data)
        if cnt == 0 {
            continue;
        }

        println!("{:>8} {:>12} {:>8} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9}",
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
                 format_int_with_commas(cnt as i64));
    }

    // Print summary rows for each operation category (META, GET, PUT)
    // These use statistically valid percentiles computed from ALL raw data
    println!(); // Separator line before summaries
    
    // Compute summary for META operations (LIST, HEAD, DELETE, STAT)
    compute_and_print_summary_row(df, run_time_secs, "META", &META_OPS, 97)?;
    
    // Compute summary for GET operations
    compute_and_print_summary_row(df, run_time_secs, "GET", &["GET"], 98)?;
    
    // Compute summary for PUT operations
    compute_and_print_summary_row(df, run_time_secs, "PUT", &["PUT"], 99)?;

    // Print grand total
    let total_ops: u64 = count_col.into_iter().flatten().map(|x| x as u64).sum();
    let total_ops_sec: f64 = total_ops as f64 / run_time_secs;
    println!("\nTotal operations: {}  ({:.2}/sec)", 
             format_int_with_commas(total_ops as i64),
             total_ops_sec);

    Ok(())
}

/// Compute and print a summary row for a category of operations
/// This computes statistically valid percentiles from ALL raw data (not averaged)
fn compute_and_print_summary_row(
    df: &DataFrame, 
    run_time_secs: f64, 
    category: &str, 
    ops: &[&str],
    bucket_idx: i32,
) -> Result<()> {
    // Build filter for matching operations
    let mut filter_expr = lit(false);
    for op in ops {
        filter_expr = filter_expr.or(col("op").eq(lit(*op)));
    }
    
    // Filter to just this category and compute stats
    let category_stats = df.clone().lazy()
        .filter(filter_expr)
        .select([
            // Latency statistics (convert ns to µs) - computed on ALL raw data
            (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
            (col("duration_ns").median() / lit(1000.0)).alias("med_lat_us"),
            (col("duration_ns").quantile(lit(0.90), QuantileMethod::Linear) / lit(1000.0)).alias("p90_lat_us"),
            (col("duration_ns").quantile(lit(0.95), QuantileMethod::Linear) / lit(1000.0)).alias("p95_lat_us"),
            (col("duration_ns").quantile(lit(0.99), QuantileMethod::Linear) / lit(1000.0)).alias("p99_lat_us"),
            (col("duration_ns").max() / lit(1000.0)).alias("max_lat_us"),
            // Size statistics
            (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
            // Throughput (sum of counts and bytes)
            (col("op").count().cast(DataType::Float64) / lit(run_time_secs)).alias("ops_per_sec"),
            ((col("bytes").sum().cast(DataType::Float64) / lit(1024.0 * 1024.0)) / lit(run_time_secs)).alias("xput_MBps"),
            col("op").count().alias("count"),
        ])
        .collect()?;

    // Check if we have any data for this category
    if category_stats.height() == 0 {
        return Ok(());
    }
    
    let count = category_stats.column("count")?.u32()?.get(0).unwrap_or(0);
    if count == 0 {
        return Ok(());
    }

    // Extract values
    let mean = category_stats.column("mean_lat_us")?.f64()?.get(0).unwrap_or(0.0);
    let med = category_stats.column("med_lat_us")?.f64()?.get(0).unwrap_or(0.0);
    let p90 = category_stats.column("p90_lat_us")?.f64()?.get(0).unwrap_or(0.0);
    let p95 = category_stats.column("p95_lat_us")?.f64()?.get(0).unwrap_or(0.0);
    let p99 = category_stats.column("p99_lat_us")?.f64()?.get(0).unwrap_or(0.0);
    let max = category_stats.column("max_lat_us")?.f64()?.get(0).unwrap_or(0.0);
    let avg = category_stats.column("avg_obj_KB")?.f64()?.get(0).unwrap_or(0.0);
    let ops = category_stats.column("ops_per_sec")?.f64()?.get(0).unwrap_or(0.0);
    let xp = category_stats.column("xput_MBps")?.f64()?.get(0).unwrap_or(0.0);

    // Print summary row with "ALL" as bucket label
    println!("{:>8} {:>12} {:>8} {:>11} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9} {:>9} {:>9}",
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
             format_int_with_commas(count as i64));

    Ok(())
}

/// Print basic stats about the dataframe (used with --basic-stats flag)
fn print_basic_stats(df: &DataFrame) {
    println!("Shape: {} rows × {} columns", df.height(), df.width());

    // Print column names and types
    println!("\nColumns:");
    for name in df.get_column_names() {
        let dtype = df.column(name).map(|s| s.dtype().clone()).unwrap_or(DataType::Null);
        println!("  - {}: {}", name, dtype);
    }

    // Print first few rows
    println!("\nSample data (first 5 rows):");
    let sample = df.head(Some(5));
    println!("{}", sample);
}
