use std::cmp::{max, min};
use chrono::{DateTime, Utc};
use polars::prelude::*;

use crate::buckets;
use crate::formatter;
use crate::parser;
use crate::stats;
use crate::types::{FileStats, PolarWarpError, Result, SkipTime};

/// Process a single file and print results
pub fn process_file(file_path: &str, skip_time: Option<SkipTime>) -> Result<(DataFrame, FileStats)> {
    // Parse the file
    let (mut df, stats) = parser::parse_file(file_path, skip_time)?;
    
    // Add byte bucket columns
    buckets::add_byte_buckets_expr(&mut df)?;
    
    // Calculate statistics
    let result = stats::calculate_file_stats(&df, stats.run_time_secs)?;
    
    // Format for display
    let formatted_result = formatter::format_dataframe_for_display(
        &result, 
        &formatter::default_columns_to_format()
    )?;
    
    // Print results
    formatter::print_dataframe(&formatted_result);
    
    Ok((df, stats))
}

/// Process multiple files and consolidate results
pub fn process_files(file_paths: &[String], skip_time: Option<SkipTime>) -> Result<()> {
    if file_paths.is_empty() {
        return Err(PolarWarpError::NoValidData);
    }
    
    // If there's only one file, just process it and return
    if file_paths.len() == 1 {
        process_file(&file_paths[0], skip_time)?;
        return Ok(());
    }
    
    // Process each file and collect the DataFrames and stats
    let mut all_dataframes = Vec::with_capacity(file_paths.len());
    let mut all_stats = Vec::with_capacity(file_paths.len());
    let mut all_throughput_metrics = Vec::with_capacity(file_paths.len());
    
    for file_path in file_paths {
        let (df, stats) = process_file(file_path, skip_time)?;
        
        // Calculate throughput metrics for the file
        let throughput_metrics = stats::calculate_throughput_metrics(&df, stats.run_time_secs)?;
        
        all_dataframes.push(df);
        all_stats.push(stats);
        all_throughput_metrics.push(throughput_metrics);
    }
    
    println!("\nDone Processing Files... Consolidating Results");
    
    // Find overlapping time range
    let (global_start, global_end) = find_time_range(&all_stats, skip_time)?;
    
    // Calculate consolidated run time
    let consolidated_run_secs = (global_end - global_start).num_microseconds().unwrap() as f64 / 1_000_000.0;
    
    println!("The consolidated running time in h:mm:ss is {}, time in seconds is: {}", 
             format_duration(consolidated_run_secs), consolidated_run_secs);
    
    // Concatenate all DataFrames
    let consolidated_df = concat(&all_dataframes, true)?;
    
    if consolidated_df.height() == 0 {
        println!("No valid data to consolidate.");
        return Ok(());
    }
    
    // Calculate consolidated statistics
    let consolidated_stats = stats::calculate_consolidated_stats(&consolidated_df, consolidated_run_secs)?;
    
    // Combine throughput metrics
    let combined_throughput = stats::combine_throughput_metrics(&all_throughput_metrics, consolidated_run_secs)?;
    
    // Join stats with throughput
    let final_result = stats::join_stats_with_throughput(&consolidated_stats, &combined_throughput)?;
    
    // Format for display
    let formatted_result = formatter::format_dataframe_for_display(
        &final_result, 
        &formatter::default_columns_to_format()
    )?;
    
    // Print consolidated results
    println!("Consolidated Results:");
    formatter::print_dataframe(&formatted_result);
    
    Ok(())
}

/// Find the global time range for consolidation
fn find_time_range(stats: &[FileStats], skip_time: Option<SkipTime>) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let mut global_start = None;
    let mut global_end = None;
    
    for stat in stats {
        let start_time = if let Some(skip) = skip_time {
            let duration = skip.as_duration();
            stat.start_time + chrono::Duration::from_std(duration).unwrap()
        } else {
            stat.start_time
        };
        
        if let Some(current_start) = global_start {
            global_start = Some(max(current_start, start_time));
        } else {
            global_start = Some(start_time);
        }
        
        if let Some(current_end) = global_end {
            global_end = Some(min(current_end, stat.end_time));
        } else {
            global_end = Some(stat.end_time);
        }
    }
    
    let global_start = global_start.ok_or(PolarWarpError::NoValidData)?;
    let global_end = global_end.ok_or(PolarWarpError::NoValidData)?;
    
    if global_start >= global_end {
        println!("No overlapping time range found between files, no Consolidated results are valid.");
        return Err(PolarWarpError::NoOverlappingTimeRange);
    }
    
    Ok((global_start, global_end))
}

/// Format duration as h:mm:ss.f
fn format_duration(seconds: f64) -> String {
    let hours = (seconds as usize) / 3600;
    let minutes = ((seconds as usize) % 3600) / 60;
    let seconds_part = seconds % 60.0;
    
    format!("{}:{:02}:{:06.6}", hours, minutes, seconds_part)
}
