use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use polars::prelude::*;
use regex::Regex;
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::types::{FileStats, PolarWarpError, Result, SkipTime};

/// Parse a file path and return a DataFrame
pub fn parse_file(file_path: &str, skip_time: Option<SkipTime>) -> Result<(DataFrame, FileStats)> {
    println!("Processing file: {}", file_path);
    
    // Create a reader based on the file extension
    let reader = create_reader(file_path)?;
    
    // Read the CSV data
    let df = CsvReader::new(reader)
        .has_header(true)
        .with_separator(b'\t')
        .with_ignore_errors(true)
        .finish()?;
    
    // Parse datetime columns
    let df = parse_datetime_columns(df)?;
    
    // Get file stats
    let stats = get_file_stats(&df, file_path)?;
    
    // Apply skip time if provided
    let df = if let Some(skip) = skip_time {
        let threshold = stats.start_time + Duration::from_std(skip.as_duration()).unwrap();
        println!("Skipping rows with 'start' <= {}.", threshold);
        
        df.filter(&col("start").gt(lit(threshold)))?
    } else {
        df
    };
    
    println!("The file run time in h:mm:ss is {}, time in seconds is: {}", 
        format_duration(stats.run_time_secs), stats.run_time_secs);
    
    Ok((df, stats))
}

/// Parse time skip from string (e.g., "90s", "5m")
pub fn parse_skip_time(skip_str: &str) -> Result<SkipTime> {
    let re = Regex::new(r"^(\d+)([sm])$").unwrap();
    
    if let Some(caps) = re.captures(skip_str) {
        let value = caps[1].parse::<u64>().map_err(|_| {
            PolarWarpError::InvalidSkipTimeFormat(format!("Invalid number in skip time: {}", skip_str))
        })?;
        
        match &caps[2] {
            "s" => Ok(SkipTime::Seconds(value)),
            "m" => Ok(SkipTime::Minutes(value)),
            _ => Err(PolarWarpError::InvalidSkipTimeFormat(format!("Invalid unit in skip time: {}", skip_str))),
        }
    } else {
        Err(PolarWarpError::InvalidSkipTimeFormat(format!("Invalid skip time format: {}", skip_str)))
    }
}

/// Format duration as h:mm:ss
fn format_duration(seconds: f64) -> String {
    let hours = (seconds as usize) / 3600;
    let minutes = ((seconds as usize) % 3600) / 60;
    let seconds_part = seconds % 60.0;
    
    format!("{}:{:02}:{:06.6}", hours, minutes, seconds_part)
}

/// Create a reader based on file extension
fn create_reader(file_path: &str) -> Result<Box<dyn Read>> {
    let file = File::open(file_path).map_err(|e| {
        PolarWarpError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Error opening file '{}': {}", file_path, e),
        ))
    })?;
    
    let reader = BufReader::new(file);
    
    if file_path.ends_with(".zst") {
        let zstd_reader = ZstdDecoder::new(reader).map_err(|e| {
            PolarWarpError::Zstd(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Error creating ZSTD decoder for '{}': {}", file_path, e),
            ))
        })?;
        
        Ok(Box::new(zstd_reader))
    } else {
        Ok(Box::new(reader))
    }
}

/// Parse the 'start' and 'end' columns as datetime
fn parse_datetime_columns(df: DataFrame) -> Result<DataFrame> {
    // Handle ISO 8601 timestamps that might end with Z (UTC indicator)
    let df = df.with_column(
        col("start")
            .str()
            .replace_all(lit("Z$"), lit("+00:00"), true)
            .alias("start")
    )?;
    
    let df = df.with_column(
        col("end")
            .str()
            .replace_all(lit("Z$"), lit("+00:00"), true)
            .alias("end")
    )?;
    
    // Parse the modified strings to datetime
    let df = df.with_column(
        col("start")
            .str()
            .strptime(
                DataType::Datetime(TimeUnit::Microseconds, None),
                "%Y-%m-%dT%H:%M:%S%.f%z",
                false,
            )
            .alias("start")
    )?;
    
    let df = df.with_column(
        col("end")
            .str()
            .strptime(
                DataType::Datetime(TimeUnit::Microseconds, None),
                "%Y-%m-%dT%H:%M:%S%.f%z",
                false,
            )
            .alias("end")
    )?;
    
    Ok(df)
}

/// Extract start time, end time, and run time from a DataFrame
fn get_file_stats(df: &DataFrame, file_path: &str) -> Result<FileStats> {
    // Get the first non-null start time
    let start_time = match df.column("start")? {
        s if s.null_count() == s.len() => {
            return Err(PolarWarpError::NoValidData);
        }
        s => {
            let datetime_series = s.datetime()?;
            let mut start_time = None;
            
            for i in 0..datetime_series.len() {
                if let Some(ts) = datetime_series.get(i) {
                    let dt = DateTime::<Utc>::from_timestamp_micros(ts).ok_or_else(|| {
                        PolarWarpError::TimeParseError(format!("Invalid timestamp: {}", ts))
                    })?;
                    start_time = Some(dt);
                    break;
                }
            }
            
            start_time.ok_or(PolarWarpError::NoValidData)?
        }
    };
    
    // Get the last non-null end time
    let end_time = match df.column("end")? {
        s if s.null_count() == s.len() => {
            return Err(PolarWarpError::NoValidData);
        }
        s => {
            let datetime_series = s.datetime()?;
            let mut end_time = None;
            
            for i in (0..datetime_series.len()).rev() {
                if let Some(ts) = datetime_series.get(i) {
                    let dt = DateTime::<Utc>::from_timestamp_micros(ts).ok_or_else(|| {
                        PolarWarpError::TimeParseError(format!("Invalid timestamp: {}", ts))
                    })?;
                    end_time = Some(dt);
                    break;
                }
            }
            
            end_time.ok_or(PolarWarpError::NoValidData)?
        }
    };
    
    let run_time_secs = (end_time - start_time).num_microseconds().unwrap() as f64 / 1_000_000.0;
    
    Ok(FileStats {
        file_path: file_path.to_string(),
        start_time,
        end_time,
        run_time_secs,
    })
}
