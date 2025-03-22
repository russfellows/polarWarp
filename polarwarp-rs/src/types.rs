use chrono::{DateTime, Utc};
use std::time::Duration;
use thiserror::Error;
use polars::prelude::PolarsError;

/// Custom error types for the application
#[derive(Error, Debug)]
pub enum PolarWarpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    
    #[error("Zstd error: {0}")]
    Zstd(#[from] std::io::Error),
    
    #[error("Time parse error: {0}")]
    TimeParseError(String),
    
    #[error("No valid data found")]
    NoValidData,
    
    #[error("No overlapping time range found between files")]
    NoOverlappingTimeRange,
    
    #[error("Missing required column: {0}")]
    MissingColumn(String),

    #[error("Invalid skip time format: {0}")]
    InvalidSkipTimeFormat(String),
}

/// Represents the time skip configuration
#[derive(Debug, Clone, Copy)]
pub enum SkipTime {
    Seconds(u64),
    Minutes(u64),
}

impl SkipTime {
    pub fn as_duration(&self) -> Duration {
        match self {
            SkipTime::Seconds(s) => Duration::from_secs(*s),
            SkipTime::Minutes(m) => Duration::from_secs(*m * 60),
        }
    }
}

/// Represents size buckets for grouping objects
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ByteBucket {
    None,
    Small,      // 1 - 32k
    Medium,     // 32k - 128k
    Large,      // 128k - 1mb
    XLarge,     // 1m - 8mb
    XXLarge,    // 8m - 64mb
    XXXLarge,   // 64m - 999mb
    Gigantic,   // >= 1 gb
}

impl ByteBucket {
    pub fn from_bytes(bytes: i64) -> Self {
        match bytes {
            0 => ByteBucket::None,
            1..=32767 => ByteBucket::Small,
            32768..=131071 => ByteBucket::Medium,
            131072..=1048575 => ByteBucket::Large,
            1048576..=8388607 => ByteBucket::XLarge,
            8388608..=67108863 => ByteBucket::XXLarge,
            67108864..=1047527422 => ByteBucket::XXXLarge,
            _ => ByteBucket::Gigantic,
        }
    }

    pub fn to_bucket_number(&self) -> u8 {
        match self {
            ByteBucket::None => 0,
            ByteBucket::Small => 1,
            ByteBucket::Medium => 2,
            ByteBucket::Large => 3,
            ByteBucket::XLarge => 4,
            ByteBucket::XXLarge => 5,
            ByteBucket::XXXLarge => 6,
            ByteBucket::Gigantic => 7,
        }
    }

    pub fn to_string(&self) -> &'static str {
        match self {
            ByteBucket::None => "None",
            ByteBucket::Small => "1 - 32k",
            ByteBucket::Medium => "32k - 128k",
            ByteBucket::Large => "128k - 1mb",
            ByteBucket::XLarge => "1m - 8mb",
            ByteBucket::XXLarge => "8m - 64mb",
            ByteBucket::XXXLarge => "64m - 999mb",
            ByteBucket::Gigantic => ">= 1 gb",
        }
    }
}

/// Represents file processing statistics
#[derive(Debug, Clone)]
pub struct FileStats {
    pub file_path: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub run_time_secs: f64,
}

/// Result type used throughout the application
pub type Result<T> = std::result::Result<T, PolarWarpError>;
