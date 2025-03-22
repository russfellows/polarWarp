use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::Path;
use std::time::Instant;

use clap::{Parser, ArgAction};
use polars::prelude::*;

/// CLI arguments
#[derive(Parser)]
#[command(
    name = "polarwarp-rs",
    version = env!("CARGO_PKG_VERSION"),
    about = "A Rust implementation of polarWarp for processing MinIO Warp output logs",
    long_about = "PolarWarp-rs processes MinIO Warp output logs to provide performance metrics."
)]
struct Args {
    /// Skip a specified amount of time from the start of each file
    /// e.g. "90s" for 90 seconds or "5m" for 5 minutes
    #[arg(short, long)]
    skip: Option<String>,

    /// Input files to process (CSV format, can be ZSTD compressed)
    #[arg(required = true, action = ArgAction::Append)]
    files: Vec<String>,

    /// Just print basic stats without processing
    #[arg(long)]
    basic_stats: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let args = Args::parse();

    // Process each file
    for file_path in &args.files {
        println!("Processing file: {}", file_path);
        
        let start = Instant::now();
        
        // Read and display file info
        process_file(file_path, args.basic_stats)?;
        
        let elapsed = start.elapsed();
        println!("Processed file in {:.2?}", elapsed);
    }

    Ok(())
}

/// Process a single file
fn process_file(file_path: &str, basic_stats_only: bool) -> Result<(), Box<dyn std::error::Error>> {
    // Read the file
    let df = read_csv_file(file_path)?;
    
    // Print basic statistics
    print_basic_stats(&df);
    
    // If only basic stats were requested, we're done
    if basic_stats_only {
        return Ok(());
    }
    
    // TODO: Add more complete processing in the future
    println!("Note: Full processing functionality not yet implemented");
    
    Ok(())
}

/// Print basic stats about the dataframe
fn print_basic_stats(df: &DataFrame) {
    println!("Shape: {} rows × {} columns", df.height(), df.width());
    
    // Print column names and types
    println!("\nColumns:");
    df.get_column_names().iter().for_each(|name| {
        println!("  - {}: {}", name, df.column(name).unwrap().dtype());
    });
    
    // Print first few rows
    println!("\nSample data (first 5 rows):");
    let sample = df.head(Some(5));
    println!("{}", sample);
}

/// Read a CSV file and return a DataFrame
fn read_csv_file(file_path: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let path = Path::new(file_path);
    
    // Use different reader based on file extension
    if file_path.ends_with(".zst") {
        // For ZSTD compressed files, we need to uncompress first
        let content = read_zst_file(path)?;
        
        // Use Cursor to read from memory
        let cursor = Cursor::new(content);
        
        let df = CsvReader::new(cursor)
            .with_separator(b'\t')
            .has_header(true)
            .with_ignore_errors(true)
            .finish()?;
        
        Ok(df)
    } else {
        // For regular CSV files
        let df = CsvReader::from_path(path)?
            .with_separator(b'\t')
            .has_header(true)
            .with_ignore_errors(true)
            .finish()?;
        
        Ok(df)
    }
}

/// Read a ZSTD compressed file into memory
fn read_zst_file(path: &Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut decoder = zstd::Decoder::new(reader)?;
    
    let mut buffer = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    
    Ok(buffer)
}
