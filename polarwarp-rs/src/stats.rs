use polars::prelude::*;
use crate::types::{Result, PolarWarpError};

/// Calculate statistics on a DataFrame for a single file
pub fn calculate_file_stats(df: &DataFrame, run_time_secs: f64) -> Result<DataFrame> {
    let result = df.group_by(["op", "bytes_bucket", "bucket_#"])?
        .agg([
            (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
            (col("duration_ns").median() / lit(1000.0)).alias("med._lat_us"),
            (col("duration_ns").quantile(lit(0.90)) / lit(1000.0)).alias("90%_lat_us"),
            (col("duration_ns").quantile(lit(0.95)) / lit(1000.0)).alias("95%_lat_us"),
            (col("duration_ns").quantile(lit(0.99)) / lit(1000.0)).alias("99%_lat_us"),
            (col("duration_ns").max() / lit(1000.0)).alias("max_lat_us"),
            (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
            (count().lit_to_series() / lit(run_time_secs)).alias("ops_/_sec"),
            ((col("bytes").sum() / lit(1024.0 * 1024.0)) / lit(run_time_secs)).alias("xput_MBps"),
            count().alias("count"),
        ])?;

    let result = result.with_column(
        col("xput_MBps").cast(DataType::Float64)
    )?;

    // Sort by bucket number and operation
    let result = result.sort(["bucket_#", "op"], vec![false, false])?;

    Ok(result)
}

/// Calculate throughput metrics for a file
pub fn calculate_throughput_metrics(df: &DataFrame, run_time_secs: f64) -> Result<DataFrame> {
    let metrics = df.group_by(["op", "bytes_bucket"])?
        .agg([
            ((col("bytes").sum() / lit(1024.0 * 1024.0)) / lit(run_time_secs)).alias("xput_MBps"),
            count().alias("count"),
        ])?;

    let metrics = metrics.with_column(
        col("op").cast(DataType::Utf8)
    )?;

    Ok(metrics)
}

/// Calculate consolidated statistics from multiple files
pub fn calculate_consolidated_stats(df: &DataFrame, run_time_secs: f64) -> Result<DataFrame> {
    let stats = df.group_by(["op", "bytes_bucket", "bucket_#"])?
        .agg([
            (col("duration_ns").mean() / lit(1000.0)).alias("mean_lat_us"),
            (col("duration_ns").median() / lit(1000.0)).alias("med._lat_us"),
            (col("duration_ns").quantile(lit(0.90)) / lit(1000.0)).alias("90%_lat_us"),
            (col("duration_ns").quantile(lit(0.95)) / lit(1000.0)).alias("95%_lat_us"),
            (col("duration_ns").quantile(lit(0.99)) / lit(1000.0)).alias("99%_lat_us"),
            (col("bytes").mean() / lit(1024.0)).alias("avg_obj_KB"),
            count().alias("tot_count"),
        ])?;

    Ok(stats)
}

/// Combine throughput metrics from multiple files
pub fn combine_throughput_metrics(metrics: &[DataFrame], run_time_secs: f64) -> Result<DataFrame> {
    if metrics.is_empty() {
        return Ok(DataFrame::new(vec![
            Series::new("op", Vec::<String>::new()),
            Series::new("bytes_bucket", Vec::<String>::new()),
            Series::new("total_xput_MBps", Vec::<f64>::new()),
            Series::new("tot_ops_/_sec", Vec::<f64>::new()),
        ])?);
    }

    // Concatenate all throughput metrics
    let combined = concat(metrics, true)?;

    // Group by op and bytes_bucket, sum xput_MBps, and calculate total ops/sec
    let combined = combined.group_by(["op", "bytes_bucket"])?
        .agg([
            col("xput_MBps").sum().alias("total_xput_MBps"),
            (col("count").sum() / lit(run_time_secs)).alias("tot_ops_/_sec"),
        ])?;

    Ok(combined)
}

/// Join consolidated stats with throughput metrics
pub fn join_stats_with_throughput(stats: &DataFrame, throughput: &DataFrame) -> Result<DataFrame> {
    let joined = stats.join(
        throughput,
        &["op", "bytes_bucket"],
        &["op", "bytes_bucket"],
        JoinType::Left,
    )?;

    // Ensure desired column order
    let desired_cols = [
        "op", "bytes_bucket", "bucket_#", "mean_lat_us", "med._lat_us",
        "90%_lat_us", "95%_lat_us", "99%_lat_us", "avg_obj_KB",
        "tot_ops_/_sec", "total_xput_MBps", "tot_count",
    ];

    let joined = joined.select(&desired_cols)?;
    
    // Sort by bucket and op
    let joined = joined.sort(["bucket_#", "op"], vec![false, false])?;

    Ok(joined)
}
