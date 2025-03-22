use polars::prelude::*;
use crate::types::{ByteBucket, Result, PolarWarpError};

/// Add byte bucket columns to a DataFrame
pub fn add_byte_buckets(df: &mut DataFrame) -> Result<()> {
    let bytes_col = df.column("bytes")?;
    
    // Create the bytes_bucket column (string representation)
    let bytes_bucket = bytes_col.i64()?
        .into_iter()
        .map(|opt_val| {
            opt_val.map(|val| ByteBucket::from_bytes(val).to_string())
        })
        .collect::<StringChunked>()
        .into_series()
        .rename("bytes_bucket");
    
    // Create the bucket_# column (numeric representation)
    let bucket_num = bytes_col.i64()?
        .into_iter()
        .map(|opt_val| {
            opt_val.map(|val| ByteBucket::from_bytes(val).to_bucket_number() as i64)
        })
        .collect::<Int64Chunked>()
        .into_series()
        .rename("bucket_#");
    
    // Add the new columns to the DataFrame
    df.with_column(bytes_bucket)?;
    df.with_column(bucket_num)?;
    
    Ok(())
}

/// Get the list of bucket names in order
pub fn bucket_order() -> Vec<&'static str> {
    vec![
        "None",
        "1 - 32k",
        "32k - 128k",
        "128k - 1mb",
        "1m - 8mb",
        "8m - 64mb",
        "64m - 999mb",
        ">= 1 gb",
    ]
}

/// Add bytes_bucket column using Polars expressions
/// This is an alternative implementation using Polars expressions
pub fn add_byte_buckets_expr(df: &mut DataFrame) -> Result<()> {
    let bytes_bucket_expr = when(col("bytes").eq(lit(0))).then(lit(NULL))
        .when(col("bytes").ge(lit(1)).and(col("bytes").lt(lit(32768)))).then(lit("1 - 32k"))
        .when(col("bytes").ge(lit(32768)).and(col("bytes").lt(lit(131072)))).then(lit("32k - 128k"))
        .when(col("bytes").ge(lit(131072)).and(col("bytes").lt(lit(1048576)))).then(lit("128k - 1mb"))
        .when(col("bytes").ge(lit(1048576)).and(col("bytes").lt(lit(8388608)))).then(lit("1m - 8mb"))
        .when(col("bytes").ge(lit(8388608)).and(col("bytes").lt(lit(67108864)))).then(lit("8m - 64mb"))
        .when(col("bytes").ge(lit(67108864)).and(col("bytes").lt(lit(1047527423)))).then(lit("64m - 999mb"))
        .otherwise(lit(">= 1 gb")).alias("bytes_bucket");

    let bucket_num_expr = when(col("bytes").eq(lit(0))).then(lit(0))
        .when(col("bytes").ge(lit(1)).and(col("bytes").lt(lit(32768)))).then(lit(1))
        .when(col("bytes").ge(lit(32768)).and(col("bytes").lt(lit(131072)))).then(lit(2))
        .when(col("bytes").ge(lit(131072)).and(col("bytes").lt(lit(1048576)))).then(lit(3))
        .when(col("bytes").ge(lit(1048576)).and(col("bytes").lt(lit(8388608)))).then(lit(4))
        .when(col("bytes").ge(lit(8388608)).and(col("bytes").lt(lit(67108864)))).then(lit(5))
        .when(col("bytes").ge(lit(67108864)).and(col("bytes").lt(lit(1047527423)))).then(lit(6))
        .otherwise(lit(7)).alias("bucket_#");

    df.with_column(bytes_bucket_expr.to_series()?)?;
    df.with_column(bucket_num_expr.to_series()?)?;

    Ok(())
}
