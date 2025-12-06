# SPDX-FileCopyrightText: 2025 Russ Fellows <russ.fellows@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
#################################
#
# Program Description:  This program is designed to process the Minio Warp tool output files.
#
#    Using the Polars package to efficiently process data, this program operates approximately 
#    20x faster than the default warp processing tool.  Additionally, this version uses about 
#    10X *less* memory than the warp program, perhaps less.  If memory usage is still an issue, 
#    this program may be modified, by reducing the "bytes_bucket" dataFrame based on the 
#    values in the column "bytes".  Each reduction in the decision tree, reduces memory usage 
#    by about 10 - 15%.  Currently, there are 8 buckets.  Reducing this to 4 buckets, would 
#    cut memory use about 50%. 
# 
#    If multiple files are given on the command line to process, it will provide statistics 
#    for each file individually, and then attempt to combine the results, if the runtimes 
#    overlap.  If there is no overlapping time, then no consolidated results can be derived.   
#
# Python Environment:  HIGHLY recommend using a modern package manager such as "uv" or "pixi" 
#    to manage packages.  If you insist, old relics such as conda may work, but I wouldn't 
#    count on it, pip is probably fine.
#
# Example: if using uv, the following should add the necessary libraries
#  uv add polars pyarrow zstandard zstd
#
################################
import polars as pl
from datetime import datetime, timedelta
import sys
import re

# Metadata operations that should be grouped together (matching Rust implementation)
META_OPS = ["LIST", "HEAD", "DELETE", "STAT"]

# Function to pretty up the output, by adding commas for readability, and using 4 digits for float
def format_with_commas(value):
    if isinstance(value, (int, float)):
        if isinstance(value, float):
            return f"{value:,.2f}"
            #return f"{value:,.4f}"
        else:
            return f"{value:,}"
    return value  # Return the value unchanged if it's not numeric


def compute_summary_rows(df, run_time_secs):
    """
    Compute summary rows for operation categories (META, GET, PUT).
    Returns a list of summary row dictionaries with statistically valid percentiles.
    """
    summary_rows = []
    
    # Define operation categories: (category_name, operations_list, bucket_idx)
    categories = [
        ("META", META_OPS, 97),
        ("GET", ["GET"], 98),
        ("PUT", ["PUT"], 99),
    ]
    
    for category_name, ops_list, bucket_idx in categories:
        # Filter to just this category
        category_df = df.filter(pl.col("op").is_in(ops_list))
        
        if category_df.height() == 0:
            continue
        
        # Compute statistically valid percentiles on ALL raw data
        stats = category_df.select([
            (pl.col("duration_ns").mean() / 1000).alias("mean_lat_us"),
            (pl.col("duration_ns").median() / 1000).alias("med._lat_us"),
            (pl.col("duration_ns").quantile(0.90) / 1000).alias("90%_lat_us"),
            (pl.col("duration_ns").quantile(0.95) / 1000).alias("95%_lat_us"),
            (pl.col("duration_ns").quantile(0.99) / 1000).alias("99%_lat_us"),
            (pl.col("duration_ns").max() / 1000).alias("max_lat_us"),
            (pl.col("bytes").mean() / 1024).alias("avg_obj_KB"),
            (pl.count("op") / run_time_secs).alias("ops_/_sec"),
            ((pl.col("bytes").sum() / (1024 * 1024)) / run_time_secs).alias("xput_MBps"),
            pl.count("op").alias("count"),
        ])
        
        row = stats.row(0, named=True)
        row["op"] = category_name
        row["bytes_bucket"] = "ALL"
        row["bucket_#"] = bucket_idx
        summary_rows.append(row)
    
    return summary_rows


def print_summary_rows(summary_rows, columns_to_format):
    """Print summary rows with formatting."""
    import pandas as pd
    
    if not summary_rows:
        return
    
    print()  # Separator line
    
    summary_df = pd.DataFrame(summary_rows)
    # Reorder columns to match main output
    column_order = ["op", "bytes_bucket", "bucket_#", "mean_lat_us", "med._lat_us", 
                    "90%_lat_us", "95%_lat_us", "99%_lat_us", "max_lat_us", 
                    "avg_obj_KB", "ops_/_sec", "xput_MBps", "count"]
    summary_df = summary_df[[c for c in column_order if c in summary_df.columns]]
    
    for column in columns_to_format:
        if column in summary_df:
            summary_df[column] = summary_df[column].map(format_with_commas)
    
    # Print without index, matching main output style
    print(summary_df.to_string(index=False))


#######

script_name = sys.argv[0]

# Check command line args, give basic usage
if len(sys.argv) < 2:
    print(f"Usage: python {script_name} [--help] : Prints this message and exits")
    print(f"Usage: python {script_name} [--skip=<time_to_skip>] <file1> <file2> ...")
    sys.exit(1)

# Process the --skip argument
skip_time = None
file_paths = []
skip_pattern = re.compile(r"--skip=(\d+)([sm])")

# Process --help flag
if "--help" in sys.argv:
    print(f"""
Usage: python {script_name} [--skip=<time_to_skip>] <file1> <file2> ...

Options:
  --skip=<time_to_skip>  Skip a specified amount of time from the start of each file. 
                         Example: --skip=90s (90 seconds) or --skip=5m (5 minutes).
  --help                 Show this help message and exit.
""")
    sys.exit(0)

# Now process remaining arguments
for arg in sys.argv[1:]:

    match = skip_pattern.match(arg)
    if match:
        value, unit = match.groups()
        value = int(value)
        if unit == "s":
            skip_time = timedelta(seconds=value)
        elif unit == "m":
            skip_time = timedelta(minutes=value)
        print(f"Using skip value of {skip_time}")
    else:
        file_paths.append(arg)

if not file_paths:
    print("Error: No input files provided.")
    sys.exit(1)

# Create empty dataFrame for consolidate results
consolidated_df = pl.DataFrame()
consolidated_throughput_df = pl.DataFrame() 
consolidated_throughputs = []

# Initialize start and stop values
global_start = None
global_end = None

#
# Primary loop, process each file
#
for file_path in file_paths:
    print(f"\nProcessing file: {file_path}")
    df = pl.read_csv(file_path, ignore_errors=True, separator='\t')

    # Note: parsing the ISO 8601 time is a bit tricky.  If the value ends in a literal capital "Z", then it may cause problems.  
    df = df.with_columns([
        pl.col("start").str.replace("Z$", "+00:00").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False).alias("start"),
        pl.col("end").str.replace("Z$", "+00:00").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False).alias("end"),
    ])

    start_time = None
    start_values_checked = []
    for value in df.select(pl.col("start").drop_nulls()).to_series():
        start_values_checked.append(value)
        if value is not None:
            start_time = value
            break

    end_time = None
    end_values_checked = []
    for value in reversed(df.select(pl.col("end").drop_nulls()).to_series()):
        end_values_checked.append(value)
        if value is not None:
            end_time = value
            break

    # If this error is raised, likey a time parsing issue, see code block up 30 lines and ISO 8601 parsing, particularly the TZ
    if start_time is None or end_time is None:
        raise ValueError("Start time or end time could not be determined. Check input data, it may be a timezone issue, is there a <Z> at the end?")

    if global_start is None or global_end is None:
        if skip_time is not None:
            global_start = start_time + skip_time
        else:
            global_start = start_time

        global_end = end_time
    else:
        global_start = max(global_start, start_time)
        global_end = min(global_end, end_time)

    run_time_secs = (end_time - global_start).total_seconds()
    run_time = (end_time - global_start)

    if skip_time is not None:
        threshold_time = start_time + skip_time
        print(f"Skipping rows with 'start' <= {threshold_time}.")
        df = df.filter(pl.col("start") > threshold_time)

    print(f"The file run time in h:mm:ss is {run_time}, time in seconds is: {run_time_secs}")

# Define the bucket order (matching sai3-bench/polarwarp-rs)
    bucket_order = ["zero", "1B-8KiB", "8KiB-64KiB", "64KiB-512KiB", "512KiB-4MiB", "4MiB-32MiB", "32MiB-256MiB", "256MiB-2GiB", ">2GiB"]

# Size bucket boundaries (matching sai3-bench)
    BUCKET_8K = 8 * 1024           # 8 KiB
    BUCKET_64K = 64 * 1024         # 64 KiB
    BUCKET_512K = 512 * 1024       # 512 KiB
    BUCKET_4M = 4 * 1024 * 1024    # 4 MiB
    BUCKET_32M = 32 * 1024 * 1024  # 32 MiB
    BUCKET_256M = 256 * 1024 * 1024  # 256 MiB
    BUCKET_2G = 2 * 1024 * 1024 * 1024  # 2 GiB

# Create buckets for byte ranges (matching sai3-bench bucket definitions)
    df = df.with_columns([
        pl.when(pl.col("bytes") == 0).then(pl.lit("zero"))
        .when((pl.col("bytes") >= 1) & (pl.col("bytes") < BUCKET_8K)).then(pl.lit("1B-8KiB"))
        .when((pl.col("bytes") >= BUCKET_8K) & (pl.col("bytes") < BUCKET_64K)).then(pl.lit("8KiB-64KiB"))
        .when((pl.col("bytes") >= BUCKET_64K) & (pl.col("bytes") < BUCKET_512K)).then(pl.lit("64KiB-512KiB"))
        .when((pl.col("bytes") >= BUCKET_512K) & (pl.col("bytes") < BUCKET_4M)).then(pl.lit("512KiB-4MiB"))
        .when((pl.col("bytes") >= BUCKET_4M) & (pl.col("bytes") < BUCKET_32M)).then(pl.lit("4MiB-32MiB"))
        .when((pl.col("bytes") >= BUCKET_32M) & (pl.col("bytes") < BUCKET_256M)).then(pl.lit("32MiB-256MiB"))
        .when((pl.col("bytes") >= BUCKET_256M) & (pl.col("bytes") < BUCKET_2G)).then(pl.lit("256MiB-2GiB"))
        .otherwise(pl.lit(">2GiB")).alias("bytes_bucket"),
        pl.when(pl.col("bytes") == 0).then(0)
        .when((pl.col("bytes") >= 1) & (pl.col("bytes") < BUCKET_8K)).then(1)
        .when((pl.col("bytes") >= BUCKET_8K) & (pl.col("bytes") < BUCKET_64K)).then(2)
        .when((pl.col("bytes") >= BUCKET_64K) & (pl.col("bytes") < BUCKET_512K)).then(3)
        .when((pl.col("bytes") >= BUCKET_512K) & (pl.col("bytes") < BUCKET_4M)).then(4)
        .when((pl.col("bytes") >= BUCKET_4M) & (pl.col("bytes") < BUCKET_32M)).then(5)
        .when((pl.col("bytes") >= BUCKET_32M) & (pl.col("bytes") < BUCKET_256M)).then(6)
        .when((pl.col("bytes") >= BUCKET_256M) & (pl.col("bytes") < BUCKET_2G)).then(7)
        .otherwise(8).alias("bucket_#")
    ])

# Now group the results by operation type and our bucket sizes
    result = df.group_by(["op", "bytes_bucket", "bucket_#"]).agg([
        (pl.col("duration_ns").mean() / 1000).alias("mean_lat_us"),
        (pl.col("duration_ns").median() / 1000).alias("med._lat_us"),
        (pl.col("duration_ns").quantile(0.90) / 1000).alias("90%_lat_us"),
        (pl.col("duration_ns").quantile(0.95) / 1000).alias("95%_lat_us"),
        (pl.col("duration_ns").quantile(0.99) / 1000).alias("99%_lat_us"),
        (pl.col("duration_ns").max() / 1000).alias("max_lat_us"),
        (pl.col("bytes").mean() / 1024).alias("avg_obj_KB"),
        (pl.count("op") / run_time_secs).alias("ops_/_sec"),
        ((pl.col("bytes").sum() / (1024 * 1024)) / run_time_secs).alias("xput_MBps"),
        pl.count("op").alias("count")
    ])

    # Ensure throughput is in Float64 format for consistency
    result = result.with_columns(pl.col("xput_MBps").cast(pl.Float64))

    # Calculate throughput metrics for the current file
    throughput_metrics = df.group_by("op", "bytes_bucket").agg([
        ((pl.col("bytes").sum() / (1024 * 1024)) / run_time_secs).alias("xput_MBps"),
        pl.count("op").alias("count"),
    ])

    # Ensure 'op' column is of type Utf8
    throughput_metrics = throughput_metrics.with_columns(pl.col("op").cast(pl.Utf8))

    final_result = result.sort(["bucket_#", "op"])
    final_result_pd = final_result.to_pandas()

# List of columns to send to the pretty comma-fyer
    columns_to_format = [
        "med._lat_us",
        "90%_lat_us",
        "95%_lat_us",
        "99%_lat_us",
        "mean_lat_us",
        "max_lat_us",
        "ops_/_sec",
        "count",
        "avg_obj_KB",
        "xput_MBps",
    ]
    for column in columns_to_format:
        if column in final_result_pd:
            final_result_pd[column] = final_result_pd[column].map(format_with_commas)

    print(final_result_pd)

    # Print summary rows for META, GET, PUT (with statistically valid percentiles)
    summary_rows = compute_summary_rows(df, run_time_secs)
    print_summary_rows(summary_rows, columns_to_format)

    consolidated_df = pl.concat([consolidated_df, df])

    # Append the metrics to consolidated_throughputs
    #consolidated_throughput_df = pl.concat([consolidated_throughput_df, throughput_metrics])
    consolidated_throughputs.append(throughput_metrics)


# Done processing each file

# If there was only one file to parse, we are done, not consolidation needed
if len(file_paths) == 1:
    sys.exit(0)

print(f"\nDone Processing Files... Consolidating Results")

if global_start >= global_end:
    print("No overlapping time range found between files, no Consolidated results are valid.")
    sys.exit(1)

consolidated_run_time = (global_end - global_start)
consolidated_run_secs = (global_end - global_start).total_seconds() 
print(f"The consolidated running time in h:mm:ss is {consolidated_run_time}, time in seconds is: {consolidated_run_secs}")

# Adjust consolidated_stats to join on both "op" and "bytes_bucket"
if consolidated_df.is_empty():
    print("No valid data to consolidate.")
    sys.exit(1)

consolidated_stats = consolidated_df.group_by(["op", "bytes_bucket", "bucket_#"]).agg([
    (pl.col("duration_ns").mean() / 1000).alias("mean_lat_us"),
    (pl.col("duration_ns").median() / 1000).alias("med._lat_us"),
    (pl.col("duration_ns").quantile(0.90) / 1000).alias("90%_lat_us"),
    (pl.col("duration_ns").quantile(0.95) / 1000).alias("95%_lat_us"),
    (pl.col("duration_ns").quantile(0.99) / 1000).alias("99%_lat_us"),
    (pl.col("bytes").mean() / 1024).alias("avg_obj_KB"),
    pl.count("op").alias("tot_count"),
])


# Combine all throughput metrics into a single DataFrame, grouped by "op" and "bytes_bucket"
if consolidated_throughputs:
    combined_throughputs = pl.concat(consolidated_throughputs).group_by(["op", "bytes_bucket"]).agg([
        pl.col("xput_MBps").sum().alias("total_xput_MBps"),
        (pl.col("count").sum() / consolidated_run_secs).alias("tot_ops_/_sec"),
    ])
else:
    combined_throughputs = pl.DataFrame({
        "op": [], "bytes_bucket": [], "total_xput_MBps": [], "tot_ops_/_sec": []
    })

# Join consolidated throughput metrics on "op" and "bytes_bucket"
consolidated_stats = consolidated_stats.join(combined_throughputs, on=["op", "bytes_bucket"], how="left")
consolidated_stats = consolidated_stats.sort(["bucket_#", "op"])

# Ensure all expected columns are present and in the desired order
desired_column_order = [
    "op", "bytes_bucket", "bucket_#", "mean_lat_us", "med._lat_us",
    "90%_lat_us", "95%_lat_us", "99%_lat_us", "avg_obj_KB",
    "tot_ops_/_sec", "total_xput_MBps", "tot_count",
]

consolidated_stats = consolidated_stats.select(desired_column_order).sort(["bucket_#", "op"])

# Convert to pandas for final output formatting
consolidated_stats_pd = consolidated_stats.to_pandas()

columns_to_format = [
    "mean_lat_us",
    "med._lat_us",
    "90%_lat_us",
    "95%_lat_us",
    "99%_lat_us",
    "avg_obj_KB",
    "tot_ops_/_sec",
    "total_xput_MBps",
    "tot_count",
]
for column in columns_to_format:
    if column in consolidated_stats_pd:
        consolidated_stats_pd[column] = consolidated_stats_pd[column].map(format_with_commas)

print("Consolidated Results:")
print(consolidated_stats_pd)

# Print summary rows for consolidated results (with statistically valid percentiles)
summary_rows = compute_summary_rows(consolidated_df, consolidated_run_secs)
print_summary_rows(summary_rows, columns_to_format)
