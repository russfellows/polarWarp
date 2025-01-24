##############################################################################
#                                                                            #
# Copyright, 2025, Signal65 / Futurum Group Limited.  All rights reserved.   #
# Author: Russ Fellows                                                       #
#                                                                            #
# Any derivation must include original copyright block                       #
#                                                                            #
##############################################################################
#
#################################
#
# Program Description:  This program is designed to process the Minio Warp tool output files.
#
#    Using the Polars package to efficiently process data, this program operates approximately 
#    20x faster than the default warp processing tool.  Additionally, this version uses about 
#    10X memory than the default program, perhaps less.  If memory usage is still an issue, 
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

# Function to pretty up the output, by adding commas for readability, and using 4 digits for float
def format_with_commas(value):
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.4f}"

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

    df = df.with_columns([
        pl.col("start").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False).alias("start"),
        pl.col("end").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%z", strict=False).alias("end"),
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

    if start_time is None or end_time is None:
        raise ValueError("Start time or end time could not be determined. Check your input data.")

    if global_start is None or global_end is None:
        if skip_time is not None:
            global_start = start_time + skip_time
        else:
            global_start = start_time

        global_end = end_time
    else:
        global_start = max(global_start, start_time)
        global_end = min(global_end, end_time)

    total_elapsed_time_sec = (end_time - global_start).total_seconds()

    if skip_time is not None:
        threshold_time = start_time + skip_time
        print(f"Skipping rows with 'start' <= {threshold_time}.")
        df = df.filter(pl.col("start") > threshold_time)

# Create buckets for byte ranges, so that we can better understand multi object size results
    df = df.with_columns(
        pl.when(pl.col("bytes") == 0).then(pl.lit(None))
        .when((pl.col("bytes") >= 1) & (pl.col("bytes") < 32768)).then(pl.lit("1 - 32k"))
        .when((pl.col("bytes") >= 32768) & (pl.col("bytes") < 131072)).then(pl.lit("32k - 128k"))
        .when((pl.col("bytes") >= 131072) & (pl.col("bytes") < 1048576)).then(pl.lit("128k - 1mb"))
        .when((pl.col("bytes") >= 1048576) & (pl.col("bytes") < 8388608)).then(pl.lit("1m - 8mb"))
        .when((pl.col("bytes") >= 8388608) & (pl.col("bytes") < 67108864)).then(pl.lit("8m - 64mb"))
        .when((pl.col("bytes") >= 67108864) & (pl.col("bytes") < 1047527423)).then(pl.lit("64m - 999mb"))
        .otherwise(pl.lit(">= 1 gb"))
        .cast(pl.Categorical).alias("bytes_bucket")
    )

# Now group the results by operation type and our bucket sizes
    result = df.group_by(["op", "bytes_bucket"]).agg([
        (pl.col("duration_ns").mean() / 1000).alias("mean_duration_us"),
        (pl.col("duration_ns").median() / 1000).alias("median_duration_us"),
        (pl.col("duration_ns").quantile(0.90) / 1000).alias("90%_duration_us"),
        (pl.col("duration_ns").quantile(0.95) / 1000).alias("95%_duration_us"),
        (pl.col("duration_ns").quantile(0.99) / 1000).alias("99%_duration_us"),
        (pl.col("duration_ns").max() / 1000).alias("max_duration_us"),
        (pl.col("bytes").mean() / 1024).alias("avg_obj_Kbytes"),
    ])

    throughput_and_count = df.group_by("op").agg([
        ((pl.col("bytes").sum() / pl.count("op")) * pl.count("op") / (total_elapsed_time_sec * 1024 * 1024)).alias("throughput_MBps"),
        pl.count("op").alias("count"),
        (pl.count("op") / total_elapsed_time_sec).alias("operation_rate_per_sec"),
    ])

    throughput_and_count = throughput_and_count.with_columns(pl.col("throughput_MBps").cast(pl.Float64))
    consolidated_throughputs.append(throughput_and_count)

    final_result = result.join(throughput_and_count, on="op")
    final_result = final_result.sort(["bytes_bucket", "op"])
    final_result_pd = final_result.to_pandas()

# List of columns to send to the pretty comma-fyer
    columns_to_format = [
        "throughput_MBps",
        "median_duration_us",
        "90%_duration_us",
        "95%_duration_us",
        "99%_duration_us",
        "mean_duration_us",
        "max_duration_us",
        "operation_rate_per_sec",
        "count",
        "avg_obj_Kbytes",
    ]
    for column in columns_to_format:
        if column in final_result_pd:
            final_result_pd[column] = final_result_pd[column].map(format_with_commas)

    print(final_result_pd)
    consolidated_df = pl.concat([consolidated_df, df])

# Done processing each file

if global_start >= global_end:
    print("No overlapping time range found between files, no Consolidated results are valid.")
    sys.exit(1)

# Now create consolidated results
if skip_time is not None:
    print(f"Consolidated output skipping rows with 'start' <= {threshold_time}.")

consolidated_df = consolidated_df.filter((pl.col("start") >= global_start) & (pl.col("start") <= global_end))
consolidated_time_range_sec = (global_end - global_start).total_seconds()

combined_throughputs = pl.concat(consolidated_throughputs).group_by("op").agg([
    pl.col("throughput_MBps").sum().alias("consolidated_throughput_MBps"),
    pl.col("count").sum().alias("total_count"),
    (pl.col("count").sum() / consolidated_time_range_sec).alias("consolidated_ops_/_sec"),
])

consolidated_stats = consolidated_df.group_by(["op", "bytes_bucket"]).agg([
    (pl.col("duration_ns").mean() / 1000).alias("mean_duration_us"),
    (pl.col("duration_ns").median() / 1000).alias("median_duration_us"),
    (pl.col("duration_ns").quantile(0.90) / 1000).alias("90%_duration_us"),
    (pl.col("duration_ns").quantile(0.95) / 1000).alias("95%_duration_us"),
    (pl.col("duration_ns").quantile(0.99) / 1000).alias("99%_duration_us"),
    (pl.col("bytes").mean() / 1024).alias("avg_obj_Kbytes"),
])

consolidated_stats = consolidated_stats.join(combined_throughputs, on="op")
consolidated_stats = consolidated_stats.sort(["bytes_bucket", "op"])
consolidated_stats_pd = consolidated_stats.to_pandas()

columns_to_format = [
    "mean_duration_us",
    "median_duration_us",
    "90%_duration_us",
    "95%_duration_us",
    "99%_duration_us",
    "consolidated_throughput_MBps",
    "consolidated_ops_/_sec",
    "total_count",
    "avg_obj_Kbytes",
]
for column in columns_to_format:
    if column in consolidated_stats_pd:
        consolidated_stats_pd[column] = consolidated_stats_pd[column].map(format_with_commas)

print("\nConsolidated Results:")
print(consolidated_stats_pd)

