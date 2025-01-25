# polarWarp
A Polars dataFrame implementation of parsing MinIO Warp object testing output logs

  Running this requires the correct python libraries and environment.  I use the "uv" package manager and virtual environment.  

## Setup
Install `uv`,`pixi`, or another package manager, such as `pip`.

For more on `uv` look here: https://docs.astral.sh/uv/

### Example
    user@host:# uv add polars pandas pyarrow zstd

## Running
    user@host:# uv run ./polars-parse-warp16.py                                                                                                                        
    Usage: python ./polars-parse-warp16.py [--help] : Prints this message and exits
    Usage: python ./polars-parse-warp16.py [--skip=<time_to_skip>] <file1> <file2> ...

## Output
    russfellows@Russ-MacStudio polars-warp % uv run ./polars-parse-warp16.py --skip=300s ~/Public/warp-data/4kb/client-4[1-2]/intel-10RGW_100T_4kib_80R15W5D_vdb-master_trial_1.csv.zst
    Using skip value of 0:05:00

    Processing file: /Users/russfellows/Public/warp-data/4kb/client-41/intel-10RGW_100T_4kib_80R15W5D_vdb-master_trial_1.csv.zst
    Skipping rows with 'start' <= 2022-12-12 22:07:38.740472+00:00.
           op bytes_bucket mean_duration_us median_duration_us 90%_duration_us 95%_duration_us 99%_duration_us  max_duration_us avg_obj_Kbytes throughput_MBps       count operation_rate_per_sec
    0  DELETE          NaN      54,976.8687         3,406.3940    103,917.6380    132,677.4890    613,013.8320  78,079,835.7480         0.0000          0.0000     812,707               387.0023
    1     GET      1 - 32k       2,564.0482           947.1700      1,306.5200      1,768.1580     13,031.7820  11,347,666.1390         4.0000         24.1878  13,003,412             6,192.0837
    2     PUT      1 - 32k      53,789.7967         3,678.5405    103,210.0120    130,807.3220    542,707.6000  78,088,455.5950         4.0000          4.5352   2,438,146             1,161.0187

    Processing file: /Users/russfellows/Public/warp-data/4kb/client-42/intel-10RGW_100T_4kib_80R15W5D_vdb-master_trial_1.csv.zst
    Skipping rows with 'start' <= 2022-12-12 22:07:38.740472+00:00.
           op bytes_bucket mean_duration_us median_duration_us 90%_duration_us 95%_duration_us 99%_duration_us  max_duration_us avg_obj_Kbytes throughput_MBps       count operation_rate_per_sec
    0  DELETE          NaN      55,455.2898         3,416.8930    104,170.6140    132,926.0150    611,289.2330  78,172,682.7070         0.0000          0.0000     811,490               386.4235
    1     GET      1 - 32k       2,593.7859           949.0460      1,310.3860      1,785.5360     13,298.0370  12,451,971.1640         4.0000         24.1515  12,983,854             6,182.7828
    2     PUT      1 - 32k      53,644.0338         3,679.8030    103,155.5860    130,730.9040    546,605.8690  78,090,811.2120         4.0000          4.5284   2,434,479             1,159.2748
    Consolidated output skipping rows with 'start' <= 2022-12-12 22:07:38.739660+00:00.

    Consolidated Results:
           op bytes_bucket mean_duration_us median_duration_us 90%_duration_us 95%_duration_us 99%_duration_us avg_obj_Kbytes consolidated_throughput_MBps total_count consolidated_ops_/_sec
    0  DELETE          NaN      55,215.9534         3,411.5210    104,037.6320    132,795.5090    612,458.2230         0.0000                       0.0000   1,624,197               773.4265
    1     GET      1 - 32k       2,578.9062           948.1000      1,308.4390      1,776.6610     13,161.5250         4.0000                      48.3393  25,987,266            12,374.8788
    2     PUT      1 - 32k      53,717.0086         3,679.1650    103,182.7300    130,773.2030    545,098.3120         4.0000                       9.0636   4,872,625             2,320.2958


