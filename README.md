# polarWarp
A Polars dataFrame implementation of parsing MinIO Warp object testing output logs.  
Why?  Because it's 37x faster, and produces better output results.  Warp's builtin tools are... painful at best.

  Running this requires the correct python libraries and environment.  I use the "uv" package manager and virtual environment.  

  Note: This program `polarWarp` runs about 37x faster than the builtin MinIO tools `warp merge` + `warp analyze`.  See specific results below.  Notice that polarWarp processes 10 files with a total line count of approximately 55M lines in 40.7 seconds.  To do the same using warp, you first have to merge the 10 files, which takes 5 minutes, and then "analyze" the results, which takes another 20m16s.  

  Note2: All timing and results were performed on a Mac Studio, M1 Ultra with 32 GB RAM and a 1 TB SSD.  

    | Program      | Language + Lib  | Total Lines: Code | Time (real) | Max Res Set Size | Page Reclaims | Page Faults |  | Tot.  File Size | Total Lines  | x Faster | x Less Mem |
    |--------------|-----------------|-------------------|-------------|------------------|---------------|-------------|--|-----------------|--------------|----------|------------|
    | polarWarp    | python + polars | 255 : 161         | 00:40.7     | 18 GB            |    5,379,425  |  135,342    |  | 2.5 GB          |  54,998,687  | 37.25    | 1.13       |
    | warp merge   | Golang          | 122 :  93         | 04:59.9     | 15 GB            |   19,802,572  |      695    |  | 2.5 GB          |  54,998,687  | -        | -          |
    | warp analyze | Golang          | 657 : 579         | 20:16.2     | 20 GB            |  139,747,943  |      237    |  | 2.5 GB          |  54,998,687  | -        | -          |

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
    user@host:# uv run ./polarWarp.py --skip=2m ./warp-mixed-2025-01-26_023730_5S0?.csv.zst 
    Using skip value of 0:02:00

    Processing file: ./warp-mixed-2025-01-26_023730_5S0w.csv.zst
    Skipping rows with 'start' <= 2025-01-26 02:39:33.913005+00:00.
    The file run time in h:mm:ss is 0:03:00.001718, time in seconds is: 180.001718
            op bytes_bucket  bucket_# mean_lat_us med._lat_us 90%_lat_us 95%_lat_us 99%_lat_us  max_lat_us avg_obj_KB ops_/_sec xput_MBps    count
    0   DELETE         None         0    3,516.70    3,365.36   4,496.95   4,999.16   6,425.77   25,166.27       0.00    391.14      0.00   70,405
    1     STAT         None         0    2,075.24    2,003.39   2,699.74   3,022.78   3,932.32  135,365.87       0.00  1,173.38      0.00  211,210
    2      GET      1 - 32k         1    2,247.77    2,149.60   2,913.59   3,255.70   4,186.27  134,553.66       4.83    218.80      1.03   39,384
    3      PUT      1 - 32k         1    6,868.28    6,607.35   8,643.44   9,506.01  11,728.90   24,555.96       4.79     73.06      0.34   13,151
    4      GET   32k - 128k         2    2,456.13    2,364.56   3,188.95   3,537.03   4,524.98   21,507.22     100.11    166.02     16.23   29,883
    5      PUT   32k - 128k         2    7,550.75    7,294.94   9,351.76  10,279.80  12,559.14   24,514.35      99.75     54.59      5.32    9,826
    6      GET   128k - 1mb         3    2,979.84    2,859.45   3,926.61   4,342.32   5,530.64  167,359.27     433.60    661.79    280.23  119,123
    7      PUT   128k - 1mb         3    8,994.19    8,688.34  11,456.24  12,438.35  14,974.51  140,686.50     430.37    221.51     93.10   39,872
    8      GET     1m - 8mb         4    6,631.01    6,058.29  10,105.29  11,216.96  14,729.11  144,246.37   3,452.72    658.12  2,219.05  118,463
    9      PUT     1m - 8mb         4   20,696.40   18,761.90  32,079.34  35,264.57  41,619.41  159,926.07   3,440.51    218.93    735.56   39,407
    10     GET    8m - 64mb         5   12,443.86   12,124.69  14,445.05  16,495.17  24,693.69   33,038.85   8,953.63     55.33    483.77    9,959
    11     PUT    8m - 64mb         5   39,970.53   39,023.14  45,669.31  49,523.29  58,705.33  138,312.27   8,959.63     18.63    163.03    3,354

    Processing file: ./warp-mixed-2025-01-26_023730_5S0x.csv.zst
    Skipping rows with 'start' <= 2025-01-26 02:39:33.913005+00:00.
    The file run time in h:mm:ss is 0:03:00.001718, time in seconds is: 180.001718
            op bytes_bucket  bucket_# mean_lat_us med._lat_us 90%_lat_us 95%_lat_us 99%_lat_us  max_lat_us avg_obj_KB ops_/_sec xput_MBps    count
    0   DELETE         None         0    3,516.70    3,365.36   4,496.95   4,999.16   6,425.77   25,166.27       0.00    391.14      0.00   70,405
    1     STAT         None         0    2,075.24    2,003.39   2,699.74   3,022.78   3,932.32  135,365.87       0.00  1,173.38      0.00  211,210
    2      GET      1 - 32k         1    2,247.77    2,149.60   2,913.59   3,255.70   4,186.27  134,553.66       4.83    218.80      1.03   39,384
    3      PUT      1 - 32k         1    6,868.28    6,607.35   8,643.44   9,506.01  11,728.90   24,555.96       4.79     73.06      0.34   13,151
    4      GET   32k - 128k         2    2,456.13    2,364.56   3,188.95   3,537.03   4,524.98   21,507.22     100.11    166.02     16.23   29,883
    5      PUT   32k - 128k         2    7,550.75    7,294.94   9,351.76  10,279.80  12,559.14   24,514.35      99.75     54.59      5.32    9,826
    6      GET   128k - 1mb         3    2,979.84    2,859.45   3,926.61   4,342.32   5,530.64  167,359.27     433.60    661.79    280.23  119,123
    7      PUT   128k - 1mb         3    8,994.19    8,688.34  11,456.24  12,438.35  14,974.51  140,686.50     430.37    221.51     93.10   39,872
    8      GET     1m - 8mb         4    6,631.01    6,058.29  10,105.29  11,216.96  14,729.11  144,246.37   3,452.72    658.12  2,219.05  118,463
    9      PUT     1m - 8mb         4   20,696.40   18,761.90  32,079.34  35,264.57  41,619.41  159,926.07   3,440.51    218.93    735.56   39,407
    10     GET    8m - 64mb         5   12,443.86   12,124.69  14,445.05  16,495.17  24,693.69   33,038.85   8,953.63     55.33    483.77    9,959
    11     PUT    8m - 64mb         5   39,970.53   39,023.14  45,669.31  49,523.29  58,705.33  138,312.27   8,959.63     18.63    163.03    3,354

    Done Processing Files... Consolidating Results
    The consolidated running time in h:mm:ss is 0:03:00.001718, time in seconds is: 180.001718
    Consolidated Results:
            op bytes_bucket  bucket_# mean_lat_us med._lat_us 90%_lat_us 95%_lat_us 99%_lat_us avg_obj_KB tot_ops_/_sec total_xput_MBps tot_count
    0   DELETE         None         0    3,516.70    3,365.36   4,496.95   4,999.16   6,425.77       0.00           nan             nan   140,810
    1     STAT         None         0    2,075.24    2,003.39   2,699.74   3,022.78   3,932.32       0.00           nan             nan   422,420
    2      GET      1 - 32k         1    2,247.77    2,149.60   2,913.59   3,255.70   4,186.27       4.83        437.60            2.07    78,768
    3      PUT      1 - 32k         1    6,868.28    6,607.35   8,643.44   9,506.01  11,728.90       4.79        146.12            0.68    26,302
    4      GET   32k - 128k         2    2,456.13    2,364.56   3,188.95   3,537.03   4,524.98     100.11        332.03           32.46    59,766
    5      PUT   32k - 128k         2    7,550.75    7,294.94   9,351.76  10,279.80  12,559.14      99.75        109.18           10.64    19,652
    6      GET   128k - 1mb         3    2,979.84    2,859.45   3,926.61   4,342.32   5,530.64     433.60      1,323.58          560.46   238,246
    7      PUT   128k - 1mb         3    8,994.19    8,688.34  11,456.24  12,438.81  14,982.02     430.37        443.02          186.19    79,744
    8      GET     1m - 8mb         4    6,631.01    6,058.29  10,105.29  11,216.96  14,734.52   3,452.72      1,316.24        4,438.11   236,926
    9      PUT     1m - 8mb         4   20,696.40   18,761.90  32,081.10  35,264.57  41,619.41   3,440.51        437.85        1,471.13    78,814
    10     GET    8m - 64mb         5   12,443.86   12,124.69  14,445.05  16,495.17  24,720.31   8,953.63        110.65          967.54    19,918
    11     PUT    8m - 64mb         5   39,970.53   39,023.14  45,669.31  49,527.33  58,766.62   8,959.63         37.27          326.07     6,708
