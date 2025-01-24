# polarWarp
A Polars dataFrame implementation of parsing MinIO Warp object testing output logs

Running this requires the correct python libraries and environment.  I use the "uv" package manager and virtual environment.  

Setup:
Install "uv", "pixi", or another package manager, such as pip.
Example:
user@host:# uv add polars pandas pyarrow zstd

Running:
user@host:# uv run ./polars-parse-warp16.py                                                                                                                        
Usage: python ./polars-parse-warp16.py [--help] : Prints this message and exits
Usage: python ./polars-parse-warp16.py [--skip=<time_to_skip>] <file1> <file2> ...



