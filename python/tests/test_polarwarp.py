# SPDX-FileCopyrightText: 2025 Russ Fellows <russ.fellows@gmail.com>
# SPDX-License-Identifier: Apache-2.0
"""
Unit tests for polarwarp.py — Python equivalents of the Rust polarwarp-rs tests.

polarwarp.py is a script with top-level execution guarded by `if __name__ == "__main__":`.
We load it via importlib so the guard prevents the script body from running, giving
us clean access to all module-level helper functions.
"""
import importlib.util
import os
import re
import sys
import tempfile
import zstandard as zstd
import pytest
import polars as pl
from datetime import timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Module fixture — load polarwarp.py once for the whole test session
# ---------------------------------------------------------------------------

_MODULE_PATH = Path(__file__).parent.parent / "polarwarp.py"


@pytest.fixture(scope="session")
def pw():
    """Return the polarwarp module loaded via importlib (no script execution)."""
    spec = importlib.util.spec_from_file_location("polarwarp", str(_MODULE_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# format_with_commas — float / int / non-numeric
# ---------------------------------------------------------------------------

class TestFormatWithCommas:
    def test_zero_float(self, pw):
        assert pw.format_with_commas(0.0) == "0.00"

    def test_thousands_float(self, pw):
        assert pw.format_with_commas(1000.0) == "1,000.00"

    def test_large_float(self, pw):
        assert pw.format_with_commas(1_234_567.0) == "1,234,567.00"

    def test_fractional_float(self, pw):
        assert pw.format_with_commas(1024.75) == "1,024.75"

    def test_int_small(self, pw):
        assert pw.format_with_commas(42) == "42"

    def test_int_million(self, pw):
        assert pw.format_with_commas(1_000_000) == "1,000,000"

    def test_non_numeric_string(self, pw):
        assert pw.format_with_commas("hello") == "hello"


# ---------------------------------------------------------------------------
# _excel_derive_path — single-file stem vs. multi-file fallback
# ---------------------------------------------------------------------------

class TestExcelDerivePath:
    def test_single_tsv_zst(self, pw):
        result = pw._excel_derive_path(["output/run1.trace.tsv.zst"])
        assert result == "output/run1.trace.xlsx"

    def test_single_plain_tsv(self, pw):
        result = pw._excel_derive_path(["results.tsv"])
        assert result == os.path.join(".", "results.xlsx")

    def test_single_csv_zst(self, pw):
        result = pw._excel_derive_path(["data/bench.csv.zst"])
        assert result == "data/bench.xlsx"

    def test_multiple_files(self, pw):
        result = pw._excel_derive_path(["a.tsv.zst", "b.tsv.zst"])
        assert result == "polarwarp-results.xlsx"

    def test_multiple_files_three(self, pw):
        result = pw._excel_derive_path(["x.tsv.zst", "y.tsv.zst", "z.tsv.zst"])
        assert result == "polarwarp-results.xlsx"


# ---------------------------------------------------------------------------
# Short-name / tab-name helpers (replicated from write_polarwarp_excel locals)
# These mirror the Rust derive_short_name and make_tab_name tests.
# ---------------------------------------------------------------------------

def _short(fp: str) -> str:
    """Replicate write_polarwarp_excel._short logic."""
    n = os.path.basename(fp)
    n = n.removesuffix('.zst')
    n = n.removesuffix('.csv') if n.endswith('.csv') else \
        n.removesuffix('.tsv') if n.endswith('.tsv') else n
    bi = n.find('[')
    if bi >= 0:
        n = n[:bi]
    return n.rstrip('-_.')[:20]


def _tab(base: str, suf: str) -> str:
    """Replicate write_polarwarp_excel._tab logic."""
    full = f"{base}-{suf}"
    return full if len(full) <= 31 else f"{base[:31-len(suf)-1]}-{suf}"


class TestShortName:
    def test_tsv_zst_extension_stripped(self):
        assert _short("output/run1.trace.tsv.zst") == "run1.trace"

    def test_plain_csv_extension_stripped(self):
        assert _short("results.csv") == "results"

    def test_bracket_timestamp_stripped(self):
        # warp output files contain [2024-01-15T10:30:00Z] timestamp brackets
        assert _short("bench[2024-01-15T10:30:00Z].tsv.zst") == "bench"

    def test_truncated_to_20_chars(self):
        long_name = "averylongunambiguousname.tsv.zst"
        result = _short(long_name)
        assert len(result) <= 20


class TestTabName:
    def test_short_name_unchanged(self):
        assert _tab("run1", "Results") == "run1-Results"

    def test_combined_within_31_chars(self):
        result = _tab("short", "Detail")
        assert result == "short-Detail"
        assert len(result) <= 31

    def test_truncated_to_31_chars(self):
        base = "x" * 30  # very long base
        result = _tab(base, "Results")
        assert len(result) == 31

    def test_truncated_preserves_suffix(self):
        base = "y" * 30
        result = _tab(base, "Summary")
        assert result.endswith("-Summary")
        assert len(result) == 31


# ---------------------------------------------------------------------------
# Skip-time pattern parsing — equivalent to Rust parse_skip_time tests
# ---------------------------------------------------------------------------

_SKIP_PATTERN = re.compile(r"^--skip=(\d+)([sm])$")


class TestSkipPattern:
    def test_seconds(self):
        m = _SKIP_PATTERN.match("--skip=90s")
        assert m is not None
        assert m.group(1) == "90"
        assert m.group(2) == "s"
        assert timedelta(seconds=int(m.group(1))) == timedelta(seconds=90)

    def test_minutes(self):
        m = _SKIP_PATTERN.match("--skip=5m")
        assert m is not None
        assert m.group(1) == "5"
        assert m.group(2) == "m"
        assert timedelta(minutes=int(m.group(1))) == timedelta(minutes=5)

    def test_zero_seconds(self):
        m = _SKIP_PATTERN.match("--skip=0s")
        assert m is not None
        assert timedelta(seconds=0) == timedelta(seconds=0)

    def test_invalid_unit_hours(self):
        assert _SKIP_PATTERN.match("--skip=2h") is None

    def test_invalid_no_prefix(self):
        assert _SKIP_PATTERN.match("skip=5s") is None

    def test_invalid_no_value(self):
        assert _SKIP_PATTERN.match("--skip=s") is None


# ---------------------------------------------------------------------------
# timedelta formatting — Python equivalent of Rust format_duration_ns tests
# ---------------------------------------------------------------------------

class TestTimedeltaFormatting:
    """Python uses str(timedelta) which produces H:MM:SS or H:MM:SS.ffffff."""

    def test_zero(self):
        assert str(timedelta(seconds=0)) == "0:00:00"

    def test_one_second(self):
        assert str(timedelta(seconds=1)) == "0:00:01"

    def test_one_minute(self):
        assert str(timedelta(seconds=60)) == "0:01:00"

    def test_one_hour(self):
        assert str(timedelta(hours=1)) == "1:00:00"

    def test_ninety_seconds(self):
        assert str(timedelta(seconds=90)) == "0:01:30"


# ---------------------------------------------------------------------------
# detect_file_type — requires real files on disk
# ---------------------------------------------------------------------------

def _write_tsv(path: str, header: str, row: str) -> None:
    """Write a minimal 2-line TSV (header + one data row) to path."""
    with open(path, "w") as f:
        f.write(header + "\n")
        f.write(row + "\n")


class TestDetectFileType:
    def test_trace_file(self, pw, tmp_path):
        p = tmp_path / "sample.tsv"
        _write_tsv(
            str(p),
            "op\tstart\tend\tduration_ns\tbytes\terror",
            "GET\t2024-01-01T00:00:00Z\t2024-01-01T00:00:01Z\t1000000000\t65536\t",
        )
        assert pw.detect_file_type(str(p)) == "trace"

    def test_summary_file(self, pw, tmp_path):
        p = tmp_path / "summary.tsv"
        _write_tsv(
            str(p),
            "op\tbps\tops_per_sec\terrors",
            "GET\t1073741824\t100.0\t0",
        )
        assert pw.detect_file_type(str(p)) == "summary"

    def test_trace_zst_file(self, pw, tmp_path):
        """detect_file_type handles .zst files via polars decompression."""
        p = tmp_path / "sample.tsv.zst"
        raw = b"op\tstart\tend\tduration_ns\tbytes\terror\nGET\t2024-01-01T00:00:00Z\t2024-01-01T00:00:01Z\t1000000000\t65536\t\n"
        cctx = zstd.ZstdCompressor()
        with open(str(p), "wb") as f:
            f.write(cctx.compress(raw))
        assert pw.detect_file_type(str(p)) == "trace"


# ---------------------------------------------------------------------------
# _compute_summary_stats_df — aggregation and TOTAL-row ordering
# ---------------------------------------------------------------------------

def _make_summary_df() -> pl.DataFrame:
    """Create a small synthetic summary DataFrame for testing."""
    return pl.DataFrame({
        "op":          ["GET", "GET", "GET", "PUT", "PUT", "TOTAL", "TOTAL"],
        "bps":         [
            1_073_741_824.0,   # 1 GiB/s
            2_147_483_648.0,   # 2 GiB/s
            1_610_612_736.0,   # 1.5 GiB/s
            536_870_912.0,     # 512 MiB/s
            1_073_741_824.0,   # 1 GiB/s
            1_342_177_280.0,   # 1.25 GiB/s
            1_610_612_736.0,   # 1.5 GiB/s
        ],
        "ops_per_sec": [100.0, 200.0, 150.0, 50.0, 100.0, 75.0, 100.0],
        "errors":      [0,     0,     0,     1,    0,     0,    0],
    })


class TestComputeSummaryStatsDf:
    def test_total_row_is_last(self, pw):
        df = _make_summary_df()
        result = pw._compute_summary_stats_df(df)
        last_op = result["op"].to_list()[-1]
        assert last_op == "TOTAL", f"Expected last row to be TOTAL, got {last_op!r}"

    def test_non_total_rows_before_total(self, pw):
        df = _make_summary_df()
        result = pw._compute_summary_stats_df(df)
        ops = result["op"].to_list()
        total_idx = ops.index("TOTAL")
        assert all(op != "TOTAL" for op in ops[:total_idx])

    def test_aggregated_row_count(self, pw):
        df = _make_summary_df()
        result = pw._compute_summary_stats_df(df)
        # 3 distinct ops: GET, PUT, TOTAL
        assert result.height == 3

    def test_get_segment_count(self, pw):
        df = _make_summary_df()
        result = pw._compute_summary_stats_df(df)
        get_row = result.filter(pl.col("op") == "GET")
        assert get_row["segments"].item() == 3

    def test_error_sum(self, pw):
        df = _make_summary_df()
        result = pw._compute_summary_stats_df(df)
        put_row = result.filter(pl.col("op") == "PUT")
        assert put_row["total_errors"].item() == 1

    def test_mean_mbps_is_float(self, pw):
        df = _make_summary_df()
        result = pw._compute_summary_stats_df(df)
        mean_val = result.filter(pl.col("op") == "GET")["mean_MBps"].item()
        assert isinstance(mean_val, float)
        # GET bps mean is (1G + 2G + 1.5G) / 3 = 1.5 GiB/s = 1536 MiB/s
        assert abs(mean_val - 1536.0) < 0.01


# ---------------------------------------------------------------------------
# Size bucket logic — inline Polars equivalent of Rust add_size_buckets tests
# ---------------------------------------------------------------------------

BUCKET_8K   = 8 * 1024
BUCKET_64K  = 64 * 1024
BUCKET_512K = 512 * 1024
BUCKET_4M   = 4 * 1024 * 1024
BUCKET_32M  = 32 * 1024 * 1024
BUCKET_256M = 256 * 1024 * 1024
BUCKET_2G   = 2 * 1024 * 1024 * 1024

BUCKET_ORDER = [
    "zero", "1B-8KiB", "8KiB-64KiB", "64KiB-512KiB",
    "512KiB-4MiB", "4MiB-32MiB", "32MiB-256MiB", "256MiB-2GiB", ">2GiB",
]


def _apply_buckets(bytes_values: list) -> pl.DataFrame:
    """Apply the same bucket Polars expressions used in polarwarp.py."""
    df = pl.DataFrame({"bytes": bytes_values})
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
        .otherwise(8).alias("bucket_#"),
    ])
    return df


class TestSizeBuckets:
    def test_zero_bytes(self):
        df = _apply_buckets([0])
        assert df["bytes_bucket"][0] == "zero"
        assert df["bucket_#"][0] == 0

    def test_all_bucket_labels(self):
        """One value from each of the nine buckets maps to the correct label."""
        inputs = [
            0,                      # zero
            1,                      # 1B-8KiB
            BUCKET_8K,              # 8KiB-64KiB
            BUCKET_64K,             # 64KiB-512KiB
            BUCKET_512K,            # 512KiB-4MiB
            BUCKET_4M,              # 4MiB-32MiB
            BUCKET_32M,             # 32MiB-256MiB
            BUCKET_256M,            # 256MiB-2GiB
            BUCKET_2G,              # >2GiB
        ]
        df = _apply_buckets(inputs)
        assert df["bytes_bucket"].to_list() == BUCKET_ORDER

    def test_bucket_num_matches_label_order(self):
        """bucket_# must equal the index of bytes_bucket in BUCKET_ORDER."""
        inputs = [
            0,
            1,
            BUCKET_8K,
            BUCKET_64K,
            BUCKET_512K,
            BUCKET_4M,
            BUCKET_32M,
            BUCKET_256M,
            BUCKET_2G,
        ]
        df = _apply_buckets(inputs)
        for row in df.iter_rows(named=True):
            label   = row["bytes_bucket"]
            num     = row["bucket_#"]
            expected_num = BUCKET_ORDER.index(label)
            assert num == expected_num, (
                f"bytes={row['bytes']}: bucket_# {num} != expected {expected_num} "
                f"for label '{label}'"
            )

    def test_boundary_values_exclusive(self):
        """A value of exactly BUCKET_8K belongs to '8KiB-64KiB', not '1B-8KiB'."""
        df = _apply_buckets([BUCKET_8K - 1, BUCKET_8K])
        assert df["bytes_bucket"][0] == "1B-8KiB"
        assert df["bytes_bucket"][1] == "8KiB-64KiB"
