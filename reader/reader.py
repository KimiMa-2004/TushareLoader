"""
Author: Qimin Ma
Date: 2026-06-01
Description: Reader (writter) that loads Parquet data from {DATAROOT}/{db_name}/{table_name}/
and returns data in the requested format: pandas Dataframe, duckdb relation,
polars DataFrame, or polars LazyFrame.

Data layout assumptions (mirrors loader/base.py):
  - Constant table: {DATAROOT}/{db_name}/{table_name}/{table_name}.parquet
  - Daily table:    {DATAROOT}/{db_name}/{table_name}/{YYYYMMDD}.parquet

The function auto-detects which layout is used.
"""

from __future__ import annotations

import os
import re
from typing import Literal, Optional, Union, List

import duckdb
import pandas as pd
import polars as pl
from dotenv import load_dotenv, find_dotenv

# ── constants ──────────────────────────────────────────────────────────
DAILY_PARQUET_PATTERN = re.compile(r"^(\d{8})\.parquet$")
OutputFormat = Literal["pandas", "duckdb", "polars", "polars_lazy"]
InfoOutputFormat = Literal["polars", "pandas", "dict"]

# Load .env once at module level so DATAROOT is available
load_dotenv(find_dotenv())
DATA_ROOT: str = os.environ.get("DATAROOT", "D:/data")


# ── helpers ────────────────────────────────────────────────────────────
def _resolve_path(db_name: str, table_name: str) -> tuple[str, str, bool]:
    """
    Resolve table path, supporting two layouts:
      1) Nested:   {DATAROOT}/{db_name}/{table_name}/  (directory)
      2) Flat:     {DATAROOT}/{db_name}/{table_name}.parquet  (single file)

    Returns (table_dir, table_name, is_nested).
      - is_nested=True  → data lives inside the directory
      - is_nested=False → data is a single flat .parquet file
    """
    nested_dir = os.path.abspath(os.path.join(DATA_ROOT, db_name, table_name))
    if os.path.isdir(nested_dir):
        return nested_dir, table_name, True

    flat_file = os.path.abspath(os.path.join(DATA_ROOT, db_name, f"{table_name}.parquet"))
    if os.path.isfile(flat_file):
        return flat_file, table_name, False

    raise FileNotFoundError(
        f"Table not found: checked {nested_dir} (directory) and {flat_file} (file)"
    )


def _is_constant_table(table_dir: str, table_name: str) -> bool:
    """
    Decide whether a table is 'constant' (single-file) by checking if
    {table_name}.parquet exists inside the directory.
    """
    return os.path.isfile(os.path.join(table_dir, f"{table_name}.parquet"))


def _collect_daily_dates(table_dir: str, dates: Optional[List[str]],
                         start: Optional[str], end: Optional[str]) -> List[str]:
    """
    Scan the table_dir for YYYYMMDD.parquet files and return the sorted
    list of date-strings that match the optional filters.

    Filters:
      dates : explicit list  -> keep only those in the list AND on disk
      start : string YYYYMMDD -> inclusive lower bound
      end   : string YYYYMMDD -> inclusive upper bound
    """
    available: set[str] = set()
    for fname in os.listdir(table_dir):
        m = DAILY_PARQUET_PATTERN.match(fname)
        if m:
            available.add(m.group(1))

    if not available:
        return []

    # Apply start/end range filter first
    if start is not None or end is not None:
        start = start.replace("-", "")[:8] if start else min(available)
        end = end.replace("-", "")[:8] if end else max(available)
        available = {d for d in available if start <= d <= end}

    # Apply explicit dates filter (intersection with available)
    if dates is not None:
        dates_clean = {d.replace("-", "")[:8] for d in dates}
        available = available & dates_clean

    return sorted(available)


def _build_select_clause(columns: Optional[List[str]]) -> str:
    """
    Build a SQL SELECT clause. If columns is None or empty, return '*'.
    Otherwise return double-quoted, comma-separated column list.
    """
    if not columns:
        return "*"
    cols_str = ", ".join(f'"{c}"' for c in columns)
    return cols_str


def _load_via_duckdb(table_dir: str, table_name: str,
                     is_constant: bool, dates: Optional[List[str]],
                     start: Optional[str], end: Optional[str],
                     columns: Optional[List[str]] = None) -> duckdb.DuckDBPyRelation:
    """
    Load the table into a duckdb relation.
    Uses duckdb's native Parquet reader (fast, zero-copy where possible).
    Columns can be filtered at the SQL level for column pruning.
    """
    select_clause = _build_select_clause(columns)

    if is_constant:
        path = os.path.join(table_dir, f"{table_name}.parquet")
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Constant table file not found: {path}")
        return duckdb.sql(
            f"SELECT {select_clause} FROM read_parquet('{path}')"
        )  # type: ignore[return-value]
    else:
        daily_dates = _collect_daily_dates(table_dir, dates, start, end)
        if not daily_dates:
            raise FileNotFoundError(
                f"No daily parquet files found in {table_dir} "
                f"(dates={dates}, start={start}, end={end})"
            )
        paths = [os.path.join(table_dir, f"{d}.parquet") for d in daily_dates]
        path_list_str = ", ".join(f"'{p}'" for p in paths)
        return duckdb.sql(
            f"SELECT {select_clause} FROM read_parquet([{path_list_str}])"
        )  # type: ignore[return-value]


# ── main API ───────────────────────────────────────────────────────────
def read_table(
    db_name: str,
    table_name: str,
    output_format: OutputFormat = "pandas",
    dates: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> Union[pd.DataFrame, duckdb.DuckDBPyRelation, pl.DataFrame, pl.LazyFrame]:
    """
    Read a table from {DATAROOT}/{db_name}/{table_name} and return it in
    the specified format.

    Parameters
    ----------
    db_name : str
        Database name (e.g. 'tushare').
    table_name : str
        Table name (e.g. 'stock_daily').
    output_format : str
        One of 'pandas', 'duckdb', 'polars', 'polars_lazy'.
    dates : list[str] or None
        Explicit list of date strings (YYYYMMDD or YYYY-MM-DD).
        Only applicable for daily tables; ignored for constant tables.
    start : str or None
        Inclusive start date (YYYYMMDD or YYYY-MM-DD).
        Only applicable for daily tables; ignored for constant tables.
    end : str or None
        Inclusive end date (YYYYMMDD or YYYY-MM-DD).
        Only applicable for daily tables; ignored for constant tables.
    columns : list[str] or None
        Optional list of column names to select. When None, all columns
        are returned. When provided, only the specified columns are loaded
        (column pruning is pushed down to Parquet for efficiency).

    Returns
    -------
    Data in the requested format.
    """
    if columns is not None and len(columns) == 0:
        raise ValueError("columns must be None or a non-empty list of column names")

    table_dir, table_name, is_nested = _resolve_path(db_name, table_name)

    # Flat layout: single .parquet file, read directly
    if not is_nested:
        # Flat file: table_dir is actually the file path
        if not os.path.isfile(table_dir):
            raise FileNotFoundError(f"Flat table file not found: {table_dir}")
        if output_format == "polars_lazy":
            lf = pl.scan_parquet(table_dir)
            if columns is not None:
                lf = lf.select(columns)
            return lf
        select_clause = _build_select_clause(columns)
        rel = duckdb.sql(f"SELECT {select_clause} FROM read_parquet('{table_dir}')")
        if output_format == "duckdb":
            return rel
        elif output_format == "pandas":
            return rel.df()
        elif output_format == "polars":
            return pl.from_arrow(rel.arrow())
        else:
            raise ValueError(f"Unknown output_format: {output_format}")

    # Nested layout: directory with one or more .parquet files
    is_constant = _is_constant_table(table_dir, table_name)

    # --- polars_lazy: scan directly, bypass duckdb ---
    if output_format == "polars_lazy":
        if is_constant:
            path = os.path.join(table_dir, f"{table_name}.parquet")
            if not os.path.isfile(path):
                raise FileNotFoundError(f"Constant table file not found: {path}")
            lf = pl.scan_parquet(path)
        else:
            daily_dates = _collect_daily_dates(table_dir, dates, start, end)
            if not daily_dates:
                raise FileNotFoundError(
                    f"No daily parquet files found in {table_dir} "
                    f"(dates={dates}, start={start}, end={end})"
                )
            paths = [os.path.join(table_dir, f"{d}.parquet") for d in daily_dates]
            lf = pl.scan_parquet(paths)
        if columns is not None:
            lf = lf.select(columns)
        return lf

    # --- duckdb-based loading (fastest for the other formats) ---
    rel = _load_via_duckdb(table_dir, table_name, is_constant, dates, start, end, columns)

    if output_format == "duckdb":
        return rel
    elif output_format == "pandas":
        return rel.df()
    elif output_format == "polars":
        # duckdb -> arrow -> polars (zero-copy via PyArrow)
        return pl.from_arrow(rel.arrow())
    else:
        raise ValueError(f"Unknown output_format: {output_format}")


def get_table_info(
    db_name: str,
    table_name: str,
    output_format: InfoOutputFormat = "polars",
) -> Union[pl.DataFrame, pd.DataFrame, dict]:
    """
    Return schema information for a table: column name, dtype, non-null
    count, and null percentage. Rows are sorted by null_pct descending.

    Parameters
    ----------
    db_name : str
        Database name (e.g. 'tushare').
    table_name : str
        Table name (e.g. 'stock_daily').
    output_format : str
        One of 'polars', 'pandas', 'dict'.

    Returns
    -------
    DataFrame or dict with columns:
        column_name, dtype, non_null_count, null_pct

    Examples
    --------
    >>> info = get_table_info("tushare", "stock_daily", "polars")
    >>> print(info)
    """
    # Use polars lazy to scan efficiently, then collect
    lf = read_table(db_name, table_name, output_format="polars_lazy")
    df = lf.collect()

    total_rows = df.height
    rows: list[dict] = []
    for col_name in df.columns:
        dtype = str(df.schema[col_name])
        non_null = (
            df.select(pl.col(col_name).is_not_null().sum()).item()
            if total_rows > 0
            else 0
        )
        non_null_int = int(non_null) if isinstance(non_null, (int, float)) else 0
        null_pct = (
            round((1.0 - non_null_int / total_rows) * 100, 2)
            if total_rows > 0
            else 0.0
        )
        rows.append({
            "column_name": col_name,
            "dtype": dtype,
            "non_null_count": non_null_int,
            "null_pct": null_pct,
        })

    result = pl.DataFrame(rows).sort("null_pct", descending=True)

    if output_format == "polars":
        return result
    elif output_format == "pandas":
        return result.to_pandas()
    elif output_format == "dict":
        return result.to_dict(as_series=False)
    else:
        raise ValueError(f"Unknown output_format: {output_format}")