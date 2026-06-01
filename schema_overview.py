"""
Author: Qimin Ma
Date: 2026-06-01
Description: Scan all DATAROOT databases and tables, generate a Markdown schema
overview with: column name, dtype, non-null count, null percentage.

Usage:
    python schema_overview.py                  # writes schema_overview.md
    python schema_overview.py -o report.md      # custom output path
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
from dotenv import load_dotenv, find_dotenv

# ── config ─────────────────────────────────────────────────────────────
load_dotenv(find_dotenv())
DATA_ROOT: str = os.environ.get("DATAROOT", "D:/data")
if not os.path.isdir(DATA_ROOT):
    print(f"ERROR: DATAROOT not found: {DATA_ROOT}", file=sys.stderr)
    sys.exit(1)


# ── helper ─────────────────────────────────────────────────────────────
def _scan_flat_tables(db_dir: str) -> list[tuple[str, str, str, str]]:
    """Scan a db directory for flat .parquet files -> (db_dir, db_name, table_name, file_path)."""
    tables: list[tuple[str, str, str, str]] = []
    db_name = os.path.basename(db_dir)
    for fname in os.listdir(db_dir):
        if fname.endswith(".parquet"):
            file_path = os.path.join(db_dir, fname)
            if os.path.isdir(file_path):
                continue  # skip nested directories misidentified as .parquet
            table_name = fname[:-8]  # strip .parquet
            tables.append((db_dir, db_name, table_name, file_path))
    return tables


def _scan_nested_tables(db_dir: str) -> list[tuple[str, str, str, str]]:
    """Scan a db directory for nested table subdirectories."""
    tables: list[tuple[str, str, str, str]] = []
    db_name = os.path.basename(db_dir)
    for entry in os.listdir(db_dir):
        entry_path = os.path.join(db_dir, entry)
        if os.path.isdir(entry_path):
            table_name = entry
            # constant layout: {table_name}/{table_name}.parquet
            const_file = os.path.join(entry_path, f"{table_name}.parquet")
            if os.path.isfile(const_file):
                tables.append((db_dir, db_name, table_name, const_file))
            else:
                # daily layout: {table_name}/{YYYYMMDD}.parquet
                daily_files = [f for f in os.listdir(entry_path) if f.endswith(".parquet")]
                if daily_files:
                    tables.append((db_dir, db_name, table_name, entry_path))
    return tables


def profile_table(db_name: str, table_name: str, path: str) -> pl.DataFrame | None:
    """
    Profile a single table and return a summary DataFrame with columns:
    database, table_name, column_name, dtype, non_null_count, null_pct, total_rows.
    """
    try:
        if os.path.isfile(path):
            df = pl.read_parquet(path)
        elif os.path.isdir(path):
            files = sorted(
                [os.path.join(path, f) for f in os.listdir(path) if f.endswith(".parquet")]
            )
            if not files:
                return None
            df = pl.read_parquet(files)
        else:
            return None
    except Exception as e:
        print(f"  [WARN] Failed to read {db_name}/{table_name}: {e}", file=sys.stderr)
        return None

    total_rows = df.height
    if total_rows == 0:
        return None

    rows: list[dict] = []
    for col_name in df.columns:
        dtype = str(df.schema[col_name])
        non_null = df.select(pl.col(col_name).is_not_null().sum()).item()
        null_pct = (1.0 - non_null / total_rows) * 100
        rows.append({
            "database": db_name,
            "table_name": table_name,
            "column_name": col_name,
            "dtype": dtype,
            "non_null_count": non_null,
            "total_rows": total_rows,
            "null_pct": round(null_pct, 2),
        })

    return pl.DataFrame(rows).sort("null_pct", descending=True)


def _format_number(n: int) -> str:
    """Format integer with thousands separator."""
    return f"{n:,}"


def _to_markdown_table(df: pl.DataFrame) -> str:
    """Convert a polars DataFrame (column_name, dtype, non_null_count, null_pct) to
    a GitHub-flavored markdown table string."""
    cols = ["column_name", "dtype", "non_null_count", "null_pct"]
    headers = ["Column", "Type", "Non-Null Count", "Null %"]
    align = [":---", ":---", "---:", "---:"]

    lines: list[str] = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(align) + " |")

    for row in df.select(cols).rows():
        name, dtype, nn, pct = row
        nn_str = _format_number(nn) if isinstance(nn, int) else str(nn)
        pct_str = f"{pct:.2f}" if pct != 0 else "0.00"
        lines.append(f"| {name} | {dtype} | {nn_str} | {pct_str} |")

    return "\n".join(lines)


# ── main ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan DATAROOT and generate a markdown schema overview."
    )
    parser.add_argument(
        "-o", "--output",
        default="schema_overview.md",
        help="Output markdown file path (default: schema_overview.md)",
    )
    args = parser.parse_args()

    output_path = os.path.abspath(args.output)
    print(f"Scanning DATAROOT: {DATA_ROOT}")

    # 1) Collect all tables
    all_tables: list[tuple[str, str, str, str]] = []  # (db_dir, db_name, table_name, path)
    for entry in sorted(os.listdir(DATA_ROOT)):
        db_path = os.path.join(DATA_ROOT, entry)
        if os.path.isdir(db_path):
            all_tables.extend(_scan_flat_tables(db_path))
            all_tables.extend(_scan_nested_tables(db_path))

    if not all_tables:
        print("No tables found.")
        return

    n_dbs = len(set(t[1] for t in all_tables))
    print(f"Found {len(all_tables)} table(s) across {n_dbs} database(s).")

    # 2) Profile each table
    all_summaries: list[pl.DataFrame] = []
    for db_dir, db_name, table_name, path in all_tables:
        print(f"  Profiling {db_name}/{table_name} ...", end=" ")
        summary = profile_table(db_name, table_name, path)
        if summary is not None:
            all_summaries.append(summary)
            print("done")
        else:
            print("SKIP (empty or error)")

    if not all_summaries:
        print("No data could be profiled.")
        return

    combined = pl.concat(all_summaries, how="vertical")

    # 3) Build markdown output
    md_lines: list[str] = []
    md_lines.append(f"# Schema Overview")
    md_lines.append(f"")
    md_lines.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ")
    md_lines.append(f"**DATAROOT**: `{DATA_ROOT}`  ")
    md_lines.append(f"**Databases**: {n_dbs}  ")
    md_lines.append(f"**Tables**: {combined['table_name'].n_unique()}  ")
    md_lines.append(f"**Total Columns**: {combined.height}  ")
    md_lines.append(f"")

    # Per-database sections
    for db_name in sorted(combined["database"].unique().to_list()):
        db_df = combined.filter(pl.col("database") == db_name)
        n_tables = db_df["table_name"].n_unique()
        md_lines.append(f"## Database: `{db_name}` ({n_tables} tables)")
        md_lines.append(f"")

        for tbl in sorted(db_df["table_name"].unique().to_list()):
            tbl_df = db_df.filter(pl.col("table_name") == tbl)
            total = tbl_df["total_rows"].item(0)
            md_lines.append(f"### Table: `{tbl}`")
            md_lines.append(f"")
            md_lines.append(f"**Rows**: {_format_number(total)} | **Columns**: {tbl_df.height}")
            md_lines.append(f"")
            md_lines.append(_to_markdown_table(tbl_df))
            md_lines.append(f"")

    # 4) Write to file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    print(f"\nSchema overview saved to: {output_path}")


if __name__ == "__main__":
    main()