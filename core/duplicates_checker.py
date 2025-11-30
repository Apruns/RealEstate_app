"""
Logic for detecting duplicate real-estate transactions in a scan file.

The goal is to mimic the SQL logic:

SELECT block_lot,sale_day,declared_profit,sold_part,city, build_year,
       building_mr, rooms_number ,count(*) as count,scan_date
FROM raw_gov_deals.real_estate_deals
WHERE scan_date = <latest_scan_date>
  AND sold_part = 1
GROUP BY block_lot,sale_day,declared_profit,sold_part,city,
         build_year,building_mr,rooms_number,scan_date
HAVING count(*) > 1
ORDER BY count DESC;
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd


# Columns used to define a "duplicate group"
DUPLICATE_KEY_COLUMNS: List[str] = [
    "block_lot",
    "sale_day",
    "declared_profit",
    "sold_part",
    "city",
    "build_year",
    "building_mr",
    "rooms_number",
    "scan_date",
]


def _filter_latest_scan(df: pd.DataFrame) -> pd.DataFrame:
    """
    If 'scan_date' exists, keep only rows with the latest scan_date.
    Otherwise, return the DataFrame as-is.
    """
    if "scan_date" not in df.columns:
        return df

    # Try to parse scan_date to datetime if it looks like a string
    try:
        scan_dt = pd.to_datetime(df["scan_date"], errors="coerce", dayfirst=True)
        latest = scan_dt.max()
        mask = scan_dt == latest
        return df.loc[mask].copy()
    except Exception:
        # If parsing fails, fall back to 'max' on the raw column
        latest = df["scan_date"].max()
        return df.loc[df["scan_date"] == latest].copy()


def _filter_sold_part_one(df: pd.DataFrame) -> pd.DataFrame:
    """
    If 'sold_part' column exists, keep only rows where sold_part == 1.
    Otherwise, return as-is.
    """
    if "sold_part" not in df.columns:
        return df

    return df.loc[df["sold_part"] == 1].copy()


def _prepare_base_duplicates_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the standard filters before duplicate detection:
    - latest scan_date (if present)
    - sold_part == 1       (if present)
    """
    base = df.copy()

    # 1. Filter to latest scan_date
    base = _filter_latest_scan(base)

    # 2. Filter to sold_part == 1
    base = _filter_sold_part_one(base)

    return base


def find_duplicate_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a summary of duplicate groups:

    - Uses only the latest scan_date (if scan_date column exists).
    - Uses only sold_part == 1 (if sold_part column exists).
    - Groups by all DUPLICATE_KEY_COLUMNS that exist in df.
    - Keeps only groups with count > 1.
    - Returns one row per duplicate combination + 'count' column.
    """
    base = _prepare_base_duplicates_df(df)

    # Determine which key columns actually exist in the DataFrame
    key_cols = [c for c in DUPLICATE_KEY_COLUMNS if c in base.columns]
    if not key_cols:
        # If none of the expected columns exist, there is nothing meaningful to group by
        return pd.DataFrame()

    grouped = (
        base
        .groupby(key_cols, dropna=False)
        .size()
        .reset_index(name="count")
    )

    # Keep only combinations that appear more than once
    duplicates_summary = grouped.loc[grouped["count"] > 1].copy()

    # Sort by count descending
    duplicates_summary = duplicates_summary.sort_values(
        by="count", ascending=False
    )

    # Reset index for neat output
    duplicates_summary = duplicates_summary.reset_index(drop=True)

    return duplicates_summary


def find_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return all rows that belong to duplicate groups.

    - Same logic as find_duplicate_summary regarding filters and keys.
    - Finds all keys with count > 1, then returns the full rows from the filtered DataFrame.
    """
    base = _prepare_base_duplicates_df(df)

    key_cols = [c for c in DUPLICATE_KEY_COLUMNS if c in base.columns]
    if not key_cols:
        return pd.DataFrame()

    # First, compute summary counts
    grouped = (
        base
        .groupby(key_cols, dropna=False)
        .size()
        .reset_index(name="count")
    )
    dup_keys = grouped.loc[grouped["count"] > 1, key_cols]

    if dup_keys.empty:
        # No duplicates according to our definition
        return pd.DataFrame(columns=base.columns)

    # Join back to get all rows that match duplicate key combinations
    merged = base.merge(dup_keys, on=key_cols, how="inner")

    # Optional: order by the same key columns + maybe declared_profit or sale_day
    sort_cols: List[str] = [c for c in key_cols if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(by=sort_cols)

    merged = merged.reset_index(drop=True)
    return merged
