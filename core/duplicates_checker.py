# core/duplicates_checker.py

from typing import Tuple, Optional, List
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


def _ensure_columns_exist(df: pd.DataFrame, columns: List[str]) -> None:
    """
    Verify that all required columns exist in the DataFrame.
    Raise a clear error listing any missing columns.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for duplicates check: {missing}")


def _filter_latest_scan(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Filter the DataFrame to keep only rows from the latest scan_date.

    If 'scan_date' does not exist, the original DataFrame is returned.
    If scan_date can be parsed as dates, the maximum date is used.
    Otherwise the maximum value (lexicographic) is used.

    Returns
    -------
    filtered_df : pd.DataFrame
        DataFrame filtered to the latest scan_date (or unchanged).
    latest_scan_value : Optional[str]
        The value of the latest scan_date used for filtering (as string),
        or None if no filtering was performed.
    """
    if "scan_date" not in df.columns:
        # Nothing to filter by, just return a copy
        return df.copy(), None

    # Try to parse as datetime (day-first, typical Israeli format)
    scan_dt = pd.to_datetime(df["scan_date"], errors="coerce", dayfirst=True)

    if scan_dt.notna().any():
        latest_dt = scan_dt.max()
        mask = scan_dt == latest_dt
        filtered = df.loc[mask].copy()
        latest_value = latest_dt.strftime("%Y-%m-%d")
        return filtered, latest_value
    else:
        # Fallback: treat scan_date as plain string/value
        latest_raw = df["scan_date"].max()
        mask = df["scan_date"] == latest_raw
        filtered = df.loc[mask].copy()
        latest_value = str(latest_raw)
        return filtered, latest_value


def _filter_sold_part_equals_one(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter DataFrame to keep only rows where sold_part = 1.

    Handles sold_part stored as string or numeric. Invalid values are ignored.
    """
    if "sold_part" not in df.columns:
        raise ValueError("Column 'sold_part' is required for duplicates check but was not found.")

    sold_numeric = pd.to_numeric(df["sold_part"], errors="coerce")
    mask = sold_numeric == 1
    return df.loc[mask].copy()


def find_duplicates_summary(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Replicate the SQL logic:

        SELECT block_lot, sale_day, declared_profit, sold_part, city, build_year,
               building_mr, rooms_number, COUNT(*) AS count, scan_date
        FROM ...
        WHERE scan_date = <latest scan_date in file>
          AND sold_part = 1
        GROUP BY block_lot, sale_day, declared_profit, sold_part, city, build_year,
                 building_mr, rooms_number, scan_date
        HAVING COUNT(*) > 1
        ORDER BY count DESC

    Parameters
    ----------
    df_raw : pd.DataFrame
        Raw scan DataFrame loaded from Excel/CSV.

    Returns
    -------
    pd.DataFrame
        One row per duplicate group:
        [block_lot, sale_day, declared_profit, sold_part, city, build_year,
         building_mr, rooms_number, scan_date, count]
    """
    # 1) Verify that all required columns exist
    _ensure_columns_exist(df_raw, DUPLICATE_KEY_COLUMNS)

    # 2) Filter to latest scan_date
    df_latest, latest_scan = _filter_latest_scan(df_raw)

    # 3) Filter to sold_part = 1
    df_latest = _filter_sold_part_equals_one(df_latest)

    if df_latest.empty:
        # No rows to group – return empty with the expected columns
        summary = pd.DataFrame(columns=DUPLICATE_KEY_COLUMNS + ["count"])
        return summary

    # 4) Group by the key columns and count occurrences
    grouped = (
        df_latest
        .groupby(DUPLICATE_KEY_COLUMNS, dropna=False)
        .size()
        .reset_index(name="count")
    )

    # 5) Keep only groups with count > 1 (duplicates)
    duplicates_summary = grouped[grouped["count"] > 1].copy()

    # 6) Sort by count descending (same as ORDER BY count DESC)
    duplicates_summary = duplicates_summary.sort_values(
        by="count",
        ascending=False
    )

    return duplicates_summary


def find_duplicate_rows(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Alternative view: instead of one row per duplicate group,
    return all original rows that belong to duplicate groups.

    This is useful if you want to inspect the actual records, not just the summary.

    Logic:
    1. Filter to latest scan_date.
    2. Filter to sold_part = 1.
    3. Mark rows where the key combination appears more than once.
    """
    _ensure_columns_exist(df_raw, DUPLICATE_KEY_COLUMNS)

    df_latest, latest_scan = _filter_latest_scan(df_raw)
    df_latest = _filter_sold_part_equals_one(df_latest)

    if df_latest.empty:
        return df_latest.copy()  # empty

    # Mark duplicate rows based on the key columns
    dup_mask = df_latest.duplicated(subset=DUPLICATE_KEY_COLUMNS, keep=False)
    duplicate_rows = df_latest.loc[dup_mask].copy()

    # Optional: sort by the key columns for easier inspection
    duplicate_rows = duplicate_rows.sort_values(by=DUPLICATE_KEY_COLUMNS)

    return duplicate_rows


if __name__ == "__main__":
    """
    Manual test example.
    Update `test_path` to point to a real scan file (Excel/CSV) before running.
    """
    import os

    from core.file_loader import load_scan_file  # adjust import if needed

    test_path = r"C:\Ariel Portnik\matan_deals\yad2_scan_example.xlsx"

    if os.path.exists(test_path):
        df_scan = load_scan_file(test_path)
        dup_summary = find_duplicates_summary(df_scan)
        print("=== Duplicate groups summary ===")
        print(dup_summary.head(20))

        dup_rows = find_duplicate_rows(df_scan)
        print("\n=== Duplicate rows (first 20) ===")
        print(dup_rows.head(20))
    else:
        print(f"Test file not found: {test_path}")
