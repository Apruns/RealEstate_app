# core/sampling_checker.py

from typing import List, Optional, Tuple
import pandas as pd


# Default join keys used to match deals between scan and RAMI
DEFAULT_JOIN_KEYS: List[str] = [
    "block_lot",
    "sale_day",
    "declared_profit",
    "city",
    "build_year",
    "building_mr",
    "rooms_number",
]


def _ensure_columns_exist(df: pd.DataFrame, columns: List[str], context: str) -> None:
    """
    Verify that all required columns exist in the DataFrame.
    Raise a clear error listing any missing columns.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {context}: {missing}")


def _filter_latest_scan(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Filter the scan DataFrame to keep only rows from the latest scan_date.

    If 'scan_date' does not exist, the original DataFrame is returned.
    If scan_date can be parsed as dates, the maximum date is used.
    Otherwise the maximum raw value is used.

    Returns
    -------
    filtered_df : pd.DataFrame
        DataFrame filtered to the latest scan_date (or unchanged).
    latest_scan_value : Optional[str]
        The value of the latest scan_date used for filtering (as string),
        or None if no filtering was performed.
    """
    if "scan_date" not in df.columns:
        return df.copy(), None

    scan_dt = pd.to_datetime(df["scan_date"], errors="coerce", dayfirst=True)

    if scan_dt.notna().any():
        latest_dt = scan_dt.max()
        mask = scan_dt == latest_dt
        filtered = df.loc[mask].copy()
        latest_value = latest_dt.strftime("%Y-%m-%d")
        return filtered, latest_value
    else:
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
        # For safety we do not enforce sold_part here, but in practice
        # your scan file should contain this column.
        return df.copy()

    sold_numeric = pd.to_numeric(df["sold_part"], errors="coerce")
    mask = sold_numeric == 1
    return df.loc[mask].copy()


def run_sampling_check(
    df_scan_raw: pd.DataFrame,
    df_rami_raw: pd.DataFrame,
    sample_size: Optional[int] = 200,
    join_keys: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[str]]:
    """
    Main logic for the sampling check:

    1. Take the latest scan_date from the scan file (if exists).
    2. Filter scan to sold_part = 1 (if column exists).
    3. Take a random sample from the RAMI file (fixed size or full RAMI if smaller).
    4. Match RAMI sample to the scan using join_keys.
    5. Mark which RAMI records are found / missing in the scan.

    Parameters
    ----------
    df_scan_raw : pd.DataFrame
        Scan DataFrame (Excel/CSV).
    df_rami_raw : pd.DataFrame
        RAMI DataFrame (Excel/HTML converted to DataFrame).
    sample_size : Optional[int]
        Number of rows to sample from RAMI. If None or >= len(df_rami_raw),
        the whole RAMI file is used.
    join_keys : Optional[List[str]]
        Columns used to match deals between scan and RAMI.
        If None, DEFAULT_JOIN_KEYS is used.

    Returns
    -------
    df_sample_with_flag : pd.DataFrame
        RAMI sample with a boolean column 'found_in_scan'.
    df_missing_only : pd.DataFrame
        Only RAMI rows from the sample that were NOT found in the scan.
    latest_scan_value : Optional[str]
        The latest scan_date value used for filtering (or None).
    """
    if join_keys is None:
        join_keys = DEFAULT_JOIN_KEYS

    # 1) Filter scan to latest scan_date
    df_scan_latest, latest_scan_value = _filter_latest_scan(df_scan_raw)

    # 2) Filter scan to sold_part = 1 (if exists)
    df_scan_latest = _filter_sold_part_equals_one(df_scan_latest)

    # 3) Take a sample from RAMI
    if sample_size is None or sample_size >= len(df_rami_raw):
        df_rami_sample = df_rami_raw.copy()
    else:
        df_rami_sample = df_rami_raw.sample(n=sample_size, random_state=42)

    # 4) Ensure join keys exist in both frames
    _ensure_columns_exist(df_scan_latest, join_keys, context="scan file")
    _ensure_columns_exist(df_rami_sample, join_keys, context="RAMI sample")

    # 5) Prepare join keys as strings to avoid type mismatches
    scan_keys = df_scan_latest[join_keys].astype(str)
    rami_keys = df_rami_sample[join_keys].astype(str)

    # Mark all scan combinations as "present"
    scan_keys_marked = scan_keys.copy()
    scan_keys_marked["__present_in_scan__"] = True

    # Left join RAMI sample on scan keys
    merged = rami_keys.merge(
        scan_keys_marked,
        how="left",
        on=join_keys,
    )

    # If __present_in_scan__ is NaN -> not found in scan
    found_mask = merged["__present_in_scan__"].fillna(False).astype(bool)

    df_sample_with_flag = df_rami_sample.copy()
    df_sample_with_flag["found_in_scan"] = found_mask

    df_missing_only = df_sample_with_flag[~df_sample_with_flag["found_in_scan"]].copy()

    return df_sample_with_flag, df_missing_only, latest_scan_value


if __name__ == "__main__":
    """
    Manual test (adjust paths before running).
    """
    import os
    from core.file_loader import load_scan_file, load_rami_file

    scan_path = r"C:\Ariel Portnik\matan_deals\yad2_scan_example.xlsx"
    rami_path = r"C:\Users\arielpo\Downloads\rami_example.xls"

    if os.path.exists(scan_path) and os.path.exists(rami_path):
        df_scan = load_scan_file(scan_path)
        df_rami = load_rami_file(rami_path)

        full_sample, missing, latest_scan = run_sampling_check(
            df_scan,
            df_rami,
            sample_size=200,
        )

        print("Latest scan_date:", latest_scan)
        print("Sample size:     ", len(full_sample))
        print("Missing in scan: ", len(missing))
    else:
        print("Please update test paths in sampling_checker.py")
