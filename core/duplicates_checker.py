# core/duplicates_checker.py

import os
from datetime import date
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

DUP_KEY_COLUMNS: List[str] = [
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

CHUNK_SIZE = 50000  # Process 50k rows at a time

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _detect_encoding(scan_path: str) -> str:
    """Try to detect encoding by reading a tiny bit of the file."""
    for enc in ["utf-8", "cp1255", "latin1"]:
        try:
            pd.read_csv(scan_path, nrows=100, encoding=enc)
            return enc
        except Exception:
            continue
    return "utf-8"  # Fallback

def _get_latest_scan_date(scan_path: str, ext: str, encoding: str) -> Optional[date]:
    """
    Efficiently find the max scan_date without loading the whole file.
    Reads only the 'scan_date' column.
    """
    try:
        if ext == ".csv":
            # Read ONLY the scan_date column
            df = pd.read_csv(scan_path, usecols=["scan_date"], encoding=encoding)
        else:
            # Excel doesn't support usecols efficiency as well as CSV, but we still try
            df = pd.read_excel(scan_path, usecols=["scan_date"])
        
        # Fast parsing of unique values only
        uniques = df["scan_date"].dropna().unique()
        parsed = pd.to_datetime(uniques, dayfirst=True, errors="coerce")
        if parsed.empty:
            return None
        return parsed.max().date()
    except Exception:
        return None

def _read_and_filter_chunks(scan_path: str, latest_date: date) -> Tuple[pd.DataFrame, int]:
    """
    Reads file in chunks, keeps only rows matching latest_date and sold_part==1.
    Returns: (filtered_df, total_rows_scanned)
    """
    ext = os.path.splitext(scan_path)[1].lower()
    total_rows = 0
    filtered_chunks = []
    
    # Setup iterator
    if ext == ".csv":
        encoding = _detect_encoding(scan_path)
        # return a TextFileReader object for chunking
        iterator = pd.read_csv(scan_path, chunksize=CHUNK_SIZE, encoding=encoding, dtype=str)
    else:
        # Excel generally reads all at once, but we can fake chunking or just read it.
        # Since Excel has row limits anyway, we usually just read it fully.
        df_full = pd.read_excel(scan_path, dtype=str)
        iterator = [df_full] 

    latest_ts = pd.Timestamp(latest_date)

    for chunk in iterator:
        total_rows += len(chunk)
        
        # 1. Parse scan_date in this chunk (Optimized)
        if "scan_date" not in chunk.columns:
            continue
            
        # Parse uniques map strategy
        chunk_dates = chunk["scan_date"].unique()
        date_map = pd.to_datetime(chunk_dates, dayfirst=True, errors="coerce")
        date_mapper = dict(zip(chunk_dates, date_map))
        parsed_series = chunk["scan_date"].map(date_mapper)
        
        # 2. Filter by date
        # Keep rows where date matches OR is missing (to be safe? usually we want exact match)
        # Strict logic: match latest date
        date_mask = (parsed_series == latest_ts)
        
        # 3. Filter by sold_part == 1
        if "sold_part" in chunk.columns:
            # fast clean
            sp = chunk["sold_part"].astype(str).str.replace(r"[^0-9\.]", "", regex=True)
            sp_numeric = pd.to_numeric(sp, errors="coerce")
            sold_mask = (sp_numeric == 1)
        else:
            sold_mask = False 

        # Combine masks
        final_mask = date_mask & sold_mask
        
        if final_mask.any():
            filtered_chunks.append(chunk[final_mask].copy())

    if not filtered_chunks:
        return pd.DataFrame(columns=DUP_KEY_COLUMNS), total_rows
        
    return pd.concat(filtered_chunks, ignore_index=True), total_rows

# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------

def run_duplicates_check(
    scan_path: str,
    output_dir: str,
    sample_limit: int = 100,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:

    os.makedirs(output_dir, exist_ok=True)
    ext = os.path.splitext(scan_path)[1].lower()
    
    # 1. Get Latest Date (Fast Pass)
    encoding = "utf-8"
    if ext == ".csv":
        encoding = _detect_encoding(scan_path)
    
    latest_scan_date = _get_latest_scan_date(scan_path, ext, encoding)
    
    if not latest_scan_date:
        raise ValueError("Could not determine a valid latest scan_date.")

    # 2. Read & Filter in Chunks (Memory Efficient)
    df_filtered, rows_before = _read_and_filter_chunks(scan_path, latest_scan_date)
    rows_after_filter = len(df_filtered)

    if df_filtered.empty:
        return {
            "rows_before": rows_before,
            "rows_after_filter": 0,
            "latest_scan_date": latest_scan_date.isoformat(),
            "duplicate_groups": 0,
            "duplicate_rows": 0,
            "output_filename": None,
        }, []

    # 3. Clean numeric columns for grouping (Optimized Regex)
    # We only clean the columns we need for the key
    for col in ["declared_profit", "sold_part", "build_year", "building_mr", "rooms_number"]:
        if col in df_filtered.columns:
            # Remove non-numeric chars
            df_filtered[col] = df_filtered[col].astype(str).str.replace(r"[^0-9\.\-]", "", regex=True)
            # Fill empty with '0' or similar to ensure grouping works? 
            # Actually pandas groupby drops NaNs by default, better to fillna
            df_filtered[col] = df_filtered[col].replace("", "0")

    # 4. Group By (Find duplicates)
    group_cols = [c for c in DUP_KEY_COLUMNS if c in df_filtered.columns]
    
    dup_groups = (
        df_filtered
        .groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="dup_count")
    )
    dup_groups = dup_groups[dup_groups["dup_count"] > 1]

    if dup_groups.empty:
        return {
            "rows_before": rows_before,
            "rows_after_filter": rows_after_filter,
            "latest_scan_date": latest_scan_date.isoformat(),
            "duplicate_groups": 0,
            "duplicate_rows": 0,
            "output_filename": None,
        }, []

    # 5. Merge back to get actual rows
    dup_groups["dup_group_id"] = dup_groups.index + 1
    dup_rows = df_filtered.merge(
        dup_groups[group_cols + ["dup_count", "dup_group_id"]],
        on=group_cols,
        how="inner",
    )
    dup_rows = dup_rows.sort_values(by=["dup_group_id", "dup_count"], ascending=[True, False])

    # 6. Export
    base_name = os.path.splitext(os.path.basename(scan_path))[0]
    today_str = date.today().strftime("%Y%m%d")
    output_filename = f"duplicates_{base_name}_{today_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    dup_rows.to_csv(output_path, index=False, encoding="utf-8-sig")

    results = {
        "rows_before": rows_before,
        "rows_after_filter": rows_after_filter,
        "latest_scan_date": latest_scan_date.isoformat(),
        "duplicate_groups": int(dup_groups["dup_group_id"].nunique()),
        "duplicate_rows": int(len(dup_rows)),
        "output_filename": output_filename,
    }

    sample_rows = dup_rows.head(sample_limit).to_dict(orient="records")
    return results, sample_rows
