# core/duplicates_checker.py

import os
from datetime import date
from typing import Dict, Any, List, Tuple
import pandas as pd

DUP_KEY_COLUMNS: List[str] = [
    "block_lot", "sale_day", "declared_profit", "sold_part", 
    "city", "build_year", "building_mr", "rooms_number", "scan_date"
]

def run_duplicates_on_chunks(
    chunk_paths: List[str], 
    output_dir: str, 
    meta_data: Dict[str, Any],
    sample_limit: int = 100
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:

    os.makedirs(output_dir, exist_ok=True)
    
    latest_scan_date = meta_data.get("max_scan_date")
    if not latest_scan_date:
        # Fallback or Error
        raise ValueError("Could not determine a valid latest scan_date from the file.")
        
    latest_ts = pd.Timestamp(latest_scan_date)
    filtered_dfs = []
    total_rows_scanned = 0

    # 1. Filter Pass
    for chunk_path in chunk_paths:
        try:
            chunk = pd.read_csv(chunk_path, dtype=str)
        except pd.errors.EmptyDataError:
            continue
            
        total_rows_scanned += len(chunk)
        
        # Column mapping
        cols_map = {str(c).lower(): c for c in chunk.columns}
        
        # Filter Logic
        if "scan_date" in cols_map:
            col_name = cols_map["scan_date"]
            # Parse Dates
            d_series = pd.to_datetime(chunk[col_name], dayfirst=True, errors='coerce')
            date_mask = (d_series == latest_ts)
        else:
            date_mask = False
            
        if "sold_part" in cols_map:
            col_name = cols_map["sold_part"]
            sp = chunk[col_name].str.replace(r"[^0-9\.]", "", regex=True)
            sp_num = pd.to_numeric(sp, errors='coerce')
            sold_mask = (sp_num == 1)
        else:
            sold_mask = False
            
        final_mask = date_mask & sold_mask
        
        if final_mask.any():
            filtered_dfs.append(chunk[final_mask].copy())

    # 2. Combine & Group
    if not filtered_dfs:
         return {
            "rows_before": total_rows_scanned,
            "duplicate_groups": 0,
            "duplicate_rows": 0,
            "output_filename": None,
            "latest_scan_date": latest_scan_date.isoformat()
        }, []

    df_filtered = pd.concat(filtered_dfs, ignore_index=True)
    
    # Clean keys for grouping
    for col in DUP_KEY_COLUMNS:
        # ensure col exists
        found = False
        for c in df_filtered.columns:
            if c.lower() == col.lower():
                df_filtered = df_filtered.rename(columns={c: col})
                found = True
                break
        if not found:
            df_filtered[col] = "0"

    # Numeric clean on keys
    num_keys = ["declared_profit", "sold_part", "build_year", "building_mr", "rooms_number"]
    for k in num_keys:
        df_filtered[k] = df_filtered[k].astype(str).str.replace(r"[^0-9\.\-]", "", regex=True).replace("", "0")

    # Group
    dup_groups = df_filtered.groupby(DUP_KEY_COLUMNS, dropna=False).size().reset_index(name="dup_count")
    dup_groups = dup_groups[dup_groups["dup_count"] > 1]
    
    if dup_groups.empty:
         return {
            "rows_before": total_rows_scanned,
            "duplicate_groups": 0,
            "duplicate_rows": 0,
            "output_filename": None,
            "latest_scan_date": latest_scan_date.isoformat()
        }, []
        
    # Merge back
    dup_groups["dup_group_id"] = dup_groups.index + 1
    dup_rows = df_filtered.merge(
        dup_groups[DUP_KEY_COLUMNS + ["dup_count", "dup_group_id"]],
        on=DUP_KEY_COLUMNS,
        how="inner"
    )
    dup_rows = dup_rows.sort_values(by=["dup_group_id", "dup_count"], ascending=[True, False])

    # Export
    base_name = os.path.splitext(meta_data.get("original_filename", "scan"))[0]
    today_str = date.today().strftime("%Y%m%d")
    output_filename = f"duplicates_{base_name}_{today_str}.csv"
    output_path = os.path.join(output_dir, output_filename)
    dup_rows.to_csv(output_path, index=False, encoding="utf-8-sig")

    results = {
        "rows_before": total_rows_scanned,
        "rows_after_filter": len(df_filtered),
        "latest_scan_date": latest_scan_date.isoformat(),
        "duplicate_groups": int(dup_groups["dup_group_id"].nunique()),
        "duplicate_rows": int(len(dup_rows)),
        "output_filename": output_filename,
    }
    sample_rows = dup_rows.head(sample_limit).to_dict(orient="records")
    return results, sample_rows
