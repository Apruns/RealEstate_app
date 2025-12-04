# core/prepare_yzer.py

import os
from datetime import date
from typing import Dict, Any, List, Tuple

import pandas as pd
import numpy as np


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

# Numeric columns (names are compared case-insensitively)
NUMERIC_TARGETS = {
    "declared_profit",
    "sale_profit",
    "full_price",
    "declared_value",
    "declared_value_dollar",
    "estimate_price",
    "estimate_price_dollar",
    "price_per_room",
    "rooms_number",
    "room_num2",
}

# Date columns (short date style)
DATE_TARGETS = {
    "deal_date",
    "sale_day",
}


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def _read_scan_file(scan_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 1 – Read scan file (CSV / Excel) as text first.
    Returns (df, info).
    """
    ext = os.path.splitext(scan_path)[1].lower()
    info: Dict[str, Any] = {"extension": ext}

    if ext == ".csv":
        # Try a couple of encodings – start with utf-8, then cp1255, then latin1
        last_error = None
        for enc in ["utf-8", "cp1255", "latin1"]:
            try:
                df = pd.read_csv(scan_path, dtype=str, encoding=enc)
                info["encoding"] = enc
                break
            except Exception as e:
                last_error = e
        else:
            raise ValueError(
                f"Could not read CSV file with common encodings. Last error: {last_error}"
            )
    elif ext in (".xls", ".xlsx", ".xlsm"):
        df = pd.read_excel(scan_path, dtype=str)
        info["encoding"] = "excel"
    else:
        raise ValueError(f"Unsupported file type for YZER preparation: {ext}")

    info["rows_before"] = int(len(df))
    info["columns_before"] = int(len(df.columns))
    info["column_names"] = list(df.columns)

    return df, info


def _build_case_insensitive_map(df: pd.DataFrame) -> Dict[str, str]:
    """
    Build a mapping: lower-case column name -> actual column name.
    """
    mapping: Dict[str, str] = {}
    for col in df.columns:
        mapping[col.lower()] = col
    return mapping


def _convert_numeric_columns(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, int]]]:
    """
    Step 2 – Convert numeric columns.
    (The '--' -> 0 replacement is done globally before this step.)

    Returns:
        df (modified),
        numeric_info: {col_name: {"converted": int, "invalid": int}}
    """
    ci_map = _build_case_insensitive_map(df)
    numeric_info: Dict[str, Dict[str, int]] = {}

    for target in NUMERIC_TARGETS:
        if target not in ci_map:
            continue  # column not present

        col_name = ci_map[target]

        # Work on a copy as strings
        series = df[col_name].astype(str).str.strip()

        # Remove inner spaces ("1 234" -> "1234")
        series = series.str.replace(" ", "", regex=False)

        # Remove thousands separators
        series = series.str.replace(",", "", regex=False)
        series = series.str.replace("'", "", regex=False)

        # Remove any non-digit / non-dot / non-minus characters (currency etc.)
        series = series.str.replace(r"[^0-9\.\-]", "", regex=True)

        numeric_series = pd.to_numeric(series, errors="coerce")

        converted = int(numeric_series.notna().sum())
        invalid = int(len(numeric_series) - converted)

        df[col_name] = numeric_series

        numeric_info[col_name] = {
            "converted": converted,
            "invalid": invalid,
        }

    return df, numeric_info


def _convert_date_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Dict[str, int]]]:
    """
    Step 3 – Convert deal_date, sale_day to datetime (short date style).
    Returns:
        df (modified),
        date_info: {col_name: {"parsed": int, "invalid": int}}
    """
    ci_map = _build_case_insensitive_map(df)
    date_info: Dict[str, Dict[str, int]] = {}

    for target in DATE_TARGETS:
        if target not in ci_map:
            continue
        col_name = ci_map[target]

        raw_series = df[col_name]

        # Try to parse directly; supports dd/mm/yyyy, dd.mm.yyyy, etc.
        parsed = pd.to_datetime(raw_series, dayfirst=True, errors="coerce")

        parsed_count = int(parsed.notna().sum())
        invalid_count = int(len(parsed) - parsed_count)

        df[col_name] = parsed

        date_info[col_name] = {
            "parsed": parsed_count,
            "invalid": invalid_count,
        }

    return df, date_info


def _replace_commas_in_text(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Step 4 – Replace ',' with ' ' in all text columns (non-numeric, non-datetime).
    Returns:
        df (modified),
        stats: {"columns": int, "cells_changed": int}
    """
    text_cols: List[str] = []
    cells_changed = 0

    for col in df.columns:
        # We consider datetime64 and number dtypes as non-text; everything else -> text
        if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_numeric_dtype(df[col]):
            continue

        if df[col].dtype == object:
            text_cols.append(col)
            series = df[col].astype(str)
            # Count how many cells actually contain commas
            has_comma = series.str.contains(",", na=False)
            cells_changed += int(has_comma.sum())
            df[col] = series.str.replace(",", " ", regex=False)

    stats = {
        "columns": len(text_cols),
        "cells_changed": cells_changed,
    }
    return df, stats


def _drop_scan_date_column(df: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
    """
    Step 6 – Drop 'scan_date' column if exists (case-insensitive).
    Returns:
        df (modified),
        removed (bool)
    """
    ci_map = _build_case_insensitive_map(df)
    if "scan_date" in ci_map:
        real_name = ci_map["scan_date"]
        df = df.drop(columns=[real_name])
        return df, True
    return df, False


# ---------------------------------------------------------
# Public API
# ---------------------------------------------------------

def run_yzer_preparation(scan_path: str, output_dir: str) -> Dict[str, Any]:
    """
    Full pipeline for preparing a scan file for YZER:
      Step 1: read file
      Step 1.5: '--' -> 0 (global, like df.replace("--", 0))
      Step 2: numeric conversion
      Step 3: date conversion
      Step 4: commas -> spaces in text
      Step 5: drop scan_date column
      Step 6: global NaN / placeholder cleanup

    Returns a stats dict with all information required for the UI.
    """
    os.makedirs(output_dir, exist_ok=True)

    # --- Step 1: read file ---
    df, file_info = _read_scan_file(scan_path)

    # --- Step 1.5: global '--' -> 0, exactly like your working snippet ---
    dash_mask = df == "--"
    dash_to_zero_count = int(dash_mask.sum().sum())
    if dash_to_zero_count:
        df = df.replace("--", 0)

    # --- Step 2: numeric conversion ---
    df, numeric_info = _convert_numeric_columns(df)

    # --- Step 3: date conversion ---
    df, date_info = _convert_date_columns(df)

    # --- Step 4: replace commas in text ---
    df, text_commas_info = _replace_commas_in_text(df)

    # --- Step 5: drop scan_date ---
    df, scan_date_removed = _drop_scan_date_column(df)

    # --- Step 6: global NaN / placeholder cleanup ---
    df = df.replace(
        [np.nan, "nan", "NaN", "NAN", "None", "NaT", "nat", "NAT"],
        "",
        regex=False,
    )

    # Rows/cols after all operations
    rows_after = int(len(df))
    cols_after = int(len(df.columns))

    # --- Export cleaned file ---
    base_name = os.path.splitext(os.path.basename(scan_path))[0]
    today_str = date.today().strftime("%Y%m%d")
    output_filename = f"yzer_ready_{base_name}_{today_str}.csv"
    output_path = os.path.join(output_dir, output_filename)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    # --- Build stats dict ---
    stats: Dict[str, Any] = {
        # General
        "extension": file_info.get("extension"),
        "encoding": file_info.get("encoding"),
        "rows_before": file_info.get("rows_before"),
        "columns_before": file_info.get("columns_before"),
        "rows_after": rows_after,
        "columns_after": cols_after,
        "column_names": file_info.get("column_names", []),

        # Steps
        "numeric_info": numeric_info,
        "date_info": date_info,
        "text_commas": text_commas_info,
        "dash_to_zero": {"occurrences": dash_to_zero_count},
        "scan_date_removed": scan_date_removed,

        # Output
        "output_filename": output_filename,
        "output_path": output_path,
    }

    return stats


# ---------------------------------------------------------
# Backwards-compatible wrapper (optional)
# ---------------------------------------------------------

def prepare_for_yzer(scan_path: str, output_dir: str):
    """
    Legacy wrapper to match the old API that returned:
        (output_filename, row_count)
    """
    stats = run_yzer_preparation(scan_path, output_dir)
    return stats["output_filename"], stats["rows_after"]
