# core/prepare_yzer.py

import os
from datetime import date
from typing import Dict, Any, List, Tuple

import pandas as pd
import numpy as np


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

# Exact canonical order for the transaction table.
# Columns are matched case-insensitively to the input.
# If a column from this list does not exist in the input file,
# we will create it as an empty column ("") and keep it in place.
PREFERRED_COLUMN_ORDER: List[str] = [
    "id",
    "code",
    "start_block",
    "end_block",
    "start_lot",
    "end_lot",
    "page",
    "line",
    "block_lot",
    "sale_day",
    "declared_profit",
    "sale_profit",
    "property_type",
    "sold_part",
    "full_price",
    "city",
    "build_year",
    "building_mr",
    "rooms_number",
    "area",
    "goch",
    "deal_date",
    "city2",
    "street",
    "home_num",
    "entrance_num",
    "apartment_num",
    "declared_value",
    "declared_value_dollar",
    "estimate_price",
    "estimate_price_dollar",
    "area_mr_bruto",
    "room_num2",
    "roof",
    "area_mr_neto",
    "floor",
    "warehouse",
    "first_build_year",
    "number_of_floors",
    "yard",
    "price_per_room",
    "apartments_in_building",
    "migrash",
    "price_per_mr",
    "parking",
    "gallery",
    "deal_type",
    "building_function",
    "house_function",
    "shuma_parts",
    "goch_appearance",
    "according_tva",
    "right_meaning",
    "neighborhood",
    "front",
    "building_pcnt",
    "registered_area",
    "building_phase_end",
    "designation",
    "mevune_area",
    "hashuma",
    "ground_function",
    "tva_detail",
    "front_len",
    "building_rights",
    "elevator_num",
    "field_area_mr",
    "scan_date",
]

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


def _reorder_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Step 1.b – Reorder columns into the canonical YZER order and
    create empty columns for any missing preferred columns.

    Logic:
      - For each name in PREFERRED_COLUMN_ORDER:
          * If the column exists in the input (case-insensitive),
            it is copied and renamed to the canonical name.
          * If it does not exist, we create a new column filled
            with empty strings ("").
      - Any remaining original columns that are not part of the
        preferred list are appended at the end, preserving their
        original relative order.
    """
    current_cols = list(df.columns)
    ci_map = _build_case_insensitive_map(df)

    new_data: Dict[str, pd.Series] = {}
    found_preferred: List[str] = []
    created_empty: List[str] = []

    # 1) Build the canonical block (preferred order)
    for canonical in PREFERRED_COLUMN_ORDER:
        lc = canonical.lower()
        if lc in ci_map:
            real_col = ci_map[lc]
            new_data[canonical] = df[real_col]
            found_preferred.append(real_col)
        else:
            # Column does not exist – create empty
            new_data[canonical] = pd.Series(
                [""] * len(df),
                index=df.index,
            )
            created_empty.append(canonical)

    # 2) Append any extra columns that were not mapped to preferred names
    remaining_cols = [c for c in current_cols if c not in found_preferred]
    for col in remaining_cols:
        new_data[col] = df[col]

    df_reordered = pd.DataFrame(new_data, index=df.index)

    reorder_info: Dict[str, Any] = {
        "preferred_order": PREFERRED_COLUMN_ORDER,
        "created_empty_columns": created_empty,
        "extra_columns_appended": remaining_cols,
        "total_columns_before": len(current_cols),
        "total_columns_after": len(df_reordered.columns),
    }

    return df_reordered, reorder_info


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
            continue  # column not present (or created as non-matching)

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
    Step 5 – Drop 'scan_date' column if exists (case-insensitive).
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
      Step 1.b: reorder columns to canonical order,
                create empty columns for missing ones
      Step 1.5: '--' -> 0 (global)
      Step 2: numeric conversion
      Step 2.1: full_price = sale_profit / sold_part
      Step 3: date conversion
      Step 3.1: fill empty deal_date with sale_day and format as DD/MM/YYYY
      Step 3.2: fill empty city2 with city
      Step 3.3: fill empty room_num2 with rooms_number
      Step 4: commas -> spaces in text
      Step 5: drop scan_date column
      Step 6: global NaN / placeholder cleanup
      Step 7: integer formatting for prices (using pandas Int64)

    Returns a stats dict with all information required for the UI.
    """
    os.makedirs(output_dir, exist_ok=True)

    # --- Step 1: read file ---
    df, file_info = _read_scan_file(scan_path)

    # --- Step 1.b: reorder columns into canonical order + create missing ones ---
    df, reorder_info = _reorder_columns(df)

    # --- Step 1.5: global '--' -> 0 ---
    dash_mask = df == "--"
    dash_to_zero_count = int(dash_mask.sum().sum())
    if dash_to_zero_count:
        df = df.replace("--", 0)

    # --- Step 2: numeric conversion ---
    df, numeric_info = _convert_numeric_columns(df)

    # --- Step 2.1: compute full_price = sale_profit / sold_part ---
    # We override any existing values in full_price.
    sale_col = "sale_profit"
    sold_part_col = "sold_part"
    if sale_col in df.columns and sold_part_col in df.columns:
        sale_series = df[sale_col]
        sold_part_series = df[sold_part_col]

        # Avoid division by zero and NaN
        valid_mask = (
            pd.to_numeric(sold_part_series, errors="coerce").notna()
            & (sold_part_series != 0)
            & pd.to_numeric(sale_series, errors="coerce").notna()
        )

        full_price = pd.Series(np.nan, index=df.index, dtype="float64")
        full_price.loc[valid_mask] = (
            sale_series[valid_mask].astype(float) /
            sold_part_series[valid_mask].astype(float)
        )

        df["full_price"] = full_price

        converted_fp = int(full_price.notna().sum())
        invalid_fp = int(len(full_price) - converted_fp)

        numeric_info["full_price"] = {
            "converted": converted_fp,
            "invalid": invalid_fp,
        }

    # --- Step 3: date conversion (to datetime objects) ---
    df, date_info = _convert_date_columns(df)

    # --- Step 3.1: Fill empty deal_date with sale_day ---
    if "deal_date" in df.columns and "sale_day" in df.columns:
        df["deal_date"] = df["deal_date"].fillna(df["sale_day"])

    # --- Step 3.2: Fill empty city2 with city ---
    # city/city2 are text (or object). "city2" might be empty strings if created by reorder.
    if "city2" in df.columns and "city" in df.columns:
        # Treat empty strings as NaN so fillna works, then fill
        df["city2"] = df["city2"].replace("", np.nan).fillna(df["city"])

    # --- Step 3.3: Fill empty room_num2 with rooms_number ---
    # Both are numeric targets, so they are float/NaN by this point.
    if "room_num2" in df.columns and "rooms_number" in df.columns:
        df["room_num2"] = df["room_num2"].fillna(df["rooms_number"])

    # --- Step 3.4: Force format to DD/MM/YYYY strings ---
    for col in DATE_TARGETS:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%d/%m/%Y')

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

    # --- Step 7: FIX - Ensure Integer formatting using Int64 ---
    # We round the values and cast to Int64 (Nullable Integer).
    # This removes .0 and allows NaNs, without converting to string.
    price_cols = [
        "declared_profit", "sale_profit", "full_price",
        "declared_value", "declared_value_dollar",
        "estimate_price", "estimate_price_dollar",
        "price_per_room", "price_per_mr"
    ]
    for col in price_cols:
        if col in df.columns:
            # Round to nearest whole number, then cast to nullable Int64
            df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    # Rows/cols after all operations
    rows_after = int(len(df))
    cols_after = int(len(df.columns))

    # --- Export cleaned file ---
    base_name = os.path.splitext(os.path.basename(scan_path))[0]
    today_str = date.today().strftime("%Y%m%d")
    output_filename = f"yzer_ready_{base_name}_{today_str}.csv"
    output_path = os.path.join(output_dir, output_filename)

    # FIX: Add line_terminator for Windows compatibility
    df.to_csv(output_path, index=False, encoding="utf-8-sig", line_terminator='\r\n')

    # --- Build stats dict ---
    stats: Dict[str, Any] = {
        # General
        "extension": file_info.get("extension"),
        "encoding": file_info.get("encoding"),
        "rows_before": file_info.get("rows_before"),
        "columns_before": file_info.get("columns_before"),
        "rows_after": rows_after,
        "columns_after": cols_after,
        "column_names_before": file_info.get("column_names", []),
        "column_names_after": list(df.columns),

        # Reordering
        "reorder_info": reorder_info,

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
