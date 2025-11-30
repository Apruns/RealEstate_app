# core/gap_checker.py

from typing import List, Dict, Any, Tuple
import pandas as pd


# Columns used to identify a transaction when comparing Scan vs Tax file.
# NOTE: property_type is intentionally NOT included here.
DEFAULT_JOIN_KEYS: List[str] = [
    "block_lot",
    "sale_day",
    "declared_profit",
    "sold_part",
    "city",
    "build_year",
    "building_mr",
    "rooms_number",
]

# Which columns we want to force to numeric / date
NUMERIC_COLS_FOR_JOIN: List[str] = [
    "declared_profit",
    "sale_profit",
    "sold_part",
    "build_year",
    "building_mr",
    "rooms_number",
]
DATE_COLS: List[str] = ["sale_day"]


def extract_goch_from_block_lot(value: Any) -> str:
    """
    Extract goch (block) from a block_lot value.

    Example:
        "028048-0058-010-00" → "28048"

    Rules:
    - Take the substring before the first '-'
    - Keep only digits
    - Strip leading zeros
    """
    if value is None:
        return ""

    s = str(value)
    first_part = s.split("-")[0]
    digits_only = "".join(ch for ch in first_part if ch.isdigit())
    if not digits_only:
        return ""
    stripped = digits_only.lstrip("0")
    return stripped or "0"


def _ensure_columns_exist(df: pd.DataFrame, cols: List[str], context: str) -> None:
    """
    Ensure that all columns in 'cols' exist in the given DataFrame.
    Raise a clear error if any are missing.
    """
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {context}: {missing}")


def normalize_numeric_and_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize numeric and date columns to have consistent types.

    For numeric columns:
      - Convert to string
      - Strip any non-digit/dot/minus characters (commas, spaces, currency symbols)
      - Convert to numeric (coerce errors to NaN)

    For date columns:
      - Convert to datetime with dayfirst=True
    """
    df = df.copy()

    for col in NUMERIC_COLS_FOR_JOIN:
        if col in df.columns:
            # Clean as Excel would: keep only digits, '.' and '-'
            df[col] = df[col].astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    return df


def infer_query_from_tax_file(
    df_tax: pd.DataFrame,
    date_col: str = "sale_day",
) -> Dict[str, Any]:
    """
    Infer the effective query that was used to generate the Tax file.

    Logic:
    - Date range:
        * date_from = min(sale_day)
        * date_to   = max(sale_day)
    - Location:
        * If 'city' exists and has values → use it (location_type = 'city')
        * Else derive goch from 'block_lot' (location_type = 'goch')

    Returns a dict:
    {
      "date_from": <Timestamp>,
      "date_to": <Timestamp>,
      "location_type": "city" or "goch",
      "location_values": list of unique cities or gochim
    }
    """
    if date_col not in df_tax.columns:
        raise ValueError(f"Tax file is missing date column '{date_col}'")

    dates = pd.to_datetime(df_tax[date_col], errors="coerce", dayfirst=True)
    if dates.notna().sum() == 0:
        raise ValueError(f"Could not parse any dates in tax file column '{date_col}'")

    date_from = dates.min()
    date_to = dates.max()

    # Try to use city as location if available
    if "city" in df_tax.columns and df_tax["city"].notna().any():
        location_type = "city"
        location_values = sorted(df_tax["city"].dropna().unique().tolist())
    else:
        # Fallback to goch derived from block_lot
        if "block_lot" not in df_tax.columns:
            raise ValueError("Tax file has neither 'city' nor 'block_lot' columns.")
        goch_series = df_tax["block_lot"].map(extract_goch_from_block_lot)
        location_type = "goch"
        location_values = sorted(
            [g for g in goch_series.dropna().unique().tolist() if g]
        )

    return {
        "date_from": date_from,
        "date_to": date_to,
        "location_type": location_type,
        "location_values": location_values,
    }


def filter_scan_by_tax_query(
    df_scan: pd.DataFrame,
    tax_query: Dict[str, Any],
    date_col: str = "sale_day",
) -> pd.DataFrame:
    """
    Apply the same query parameters (date range + location) inferred from the Tax file
    to the Scan file.

    - Date column is 'sale_day'.
    - Location:
        * If tax_query["location_type"] == "city":
             match df_scan['city'] ∈ location_values
        * If 'goch':
             derive goch from df_scan['block_lot'] and match to location_values
    """
    if date_col not in df_scan.columns:
        raise ValueError(f"Scan file is missing date column '{date_col}'")

    date_from = tax_query["date_from"]
    date_to = tax_query["date_to"]
    location_type = tax_query["location_type"]
    location_values = tax_query["location_values"]

    # Date filter (we assume normalize_numeric_and_dates already parsed dates,
    # but calling to_datetime again is safe and idempotent)
    scan_dates = pd.to_datetime(df_scan[date_col], errors="coerce", dayfirst=True)
    date_mask = (scan_dates >= date_from) & (scan_dates <= date_to)

    # Location filter
    if location_type == "city":
        if "city" not in df_scan.columns:
            raise ValueError("Scan file has no 'city' column but tax query is by city.")
        loc_mask = df_scan["city"].isin(location_values)
    elif location_type == "goch":
        if "block_lot" not in df_scan.columns:
            raise ValueError("Scan file has no 'block_lot' column but tax query is by goch.")
        scan_goch = df_scan["block_lot"].map(extract_goch_from_block_lot)
        loc_mask = scan_goch.isin(location_values)
    else:
        raise ValueError(f"Unsupported location_type: {location_type}")

    # Combined filter
    filtered = df_scan[date_mask & loc_mask].copy()
    return filtered


def find_missing_transactions(
    df_scan_raw: pd.DataFrame,
    df_tax_raw: pd.DataFrame,
    date_col: str = "sale_day",
    join_keys: List[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Core reconciliation logic (Data Reconciliation & Gap Analysis):

    1. Normalize numeric and date columns in BOTH Tax and Scan dataframes.
    2. Infer the effective query from the Tax file (date range + location).
    3. Apply the exact same query parameters to the Scan file.
    4. Compare Tax vs Scan using join_keys to find transactions that exist in
       the Tax file but are missing from the Scan file.

    Returns:
        missing_df: DataFrame with Tax rows that are not present in the Scan file
        meta: {
          "date_from": ...,
          "date_to": ...,
          "location_type": ...,
          "location_values": [...],
          "join_keys": [...]
        }
    """
    if join_keys is None:
        join_keys = DEFAULT_JOIN_KEYS

    # 1) Normalize numeric and date columns
    df_tax_norm = normalize_numeric_and_dates(df_tax_raw)
    df_scan_norm = normalize_numeric_and_dates(df_scan_raw)

    # 2) Infer query (from Tax file)
    tax_query = infer_query_from_tax_file(df_tax_norm, date_col=date_col)

    # 3) Filter Scan file using the same query
    df_scan_filtered = filter_scan_by_tax_query(df_scan_norm, tax_query, date_col=date_col)

    # 4) Ensure join keys exist in both dataframes
    _ensure_columns_exist(df_tax_norm, join_keys, "Tax file")
    _ensure_columns_exist(df_scan_filtered, join_keys, "Scan file (filtered)")

    # 5) Prepare keys for merge (keep real types, no string casting now)
    tax_keys = df_tax_norm[join_keys].copy()
    scan_keys = df_scan_filtered[join_keys].copy()

    scan_keys_marked = scan_keys.copy()
    scan_keys_marked["__in_scan__"] = True

    # 6) Left-join Tax on Scan keys
    merged = tax_keys.merge(
        scan_keys_marked,
        how="left",
        on=join_keys,
    )

    # __in_scan__ is NaN when there is no match in the Scan file
    found_mask = merged["__in_scan__"].fillna(False).astype(bool)

    # 7) Attach flag to the full Tax dataframe (original tax rows, but with flag)
    tax_with_flag = df_tax_raw.copy()
    tax_with_flag["found_in_scan"] = found_mask

    # 8) Keep only missing transactions (Tax but not in Scan)
    missing_df = tax_with_flag[~tax_with_flag["found_in_scan"]].copy()

    meta = {
        "date_from": tax_query["date_from"],
        "date_to": tax_query["date_to"],
        "location_type": tax_query["location_type"],
        "location_values": tax_query["location_values"],
        "join_keys": join_keys,
    }

    return missing_df, meta


if __name__ == "__main__":
    """
    Optional manual test (update paths before running).
    """
    import os
    from core.file_loader import load_scan_file, load_rami_file

    scan_path = r"C:\Ariel Portnik\RealEstate_app\examples\yad2_scan_2025_10.xlsx"
    tax_path = r"C:\Ariel Portnik\RealEstate_app\examples\קריית שמונה - 27_05_2025 - 27_08_2025.xls"

    if os.path.exists(scan_path) and os.path.exists(tax_path):
        df_scan = load_scan_file(scan_path)
        df_tax = load_rami_file(tax_path)

        missing, meta = find_missing_transactions(df_scan, df_tax)

        print("Query metadata:")
        print("  Date from      :", meta["date_from"])
        print("  Date to        :", meta["date_to"])
        print("  Location type  :", meta["location_type"])
        print("  Location values:", meta["location_values"])
        print("  Join keys      :", meta["join_keys"])
        print("Missing rows     :", len(missing))
    else:
        print("Please update test paths in gap_checker.py")
