# core/rami_parser.py

import os
import re
from typing import Tuple, Dict, Any, List

import numpy as np
import pandas as pd

# --------------------------------------
# Column mapping & schema
# --------------------------------------

RAMI_HEADER_MAP = {
    "גוש חלקה": "block_lot",
    "יום מכירה": "sale_day",
    'תמורה מוצהרת בש"ח': "declared_profit",
    'שווי מכירה בש"ח': "sale_profit",
    "מהות": "property_type",
    "חלק נמכר": "sold_part",
    "ישוב": "city",
    "שנת בניה": "build_year",
    "שטח": "building_mr",
    "חדרים": "rooms_number",
}

NUMERIC_COLS = [
    "declared_profit",
    "sale_profit",
    "sold_part",
    "build_year",
    "building_mr",
    "rooms_number",
]

DATE_COLS = ["sale_day"]


# --------------------------------------
# Helpers
# --------------------------------------

def _load_excel_or_html(path: str) -> pd.DataFrame:
    """
    Try read_excel. If it fails (HTML-style .xls), fallback to read_html.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        try:
            return pd.read_excel(path)
        except Exception:
            # HTML-style .xls
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                html = f.read()
            tables = pd.read_html(html)
            if not tables:
                raise ValueError("No tables found in RAMI HTML file.")
            return tables[0]
    else:
        raise ValueError(f"Unsupported RAMI file type: {ext}")


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {}
    for col in df.columns:
        s = str(col).strip()
        new_cols[col] = RAMI_HEADER_MAP.get(s, s)
    return df.rename(columns=new_cols)


def _cast_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            continue
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace("\u00a0", "", regex=False)
            .replace({"": np.nan, "nan": np.nan, "NaN": np.nan})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _cast_dates(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
    return df


def _parse_dates_from_filename(filename: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    From filename like:
      'ירושלים - 26.01.25-28.01.25 (2).xls'
      'גוש 6631 - 01.03.25-24.11.25.xls'
    extract two dates (dd.mm.yy).
    """
    base = os.path.basename(filename)
    matches = re.findall(r"\d{2}\.\d{2}\.\d{2}", base)
    if len(matches) >= 2:
        try:
            d1 = pd.to_datetime(matches[0], format="%d.%m.%y", dayfirst=True)
            d2 = pd.to_datetime(matches[1], format="%d.%m.%y", dayfirst=True)
            return min(d1, d2), max(d1, d2)
        except Exception:
            pass
    return None, None


# --------------------------------------
# Public API
# --------------------------------------

def load_and_normalize_rami(path: str) -> pd.DataFrame:
    """
    Load RAMI file and normalize:
    - headers (Hebrew -> English)
    - numeric & date columns
    """
    df_raw = _load_excel_or_html(path)
    df = df_raw.copy()
    df = _normalize_headers(df)
    df = _cast_numeric(df, NUMERIC_COLS)
    df = _cast_dates(df, DATE_COLS)
    return df


def extract_context(path: str, df_rami: pd.DataFrame) -> Dict[str, Any]:
    """
    Extract filters from RAMI:
    - file_type: 'city' or 'block'
    - date_from, date_to
    - list of cities
    - list of block_lot
    """
    base = os.path.basename(path)
    base_name = os.path.splitext(base)[0].strip()

    # detect type by filename
    if base_name.startswith("גוש"):
        file_type = "block"
    else:
        file_type = "city"

    # dates from filename, fallback to sale_day column
    date_from, date_to = _parse_dates_from_filename(base)
    if (date_from is None or date_to is None) and "sale_day" in df_rami.columns:
        non_null = df_rami["sale_day"].dropna()
        if not non_null.empty:
            date_from = non_null.min()
            date_to = non_null.max()

    if date_from is None or date_to is None:
        raise ValueError("Could not determine date range from RAMI file name or content.")

    cities = []
    if "city" in df_rami.columns:
        cities = (
            df_rami["city"]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

    block_lots = []
    if "block_lot" in df_rami.columns:
        block_lots = (
            df_rami["block_lot"]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

    return {
        "file_type": file_type,   # 'city' or 'block'
        "date_from": date_from,
        "date_to": date_to,
        "cities": cities,
        "block_lots": block_lots,
    }
