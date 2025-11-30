"""
Utilities for preparing a scan file for YZER upload.

Main responsibilities:
- Force numeric columns to be real numbers (similar to how Excel interprets them).
- Force date columns into a strict short-date format (DD/MM/YYYY).
- Apply global text cleanup (commas, special markers, NaN-like values).
- Drop technical columns that should not be sent (e.g. scan_date).
- Export a cleaned CSV file.

Used by: /prepare-yzer route in app.py
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd


# Columns that should be treated as numeric
NUMERIC_COLUMNS = [
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
]

# Columns that should be treated as dates
DATE_COLUMNS = [
    "sale_day",
    "deal_date",
]

# Columns that we do not want in the final YZER file
COLUMNS_TO_DROP = [
    "scan_date",
]


def _force_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert configured NUMERIC_COLUMNS to numeric values.

    - Removes any characters that are not digits, decimal point, or minus sign.
    - Uses pd.to_numeric(errors="coerce") so invalid values become NaN.
    """

    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            continue

        # Work on a string representation, then strip non-numeric characters
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[^\d.-]", "", regex=True)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _force_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert configured DATE_COLUMNS to datetime, then format as DD/MM/YYYY.
    """

    for col in DATE_COLUMNS:
        if col not in df.columns:
            continue

        # Parse to datetime (dayfirst=True to match Israeli date format)
        dt_series = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

        # Format as short date string (DD/MM/YYYY)
        df[col] = dt_series.dt.strftime("%d/%m/%Y")

    return df


def _global_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply global cleanup rules:
    - Replace commas in textual fields with spaces.
    - Replace '--' with 0.
    - Replace various NaN-like tokens with an empty string.
    """

    # Replace commas with spaces for any scalar value (str/int/float)
    df = df.map(
        lambda x: str(x).replace(",", " ")
        if isinstance(x, (str, int, float))
        else x
    )

    # Convert '--' to 0
    df = df.replace("--", 0)

    # Replace different NaN-like markers with empty string
    df = df.replace(
        [np.nan, "nan", "NaN", "NAN", "None", "NaT", "nat", "NAT"],
        "",
        regex=False,
    )

    return df


def _drop_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop technical or internal columns that should not appear in the final file.
    """
    existing_to_drop = [c for c in COLUMNS_TO_DROP if c in df.columns]
    if existing_to_drop:
        df = df.drop(columns=existing_to_drop)
    return df


def prepare_yzer_file(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function used by the Flask route.

    Receives a raw scan DataFrame and returns a cleaned DataFrame
    ready to be exported as a YZER upload file.
    """

    # 1. Force numeric types for numeric columns
    df = _force_numeric(df)

    # 2. Force date formats
    df = _force_dates(df)

    # 3. Global cleanup (commas, NaNs, '--', etc.)
    df = _global_cleanup(df)

    # 4. Drop internal/technical columns
    df = _drop_unwanted_columns(df)

    return df


def export_yzer(df: pd.DataFrame, original_name: str | None = None) -> str:
    """
    Export the cleaned DataFrame to a CSV file.

    - The output file will be placed in the current working directory.
    - Name pattern: <original_base>_yzer_<YYYYMMDD_HHMMSS>.csv
      If original_name is missing, uses "scan_file" as base.

    Returns:
        Absolute path to the created file.
    """

    # Base name for file
    if original_name:
        base_name = os.path.splitext(os.path.basename(original_name))[0]
    else:
        base_name = "scan_file"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_name = f"{base_name}_yzer_{timestamp}.csv"

    output_path = os.path.join(os.getcwd(), output_name)

    # Ensure directory exists (normally it will be the project root)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    return output_path
