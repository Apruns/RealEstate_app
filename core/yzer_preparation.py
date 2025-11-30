# core/yzer_preparation.py

import os
from typing import Optional

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

# Values that should be treated as "empty"
NULL_MARKERS = [
    np.nan,
    "nan",
    "NaN",
    "NAN",
    "None",
    "NaT",
    "nat",
    "NAT",
]


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force-convert configured columns to numeric, mimicking Excel-like behavior.
    - Remove all non-numeric characters except digits, dot and minus.
    - Use pd.to_numeric(..., errors="coerce") so invalid values become NaN.
    """
    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            # Column does not exist in this file – skip silently
            continue

        # Convert to string, remove any character that is NOT digit, dot or minus
        # This handles cases like "1,200,000 ₪" or "  3  rooms".
        series = df[col].astype(str).str.replace(r"[^\d\.\-]", "", regex=True)

        # Force convert to numeric
        df[col] = pd.to_numeric(series, errors="coerce")

    return df


def clean_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse configured columns as dates and format them as short date strings (DD/MM/YYYY).
    - dayfirst=True because the expected format is DD/MM/YYYY.
    - errors="coerce" so invalid values become NaT.
    - Finally, format to "DD/MM/YYYY" strings, as in the reference code.
    """
    for col in DATE_COLUMNS:
        if col not in df.columns:
            continue

        # Parse to datetime
        dt_series = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

        # Format as 'Short Date' string
        df[col] = dt_series.dt.strftime("%d/%m/%Y")

    return df


def global_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply global cleanup rules, matching your example logic:
    - Replace commas with spaces in any scalar (str/int/float) value.
    - Replace '--' with 0.
    - Replace various null markers with empty string.
    - Drop 'scan_date' column if it exists.
    """

    # Replace commas with spaces for all scalar values (similar to df.map(...) in the example)
    df = df.map(
        lambda x: str(x).replace(",", " ")
        if isinstance(x, (str, int, float))
        else x
    )

    # Replace '--' with 0 (this may affect both numeric and text columns)
    df = df.replace("--", 0)

    # Replace null markers with empty string
    df = df.replace(NULL_MARKERS, "", regex=False)

    # Drop 'scan_date' column if it exists
    if "scan_date" in df.columns:
        df = df.drop(columns=["scan_date"])

    return df


def prepare_for_yzer(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Main entry point for the YZER preparation pipeline.

    This function:
    1. Copies the raw DataFrame.
    2. Cleans numeric columns using Excel-like behavior.
    3. Parses and formats date columns as short date strings (DD/MM/YYYY).
    4. Applies global cleanup rules (commas, '--', null markers, scan_date).
    5. Returns the cleaned DataFrame ready for export to YZER.
    """
    df = df_raw.copy()

    # 1) Numeric cleanup
    df = clean_numeric_columns(df)

    # 2) Date cleanup
    df = clean_date_columns(df)

    # 3) Global cleanup (commas, '--', null markers, scan_date)
    df = global_cleanup(df)

    return df


def export_yzer(
    df_clean: pd.DataFrame,
    output_path: str,
    as_csv: bool = True,
) -> str:
    """
    Export the cleaned YZER DataFrame to a file.

    Parameters
    ----------
    df_clean : pd.DataFrame
        The cleaned DataFrame returned by `prepare_for_yzer`.
    output_path : str
        Full path where the file will be saved (without extension handling).
        If as_csv=True, '.csv' is recommended. If as_csv=False, '.xlsx' is recommended.
    as_csv : bool
        If True, exports to CSV with UTF-8 BOM.
        If False, exports to Excel (.xlsx) using openpyxl.

    Returns
    -------
    str
        The actual path of the saved file.
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if as_csv:
        df_clean.to_csv(
            output_path,
            index=False,
            encoding="utf-8-sig",
            na_rep="",
        )
    else:
        # For Excel export, we can still keep date columns as strings,
        # Excel will display them as text formatted DD/MM/YYYY.
        df_clean.to_excel(output_path, index=False)

    return output_path


if __name__ == "__main__":
    """
    Simple manual test for the module.
    Update `input_path` and `output_path` before running this file directly.
    """
    test_input_path = r"C:\Ariel Portnik\matan_deals\yad2_scan_example.xlsx"
    test_output_path = r"C:\Ariel Portnik\matan_deals\yad2_scan_example_yzer_ready.csv"

    if os.path.exists(test_input_path):
        raw_df = pd.read_excel(test_input_path)
        cleaned_df = prepare_for_yzer(raw_df)
        saved_path = export_yzer(cleaned_df, test_output_path, as_csv=True)
        print(f"Cleaned YZER file saved to: {saved_path}")
    else:
        print(f"Test input file not found: {test_input_path}")
