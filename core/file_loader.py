import os
from io import BytesIO

import pandas as pd


def load_scan_file(file_storage):
    """
    Load a 'scan file' (internal data) from an uploaded file.

    Supported formats:
    - .csv
    - .xlsx / .xls (normal Excel)

    Returns a pandas DataFrame with the original columns.
    """
    filename = (file_storage.filename or "").lower()

    if filename.endswith(".csv"):
        # CSV scan file
        df = pd.read_csv(file_storage)
        return df

    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        # Standard Excel scan file
        df = pd.read_excel(file_storage)
        return df

    # Fallback: try to read as Excel
    df = pd.read_excel(file_storage)
    return df


def load_ram_file(file_storage):
    """
    Load a 'Tax Authority' (RAMI) file from an uploaded file.

    The file may be:
    - A normal Excel (.xlsx / .xls)
    - An HTML-style .xls exported from the Tax Authority site

    This function:
    1. Reads the table.
    2. Renames Hebrew columns to our internal English names.
    3. Returns a DataFrame with at least these columns (if they exist in source):
       - block_lot
       - sale_day
       - declared_profit
       - sale_profit
       - property_type
       - sold_part
       - city
       - build_year
       - building_mr
       - rooms_number
    """
    filename = (file_storage.filename or "").lower()

    # Read file content into bytes (so we can reuse it for html/excel parsing)
    raw_bytes = file_storage.read()
    file_storage.seek(0)  # reset pointer for any future use

    # Decide how to parse
    if filename.endswith(".xlsx"):
        # Standard Excel
        df = pd.read_excel(BytesIO(raw_bytes))
    elif filename.endswith(".csv"):
        # Rare case: RAMI as CSV
        df = pd.read_csv(BytesIO(raw_bytes))
    elif filename.endswith(".xls") or filename.endswith(".html") or filename.endswith(".htm"):
        # Very common RAMI format: .xls that is actually HTML
        # read_html will parse tables out of the HTML
        tables = pd.read_html(raw_bytes)
        if not tables:
            # Fallback: try Excel engine anyway
            df = pd.read_excel(BytesIO(raw_bytes))
        else:
            # Take the largest table (usually the deals table)
            df = max(tables, key=lambda t: len(t))
    else:
        # Unknown extension – try excel as a best guess
        df = pd.read_excel(BytesIO(raw_bytes))

    # Mapping from Hebrew column names to our internal schema
    hebrew_to_internal = {
        "גוש חלקה": "block_lot",
        "יום מכירה": "sale_day",
        "תמורה מוצהרת בש\"ח": "declared_profit",
        "שווי מכירה בש\"ח": "sale_profit",
        "מהות": "property_type",
        "חלק נמכר": "sold_part",
        "ישוב": "city",
        "שנת בניה": "build_year",
        "שטח": "building_mr",
        "חדרים": "rooms_number",
    }

    # Rename any columns that match the Hebrew names
    df = df.rename(columns=hebrew_to_internal)

    # Keep only the columns we care about (if they exist)
    desired_cols = list(hebrew_to_internal.values())
    existing_cols = [c for c in desired_cols if c in df.columns]

    if not existing_cols:
        # If mapping failed completely, just return the original df
        # (This will help us debug on real files if needed)
        return df

    df = df[existing_cols].copy()

    return df
