# core/file_loader.py

import os
import pandas as pd


# Mapping from Hebrew RAMI column names to the internal English schema
RAMI_COLUMN_MAP = {
    "גוש חלקה": "block_lot",
    "יום מכירה": "sale_day",
    'תמורה מוצהרת בש"ח': "declared_profit",
    'שווי מכירה בש"ח': "sale_profit",
    "מהות": "property_type",
    "חלק נמכר": "sold_part",
    "ישוב": "city",
    "שנת בניה": "build_year",
    "שטח": "building_mr",   # ✅ fixed: was 'area', now 'building_mr'
    "חדרים": "rooms_number",
}


def _normalize_rami_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename RAMI columns from Hebrew to the internal English names.

    The function only renames a column if:
    - The Hebrew name exists in df.columns, and
    - The English target name does NOT already exist
      (to avoid overwriting if the file is already in English).
    """
    rename_map = {}
    for heb, eng in RAMI_COLUMN_MAP.items():
        if heb in df.columns and eng not in df.columns:
            rename_map[heb] = eng

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def load_scan_file(path: str) -> pd.DataFrame:
    """
    Load a scan file (Excel or CSV) and return it as a DataFrame.
    This is used for internal Yad2 scan files.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Scan file not found: {path}")

    lower = path.lower()

    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(path)
    elif lower.endswith(".csv"):
        return pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported scan file type: {path}")


def load_rami_file(path: str) -> pd.DataFrame:
    """
    Load a Tax Authority (RAMI) file and return a DataFrame
    with normalized column names (English).

    Supported cases:
    - Real Excel: .xlsx / .xls
    - "Fake Excel": HTML content saved with .xls extension
    - HTML: .html / .htm

    For .xls/.xlsx:
      1. Try pd.read_excel
      2. If that fails, fall back to pd.read_html and take the first table
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"RAMI file not found: {path}")

    lower = path.lower()

    # Excel-like extensions
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        # Try native Excel first
        try:
            df = pd.read_excel(path)
        except Exception:
            # Fall back to HTML parsing (common for "Excel-like" HTML files)
            tables = pd.read_html(path)
            if not tables:
                raise ValueError(f"Could not read RAMI file as Excel or HTML: {path}")
            df = tables[0]

        return _normalize_rami_columns(df)

    # HTML extensions
    if lower.endswith(".html") or lower.endswith(".htm"):
        tables = pd.read_html(path)
        if not tables:
            raise ValueError(f"No tables found in RAMI HTML file: {path}")
        df = tables[0]
        return _normalize_rami_columns(df)

    # Unsupported
    raise ValueError(f"Unsupported RAMI file type: {path}")


if __name__ == "__main__":
    """
    Optional manual test (update paths before running).
    """
    scan_path = r"C:\Ariel Portnik\RealEstate_app\examples\yad2_scan_2025_10.xlsx"
    rami_path = r"C:\Ariel Portnik\RealEstate_app\examples\קריית שמונה - 27_05_2025 - 27_08_2025.xls"

    if os.path.exists(scan_path):
        df_scan = load_scan_file(scan_path)
        print("Scan file loaded. Shape:", df_scan.shape)
    else:
        print("Scan test file not found (update path in file_loader.py).")

    if os.path.exists(rami_path):
        df_rami = load_rami_file(rami_path)
        print("RAMI file loaded. Shape:", df_rami.shape)
        print("RAMI columns after mapping:", list(df_rami.columns))
    else:
        print("RAMI test file not found (update path in file_loader.py).")
