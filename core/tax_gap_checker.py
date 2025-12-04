import os
import re
import zipfile
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd


# ------------------------------------------------------------------
# Column configuration
# ------------------------------------------------------------------

# Hebrew → canonical English
HEBREW_TO_CANONICAL = {
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

# English variants / aliases → canonical English
ENGLISH_ALIASES = {
    "block lot": "block_lot",
    "block_lot": "block_lot",
    "blocklot": "block_lot",

    "sale day": "sale_day",
    "sale_day": "sale_day",

    "declared profit": "declared_profit",
    "declared_profit": "declared_profit",

    "sale profit": "sale_profit",
    "sale_profit": "sale_profit",

    "sold part": "sold_part",
    "sold_part": "sold_part",

    "city": "city",

    "build year": "build_year",
    "build_year": "build_year",

    "building mr": "building_mr",
    "building_mr": "building_mr",

    "rooms number": "rooms_number",
    "rooms_number": "rooms_number",
}

KEY_COLUMNS = [
    "block_lot",
    "sale_day",
    "declared_profit",
    "sale_profit",
    "sold_part",
    "build_year",
    "building_mr",
    "rooms_number",
]

NUMERIC_COLUMNS = [
    "declared_profit",
    "sale_profit",
    "sold_part",
    "build_year",
    "building_mr",
    "rooms_number",
]

DATE_COLUMNS = ["sale_day"]


def _format_ts(ts: Optional[pd.Timestamp]) -> str:
    """Format a Timestamp (or None) to a yyyy-mm-dd string or 'unknown'."""
    if isinstance(ts, pd.Timestamp) and not pd.isna(ts):
        return ts.strftime("%Y-%m-%d")
    if isinstance(ts, str) and ts:
        return ts
    return "unknown"


# ------------------------------------------------------------------
# Helpers: reading & normalizing data
# ------------------------------------------------------------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename Hebrew and English-variant columns to the canonical English names.
    Works for both scan and RAMI dataframes.
    """
    rename_map: Dict[str, str] = {}

    for col in df.columns:
        col_str = str(col).strip()

        # Hebrew exact match
        if col_str in HEBREW_TO_CANONICAL:
            rename_map[col] = HEBREW_TO_CANONICAL[col_str]
            continue

        # English aliases (case-insensitive)
        col_lower = col_str.lower()
        if col_lower in ENGLISH_ALIASES:
            rename_map[col] = ENGLISH_ALIASES[col_lower]

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _clean_numeric_and_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize numeric and date columns in-place and return df."""
    # Numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            series = df[col].astype(str)
            series = (
                series.str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace("\u200f", "", regex=False)
                .str.replace("\u200e", "", regex=False)
            )
            df[col] = pd.to_numeric(series, errors="coerce")

    # Date columns
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


def _read_scan_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in (".xls", ".xlsx", ".xlsm"):
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported scan file type: {ext}")
    df = _normalize_columns(df)
    df = _clean_numeric_and_dates(df)
    return df


def _read_rami_file(path: str) -> pd.DataFrame:
    """
    RAMI file can be a real Excel or an HTML-style .xls file.
    We try Excel first; if that fails we treat it as HTML.
    """
    ext = os.path.splitext(path)[1].lower()

    if ext in (".xlsx", ".xlsm", ".xls"):
        # Try as Excel first
        try:
            df = pd.read_excel(path)
        except Exception:
            # HTML-style .xls (RAMI style)
            if ext == ".xls":
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    html = f.read()
                tables = pd.read_html(html)
                if not tables:
                    raise ValueError("Failed to parse RAMI .xls file as HTML.")
                df = tables[0]
            else:
                raise
    else:
        raise ValueError(f"Unsupported RAMI file type: {ext}")

    df = _normalize_columns(df)
    df = _clean_numeric_and_dates(df)
    return df


# ------------------------------------------------------------------
# Parse filter & dates from cells A2–A4 (preferred) or filename (fallback)
# ------------------------------------------------------------------

def _parse_rami_from_cells(path: str) -> Optional[Tuple[str, str, Optional[pd.Timestamp], Optional[pd.Timestamp]]]:
    """
    Try to identify filter_type (city/block), filter_value and date range
    by reading the first column cells A2–A4 (rows 2–4) from the RAMI file.
    If parsing fails, return None and let caller fallback to filename logic.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext not in (".xls", ".xlsx", ".xlsm"):
        return None

    try:
        meta_df = pd.read_excel(path, header=None, usecols=[0], nrows=4)
    except Exception:
        return None

    # Extract A2–A4 as strings
    values: List[str] = []
    for row_idx in range(1, 4):  # row indices 1,2,3 => A2,A3,A4
        if row_idx < len(meta_df.index):
            v = meta_df.iloc[row_idx, 0]
            if pd.notna(v):
                text = str(v).strip()
                if text:
                    values.append(text)

    if not values:
        return None

    meta_text = " ".join(values)

    # --- Detect type & filter value ---

    has_hebrew = bool(re.search(r"[\u0590-\u05FF]", meta_text))
    digits = re.findall(r"\d+", meta_text)

    filter_type: Optional[str] = None
    filter_value: Optional[str] = None

    # 1) Explicit "גוש" → block
    if "גוש" in meta_text:
        filter_type = "block"
        m = re.search(r"גוש\s*([\d, ]+)", meta_text)
        if m:
            nums = re.findall(r"\d+", m.group(1))
            if nums:
                filter_value = ",".join(nums)
        if not filter_value and digits:
            filter_value = ",".join(digits)

    # 2) No "גוש", but digits and no Hebrew → also block (e.g. "3653_")
    elif digits and not has_hebrew:
        filter_type = "block"
        filter_value = ",".join(digits)

    # 3) Otherwise → city, take A2 as city name
    else:
        filter_type = "city"
        filter_value = values[0]

    # --- Dates from A2–A4 text ---

    # Allow dd.mm.yy, dd.mm.yyyy, dd/mm/yy, dd/mm/yyyy
    date_strings = re.findall(r"\d{1,2}[./]\d{1,2}[./]\d{2,4}", meta_text)
    date_from: Optional[pd.Timestamp] = None
    date_to: Optional[pd.Timestamp] = None

    if len(date_strings) >= 1:
        date_from = pd.to_datetime(date_strings[0], dayfirst=True, errors="coerce")
    if len(date_strings) >= 2:
        date_to = pd.to_datetime(date_strings[1], dayfirst=True, errors="coerce")

    # If we couldn't get any reasonable filter info, bail out
    if not filter_type or not filter_value:
        return None

    return filter_type, filter_value, date_from, date_to


def _parse_rami_from_filename(path: str) -> Tuple[str, str, Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Fallback: old behavior – use filename if we can't read A2–A4.
    """
    base = os.path.splitext(os.path.basename(path))[0]

    # dates like dd.mm.yy or dd.mm.yyyy
    date_strings = re.findall(r"\d{2}\.\d{2}\.\d{2,4}", base)
    date_from: Optional[pd.Timestamp] = None
    date_to: Optional[pd.Timestamp] = None
    if len(date_strings) >= 2:
        date_from = pd.to_datetime(date_strings[0], dayfirst=True, errors="coerce")
        date_to = pd.to_datetime(date_strings[1], dayfirst=True, errors="coerce")

    # part before the first '-'
    left_part = base.split("-")[0].strip()

    has_hebrew = bool(re.search(r"[\u0590-\u05FF]", left_part))
    digits = re.findall(r"\d+", left_part)

    # 1) explicit "גוש" => block file
    if "גוש" in left_part:
        filter_type = "block"
        filter_value = digits[0] if digits else left_part

    # 2) numeric-only (no Hebrew letters) => block file
    elif digits and not has_hebrew:
        filter_type = "block"
        filter_value = ",".join(digits)

    # 3) otherwise => city file
    else:
        filter_type = "city"
        filter_value = left_part

    return filter_type, filter_value, date_from, date_to


def _parse_rami_context(path: str) -> Tuple[str, str, Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Main entry: first try A2–A4; if that fails, fallback to filename.
    """
    meta_res = _parse_rami_from_cells(path)
    if meta_res is not None:
        return meta_res
    return _parse_rami_from_filename(path)


# ------------------------------------------------------------------
# Filtering helpers
# ------------------------------------------------------------------

def _extract_block_ids_from_series(series: pd.Series) -> pd.Series:
    """
    Take a Series of 'block_lot'-style values (e.g. '028048-0058-010-00')
    and return a Series of normalized block IDs as strings (e.g. '28048').
    """
    block_raw = series.astype(str).str.split("-", n=1).str[0]
    block_digits = block_raw.str.extract(r"(\d+)", expand=False)
    block_digits = block_digits.fillna("").str.lstrip("0")
    return block_digits


def _filter_rami_by_dates(
    df: pd.DataFrame,
    date_from: Optional[pd.Timestamp],
    date_to: Optional[pd.Timestamp],
) -> pd.DataFrame:
    """
    RAMI files are already city/block-specific, so here we only filter by date.
    """
    out = df.copy()
    if "sale_day" in out.columns and date_from is not None and date_to is not None:
        out = out[(out["sale_day"] >= date_from) & (out["sale_day"] <= date_to)]
    return out


def _filter_scan_by_context(
    df_scan: pd.DataFrame,
    filter_type: str,
    filter_value: str,
    date_from: Optional[pd.Timestamp],
    date_to: Optional[pd.Timestamp],
    rami_context_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Apply date range + city/block filter on the internal scan file.

    City:
      - Prefer using distinct city names from RAMI
        (`rami_context_df['city']`) and keep scan rows whose city is in that set.
      - If not available, fall back to using filter_value as a substring on city.

    Block:
      - Prefer using distinct block IDs from RAMI (`block_lot` → normalized),
        and keep scan rows whose normalized block ID is in that set.
      - If not available, fall back to using filter_value (digits only).
    """
    out = df_scan.copy()

    # --- Date range ---
    if "sale_day" in out.columns and date_from is not None and date_to is not None:
        out = out[(out["sale_day"] >= date_from) & (out["sale_day"] <= date_to)]

    if rami_context_df is None:
        return out

    # --- City context ---
    if filter_type == "city" and "city" in out.columns:
        if "city" in rami_context_df.columns:
            cities = (
                rami_context_df["city"]
                .dropna()
                .astype(str)
                .unique()
            )
            cities_set = {c for c in cities if c}
            if cities_set:
                out = out[out["city"].astype(str).isin(cities_set)]
                return out

        # Fallback: use filter_value from cells/filename
        fv = str(filter_value).strip()
        if fv:
            pattern = re.escape(fv)
            out = out[out["city"].astype(str).str.contains(pattern, case=False, na=False)]
        return out

    # --- Block context ---
    if filter_type == "block" and "block_lot" in out.columns:
        if "block_lot" in rami_context_df.columns:
            rami_blocks = _extract_block_ids_from_series(rami_context_df["block_lot"])
            rami_blocks_set = {b for b in rami_blocks.unique() if b}
            if rami_blocks_set:
                scan_blocks = _extract_block_ids_from_series(out["block_lot"])
                out = out[scan_blocks.isin(rami_blocks_set)]
                return out

        # Fallback: use filter_value digits only
        fv_digits = re.sub(r"\D", "", str(filter_value))
        fv_block = fv_digits.lstrip("0") or fv_digits
        scan_blocks = _extract_block_ids_from_series(out["block_lot"])
        out = out[scan_blocks == fv_block]
        return out

    return out


def _ensure_required_columns(df: pd.DataFrame, label: str) -> None:
    missing = [c for c in KEY_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"{label} is missing required columns: {', '.join(missing)}. "
            f"Make sure the file headers match the expected schema."
        )


# ------------------------------------------------------------------
# Core per-RAMI-file logic
# ------------------------------------------------------------------

def _gap_for_one_rami(
    scan_df_all: pd.DataFrame,
    rami_path: str,
) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Run the gap logic for a single RAMI file (already on disk).
    Returns:
      file_stats: dict describing this RAMI file
      missing_df: DataFrame of missing deals (RAMI not in scan) for this file
    In case of error, file_stats['status'] = 'error' and missing_df is empty.
    """
    file_name = os.path.basename(rami_path)

    try:
        # 1. Read RAMI and parse context
        rami_df_all = _read_rami_file(rami_path)
        rami_rows_total = len(rami_df_all)

        filter_type, filter_value, date_from, date_to = _parse_rami_context(rami_path)
        date_from_str = _format_ts(date_from)
        date_to_str = _format_ts(date_to)

        # 2. Filter RAMI by dates
        rami_filtered = _filter_rami_by_dates(rami_df_all, date_from, date_to)

        # 3. Filter scan by context
        scan_filtered = _filter_scan_by_context(
            scan_df_all,
            filter_type,
            filter_value,
            date_from,
            date_to,
            rami_context_df=rami_filtered,
        )

        # 4. Ensure required columns exist
        _ensure_required_columns(
            scan_filtered if len(scan_filtered) > 0 else scan_df_all,
            "Scan file (after filtering)",
        )
        _ensure_required_columns(
            rami_filtered if len(rami_filtered) > 0 else rami_df_all,
            "RAMI file (after filtering)",
        )

        # 5. Compare keys: which RAMI deals are missing in scan?
        merge_cols = KEY_COLUMNS.copy()

        scan_keys = scan_filtered[merge_cols].drop_duplicates()

        merged = rami_filtered.merge(
            scan_keys,
            on=merge_cols,
            how="left",
            indicator=True,
        )

        missing_df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        missing_count = int(len(missing_df))
        rami_rows_filtered = int(len(rami_filtered))
        scan_rows_filtered = int(len(scan_filtered))

        file_stats: Dict[str, Any] = {
            "rami_filename": file_name,
            "status": "ok",
            "filter_type": filter_type,
            "filter_value": filter_value,
            "date_from": date_from_str,
            "date_to": date_to_str,
            "rami_rows_total": int(rami_rows_total),
            "rami_rows_filtered": rami_rows_filtered,
            "scan_rows_filtered": scan_rows_filtered,
            "missing_count": missing_count,
            "error_message": "",
        }

        return file_stats, missing_df

    except Exception as e:
        # In case of any error – mark this file as error but do not stop the whole process
        file_stats = {
            "rami_filename": file_name,
            "status": "error",
            "filter_type": None,
            "filter_value": None,
            "date_from": None,
            "date_to": None,
            "rami_rows_total": 0,
            "rami_rows_filtered": 0,
            "scan_rows_filtered": 0,
            "missing_count": 0,
            "error_message": str(e),
        }
        return file_stats, pd.DataFrame()


# ------------------------------------------------------------------
# Main public function – supports single RAMI or ZIP with many
# ------------------------------------------------------------------

def run_tax_gap_check(
    scan_path: str,
    rami_path: str,
    output_dir: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    RAMI vs scan comparison.

    Supports:
      - Single RAMI Excel/.xls/.xlsx/.xlsm file
      - ZIP with multiple RAMI files inside

    Returns:
      stats: dict with global summary and per-file details
      sample_rows: list of up to 50 dicts (preview of missing deals across all files)
    """
    os.makedirs(output_dir, exist_ok=True)

    # 1. Load scan once
    scan_df_all = _read_scan_file(scan_path)
    scan_rows_total = int(len(scan_df_all))

    rami_ext = os.path.splitext(rami_path)[1].lower()

    all_files_stats: List[Dict[str, Any]] = []
    missing_all_df_list: List[pd.DataFrame] = []

    # ------------------------------------------------------------------
    # Case A: ZIP with multiple RAMI files
    # ------------------------------------------------------------------
    if rami_ext == ".zip":
        with zipfile.ZipFile(rami_path, "r") as zf:
            members = [
                m for m in zf.namelist()
                if m.lower().endswith((".xls", ".xlsx", ".xlsm"))
            ]

            if not members:
                # No usable RAMI files in zip – record one error entry
                all_files_stats.append({
                    "rami_filename": os.path.basename(rami_path),
                    "status": "error",
                    "filter_type": None,
                    "filter_value": None,
                    "date_from": None,
                    "date_to": None,
                    "rami_rows_total": 0,
                    "rami_rows_filtered": 0,
                    "scan_rows_filtered": 0,
                    "missing_count": 0,
                    "error_message": "ZIP file does not contain any .xls/.xlsx/.xlsm RAMI files.",
                })
            else:
                # Extract to a temporary subdirectory under output_dir
                tmp_dir = os.path.join(output_dir, "_rami_zip_tmp")
                os.makedirs(tmp_dir, exist_ok=True)

                for member in members:
                    # Flatten any inner folders
                    safe_name = member.replace("/", "_")
                    extracted_path = os.path.join(tmp_dir, safe_name)

                    with zf.open(member) as src, open(extracted_path, "wb") as dst:
                        dst.write(src.read())

                    file_stats, missing_df = _gap_for_one_rami(
                        scan_df_all,
                        extracted_path,
                    )
                    all_files_stats.append(file_stats)

                    if file_stats.get("status") == "ok" and not missing_df.empty:
                        df_copy = missing_df.copy()
                        df_copy["rami_source"] = file_stats.get("rami_filename")
                        missing_all_df_list.append(df_copy)

    # ------------------------------------------------------------------
    # Case B: Single RAMI Excel / HTML style .xls
    # ------------------------------------------------------------------
    else:
        file_stats, missing_df = _gap_for_one_rami(
            scan_df_all,
            rami_path,
        )
        all_files_stats.append(file_stats)

        if file_stats.get("status") == "ok" and not missing_df.empty:
            df_copy = missing_df.copy()
            df_copy["rami_source"] = file_stats.get("rami_filename")
            missing_all_df_list.append(df_copy)

    # ------------------------------------------------------------------
    # Aggregate results across all files
    # ------------------------------------------------------------------

    if missing_all_df_list:
        missing_all_df = pd.concat(missing_all_df_list, ignore_index=True)
    else:
        missing_all_df = pd.DataFrame()

    rami_rows_total_all = int(sum(f.get("rami_rows_total", 0) for f in all_files_stats))
    missing_total = int(len(missing_all_df))

    file_count_total = len(all_files_stats)
    file_count_success = sum(1 for f in all_files_stats if f.get("status") == "ok")
    file_count_error = sum(1 for f in all_files_stats if f.get("status") == "error")

    # Per-file percentages
    for f in all_files_stats:
        rami_rows_filtered = f.get("rami_rows_filtered", 0) or 0
        missing_count = f.get("missing_count", 0) or 0

        if rami_rows_filtered > 0 and missing_count > 0:
            f["missing_pct_of_file"] = round(missing_count / rami_rows_filtered * 100, 2)
        else:
            f["missing_pct_of_file"] = 0.0

        if rami_rows_total_all > 0 and missing_count > 0:
            f["missing_pct_of_global_deals"] = round(
                missing_count / rami_rows_total_all * 100, 2
            )
        else:
            f["missing_pct_of_global_deals"] = 0.0

    if rami_rows_total_all > 0 and missing_total > 0:
        global_missing_pct = round(missing_total / rami_rows_total_all * 100, 2)
    else:
        global_missing_pct = 0.0

    # ------------------------------------------------------------------
    # Write combined output CSV (if there are any missing deals)
    # ------------------------------------------------------------------
    if not missing_all_df.empty:
        if rami_ext == ".zip":
            output_filename = "tax_gap_multi_summary.csv"
        else:
            # Single file – re-use the first file's filter info for the name
            f0 = all_files_stats[0]
            filter_type = f0.get("filter_type") or "unknown_filter"
            filter_val = str(f0.get("filter_value") or "unknown").replace(" ", "_")
            date_from_str = _format_ts(f0.get("date_from"))
            date_to_str = _format_ts(f0.get("date_to"))
            output_filename = f"tax_gap_{filter_type}_{filter_val}_{date_from_str}_to_{date_to_str}.csv"

        output_path = os.path.join(output_dir, output_filename)
        missing_all_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    else:
        output_filename = None
        output_path = None

    # ------------------------------------------------------------------
    # Build global stats dict
    # ------------------------------------------------------------------
    stats: Dict[str, Any] = {
        "scan_rows_total": scan_rows_total,
        "rami_rows_total_all": rami_rows_total_all,
        "missing_total": missing_total,
        "global_missing_pct": global_missing_pct,
        "file_count_total": file_count_total,
        "file_count_success": file_count_success,
        "file_count_error": file_count_error,
        "files": all_files_stats,
        "output_filename": output_filename,
        "output_path": output_path,
    }

    sample_rows = missing_all_df.head(50).to_dict(orient="records")

    return stats, sample_rows
