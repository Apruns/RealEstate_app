# core/duplicates_checker.py

import os
from datetime import date
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

# העמודות שעל פיהן מזהים כפילויות – חייבות להיות קיימות בקובץ
DUP_KEY_COLUMNS: List[str] = [
    "block_lot",
    "sale_day",
    "declared_profit",
    "sold_part",
    "city",
    "build_year",
    "building_mr",
    "rooms_number",
    "scan_date",
]


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _read_scan_file(scan_path: str) -> pd.DataFrame:
    """
    קורא את קובץ הסריקה (CSV / Excel) ומחזיר DataFrame.
    אין כאן ניקוי או המרה – רק טעינה.
    """
    ext = os.path.splitext(scan_path)[1].lower()

    if ext == ".csv":
        # לא מכריח dtype=str – נותן לפנדהס לנחש, אנחנו נטפל בעמודות החשובות ידנית
        df = pd.read_csv(scan_path)
    elif ext in (".xls", ".xlsx", ".xlsm"):
        df = pd.read_excel(scan_path)
    else:
        raise ValueError(f"Unsupported file type for duplicates check: {ext}")

    return df


def _ensure_required_columns(df: pd.DataFrame) -> None:
    """מוודא שכל העמודות הדרושות קיימות; אחרת זורק שגיאה ברורה."""
    missing = [col for col in DUP_KEY_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns for duplicates check: "
            + ", ".join(missing)
        )


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------

def run_duplicates_check(
    scan_path: str,
    output_dir: str,
    sample_limit: int = 100,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    מריץ את תהליך איתור הכפילויות על קובץ סריקה אחד.

    לוגיקה:
      1. קריאת הקובץ.
      2. מציאת תאריך ה-scan האחרון בעמודה scan_date.
      3. סינון לשורות:
           - scan_date == latest_scan_date
           - sold_part == 1
      4. GROUP BY על כל עמודות המפתח (כולל scan_date) וספירת שורות.
      5. שמירת קבוצות עם dup_count > 1 בלבד.
      6. Merge חזרה ל-DataFrame לקבלת כל השורות הכפולות בפועל.
      7. שמירת קובץ CSV עם כל הכפילויות והחזרת סטטיסטיקות + sample rows.

    מחזיר:
      results: dict עם נתונים לסיכום במסך.
      sample_rows: רשימת dict-ים לתצוגה בטבלה (עד sample_limit שורות).
    """
    os.makedirs(output_dir, exist_ok=True)

    # --- Step 1: Read file ---
    df = _read_scan_file(scan_path)
    rows_before = int(len(df))

    # --- Step 2: Ensure required columns exist ---
    _ensure_required_columns(df)

    # --- Step 3: Parse latest scan_date ---
    # שומר גם את ערך המחרוזת המקורי וגם את ה-parsed
    scan_parsed = pd.to_datetime(df["scan_date"], errors="coerce", dayfirst=True)
    if scan_parsed.isna().all():
        raise ValueError("Could not parse any valid dates in 'scan_date' column.")

    latest_scan_ts = scan_parsed.max()
    latest_scan_date = latest_scan_ts.date()  # לשימוש בסיכום / תצוגה

    # --- Step 4: Filter to latest scan_date & sold_part = 1 ---
    sold_numeric = pd.to_numeric(df["sold_part"], errors="coerce")
    mask = (scan_parsed == latest_scan_ts) & (sold_numeric == 1)

    df_filtered = df.loc[mask].copy()
    rows_after_filter = int(len(df_filtered))

    if rows_after_filter == 0:
        # אין שורות לסרוק – מחזירים סטטיסטיקות בלבד, ללא כפילויות
        results = {
            "rows_before": rows_before,
            "rows_after_filter": rows_after_filter,
            "latest_scan_date": latest_scan_date.isoformat(),
            "duplicate_groups": 0,
            "duplicate_rows": 0,
            "key_columns": DUP_KEY_COLUMNS,
            "output_filename": None,
            "output_path": None,
        }
        return results, []

    # --- Step 5: Group by key columns and count ---
    group_cols = DUP_KEY_COLUMNS

    dup_groups = (
        df_filtered
        .groupby(group_cols, dropna=False)
        .size()
        .reset_index(name="dup_count")
    )

    # HAVING count(*) > 1
    dup_groups = dup_groups[dup_groups["dup_count"] > 1]

    if dup_groups.empty:
        # יש שורות אחרונות, אבל אין כפילויות
        results = {
            "rows_before": rows_before,
            "rows_after_filter": rows_after_filter,
            "latest_scan_date": latest_scan_date.isoformat(),
            "duplicate_groups": 0,
            "duplicate_rows": 0,
            "key_columns": DUP_KEY_COLUMNS,
            "output_filename": None,
            "output_path": None,
        }
        return results, []

    # --- Step 6: Assign group IDs and merge back to actual rows ---
    dup_groups = dup_groups.reset_index(drop=True)
    dup_groups["dup_group_id"] = dup_groups.index + 1  # 1..N

    dup_rows = df_filtered.merge(
        dup_groups[group_cols + ["dup_count", "dup_group_id"]],
        on=group_cols,
        how="inner",
    )

    # קצת סדר: למיין לפי dup_group_id ואז dup_count (ירידה)
    dup_rows = dup_rows.sort_values(
        by=["dup_group_id", "dup_count"],
        ascending=[True, False],
    )

    duplicate_groups = int(dup_groups["dup_group_id"].nunique())
    duplicate_rows = int(len(dup_rows))

    # --- Step 7: Export CSV with all duplicate rows ---
    base_name = os.path.splitext(os.path.basename(scan_path))[0]
    today_str = date.today().strftime("%Y%m%d")
    output_filename = f"duplicates_{base_name}_{today_str}.csv"
    output_path = os.path.join(output_dir, output_filename)

    dup_rows.to_csv(output_path, index=False, encoding="utf-8-sig")

    # --- Build results dict ---
    results: Dict[str, Any] = {
        "rows_before": rows_before,
        "rows_after_filter": rows_after_filter,
        "latest_scan_date": latest_scan_date.isoformat(),
        "duplicate_groups": duplicate_groups,
        "duplicate_rows": duplicate_rows,
        "key_columns": DUP_KEY_COLUMNS,
        "output_filename": output_filename,
        "output_path": output_path,
    }

    # sample rows לתצוגה
    sample_df = dup_rows.head(sample_limit)
    sample_rows: List[Dict[str, Any]] = sample_df.to_dict(orient="records")

    return results, sample_rows
