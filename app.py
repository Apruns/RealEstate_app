# C:\Ariel Portnik\RealEstate_app\app.py

import os
from datetime import datetime

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    send_from_directory,
    flash,
)
from werkzeug.utils import secure_filename
import pandas as pd

from core.prepare_yzer import run_yzer_preparation
from core.tax_gap_checker import run_tax_gap_check
from core.duplicates_checker import run_duplicates_check

# ------------------------------------------------------------------
# Paths & config
# ------------------------------------------------------------------

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".xlsm"}

app = Flask(__name__, static_folder="static_css", static_url_path="/static")

# SECRET KEY (for Flask messages; taken from env if exists)
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY",
    "change_this_to_a_random_secret_for_local_dev",
)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["OUTPUT_FOLDER"] = OUTPUT_FOLDER


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _allowed_file(filename: str) -> bool:
    """Check if file extension is allowed (generic CSV/Excel)."""
    _, ext = os.path.splitext(filename)
    return ext.lower() in ALLOWED_EXTENSIONS


# ------------------------------------------------------------------
# Routes: Home
# ------------------------------------------------------------------

@app.route("/")
def index():
    """Landing page – go directly to home (no login)."""
    return redirect(url_for("home"))


@app.route("/home")
def home():
    return render_template("home.html", active_tool="home")


# ------------------------------------------------------------------
# Routes: YZER Prep
# ------------------------------------------------------------------

@app.route("/prepare-yzer", methods=["GET", "POST"])
def prepare_yzer_view():
    result = None
    download_filename = None

    if request.method == "POST":
        file = request.files.get("scan_file")

        if not file or file.filename == "":
            flash("Please upload a scan file.", "error")
            return redirect(url_for("prepare_yzer_view"))

        if not _allowed_file(file.filename):
            flash("Unsupported file type. Please upload CSV / Excel.", "error")
            return redirect(url_for("prepare_yzer_view"))

        filename = secure_filename(file.filename)
        input_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(input_path)

        try:
            stats = run_yzer_preparation(
                input_path,
                app.config["OUTPUT_FOLDER"],
            )

            result = stats
            download_filename = stats.get("output_filename")

            flash("Cleaning completed successfully.", "success")
        except Exception as e:
            app.logger.exception("Error during YZER preparation: %s", e)
            flash(f"Error during YZER preparation: {e}", "error")

    return render_template(
        "prepare_yzer.html",
        active_tool="yzer",
        result=result,
        download_filename=download_filename,
    )


# ------------------------------------------------------------------
# Routes: Duplicates Detection
# ------------------------------------------------------------------

@app.route("/duplicates-check", methods=["GET", "POST"])
def duplicates_view():
    """
    Upload a scan file and run duplicates detection.
    Expects core.duplicates_checker.run_duplicates_check to return:
      results, sample_rows
    where:
      - results is a dict with at least 'output_filename'
      - sample_rows is a list of dicts / rows for preview
    """
    results = None
    sample_rows = None
    download_filename = None

    if request.method == "POST":
        file = request.files.get("scan_file")

        if not file or file.filename == "":
            flash("Please upload a scan file.", "error")
            return redirect(url_for("duplicates_view"))

        if not _allowed_file(file.filename):
            flash("Unsupported file type. Please upload CSV / Excel.", "error")
            return redirect(url_for("duplicates_view"))

        filename = secure_filename(file.filename)
        input_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(input_path)

        try:
            # Adjust according to your actual signature if different
            results, sample_rows = run_duplicates_check(
                input_path,
                app.config["OUTPUT_FOLDER"],
            )

            download_filename = results.get("output_filename")
            flash("Duplicates check completed successfully.", "success")
        except Exception as e:
            app.logger.exception("Error during duplicates check: %s", e)
            flash(f"Error during duplicates check: {e}", "error")

    return render_template(
        "duplicates.html",
        active_tool="duplicates",
        results=results,
        sample_rows=sample_rows,
        download_filename=download_filename,
    )


# ------------------------------------------------------------------
# Routes: Tax Gap Check (now supports ZIP with multiple RAMI files)
# ------------------------------------------------------------------

@app.route("/tax-gap-check", methods=["GET", "POST"])
def tax_gap_view():
    results = None
    sample_rows = None
    download_filename = None

    if request.method == "POST":
        scan_file = request.files.get("scan_file")
        rami_file = request.files.get("rami_file")

        if not scan_file or scan_file.filename == "":
            flash("Please upload the internal scan file.", "error")
            return redirect(url_for("tax_gap_view"))

        if not rami_file or rami_file.filename == "":
            flash("Please upload the RAMI file (or ZIP).", "error")
            return redirect(url_for("tax_gap_view"))

        scan_ext = os.path.splitext(scan_file.filename)[1].lower()
        rami_ext = os.path.splitext(rami_file.filename)[1].lower()

        # Scan file must still be a regular CSV/Excel
        if scan_ext not in {".csv", ".xls", ".xlsx", ".xlsm"}:
            flash("Unsupported scan file type. Please upload CSV / Excel.", "error")
            return redirect(url_for("tax_gap_view"))

        # RAMI file can be a single Excel/HTML .xls/.xlsx/.xlsm or a ZIP with multiple files
        if rami_ext not in {".xls", ".xlsx", ".xlsm", ".zip"}:
            flash(
                "Unsupported RAMI file type. Please upload Excel (.xls/.xlsx/.xlsm) "
                "or a .zip containing multiple RAMI files.",
                "error",
            )
            return redirect(url_for("tax_gap_view"))

        scan_name = secure_filename(scan_file.filename)
        rami_name = secure_filename(rami_file.filename)

        scan_path = os.path.join(app.config["UPLOAD_FOLDER"], scan_name)
        rami_path = os.path.join(app.config["UPLOAD_FOLDER"], rami_name)

        scan_file.save(scan_path)
        rami_file.save(rami_path)

        try:
            results, sample_rows = run_tax_gap_check(
                scan_path,
                rami_path,
                app.config["OUTPUT_FOLDER"],
            )
            download_filename = results.get("output_filename")
            flash("Tax gap analysis completed successfully.", "success")
        except Exception as e:
            app.logger.exception("Error during tax gap analysis: %s", e)
            flash(f"Error during tax gap analysis: {e}", "error")

    return render_template(
        "tax_gap.html",
        active_tool="tax_gap",
        results=results,
        sample_rows=sample_rows,
        download_filename=download_filename,
    )


# ------------------------------------------------------------------
# File download route (for outputs)
# ------------------------------------------------------------------

@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(
        app.config["OUTPUT_FOLDER"],
        filename,
        as_attachment=True,
        download_name=filename,
    )


# ------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True)
