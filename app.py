# app.py

import os
import shutil
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

# --- Import Core Modules ---
# Ensure you have created core/file_splitter.py as discussed!
from core import file_splitter 
from core import prepare_yzer
from core import duplicates_checker
from core.tax_gap_checker import run_tax_gap_check

# ------------------------------------------------------------------
# Paths & config
# ------------------------------------------------------------------

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".xlsm", ".zip"}

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
    """Check if file extension is allowed."""
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
# Routes: YZER Prep (Optimized with Splitter)
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
        
        # Temp folder for chunks
        temp_chunk_dir = os.path.join(app.config["UPLOAD_FOLDER"], "temp_yzer_chunks")

        try:
            # 1. SPLIT INPUT FILE
            chunk_paths, metadata = file_splitter.split_file_to_chunks(input_path, temp_chunk_dir)

            # 2. PROCESS CHUNKS
            stats = prepare_yzer.run_yzer_on_chunks(
                chunk_paths,
                app.config["OUTPUT_FOLDER"],
                metadata.get("original_filename", filename)
            )

            result = stats
            download_filename = stats.get("output_filename")

            flash("Cleaning completed successfully.", "success")
        
        except Exception as e:
            app.logger.exception("Error during YZER preparation: %s", e)
            flash(f"Error during YZER preparation: {e}", "error")
            
        finally:
            # 3. CLEANUP TEMP CHUNKS
            if os.path.exists(temp_chunk_dir):
                shutil.rmtree(temp_chunk_dir)

    return render_template(
        "prepare_yzer.html",
        active_tool="yzer",
        result=result,
        download_filename=download_filename,
    )


# ------------------------------------------------------------------
# Routes: Duplicates Detection (Optimized with Splitter)
# ------------------------------------------------------------------

@app.route("/duplicates-check", methods=["GET", "POST"])
def duplicates_view():
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

        # Temp folder for chunks
        temp_chunk_dir = os.path.join(app.config["UPLOAD_FOLDER"], "temp_dup_chunks")

        try:
            # 1. SPLIT INPUT FILE
            # This also extracts max_scan_date efficiently
            chunk_paths, metadata = file_splitter.split_file_to_chunks(input_path, temp_chunk_dir)
            
            # 2. PROCESS CHUNKS
            results, sample_rows = duplicates_checker.run_duplicates_on_chunks(
                chunk_paths,
                app.config["OUTPUT_FOLDER"],
                metadata
            )

            download_filename = results.get("output_filename")
            flash("Duplicates check completed successfully.", "success")
        
        except Exception as e:
            app.logger.exception("Error during duplicates check: %s", e)
            flash(f"Error during duplicates check: {e}", "error")
            
        finally:
            # 3. CLEANUP TEMP CHUNKS
            if os.path.exists(temp_chunk_dir):
                shutil.rmtree(temp_chunk_dir)

    return render_template(
        "duplicates.html",
        active_tool="duplicates",
        results=results,
        sample_rows=sample_rows,
        download_filename=download_filename,
    )


# ------------------------------------------------------------------
# Routes: Tax Gap Check (Existing Logic)
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

        # Save files
        scan_name = secure_filename(scan_file.filename)
        rami_name = secure_filename(rami_file.filename)

        scan_path = os.path.join(app.config["UPLOAD_FOLDER"], scan_name)
        rami_path = os.path.join(app.config["UPLOAD_FOLDER"], rami_name)

        scan_file.save(scan_path)
        rami_file.save(rami_path)

        try:
            # Note: Tax Gap logic is not yet refactored to use file_splitter chunks.
            # It still processes the scan file directly.
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
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
