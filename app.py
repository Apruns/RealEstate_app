# app.py

import os
import tempfile
from datetime import datetime

from flask import (
    Flask,
    request,
    send_file,
    render_template_string,
    redirect,
    url_for,
    flash,
)

from core.file_loader import load_scan_file, load_rami_file
from core.yzer_preparation import prepare_for_yzer, export_yzer
from core.duplicates_checker import find_duplicates_summary, find_duplicate_rows
from core.gap_checker import find_missing_transactions


app = Flask(__name__)
app.secret_key = "realestate-app-secret-key"  # needed for flash messages
CURRENT_YEAR = datetime.now().year


# -------------------------------------------------
# Shared base styles (minimalist, professional UI)
# -------------------------------------------------
BASE_STYLE = """
    * {
        box-sizing: border-box;
    }
    body {
        margin: 0;
        padding: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
        background: #f3f4f6;
        color: #111827;
    }
    a {
        color: inherit;
    }
    .layout {
        min-height: 100vh;
        display: flex;
        flex-direction: column;
    }
    .topbar {
        background: #111827;
        color: #f9fafb;
        padding: 12px 24px;
        box-shadow: 0 1px 4px rgba(15, 23, 42, 0.35);
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .topbar-title {
        font-weight: 600;
        letter-spacing: 0.02em;
    }
    .topbar-nav a {
        margin-left: 18px;
        font-size: 0.95rem;
        color: #e5e7eb;
        text-decoration: none;
        opacity: 0.9;
    }
    .topbar-nav a:hover {
        opacity: 1;
        text-decoration: underline;
    }
    .topbar-nav a.active {
        font-weight: 600;
        color: #60a5fa;
    }
    .page {
        flex: 1;
        display: flex;
        align-items: flex-start;
        justify-content: center;
        padding: 32px 16px 24px;
    }
    .card {
        width: 100%;
        max-width: 820px;
        background: #ffffff;
        border-radius: 14px;
        padding: 24px 26px 26px;
        box-shadow: 0 14px 30px rgba(15, 23, 42, 0.10);
    }
    h1 {
        margin: 0 0 6px 0;
        font-size: 1.7rem;
        letter-spacing: 0.01em;
    }
    .subtitle {
        margin: 0 0 20px 0;
        font-size: 0.97rem;
        color: #6b7280;
    }
    .section-title {
        font-weight: 600;
        font-size: 0.95rem;
        margin-top: 6px;
        margin-bottom: 4px;
    }
    .hint {
        font-size: 0.9rem;
        color: #4b5563;
        margin-bottom: 18px;
        line-height: 1.45;
    }
    .hint ul {
        margin: 6px 0 0 18px;
        padding: 0;
    }
    .hint li {
        margin-bottom: 4px;
    }
    form {
        border-radius: 12px;
        padding: 18px 20px 16px;
        background: #f9fafb;
        border: 1px solid #e5e7eb;
    }
    label {
        font-size: 0.9rem;
        font-weight: 500;
        color: #374151;
    }
    input[type="file"] {
        width: 100%;
        margin-top: 6px;
        margin-bottom: 14px;
        font-size: 0.9rem;
    }
    .button-row {
        margin-top: 4px;
    }
    button {
        background: #2563eb;
        color: #ffffff;
        border: none;
        padding: 8px 18px;
        border-radius: 999px;
        cursor: pointer;
        font-size: 0.9rem;
        font-weight: 500;
        letter-spacing: 0.01em;
        box-shadow: 0 8px 16px rgba(37, 99, 235, 0.25);
        transition: background 0.15s ease, transform 0.08s ease,
                    box-shadow 0.15s ease;
    }
    button:hover {
        background: #1d4ed8;
        transform: translateY(-1px);
        box-shadow: 0 10px 20px rgba(37, 99, 235, 0.30);
    }
    button:active {
        background: #1d4ed8;
        transform: translateY(0);
        box-shadow: 0 6px 12px rgba(37, 99, 235, 0.22);
    }
    .flash {
        padding: 10px 12px;
        border-radius: 10px;
        margin-bottom: 16px;
        background-color: #fef2f2;
        color: #b91c1c;
        border: 1px solid #fecaca;
        font-size: 0.88rem;
    }
    .radio-group {
        margin-top: 4px;
        margin-bottom: 10px;
        font-size: 0.9rem;
    }
    .radio-group label {
        font-weight: 400;
        margin-right: 18px;
    }
    .muted {
        font-size: 0.85rem;
        color: #6b7280;
        margin-top: 10px;
    }
    .footer {
        padding: 10px 24px 14px;
        text-align: center;
        font-size: 0.8rem;
        color: #9ca3af;
        border-top: 1px solid #e5e7eb;
        background: #f9fafb;
    }
    @media (max-width: 640px) {
        .topbar {
            flex-direction: column;
            align-items: flex-start;
            padding: 10px 16px;
        }
        .topbar-nav {
            margin-top: 6px;
        }
        .topbar-nav a {
            margin-left: 0;
            margin-right: 14px;
        }
        .card {
            padding: 18px 16px 20px;
        }
        .footer {
            padding-left: 16px;
            padding-right: 16px;
        }
    }
"""


# ------------------------
# Inline HTML templates
# ------------------------

INDEX_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>RealEstate App</title>
    <style>
        {{ base_style|safe }}
    </style>
</head>
<body>
<div class="layout">
    <header class="topbar">
        <div class="topbar-title">RealEstate App</div>
        <nav class="topbar-nav">
            <a href="{{ url_for('prepare_yzer') }}">Prepare file for YZER</a>
            <a href="{{ url_for('duplicates') }}">Duplicates check</a>
            <a href="{{ url_for('gap_check') }}">Tax gap check</a>
        </nav>
    </header>

    <main class="page">
        <section class="card">
            <h1>RealEstate App</h1>
            <p class="subtitle">
                Data preparation and validation toolkit for real-estate scan files.
            </p>

            {% with messages = get_flashed_messages() %}
              {% if messages %}
                {% for msg in messages %}
                  <div class="flash">{{ msg }}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}

            <p class="section-title">What you can do here</p>
            <p class="hint">
                This app is designed to support a repeatable, high-quality workflow for
                real-estate transaction data:
            </p>
            <ul class="hint">
                <li><strong>Prepare file for YZER</strong> – clean and normalize a raw scan file so it is ready for upload.</li>
                <li><strong>Duplicates check</strong> – detect duplicate deals inside a single scan file using strict business keys.</li>
                <li><strong>Tax gap check</strong> – compare a scan file with a Tax Authority file to find missing transactions.</li>
            </ul>

            <p class="muted">
                Use the navigation in the header to start one of the flows.
            </p>
        </section>
    </main>

    <footer class="footer">
        © {{ current_year }} RealEstate App · Ariel Portnik. All rights reserved.
    </footer>
</div>
</body>
</html>
"""

PREPARE_YZER_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Prepare file for YZER - RealEstate App</title>
    <style>
        {{ base_style|safe }}
    </style>
</head>
<body>
<div class="layout">
    <header class="topbar">
        <div class="topbar-title">RealEstate App</div>
        <nav class="topbar-nav">
            <a href="{{ url_for('prepare_yzer') }}" class="active">Prepare file for YZER</a>
            <a href="{{ url_for('duplicates') }}">Duplicates check</a>
            <a href="{{ url_for('gap_check') }}">Tax gap check</a>
        </nav>
    </header>

    <main class="page">
        <section class="card">
            <h1>Prepare file for YZER</h1>
            <p class="subtitle">
                Clean and normalize a raw scan file so it is ready for upload to YZER.
            </p>

            {% with messages = get_flashed_messages() %}
              {% if messages %}
                {% for msg in messages %}
                  <div class="flash">{{ msg }}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}

            <div class="hint">
                <div class="section-title">What this step does</div>
                <ul>
                    <li>Force numeric formatting for price and room fields.</li>
                    <li>Parse and standardize dates to <code>DD/MM/YYYY</code>.</li>
                    <li>Replace problematic characters (commas, markers like <code>--</code>, empty values).</li>
                    <li>Output a cleaned CSV file ready for the YZER system.</li>
                </ul>
            </div>

            <form method="post" enctype="multipart/form-data">
                <label for="file">Scan file (Excel or CSV):</label>
                <input type="file" name="file" id="file" accept=".xlsx,.xls,.csv" required>

                <div class="button-row">
                    <button type="submit">Run YZER preparation</button>
                </div>
            </form>
        </section>
    </main>

    <footer class="footer">
        © {{ current_year }} RealEstate App · Ariel Portnik. All rights reserved.
    </footer>
</div>
</body>
</html>
"""

DUPLICATES_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Duplicates check - RealEstate App</title>
    <style>
        {{ base_style|safe }}
    </style>
</head>
<body>
<div class="layout">
    <header class="topbar">
        <div class="topbar-title">RealEstate App</div>
        <nav class="topbar-nav">
            <a href="{{ url_for('prepare_yzer') }}">Prepare file for YZER</a>
            <a href="{{ url_for('duplicates') }}" class="active">Duplicates check</a>
            <a href="{{ url_for('gap_check') }}">Tax gap check</a>
        </nav>
    </header>

    <main class="page">
        <section class="card">
            <h1>Duplicates check</h1>
            <p class="subtitle">
                Identify duplicate deals inside a single scan file using strict business keys.
            </p>

            {% with messages = get_flashed_messages() %}
              {% if messages %}
                {% for msg in messages %}
                  <div class="flash">{{ msg }}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}

            <div class="hint">
                <div class="section-title">Logic</div>
                <ul>
                    <li>Automatically detect the latest <code>scan_date</code> in the file.</li>
                    <li>Filter to <code>sold_part = 1</code>.</li>
                    <li>Group by <code>block_lot, sale_day, declared_profit, sold_part, city, build_year, building_mr, rooms_number, scan_date</code>.</li>
                    <li>Return only keys that appear more than once (duplicate deals).</li>
                </ul>
            </div>

            <form method="post" enctype="multipart/form-data">
                <label for="file">Scan file (Excel or CSV):</label>
                <input type="file" name="file" id="file" accept=".xlsx,.xls,.csv" required>

                <div class="section-title" style="margin-top:4px;">Output type</div>
                <div class="radio-group">
                    <label>
                        <input type="radio" name="mode" value="summary" checked>
                        Summary – one row per duplicate group + count
                    </label>
                    <label>
                        <input type="radio" name="mode" value="rows">
                        Full rows – all duplicate records
                    </label>
                </div>

                <div class="button-row">
                    <button type="submit">Run duplicates check</button>
                </div>
            </form>
        </section>
    </main>

    <footer class="footer">
        © {{ current_year }} RealEstate App · Ariel Portnik. All rights reserved.
    </footer>
</div>
</body>
</html>
"""

GAP_CHECK_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Tax gap check - RealEstate App</title>
    <style>
        {{ base_style|safe }}
    </style>
</head>
<body>
<div class="layout">
    <header class="topbar">
        <div class="topbar-title">RealEstate App</div>
        <nav class="topbar-nav">
            <a href="{{ url_for('prepare_yzer') }}">Prepare file for YZER</a>
            <a href="{{ url_for('duplicates') }}">Duplicates check</a>
            <a href="{{ url_for('gap_check') }}" class="active">Tax gap check</a>
        </nav>
    </header>

    <main class="page">
        <section class="card">
            <h1>Tax gap check (Data Reconciliation)</h1>
            <p class="subtitle">
                Compare a scan file with a Tax Authority file and extract deals that exist in Tax but are missing from the scan.
            </p>

            {% with messages = get_flashed_messages() %}
              {% if messages %}
                {% for msg in messages %}
                  <div class="flash">{{ msg }}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}

            <div class="hint">
                <div class="section-title">How it works</div>
                <ul>
                    <li>Read the Tax Authority file and infer the effective query (date range from <code>sale_day</code> and location from <code>city</code> or goch).</li>
                    <li>Apply the same query to the scan file.</li>
                    <li>Compare both datasets on key fields (block_lot, sale_day, prices, rooms, etc.).</li>
                    <li>Export a CSV with the Tax deals that were not found in the filtered scan file.</li>
                </ul>
            </div>

            <form method="post" enctype="multipart/form-data">
                <label for="scan_file">Scan file (Excel or CSV):</label>
                <input type="file" name="scan_file" id="scan_file" accept=".xlsx,.xls,.csv" required>

                <label for="tax_file">Tax Authority file (Excel or HTML-style .xls/.html):</label>
                <input type="file" name="tax_file" id="tax_file" accept=".xlsx,.xls,.html,.htm" required>

                <div class="button-row">
                    <button type="submit">Run tax gap check</button>
                </div>
            </form>

            <p class="muted">
                Tip: use files for the same city and overlapping date ranges for clearer results.
            </p>
        </section>
    </main>

    <footer class="footer">
        © {{ current_year }} RealEstate App · Ariel Portnik
    </footer>
</div>
</body>
</html>
"""


# ------------------------
# Routes
# ------------------------

@app.route("/")
def index():
    return render_template_string(
        INDEX_TEMPLATE,
        base_style=BASE_STYLE,
        current_year=CURRENT_YEAR,
    )


@app.route("/prepare-yzer", methods=["GET", "POST"])
def prepare_yzer():
    if request.method == "GET":
        return render_template_string(
            PREPARE_YZER_TEMPLATE,
            base_style=BASE_STYLE,
            current_year=CURRENT_YEAR,
        )

    if "file" not in request.files:
        flash("No file part in the request.")
        return redirect(request.url)

    uploaded_file = request.files["file"]

    if uploaded_file.filename == "":
        flash("No file selected.")
        return redirect(request.url)

    try:
        suffix = os.path.splitext(uploaded_file.filename)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_input:
            uploaded_file.save(tmp_input.name)
            input_path = tmp_input.name

        df_raw = load_scan_file(input_path)
        df_clean = prepare_for_yzer(df_raw)

        base_name, _ = os.path.splitext(uploaded_file.filename)
        tmp_dir = tempfile.mkdtemp()
        output_filename = f"{base_name}_yzer_clean.csv"
        output_path = os.path.join(tmp_dir, output_filename)

        export_yzer(df_clean, output_path, as_csv=True)

        return send_file(
            output_path,
            as_attachment=True,
            download_name=output_filename,
            mimetype="text/csv",
        )

    except Exception as e:
        flash(f"Error during YZER preparation: {e}")
        return redirect(request.url)


@app.route("/duplicates", methods=["GET", "POST"])
def duplicates():
    if request.method == "GET":
        return render_template_string(
            DUPLICATES_TEMPLATE,
            base_style=BASE_STYLE,
            current_year=CURRENT_YEAR,
        )

    if "file" not in request.files:
        flash("No file part in the request.")
        return redirect(request.url)

    uploaded_file = request.files["file"]

    if uploaded_file.filename == "":
        flash("No file selected.")
        return redirect(request.url)

    mode = request.form.get("mode", "summary")

    try:
        suffix = os.path.splitext(uploaded_file.filename)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_input:
            uploaded_file.save(tmp_input.name)
            input_path = tmp_input.name

        df_raw = load_scan_file(input_path)

        if mode == "rows":
            df_result = find_duplicate_rows(df_raw)
            suffix_name = "duplicate_rows"
        else:
            df_result = find_duplicates_summary(df_raw)
            suffix_name = "duplicates_summary"

        if df_result.empty:
            flash("No duplicates found for the latest scan with sold_part = 1.")
            return redirect(request.url)

        base_name, _ = os.path.splitext(uploaded_file.filename)
        tmp_dir = tempfile.mkdtemp()
        output_filename = f"{base_name}_{suffix_name}.csv"
        output_path = os.path.join(tmp_dir, output_filename)

        df_result.to_csv(output_path, index=False, encoding="utf-8-sig")

        return send_file(
            output_path,
            as_attachment=True,
            download_name=output_filename,
            mimetype="text/csv",
        )

    except Exception as e:
        flash(f"Error during duplicates check: {e}")
        return redirect(request.url)


@app.route("/gap-check", methods=["GET", "POST"])
def gap_check():
    if request.method == "GET":
        return render_template_string(
            GAP_CHECK_TEMPLATE,
            base_style=BASE_STYLE,
            current_year=CURRENT_YEAR,
        )

    if "scan_file" not in request.files or "tax_file" not in request.files:
        flash("Both scan file and Tax file are required.")
        return redirect(request.url)

    scan_file = request.files["scan_file"]
    tax_file = request.files["tax_file"]

    if scan_file.filename == "" or tax_file.filename == "":
        flash("Both scan file and Tax file must be selected.")
        return redirect(request.url)

    try:
        scan_suffix = os.path.splitext(scan_file.filename)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=scan_suffix) as tmp_scan:
            scan_file.save(tmp_scan.name)
            scan_path = tmp_scan.name

        tax_suffix = os.path.splitext(tax_file.filename)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=tax_suffix) as tmp_tax:
            tax_file.save(tmp_tax.name)
            tax_path = tmp_tax.name

        df_scan = load_scan_file(scan_path)
        df_tax = load_rami_file(tax_path)

        missing_df, meta = find_missing_transactions(df_scan, df_tax, date_col="sale_day")

        if missing_df.empty:
            flash(
                "Tax gap check completed: no missing transactions were found "
                "for the inferred date range and location."
            )
            return redirect(request.url)

        tax_base_name, _ = os.path.splitext(tax_file.filename)
        tmp_dir = tempfile.mkdtemp()
        output_filename = f"{tax_base_name}_missing_from_scan.csv"
        output_path = os.path.join(tmp_dir, output_filename)

        missing_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return send_file(
            output_path,
            as_attachment=True,
            download_name=output_filename,
            mimetype="text/csv",
        )

    except Exception as e:
        flash(f"Error during tax gap check: {e}")
        return redirect(request.url)


# ------------------------
# Entry point
# ------------------------

if __name__ == "__main__":
    app.run(debug=True)
