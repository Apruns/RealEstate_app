# app.py – public version, no login

import os
from datetime import datetime

from flask import (
    Flask,
    request,
    redirect,
    url_for,
    render_template_string,
    send_file,
    flash,
)

from core.file_loader import load_scan_file, load_ram_file
from core.yzer_preparation import prepare_yzer_file, export_yzer
from core.duplicates_checker import find_duplicate_summary, find_duplicate_rows
from core.gap_checker import find_missing_transactions


# ----------------------------------------------------------------------
# Basic Flask configuration
# ----------------------------------------------------------------------
app = Flask(__name__)

# Secret key for flash messages
app.secret_key = os.getenv("FLASK_SECRET_KEY", "realestate-app-secret-key")

CURRENT_YEAR = datetime.now().year


def render_base_page(title: str, body_html: str):
    """
    Helper to render a simple Bootstrap layout with consistent header.
    """
    year = CURRENT_YEAR

    template = """
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>{{ title }} - RealEstate App</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
          rel="stylesheet"
        >
        <style>
          body {
            padding-top: 60px;
          }
          .navbar-brand {
            font-weight: 600;
          }
          footer {
            margin-top: 40px;
            padding: 20px 0;
            border-top: 1px solid #e5e5e5;
            color: #777;
          }
        </style>
    </head>
    <body>
      <nav class="navbar navbar-expand-lg navbar-light bg-light fixed-top border-bottom">
        <div class="container-fluid">
          <a class="navbar-brand" href="{{ url_for('home') }}">RealEstate App</a>
          <button class="navbar-toggler" type="button" data-bs-toggle="collapse"
                  data-bs-target="#navbarNav" aria-controls="navbarNav"
                  aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
          </button>
          <div class="collapse navbar-collapse" id="navbarNav">
            <ul class="navbar-nav me-auto mb-2 mb-lg-0">
              <li class="nav-item">
                <a class="nav-link" href="{{ url_for('prepare_yzer') }}">Prepare file for YZER</a>
              </li>
              <li class="nav-item">
                <a class="nav-link" href="{{ url_for('duplicates_check') }}">Duplicates check</a>
              </li>
              <li class="nav-item">
                <a class="nav-link" href="{{ url_for('gap_check') }}">Tax gap check</a>
              </li>
            </ul>
            <span class="navbar-text">
              Public access – no login required
            </span>
          </div>
        </div>
      </nav>

      <main class="container">
        {% with messages = get_flashed_messages() %}
          {% if messages %}
            <div class="mt-2">
              {% for msg in messages %}
                <div class="alert alert-info" role="alert">{{ msg }}</div>
              {% endfor %}
            </div>
          {% endif %}
        {% endwith %}

        {{ body_html|safe }}
      </main>

      <footer class="container text-center">
        <small>&copy; {{ year }} RealEstate App. All rights reserved.</small>
      </footer>

      <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """

    return render_template_string(
        template,
        title=title,
        body_html=body_html,
        year=year,
    )


# ----------------------------------------------------------------------
# Application routes
# ----------------------------------------------------------------------
@app.route("/")
def home():
    body = """
    <div class="row">
      <div class="col-lg-8">
        <h1 class="mb-3">RealEstate App</h1>
        <p class="lead">
          Data preparation and validation toolkit for real-estate scan files.
        </p>
        <p>
          Use the navigation links above to:
        </p>
        <ul>
          <li><strong>Prepare file for YZER</strong> – clean and format a scan file for upload.</li>
          <li><strong>Duplicates check</strong> – find duplicate transactions within a scan file.</li>
          <li><strong>Tax gap check</strong> – compare scan file with Tax Authority data
              and find missing transactions.</li>
        </ul>
      </div>
    </div>
    """
    return render_base_page("Home", body)


# ------------- Prepare file for YZER -----------------------------------
@app.route("/prepare-yzer", methods=["GET", "POST"])
def prepare_yzer():
    if request.method == "POST":
        file = request.files.get("scan_file")
        if not file:
            flash("Please upload a scan file.")
            return redirect(url_for("prepare_yzer"))

        try:
            df = load_scan_file(file)
            df_clean = prepare_yzer_file(df)
            output_path = export_yzer(df_clean, original_name=file.filename)
            return send_file(
                output_path,
                as_attachment=True,
                download_name=os.path.basename(output_path),
            )
        except Exception as exc:
            flash(f"Error during YZER preparation: {exc}")
            return redirect(url_for("prepare_yzer"))

    body = """
    <h1 class="h4 mb-3">Prepare file for YZER</h1>
    <p class="text-muted">
      Upload a raw scan file (Excel or CSV). The app will clean numeric and date fields,
      apply global cleanup and return a file ready for upload to YZER.
    </p>
    <form method="post" enctype="multipart/form-data" class="mt-3">
      <div class="mb-3">
        <label for="scan_file" class="form-label">Scan file (Excel or CSV)</label>
        <input class="form-control" type="file" id="scan_file" name="scan_file"
               accept=".xlsx,.xls,.csv" required>
      </div>
      <button type="submit" class="btn btn-primary">Run YZER preparation</button>
    </form>
    """
    return render_base_page("Prepare file for YZER", body)


# ------------- Duplicates check ---------------------------------------
@app.route("/duplicates", methods=["GET", "POST"])
def duplicates_check():
    if request.method == "POST":
        file = request.files.get("scan_file")
        output_type = request.form.get("output_type", "summary")

        if not file:
            flash("Please upload a scan file.")
            return redirect(url_for("duplicates_check"))

        try:
            df = load_scan_file(file)
            if output_type == "full":
                duplicates_df = find_duplicate_rows(df)
                output_path = os.path.join(
                    os.getcwd(), "duplicates_full_output.csv"
                )
                duplicates_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            else:
                summary_df = find_duplicate_summary(df)
                output_path = os.path.join(
                    os.getcwd(), "duplicates_summary_output.csv"
                )
                summary_df.to_csv(output_path, index=False, encoding="utf-8-sig")

            return send_file(
                output_path,
                as_attachment=True,
                download_name=os.path.basename(output_path),
            )
        except Exception as exc:
            flash(f"Error during duplicates check: {exc}")
            return redirect(url_for("duplicates_check"))

    body = """
    <h1 class="h4 mb-3">Duplicates check</h1>
    <p class="text-muted">
      Upload a scan file (Excel or CSV). The app will:
    </p>
    <ul>
      <li>Use the latest <code>scan_date</code> in the file (if exists).</li>
      <li>Filter to <code>sold_part = 1</code> (if exists).</li>
      <li>Group by: <code>block_lot, sale_day, declared_profit, sold_part, city,
          build_year, building_mr, rooms_number, scan_date</code>.</li>
      <li>Return only combinations that appear more than once.</li>
    </ul>
    <form method="post" enctype="multipart/form-data" class="mt-3">
      <div class="mb-3">
        <label for="scan_file" class="form-label">Scan file (Excel or CSV)</label>
        <input class="form-control" type="file" id="scan_file" name="scan_file"
               accept=".xlsx,.xls,.csv" required>
      </div>

      <div class="mb-3">
        <label class="form-label d-block">Output type:</label>
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="output_type"
                 id="output_summary" value="summary" checked>
          <label class="form-check-label" for="output_summary">
            Summary (one row per duplicate group + count)
          </label>
        </div>
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="output_type"
                 id="output_full" value="full">
          <label class="form-check-label" for="output_full">
            Full rows (all duplicate records)
          </label>
        </div>
      </div>

      <button type="submit" class="btn btn-primary">Run duplicates check</button>
    </form>
    """
    return render_base_page("Duplicates check", body)


# ------------- Tax gap check (data reconciliation) --------------------
@app.route("/gap-check", methods=["GET", "POST"])
def gap_check():
    if request.method == "POST":
        scan_file = request.files.get("scan_file")
        tax_file = request.files.get("tax_file")

        if not scan_file or not tax_file:
            flash("Please upload both the scan file and the Tax Authority file.")
            return redirect(url_for("gap_check"))

        try:
            scan_df = load_scan_file(scan_file)
            tax_df = load_ram_file(tax_file)

            missing_df, output_path = find_missing_transactions(
                scan_df, tax_df, original_tax_name=tax_file.filename
            )

            return send_file(
                output_path,
                as_attachment=True,
                download_name=os.path.basename(output_path),
            )
        except Exception as exc:
            flash(f"Error during tax gap check: {exc}")
            return redirect(url_for("gap_check"))

    body = """
    <h1 class="h4 mb-3">Tax gap check (Data Reconciliation)</h1>
    <p class="text-muted">
      Upload:
    </p>
    <ul>
      <li>A <strong>Scan file</strong> (internal data, Excel or CSV).</li>
      <li>A <strong>Tax Authority file</strong> (Excel or HTML-style <code>.xls</code>).</li>
    </ul>
    <p class="text-muted mb-3">
      The app will:
    </p>
    <ol>
      <li>Infer the date range and location from the Tax file (<code>sale_day</code> + city/goch).</li>
      <li>Apply the same filters to the Scan file.</li>
      <li>Find transactions that exist in the Tax file but are missing from the Scan file.</li>
      <li>Export a file containing only these missing transactions.</li>
    </ol>

    <form method="post" enctype="multipart/form-data" class="mt-3">
      <div class="mb-3">
        <label for="scan_file" class="form-label">Scan file (Excel or CSV)</label>
        <input class="form-control" type="file" id="scan_file" name="scan_file"
               accept=".xlsx,.xls,.csv" required>
      </div>
      <div class="mb-3">
        <label for="tax_file" class="form-label">
          Tax Authority file (Excel or HTML-style .xls/.html)
        </label>
        <input class="form-control" type="file" id="tax_file" name="tax_file"
               accept=".xlsx,.xls,.html" required>
      </div>
      <button type="submit" class="btn btn-primary">Run tax gap check</button>
    </form>
    """
    return render_base_page("Tax gap check", body)


# ----------------------------------------------------------------------
# App startup
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Local development server
    app.run(debug=True, host="0.0.0.0", port=5000)
