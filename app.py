# app.py

import os
from datetime import datetime
from functools import wraps

from flask import (
    Flask,
    request,
    redirect,
    url_for,
    render_template_string,
    send_file,
    flash,
    session,
)

import psycopg2
from psycopg2.extras import RealDictCursor

from core.file_loader import load_scan_file, load_ram_file
from core.yzer_preparation import prepare_yzer_file, export_yzer
from core.duplicates_checker import find_duplicate_summary, find_duplicate_rows
from core.gap_checker import find_missing_transactions


# ----------------------------------------------------------------------
# Basic Flask configuration
# ----------------------------------------------------------------------
app = Flask(__name__)

# Secret key for sessions and flash messages
app.secret_key = os.getenv("FLASK_SECRET_KEY", "realestate-app-secret-key")

CURRENT_YEAR = datetime.now().year

# Access code – shared between all users
ACCESS_CODE = os.getenv("ACCESS_CODE", "12345")

# Admin token for approval links (for n8n or manual use)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "change-this-admin-token")

# Optional: hard-coded whitelist used only when DATABASE_URL is missing
ALLOWED_EMAILS = [
    "ariel.portnik@gmail.com",
    "arielpo@yad2.co.il"]

# Database connection string from Render
DATABASE_URL = os.getenv("DATABASE_URL")


# ----------------------------------------------------------------------
# Database helpers
# ----------------------------------------------------------------------
def get_db_connection():
    """
    Open a new connection to the PostgreSQL database using DATABASE_URL.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    conn = psycopg2.connect(DATABASE_URL)
    return conn


def init_db():
    """
    Create the 'users' table if it does not exist.
    Used for login approvals (pending/approved/blocked).
    """
    if not DATABASE_URL:
        # When running locally without DB, skip silently
        print("DATABASE_URL not set, skipping init_db()")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    conn.commit()
    cur.close()
    conn.close()
    print("users table is ready (init_db)")


def get_user_by_email(email: str):
    """
    Return a user row as dict or None.
    """
    if not DATABASE_URL:
        return None

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE email = %s;", (email,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def create_user(email: str, status: str = "pending"):
    """
    Insert a new user with given status.
    """
    if not DATABASE_URL:
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users (email, status) VALUES (%s, %s) "
        "ON CONFLICT (email) DO NOTHING;",
        (email, status),
    )
    conn.commit()
    cur.close()
    conn.close()


def update_user_status(email: str, status: str):
    """
    Update user status (pending / approved / blocked).
    """
    if not DATABASE_URL:
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET status = %s WHERE email = %s;", (status, email))
    conn.commit()
    cur.close()
    conn.close()


# ----------------------------------------------------------------------
# Authentication helpers
# ----------------------------------------------------------------------
def login_required(view_func):
    """
    Decorator to enforce login for protected routes.
    """
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if "user_email" not in session:
            # Redirect to login and keep the original target
            next_url = request.path
            return redirect(url_for("login", next=next_url))
        return view_func(*args, **kwargs)

    return wrapped


def render_base_page(title: str, body_html: str):
    """
    Helper to render a simple Bootstrap layout with consistent header.
    """
    user_email = session.get("user_email")
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
            <span class="navbar-text me-3">
              {% if user_email %}
                Logged in as <strong>{{ user_email }}</strong>
              {% else %}
                Not logged in
              {% endif %}
            </span>
            {% if user_email %}
              <a class="btn btn-outline-secondary btn-sm" href="{{ url_for('logout') }}">Logout</a>
            {% else %}
              <a class="btn btn-primary btn-sm" href="{{ url_for('login') }}">Login</a>
            {% endif %}
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
        user_email=user_email,
        body_html=body_html,
        year=year,
    )



# ----------------------------------------------------------------------
# Authentication routes
# ----------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    """
    Login form:
    - User enters email + access code.
    - If DATABASE_URL exists:
        - New email -> create user with status 'pending' and show message.
        - Existing email:
            - 'approved' -> login.
            - 'pending'  -> inform still pending.
            - 'blocked'  -> deny.
    - If DATABASE_URL is missing:
        - Fall back to ALLOWED_EMAILS list.
    """
    next_url = request.args.get("next") or url_for("home")

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        code = (request.form.get("access_code") or "").strip()

        if not email or not code:
            flash("Please provide both email and access code.")
            return redirect(url_for("login", next=next_url))

        if code != ACCESS_CODE:
            flash("Access code is incorrect.")
            return redirect(url_for("login", next=next_url))

        # Path 1: database-based approval
        if DATABASE_URL:
            user = get_user_by_email(email)

            if user is None:
                # First time: create as pending and do not log in yet
                create_user(email, status="pending")
                flash(
                    "Your request was received. Your email is now pending approval. "
                    "You will be able to log in after it is approved."
                )
                return redirect(url_for("login", next=next_url))

            status = user["status"]

            if status == "blocked":
                flash("Your access is blocked. Please contact the administrator.")
                return redirect(url_for("login", next=next_url))

            if status == "pending":
                flash("Your email is still pending approval. Please try again later.")
                return redirect(url_for("login", next=next_url))

            if status == "approved":
                session["user_email"] = email
                flash("Login successful.")
                return redirect(next_url)

            # Safety fallback
            flash("Unknown user status. Please contact the administrator.")
            return redirect(url_for("login", next=next_url))

        # Path 2: no database – fallback to hard-coded list
        if email not in ALLOWED_EMAILS:
            flash("Your email is not in the approved list.")
            return redirect(url_for("login", next=next_url))

        session["user_email"] = email
        flash("Login successful.")
        return redirect(next_url)

    # GET request – show login form
    body = """
    <div class="row justify-content-center">
      <div class="col-md-6 col-lg-4">
        <h1 class="h4 mb-3">Login</h1>
        <p class="text-muted">
          Enter your work email and the shared access code.<br>
          New emails will be marked as <strong>pending</strong> until approved.
        </p>
        <form method="post">
          <div class="mb-3">
            <label for="email" class="form-label">Email address</label>
            <input type="email" class="form-control" id="email" name="email"
                   placeholder="name@example.com" required>
          </div>
          <div class="mb-3">
            <label for="access_code" class="form-label">Access code</label>
            <input type="password" class="form-control" id="access_code"
                   name="access_code" required>
          </div>
          <button type="submit" class="btn btn-primary w-100">Login</button>
        </form>
      </div>
    </div>
    """
    return render_base_page("Login", body)


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.")
    return redirect(url_for("home"))


# ----------------------------------------------------------------------
# Admin endpoints for approval (for future n8n integration)
# ----------------------------------------------------------------------
@app.route("/admin/approve")
def admin_approve():
    """
    Approve a user by email.
    Intended to be called from an email link or n8n webhook.

    Example URL:
    /admin/approve?token=ADMIN_TOKEN&email=user@example.com
    """
    token = request.args.get("token")
    email = (request.args.get("email") or "").strip().lower()

    if token != ADMIN_TOKEN or not email:
        return "Unauthorized", 403

    update_user_status(email, "approved")
    return f"User {email} approved."


@app.route("/admin/block")
def admin_block():
    """
    Block a user by email.
    Example:
    /admin/block?token=ADMIN_TOKEN&email=user@example.com
    """
    token = request.args.get("token")
    email = (request.args.get("email") or "").strip().lower()

    if token != ADMIN_TOKEN or not email:
        return "Unauthorized", 403

    update_user_status(email, "blocked")
    return f"User {email} blocked."


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
@login_required
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
@login_required
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
      <li>Use the latest <code>scan_date</code> in the file.</li>
      <li>Filter to <code>sold_part = 1</code>.</li>
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
@login_required
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
# Initialize database schema on startup (only if DATABASE_URL is set)
init_db()

if __name__ == "__main__":
    # Local development server
    app.run(debug=True, host="0.0.0.0", port=5000)
