from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, Response
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import os, time, hashlib, json
from datetime import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

import io, csv, re


# =======================
# KONFIGURASI
# =======================
RAW_SPREADSHEET_ID  = "1Wclnp-YYaY838Ef38P170Xg2Q4Y-ZWycqzyxNi7cU1Q"
DASH_SPREADSHEET_ID = "12x5yfeGiBKH29ZuAXLV8aFXN02Xgqoh9sfQ42myPuLk"
LOG_SPREADSHEET_ID  = "16N5IOx-dRt0KvwTkptWmiMPgAdlbmWibKkDjm10XG_s"
CREDENTIALS_FILE    = "dashboard-monitoring-468708-022bf9f1140e.json"


SUPPORTED_TYPES = ["whatsapp_call", "whatsapp_messaging", "ping", "browsing", "video", "speed_testing", "4g_param"]
ALL_OPTION = "ALL_SHEETS"

ALIAS_MAP = {
    "whatsapp_call":      {"whatsapp_call", "whatsapp call", "WhatsApp_Call", "WhatsApp Call"},
    "whatsapp_messaging": {"whatsapp_messaging", "whatsapp messaging", "WhatsApp_Messaging", "WhatsApp Messaging"},
    "ping":               {"ping", "Ping"},
    "browsing":           {"browsing", "Browsing"},
    "video":              {"video", "Video"},
    "speed_testing":      {"speed_testing", "speed testing", "Speed_Testing", "Speed Testing"},
    "4g_param":           {"4g param", "4g_param", "4G Param", "4G_Param"},
}

def normalize_sheet_name_for_type(name: str) -> str | None:
    raw = name.strip()
    if raw.lower().startswith(("raw_", "dash_")):
        raw = raw.split("_", 1)[1]
    key = raw.lower().replace(" ", "_")
    for jenis, aliases in ALIAS_MAP.items():
        if key in {a.lower().replace(" ", "_") for a in aliases}:
            return jenis
    return key if key in SUPPORTED_TYPES else None

REQUIRED_COLS = {
    "whatsapp_call":      ["Collection_Name", "Latitude", "Longitude", "Provider", "Status", "Call_Setup_Time", "Avg_MOS"],
    "whatsapp_messaging": ["Collection_Name", "Latitude", "Longitude", "Provider", "Status", "Duration", "Media_Type"],
    "ping":               ["Collection_Name", "Latitude", "Longitude", "Provider", "Status", "Avg_RTT", "Total_Pings"],
    "browsing":           ["Collection_Name", "Latitude", "Longitude", "Provider", "Status", "Throughput_(ResultsHTTPBrowserTest)", "Throughput_(vResults)"],
    "video":              ["Collection_Name", "Latitude", "Longitude", "Provider", "Status", "Video_Resolution", "TimeToFirstPicture", "VMOS"],
    "speed_testing":      ["Collection_Name", "Latitude", "Longitude", "Provider", "Status", "App_ServiceProvider", "DL_Throughput", "UL_Throughput", "Latency", "Packet_Loss"],
    "4g_param":           ["Collection_Name", "RSRP", "PCI_LTE", "Provider", "RSRQ", "SINR", "LatLong", "Value RSRP", "Value RSRQ", "Value SINR"],
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

if GOOGLE_CREDENTIALS_JSON:
    # Render: kredensial disimpan sebagai ENV (JSON utuh)
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=SCOPES)
    CRED_SOURCE = "env"
else:
    # Lokal: pakai file di disk (nama default/atau GOOGLE_CREDENTIALS_FILE/GOOGLE_APPLICATION_CREDENTIALS)
    CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", CREDENTIALS_FILE)
    if not os.path.isfile(CREDENTIALS_FILE):
        cred_env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_env_path and os.path.isfile(cred_env_path):
            CREDENTIALS_FILE = cred_env_path
        else:
            raise FileNotFoundError(f"Credentials JSON tidak ditemukan: {CREDENTIALS_FILE}")
    # <- PENTING: selalu set creds di luar if-not-file
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    CRED_SOURCE = "file"

gc = gspread.authorize(creds)


app = Flask(__name__)
# Ambil secret dari ENV di server, fallback dev untuk lokal
app.secret_key = os.getenv("FLASK_SECRET_KEY", "s1muly4d1-2025-s3cr3t-k3y")

# ===== LoginManager setup =====
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

class User(UserMixin):
    def __init__(self, id, name, password):
        self.id = id
        self.name = name
        self.password = password

    def get_id(self):
        return self.id

users = {"admin": User(id="admin", name="balmontangerang", password="password123")}

@login_manager.user_loader
def load_user(user_id):
    return users.get(user_id)

# ===== Google Sheet Helpers =====
def ws_name_raw(jenis):  return f"RAW_{jenis}"
def ws_name_dash(jenis): return f"DASH_{jenis}"

def open_or_create_ws(spreadsheet_id: str, title: str, rows=1000, cols=50):
    sh = gc.open_by_key(spreadsheet_id)
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        sh.add_worksheet(title=title, rows=rows, cols=cols)
        return sh.worksheet(title)

def ensure_header(ws, columns):
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(columns)

def df_to_rows(df: pd.DataFrame):
    return df.astype(object).where(pd.notnull(df), "").values.tolist()

def append_df(ws, df: pd.DataFrame, batch=500):
    ensure_header(ws, df.columns.tolist())
    rows = df_to_rows(df)
    for i in range(0, len(rows), batch):
        ws.append_rows(rows[i:i+batch], value_input_option="RAW")

def add_system_cols(df: pd.DataFrame, source_name: str):
    df = df.copy()
    df["_uploaded_at_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["_source_name"] = source_name
    df["_batch_id"] = hashlib.md5(str(time.time()).encode()).hexdigest()[:10]
    return df


    # ===== logging file upload =====
def log_upload_event(file_name: str, username: str):
    try:
        log_ws = open_or_create_ws(LOG_SPREADSHEET_ID, "UPLOAD_LOG")
        ensure_header(log_ws, ["Tanggal Upload", "Username", "Nama File"])
        log_ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            username,
            file_name
        ])
    except Exception as e:
        print(f"❌ Gagal mencatat log upload: {e}")


# ===== ROUTES =====
@app.route("/")
@login_required
def home():
    print("✅ Halaman HOME diakses oleh:", current_user.get_id())
    return render_template("home.html")

@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "GET":
        return render_template("upload.html")

    f = request.files.get("file")
    if not f or f.filename == "":
        flash("Pilih file Excel terlebih dahulu.")
        return redirect(url_for("upload"))

    fname = f.filename.lower()
    try:
        if not fname.endswith((".xlsx", ".xls")):
            flash("Hanya mendukung Excel (.xlsx/.xls).")
            return redirect(url_for("upload"))

        xls = pd.ExcelFile(f)
        processed_any = False
        for sheet_name in xls.sheet_names:
            jenis_norm = normalize_sheet_name_for_type(sheet_name)
            if not jenis_norm:
                continue
            df = xls.parse(sheet_name)
            must = REQUIRED_COLS.get(jenis_norm, [])
            if [c for c in must if c not in df.columns]:
                continue
            raw_ws  = open_or_create_ws(RAW_SPREADSHEET_ID,  ws_name_raw(jenis_norm))
            dash_ws = open_or_create_ws(DASH_SPREADSHEET_ID, ws_name_dash(jenis_norm))
            append_df(raw_ws, df)
            append_df(dash_ws, df)
            processed_any = True

        if not processed_any:
            flash("Terdapat data tidak valid. Silahkan cek kembali template Anda.")
        else:
            log_upload_event(f.filename, current_user.name)
            flash("✅ Upload sukses. Data sudah ditambahkan ke Database")

    except Exception as e:
        flash(f"Terjadi kesalahan: {e}")

    return redirect(url_for("upload"))

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")

@app.route("/tentang")
@login_required
def tentang():
    return render_template("tentang.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        user = users.get(username)
        if user and password == user.password:
            login_user(user)
            return redirect(url_for("home"))
        flash("Login gagal. Coba lagi.", "error")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    session.clear()  # <--- penting
    flash("Anda telah logout.")
    return redirect(url_for("login"))

@app.route("/health")
def health():
    try:
        gc.open_by_key(RAW_SPREADSHEET_ID).worksheets()
        gc.open_by_key(DASH_SPREADSHEET_ID).worksheets()
        return {
            "ok": True,
            "message": "Service account & spreadsheet akses OK",
            "cred_source": CRED_SOURCE  # "env" atau "file"
        }, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


# =======================
# DATA VIEWER (Google Sheets)
# =======================

# Pemetaan nama "tab" (pendek) -> "jenis" normalisasi untuk nama worksheet DASH_
TAB_TO_JENIS = {
    "wam": "whatsapp_messaging",
    "wac": "whatsapp_call",
    "ping": "ping",
    "browsing": "browsing",
    "video": "video",
    "speedtest": "speed_testing",
    "rsrp": "4g_param",
}

# Kolom-kolom default untuk urutan tampil (jika ada di sheet)
DEFAULT_COLUMNS_GS = {
    "whatsapp_messaging": ["Collection_Name","Provider","Status","Duration","Media_Type","Latitude","Longitude","_uploaded_at_utc","_source_name","_batch_id"],
    "whatsapp_call":      ["Collection_Name","Provider","Status","Call_Setup_Time","Avg_MOS","Latitude","Longitude","_uploaded_at_utc","_source_name","_batch_id"],
    "ping":               ["Collection_Name","Provider","Status","Avg_RTT","Total_Pings","Latitude","Longitude","_uploaded_at_utc","_source_name","_batch_id"],
    "browsing":           ["Collection_Name","Provider","Status","Throughput_(ResultsHTTPBrowserTest)","Throughput_(vResults)","Latitude","Longitude","_uploaded_at_utc","_source_name","_batch_id"],
    "video":              ["Collection_Name","Provider","Status","Video_Resolution","TimeToFirstPicture","VMOS","Latitude","Longitude","_uploaded_at_utc","_source_name","_batch_id"],
    "speed_testing":      ["Collection_Name","Provider","Status","App_ServiceProvider","DL_Throughput","UL_Throughput","Latency","Packet_Loss","Latitude","Longitude","_uploaded_at_utc","_source_name","_batch_id"],
    "4g_param":           ["Collection_Name","Provider","RSRP","RSRQ","SINR","PCI_LTE","LatLong","Value RSRP","Value RSRQ","Value SINR","_uploaded_at_utc","_source_name","_batch_id"],
}

def normalize_table_to_jenis(table_name: str) -> str:
    key = (table_name or "").strip().lower()
    # izinkan nilai langsung (browsing/ping/video/speed_testing/dll)
    if key in DEFAULT_COLUMNS_GS:
        return key
    return TAB_TO_JENIS.get(key, "browsing")

def get_dash_df(jenis: str):
    # Baca worksheet DASH_<jenis> dari spreadsheet "dash"
    try:
        sh = gc.open_by_key(DASH_SPREADSHEET_ID)
        ws = sh.worksheet(ws_name_dash(jenis))  # contoh: DASH_browsing
        values = ws.get_all_values()
        if not values:
            return [], []
        headers = values[0]
        rows = values[1:]
        # Bangun records
        recs = [dict(zip(headers, r + [""]*(len(headers)-len(r)))) for r in rows]
        return headers, recs
    except Exception as e:
        print("❌ Gagal membaca DASH sheet:", e)
        return [], []

def filter_records(records, q=None, wilayah=None, provider=None, kategori=None):
    # wilayah: jika ada kolom 'Wilayah' gunakan equality;
    # fallback: cari di Collection_Name kalau pengguna tetap kirimkan.
    def match(rec):
        # Filter provider (equality, jika kolom ada)
        if provider:
            if "Provider" in rec and str(rec["Provider"]).strip() != str(provider).strip():
                return False

        # Filter wilayah (equality pada 'Wilayah' jika ada; kalau tidak ada, tolerate contains pada 'Collection_Name')
        if wilayah:
            if "Wilayah" in rec:
                if str(rec["Wilayah"]).strip() != str(wilayah).strip():
                    return False
            elif "Collection_Name" in rec:
                if str(wilayah).strip().lower() not in str(rec["Collection_Name"]).lower():
                    return False

        # Filter kategori (DT/ST) – tidak ada kolom standar; diabaikan bila tidak ada
        if kategori:
            # jika Anda menambahkan kolom 'Test_Type' di templates, aktifkan ini:
            if "Test_Type" in rec and str(rec["Test_Type"]).strip() != str(kategori).strip():
                return False

        # Filter q (substring di kolom string apa pun)
        if q:
            ql = str(q).lower()
            found = False
            for k, v in rec.items():
                if v is None: 
                    continue
                s = str(v)
                # skip nilai yang terlihat seperti angka murni demi performa
                if s and any(c.isalpha() for c in s):
                    if ql in s.lower():
                        found = True
                        break
            if not found:
                return False

        return True

    return [r for r in records if match(r)]

def paginate(records, page, page_size):
    total_rows = len(records)
    total_pages = max(1, (total_rows + page_size - 1)//page_size)
    page = max(1, min(page, total_pages))
    start = (page-1)*page_size
    end = start + page_size
    return records[start:end], page, total_pages, total_rows

@app.route("/data")
@login_required
def page_data():
    # Halaman viewer (JS akan memanggil /api/data)
    return render_template("data.html")

@app.route("/api/data")
@login_required
def api_data():
    table = request.args.get("table", "browsing")
    jenis = normalize_table_to_jenis(table)
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 25))
    q = request.args.get("q", "")
    wilayah = request.args.get("wilayah", "")
    provider = request.args.get("provider", "")
    kategori = request.args.get("kategori", "")

    headers, recs = get_dash_df(jenis)
    if not headers:
        return jsonify({"columns": [], "rows": [], "page": 1, "total_pages": 1, "total_rows": 0})

    # Filter
    recs = filter_records(recs, q=q or None, wilayah=wilayah or None, provider=provider or None, kategori=kategori or None)

    # Reorder columns by DEFAULT_COLUMNS_GS if present
    pref = DEFAULT_COLUMNS_GS.get(jenis, [])
    columns = [c for c in pref if c in headers]
    for c in headers:
        if c not in columns:
            columns.append(c)

    # Pagination
    page_recs, page, total_pages, total_rows = paginate(recs, page, page_size)

    # Return
    # rows as list of dict but with only selected columns
    rows = [{k: (r.get(k, "") if r.get(k, "") is not None else "") for k in columns} for r in page_recs]
    return jsonify({"columns": columns, "rows": rows, "page": page, "total_pages": total_pages, "total_rows": total_rows})

@app.route("/api/data.csv")
@login_required
def api_data_csv():
    table = request.args.get("table", "browsing")
    jenis = normalize_table_to_jenis(table)
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 1000))
    q = request.args.get("q", "")
    wilayah = request.args.get("wilayah", "")
    provider = request.args.get("provider", "")
    kategori = request.args.get("kategori", "")

    headers, recs = get_dash_df(jenis)
    if not headers:
        return Response("", mimetype="text/csv")

    # Filter
    recs = filter_records(recs, q=q or None, wilayah=wilayah or None, provider=provider or None, kategori=kategori or None)

    # Reorder columns
    pref = DEFAULT_COLUMNS_GS.get(jenis, [])
    columns = [c for c in pref if c in headers]
    for c in headers:
        if c not in columns:
            columns.append(c)

    # Pagination slice for export (allow larger page_size)
    page_recs, page, total_pages, total_rows = paginate(recs, page, page_size)

    # Build CSV
    import io, csv
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for r in page_recs:
        writer.writerow({k: r.get(k, "") for k in columns})
    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8-sig"))

    return Response(
        csv_bytes.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{jenis}.csv"'}
    )

if __name__ == "__main__":

    app.run(debug=True)





