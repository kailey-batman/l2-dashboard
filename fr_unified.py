import streamlit as st
import pandas as pd
import anthropic
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import re
import base64
import threading
import urllib.parse
import secrets
import requests as _http
from datetime import datetime, timedelta
from collections import defaultdict
import hashlib
import hmac as _hmac
import io
import extra_streamlit_components as stx

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Feature Request Dashboard",
    page_icon="logo.svg",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .stApp { background-color: #2D333B; }

    .header-container {
        display: flex; align-items: center; gap: 16px; padding: 0.5rem 0 0.5rem 0;
    }
    .header-container img { width: 48px; height: 48px; }
    .header-container h1 { color: #00E676; margin: 0; font-size: 2rem; }
    .header-subtitle { color: #9E9E9E; font-size: 0.95rem; margin-top: -4px; padding-bottom: 1rem; }

    [data-testid="stMetric"] {
        background-color: #373E47; border: 1px solid #444C56; border-radius: 10px; padding: 16px;
    }
    [data-testid="stMetricLabel"] { color: #9E9E9E !important; }
    [data-testid="stMetricValue"] { color: #E0E0E0 !important; }
    [data-testid="stMetricDelta"] { color: #00E676 !important; }

    [data-testid="stSidebar"] { background-color: #333A44; border-right: 1px solid #444C56; }
    [data-testid="stSidebar"] .stMarkdown h2 { color: #00E676; }

    .stTabs [data-baseweb="tab"] { color: #9E9E9E; }
    .stTabs [aria-selected="true"] { color: #00E676 !important; border-bottom-color: #00E676 !important; }

    .stButton > button[kind="primary"] {
        background-color: #00E676; color: #2D333B; border: none; font-weight: 600;
    }
    .stButton > button[kind="primary"]:hover { background-color: #00C853; color: #2D333B; }

    .stDownloadButton > button {
        background-color: #373E47; color: #00E676; border: 1px solid #00E676;
    }
    .stDownloadButton > button:hover { background-color: #00E676; color: #2D333B; }

    .streamlit-expanderHeader { color: #E0E0E0; background-color: #373E47; }
    hr { border-color: #444C56; }
    [data-baseweb="select"] { background-color: #373E47; }
    .stDataFrame { border: 1px solid #444C56; border-radius: 8px; overflow-x: auto !important; }

    .progress-banner {
        background-color: #1A2F1A; border: 1px solid #00E676; border-radius: 8px;
        padding: 12px 20px; margin-bottom: 16px;
    }
    .progress-banner .progress-text { color: #00E676; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# CONFIGURATION
# ============================================================

_APP_DIR = os.path.dirname(os.path.abspath(__file__))

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "10Fz0O-aSGYsKpi51vdi9p7dk2gj4UC_f254LmYgsC5M")
MAIN_TAB = os.environ.get("MAIN_TAB_NAME", "Stories")

COLUMNS = {
    "id":           "id",
    "timestamp":    "created_at",
    "title":        "name",
    "description":  "description",
    "type":         "type",
    "product_area": "product_area",
    "submitter":    "requester",
    "owners":       "owners",
    "company":      "",
    "priority":     "priority",
    "severity":     "severity",
    "status":       "state",
    "labels":       "labels",
    "epic":         "epic",
    "team":         "team",
    "use_case":     "",
    "impact":       "",
    "link":         "app_url",
}

FEATURE_REQUEST_TYPE = "feature"
INTERNAL_DOMAIN = "fieldguide.io"
CUSTOMER_KEYWORDS = [
    "customer", "client", "account", "partner", "user", "they ", "their team",
    "company", "org ", "organization", "enterprise", "prospect", "vendor",
]
ANALYSIS_BATCH_SIZE = 20
CHATBOT_MAX_TICKETS = None  # No limit — include all tickets
CONTACTS_FILE = os.path.join(_APP_DIR, "fr_contacts.json")
CONTACTS_PROGRESS_FILE = os.path.join(_APP_DIR, "fr_contacts_progress.json")
SUMMARIES_FILE = os.path.join(_APP_DIR, "fr_summaries.json")
SUMMARIES_PROGRESS_FILE = os.path.join(_APP_DIR, "fr_summaries_progress.json")
SUMMARY_BATCH_SIZE = 20

_contacts_lock = threading.Lock()
_summaries_lock = threading.Lock()

# ============================================================
# GOOGLE SHEETS AUTH
# ============================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


@st.cache_resource
def get_gsheet_client():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
        return gspread.authorize(creds)
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=SCOPES
        )
        return gspread.authorize(creds)
    except Exception:
        pass
    sa_path = os.path.join(_APP_DIR, "service_account.json")
    if os.path.exists(sa_path):
        creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
        return gspread.authorize(creds)
    return None


# ============================================================
# GOOGLE OAUTH
# ============================================================

_ALLOWED_DOMAIN = "fieldguide.io"
_GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
_OAUTH_SCOPES = "openid email profile"
_ACCESS_LOG_TAB = "Access Log"
_ACCESS_LOG_FILE = os.path.join(_APP_DIR, "access_log.json")
_AUTH_COOKIE = "fg_auth"
_COOKIE_TTL_HOURS = 24


def _cookie_mgr():
    return stx.CookieManager(key="_fg_cookie_mgr")


def _encode_auth(user):
    secret = os.environ.get("COOKIE_SECRET", os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "fg-dashboard"))
    exp = (datetime.utcnow() + timedelta(hours=_COOKIE_TTL_HOURS)).isoformat()
    payload = json.dumps({"u": user, "e": exp}, separators=(",", ":"))
    sig = _hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return base64.b64encode(f"{payload}.{sig}".encode()).decode()


def _decode_auth(value):
    try:
        secret = os.environ.get("COOKIE_SECRET", os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "fg-dashboard"))
        decoded = base64.b64decode(value.encode()).decode()
        payload_str, sig = decoded.rsplit(".", 1)
        expected = _hmac.new(secret.encode(), payload_str.encode(), hashlib.sha256).hexdigest()
        if not _hmac.compare_digest(expected, sig):
            return None
        data = json.loads(payload_str)
        if datetime.fromisoformat(data["e"]) < datetime.utcnow():
            return None
        return data["u"]
    except Exception:
        return None


def _get_oauth_creds():
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
    redirect_uri = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "http://localhost:8501")
    return client_id, client_secret, redirect_uri


def _build_auth_url():
    client_id, _, redirect_uri = _get_oauth_creds()
    state = secrets.token_urlsafe(32)
    st.session_state["_oauth_state"] = state
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": _OAUTH_SCOPES,
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }
    return f"{_GOOGLE_AUTH_URL}?{urllib.parse.urlencode(params)}"


def _exchange_code(code, state):
    client_id, client_secret, redirect_uri = _get_oauth_creds()
    try:
        resp = _http.post(_GOOGLE_TOKEN_URL, data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }, timeout=10)
    except Exception as e:
        return None, f"Token exchange error: {e}"
    if not resp.ok:
        return None, "Token exchange failed. Please try again."
    access_token = resp.json().get("access_token")
    if not access_token:
        return None, "No access token received."
    try:
        ui_resp = _http.get(_GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"}, timeout=10)
    except Exception as e:
        return None, f"User info error: {e}"
    if not ui_resp.ok:
        return None, "Failed to retrieve user info."
    info = ui_resp.json()
    email = info.get("email", "")
    if not email.lower().endswith(f"@{_ALLOWED_DOMAIN}"):
        return None, f"Access denied. Only @{_ALLOWED_DOMAIN} accounts are permitted."
    return {"email": email, "name": info.get("name", email), "picture": info.get("picture", "")}, None


def _show_login_page():
    client_id, _, _ = _get_oauth_creds()
    if not client_id:
        # Dev mode: auto-login when OAuth not configured
        st.session_state["_auth_user"] = {"email": "dev@fieldguide.io", "name": "Dev Mode", "picture": ""}
        st.rerun()
        return
    auth_url = _build_auth_url()
    logo_html = ""
    logo_path = os.path.join(_APP_DIR, "logo.svg")
    if os.path.exists(logo_path):
        with open(logo_path, "r") as f:
            logo_svg = f.read()
        logo_b64 = base64.b64encode(logo_svg.encode()).decode()
        logo_html = f'<img src="data:image/svg+xml;base64,{logo_b64}" style="width:60px;height:60px;margin-bottom:8px;" />'
    google_logo_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48" width="20" height="20">'
        '<path fill="#4285F4" d="M44.5 20H24v8.5h11.8C34.7 33.9 29.1 37 24 37c-7.2 0-13-5.8-13-13s5.8-13 13-13c3.1 0 5.9 1.1 8.1 2.9l6.4-6.4C34.6 4.1 29.6 2 24 2 11.8 2 2 11.8 2 24s9.8 22 22 22c11 0 21-8 21-22 0-1.3-.2-2.7-.5-4z"/>'
        '<path fill="#34A853" d="M6.3 14.7l7 5.1C15.1 16.2 19.2 13 24 13c3.1 0 5.9 1.1 8.1 2.9l6.4-6.4C34.6 4.1 29.6 2 24 2 16.2 2 9.4 7.3 6.3 14.7z"/>'
        '<path fill="#FBBC05" d="M24 46c5.5 0 10.5-1.8 14.4-4.9l-6.7-5.5C29.7 37.5 27 38.5 24 38.5c-5.1 0-9.4-3.2-11.1-7.7l-7 5.4C9.2 42.3 16.1 46 24 46z"/>'
        '<path fill="#EA4335" d="M44.5 20H24v8.5h11.8c-1 3-3.2 5.5-6.1 7.1l6.7 5.5C41.1 37.3 45 31.1 45 24c0-1.3-.2-2.7-.5-4z"/>'
        '</svg>'
    )
    st.markdown(f"""
    <style>
        .stApp {{ background-color: #2D333B; }}
        .login-wrapper {{ display:flex;justify-content:center;align-items:center;min-height:80vh;padding:2rem; }}
        .login-card {{
            background-color:#373E47;border:1px solid #444C56;border-radius:16px;
            padding:48px 40px;max-width:420px;width:100%;text-align:center;
            box-shadow:0 8px 32px rgba(0,0,0,0.4);
        }}
        .login-card h1 {{ color:#00E676;font-size:1.7rem;margin:12px 0 8px 0; }}
        .login-card .login-sub {{ color:#9E9E9E;font-size:0.95rem;margin-bottom:36px; }}
        .google-btn {{
            display:inline-flex;align-items:center;gap:12px;background-color:#ffffff;
            color:#3c4043;font-size:15px;font-weight:500;padding:12px 24px;border-radius:8px;
            text-decoration:none !important;border:1px solid #dadce0;
            transition:background-color 0.15s,box-shadow 0.15s;
        }}
        .google-btn:hover {{ background-color:#f8f9fa;box-shadow:0 2px 8px rgba(0,0,0,0.25);color:#3c4043 !important; }}
        .login-note {{ color:#616a75;font-size:0.75rem;margin-top:24px; }}
    </style>
    <div class="login-wrapper"><div class="login-card">
        {logo_html}
        <h1>Feature Request Dashboard</h1>
        <div class="login-sub">Sign in with your Fieldguide Google account to continue.</div>
        <a href="{auth_url}" class="google-btn">{google_logo_svg} Sign in with Google</a>
        <div class="login-note">Only @fieldguide.io accounts are permitted.</div>
    </div></div>
    """, unsafe_allow_html=True)


def _is_admin():
    email = st.session_state.get("_auth_user", {}).get("email", "").lower()
    raw = os.environ.get("DASHBOARD_ADMIN_EMAILS", "")
    admins = [e.strip().lower() for e in raw.split(",") if e.strip()]
    return bool(admins) and email in admins


def _log_visit(user_info):
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "email": user_info.get("email", ""),
        "name": user_info.get("name", ""),
    }
    try:
        existing = []
        if os.path.exists(_ACCESS_LOG_FILE):
            with open(_ACCESS_LOG_FILE, "r") as f:
                existing = json.load(f)
        existing.append(entry)
        with open(_ACCESS_LOG_FILE, "w") as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass
    try:
        client = get_gsheet_client()
        if client and SHEET_ID:
            ss = client.open_by_key(SHEET_ID)
            try:
                ws = ss.worksheet(_ACCESS_LOG_TAB)
            except Exception:
                ws = ss.add_worksheet(title=_ACCESS_LOG_TAB, rows=5000, cols=3)
                ws.append_row(["Timestamp", "Email", "Name"])
            ws.append_row([entry["timestamp"], entry["email"], entry["name"]])
    except Exception:
        pass


def _load_access_log():
    try:
        client = get_gsheet_client()
        if client and SHEET_ID:
            ss = client.open_by_key(SHEET_ID)
            ws = ss.worksheet(_ACCESS_LOG_TAB)
            rows = ws.get_all_records()
            if rows:
                return pd.DataFrame(rows)
    except Exception:
        pass
    if os.path.exists(_ACCESS_LOG_FILE):
        try:
            with open(_ACCESS_LOG_FILE, "r") as f:
                data = json.load(f)
            if data:
                return pd.DataFrame(data).rename(
                    columns={"timestamp": "Timestamp", "email": "Email", "name": "Name"}
                )
        except Exception:
            pass
    return pd.DataFrame(columns=["Timestamp", "Email", "Name"])


# ============================================================
# CUSTOM FIELDS PARSING
# ============================================================

def _parse_custom_fields_text(cf_raw: str) -> dict:
    cf_raw = cf_raw.strip()
    if not cf_raw or cf_raw in ("nan", "[]", "{}"):
        return {}
    if cf_raw.startswith("[") or cf_raw.startswith("{"):
        try:
            cf = json.loads(cf_raw)
            result = {}
            items = cf if isinstance(cf, list) else [cf]
            for item in items:
                if isinstance(item, dict):
                    k = (item.get("name") or item.get("field_name") or "").lower().strip()
                    v = str(item.get("value") or item.get("value_name") or "").strip()
                    if k and v and v != "nan":
                        result[k] = v
            return result
        except (json.JSONDecodeError, TypeError):
            pass
    result = {}
    for part in cf_raw.replace(";", "\n").split("\n"):
        part = part.strip()
        if "=" not in part:
            continue
        key, _, val = part.partition("=")
        key = key.strip().lower()
        val = val.strip()
        if key and val and val != "nan":
            result[key] = val
    return result


def _fill_from_custom_fields(df: pd.DataFrame) -> pd.DataFrame:
    if "custom_fields" not in df.columns:
        return df
    keyword_to_col = [
        ("product area",   COLUMNS.get("product_area", "product_area")),
        ("product_area",   COLUMNS.get("product_area", "product_area")),
        ("priority",       COLUMNS.get("priority", "priority")),
        ("severity",       COLUMNS.get("severity", "severity")),
    ]
    for idx, row in df.iterrows():
        cf_raw = str(row.get("custom_fields", ""))
        parsed = _parse_custom_fields_text(cf_raw)
        if not parsed:
            continue
        for keyword, col in keyword_to_col:
            if col not in df.columns:
                continue
            current = str(df.at[idx, col]).strip()
            if current and current not in ("nan", "None", ""):
                continue
            for k, v in parsed.items():
                if keyword in k:
                    df.at[idx, col] = v
                    break
    return df


# ============================================================
# DATA LOAD
# ============================================================

@st.cache_data(ttl=300)
def load_feature_requests() -> pd.DataFrame:
    client = get_gsheet_client()
    if client is None:
        st.error("Google Sheets not configured. Add service_account.json or set GOOGLE_SERVICE_ACCOUNT_JSON.")
        return pd.DataFrame()
    try:
        sh = client.open_by_key(SHEET_ID)
        ws = sh.worksheet(MAIN_TAB)
        df = pd.DataFrame(ws.get_all_records())
        if df.empty:
            return df
        type_col = COLUMNS.get("type", "")
        if FEATURE_REQUEST_TYPE and type_col and type_col in df.columns:
            df = df[
                df[type_col].astype(str).str.strip().str.lower() == FEATURE_REQUEST_TYPE.lower()
            ]
        if "is_completed" in df.columns:
            df = df[df["is_completed"].astype(str).str.strip().str.lower() != "true"]
        ts_col = COLUMNS.get("timestamp", "")
        if ts_col and ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
            df = df[df[ts_col] >= "2025-01-01"]
        df = _fill_from_custom_fields(df)
        return df.reset_index(drop=True)
    except Exception as e:
        st.error(f"Error loading Google Sheet: {e}")
        return pd.DataFrame()


# ============================================================
# COMPANY TIERS
# ============================================================

_DEFAULT_COMPANY_TIERS = {
    "tiers": {
        "1": {"name": "Enterprise", "multiplier": 3.0},
        "2": {"name": "Strategic", "multiplier": 2.0},
        "3": {"name": "Standard", "multiplier": 1.0},
    },
    "companies": {
        "KPMG": {"tier": 1}, "EY": {"tier": 1}, "Deloitte": {"tier": 1},
        "PWC": {"tier": 1}, "PwC": {"tier": 1}, "Moss Adams": {"tier": 1},
        "CLA": {"tier": 1}, "CliftonLarsonAllen": {"tier": 1},
        "Baker Tilly": {"tier": 1}, "BDO": {"tier": 1}, "RSM": {"tier": 1},
        "RSM US LLP": {"tier": 1}, "Crowe": {"tier": 1},
        "Grant Thornton": {"tier": 1}, "Forvis": {"tier": 1}, "Forvis Mazars": {"tier": 1},
        "Schellman": {"tier": 2}, "Schneider Downs": {"tier": 2},
        "BerryDunn": {"tier": 2}, "Insight Assurance": {"tier": 2},
        "Cherry Bekaert": {"tier": 2}, "Armanino": {"tier": 2},
        "Wipfli": {"tier": 2}, "Eide Bailly": {"tier": 2},
        "Mazars": {"tier": 2}, "Marcum": {"tier": 2}, "Plante Moran": {"tier": 2},
        "CBIZ": {"tier": 2}, "CBIZ LLC": {"tier": 2},
        "Wolf & Co": {"tier": 2}, "Wolf & Company": {"tier": 2},
        "LBMC": {"tier": 2}, "HoganTaylor": {"tier": 2},
        "Weaver": {"tier": 2}, "UHY": {"tier": 2}, "Frazier & Deeter": {"tier": 2},
        "Kaufman Rossin": {"tier": 2}, "Elliott Davis": {"tier": 2}, "Sensiba": {"tier": 2},
        "SBS Cybersecurity": {"tier": 3}, "LerroSarbey": {"tier": 3},
        "Meditology Services": {"tier": 3}, "Johanson Group": {"tier": 3},
        "Clearwater Security": {"tier": 3}, "CompliancePoint": {"tier": 3},
        "HBK CPAs": {"tier": 3}, "Brightstar Lottery": {"tier": 3}, "PCI": {"tier": 3},
    }
}


@st.cache_data
def load_company_tiers() -> dict:
    path = os.path.join(_APP_DIR, "company_tiers.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return _DEFAULT_COMPANY_TIERS


def get_company_tier(company_name, tiers: dict) -> int:
    if not company_name:
        return 3
    companies = tiers.get("companies", {})
    for key, val in companies.items():
        if key.lower() in str(company_name).lower() or str(company_name).lower() in key.lower():
            return val.get("tier", 3) if isinstance(val, dict) else int(val)
    return 3


def get_tier_name(tier: int, tiers: dict) -> str:
    return tiers.get("tiers", {}).get(str(tier), {}).get("name", "Standard")


# ============================================================
# DESCRIPTION PARSING
# ============================================================

def parse_description(description: str) -> dict:
    result = {"company_name": None, "customer_urgency": None, "has_workaround": None}
    if not description:
        return result
    for pattern in [
        r"###\s*Company Name and ID\s*\n([^\n]+?)(?:\s*-\s*\d+)?(?:\n|$)",
        r"###?\s*Firm:?\s*\n\s*(.+?)(?:\n|$)",
        r"^\*\*Customer:?\*\*\s*(.+?)(?:\n|$)",
    ]:
        m = re.search(pattern, description, re.IGNORECASE | re.MULTILINE)
        if m:
            val = m.group(1).strip().rstrip(",|").replace("&amp;", "&").strip()
            if val and val.lower() not in ("n/a", "na", "none", ""):
                result["company_name"] = val
                break
    m = re.search(r"###\s*Customer Urgency\s*\n([^\n]+)", description, re.IGNORECASE)
    if m:
        result["customer_urgency"] = m.group(1).strip().lower()
    m = re.search(r"###\s*Is there a workaround\?[^\n]*\n([^\n]+)", description, re.IGNORECASE)
    if m:
        w = m.group(1).strip().lower()
        result["has_workaround"] = True if w.startswith("yes") else (False if w.startswith("no") else None)
    return result


# ============================================================
# PRIORITIZATION SCORING
# ============================================================

def score_ticket(row: pd.Series, company_tiers: dict, company_issue_counts: dict) -> tuple:
    """Returns (severity_label, points)."""
    points = 0
    desc = str(row.get(COLUMNS.get("description", "description"), "") or "")
    parsed = parse_description(desc)
    company_name = parsed.get("company_name")
    urgency = (parsed.get("customer_urgency") or "").lower()
    has_workaround = parsed.get("has_workaround")
    priority_field = str(row.get(COLUMNS.get("priority", "priority"), "") or "").lower()

    if any(w in urgency for w in ("urgent", "critical", "blocks")):
        points += 8
    elif any(w in urgency for w in ("high", "important")):
        points += 5

    if "critical" in priority_field or "urgent" in priority_field:
        points += 2
    elif "high" in priority_field:
        points += 1

    tier = get_company_tier(company_name, company_tiers)
    points += {1: 3, 2: 2, 3: 1}.get(tier, 1)

    if company_name:
        count = company_issue_counts.get(company_name.lower(), 0)
        if count >= 5:
            points += 2
        elif count >= 3:
            points += 1

    if has_workaround is False:
        points += 2

    type_col = COLUMNS.get("type", "type")
    if str(row.get(type_col, "")).lower() == "bug":
        points += 1

    severity = "P1" if points >= 10 else ("P2" if points >= 5 else "P3")
    return severity, points, company_name


def enrich_with_scores(df: pd.DataFrame, company_tiers: dict) -> pd.DataFrame:
    df = df.copy()
    desc_col = COLUMNS.get("description", "description")

    def _parse_company(desc):
        return parse_description(str(desc or "")).get("company_name") or ""

    df["company_name_parsed"] = df[desc_col].apply(_parse_company) if desc_col in df.columns else ""

    counts = defaultdict(int)
    for name in df["company_name_parsed"]:
        if name:
            counts[name.lower()] += 1

    severities, score_pts, tiers, tier_labels = [], [], [], []
    for _, row in df.iterrows():
        sev, pts, _ = score_ticket(row, company_tiers, counts)
        tier = get_company_tier(row.get("company_name_parsed"), company_tiers)
        severities.append(sev)
        score_pts.append(pts)
        tiers.append(tier)
        tier_labels.append(get_tier_name(tier, company_tiers))

    df["severity_label"] = severities
    df["score_pts"] = score_pts
    df["tier"] = tiers
    df["tier_label"] = tier_labels
    df = df.sort_values("score_pts", ascending=False).reset_index(drop=True)
    return df


# ============================================================
# THEME CLASSIFICATION
# ============================================================

THEMES = {
    "Critical Issues": {
        "keywords": ["broken", "error", "bug", "crash", "fail", "not working", "outage", "down",
                     "issue", "problem", "incorrect", "wrong", "missing", "lost"],
        "color": "#FF6B6B",
    },
    "AI / Agent": {
        "keywords": ["ai", "agent", "claude", "llm", "artificial intelligence", "machine learning",
                     "automation", "auto-fill", "auto fill", "smart", "suggest", "copilot"],
        "color": "#CE93D8",
    },
    "Sheets / Workpapers": {
        "keywords": ["sheet", "workpaper", "spreadsheet", "tab", "cell", "row", "column", "formula",
                     "excel", "grid", "table", "trbc", "rollforward"],
        "color": "#81C784",
    },
    "Files / Documents": {
        "keywords": ["file", "document", "pdf", "upload", "download", "attachment", "export",
                     "import", "folder", "archive", "version", "annotation"],
        "color": "#FFB74D",
    },
    "Requests / Confirmations": {
        "keywords": ["request", "confirmation", "approve", "sign", "signature", "status",
                     "workflow", "review", "submit", "send", "notify", "notification"],
        "color": "#4FC3F7",
    },
    "Reporting / Insights": {
        "keywords": ["report", "insight", "dashboard", "analytics", "metric", "chart", "graph",
                     "data", "export", "summary", "trend", "count"],
        "color": "#FFD54F",
    },
    "Access / Permissions": {
        "keywords": ["permission", "access", "role", "user", "admin", "login", "auth", "sso",
                     "invite", "member", "team", "guest", "security"],
        "color": "#FF8A65",
    },
    "Testing / QA": {
        "keywords": ["test", "testing", "qa", "quality", "audit", "compliance", "soc", "pentest",
                     "evidence", "control", "finding", "sample"],
        "color": "#A5D6A7",
    },
    "Integrations": {
        "keywords": ["integration", "api", "connect", "sync", "webhook", "zapier", "slack",
                     "microsoft", "salesforce", "third-party", "embed"],
        "color": "#80DEEA",
    },
    "Navigation / UX": {
        "keywords": ["navigation", "ux", "ui", "interface", "layout", "menu", "button", "click",
                     "search", "filter", "sort", "drag", "sidebar", "toolbar"],
        "color": "#B0BEC5",
    },
    "Performance": {
        "keywords": ["slow", "performance", "speed", "fast", "loading", "timeout", "lag", "latency",
                     "memory", "cache", "optimize"],
        "color": "#F48FB1",
    },
}


def classify_themes(text: str) -> list:
    text_lower = text.lower()
    matched = [theme for theme, cfg in THEMES.items() if any(kw in text_lower for kw in cfg["keywords"])]
    return matched if matched else ["Other"]


def enrich_with_themes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    title_col = COLUMNS.get("title", "name")
    desc_col = COLUMNS.get("description", "description")

    def _themes(row):
        text = " ".join([str(row.get(title_col, "") or ""), str(row.get(desc_col, "") or "")])
        return classify_themes(text)

    df["themes"] = df.apply(_themes, axis=1)
    df["themes_str"] = df["themes"].apply(lambda t: ", ".join(t))
    return df


# ============================================================
# ANTHROPIC CLIENT
# ============================================================

@st.cache_resource
def get_anthropic_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        try:
            api_key = st.secrets["ANTHROPIC_API_KEY"]
        except Exception:
            pass
    if not api_key:
        return None
    return anthropic.Anthropic(api_key=api_key)


# ============================================================
# CONTACT EXTRACTION
# ============================================================

def load_contacts() -> dict:
    if os.path.exists(CONTACTS_FILE):
        try:
            with open(CONTACTS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_contacts(contacts: dict):
    with _contacts_lock:
        with open(CONTACTS_FILE, "w") as f:
            json.dump(contacts, f)


def load_contacts_progress() -> dict:
    if os.path.exists(CONTACTS_PROGRESS_FILE):
        try:
            with open(CONTACTS_PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"done": 0, "total": 0, "running": False}


def _is_internal_heuristic(requester: str, description: str) -> bool:
    if INTERNAL_DOMAIN and INTERNAL_DOMAIN in requester.lower():
        if not any(kw in description.lower() for kw in CUSTOMER_KEYWORDS):
            return True
    return False


def _analyze_batch(ai: anthropic.Anthropic, batch: list) -> list:
    ticket_lines = []
    for t in batch:
        ticket_lines.append(
            f"[{t['id']}] Title: {t['title']}\n"
            f"Requester: {t['requester']}\n"
            f"Description: {t['description'][:600]}"
        )
    tickets_text = ("\n" + "=" * 60 + "\n").join(ticket_lines)
    prompt = f"""You are reviewing Shortcut feature request tickets. For each ticket determine:
1. Is it customer-driven? (A real customer/client/account requested it, even if filed internally on their behalf.)
2. Is there a named contact person to notify? (e.g. "Britni from Wipfli suggested this")

Tickets:

{tickets_text}

Respond with a JSON array — one object per ticket, in the same order:
[{{"id": "<id>", "is_customer_ticket": true/false, "name": "or null", "company": "or null", "role": "or null"}}]
No other text."""
    try:
        resp = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=ANALYSIS_BATCH_SIZE * 80,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = "\n".join(text.split("\n")[1:]).rstrip("`").strip()
        return json.loads(text)
    except Exception:
        return [{"id": t["id"], "is_customer_ticket": True, "name": None, "company": None, "role": None} for t in batch]


def _run_contact_extraction_thread(df: pd.DataFrame, ai: anthropic.Anthropic):
    contacts = load_contacts()
    id_col    = COLUMNS.get("id", "id")
    title_col = COLUMNS.get("title", "name")
    desc_col  = COLUMNS.get("description", "description")
    req_col   = COLUMNS.get("submitter", "requester")

    to_analyze = []
    for _, row in df.iterrows():
        ticket_id = str(row.get(id_col, ""))
        if ticket_id in contacts:
            continue
        description = str(row.get(desc_col, ""))
        requester = str(row.get(req_col, ""))
        if _is_internal_heuristic(requester, description):
            contacts[ticket_id] = {"is_customer_ticket": False, "name": None, "company": None, "role": None}
        else:
            to_analyze.append({
                "id": ticket_id,
                "title": str(row.get(title_col, "")),
                "description": description,
                "requester": requester,
            })

    save_contacts(contacts)
    total = len(to_analyze)

    for batch_start in range(0, total, ANALYSIS_BATCH_SIZE):
        batch = to_analyze[batch_start: batch_start + ANALYSIS_BATCH_SIZE]
        results = _analyze_batch(ai, batch)
        for r in results:
            contacts[str(r.get("id", ""))] = {
                "is_customer_ticket": r.get("is_customer_ticket", True),
                "name": r.get("name"),
                "company": r.get("company"),
                "role": r.get("role"),
            }
        done = batch_start + len(batch)
        save_contacts(contacts)
        with open(CONTACTS_PROGRESS_FILE, "w") as f:
            json.dump({"done": done, "total": total, "running": True}, f)

    save_contacts(contacts)
    with open(CONTACTS_PROGRESS_FILE, "w") as f:
        json.dump({"done": total, "total": total, "running": False}, f)


def start_contact_extraction(df: pd.DataFrame, ai: anthropic.Anthropic):
    t = threading.Thread(target=_run_contact_extraction_thread, args=(df, ai), daemon=True)
    t.start()


# ============================================================
# TICKET SUMMARY EXTRACTION (cached, incremental)
# ============================================================

def load_summaries() -> dict:
    if os.path.exists(SUMMARIES_FILE):
        try:
            with open(SUMMARIES_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_summaries(summaries: dict):
    with _summaries_lock:
        with open(SUMMARIES_FILE, "w") as f:
            json.dump(summaries, f)


def load_summaries_progress() -> dict:
    if os.path.exists(SUMMARIES_PROGRESS_FILE):
        try:
            with open(SUMMARIES_PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"done": 0, "total": 0, "running": False}


def _summarize_batch(ai: anthropic.Anthropic, batch: list) -> list:
    ticket_lines = []
    for t in batch:
        ticket_lines.append(
            f"[{t['id']}] Title: {t['title']}\n"
            f"Description: {t['description'][:600]}"
        )
    tickets_text = ("\n" + "=" * 60 + "\n").join(ticket_lines)
    prompt = f"""Summarize each feature request ticket in ONE concise sentence (max 120 chars).
Capture the core ask — what the user wants and why.

Tickets:

{tickets_text}

Respond with a JSON array — one object per ticket, in the same order:
[{{"id": "<id>", "summary": "one sentence summary"}}]
No other text."""
    try:
        resp = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=SUMMARY_BATCH_SIZE * 60,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = "\n".join(text.split("\n")[1:]).rstrip("`").strip()
        return json.loads(text)
    except Exception:
        return [{"id": t["id"], "summary": t["title"]} for t in batch]


def _run_summary_extraction_thread(df: pd.DataFrame, ai: anthropic.Anthropic):
    summaries = load_summaries()
    id_col = COLUMNS.get("id", "id")
    title_col = COLUMNS.get("title", "name")
    desc_col = COLUMNS.get("description", "description")

    to_summarize = []
    for _, row in df.iterrows():
        ticket_id = str(row.get(id_col, ""))
        if ticket_id in summaries:
            continue
        to_summarize.append({
            "id": ticket_id,
            "title": str(row.get(title_col, "")),
            "description": str(row.get(desc_col, "")),
        })

    total = len(to_summarize)

    for batch_start in range(0, total, SUMMARY_BATCH_SIZE):
        batch = to_summarize[batch_start: batch_start + SUMMARY_BATCH_SIZE]
        results = _summarize_batch(ai, batch)
        for r in results:
            summaries[str(r.get("id", ""))] = r.get("summary", "")
        done = batch_start + len(batch)
        save_summaries(summaries)
        with open(SUMMARIES_PROGRESS_FILE, "w") as f:
            json.dump({"done": done, "total": total, "running": True}, f)

    save_summaries(summaries)
    with open(SUMMARIES_PROGRESS_FILE, "w") as f:
        json.dump({"done": total, "total": total, "running": False}, f)


def start_summary_extraction(df: pd.DataFrame, ai: anthropic.Anthropic):
    t = threading.Thread(target=_run_summary_extraction_thread, args=(df, ai), daemon=True)
    t.start()


def apply_contacts_to_df(df: pd.DataFrame, contacts: dict) -> pd.DataFrame:
    id_col = COLUMNS.get("id", "id")
    def _contact_str(row):
        tid = str(row.get(id_col, ""))
        c = contacts.get(tid, {})
        name = c.get("name") or ""
        company = c.get("company") or ""
        if name and company:
            return f"{name} ({company})"
        return name or company or ""
    df = df.copy()
    df["contact"] = df.apply(_contact_str, axis=1)
    return df


# ============================================================
# CHATBOT HELPERS
# ============================================================

def _get(row: pd.Series, key: str, default: str = "") -> str:
    col = COLUMNS.get(key, "")
    if col and col in row.index:
        val = row.get(col, "")
        return str(val).strip() if pd.notna(val) and str(val).strip() else default
    return default


def format_tickets_for_context(df: pd.DataFrame, contacts: dict, summaries: dict | None = None) -> str:
    if df.empty:
        return "No feature request tickets available."
    id_col = COLUMNS.get("id", "id")
    sample = df if CHATBOT_MAX_TICKETS is None else df.head(CHATBOT_MAX_TICKETS)
    if summaries is None:
        summaries = {}
    lines = []
    for i, (_, row) in enumerate(sample.iterrows(), start=1):
        title       = _get(row, "title") or f"Ticket #{i}"
        ticket_id   = _get(row, "id")
        area        = _get(row, "product_area")
        severity_label = str(row.get("severity_label", ""))
        score_pts   = str(row.get("score_pts", ""))
        company     = str(row.get("company_name_parsed", ""))
        tier_label  = str(row.get("tier_label", ""))
        themes_str  = str(row.get("themes_str", ""))
        c = contacts.get(str(row.get(id_col, "")), {})
        contact_name    = c.get("name") or ""
        contact_company = c.get("company") or ""
        contact_role    = c.get("role") or ""

        # Use cached summary if available, else fall back to truncated description
        summary = summaries.get(str(row.get(id_col, "")), "")
        if not summary:
            raw_desc = _get(row, "description")
            summary = (raw_desc[:200] + "...") if len(raw_desc) > 200 else raw_desc

        id_part = f"sc-{ticket_id}" if ticket_id else f"#{i}"
        header = f"[{id_part}] {title}"
        if severity_label: header += f"  |  {severity_label} ({score_pts}pts)"
        if company:        header += f"  |  {company} ({tier_label})"
        if area:           header += f"  |  {area}"

        body_lines = [header]
        if themes_str:  body_lines.append(f"   Themes: {themes_str}")
        if summary:     body_lines.append(f"   Summary: {summary}")
        if contact_name:
            cl = f"   Contact: {contact_name}"
            if contact_company: cl += f" ({contact_company})"
            if contact_role:    cl += f" — {contact_role}"
            body_lines.append(cl)
        lines.append("\n".join(body_lines))

    return "\n\n".join(lines)


def build_system_prompt(df: pd.DataFrame, contacts: dict, summaries: dict | None = None) -> str:
    ticket_count = len(df) if CHATBOT_MAX_TICKETS is None else min(len(df), CHATBOT_MAX_TICKETS)
    tickets_text = format_tickets_for_context(df, contacts, summaries)
    return f"""You are a product analyst specializing in NPI (New Product Introduction) impact assessment.

You have access to {ticket_count} customer feature request ticket(s), each scored by priority (P1/P2/P3) based on company tier, urgency, and volume.

## Feature Request Tickets

{tickets_text}

---

## Your Job

When the user describes an NPI change, respond with:

1. **Brief restatement** of the NPI change as you understand it (one sentence).

2. **Directly Addressed (N tickets):** — tickets the NPI change fully or substantially fulfills.
   - #[id] **[Title]** — [one sentence why] | Contact: [name if known]

3. **Potentially Impacted (N tickets):** — tickets in the same area that may be partially addressed or affected.
   - #[id] **[Title]** — [one sentence on the impact] | Contact: [name if known]

4. **Related Context (N tickets):** — tickets in adjacent areas worth considering.
   - #[id] **[Title]** — [one sentence on the connection]

5. **Summary** — 2–3 sentences: how much existing demand does this NPI cover? Major unaddressed themes? Which Enterprise/Strategic customers are affected?

**Be thorough.** Prioritize P1 and P2 tickets, and Enterprise/Strategic tier companies. Err on the side of inclusion.

For follow-up questions, answer conversationally using the ticket data above."""


# ============================================================
# UTILITY
# ============================================================

def resolve_col(key: str, df: pd.DataFrame):
    col = COLUMNS.get(key, "")
    return col if col and col in df.columns else None


def _metric_card(label, value, sub=None):
    sub_html = f'<div style="color:#9E9E9E;font-size:0.78rem;margin-top:4px;">{sub}</div>' if sub else ""
    return f"""
    <div style="background-color:#373E47;border:1px solid #444C56;border-radius:10px;
                padding:16px 20px;min-height:100px;display:flex;flex-direction:column;justify-content:space-between;">
        <div style="color:#9E9E9E;font-size:0.85rem;font-weight:700;">{label}</div>
        <div style="color:#E0E0E0;font-size:2rem;font-weight:700;line-height:1.1;">{value}</div>
        {sub_html}
    </div>"""


def _tier_badge(tier_label: str) -> str:
    styles = {
        "Enterprise": "background-color:#FF6B6B33;color:#FF6B6B;border:1px solid #FF6B6B55;",
        "Strategic":  "background-color:#FFB74D33;color:#FFB74D;border:1px solid #FFB74D55;",
        "Standard":   "background-color:#9E9E9E22;color:#9E9E9E;border:1px solid #9E9E9E44;",
    }
    style = styles.get(tier_label, styles["Standard"])
    return f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:700;{style}">{tier_label}</span>'


# ============================================================
# MAIN APP
# ============================================================

def main():
    # Cookie manager (must render on every run)
    _cookies = _cookie_mgr()

    # Restore session from cookie
    if not st.session_state.get("_auth_user"):
        _cookie_val = _cookies.get(_AUTH_COOKIE)
        if _cookie_val:
            _restored = _decode_auth(_cookie_val)
            if _restored:
                st.session_state["_auth_user"] = _restored

    # OAuth callback
    _qp = st.query_params
    if "code" in _qp and "state" in _qp:
        with st.spinner("Signing you in…"):
            _user, _err = _exchange_code(_qp["code"], _qp["state"])
        if _err:
            st.error(_err)
        else:
            st.session_state["_auth_user"] = _user
            _log_visit(_user)
            _cookies.set(_AUTH_COOKIE, _encode_auth(_user),
                         expires_at=datetime.now() + timedelta(hours=_COOKIE_TTL_HOURS))
        st.query_params.clear()
        st.rerun()

    if not st.session_state.get("_auth_user"):
        _show_login_page()
        st.stop()

    # Header
    logo_path = os.path.join(_APP_DIR, "logo.svg")
    if os.path.exists(logo_path):
        with open(logo_path, "r") as f:
            logo_svg = f.read()
        logo_b64 = base64.b64encode(logo_svg.encode()).decode()
        st.markdown(f"""
        <div class="header-container">
            <img src="data:image/svg+xml;base64,{logo_b64}" />
            <h1>Feature Request Dashboard</h1>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown('<h1 style="color:#00E676;">Feature Request Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Customer feature requests · NPI impact analysis · Priority scoring</div>', unsafe_allow_html=True)

    # User bar
    _auth_user = st.session_state.get("_auth_user", {})
    _col_spacer, _col_user = st.columns([6, 1])
    with _col_user:
        with st.popover(f"👤 {_auth_user.get('email', '')}", use_container_width=True):
            st.markdown(f"**{_auth_user.get('name', '')}**")
            st.markdown(f"`{_auth_user.get('email', '')}`")
            if st.button("Sign out", key="_logout_btn", use_container_width=True):
                del st.session_state["_auth_user"]
                _cookies.delete(_AUTH_COOKIE)
                st.rerun()

    # Sidebar
    with st.sidebar:
        if os.path.exists(logo_path):
            st.image(logo_path, width=60)
        st.markdown("---")
        st.markdown("### Data")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        st.caption(f"Last loaded: {datetime.now().strftime('%H:%M:%S')}")

    # Load data
    with st.spinner("Loading feature requests…"):
        df_raw = load_feature_requests()

    company_tiers = load_company_tiers()

    if not df_raw.empty:
        df_raw = enrich_with_scores(df_raw, company_tiers)
        df_raw = enrich_with_themes(df_raw)

    # Contact extraction
    contacts = load_contacts()
    progress = load_contacts_progress()
    ai = get_anthropic_client()

    if not df_raw.empty and ai:
        id_col_name = COLUMNS.get("id", "id")
        unanalyzed = [
            str(row.get(id_col_name, ""))
            for _, row in df_raw.iterrows()
            if str(row.get(id_col_name, "")) not in contacts
        ]
        if unanalyzed and not progress.get("running"):
            start_contact_extraction(df_raw, ai)
            progress = {"running": True, "done": 0, "total": len(df_raw)}

    # Summary extraction (runs after contact extraction finishes, or in parallel if contacts done)
    summaries = load_summaries()
    sum_progress = load_summaries_progress()

    if not df_raw.empty and ai:
        id_col_name = COLUMNS.get("id", "id")
        unsummarized = [
            str(row.get(id_col_name, ""))
            for _, row in df_raw.iterrows()
            if str(row.get(id_col_name, "")) not in summaries
        ]
        if unsummarized and not sum_progress.get("running"):
            start_summary_extraction(df_raw, ai)
            sum_progress = {"running": True, "done": 0, "total": len(unsummarized)}

    any_running = progress.get("running") or sum_progress.get("running")
    if any_running:
        st.markdown('<meta http-equiv="refresh" content="4">', unsafe_allow_html=True)

    if progress.get("running"):
        done  = progress.get("done", 0)
        total = progress.get("total", 1)
        st.markdown(f"""
        <div class="progress-banner">
            <span class="progress-text">Analyzing tickets… {done}/{total} processed — filtering to customer requests</span>
        </div>
        """, unsafe_allow_html=True)
        st.progress(done / total if total > 0 else 0)

    if sum_progress.get("running"):
        done  = sum_progress.get("done", 0)
        total = sum_progress.get("total", 1)
        st.markdown(f"""
        <div class="progress-banner">
            <span class="progress-text">Summarizing tickets… {done}/{total} processed — building chatbot context</span>
        </div>
        """, unsafe_allow_html=True)
        st.progress(done / total if total > 0 else 0)

    df = df_raw.copy() if not df_raw.empty else df_raw
    if not df.empty:
        df = apply_contacts_to_df(df, contacts)
        id_col_name = COLUMNS.get("id", "id")
        if contacts:
            def _is_not_customer(row):
                tid = str(row.get(id_col_name, ""))
                c = contacts.get(tid)
                if c is None:
                    return False
                return c.get("is_customer_ticket") is False
            df = df[~df.apply(_is_not_customer, axis=1)]

    with st.sidebar:
        with st.expander("🔧 Debug", expanded=False):
            if not df.empty and "custom_fields" in df.columns:
                sample = df["custom_fields"].dropna().astype(str)
                sample = sample[sample.str.strip().str.len() > 5]
                if not sample.empty:
                    st.text("Raw custom_fields (first row):")
                    st.code(sample.iloc[0][:800])
                    st.text("Parsed:")
                    st.json(_parse_custom_fields_text(sample.iloc[0]))
            elif df.empty:
                st.text("No data loaded.")

    # ── TABS ─────────────────────────────────────────────────
    _admin_mode = _is_admin()
    tab_labels = ["📋 Feature Requests", "🏢 By Company", "🏷️ Themes", "💬 NPI Chatbot", "📊 Google Sheet"]
    if _admin_mode:
        tab_labels.append("Admin")

    tabs = st.tabs(tab_labels)
    tab_fr, tab_company, tab_themes, tab_npi, tab_sheet = tabs[:5]
    tab_admin = tabs[5] if _admin_mode else None

    # ════════════════════════════════════════════════════════
    # TAB 1 — FEATURE REQUESTS
    # ════════════════════════════════════════════════════════
    with tab_fr:
        if df.empty:
            st.warning("No feature request tickets found. Check your sheet ID, tab name, and column configuration.")
            st.stop()

        p1_count = (df["severity_label"] == "P1").sum()
        p2_count = (df["severity_label"] == "P2").sum()
        contacts_extracted = sum(1 for v in contacts.values() if v.get("name"))
        enterprise_count = (df["tier"] == 1).sum()
        area_col = resolve_col("product_area", df)

        st.markdown(f'<div style="color:#9E9E9E;font-size:0.9rem;margin-bottom:12px;">{len(df):,} open feature requests from 2025 · sorted by priority score</div>', unsafe_allow_html=True)

        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            st.markdown(_metric_card("Total Requests", f"{len(df):,}"), unsafe_allow_html=True)
        with m2:
            pct = f"{p1_count/len(df)*100:.0f}% of total" if len(df) > 0 else ""
            st.markdown(_metric_card("P1 Critical", p1_count, sub=pct), unsafe_allow_html=True)
        with m3:
            st.markdown(_metric_card("P2 High", p2_count), unsafe_allow_html=True)
        with m4:
            st.markdown(_metric_card("Enterprise Tickets", enterprise_count, sub=f"{contacts_extracted} contacts extracted"), unsafe_allow_html=True)
        with m5:
            area_count = df[area_col].nunique() if area_col else "—"
            st.markdown(_metric_card("Product Areas", area_count), unsafe_allow_html=True)

        st.markdown("---")

        analyzed = len(contacts)
        if analyzed < len(df):
            st.caption(f"Analysis running — {analyzed}/{len(df)} tickets reviewed · {contacts_extracted} contacts found · {len(df) - analyzed} remaining")
        else:
            st.caption(f"Analysis complete — {analyzed} tickets reviewed · {contacts_extracted} contacts found")

        st.markdown("---")

        # Filters
        with st.expander("🔍 Search & Filter", expanded=False):
            fc1, fc2, fc3, fc4, fc5 = st.columns(5)
            with fc1:
                search = st.text_input("Keyword", placeholder="Search title / description…")
            with fc2:
                sev_filter = st.selectbox("Severity", ["All", "P1", "P2", "P3"])
            with fc3:
                tier_filter = st.selectbox("Customer Tier", ["All", "Enterprise", "Strategic", "Standard"])
            with fc4:
                if area_col:
                    areas = ["All"] + sorted(df[area_col].dropna().astype(str).unique().tolist())
                    area_filter = st.selectbox("Product Area", areas)
                else:
                    area_filter = "All"
            with fc5:
                status_col = resolve_col("status", df)
                if status_col:
                    statuses = ["All"] + sorted(df[status_col].dropna().astype(str).unique().tolist())
                    status_filter = st.selectbox("Status", statuses)
                else:
                    status_filter = "All"

        fdf = df.copy()
        title_col = resolve_col("title", fdf)
        desc_col  = resolve_col("description", fdf)

        if search:
            mask = pd.Series([False] * len(fdf), index=fdf.index)
            for col in [title_col, desc_col]:
                if col:
                    mask |= fdf[col].astype(str).str.contains(search, case=False, na=False)
            fdf = fdf[mask]
        if sev_filter != "All":
            fdf = fdf[fdf["severity_label"] == sev_filter]
        if tier_filter != "All":
            fdf = fdf[fdf["tier_label"] == tier_filter]
        if area_filter != "All" and area_col:
            fdf = fdf[fdf[area_col].astype(str) == area_filter]
        if status_filter != "All" and status_col:
            fdf = fdf[fdf[status_col].astype(str) == status_filter]

        if len(fdf) != len(df):
            st.caption(f"Showing {len(fdf):,} of {len(df):,} tickets")

        # Table — pick columns that exist
        display_cols = []
        for k in ["id", "title", "link", "timestamp", "submitter", "contact",
                   "severity_label", "company_name_parsed", "tier_label",
                   "product_area", "priority", "status", "labels"]:
            if k in ("contact", "severity_label", "company_name_parsed", "tier_label"):
                if k in fdf.columns:
                    display_cols.append(k)
            else:
                col = COLUMNS.get(k)
                if col and col in fdf.columns:
                    display_cols.append(col)

        if not display_cols:
            display_cols = list(fdf.columns[:6])

        col_config = {}
        link_col = COLUMNS.get("link", "")
        if link_col and link_col in display_cols:
            col_config[link_col] = st.column_config.LinkColumn("Link", display_text="Shortcut ↗")

        st.dataframe(fdf[display_cols], use_container_width=True, height=400, column_config=col_config)

        # Ticket detail
        st.markdown("---")
        st.subheader("🔎 Ticket Detail")
        if title_col and not fdf.empty:
            options = ["— select a ticket —"] + fdf[title_col].astype(str).tolist()
            selected = st.selectbox("Select ticket", options)
            if selected != "— select a ticket —":
                row = fdf[fdf[title_col].astype(str) == selected].iloc[0]
                dcol1, dcol2 = st.columns([3, 1])
                with dcol1:
                    if desc_col:
                        st.markdown("**Description**")
                        st.markdown(str(row.get(desc_col, "—")))
                with dcol2:
                    sev = row.get("severity_label", "")
                    tier_lbl = row.get("tier_label", "")
                    pts = row.get("score_pts", 0)
                    themes = row.get("themes_str", "")
                    company = row.get("company_name_parsed", "")
                    if sev:
                        sev_color = {"P1": "#FF6B6B", "P2": "#FFB74D", "P3": "#9E9E9E"}.get(sev, "#9E9E9E")
                        st.markdown(f'**Severity:** <span style="color:{sev_color};font-weight:700;">{sev}</span> ({pts} pts)', unsafe_allow_html=True)
                    if company:
                        st.markdown(f"**Company:** {company}")
                    if tier_lbl:
                        st.markdown(_tier_badge(tier_lbl), unsafe_allow_html=True)
                    if themes:
                        st.markdown(f"**Themes:** {themes}")
                    contact_val = str(row.get("contact", "")).strip()
                    if contact_val:
                        st.markdown(f"**Contact:** {contact_val}")
                    else:
                        id_val = str(row.get(COLUMNS.get("id", "id"), ""))
                        msg = "_(none found)_" if id_val in contacts else "_(not yet extracted)_"
                        st.markdown(f"**Contact:** {msg}")
                    for key in ["submitter", "product_area", "priority", "status", "labels", "epic", "team", "timestamp"]:
                        col_name = COLUMNS.get(key, "")
                        if col_name and col_name in row.index and pd.notna(row.get(col_name)):
                            val = row.get(col_name)
                            if hasattr(val, "strftime"):
                                val = val.strftime("%Y-%m-%d")
                            st.markdown(f"**{key.replace('_', ' ').title()}:** {val}")

        # Charts
        st.markdown("---")
        cc1, cc2 = st.columns(2)
        with cc1:
            st.subheader("By Severity")
            sev_counts = df["severity_label"].value_counts().reindex(["P1", "P2", "P3"], fill_value=0).reset_index()
            sev_counts.columns = ["Severity", "Count"]
            st.bar_chart(sev_counts.set_index("Severity"))
        with cc2:
            ts_col = resolve_col("timestamp", df)
            if ts_col:
                st.subheader("Submissions Over Time")
                df_t = df[[ts_col]].dropna().copy()
                df_t["month"] = df_t[ts_col].dt.to_period("M").astype(str)
                monthly = df_t["month"].value_counts().sort_index().reset_index()
                monthly.columns = ["Month", "Count"]
                st.bar_chart(monthly.set_index("Month"))

        # Downloads
        st.markdown("---")
        dl1, dl2 = st.columns(2)
        with dl1:
            buf = io.StringIO()
            fdf.to_csv(buf, index=False)
            st.download_button("⬇️ Download Filtered CSV", buf.getvalue(),
                file_name=f"fr_filtered_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv", use_container_width=True)
        with dl2:
            buf2 = io.StringIO()
            df.to_csv(buf2, index=False)
            st.download_button("⬇️ Download All Feature Requests", buf2.getvalue(),
                file_name=f"fr_all_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv", use_container_width=True)

    # ════════════════════════════════════════════════════════
    # TAB 2 — BY COMPANY
    # ════════════════════════════════════════════════════════
    with tab_company:
        if df.empty:
            st.warning("No data loaded.")
        else:
            st.subheader("Feature Requests by Company")
            title_col = COLUMNS.get("title", "name")
            link_col  = COLUMNS.get("link", "app_url")

            company_df = df[df["company_name_parsed"].str.strip() != ""].copy()
            unknown_count = (df["company_name_parsed"].str.strip() == "").sum()

            groups = defaultdict(list)
            for _, row in company_df.iterrows():
                groups[row["company_name_parsed"]].append(row)

            def _group_sort(item):
                _, rows = item
                p1 = sum(1 for r in rows if r.get("severity_label") == "P1")
                p2 = sum(1 for r in rows if r.get("severity_label") == "P2")
                tier = min(r.get("tier", 3) for r in rows)
                return (-p1, -p2, tier, -len(rows))

            sorted_groups = sorted(groups.items(), key=_group_sort)

            sm1, sm2, sm3, sm4 = st.columns(4)
            with sm1:
                st.markdown(_metric_card("Companies", len(sorted_groups)), unsafe_allow_html=True)
            with sm2:
                ent = sum(1 for _, rows in sorted_groups if any(r.get("tier") == 1 for r in rows))
                st.markdown(_metric_card("Enterprise", ent), unsafe_allow_html=True)
            with sm3:
                strat = sum(1 for _, rows in sorted_groups
                            if any(r.get("tier") == 2 for r in rows)
                            and all(r.get("tier", 3) >= 2 for r in rows))
                st.markdown(_metric_card("Strategic", strat), unsafe_allow_html=True)
            with sm4:
                st.markdown(_metric_card("Unknown Company", unknown_count), unsafe_allow_html=True)

            st.markdown("---")
            company_search = st.text_input("Filter companies", placeholder="Search company name…", key="company_search")

            for company_name, rows in sorted_groups:
                if company_search and company_search.lower() not in company_name.lower():
                    continue
                p1s = [r for r in rows if r.get("severity_label") == "P1"]
                p2s = [r for r in rows if r.get("severity_label") == "P2"]
                p3s = [r for r in rows if r.get("severity_label") == "P3"]
                tier = rows[0].get("tier", 3)
                tier_lbl = rows[0].get("tier_label", "Standard")
                total_pts = sum(r.get("score_pts", 0) for r in rows)

                sev_parts = []
                if p1s: sev_parts.append(f'<span style="color:#FF6B6B;font-weight:700;">{len(p1s)} P1</span>')
                if p2s: sev_parts.append(f'<span style="color:#FFB74D;font-weight:700;">{len(p2s)} P2</span>')
                if p3s: sev_parts.append(f'<span style="color:#9E9E9E;">{len(p3s)} P3</span>')
                sev_html = " · ".join(sev_parts) if sev_parts else '<span style="color:#9E9E9E;">P3</span>'

                with st.expander(f"{company_name} — {len(rows)} tickets", expanded=(tier == 1 and len(rows) >= 3)):
                    st.markdown(f'{_tier_badge(tier_lbl)} **{tier_lbl}** · {sev_html} · Total score: {total_pts} pts', unsafe_allow_html=True)
                    st.markdown("---")
                    for row in sorted(rows, key=lambda r: r.get("score_pts", 0), reverse=True):
                        sev = row.get("severity_label", "P3")
                        pts = row.get("score_pts", 0)
                        title = str(row.get(title_col, "—"))
                        lnk = str(row.get(link_col, "")) if link_col and link_col in row.index else ""
                        contact = str(row.get("contact", "")).strip()
                        themes = str(row.get("themes_str", "")).strip()
                        sev_color = {"P1": "#FF6B6B", "P2": "#FFB74D", "P3": "#9E9E9E"}.get(sev, "#9E9E9E")
                        title_html = f'<a href="{lnk}" target="_blank" style="color:#E0E0E0;text-decoration:none;">{title}</a>' if lnk and lnk not in ("nan", "") else title
                        line = f'<span style="color:{sev_color};font-weight:700;">{sev}</span> ({pts}pts) &nbsp;{title_html}'
                        if contact:
                            line += f' &nbsp;<span style="color:#9E9E9E;font-size:0.85rem;">· {contact}</span>'
                        if themes:
                            line += f' &nbsp;<span style="color:#616a75;font-size:0.8rem;">· {themes}</span>'
                        st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #3a4149;">{line}</div>', unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════
    # TAB 3 — THEMES
    # ════════════════════════════════════════════════════════
    with tab_themes:
        if df.empty:
            st.warning("No data loaded.")
        else:
            st.subheader("Feature Requests by Theme")
            title_col = COLUMNS.get("title", "name")
            link_col  = COLUMNS.get("link", "app_url")

            theme_tickets = defaultdict(list)
            for _, row in df.iterrows():
                for theme in row.get("themes", ["Other"]):
                    theme_tickets[theme].append(row)

            sorted_themes = sorted(theme_tickets.items(), key=lambda x: -len(x[1]))

            theme_summary = pd.DataFrame({
                "Theme": [t for t, _ in sorted_themes],
                "Count": [len(rows) for _, rows in sorted_themes],
            })
            st.bar_chart(theme_summary.set_index("Theme"))
            st.markdown("---")

            for theme, rows in sorted_themes:
                color = THEMES.get(theme, {}).get("color", "#9E9E9E")
                p1s = sum(1 for r in rows if r.get("severity_label") == "P1")
                p2s = sum(1 for r in rows if r.get("severity_label") == "P2")
                sev_str = ""
                if p1s: sev_str += f" · {p1s} P1"
                if p2s: sev_str += f" · {p2s} P2"
                with st.expander(f"{theme} — {len(rows)} tickets{sev_str}", expanded=False):
                    for row in sorted(rows, key=lambda r: r.get("score_pts", 0), reverse=True):
                        sev = row.get("severity_label", "P3")
                        pts = row.get("score_pts", 0)
                        title = str(row.get(title_col, "—"))
                        lnk = str(row.get(link_col, "")) if link_col and link_col in row.index else ""
                        company = str(row.get("company_name_parsed", "")).strip()
                        tier_lbl = str(row.get("tier_label", "")).strip()
                        sev_color = {"P1": "#FF6B6B", "P2": "#FFB74D", "P3": "#9E9E9E"}.get(sev, "#9E9E9E")
                        title_html = f'<a href="{lnk}" target="_blank" style="color:#E0E0E0;text-decoration:none;">{title}</a>' if lnk and lnk not in ("nan", "") else title
                        company_html = f'<span style="color:#9E9E9E;font-size:0.85rem;">· {company} ({tier_lbl})</span>' if company else ""
                        line = f'<span style="color:{sev_color};font-weight:700;">{sev}</span> ({pts}pts) &nbsp;{title_html} &nbsp;{company_html}'
                        st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #3a4149;">{line}</div>', unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════
    # TAB 4 — NPI CHATBOT
    # ════════════════════════════════════════════════════════
    with tab_npi:
        st.subheader("💬 NPI Impact Chatbot")
        st.markdown(
            "Describe a **New Product Introduction (NPI) change** — a new feature, product update, "
            "or architectural change — and I'll identify which feature request tickets would be "
            "impacted or addressed, including who to notify. Tickets are pre-scored by priority."
        )
        if df.empty:
            st.warning("No feature request tickets loaded. Please fix your data source first.")
            st.stop()

        if "chat_messages" not in st.session_state:
            st.session_state.chat_messages = []

        bar1, bar2 = st.columns([5, 1])
        with bar1:
            ticket_count = len(df) if CHATBOT_MAX_TICKETS is None else min(len(df), CHATBOT_MAX_TICKETS)
            contacts_extracted = sum(1 for v in contacts.values() if v.get("name"))
            summarized_count = sum(1 for tid in df[COLUMNS.get("id", "id")].astype(str) if tid in summaries)
            label = f"Analyzing all {ticket_count:,} tickets (scored P1–P3)"
            if CHATBOT_MAX_TICKETS is not None and ticket_count < len(df):
                label = f"Analyzing {ticket_count:,} tickets (scored P1–P3) ({ticket_count / len(df) * 100:.0f}% of total)"
            parts = []
            if summarized_count:
                parts.append(f"{summarized_count:,} summarized")
            if contacts_extracted:
                parts.append(f"{contacts_extracted} contacts")
            if parts:
                label += f" · {' · '.join(parts)}"
            st.caption(label)
        with bar2:
            if st.button("🗑️ Clear Chat", use_container_width=True):
                st.session_state.chat_messages = []
                st.rerun()

        for msg in st.session_state.chat_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        if user_input := st.chat_input("e.g. 'We're adding bulk PDF export to the reporting module'…"):
            st.session_state.chat_messages.append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)

            with st.chat_message("assistant"):
                placeholder = st.empty()
                full_response = ""
                ai = get_anthropic_client()
                if ai is None:
                    full_response = "ANTHROPIC_API_KEY is not configured."
                    placeholder.markdown(full_response)
                else:
                    system_prompt = build_system_prompt(df, contacts, summaries)
                    api_msgs = [{"role": m["role"], "content": m["content"]}
                                for m in st.session_state.chat_messages]
                    try:
                        with ai.messages.stream(
                            model="claude-opus-4-6",
                            max_tokens=4096,
                            thinking={"type": "adaptive"},
                            system=system_prompt,
                            messages=api_msgs,
                        ) as stream:
                            for chunk in stream.text_stream:
                                full_response += chunk
                                placeholder.markdown(full_response + "▌")
                        placeholder.markdown(full_response)
                    except Exception as e:
                        full_response = f"Claude API error: {e}"
                        placeholder.markdown(full_response)

            st.session_state.chat_messages.append({"role": "assistant", "content": full_response})

    # ════════════════════════════════════════════════════════
    # TAB 5 — GOOGLE SHEET
    # ════════════════════════════════════════════════════════
    with tab_sheet:
        st.subheader("📊 Source Google Sheet")
        if not SHEET_ID:
            st.info("Set GOOGLE_SHEET_ID environment variable to embed the sheet here.")
        else:
            st.markdown(
                f'<iframe src="https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit'
                f'?usp=sharing&rm=minimal" width="100%" height="720" frameborder="0"></iframe>',
                unsafe_allow_html=True,
            )

    # ════════════════════════════════════════════════════════
    # TAB ADMIN
    # ════════════════════════════════════════════════════════
    if _admin_mode and tab_admin is not None:
        with tab_admin:
            st.subheader("Access Log")
            log_df = _load_access_log()
            if log_df.empty:
                st.info("No visits recorded yet.")
            else:
                log_df.columns = [c.capitalize() for c in log_df.columns]
                if "Timestamp" in log_df.columns:
                    log_df["Timestamp"] = pd.to_datetime(log_df["Timestamp"], errors="coerce")
                    log_df = log_df.sort_values("Timestamp", ascending=False).reset_index(drop=True)
                    log_df["Timestamp"] = log_df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
                m1, m2 = st.columns(2)
                m1.metric("Total logins", len(log_df))
                m2.metric("Unique users", log_df["Email"].nunique() if "Email" in log_df.columns else 0)
                st.divider()
                if "Email" in log_df.columns:
                    counts = (
                        log_df.groupby("Email")
                        .agg(Logins=("Email", "count"), Last_seen=("Timestamp", "max"))
                        .reset_index()
                        .rename(columns={"Last_seen": "Last seen"})
                        .sort_values("Logins", ascending=False)
                    )
                    st.dataframe(counts, use_container_width=True, hide_index=True)
                    st.divider()
                st.dataframe(log_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
