"""
L2 Capability Analyzer — Interactive Dashboard
Run with: streamlit run l2_dashboard.py
"""

import streamlit as st
import pandas as pd
import csv
import json
import time
import os
import base64
from datetime import datetime
from anthropic import Anthropic
import gspread
from google.oauth2.service_account import Credentials
import threading

# ── L2 Supported Capabilities ──────────────────────────────────────────────
L2_CAPABILITIES = """
L2 Support can handle the following types of tasks:

1. **Data Restores** – Restoring lost, deleted, or corrupted data for customers from backups or database records.
2. **Small Code Changes** – Minor bug fixes, configuration tweaks, or small feature adjustments that don't require deep architectural changes.
3. **Account Access Issues** – Resolving login problems, password resets, multi-org access issues, SSO configuration, and permission/role adjustments.
4. **Configuration Changes** – Updating application settings, feature flags, environment variables, or tenant-level configurations on behalf of customers.
5. **Data Exports / Imports** – Assisting with bulk data exports, CSV imports, or data migration tasks that follow established procedures.
6. **User Management** – Adding, removing, or modifying user accounts, roles, and permissions within the platform.
7. **Basic Troubleshooting & Diagnostics** – Investigating error logs, reproducing reported issues, and identifying root causes for common/known issues.
8. **Database Queries** – Running predefined or simple ad-hoc queries to look up customer data, verify states, or gather diagnostic information.
9. **Documentation & Runbook Execution** – Following existing runbooks or standard operating procedures to resolve known issue types.
10. **Integration Support** – Troubleshooting API integrations, webhook failures, or third-party connection issues using existing documentation.
11. **Cache / Queue Management** – Clearing caches, reprocessing stuck queue items, or restarting background jobs.
12. **Environment & Deployment Support** – Assisting with deployment verification, rollback of recent changes, or environment health checks.
13. **Customer Communication** – Providing status updates, workarounds, and resolution summaries to customers for known issues.

L2 CANNOT handle:
- Large-scale architectural changes or rewrites
- New feature development requiring design review
- Issues requiring deep investigation into unknown/novel bugs with no prior precedent
- Security incidents or vulnerability remediation
- Infrastructure-level changes (networking, cloud resource provisioning)
- Changes requiring cross-team coordination or product decisions
- Performance optimization requiring profiling and benchmarking
"""

EVALUATION_PROMPT = """You are an evaluator that determines whether an L2 support team can handle a given support ticket.

Here are the capabilities that L2 support can handle:
{capabilities}

Now evaluate the following support ticket:

**Ticket Name:** {name}

**Description:**
{description}

**Intercom Transcript:**
{intercom_transcript}

**Slack Conversation:**
{slack_transcript}

**Shortcut Ticket Activity:**
{shortcut_activity}

IMPORTANT: First assess whether there is enough information to make a meaningful evaluation. If the ticket has very little data — for example, only a title with no description, no transcripts, no Shortcut activity, and no Slack conversation — then you do NOT have enough context to evaluate it. In that case, set the decision to "Insufficient Data".

If there IS enough data, determine whether L2 can support this task based on the ticket details and L2's defined capabilities.

Also classify the ticket into the single most relevant category from this list:
- Data Restores
- Small Code Changes
- Account Access Issues
- Configuration Changes
- Data Exports / Imports
- User Management
- Basic Troubleshooting
- Database Queries
- Runbook Execution
- Integration Support
- Cache / Queue Management
- Deployment Support
- Customer Communication
- New Feature Request
- Bug Fix (Engineering Required)
- Security Incident
- Infrastructure Change
- Other

SUPPORT PERSON IDENTIFICATION:
Identify the support person who was conversing with the customer in the Intercom Transcript. The Intercom Transcript uses this format for messages:
- "--- Support (Person Name) ---" for internal support team messages
- "--- Customer (Person Name) ---" for customer messages
- "--- Support (Your Guideian) ---" is an automated bot message, NOT a real person — ignore these

Look for lines matching "--- Support (Name) ---" where Name is NOT "Your Guideian". That person is the support person. If multiple support people appear, pick the one who sent the most messages. Only use names from "--- Support (...) ---" lines — do NOT use names from "--- Customer (...) ---" lines (those are customers). If no real support person appears in the Intercom Transcript, use "Unknown".

L2 ENGINEER INVOLVEMENT:
Our L2 engineers are Sean and Jayson. You must determine if either performed concrete technical work to fix the issue.

EXTREMELY STRICT CRITERIA — the bar is very high. Default is ALWAYS "None" unless you find undeniable proof.

The ONLY evidence that counts is found in the **Shortcut Ticket Activity** section. Do NOT use Intercom Transcript or Slack Conversation to determine L2 involvement — those are customer/support communications.

You MUST find at least one of these in the Shortcut Ticket Activity to credit Sean or Jayson:
- A PR or commit they authored (look for "opened a pull request", "committed", GitHub links, branch names attributed to them)
- A data fix they explicitly performed (e.g., "Sean ran the SQL to restore...", "Jayson executed the data migration...")
- A configuration change or deploy they personally made (explicitly stated, not implied)

These DO NOT count — even if Sean or Jayson did them:
- Commenting on a ticket
- Being assigned to a ticket
- Asking or answering questions
- Investigating or reproducing an issue
- Communicating with customers (Intercom, Slack)
- Triaging, updating status, or moving tickets
- Being mentioned by someone else
- Proposing a solution without implementing it

If you cannot point to a specific PR, commit, data fix, or deploy BY NAME in the Shortcut Ticket Activity, return "None".

- "Responsible" = they authored the PR/commit/fix that resolved the issue (explicit proof required)
- "Assisted" = they authored a secondary technical contribution while someone else delivered the primary fix
- "None" = anything else (THIS IS THE DEFAULT — use this unless proof is undeniable)

Also rate your confidence in this decision from 1 to 5:
- 1 = Very uncertain, could easily go either way
- 2 = Somewhat uncertain, limited information
- 3 = Moderate confidence
- 4 = High confidence
- 5 = Very high confidence, clear-cut case

Respond with ONLY valid JSON in this exact format (no markdown, no code fences):
{{
  "decision": "L2 Can Support" or "L2 Cannot Support" or "Partially Supported" or "Insufficient Data",
  "category": "one category from the list above",
  "support_person": "Name of the support person handling the ticket, or Unknown",
  "l2_engineer": "Sean" or "Jayson" or "None",
  "l2_involvement": "Responsible" or "Assisted" or "None",
  "confidence": 1-5,
  "explanation": "A concise 2-3 sentence explanation of why L2 can or cannot handle this ticket, referencing specific L2 capabilities or gaps."
}}
"""

# ── Paths & Config ──────────────────────────────────────────────────────────
APP_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(APP_DIR, "l2_results.json")
OVERRIDES_FILE = os.path.join(APP_DIR, "l2_overrides.json")
HISTORY_DIR = os.path.join(APP_DIR, "history")
PROGRESS_FILE = os.path.join(APP_DIR, "analysis_progress.json")

GOOGLE_SHEET_ID = "1dRC3DkwOKjhdZveTp2xuSC_roeoxWOUcoP-XWsKQkeo"
GOOGLE_SHEET_TAB = "Tickets"
RESULTS_SHEET_TAB = "Results"
OVERRIDES_SHEET_TAB = "Overrides"
GOOGLE_SHEET_EMBED_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit?gid=0&rm=minimal"

# Service account credentials - check for file or Streamlit secrets
SERVICE_ACCOUNT_FILE = os.path.join(APP_DIR, "service_account.json")


def get_gspread_client():
    """Get authenticated gspread client."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    # Try env var first (for Railway), then Streamlit secrets, then local file
    creds = None
    env_creds = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if env_creds:
        creds_dict = json.loads(env_creds)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        try:
            if "gcp_service_account" in st.secrets:
                creds_dict = dict(st.secrets["gcp_service_account"])
                creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        except Exception:
            pass
    if creds is None and os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    if creds is None:
        return None
    return gspread.authorize(creds)


@st.cache_data(ttl=60)
def load_google_sheet():
    """Fetch the Google Sheet as a DataFrame. Cached for 60 seconds."""
    try:
        client = get_gspread_client()
        if client is None:
            return None
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet(GOOGLE_SHEET_TAB)
        rows = worksheet.get_all_values()
        if not rows:
            return None
        headers = rows[0]
        # Deduplicate headers by appending _2, _3, etc.
        seen = {}
        unique_headers = []
        for h in headers:
            if h in seen:
                seen[h] += 1
                unique_headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 1
                unique_headers.append(h)
        data = rows[1:]
        return pd.DataFrame(data, columns=unique_headers)
    except Exception as e:
        st.error(f"Google Sheet error: {e}")
        return None


def evaluate_ticket(client, name, description, intercom_transcript="", slack_transcript="", shortcut_activity=""):
    prompt = EVALUATION_PROMPT.format(
        capabilities=L2_CAPABILITIES,
        name=name or "(no name)",
        description=description or "(no description)",
        intercom_transcript=intercom_transcript or "(none)",
        slack_transcript=slack_transcript or "(none)",
        shortcut_activity=shortcut_activity or "(none)",
    )
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"decision": "Error", "category": "Other", "support_person": "Unknown", "l2_engineer": "None", "l2_involvement": "None", "confidence": 0, "explanation": f"Parse error: {text[:200]}"}


def color_decision(val):
    if val == "L2 Can Support":
        return "background-color: #0a3d1f; color: #00E676"
    elif val == "L2 Cannot Support":
        return "background-color: #3d0a0a; color: #ff5252"
    elif val == "Partially Supported":
        return "background-color: #3d3a0a; color: #FFD740"
    return ""


RESULTS_COLUMNS = ["name", "shortcut_url", "description", "decision", "category", "support_person",
                    "l2_engineer", "l2_involvement", "confidence", "explanation"]


def _get_or_create_worksheet(tab_name, headers=None):
    """Get a worksheet by name, creating it if it doesn't exist."""
    try:
        client = get_gspread_client()
        if client is None:
            return None, None
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        try:
            ws = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=20)
            if headers:
                ws.update("A1", [headers])
        return spreadsheet, ws
    except Exception:
        return None, None


def save_results_to_sheet(results):
    """Write all results to the Results tab in Google Sheets."""
    try:
        _, ws = _get_or_create_worksheet(RESULTS_SHEET_TAB, headers=RESULTS_COLUMNS)
        if ws is None:
            # Fall back to local file
            with open(RESULTS_FILE, "w") as f:
                json.dump(results, f, indent=2)
            return
        # Clear existing data and write fresh
        ws.clear()
        rows = [RESULTS_COLUMNS]
        for r in results:
            rows.append([str(r.get(col, "")) for col in RESULTS_COLUMNS])
        ws.update(f"A1:I{len(rows)}", rows)
    except Exception:
        # Fall back to local file
        with open(RESULTS_FILE, "w") as f:
            json.dump(results, f, indent=2)


def load_results_from_sheet():
    """Load results from the Results tab in Google Sheets."""
    try:
        _, ws = _get_or_create_worksheet(RESULTS_SHEET_TAB, headers=RESULTS_COLUMNS)
        if ws is None:
            return None
        data = ws.get_all_records()
        if not data:
            return None
        df = pd.DataFrame(data)
        # Convert confidence to numeric
        if "confidence" in df.columns:
            df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        return None


def load_results():
    """Load results from Google Sheet, falling back to local file."""
    df = load_results_from_sheet()
    if df is not None and not df.empty:
        for col, default in [("category", "Other"), ("confidence", 0), ("support_person", "Unknown"),
                              ("l2_engineer", "None"), ("l2_involvement", "None"), ("shortcut_url", "")]:
            if col not in df.columns:
                df[col] = default
        return df
    # Fallback to local file
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        for col, default in [("category", "Other"), ("confidence", 0), ("support_person", "Unknown"),
                              ("l2_engineer", "None"), ("l2_involvement", "None"), ("shortcut_url", "")]:
            if col not in df.columns:
                df[col] = default
        return df
    return None


OVERRIDES_COLUMNS = ["name", "corrected_decision", "reason", "timestamp", "original_decision"]


def load_overrides():
    """Load overrides from Google Sheet, falling back to local file."""
    try:
        _, ws = _get_or_create_worksheet(OVERRIDES_SHEET_TAB, headers=OVERRIDES_COLUMNS)
        if ws is not None:
            data = ws.get_all_records()
            if data:
                return {r["name"]: r for r in data}
    except Exception:
        pass
    if os.path.exists(OVERRIDES_FILE):
        with open(OVERRIDES_FILE) as f:
            return json.load(f)
    return {}


def save_overrides(overrides):
    """Save overrides to Google Sheet and local file."""
    # Local file as backup
    with open(OVERRIDES_FILE, "w") as f:
        json.dump(overrides, f, indent=2)
    # Google Sheet
    try:
        _, ws = _get_or_create_worksheet(OVERRIDES_SHEET_TAB, headers=OVERRIDES_COLUMNS)
        if ws is not None:
            ws.clear()
            rows = [OVERRIDES_COLUMNS]
            for name, ov in overrides.items():
                rows.append([name, ov.get("corrected_decision", ""), ov.get("reason", ""),
                             ov.get("timestamp", ""), ov.get("original_decision", "")])
            ws.update(f"A1:E{len(rows)}", rows)
    except Exception:
        pass


def save_history_snapshot(results):
    """Save a timestamped snapshot for trend tracking."""
    os.makedirs(HISTORY_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(HISTORY_DIR, f"run_{ts}.json")
    with open(path, "w") as f:
        json.dump(results, f, indent=2)


def load_history():
    """Load all historical snapshots for trend tracking."""
    if not os.path.exists(HISTORY_DIR):
        return []
    snapshots = []
    for fname in sorted(os.listdir(HISTORY_DIR)):
        if fname.endswith(".json"):
            fpath = os.path.join(HISTORY_DIR, fname)
            ts_str = fname.replace("run_", "").replace(".json", "")
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S")
            except ValueError:
                continue
            with open(fpath) as f:
                data = json.load(f)
            total = len(data)
            supported = sum(1 for r in data if r.get("decision") == "L2 Can Support")
            unsupported = sum(1 for r in data if r.get("decision") == "L2 Cannot Support")
            partial = sum(1 for r in data if r.get("decision") == "Partially Supported")
            snapshots.append({
                "date": ts,
                "total": total,
                "L2 Can Support": supported,
                "L2 Cannot Support": unsupported,
                "Partially Supported": partial,
                "L2 Coverage %": round(supported / total * 100, 1) if total > 0 else 0,
            })
    return snapshots


def get_analysis_progress():
    """Read the current analysis progress from file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None


def set_analysis_progress(current, total, name, status="running"):
    """Write analysis progress to file."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump({
            "status": status,
            "current": current,
            "total": total,
            "ticket_name": name,
            "updated_at": datetime.now().isoformat(),
        }, f)


def clear_analysis_progress():
    """Remove the progress file when done."""
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


def run_analysis_background(rows, existing_results, rerun_all):
    """Run analysis in a background thread, saving results to Google Sheets."""
    try:
        client = Anthropic()
        analyzed_names = {r["name"] for r in existing_results}

        if rerun_all:
            new_rows = rows
            base_results = []
        else:
            new_rows = [r for r in rows if r.get("name", "").strip() not in analyzed_names]
            base_results = list(existing_results)

        if not new_rows:
            set_analysis_progress(0, 0, "", status="complete")
            return

        new_results = []
        for i, row in enumerate(new_rows):
            name = row.get("name", "").strip()
            shortcut_id = row.get("id", "").strip()
            shortcut_url = f"https://app.shortcut.com/fieldguide/story/{shortcut_id}" if shortcut_id else ""
            desc = row.get("description", "").strip()
            intercom_transcript = (row.get("Intercom Transcription", "") or row.get("Intercom Transcript", "")).strip()
            slack_transcript = (row.get("Slack Transcript", "") or row.get("Slack Conversation Transcript", "")).strip()
            shortcut_activity = row.get("Shortcut Ticket Activity", "").strip()

            set_analysis_progress(i + 1, len(new_rows), name)

            result = evaluate_ticket(client, name, desc, intercom_transcript, slack_transcript, shortcut_activity)
            new_results.append({
                "name": name,
                "shortcut_url": shortcut_url,
                "description": desc[:200],
                "decision": result.get("decision", "Error"),
                "category": result.get("category", "Other"),
                "support_person": result.get("support_person", "Unknown"),
                "l2_engineer": result.get("l2_engineer", "None"),
                "l2_involvement": result.get("l2_involvement", "None"),
                "confidence": result.get("confidence", 0),
                "explanation": result.get("explanation", ""),
            })

            # Save incrementally every 5 tickets (and on last)
            if (i + 1) % 5 == 0 or i == len(new_rows) - 1:
                all_results = base_results + new_results
                save_results_to_sheet(all_results)
                # Also save local as backup
                with open(RESULTS_FILE, "w") as f:
                    json.dump(all_results, f, indent=2)

            if i < len(new_rows) - 1:
                time.sleep(0.5)

        # Final save & snapshot
        all_results = base_results + new_results
        save_results_to_sheet(all_results)
        with open(RESULTS_FILE, "w") as f:
            json.dump(all_results, f, indent=2)
        save_history_snapshot(all_results)

        set_analysis_progress(len(new_rows), len(new_rows), "", status="complete")
    except Exception as e:
        set_analysis_progress(0, 0, str(e), status="error")


# ── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Escalation Tracker", page_icon="logo.svg", layout="wide")

# ── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #2D333B; }
    .header-container {
        display: flex; align-items: center; gap: 16px; padding: 0.5rem 0 1.5rem 0;
    }
    .header-container img { width: 48px; height: 48px; }
    .header-container h1 { color: #00E676; margin: 0; font-size: 2rem; }
    .header-subtitle { color: #9E9E9E; font-size: 0.95rem; margin-top: -8px; padding-bottom: 1rem; }

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
    .stDataFrame [data-testid="stDataFrameResizable"] { width: 100% !important; min-width: 0 !important; }

    /* Category stats cards */
    .cat-stat-card {
        background-color: #373E47; border: 1px solid #444C56; border-radius: 8px;
        padding: 12px 16px; margin-bottom: 8px;
    }
    .cat-stat-card .cat-name { color: #00E676; font-weight: 600; font-size: 0.9rem; }
    .cat-stat-card .cat-detail { color: #9E9E9E; font-size: 0.8rem; }

    /* Progress banner */
    .progress-banner {
        background-color: #1A2F1A; border: 1px solid #00E676; border-radius: 8px;
        padding: 12px 20px; margin-bottom: 16px;
    }
    .progress-banner .progress-text { color: #00E676; font-weight: 600; }

    /* Override badge */
    .override-badge {
        display: inline-block; background-color: #FFD740; color: #2D333B;
        font-size: 0.7rem; font-weight: 700; padding: 2px 8px; border-radius: 4px;
    }

    /* Confidence stars */
    .confidence-stars { color: #00E676; letter-spacing: 2px; }
    .confidence-dim { color: #444C56; }
</style>
""", unsafe_allow_html=True)

# ── Header ──────────────────────────────────────────────────────────────────
logo_path = os.path.join(APP_DIR, "logo.svg")
if os.path.exists(logo_path):
    with open(logo_path, "r") as f:
        logo_svg = f.read()
    logo_b64 = base64.b64encode(logo_svg.encode()).decode()
    st.markdown(f"""
    <div class="header-container">
        <img src="data:image/svg+xml;base64,{logo_b64}" />
        <h1>Escalation Tracker</h1>
    </div>
    """, unsafe_allow_html=True)
else:
    st.title("Escalation Tracker")

st.markdown('<div class="header-subtitle">Engineering escalation tracking &amp; L2 capability analysis</div>', unsafe_allow_html=True)

# ── Analysis progress banner (file-based, survives refresh) ────────────────
analysis_progress = get_analysis_progress()
if analysis_progress and analysis_progress.get("status") == "running":
    prog_current = analysis_progress.get("current", 0)
    prog_total = analysis_progress.get("total", 1)
    prog_name = analysis_progress.get("ticket_name", "")
    prog_pct = prog_current / prog_total if prog_total > 0 else 0

    st.markdown(f"""
    <div class="progress-banner">
        <span class="progress-text">Analysis in progress: [{prog_current}/{prog_total}] {prog_name[:60]}</span>
    </div>
    """, unsafe_allow_html=True)
    st.progress(prog_pct)
    # Auto-refresh every 3 seconds to update progress
    st.markdown("""<meta http-equiv="refresh" content="3">""", unsafe_allow_html=True)
elif analysis_progress and analysis_progress.get("status") == "complete":
    prog_total = analysis_progress.get("total", 0)
    if prog_total > 0:
        st.success(f"Analysis complete: {prog_total} tickets processed.")
    clear_analysis_progress()

# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    if os.path.exists(logo_path):
        st.image(logo_path, width=60)
    st.header("L2 Capabilities")
    st.markdown("""
    1. Data Restores
    2. Small Code Changes
    3. Account Access Issues
    4. Configuration Changes
    5. Data Exports / Imports
    6. User Management
    7. Basic Troubleshooting
    8. Database Queries
    9. Runbook Execution
    10. Integration Support
    11. Cache / Queue Mgmt
    12. Deployment Support
    13. Customer Communication
    """)

# ── Tabs ────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab5 = st.tabs(["Results", "Run Analysis", "Trends", "Google Sheet"])

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1: RESULTS
# ═══════════════════════════════════════════════════════════════════════════
with tab1:
    results_df = load_results()
    overrides = load_overrides()

    if results_df is not None and not results_df.empty:
        # ── Metric filter state ──────────────────────────────────────
        if "metric_filter" not in st.session_state:
            st.session_state.metric_filter = None

        # ── Summary metrics ─────────────────────────────────────────
        total = len(results_df)
        supported = len(results_df[results_df["decision"] == "L2 Can Support"])
        unsupported = len(results_df[results_df["decision"] == "L2 Cannot Support"])
        partial = len(results_df[results_df["decision"] == "Partially Supported"])
        insufficient = len(results_df[results_df["decision"] == "Insufficient Data"])
        avg_conf = results_df[results_df["decision"] != "Insufficient Data"]["confidence"].mean() if "confidence" in results_df.columns else 0

        l2_involved = results_df[results_df["l2_involvement"] != "None"]
        l2_responsible = results_df[results_df["l2_involvement"] == "Responsible"]
        l2_assisted = results_df[results_df["l2_involvement"] == "Assisted"]
        sean_tickets = results_df[results_df["l2_engineer"] == "Sean"]
        jayson_tickets = results_df[results_df["l2_engineer"] == "Jayson"]
        could_but_didnt_df = results_df[
            (results_df["decision"] == "L2 Can Support") &
            (results_df["l2_involvement"] == "None")
        ]

        # ── Metric card + drill-down style overrides ─────────────────
        st.markdown("""
        <style>
            /* All metric cards same height */
            [data-testid="stMetric"] {
                min-height: 120px !important;
                display: flex !important;
                flex-direction: column !important;
                justify-content: space-between !important;
                padding-bottom: 8px !important;
            }
            /* Metric label */
            [data-testid="stMetricLabel"] label, [data-testid="stMetricLabel"] p {
                font-size: 14px !important;
                font-weight: 700 !important;
            }
            /* Metric value */
            [data-testid="stMetricValue"] {
                font-size: 20px !important;
            }
        </style>
        """, unsafe_allow_html=True)

        def metric_card_html(label, value, delta=None, delta_color="normal"):
            """Return HTML string for a metric card."""
            parts = []
            parts.append('<div style="background-color:#373E47;border:1px solid #444C56;border-radius:10px;padding:14px;height:130px;display:flex;flex-direction:column;justify-content:space-between;">')
            parts.append(f'<div style="color:#9E9E9E;font-size:16px;font-weight:700;">{label}</div>')
            parts.append(f'<div style="color:#E0E0E0;font-size:28px;font-weight:700;">{value}</div>')
            if delta:
                c = "#00E676" if delta_color == "normal" else "#ff5252"
                parts.append(f'<span style="color:{c};font-size:12px;">&#8593; {delta}</span>')
            else:
                parts.append('<span style="font-size:12px;visibility:hidden;">&#8593; 0%</span>')
            parts.append('<div style="color:#636b75;font-size:8px;">&#8595; drill down</div>')
            parts.append('</div>')
            return "".join(parts)

        # Style trigger buttons to overlay the card
        st.markdown("""<style>
            div[data-testid="stButton"] button[kind="secondary"] {
                position:relative !important;
                margin-top:-140px !important;
                height:140px !important;
                min-height:140px !important;
                width:100% !important;
                opacity:0 !important;
                cursor:pointer !important;
                border:none !important;
                padding:0 !important;
            }
        </style>""", unsafe_allow_html=True)

        # ── Row 1: L2 Capability Assessment ───────────────────────────
        st.markdown("**L2 Capability Assessment**")
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            st.markdown(metric_card_html("Total Escalations", total), unsafe_allow_html=True)
            if st.button("x", key="btn_total", use_container_width=True):
                st.session_state.metric_filter = None
                st.rerun()
        with col2:
            pct = f"{supported/total*100:.0f}%" if total > 0 else "0%"
            st.markdown(metric_card_html("L2 Can Support", supported, delta=pct), unsafe_allow_html=True)
            if st.button("x", key="btn_supported", use_container_width=True):
                st.session_state.metric_filter = ("decision", "L2 Can Support")
                st.rerun()
        with col3:
            pct = f"{unsupported/total*100:.0f}%" if total > 0 else "0%"
            st.markdown(metric_card_html("L2 Cannot Support", unsupported, delta=pct, delta_color="inverse"), unsafe_allow_html=True)
            if st.button("x", key="btn_unsupported", use_container_width=True):
                st.session_state.metric_filter = ("decision", "L2 Cannot Support")
                st.rerun()
        with col4:
            st.markdown(metric_card_html("Partially Supported", partial), unsafe_allow_html=True)
            if st.button("x", key="btn_partial", use_container_width=True):
                st.session_state.metric_filter = ("decision", "Partially Supported")
                st.rerun()
        with col5:
            st.markdown(metric_card_html("Insufficient Data", insufficient), unsafe_allow_html=True)
            if st.button("x", key="btn_insufficient", use_container_width=True):
                st.session_state.metric_filter = ("decision", "Insufficient Data")
                st.rerun()
        with col6:
            html = metric_card_html("Avg Confidence", f"{avg_conf:.1f}/5")
            # Remove drill down line for non-clickable card
            html = html.replace("&#8595; drill down", "")
            st.markdown(html, unsafe_allow_html=True)

        # ── Row 2: Actual L2 involvement ──────────────────────────────
        st.markdown("**Actual L2 Engineer Involvement**")
        l2_col1, l2_col2, l2_col3, l2_col4, l2_col5 = st.columns(5)

        with l2_col1:
            pct = f"{len(l2_involved)/total*100:.0f}%" if total > 0 else "0%"
            st.markdown(metric_card_html("L2 Involved", len(l2_involved), delta=pct), unsafe_allow_html=True)
            if st.button("x", key="btn_l2_involved", use_container_width=True):
                st.session_state.metric_filter = ("l2_involvement", "!=None")
                st.rerun()
        with l2_col2:
            st.markdown(metric_card_html("L2 Responsible", len(l2_responsible)), unsafe_allow_html=True)
            if st.button("x", key="btn_l2_responsible", use_container_width=True):
                st.session_state.metric_filter = ("l2_involvement", "Responsible")
                st.rerun()
        with l2_col3:
            st.markdown(metric_card_html("L2 Assisted", len(l2_assisted)), unsafe_allow_html=True)
            if st.button("x", key="btn_l2_assisted", use_container_width=True):
                st.session_state.metric_filter = ("l2_involvement", "Assisted")
                st.rerun()
        with l2_col4:
            st.markdown(metric_card_html("Sean", len(sean_tickets)), unsafe_allow_html=True)
            if st.button("x", key="btn_sean", use_container_width=True):
                st.session_state.metric_filter = ("l2_engineer", "Sean")
                st.rerun()
        with l2_col5:
            st.markdown(metric_card_html("Jayson", len(jayson_tickets)), unsafe_allow_html=True)
            if st.button("x", key="btn_jayson", use_container_width=True):
                st.session_state.metric_filter = ("l2_engineer", "Jayson")
                st.rerun()

        # ── Gap analysis ──────────────────────────────────────────────
        if len(could_but_didnt_df) > 0:
            gap_col1, gap_col2 = st.columns([5, 1])
            with gap_col1:
                st.info(f"**Gap:** {len(could_but_didnt_df)} tickets L2 *could* have supported but had no L2 involvement")
            with gap_col2:
                if st.button("↓ drill down", key="btn_gap", use_container_width=True):
                    st.session_state.metric_filter = ("gap", "could_but_didnt")
                    st.rerun()

        # ── Active filter indicator ──────────────────────────────────
        if st.session_state.metric_filter is not None:
            filt = st.session_state.metric_filter
            if filt[0] == "gap":
                label = "Gap: Could support but no L2 involvement"
            elif filt[1] == "!=None":
                label = "L2 Involved (any)"
            else:
                label = f"{filt[0]}: {filt[1]}"
            filter_col1, filter_col2 = st.columns([5, 1])
            with filter_col1:
                st.warning(f"Filtered by: **{label}**")
            with filter_col2:
                if st.button("Clear Filter", key="btn_clear_filter", use_container_width=True):
                    st.session_state.metric_filter = None
                    st.rerun()

        # ── Comparison with human labels ────────────────────────────────
        if "human_decision" in results_df.columns or overrides:
            override_count = len(overrides)
            if override_count > 0:
                agree = sum(1 for name, ov in overrides.items() if
                            not results_df[results_df["name"] == name].empty and
                            results_df[results_df["name"] == name].iloc[0]["decision"] == ov.get("corrected_decision"))
                st.markdown(f"**Reviewer Overrides:** {override_count} tickets reviewed | "
                            f"**Agreement rate:** {agree}/{override_count} "
                            f"({agree/override_count*100:.0f}% agree with model)")

        st.divider()

        # ── Search, Filters & Sort (collapsed by default) ──────────────
        search_query = ""
        filter_option = "All"
        filter_category = "All"
        confidence_filter = "All"
        sort_by = "name"
        sort_order = "Ascending"

        with st.expander("Search, Filter & Sort", expanded=False):
            search_query = st.text_input("Search tickets (name or description):", placeholder="Type to search...")

            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                filter_option = st.selectbox(
                    "Filter by decision:",
                    ["All", "L2 Can Support", "L2 Cannot Support", "Partially Supported", "Insufficient Data"],
                )
            with col_f2:
                categories = ["All"] + sorted([str(c) for c in results_df["category"].dropna().unique().tolist()])
                filter_category = st.selectbox("Filter by category:", categories)
            with col_f3:
                confidence_filter = st.selectbox(
                    "Filter by confidence:",
                    ["All", "Low (1-2)", "Medium (3)", "High (4-5)"],
                )

            sort_col1, sort_col2 = st.columns([2, 1])
            with sort_col1:
                sort_by = st.selectbox("Sort by:", ["name", "decision", "category", "confidence"])
            with sort_col2:
                sort_order = st.selectbox("Order:", ["Ascending", "Descending"])

        # Exclude Insufficient Data unless explicitly drilling into it
        mf = st.session_state.get("metric_filter")
        if mf is not None and mf == ("decision", "Insufficient Data"):
            filtered = results_df.copy()
        else:
            filtered = results_df[results_df["decision"] != "Insufficient Data"].copy()

        # Apply metric card filter
        if mf is not None:
            if mf[0] == "gap":
                filtered = filtered[
                    (filtered["decision"] == "L2 Can Support") &
                    (filtered["l2_involvement"] == "None")
                ]
            elif mf[1] == "!=None":
                filtered = filtered[filtered[mf[0]] != "None"]
            else:
                filtered = filtered[filtered[mf[0]] == mf[1]]

        if search_query:
            mask = (
                filtered["name"].str.contains(search_query, case=False, na=False) |
                filtered["description"].str.contains(search_query, case=False, na=False)
            )
            filtered = filtered[mask]
        if filter_option != "All":
            filtered = filtered[filtered["decision"] == filter_option]
        if filter_category != "All":
            filtered = filtered[filtered["category"] == filter_category]
        if confidence_filter == "Low (1-2)":
            filtered = filtered[filtered["confidence"] <= 2]
        elif confidence_filter == "Medium (3)":
            filtered = filtered[filtered["confidence"] == 3]
        elif confidence_filter == "High (4-5)":
            filtered = filtered[filtered["confidence"] >= 4]

        st.markdown(f"**Showing {len(filtered)} of {total} tickets**")
        filtered = filtered.sort_values(
            by=sort_by,
            ascending=(sort_order == "Ascending")
        ).reset_index(drop=True)

        # ── Pagination ──────────────────────────────────────────────────
        ROWS_PER_PAGE = 50
        if "current_page" not in st.session_state:
            st.session_state.current_page = 1
        total_pages = max(1, (len(filtered) + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE)
        # Clamp page if filters reduced total
        if st.session_state.current_page > total_pages:
            st.session_state.current_page = 1

        start_idx = (st.session_state.current_page - 1) * ROWS_PER_PAGE
        end_idx = min(start_idx + ROWS_PER_PAGE, len(filtered))
        page_df = filtered.iloc[start_idx:end_idx]

        # ── Clickable table ────────────────────────────────────────────
        display_cols = ["name", "support_person", "category", "decision", "l2_engineer", "l2_involvement", "confidence"]
        available_cols = [c for c in display_cols if c in page_df.columns]

        # Build display table
        styled_page = page_df[available_cols].copy()

        # Add shortcut link column from URL
        has_urls = "shortcut_url" in page_df.columns and page_df["shortcut_url"].str.startswith("http").any()
        if has_urls:
            styled_page.insert(0, "link", page_df["shortcut_url"])

        styled_page["decision"] = styled_page["decision"].map({
            "L2 Can Support": "\u2705 L2 Can Support",
            "L2 Cannot Support": "\u274c L2 Cannot Support",
            "Partially Supported": "\u26a0\ufe0f Partially Supported",
            "Insufficient Data": "\u2753 Insufficient Data",
        }).fillna(styled_page.get("decision", ""))

        col_config = {}
        if has_urls:
            col_config["link"] = st.column_config.LinkColumn(
                "Open",
                display_text="Open",
                width="small",
            )

        selection = st.dataframe(
            styled_page,
            use_container_width=True,
            height=600,
            on_select="rerun",
            selection_mode="single-row",
            column_config=col_config,
        )

        # ── Pagination arrows below table ──────────────────────────────
        pg_left, pg_info, pg_right = st.columns([1, 2, 1])
        with pg_left:
            if st.button("< Previous", disabled=(st.session_state.current_page <= 1)):
                st.session_state.current_page -= 1
                st.rerun()
        with pg_info:
            st.markdown(f"<div style='text-align:center; color:#9E9E9E;'>Page {st.session_state.current_page} of {total_pages} &nbsp;|&nbsp; {start_idx+1}-{end_idx} of {len(filtered)} results</div>", unsafe_allow_html=True)
        with pg_right:
            if st.button("Next >", disabled=(st.session_state.current_page >= total_pages)):
                st.session_state.current_page += 1
                st.rerun()

        # Update selected ticket from row click
        if selection and selection.selection and selection.selection.rows:
            clicked_idx = selection.selection.rows[0]
            if clicked_idx < len(page_df):
                st.session_state["detail_ticket"] = page_df.iloc[clicked_idx]["name"]

        # ── Detail view with override ──────────────────────────────────
        st.divider()
        st.subheader("Ticket Detail View")
        ticket_names = filtered["name"].tolist()
        if ticket_names:
            # Determine which ticket to show
            detail_ticket = st.session_state.get("detail_ticket", None)
            if detail_ticket and detail_ticket in ticket_names:
                default_idx = ticket_names.index(detail_ticket)
            else:
                default_idx = 0

            def on_select_change():
                st.session_state["detail_ticket"] = st.session_state["detail_select_widget"]

            selected = st.selectbox(
                "Select a ticket:",
                ticket_names,
                index=default_idx,
                key="detail_select_widget",
                on_change=on_select_change,
            )

            # Always use session state as source of truth
            if detail_ticket and detail_ticket in ticket_names:
                selected = detail_ticket
            row = filtered[filtered["name"] == selected].iloc[0]

            sc_url = row.get("shortcut_url", "")
            if sc_url:
                st.markdown(f"### [{selected}]({sc_url})")
            else:
                st.markdown(f"### {selected}")

            col_left, col_mid, col_right = st.columns([1, 1, 2])
            with col_left:
                decision = row["decision"]
                if decision == "L2 Can Support":
                    st.success(f"**{decision}**")
                elif decision == "L2 Cannot Support":
                    st.error(f"**{decision}**")
                elif decision == "Insufficient Data":
                    st.caption(f"**{decision}**")
                else:
                    st.warning(f"**{decision}**")

                # L2 involvement badge
                l2_eng = row.get("l2_engineer", "None")
                l2_inv = row.get("l2_involvement", "None")
                if l2_inv == "Responsible":
                    st.success(f"**L2: {l2_eng}** — Responsible")
                elif l2_inv == "Assisted":
                    st.warning(f"**L2: {l2_eng}** — Assisted")
                else:
                    st.caption("No L2 involvement detected")

            with col_mid:
                try:
                    conf = int(float(row.get("confidence", 0) or 0))
                except (ValueError, TypeError):
                    conf = 0
                stars = "★" * conf + "☆" * (5 - conf)
                st.markdown(f"**Confidence:** <span class='confidence-stars'>{stars}</span> ({conf}/5)", unsafe_allow_html=True)
                st.markdown(f"**Category:** {row.get('category', 'Other')}")
                st.markdown(f"**Support Person:** {row.get('support_person', 'Unknown')}")

            with col_right:
                st.markdown(f"**Explanation:** {row['explanation']}")

            # ── Manual L2 involvement tagging ──────────────────────────
            with st.expander("Tag L2 Involvement"):
                tag_col1, tag_col2 = st.columns(2)
                with tag_col1:
                    tag_engineer = st.selectbox(
                        "L2 Engineer:",
                        ["None", "Sean", "Jayson"],
                        index=["None", "Sean", "Jayson"].index(row.get("l2_engineer", "None")) if row.get("l2_engineer", "None") in ["None", "Sean", "Jayson"] else 0,
                        key=f"tag_eng_{selected}",
                    )
                with tag_col2:
                    tag_involvement = st.selectbox(
                        "Involvement:",
                        ["None", "Responsible", "Assisted"],
                        index=["None", "Responsible", "Assisted"].index(row.get("l2_involvement", "None")) if row.get("l2_involvement", "None") in ["None", "Responsible", "Assisted"] else 0,
                        key=f"tag_inv_{selected}",
                    )
                if st.button("Save L2 Tag", key=f"save_tag_{selected}"):
                    # Load current results, update, and save to sheet + local
                    all_results = []
                    if os.path.exists(RESULTS_FILE):
                        with open(RESULTS_FILE) as f:
                            all_results = json.load(f)
                    for r in all_results:
                        if r["name"] == selected:
                            r["l2_engineer"] = tag_engineer
                            r["l2_involvement"] = tag_involvement
                            break
                    save_results_to_sheet(all_results)
                    with open(RESULTS_FILE, "w") as f:
                        json.dump(all_results, f, indent=2)
                    st.success(f"Saved: {tag_engineer} — {tag_involvement}")
                    st.rerun()

            if "description" in row and row["description"]:
                with st.expander("Full Description"):
                    st.text(row["description"])

            # Override / Disagree
            existing_override = overrides.get(selected, {})
            with st.expander("Disagree? Override this decision" + (" (override exists)" if existing_override else "")):
                if existing_override:
                    st.info(f"Current override: **{existing_override.get('corrected_decision')}** — \"{existing_override.get('reason', '')}\"")

                override_decision = st.selectbox(
                    "Your corrected decision:",
                    ["L2 Can Support", "L2 Cannot Support", "Partially Supported"],
                    key=f"override_{selected}",
                )
                override_reason = st.text_input("Reason for override:", key=f"reason_{selected}")
                if st.button("Save Override", key=f"btn_{selected}"):
                    overrides[selected] = {
                        "corrected_decision": override_decision,
                        "reason": override_reason,
                        "timestamp": datetime.now().isoformat(),
                        "original_decision": decision,
                    }
                    save_overrides(overrides)
                    st.success("Override saved.")

        # ── Downloads ───────────────────────────────────────────────────
        st.divider()
        dl_col1, dl_col2, dl_col3 = st.columns(3)
        with dl_col1:
            csv_all = filtered.to_csv(index=False)
            st.download_button(
                label="Download Filtered Results (CSV)",
                data=csv_all,
                file_name="l2_analysis_filtered.csv",
                mime="text/csv",
            )
        with dl_col2:
            cannot_support = results_df[results_df["decision"] == "L2 Cannot Support"].to_csv(index=False)
            st.download_button(
                label="Download 'Cannot Support' Only",
                data=cannot_support,
                file_name="l2_cannot_support.csv",
                mime="text/csv",
            )
        with dl_col3:
            if overrides:
                overrides_csv = pd.DataFrame([
                    {"name": k, **v} for k, v in overrides.items()
                ]).to_csv(index=False)
                st.download_button(
                    label="Download Overrides (CSV)",
                    data=overrides_csv,
                    file_name="l2_overrides.csv",
                    mime="text/csv",
                )
    else:
        st.info("No results found yet. Go to the **Run Analysis** tab to process tickets.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2: RUN ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Run New Analysis")

    # Check if analysis is already running
    current_progress = get_analysis_progress()
    is_running = current_progress and current_progress.get("status") == "running"

    if is_running:
        prog = current_progress
        st.warning(f"Analysis is already running: [{prog['current']}/{prog['total']}] {prog['ticket_name'][:60]}")
        st.progress(prog["current"] / prog["total"] if prog["total"] > 0 else 0)
        if st.button("Cancel Analysis"):
            clear_analysis_progress()
            st.rerun()
    else:
        max_rows = st.number_input("Max rows to process (0 = all)", min_value=0, value=10, step=5)

        rerun_all = st.checkbox("Re-analyze all tickets (ignore previous results)", value=False)

        data_source = st.radio(
            "Data source:",
            ["Google Sheet (live in railway)", "Upload CSV"],
            horizontal=True,
        )

        uploaded = None
        if data_source == "Upload CSV":
            uploaded = st.file_uploader("Upload a CSV file with ticket data", type=["csv"])

        if data_source == "Google Sheet (live in railway)":
            sheet_df = load_google_sheet()
            if sheet_df is not None:
                st.success(f"Connected to Google Sheet: **{len(sheet_df)} rows** found")
                with st.expander("Preview sheet data"):
                    st.dataframe(sheet_df.head(5), use_container_width=True)
            else:
                st.error("Could not connect to Google Sheet. Make sure the sheet is shared with the service account.")

        if st.button("Run Analysis", type="primary"):
            rows = []
            if data_source == "Google Sheet (live in railway)":
                sheet_df = load_google_sheet()
                if sheet_df is not None:
                    rows = sheet_df.to_dict("records")
                else:
                    st.error("Could not load Google Sheet.")
            elif uploaded:
                raw = uploaded.read().decode("utf-8-sig")
                reader = csv.DictReader(raw.splitlines())
                rows = list(reader)
            else:
                st.error("No file provided.")

            if rows:
                if max_rows > 0:
                    rows = rows[:max_rows]

                # Load existing results from sheet (or local fallback)
                existing_results = []
                if not rerun_all:
                    df_existing = load_results()
                    if df_existing is not None and not df_existing.empty:
                        existing_results = df_existing.to_dict("records")

                # Check how many are new
                analyzed_names = {r["name"] for r in existing_results}
                if rerun_all:
                    new_count = len(rows)
                else:
                    new_count = sum(1 for r in rows if r.get("name", "").strip() not in analyzed_names)

                if new_count == 0 and not rerun_all:
                    st.info("All tickets have already been analyzed. Check 'Re-analyze all' to rerun.")
                else:
                    # Launch background thread
                    set_analysis_progress(0, new_count, "Starting...", status="running")
                    thread = threading.Thread(
                        target=run_analysis_background,
                        args=(rows, existing_results, rerun_all),
                        daemon=True,
                    )
                    thread.start()
                    st.success(f"Analysis started in background: {new_count} tickets to process. You can refresh or switch tabs — progress is shown at the top of the page.")
                    time.sleep(1)
                    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3: TRENDS
# ═══════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Trends & Insights")

    trends_df = load_results()
    if trends_df is not None and not trends_df.empty:
        if "category" not in trends_df.columns:
            trends_df["category"] = "Other"
        if "l2_engineer" not in trends_df.columns:
            trends_df["l2_engineer"] = "None"
        if "l2_involvement" not in trends_df.columns:
            trends_df["l2_involvement"] = "None"
        if "confidence" not in trends_df.columns:
            trends_df["confidence"] = 0
        if "support_person" not in trends_df.columns:
            trends_df["support_person"] = "Unknown"

        # Exclude insufficient data for most charts
        valid_df = trends_df[trends_df["decision"] != "Insufficient Data"]

        # ── Row 1: Decision & Category charts ─────────────────────────
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            st.markdown("**Decision Distribution**")
            decision_counts = trends_df["decision"].value_counts()
            chart_data = pd.DataFrame({
                "Decision": decision_counts.index,
                "Count": decision_counts.values
            })
            st.bar_chart(chart_data, x="Decision", y="Count", color="#00E676")

        with chart_col2:
            st.markdown("**Category Breakdown**")
            cat_counts = valid_df["category"].value_counts()
            chart_data2 = pd.DataFrame({
                "Category": cat_counts.index,
                "Count": cat_counts.values
            })
            st.bar_chart(chart_data2, x="Category", y="Count", color="#00E676")

        st.divider()

        # ── Row 2: L2 Engineer & Support Person charts ────────────────
        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
            st.markdown("**L2 Engineer Workload**")
            l2_active = valid_df[valid_df["l2_engineer"] != "None"]
            if not l2_active.empty:
                eng_counts = l2_active["l2_engineer"].value_counts()
                eng_data = pd.DataFrame({
                    "Engineer": eng_counts.index,
                    "Tickets": eng_counts.values
                })
                st.bar_chart(eng_data, x="Engineer", y="Tickets", color="#00E676")

                # Breakdown by involvement type
                eng_inv = l2_active.groupby(["l2_engineer", "l2_involvement"]).size().reset_index(name="count")
                st.dataframe(eng_inv, use_container_width=True, hide_index=True)
            else:
                st.caption("No L2 engineer involvement data yet.")

        with chart_col4:
            st.markdown("**Top Support Persons (by ticket count)**")
            sp_counts = valid_df[valid_df["support_person"] != "Unknown"]["support_person"].value_counts().head(10)
            if not sp_counts.empty:
                sp_data = pd.DataFrame({
                    "Support Person": sp_counts.index,
                    "Tickets": sp_counts.values
                })
                st.bar_chart(sp_data, x="Support Person", y="Tickets", color="#00E676")
            else:
                st.caption("No support person data yet.")

        st.divider()

        # ── Row 3: Capability vs Actual & Confidence ──────────────────
        chart_col5, chart_col6 = st.columns(2)

        with chart_col5:
            st.markdown("**L2 Potential vs Actual**")
            could_support = len(valid_df[valid_df["decision"] == "L2 Can Support"])
            actually_handled = len(valid_df[valid_df["l2_involvement"] != "None"])
            gap_data = pd.DataFrame({
                "Metric": ["Could Support", "Actually Handled", "Gap"],
                "Count": [could_support, actually_handled, max(0, could_support - actually_handled)]
            })
            st.bar_chart(gap_data, x="Metric", y="Count", color="#00E676")

        with chart_col6:
            st.markdown("**Confidence Distribution**")
            conf_valid = valid_df[valid_df["confidence"] > 0]
            if not conf_valid.empty:
                conf_counts = conf_valid["confidence"].value_counts().sort_index()
                conf_data = pd.DataFrame({
                    "Confidence": conf_counts.index.astype(str),
                    "Count": conf_counts.values
                })
                st.bar_chart(conf_data, x="Confidence", y="Count", color="#00E676")
            else:
                st.caption("No confidence data yet.")

        st.divider()

        # ── Category Summary Stats (full view, not collapsed) ─────────
        st.subheader("Category Summary")
        for cat in sorted([str(c) for c in valid_df["category"].dropna().unique()]):
            cat_df = valid_df[valid_df["category"] == cat]
            cat_total = len(cat_df)
            cat_supported = len(cat_df[cat_df["decision"] == "L2 Can Support"])
            cat_unsupported = len(cat_df[cat_df["decision"] == "L2 Cannot Support"])
            cat_partial = len(cat_df[cat_df["decision"] == "Partially Supported"])
            cat_l2_handled = len(cat_df[cat_df["l2_involvement"] != "None"])
            cat_avg_conf = cat_df["confidence"].mean() if not cat_df.empty else 0
            st.markdown(f"""
            <div class="cat-stat-card">
                <div class="cat-name">{cat} ({cat_total} tickets)</div>
                <div class="cat-detail">
                    Can Support: {cat_supported} | Cannot Support: {cat_unsupported} | Partial: {cat_partial} | L2 Handled: {cat_l2_handled} | Avg Confidence: {cat_avg_conf:.1f}/5
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.divider()

        # ── Key Insights ──────────────────────────────────────────────
        st.subheader("Key Insights")

        total_valid = len(valid_df)
        if total_valid > 0:
            # Most common category
            top_cat = valid_df["category"].value_counts().index[0]
            top_cat_count = valid_df["category"].value_counts().values[0]
            st.markdown(f"- **Most common category:** {top_cat} ({top_cat_count} tickets, {top_cat_count/total_valid*100:.0f}%)")

            # L2 coverage rate
            l2_coverage = len(valid_df[valid_df["decision"] == "L2 Can Support"]) / total_valid * 100
            st.markdown(f"- **L2 coverage potential:** {l2_coverage:.0f}% of tickets could be handled by L2")

            # Actual L2 rate
            actual_l2 = len(valid_df[valid_df["l2_involvement"] != "None"]) / total_valid * 100
            st.markdown(f"- **Actual L2 handling rate:** {actual_l2:.0f}% of tickets had L2 involvement")

            # Low confidence tickets needing review
            low_conf = len(valid_df[valid_df["confidence"] <= 2])
            if low_conf > 0:
                st.markdown(f"- **Needs review:** {low_conf} tickets with low confidence (1-2) should be manually reviewed")

            # Categories where L2 could help more
            for cat in [str(c) for c in valid_df["category"].dropna().unique()]:
                cat_df = valid_df[valid_df["category"] == cat]
                could = len(cat_df[cat_df["decision"] == "L2 Can Support"])
                did = len(cat_df[cat_df["l2_involvement"] != "None"])
                if could > 3 and did == 0:
                    st.markdown(f"- **Opportunity:** {cat} has {could} tickets L2 could support but no L2 involvement")

        st.divider()

        # ── Historical trends ─────────────────────────────────────────
        st.subheader("Historical Trends")
        st.markdown("Track how L2 coverage changes across analysis runs over time.")

        history = load_history()
        if history:
            hist_df = pd.DataFrame(history)
            hist_df["date"] = pd.to_datetime(hist_df["date"])
            hist_df = hist_df.sort_values("date")

            st.markdown("**L2 Coverage % Over Time**")
            st.line_chart(hist_df.set_index("date")["L2 Coverage %"], color="#00E676")

            st.markdown("**Decision Breakdown Over Time**")
            breakdown = hist_df.set_index("date")[["L2 Can Support", "L2 Cannot Support", "Partially Supported"]]
            st.area_chart(breakdown)

            st.divider()
            st.markdown("**Run History**")
            display_hist = hist_df.copy()
            display_hist["date"] = display_hist["date"].dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(display_hist, use_container_width=True)
        else:
            st.info("No historical data yet. Each time you run an analysis, a snapshot is saved here.")
    else:
        st.info("No results yet. Run an analysis first to see charts and trends.")


# ═══════════════════════════════════════════════════════════════════════════
# TAB 5: GOOGLE SHEET
# ═══════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("Live Google Sheet")
    st.markdown("Edit the ticket data directly in the embedded Google Sheet below. Changes are saved automatically to the sheet.")

    st.components.v1.iframe(
        GOOGLE_SHEET_EMBED_URL,
        height=800,
        scrolling=True,
    )

    st.markdown(f"[Open in Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit?gid=0)")
