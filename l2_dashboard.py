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
import urllib.parse
import secrets
import requests as _http
import hmac
import hashlib
from datetime import datetime, timedelta
from anthropic import Anthropic
import gspread
from google.oauth2.service_account import Credentials
import threading
import re
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

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
Do NOT attempt to determine L2 engineer involvement from the ticket data. L2 involvement is tracked manually via a separate field. Always return "None" for both l2_engineer and l2_involvement.

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
L2_TAG_OVERRIDES_FILE = os.path.join(APP_DIR, "l2_tag_overrides.json")
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


def parse_shortcut_activity_for_l2(shortcut_activity):
    """
    Parse Shortcut activity log to find the most recent L2 Support Level change
    and the engineer (Jayson or Sean) who made it.

    Handles lines like:
      [2026-03-24T23:35:21.505Z] Jayson Speer — update story changed L2 Support Level: 4 - Near-Complete (Assisted) → L2 Support Level: 3 - Framework Provided: Ticket Name
      [2026-03-24T23:35:21.505Z] Sean Smith — update story set L2 Support Level: 5 - Independent Resolution: Ticket Name

    Returns: (l2_involvement_str, l2_engineer_str)
    """
    if not shortcut_activity:
        return "None", "None"

    l2_level_map = {
        "5": "5 - Independent Resolution",
        "4": "4 - Near-Complete (Assisted)",
        "3": "3 - Framework Provided",
        "2": "2 - Technical Enrichment",
        "1": "1 - Escalated (No Context)",
    }

    matches = []

    # Strategy 1: full structured match with timestamp + person
    # Tolerates em-dash (—), en-dash (–), or hyphen-minus (-) as separator
    # Tolerates → or -> as arrow
    pattern = re.compile(
        r'\[(\d{4}-\d{2}-\d{2}T[\d:.Z]+)\]\s+([^\u2014\u2013\-\n]+?)\s*[\u2014\u2013]\s*'
        r'update story\s+(?:changed .+?(?:\u2192|->)\s*L2 Support Level:\s*|set L2 Support Level:\s*)(\d)',
        re.IGNORECASE,
    )
    for m in pattern.finditer(shortcut_activity):
        matches.append((m.group(1), m.group(2).strip(), m.group(3)))

    # Strategy 2: simpler fallback — just find any "L2 Support Level: DIGIT" with a person name nearby
    if not matches:
        simple = re.compile(
            r'\[(\d{4}-\d{2}-\d{2}T[\d:.Z]+)\]\s+(\S[^\[\n]*?)\s*[\u2014\u2013\-]{1,3}\s*.*?L2 Support Level:\s*(\d)',
            re.IGNORECASE,
        )
        for m in simple.finditer(shortcut_activity):
            # Only include lines that are actually setting/changing L2 level
            line = shortcut_activity[m.start():shortcut_activity.find('\n', m.start()) if '\n' in shortcut_activity[m.start():] else m.end() + 200]
            if 'l2 support level' in line.lower():
                matches.append((m.group(1), m.group(2).strip(), m.group(3)))

    if not matches:
        return "None", "None"

    # Most recent change wins (ISO timestamps sort lexicographically)
    matches.sort(key=lambda x: x[0])
    _, person, level_num = matches[-1]

    l2_involvement = l2_level_map.get(level_num, f"{level_num} - Unknown")

    person_lower = person.lower()
    if "jayson" in person_lower:
        l2_engineer = "Jayson"
    elif "sean" in person_lower:
        l2_engineer = "Sean"
    else:
        l2_engineer = "None"

    return l2_involvement, l2_engineer


@st.cache_data(ttl=60, show_spinner=False)
def _build_live_map_from_sheet(sheet_df):
    """Parse Shortcut activity from every sheet row and return a name→(involvement, engineer) dict.
    Cached so the expensive iterrows + regex run only once per 60s, not on every render."""
    live_map = {}
    if sheet_df is None:
        return live_map, None
    activity_col = next((c for c in sheet_df.columns if "activity" in c.lower()), None)
    name_col = next((c for c in sheet_df.columns if c.lower() == "name"), None)
    if not activity_col or not name_col:
        return live_map, f"Could not find the Shortcut activity column. Sheet columns: {list(sheet_df.columns)}"
    for _, sr in sheet_df.iterrows():
        tname = str(sr.get(name_col, "")).strip()
        activity_text = str(sr.get(activity_col, "")).strip()
        if tname:
            live_map[tname] = parse_shortcut_activity_for_l2(activity_text)
    return live_map, None


def color_decision(val):
    if val == "L2 Can Support":
        return "background-color: #0a3d1f; color: #00E676"
    elif val == "L2 Cannot Support":
        return "background-color: #3d0a0a; color: #ff5252"
    elif val == "Partially Supported":
        return "background-color: #3d3a0a; color: #FFD740"
    return ""


RESULTS_COLUMNS = ["name", "shortcut_url", "created_at", "state", "description", "decision", "category",
                    "support_person", "l2_engineer", "l2_involvement", "confidence", "explanation"]


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
        ws.update("A1", rows)
    except Exception:
        # Fall back to local file
        with open(RESULTS_FILE, "w") as f:
            json.dump(results, f, indent=2)


@st.cache_data(ttl=60, show_spinner=False)
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


def _apply_result_defaults(df):
    for col, default in [("category", "Other"), ("confidence", 0), ("support_person", "Unknown"),
                          ("l2_engineer", "None"), ("l2_involvement", "None"), ("shortcut_url", ""), ("created_at", ""), ("state", "")]:
        if col not in df.columns:
            df[col] = default
    return df


def load_results():
    """Load results from Google Sheet or local file, preferring whichever has more data."""
    sheet_df = load_results_from_sheet()
    local_df = None
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE) as f:
                data = json.load(f)
            local_df = pd.DataFrame(data)
        except Exception:
            pass

    sheet_count = len(sheet_df) if sheet_df is not None and not sheet_df.empty else 0
    local_count = len(local_df) if local_df is not None and not local_df.empty else 0

    if local_count > sheet_count:
        return _apply_result_defaults(local_df)
    if sheet_count > 0:
        return _apply_result_defaults(sheet_df)
    return None


OVERRIDES_COLUMNS = ["name", "corrected_decision", "reason", "timestamp", "original_decision"]


@st.cache_data(ttl=300, show_spinner=False)
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
    load_overrides.clear()


def load_l2_tag_overrides():
    """Load manual L2 tag overrides from local file. Returns dict: name → {l2_engineer, l2_involvement}."""
    if os.path.exists(L2_TAG_OVERRIDES_FILE):
        try:
            with open(L2_TAG_OVERRIDES_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_l2_tag_overrides(tag_overrides):
    """Persist manual L2 tag overrides to local file."""
    with open(L2_TAG_OVERRIDES_FILE, "w") as f:
        json.dump(tag_overrides, f, indent=2)


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
        analyzed_names = {r["name"].strip() for r in existing_results}

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
            created_at = row.get("created_at", "").strip()
            state = row.get("state", "").strip()
            desc = row.get("description", "").strip()
            intercom_transcript = (row.get("Intercom Transcription", "") or row.get("Intercom Transcript", "")).strip()
            slack_transcript = (row.get("Slack Transcript", "") or row.get("Slack Conversation Transcript", "")).strip()
            shortcut_activity = (row.get("Shortcut Activity Export", "") or row.get("Shortcut Ticket Activity", "")).strip()

            set_analysis_progress(i + 1, len(new_rows), name)

            # Derive L2 involvement and engineer from Shortcut activity log
            l2_involvement, l2_engineer = parse_shortcut_activity_for_l2(shortcut_activity)

            result = evaluate_ticket(client, name, desc, intercom_transcript, slack_transcript, shortcut_activity)
            new_results.append({
                "name": name,
                "shortcut_url": shortcut_url,
                "created_at": created_at,
                "state": state,
                "description": desc[:200],
                "decision": result.get("decision", "Error"),
                "category": result.get("category", "Other"),
                "support_person": result.get("support_person", "Unknown"),
                "l2_engineer": l2_engineer,
                "l2_involvement": l2_involvement,
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


# ── Google OAuth Authentication ─────────────────────────────────────────────
import streamlit.components.v1 as _stc

_AUTH_COOKIE = "fg_l2_auth"
_COOKIE_TTL_HOURS = 24


def _set_auth_cookie(user_info):
    encoded = _encode_auth(user_info)
    max_age = _COOKIE_TTL_HOURS * 3600
    _stc.html(
        f'<script>document.cookie="{_AUTH_COOKIE}={encoded}; path=/; max-age={max_age}; SameSite=Lax";</script>',
        height=0,
    )


def _clear_auth_cookie():
    _stc.html(
        f'<script>document.cookie="{_AUTH_COOKIE}=; path=/; max-age=0";</script>',
        height=0,
    )


def _read_auth_cookie():
    try:
        return st.context.cookies.get(_AUTH_COOKIE)
    except Exception:
        return None


def _encode_auth(user):
    secret = os.environ.get("COOKIE_SECRET", os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "fg-dashboard"))
    exp = (datetime.utcnow() + timedelta(hours=_COOKIE_TTL_HOURS)).isoformat()
    payload = json.dumps({"u": user, "e": exp}, separators=(",", ":"))
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return base64.b64encode(f"{payload}.{sig}".encode()).decode()


def _decode_auth(value):
    try:
        secret = os.environ.get("COOKIE_SECRET", os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "fg-dashboard"))
        decoded = base64.b64decode(value.encode()).decode()
        payload_str, sig = decoded.rsplit(".", 1)
        expected = hmac.new(secret.encode(), payload_str.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None
        data = json.loads(payload_str)
        if datetime.fromisoformat(data["e"]) < datetime.utcnow():
            return None
        return data["u"]
    except Exception:
        return None


_ALLOWED_DOMAIN = "fieldguide.io"
_GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
_OAUTH_SCOPES = "openid email profile"


def _get_oauth_creds():
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
    redirect_uri = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "http://localhost:8501")
    return client_id, client_secret, redirect_uri


def _build_auth_url():
    client_id, _, redirect_uri = _get_oauth_creds()
    state = secrets.token_urlsafe(32)
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
    """Exchange OAuth code for user info. Returns (user_dict, error_str)."""

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
    """Render a centered login card."""
    client_id, _, _ = _get_oauth_creds()
    if not client_id:
        st.error(
            "Google OAuth is not configured. "
            "Set GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, and "
            "GOOGLE_OAUTH_REDIRECT_URI environment variables."
        )
        return

    if "_auth_error" in st.session_state:
        st.error(st.session_state.pop("_auth_error"))

    auth_url = _build_auth_url()

    # If user clicked login, redirect via JS (navigates same tab)
    if st.session_state.get("_do_login"):
        del st.session_state["_do_login"]
        _stc.html(f'<script>window.parent.location.href="{auth_url}";</script>', height=0)
        st.stop()

    logo_html = ""
    logo_path = os.path.join(APP_DIR, "logo.svg")
    if os.path.exists(logo_path):
        with open(logo_path, "r") as f:
            logo_svg = f.read()
        logo_b64 = base64.b64encode(logo_svg.encode()).decode()
        logo_html = f'<img src="data:image/svg+xml;base64,{logo_b64}" style="width:60px;height:60px;margin-bottom:8px;" />'

    st.markdown(f"""
    <style>
        .stApp {{ background-color: #2D333B; }}
        .login-wrapper {{
            display: flex; justify-content: center; align-items: center;
            min-height: 60vh; padding: 2rem;
        }}
        .login-card {{
            background-color: #373E47; border: 1px solid #444C56; border-radius: 16px;
            padding: 48px 40px; max-width: 420px; width: 100%; text-align: center;
            box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        }}
        .login-card h1 {{ color: #00E676; font-size: 1.7rem; margin: 12px 0 8px 0; }}
        .login-card .login-sub {{ color: #9E9E9E; font-size: 0.95rem; margin-bottom: 12px; }}
        .login-note {{ color: #616a75; font-size: 0.75rem; margin-top: 8px; }}
    </style>
    <div class="login-wrapper">
        <div class="login-card">
            {logo_html}
            <h1>Escalation Tracker</h1>
            <div class="login-sub">Sign in with your Fieldguide Google account to continue.</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    _c1, _c2, _c3 = st.columns([1.5, 2, 1.5])
    with _c2:
        if st.button("Sign in with Google", use_container_width=True, type="primary"):
            st.session_state["_do_login"] = True
            st.rerun()

    st.markdown('<div class="login-note" style="text-align:center;">Only @fieldguide.io accounts are permitted.</div>', unsafe_allow_html=True)


# ── Admin helpers ────────────────────────────────────────────────────────────
_ACCESS_LOG_TAB = "Access Log"
_ACCESS_LOG_FILE = os.path.join(APP_DIR, "access_log.json")


def _is_admin():
    email = st.session_state.get("_auth_user", {}).get("email", "").lower()
    raw = os.environ.get("DASHBOARD_ADMIN_EMAILS", "")
    admins = [e.strip().lower() for e in raw.split(",") if e.strip()]
    return bool(admins) and email in admins


def _log_visit(user_info):
    """Append a login event to the local file and Google Sheets Access Log tab."""
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "email": user_info.get("email", ""),
        "name": user_info.get("name", ""),
    }
    # Local JSON file (fast cache; ephemeral on Railway between redeploys)
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
    # Google Sheets (persistent across redeploys)
    try:
        client = get_gspread_client()
        if client:
            ss = client.open_by_key(GOOGLE_SHEET_ID)
            try:
                ws = ss.worksheet(_ACCESS_LOG_TAB)
            except Exception:
                ws = ss.add_worksheet(title=_ACCESS_LOG_TAB, rows=5000, cols=3)
                ws.append_row(["Timestamp", "Email", "Name"])
            ws.append_row([entry["timestamp"], entry["email"], entry["name"]])
    except Exception:
        pass


def _load_access_log():
    """Return access log as a DataFrame. Reads Sheets first, falls back to local file."""
    try:
        client = get_gspread_client()
        if client:
            ss = client.open_by_key(GOOGLE_SHEET_ID)
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


# ── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Escalation Tracker", page_icon="logo.svg", layout="wide")

# ── Restore session from cookie ──────────────────────────────────────────────
if not st.session_state.get("_auth_user"):
    _cookie_val = _read_auth_cookie()
    if _cookie_val:
        _restored = _decode_auth(_cookie_val)
        if _restored:
            st.session_state["_auth_user"] = _restored

# ── OAuth callback handler ────────────────────────────────────────────────────
_qp = st.query_params
if "code" in _qp:
    with st.spinner("Signing you in…"):
        _user, _err = _exchange_code(_qp.get("code", ""), _qp.get("state", ""))
    if _err:
        st.session_state["_auth_error"] = _err
    else:
        st.session_state["_auth_user"] = _user
        _log_visit(_user)
    st.query_params.clear()
    st.rerun()

if not st.session_state.get("_auth_user"):
    _show_login_page()
    st.stop()

# Refresh the auth cookie on every authenticated page load
_set_auth_cookie(st.session_state["_auth_user"])

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

    [data-testid="stSidebar"] { display: none; }
    [data-testid="stAppViewContainer"] { padding-left: 1rem; }

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

# ── Logged-in user bar ───────────────────────────────────────────────────────
_auth_user = st.session_state.get("_auth_user", {})
_col_spacer, _col_user = st.columns([6, 1])
with _col_user:
    with st.popover(f"👤 {_auth_user.get('email', '')}", use_container_width=True):
        st.markdown(f"**{_auth_user.get('name', '')}**")
        st.markdown(f"`{_auth_user.get('email', '')}`")
        if st.button("Sign out", key="_logout_btn", use_container_width=True):
            del st.session_state["_auth_user"]
            _clear_auth_cookie()
            st.rerun()

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
    # Soft rerun every 3 seconds — no full page reload, no flash
    st_autorefresh(interval=3000, key="analysis_refresh")
elif analysis_progress and analysis_progress.get("status") == "complete":
    prog_total = analysis_progress.get("total", 0)
    if prog_total > 0:
        st.success(f"Analysis complete: {prog_total} tickets processed.")
    clear_analysis_progress()


# ── Tabs ────────────────────────────────────────────────────────────────────
_admin_mode = _is_admin()
if _admin_mode:
    tab1, tab2, tab3, tab5, tab_admin = st.tabs(
        ["Results", "Run Analysis", "Trends", "Google Sheet", "Admin"]
    )
else:
    tab1, tab2, tab3, tab5 = st.tabs(["Results", "Run Analysis", "Trends", "Google Sheet"])
    tab_admin = None

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1: RESULTS
# ═══════════════════════════════════════════════════════════════════════════
with tab1:
    results_df = load_results()
    overrides = load_overrides()

    # ── Build live L2 data from ALL sheet rows ───────────────────────
    # Parse Shortcut activity across all 682 tickets so L2 level/engineer
    # metrics reflect every ticket Jayson/Sean has tagged, whether or not
    # that ticket has been through the AI analysis yet.
    sheet_df_live = load_google_sheet()
    live_map, _live_map_err = _build_live_map_from_sheet(sheet_df_live)
    _live_map_ready = bool(live_map)
    if _live_map_err:
        st.warning(_live_map_err)

    # Apply manual tag overrides on top of sheet-parsed values
    l2_tag_overrides = load_l2_tag_overrides()
    for tname, ov in l2_tag_overrides.items():
        live_map[tname] = (ov.get("l2_involvement", "None"), ov.get("l2_engineer", "None"))

    # Re-patch results_df with overrides applied
    if results_df is not None and not results_df.empty and live_map:
        results_df["l2_involvement"] = results_df["name"].map(
            lambda n: live_map.get(n, ("None", "None"))[0]
        ).fillna("None")
        results_df["l2_engineer"] = results_df["name"].map(
            lambda n: live_map.get(n, ("None", "None"))[1]
        ).fillna("None")

    # Pre-compute L2 stats from ALL sheet tickets (not just analyzed ones)
    _tagged = [(inv, eng) for inv, eng in live_map.values() if inv != "None"]
    l2_involved_count   = len(_tagged)
    sean_count          = sum(1 for _, eng in _tagged if eng == "Sean")
    jayson_count        = sum(1 for _, eng in _tagged if eng == "Jayson")
    l2_level_5_count    = sum(1 for inv, _ in _tagged if inv.startswith("5"))
    l2_level_4_count    = sum(1 for inv, _ in _tagged if inv.startswith("4"))
    l2_level_3_count    = sum(1 for inv, _ in _tagged if inv.startswith("3"))
    l2_level_2_count    = sum(1 for inv, _ in _tagged if inv.startswith("2"))
    l2_level_1_count    = sum(1 for inv, _ in _tagged if inv.startswith("1"))
    avg_l2_level        = (sum(int(inv[0]) for inv, _ in _tagged if inv[0].isdigit()) / l2_involved_count
                           if l2_involved_count > 0 else 0)

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

        # ── Row 2: L2 Engineer Involvement (from all 682 sheet rows) ──
        st.markdown("**L2 Engineer Involvement**")
        l2_r1c1, l2_r1c2, l2_r1c3, l2_r1c4 = st.columns(4)

        with l2_r1c1:
            sheet_total = len(sheet_df_live) if sheet_df_live is not None else total
            pct = f"{l2_involved_count/sheet_total*100:.0f}%" if sheet_total > 0 else "0%"
            st.markdown(metric_card_html("L2 Involved", l2_involved_count, delta=pct), unsafe_allow_html=True)
            if st.button("x", key="btn_l2_involved", use_container_width=True):
                st.session_state.metric_filter = ("l2_involvement", "!=None")
                st.rerun()
        with l2_r1c2:
            st.markdown(metric_card_html("Sean", sean_count), unsafe_allow_html=True)
            if st.button("x", key="btn_sean", use_container_width=True):
                st.session_state.metric_filter = ("l2_engineer", "Sean")
                st.rerun()
        with l2_r1c3:
            st.markdown(metric_card_html("Jayson", jayson_count), unsafe_allow_html=True)
            if st.button("x", key="btn_jayson", use_container_width=True):
                st.session_state.metric_filter = ("l2_engineer", "Jayson")
                st.rerun()
        with l2_r1c4:
            html = metric_card_html("Avg L2 Level", f"{avg_l2_level:.1f}/5")
            html = html.replace("&#8595; drill down", "")
            st.markdown(html, unsafe_allow_html=True)

        # ── Row 3: L2 Support Levels (from all 682 sheet rows) ────────
        st.markdown("**L2 Support Levels**")
        lv1, lv2, lv3, lv4, lv5 = st.columns(5)

        with lv1:
            st.markdown(metric_card_html("5 - Independent", l2_level_5_count), unsafe_allow_html=True)
            if st.button("x", key="btn_lv5", use_container_width=True):
                st.session_state.metric_filter = ("l2_level", "5")
                st.rerun()
        with lv2:
            st.markdown(metric_card_html("4 - Near-Complete", l2_level_4_count), unsafe_allow_html=True)
            if st.button("x", key="btn_lv4", use_container_width=True):
                st.session_state.metric_filter = ("l2_level", "4")
                st.rerun()
        with lv3:
            st.markdown(metric_card_html("3 - Framework", l2_level_3_count), unsafe_allow_html=True)
            if st.button("x", key="btn_lv3", use_container_width=True):
                st.session_state.metric_filter = ("l2_level", "3")
                st.rerun()
        with lv4:
            st.markdown(metric_card_html("2 - Enrichment", l2_level_2_count), unsafe_allow_html=True)
            if st.button("x", key="btn_lv2", use_container_width=True):
                st.session_state.metric_filter = ("l2_level", "2")
                st.rerun()
        with lv5:
            st.markdown(metric_card_html("1 - Escalated", l2_level_1_count), unsafe_allow_html=True)
            if st.button("x", key="btn_lv1", use_container_width=True):
                st.session_state.metric_filter = ("l2_level", "1")
                st.rerun()


        # ── Active filter indicator ──────────────────────────────────
        if st.session_state.metric_filter is not None:
            filt = st.session_state.metric_filter
            level_labels = {"5": "5 - Independent Resolution", "4": "4 - Near-Complete", "3": "3 - Framework Provided", "2": "2 - Technical Enrichment", "1": "1 - Escalated"}
            if filt[0] == "gap":
                label = "Gap: Could support but no L2 involvement"
            elif filt[0] == "l2_level":
                label = f"L2 Level: {level_labels.get(filt[1], filt[1])}"
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

        # ── Charts ──────────────────────────────────────────────────────
        chart_left, chart_right = st.columns(2)

        with chart_left:
            # Decision distribution donut
            decision_counts = results_df["decision"].value_counts()
            donut_labels = list(decision_counts.index)
            donut_values = list(decision_counts.values)
            color_map = {
                "L2 Can Support": "#00E676",
                "L2 Cannot Support": "#ff5252",
                "Partially Supported": "#FFD740",
                "Insufficient Data": "#9E9E9E",
                "Error": "#444C56",
            }
            donut_colors = [color_map.get(l, "#444C56") for l in donut_labels]
            fig_donut = go.Figure(go.Pie(
                labels=donut_labels,
                values=donut_values,
                hole=0.55,
                marker=dict(colors=donut_colors, line=dict(color="#2D333B", width=2)),
                textfont=dict(color="#E0E0E0"),
                hovertemplate="%{label}: %{value} (%{percent})<extra></extra>",
            ))
            fig_donut.update_layout(
                title=dict(text="Decision Distribution", font=dict(color="#E0E0E0", size=14)),
                paper_bgcolor="#373E47", plot_bgcolor="#373E47",
                font=dict(color="#E0E0E0"),
                legend=dict(font=dict(color="#9E9E9E"), bgcolor="#373E47"),
                margin=dict(t=40, b=10, l=10, r=10),
                height=280,
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        with chart_right:
            # Top categories horizontal bar
            if "category" in results_df.columns:
                cat_counts = (
                    results_df[results_df["category"].notna() & (results_df["category"] != "Other")]
                    ["category"].value_counts().head(10)
                )
                fig_bar = go.Figure(go.Bar(
                    x=cat_counts.values,
                    y=cat_counts.index,
                    orientation="h",
                    marker=dict(color="#00E676", opacity=0.85),
                    hovertemplate="%{y}: %{x}<extra></extra>",
                ))
                fig_bar.update_layout(
                    title=dict(text="Top Categories", font=dict(color="#E0E0E0", size=14)),
                    paper_bgcolor="#373E47", plot_bgcolor="#373E47",
                    font=dict(color="#E0E0E0"),
                    xaxis=dict(color="#9E9E9E", gridcolor="#444C56"),
                    yaxis=dict(color="#9E9E9E", autorange="reversed"),
                    margin=dict(t=40, b=10, l=10, r=10),
                    height=280,
                )
                st.plotly_chart(fig_bar, use_container_width=True)

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
            elif mf[0] == "l2_level":
                filtered = filtered[filtered["l2_involvement"].str.startswith(mf[1], na=False)]
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
        display_cols = ["created_at", "name", "state", "link", "support_person", "category", "decision", "l2_engineer", "l2_involvement", "confidence"]
        available_cols_base = [c for c in ["created_at", "name", "state", "support_person", "category", "decision", "l2_engineer", "l2_involvement", "confidence"] if c in page_df.columns]

        # Build display table
        styled_page = page_df[available_cols_base].copy()

        # Format created_at to just the date
        if "created_at" in styled_page.columns:
            styled_page["created_at"] = styled_page["created_at"].apply(
                lambda x: str(x).split(" ")[0] if isinstance(x, str) and " " in x else x
            )

        # Add Shortcut Link column
        has_urls = "shortcut_url" in page_df.columns and page_df["shortcut_url"].astype(str).str.startswith("http").any()
        if has_urls:
            styled_page.insert(3, "link", page_df["shortcut_url"])

        styled_page["decision"] = styled_page["decision"].map({
            "L2 Can Support": "\u2705 L2 Can Support",
            "L2 Cannot Support": "\u274c L2 Cannot Support",
            "Partially Supported": "\u26a0\ufe0f Partially Supported",
            "Insufficient Data": "\u2753 Insufficient Data",
        }).fillna(styled_page.get("decision", ""))

        col_config = {
            "created_at": st.column_config.TextColumn("Filed", width="small"),
            "state": st.column_config.TextColumn("Status", width="small"),
        }
        if has_urls:
            col_config["link"] = st.column_config.LinkColumn(
                "Link",
                display_text="Shortcut Link",
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
                l2_inv = str(row.get("l2_involvement", "None"))
                if l2_inv != "None" and l2_inv != "":
                    level_num = l2_inv[0] if l2_inv else ""
                    if level_num in ("4", "5"):
                        st.success(f"**L2: {l2_eng}** — {l2_inv}")
                    elif level_num == "3":
                        st.warning(f"**L2: {l2_eng}** — {l2_inv}")
                    else:
                        st.info(f"**L2: {l2_eng}** — {l2_inv}")
                else:
                    st.caption("No L2 involvement")

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
                        ["None", "5 - Independent Resolution", "4 - Near-Complete (Assisted)", "3 - Framework Provided", "2 - Technical Enrichment", "1 - Escalated (No Context)"],
                        index=0,
                        key=f"tag_inv_{selected}",
                    )
                if st.button("Save L2 Tag", key=f"save_tag_{selected}"):
                    # Save to override file so it persists across sheet re-parses
                    _tag_ovs = load_l2_tag_overrides()
                    _tag_ovs[selected] = {"l2_engineer": tag_engineer, "l2_involvement": tag_involvement}
                    save_l2_tag_overrides(_tag_ovs)
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
                # Load existing results from sheet (or local fallback)
                existing_results = []
                if not rerun_all:
                    df_existing = load_results()
                    if df_existing is not None and not df_existing.empty:
                        existing_results = df_existing.to_dict("records")

                # Check how many are new — apply max_rows AFTER filtering so bottom rows aren't excluded
                analyzed_names = {r["name"].strip() for r in existing_results}
                if rerun_all:
                    rows_to_analyze = rows[:max_rows] if max_rows > 0 else rows
                    new_count = len(rows_to_analyze)
                else:
                    new_rows = [r for r in rows if r.get("name", "").strip() not in analyzed_names]
                    if max_rows > 0:
                        new_rows = new_rows[:max_rows]
                    new_count = len(new_rows)
                    rows_to_analyze = [r for r in rows if r.get("name", "").strip() in analyzed_names] + new_rows

                if new_count == 0 and not rerun_all:
                    st.info("All tickets have already been analyzed. Check 'Re-analyze all' to rerun.")
                else:
                    # Launch background thread
                    set_analysis_progress(0, new_count, "Starting...", status="running")
                    thread = threading.Thread(
                        target=run_analysis_background,
                        args=(rows_to_analyze, existing_results, rerun_all),
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


# ═══════════════════════════════════════════════════════════════════════════
# TAB ADMIN (admin users only)
# ═══════════════════════════════════════════════════════════════════════════
if _admin_mode and tab_admin is not None:
    with tab_admin:
        st.subheader("Access Log")
        st.markdown("Every time a user authenticates, their login is recorded here.")

        log_df = _load_access_log()

        if log_df.empty:
            st.info("No visits recorded yet. Logs are written when users sign in.")
        else:
            # Ensure consistent column naming
            log_df.columns = [c.capitalize() for c in log_df.columns]
            if "Timestamp" in log_df.columns:
                log_df["Timestamp"] = pd.to_datetime(log_df["Timestamp"], errors="coerce")
                log_df = log_df.sort_values("Timestamp", ascending=False).reset_index(drop=True)
                log_df["Timestamp"] = log_df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # Summary metrics
            total_visits = len(log_df)
            unique_users = log_df["Email"].nunique() if "Email" in log_df.columns else 0
            m1, m2 = st.columns(2)
            m1.metric("Total logins", total_visits)
            m2.metric("Unique users", unique_users)

            st.divider()

            # Per-user breakdown
            if "Email" in log_df.columns:
                st.markdown("**Logins per user**")
                counts = (
                    log_df.groupby("Email")
                    .agg(Logins=("Email", "count"), Last_seen=("Timestamp", "max"))
                    .reset_index()
                    .rename(columns={"Last_seen": "Last seen"})
                    .sort_values("Logins", ascending=False)
                )
                st.dataframe(counts, use_container_width=True, hide_index=True)

                st.divider()

            # Full log
            st.markdown("**Full login history**")
            st.dataframe(log_df, use_container_width=True, hide_index=True)
