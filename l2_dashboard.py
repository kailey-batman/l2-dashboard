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
{transcript}

Based on the ticket details and L2's defined capabilities, determine whether L2 can support this task.

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

Also identify the support person who was handling customer communication for this ticket. Look for names of support agents, representatives, or team members in the Intercom transcript and description who were responding to or assisting the customer. This is the internal support person, NOT the customer. If you cannot identify a specific support person, use "Unknown".

Also rate your confidence in this decision from 1 to 5:
- 1 = Very uncertain, could easily go either way
- 2 = Somewhat uncertain, limited information
- 3 = Moderate confidence
- 4 = High confidence
- 5 = Very high confidence, clear-cut case

Respond with ONLY valid JSON in this exact format (no markdown, no code fences):
{{
  "decision": "L2 Can Support" or "L2 Cannot Support" or "Partially Supported",
  "category": "one category from the list above",
  "support_person": "Name of the support person handling the ticket, or Unknown",
  "confidence": 1-5,
  "explanation": "A concise 2-3 sentence explanation of why L2 can or cannot handle this ticket, referencing specific L2 capabilities or gaps."
}}
"""

# ── Paths ───────────────────────────────────────────────────────────────────
APP_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(APP_DIR, "l2_results.json")
OVERRIDES_FILE = os.path.join(APP_DIR, "l2_overrides.json")
HISTORY_DIR = os.path.join(APP_DIR, "history")


def evaluate_ticket(client, name, description, transcript):
    prompt = EVALUATION_PROMPT.format(
        capabilities=L2_CAPABILITIES,
        name=name or "(no name)",
        description=description or "(no description)",
        transcript=transcript or "(no transcript)",
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
        return {"decision": "Error", "category": "Other", "support_person": "Unknown", "confidence": 0, "explanation": f"Parse error: {text[:200]}"}


def color_decision(val):
    if val == "L2 Can Support":
        return "background-color: #0a3d1f; color: #00E676"
    elif val == "L2 Cannot Support":
        return "background-color: #3d0a0a; color: #ff5252"
    elif val == "Partially Supported":
        return "background-color: #3d3a0a; color: #FFD740"
    return ""


def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        if "category" not in df.columns:
            df["category"] = "Other"
        if "confidence" not in df.columns:
            df["confidence"] = 0
        if "support_person" not in df.columns:
            df["support_person"] = "Unknown"
        return df
    return None


def load_overrides():
    if os.path.exists(OVERRIDES_FILE):
        with open(OVERRIDES_FILE) as f:
            return json.load(f)
    return {}


def save_overrides(overrides):
    with open(OVERRIDES_FILE, "w") as f:
        json.dump(overrides, f, indent=2)


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


# ── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="L2 Capability Analyzer", page_icon="logo.svg", layout="wide")

# ── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    .header-container {
        display: flex; align-items: center; gap: 16px; padding: 0.5rem 0 1.5rem 0;
    }
    .header-container img { width: 48px; height: 48px; }
    .header-container h1 { color: #00E676; margin: 0; font-size: 2rem; }
    .header-subtitle { color: #9E9E9E; font-size: 0.95rem; margin-top: -8px; padding-bottom: 1rem; }

    [data-testid="stMetric"] {
        background-color: #1A1F2B; border: 1px solid #2A2F3B; border-radius: 10px; padding: 16px;
    }
    [data-testid="stMetricLabel"] { color: #9E9E9E !important; }
    [data-testid="stMetricValue"] { color: #E0E0E0 !important; }
    [data-testid="stMetricDelta"] { color: #00E676 !important; }

    [data-testid="stSidebar"] { background-color: #141820; border-right: 1px solid #2A2F3B; }
    [data-testid="stSidebar"] .stMarkdown h2 { color: #00E676; }

    .stTabs [data-baseweb="tab"] { color: #9E9E9E; }
    .stTabs [aria-selected="true"] { color: #00E676 !important; border-bottom-color: #00E676 !important; }

    .stButton > button[kind="primary"] {
        background-color: #00E676; color: #0E1117; border: none; font-weight: 600;
    }
    .stButton > button[kind="primary"]:hover { background-color: #00C853; color: #0E1117; }

    .stDownloadButton > button {
        background-color: #1A1F2B; color: #00E676; border: 1px solid #00E676;
    }
    .stDownloadButton > button:hover { background-color: #00E676; color: #0E1117; }

    .streamlit-expanderHeader { color: #E0E0E0; background-color: #1A1F2B; }
    hr { border-color: #2A2F3B; }
    [data-baseweb="select"] { background-color: #1A1F2B; }
    .stDataFrame { border: 1px solid #2A2F3B; border-radius: 8px; overflow-x: auto !important; }
    .stDataFrame [data-testid="stDataFrameResizable"] { width: 100% !important; min-width: 0 !important; }

    /* Category stats cards */
    .cat-stat-card {
        background-color: #1A1F2B; border: 1px solid #2A2F3B; border-radius: 8px;
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
        display: inline-block; background-color: #FFD740; color: #0E1117;
        font-size: 0.7rem; font-weight: 700; padding: 2px 8px; border-radius: 4px;
    }

    /* Confidence stars */
    .confidence-stars { color: #00E676; letter-spacing: 2px; }
    .confidence-dim { color: #2A2F3B; }
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
        <h1>L2 Capability Analyzer</h1>
    </div>
    """, unsafe_allow_html=True)
else:
    st.title("L2 Capability Analyzer")

st.markdown('<div class="header-subtitle">Evaluate whether L2 support can handle each ticket using Claude AI.</div>', unsafe_allow_html=True)

# ── Analysis progress banner (shows on all tabs) ───────────────────────────
if "analysis_running" not in st.session_state:
    st.session_state.analysis_running = False
if "analysis_progress" not in st.session_state:
    st.session_state.analysis_progress = 0.0
if "analysis_status" not in st.session_state:
    st.session_state.analysis_status = ""

if st.session_state.analysis_running:
    st.markdown(f"""
    <div class="progress-banner">
        <span class="progress-text">Analysis in progress: {st.session_state.analysis_status}</span>
    </div>
    """, unsafe_allow_html=True)
    st.progress(st.session_state.analysis_progress)

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
tab1, tab2, tab3, tab4 = st.tabs(["Results", "Run Analysis", "Trends", "Slack"])

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1: RESULTS
# ═══════════════════════════════════════════════════════════════════════════
with tab1:
    results_df = load_results()
    overrides = load_overrides()

    if results_df is not None and not results_df.empty:
        # ── Summary metrics ─────────────────────────────────────────────
        col1, col2, col3, col4, col5 = st.columns(5)
        total = len(results_df)
        supported = len(results_df[results_df["decision"] == "L2 Can Support"])
        unsupported = len(results_df[results_df["decision"] == "L2 Cannot Support"])
        partial = len(results_df[results_df["decision"] == "Partially Supported"])
        avg_conf = results_df["confidence"].mean() if "confidence" in results_df.columns else 0

        col1.metric("Total Tickets", total)
        col2.metric("L2 Can Support", supported, delta=f"{supported/total*100:.0f}%")
        col3.metric("L2 Cannot Support", unsupported, delta=f"{unsupported/total*100:.0f}%", delta_color="inverse")
        col4.metric("Partially Supported", partial)
        col5.metric("Avg Confidence", f"{avg_conf:.1f}/5")

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

        # ── Category summary stats ──────────────────────────────────────
        with st.expander("Category Summary Stats"):
            for cat in sorted(results_df["category"].unique()):
                cat_df = results_df[results_df["category"] == cat]
                cat_total = len(cat_df)
                cat_supported = len(cat_df[cat_df["decision"] == "L2 Can Support"])
                cat_unsupported = len(cat_df[cat_df["decision"] == "L2 Cannot Support"])
                cat_partial = len(cat_df[cat_df["decision"] == "Partially Supported"])
                cat_avg_conf = cat_df["confidence"].mean() if "confidence" in cat_df.columns else 0
                st.markdown(f"""
                <div class="cat-stat-card">
                    <div class="cat-name">{cat} ({cat_total} tickets)</div>
                    <div class="cat-detail">
                        Can Support: {cat_supported} | Cannot Support: {cat_unsupported} | Partial: {cat_partial} | Avg Confidence: {cat_avg_conf:.1f}/5
                    </div>
                </div>
                """, unsafe_allow_html=True)

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
                    ["All", "L2 Can Support", "L2 Cannot Support", "Partially Supported"],
                )
            with col_f2:
                categories = ["All"] + sorted(results_df["category"].unique().tolist())
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

        filtered = results_df.copy()
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
        ROWS_PER_PAGE = 25
        total_pages = max(1, (len(filtered) + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE)

        page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
        with page_col2:
            current_page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
        start_idx = (current_page - 1) * ROWS_PER_PAGE
        end_idx = min(start_idx + ROWS_PER_PAGE, len(filtered))
        page_df = filtered.iloc[start_idx:end_idx]

        st.markdown(f"*Page {current_page} of {total_pages} ({start_idx+1}-{end_idx} of {len(filtered)} results)*")

        # ── Clickable table ────────────────────────────────────────────
        display_cols = ["name", "support_person", "category", "decision", "confidence"]
        available_cols = [c for c in display_cols if c in page_df.columns]

        selection = st.dataframe(
            page_df[available_cols],
            use_container_width=True,
            height=min(400, len(page_df) * 40 + 40),
            on_select="rerun",
            selection_mode="single-row",
        )

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

            st.markdown(f"### {selected}")

            col_left, col_mid, col_right = st.columns([1, 1, 2])
            with col_left:
                decision = row["decision"]
                if decision == "L2 Can Support":
                    st.success(f"**{decision}**")
                elif decision == "L2 Cannot Support":
                    st.error(f"**{decision}**")
                else:
                    st.warning(f"**{decision}**")

            with col_mid:
                conf = int(row.get("confidence", 0))
                stars = "★" * conf + "☆" * (5 - conf)
                st.markdown(f"**Confidence:** <span class='confidence-stars'>{stars}</span> ({conf}/5)", unsafe_allow_html=True)
                st.markdown(f"**Category:** {row.get('category', 'Other')}")
                st.markdown(f"**Support Person:** {row.get('support_person', 'Unknown')}")

            with col_right:
                st.markdown(f"**Explanation:** {row['explanation']}")

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

    max_rows = st.number_input("Max rows to process (0 = all)", min_value=0, value=10, step=5)
    uploaded = st.file_uploader("Upload a CSV file with ticket data", type=["csv"])
    default_path = "/Users/kaileythorpe/Downloads/Ticket Escalation Tracker 2 (1).csv"

    use_default = st.checkbox(f"Use default file: `{os.path.basename(default_path)}`", value=True)

    if st.button("Run Analysis", type="primary"):
        if uploaded:
            raw = uploaded.read().decode("utf-8-sig")
            reader = csv.DictReader(raw.splitlines())
            rows = list(reader)
        elif use_default and os.path.exists(default_path):
            with open(default_path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        else:
            st.error("No file provided.")
            rows = []

        if rows:
            if max_rows > 0:
                rows = rows[:max_rows]

            client = Anthropic()
            results = []

            st.session_state.analysis_running = True
            progress_bar = st.progress(0, text="Starting analysis...")
            status_text = st.empty()

            for i, row in enumerate(rows):
                name = row.get("name", "").strip()
                desc = row.get("description", "").strip()
                transcript = row.get("Intercom Transcript", "").strip()

                pct = i / len(rows)
                status = f"[{i+1}/{len(rows)}] {name[:60]}..."
                progress_bar.progress(pct, text=status)
                status_text.markdown(f"**Evaluating:** {name[:80]}")

                st.session_state.analysis_progress = pct
                st.session_state.analysis_status = status

                result = evaluate_ticket(client, name, desc, transcript)
                results.append({
                    "name": name,
                    "description": desc[:200],
                    "decision": result.get("decision", "Error"),
                    "category": result.get("category", "Other"),
                    "support_person": result.get("support_person", "Unknown"),
                    "confidence": result.get("confidence", 0),
                    "explanation": result.get("explanation", ""),
                })

                if i < len(rows) - 1:
                    time.sleep(0.5)

            progress_bar.progress(1.0, text="Complete!")
            status_text.empty()

            st.session_state.analysis_running = False
            st.session_state.analysis_progress = 0.0
            st.session_state.analysis_status = ""

            # Save results
            with open(RESULTS_FILE, "w") as f:
                json.dump(results, f, indent=2)

            # Save history snapshot
            save_history_snapshot(results)

            st.success(f"Analyzed {len(results)} tickets. Switch to **Results** tab to explore.")
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3: TRENDS
# ═══════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Trends & Charts")

    trends_df = load_results()
    if trends_df is not None and not trends_df.empty:
        if "category" not in trends_df.columns:
            trends_df["category"] = "Other"

        # ── Current snapshot charts ─────────────────────────────────
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
            cat_counts = trends_df["category"].value_counts()
            chart_data2 = pd.DataFrame({
                "Category": cat_counts.index,
                "Count": cat_counts.values
            })
            st.bar_chart(chart_data2, x="Category", y="Count", color="#00E676")

        st.divider()

        # ── Historical trends ───────────────────────────────────────
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
# TAB 4: SLACK INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Slack Integration")
    st.markdown("Generate a weekly summary to post in Slack.")

    results_df_slack = load_results()
    if results_df_slack is not None and not results_df_slack.empty:
        total = len(results_df_slack)
        supported = len(results_df_slack[results_df_slack["decision"] == "L2 Can Support"])
        unsupported = len(results_df_slack[results_df_slack["decision"] == "L2 Cannot Support"])
        partial = len(results_df_slack[results_df_slack["decision"] == "Partially Supported"])

        # Top categories
        top_cats = results_df_slack["category"].value_counts().head(5)
        cat_lines = "\n".join([f"  - {cat}: {count} tickets" for cat, count in top_cats.items()])

        # Low confidence tickets
        if "confidence" in results_df_slack.columns:
            low_conf = results_df_slack[results_df_slack["confidence"] <= 2]
            low_conf_count = len(low_conf)
        else:
            low_conf_count = 0

        slack_message = f"""*L2 Capability Analysis Summary*
:bar_chart: *{total} tickets analyzed*

*Decision Breakdown:*
:white_check_mark: L2 Can Support: *{supported}* ({supported/total*100:.0f}%)
:x: L2 Cannot Support: *{unsupported}* ({unsupported/total*100:.0f}%)
:warning: Partially Supported: *{partial}* ({partial/total*100:.0f}%)

*Top Categories:*
{cat_lines}

:mag: *{low_conf_count} tickets* flagged for manual review (low confidence)

_Generated by L2 Capability Analyzer_"""

        st.markdown("**Preview:**")
        st.code(slack_message, language=None)

        webhook_url = st.text_input("Slack Webhook URL:", type="password",
                                     help="Create an incoming webhook at api.slack.com/apps")

        if st.button("Send to Slack", type="primary"):
            if webhook_url:
                try:
                    import urllib.request
                    payload = json.dumps({"text": slack_message}).encode("utf-8")
                    req = urllib.request.Request(
                        webhook_url,
                        data=payload,
                        headers={"Content-Type": "application/json"},
                    )
                    urllib.request.urlopen(req)
                    st.success("Sent to Slack!")
                except Exception as e:
                    st.error(f"Failed to send: {e}")
            else:
                st.warning("Enter a Slack webhook URL first.")

        st.download_button(
            label="Copy as text instead",
            data=slack_message,
            file_name="l2_slack_summary.txt",
            mime="text/plain",
        )
    else:
        st.info("No results to summarize. Run an analysis first.")
