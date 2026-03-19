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

Respond with ONLY valid JSON in this exact format (no markdown, no code fences):
{{
  "decision": "L2 Can Support" or "L2 Cannot Support" or "Partially Supported",
  "explanation": "A concise 2-3 sentence explanation of why L2 can or cannot handle this ticket, referencing specific L2 capabilities or gaps."
}}
"""


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
        return {"decision": "Error", "explanation": f"Parse error: {text[:200]}"}


def color_decision(val):
    if val == "L2 Can Support":
        return "background-color: #d4edda; color: #155724"
    elif val == "L2 Cannot Support":
        return "background-color: #f8d7da; color: #721c24"
    elif val == "Partially Supported":
        return "background-color: #fff3cd; color: #856404"
    return ""


# ── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="L2 Capability Analyzer", page_icon="📋", layout="wide")

st.title("📋 L2 Capability Analyzer")
st.markdown("Evaluate whether L2 support can handle each ticket using Claude AI.")

# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Settings")
    max_rows = st.number_input("Max rows to process (0 = all)", min_value=0, value=10, step=5)
    st.divider()
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

# ── Load existing results or CSV ────────────────────────────────────────────
tab1, tab2 = st.tabs(["📊 View Results", "🚀 Run Analysis"])

with tab1:
    st.subheader("Analysis Results")

    results_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "l2_results.json")
    csv_results = "/Users/kaileythorpe/Downloads/l2_analysis_test_10.csv"

    # Try loading JSON results first, then CSV
    results_df = None
    if os.path.exists(results_file):
        with open(results_file) as f:
            data = json.load(f)
        results_df = pd.DataFrame(data)
    elif os.path.exists(csv_results):
        results_df = pd.read_csv(csv_results)

    if results_df is not None and not results_df.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        total = len(results_df)
        supported = len(results_df[results_df["decision"] == "L2 Can Support"])
        unsupported = len(results_df[results_df["decision"] == "L2 Cannot Support"])
        partial = len(results_df[results_df["decision"] == "Partially Supported"])

        col1.metric("Total Tickets", total)
        col2.metric("L2 Can Support", supported, delta=f"{supported/total*100:.0f}%")
        col3.metric("L2 Cannot Support", unsupported)
        col4.metric("Partially Supported", partial)

        st.divider()

        # Filter
        filter_option = st.selectbox(
            "Filter by decision:",
            ["All", "L2 Can Support", "L2 Cannot Support", "Partially Supported"],
        )
        if filter_option != "All":
            filtered = results_df[results_df["decision"] == filter_option]
        else:
            filtered = results_df

        # Styled table
        st.dataframe(
            filtered.style.applymap(color_decision, subset=["decision"]),
            use_container_width=True,
            height=400,
        )

        # Detail view
        st.divider()
        st.subheader("Ticket Detail View")
        ticket_names = filtered["name"].tolist()
        if ticket_names:
            selected = st.selectbox("Select a ticket:", ticket_names)
            row = filtered[filtered["name"] == selected].iloc[0]
            col_left, col_right = st.columns([1, 2])
            with col_left:
                decision = row["decision"]
                if decision == "L2 Can Support":
                    st.success(f"**{decision}**")
                elif decision == "L2 Cannot Support":
                    st.error(f"**{decision}**")
                else:
                    st.warning(f"**{decision}**")
            with col_right:
                st.markdown(f"**Explanation:** {row['explanation']}")
            if "description" in row and row["description"]:
                with st.expander("Description"):
                    st.text(row["description"])

        # Download
        st.divider()
        csv_download = filtered.to_csv(index=False)
        st.download_button(
            label="📥 Download Results as CSV",
            data=csv_download,
            file_name="l2_analysis_results.csv",
            mime="text/csv",
        )
    else:
        st.info("No results found yet. Go to the **Run Analysis** tab to process tickets.")

with tab2:
    st.subheader("Run New Analysis")

    uploaded = st.file_uploader("Upload a CSV file with ticket data", type=["csv"])
    default_path = "/Users/kaileythorpe/Downloads/Ticket Escalation Tracker 2 (1).csv"

    use_default = st.checkbox(f"Use default file: `{os.path.basename(default_path)}`", value=True)

    if st.button("▶ Run Analysis", type="primary"):
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
            progress = st.progress(0, text="Starting analysis...")

            for i, row in enumerate(rows):
                name = row.get("name", "").strip()
                desc = row.get("description", "").strip()
                transcript = row.get("Intercom Transcript", "").strip()

                progress.progress((i) / len(rows), text=f"Evaluating: {name[:60]}...")

                result = evaluate_ticket(client, name, desc, transcript)
                results.append({
                    "name": name,
                    "description": desc[:200],
                    "decision": result.get("decision", "Error"),
                    "explanation": result.get("explanation", ""),
                })

                if i < len(rows) - 1:
                    time.sleep(0.5)

            progress.progress(1.0, text="Complete!")

            # Save results
            results_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "l2_results.json")
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2)

            st.success(f"Analyzed {len(results)} tickets. Switch to **View Results** tab to explore.")
            st.rerun()
