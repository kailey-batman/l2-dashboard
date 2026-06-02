"""
Coaching tab for the L2 dashboard.
Pulls weekly coaching reports from Notion and renders team-level rollups,
per-rep at-a-glance, and a link to the full Notion roster.

Permissions are gated upstream: only emails in DASHBOARD_COACHING_EMAILS see this tab.

Required env vars:
- NOTION_TOKEN: Notion integration token (workspace must have integration added)
- (Optional) NOTION_ROSTER_DB_URL: overrides the default roster URL shown in the link section
"""

import os
import time
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import plotly.graph_objects as go


# ── Stable Notion IDs (Support Team Coaching parent DB + per-rep child DBs) ──
ROSTER_DATA_SOURCE_ID = "013dbf3f-7c65-49ca-a172-5a7c8d0ce551"
ROSTER_DB_URL = "https://www.notion.so/d53e23efbfc241af878478eb90673eec"

# Each rep's child "Coaching Reports" data source ID
REP_REPORTS_DS = {
    "Mirah":   "46697662-2ca4-4aea-8f90-0580668cb2f9",
    "Lena":    "70196692-afd9-4d39-85f6-5bcb07c01148",
    "Matthew": "1386f023-0cdc-49de-afe0-935a697c2d2f",
    "Dylan":   "2c404e9b-dad2-4fd5-8153-67bd7f96223e",
    "Lauren":  "61a5c03e-5996-4cac-aded-e24c0d9c219d",
    "Pablo":   "996932ce-2b89-4fc7-98c4-4a4cd36c875d",
    "Taliyah": "246330fe-cb49-4b73-8573-02698603b104",
}

RATING_ORDER = ["Exceeds", "Meets", "Developing", "Needs work"]
RATING_COLORS = {
    "Exceeds":    "#00E676",
    "Meets":      "#42A5F5",
    "Developing": "#FFC107",
    "Needs work": "#EF5350",
}
DIMENSIONS = [
    "Writing & tone",
    "Technical accuracy",
    "Escalation judgment",
    "Troubleshooting depth",
    "Overall",
]


# ── Notion client helper ─────────────────────────────────────────────────────
def _get_notion_client():
    """Return a Notion client or None if token is missing."""
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        return None
    try:
        from notion_client import Client
        return Client(auth=token)
    except ImportError:
        return None


def _prop(page_props, key, default=None):
    """Extract a property value from a Notion page's properties dict."""
    p = page_props.get(key)
    if p is None:
        return default
    t = p.get("type")
    if t == "title":
        items = p.get("title", [])
        return "".join(i.get("plain_text", "") for i in items) or default
    if t == "rich_text":
        items = p.get("rich_text", [])
        return "".join(i.get("plain_text", "") for i in items) or default
    if t == "number":
        return p.get("number", default)
    if t == "select":
        sel = p.get("select")
        return sel.get("name") if sel else default
    if t == "status":
        sts = p.get("status")
        return sts.get("name") if sts else default
    if t == "date":
        d = p.get("date")
        if d:
            return d.get("start")
        return default
    return default


@st.cache_data(ttl=300, show_spinner=False)
def fetch_roster_rows():
    """Return list of dicts, one per rep, from the roster DB."""
    client = _get_notion_client()
    if client is None:
        return []
    try:
        results = client.databases.query(database_id=ROSTER_DATA_SOURCE_ID).get("results", [])
    except Exception as e:
        st.error(f"Notion roster fetch failed: {e}")
        return []
    rows = []
    for page in results:
        props = page.get("properties", {})
        rows.append({
            "Name": _prop(props, "Name", default=""),
            "Full name": _prop(props, "Full name", default=""),
            "Slack handle": _prop(props, "Slack handle", default=""),
            "Intercom admin ID": _prop(props, "Intercom admin ID"),
            "Last reviewed": _prop(props, "Last reviewed"),
            "Latest overall": _prop(props, "Latest overall"),
            "Page URL": page.get("url", ""),
            "Page ID": page.get("id", ""),
        })
    return rows


@st.cache_data(ttl=300, show_spinner=False)
def fetch_weekly_history():
    """Return list of dicts, one per weekly report across all reps."""
    client = _get_notion_client()
    if client is None:
        return []
    all_rows = []
    for rep_name, ds_id in REP_REPORTS_DS.items():
        try:
            results = client.databases.query(database_id=ds_id).get("results", [])
        except Exception:
            continue
        for page in results:
            props = page.get("properties", {})
            all_rows.append({
                "Rep": rep_name,
                "Report": _prop(props, "Report", default=""),
                "Week of": _prop(props, "Week of"),
                "Conversations reviewed": _prop(props, "Conversations reviewed"),
                "New conversations": _prop(props, "New conversations"),
                "Replies sent": _prop(props, "Replies sent"),
                "Shortcut tickets filed": _prop(props, "Shortcut tickets filed"),
                "Median first response (m)": _prop(props, "Median first response (m)"),
                "Median response time (m)": _prop(props, "Median response time (m)"),
                "Median time to close (h)": _prop(props, "Median time to close (h)"),
                "Median handling time (h)": _prop(props, "Median handling time (h)"),
                "CSAT": _prop(props, "CSAT"),
                "Writing & tone": _prop(props, "Writing & tone"),
                "Technical accuracy": _prop(props, "Technical accuracy"),
                "Escalation judgment": _prop(props, "Escalation judgment"),
                "Troubleshooting depth": _prop(props, "Troubleshooting depth"),
                "Overall": _prop(props, "Overall"),
                "Top strength": _prop(props, "Top strength", default=""),
                "Top opportunity": _prop(props, "Top opportunity", default=""),
                "Status": _prop(props, "Status"),
                "Page URL": page.get("url", ""),
            })
    return all_rows


# ── Rendering ────────────────────────────────────────────────────────────────
def _render_no_token_warning():
    st.warning(
        "Notion integration token not configured. Set `NOTION_TOKEN` on the Railway environment "
        "(value from the Notion integration that's connected to the Support Team Coaching workspace)."
    )
    st.markdown(
        "Once configured, the new tab will read directly from the [Support Team Coaching database]"
        f"({ROSTER_DB_URL})."
    )


def _render_team_rollups(weekly_df: pd.DataFrame):
    st.subheader("Team rollups")
    if weekly_df.empty:
        st.info("No weekly reports yet.")
        return

    # Filter to most recent week per rep (Latest week)
    weekly_df = weekly_df.copy()
    weekly_df["_week_dt"] = pd.to_datetime(weekly_df["Week of"], errors="coerce")
    valid = weekly_df.dropna(subset=["_week_dt"])
    if valid.empty:
        st.info("No dated weekly reports yet.")
        return

    latest_per_rep = valid.sort_values("_week_dt").groupby("Rep").tail(1)

    # Top metric tiles for latest week across team
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        n_reps = len(latest_per_rep)
        st.metric("Reps reviewed this cycle", n_reps)
    with col2:
        avg_csat = latest_per_rep["CSAT"].dropna().mean()
        st.metric("Avg latest CSAT", f"{avg_csat:.0f}%" if pd.notna(avg_csat) else "n/a")
    with col3:
        total_tickets = latest_per_rep["Shortcut tickets filed"].fillna(0).sum()
        st.metric("Tickets filed (latest week)", int(total_tickets))
    with col4:
        total_replies = latest_per_rep["Replies sent"].fillna(0).sum()
        st.metric("Replies sent (latest week)", int(total_replies))

    st.divider()

    # Dimension distribution (stacked bar across reps for each dimension)
    st.markdown("**Latest week rating distribution by dimension**")
    dim_counts = {dim: latest_per_rep[dim].value_counts() for dim in DIMENSIONS}
    bar_data = []
    for dim in DIMENSIONS:
        counts = dim_counts[dim]
        for rating in RATING_ORDER:
            bar_data.append({
                "Dimension": dim,
                "Rating": rating,
                "Count": int(counts.get(rating, 0)),
            })
    bar_df = pd.DataFrame(bar_data)

    fig = go.Figure()
    for rating in RATING_ORDER:
        sub = bar_df[bar_df["Rating"] == rating]
        fig.add_trace(go.Bar(
            name=rating,
            x=sub["Dimension"],
            y=sub["Count"],
            marker_color=RATING_COLORS[rating],
        ))
    fig.update_layout(
        barmode="stack",
        height=380,
        margin=dict(l=10, r=10, t=20, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="#373E47",
        paper_bgcolor="#2D333B",
        font=dict(color="#E0E0E0"),
        yaxis=dict(title="Reps", gridcolor="#4A5160"),
        xaxis=dict(gridcolor="#4A5160"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # CSAT trend over time (line per rep)
    st.markdown("**CSAT trend over time**")
    trend = valid.copy()
    trend = trend.dropna(subset=["CSAT"])
    if trend.empty:
        st.caption("No CSAT data yet.")
    else:
        fig2 = go.Figure()
        for rep in sorted(trend["Rep"].unique()):
            sub = trend[trend["Rep"] == rep].sort_values("_week_dt")
            fig2.add_trace(go.Scatter(
                x=sub["_week_dt"], y=sub["CSAT"], mode="lines+markers", name=rep,
            ))
        fig2.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=20, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="#373E47",
            paper_bgcolor="#2D333B",
            font=dict(color="#E0E0E0"),
            xaxis=dict(title="Week of", gridcolor="#4A5160"),
            yaxis=dict(title="CSAT %", gridcolor="#4A5160", range=[0, 105]),
        )
        st.plotly_chart(fig2, use_container_width=True)


def _render_per_rep_table(roster_rows, weekly_df: pd.DataFrame):
    st.subheader("Per-rep at-a-glance (latest week)")
    if not roster_rows:
        st.info("No reps in the roster.")
        return

    # Build a row per rep with their latest week's stats
    weekly_df = weekly_df.copy()
    weekly_df["_week_dt"] = pd.to_datetime(weekly_df["Week of"], errors="coerce")
    latest_by_rep = {}
    for rep, group in weekly_df.dropna(subset=["_week_dt"]).groupby("Rep"):
        latest_by_rep[rep] = group.sort_values("_week_dt").iloc[-1].to_dict()

    rows = []
    for r in roster_rows:
        name = r.get("Name")
        latest = latest_by_rep.get(name, {})
        rows.append({
            "Rep": name,
            "Last reviewed": r.get("Last reviewed") or "",
            "Latest overall": latest.get("Overall") or r.get("Latest overall") or "",
            "Convos": latest.get("Conversations reviewed"),
            "Tickets": latest.get("Shortcut tickets filed"),
            "FRT (m)": latest.get("Median first response (m)"),
            "TTC (h)": latest.get("Median time to close (h)"),
            "CSAT %": latest.get("CSAT"),
            "Top opportunity": (latest.get("Top opportunity") or "")[:120],
            "Page": r.get("Page URL"),
        })
    df = pd.DataFrame(rows)

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Page": st.column_config.LinkColumn("Notion", display_text="Open"),
            "Top opportunity": st.column_config.TextColumn("Top opportunity", width="large"),
            "CSAT %": st.column_config.NumberColumn("CSAT %", format="%d%%"),
            "FRT (m)": st.column_config.NumberColumn("FRT (m)", format="%.1f"),
            "TTC (h)": st.column_config.NumberColumn("TTC (h)", format="%.1f"),
        },
    )


def _render_coaching_roster_link():
    st.subheader("Coaching roster")
    st.markdown(
        f"Full coaching history with weekly TLDRs and per-rep profiles is in Notion: "
        f"[Open Support Team Coaching →]({ROSTER_DB_URL})"
    )
    st.caption(
        "Each rep's page contains an embedded weekly history database with standout work + "
        "coaching items + ratings. Click into a rep to drill in."
    )


def render():
    """Entry point — called from l2_dashboard.py inside `with tab_coaching:`."""
    st.subheader("Support team coaching")

    client = _get_notion_client()
    if client is None:
        _render_no_token_warning()
        return

    with st.spinner("Loading from Notion..."):
        roster_rows = fetch_roster_rows()
        weekly_rows = fetch_weekly_history()

    weekly_df = pd.DataFrame(weekly_rows) if weekly_rows else pd.DataFrame()

    _render_team_rollups(weekly_df)
    st.divider()
    _render_per_rep_table(roster_rows, weekly_df)
    st.divider()
    _render_coaching_roster_link()
