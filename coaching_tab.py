"""
Coaching tab for the L2 dashboard.
Pulls weekly coaching reports from Notion and renders team-level rollups,
per-rep tabs with full Notion notes, and a link to the full Notion roster.

Permissions are gated upstream: only emails in DASHBOARD_COACHING_EMAILS see this tab.

Required env vars:
- NOTION_TOKEN: Notion integration token (workspace must have integration added)
"""

import os
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


def _query_data_source(client, ds_id):
    """Query a Notion data source. Handles both old (databases.query) and new (data_sources.query) SDK versions."""
    try:
        return client.data_sources.query(data_source_id=ds_id).get("results", [])
    except AttributeError:
        return client.databases.query(database_id=ds_id).get("results", [])


# ── Rich text + block → markdown converters ─────────────────────────────────
def _rich_text_to_md(rich_text_list):
    """Convert a Notion rich_text array to a markdown string."""
    out = []
    for rt in rich_text_list or []:
        text = rt.get("plain_text", "")
        if not text:
            continue
        ann = rt.get("annotations") or {}
        href = rt.get("href")
        if ann.get("code"):
            text = f"`{text}`"
        if ann.get("bold"):
            text = f"**{text}**"
        if ann.get("italic"):
            text = f"*{text}*"
        if href:
            text = f"[{text}]({href})"
        out.append(text)
    return "".join(out)


def _table_block_to_md(client, block_id, has_column_header=True):
    """Fetch a table block's children (table_row blocks) and render as a markdown table."""
    try:
        rows = client.blocks.children.list(block_id=block_id).get("results", [])
    except Exception:
        return ""
    lines = []
    for i, r in enumerate(rows):
        cells = r.get("table_row", {}).get("cells", []) or []
        rendered = [_rich_text_to_md(c) or "" for c in cells]
        # Escape pipes so markdown table syntax stays valid
        rendered = [c.replace("|", "\\|").replace("\n", " ") for c in rendered]
        lines.append("| " + " | ".join(rendered) + " |")
        if i == 0 and has_column_header:
            lines.append("|" + "|".join(["---"] * len(rendered)) + "|")
    return "\n".join(lines)


def _blocks_to_markdown(blocks, client=None):
    """Convert a list of Notion block objects to a single markdown string."""
    lines = []
    for block in blocks:
        btype = block.get("type")
        if not btype:
            continue
        b = block.get(btype, {})
        rt = b.get("rich_text", [])
        text = _rich_text_to_md(rt)
        if btype == "paragraph":
            lines.append(text or "")
        elif btype == "heading_1":
            lines.append(f"# {text}")
        elif btype == "heading_2":
            lines.append(f"## {text}")
        elif btype == "heading_3":
            lines.append(f"### {text}")
        elif btype == "bulleted_list_item":
            lines.append(f"- {text}")
        elif btype == "numbered_list_item":
            lines.append(f"1. {text}")
        elif btype == "quote":
            lines.append(f"> {text}")
        elif btype == "code":
            lang = b.get("language", "")
            lines.append(f"```{lang}\n{text}\n```")
        elif btype == "divider":
            lines.append("---")
        elif btype == "callout":
            icon = (b.get("icon") or {}).get("emoji", "💡")
            lines.append(f"> {icon} {text}")
        elif btype == "table" and client is not None:
            md = _table_block_to_md(
                client,
                block.get("id"),
                has_column_header=b.get("has_column_header", True),
            )
            if md:
                lines.append(md)
        # skip child_database, child_page, image (not needed for coaching content)
    return "\n\n".join(lines)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_page_body(page_id):
    """Fetch a Notion page's child blocks and return as a markdown string. Cached 5 min."""
    if not page_id:
        return ""
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        return ""
    try:
        from notion_client import Client
        client = Client(auth=token)
        blocks = client.blocks.children.list(block_id=page_id).get("results", [])
    except Exception as e:
        return f"_(failed to fetch page content: {e})_"
    return _blocks_to_markdown(blocks, client=client)


# ── Data fetchers (Notion → DataFrame) ───────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def fetch_roster_rows():
    """Return list of dicts, one per rep, from the roster DB."""
    client = _get_notion_client()
    if client is None:
        return []
    try:
        results = _query_data_source(client, ROSTER_DATA_SOURCE_ID)
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
            results = _query_data_source(client, ds_id)
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
                "Page ID": page.get("id", ""),
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


def _render_date_filter(weekly_df):
    """Returns (start_date, end_date, filtered_df). filtered_df has _week_dt column.

    Includes preset buttons (Last week / This month / Last 4 weeks / This quarter / Last quarter)
    that update the session state and rerun, plus a manual date picker.
    """
    if weekly_df.empty:
        return None, None, weekly_df
    df = weekly_df.copy()
    df["_week_dt"] = pd.to_datetime(df["Week of"], errors="coerce")
    valid_dates = df["_week_dt"].dropna()
    if valid_dates.empty:
        return None, None, df

    data_min = valid_dates.min().date()
    data_max = valid_dates.max().date()
    today = datetime.now().date()

    def _clamp(s, e):
        s_c = max(s, data_min)
        e_c = min(e, data_max)
        if s_c > e_c:
            # Preset range is entirely outside the available data (e.g., "Last week"
            # when no data has been written for it yet). Default to the latest day
            # of data so the widget stays valid; the filtered table will show "no data".
            return data_max, data_max
        return s_c, e_c

    def _last_week():
        weekday = today.weekday()  # Mon=0, Sun=6
        this_mon = today - timedelta(days=weekday)
        last_mon = this_mon - timedelta(days=7)
        last_sun = this_mon - timedelta(days=1)
        return _clamp(last_mon, last_sun)

    def _this_month():
        return _clamp(today.replace(day=1), today)

    def _last_4_weeks():
        return _clamp(today - timedelta(days=28), today)

    def _this_quarter():
        q_month = ((today.month - 1) // 3) * 3 + 1
        return _clamp(today.replace(month=q_month, day=1), today)

    def _last_quarter():
        q_month = ((today.month - 1) // 3) * 3 + 1
        this_q_start = today.replace(month=q_month, day=1)
        last_q_end = this_q_start - timedelta(days=1)
        last_q_month = ((last_q_end.month - 1) // 3) * 3 + 1
        last_q_start = last_q_end.replace(month=last_q_month, day=1)
        return _clamp(last_q_start, last_q_end)

    presets = [
        ("Last week", _last_week),
        ("This month", _this_month),
        ("Last 4 weeks", _last_4_weeks),
        ("This quarter", _this_quarter),
        ("Last quarter", _last_quarter),
    ]

    st.markdown("**Filter date range**")
    # Render ALL preset buttons (including "All time") BEFORE the date_input widgets,
    # so click handlers can safely mutate session_state without Streamlit raising
    # "cannot be modified after the widget is instantiated".
    presets_with_all = presets + [("All time", lambda: (data_min, data_max))]
    preset_cols = st.columns(len(presets_with_all))
    for col, (label, fn) in zip(preset_cols, presets_with_all):
        with col:
            key = "preset_" + label.replace(" ", "_").lower()
            if st.button(label, key=key, use_container_width=True):
                s, e = fn()
                st.session_state["coaching_start"] = s
                st.session_state["coaching_end"] = e
                st.rerun()

    # Initialize session state if not yet set
    if "coaching_start" not in st.session_state:
        st.session_state["coaching_start"] = data_min
    if "coaching_end" not in st.session_state:
        st.session_state["coaching_end"] = data_max

    # Clamp session state to current data range (in case data shifted)
    if st.session_state["coaching_start"] < data_min:
        st.session_state["coaching_start"] = data_min
    if st.session_state["coaching_end"] > data_max:
        st.session_state["coaching_end"] = data_max

    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input(
            "Start", min_value=data_min, max_value=data_max, key="coaching_start"
        )
    with c2:
        end = st.date_input(
            "End", min_value=data_min, max_value=data_max, key="coaching_end"
        )

    mask = (
        (df["_week_dt"].dt.date >= start) & (df["_week_dt"].dt.date <= end)
    ) | df["_week_dt"].isna()
    return start, end, df[mask].copy()


def _render_team_rollups(weekly_df: pd.DataFrame):
    st.subheader("Team rollups")
    if weekly_df.empty:
        st.info("No weekly reports in the selected date range.")
        return

    if "_week_dt" not in weekly_df.columns:
        weekly_df = weekly_df.copy()
        weekly_df["_week_dt"] = pd.to_datetime(weekly_df["Week of"], errors="coerce")
    valid = weekly_df.dropna(subset=["_week_dt"])
    if valid.empty:
        st.info("No dated weekly reports in the selected date range.")
        return

    latest_per_rep = valid.sort_values("_week_dt").groupby("Rep").tail(1)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Reps in range", len(latest_per_rep))
    with col2:
        avg_csat = latest_per_rep["CSAT"].dropna().mean()
        st.metric("Avg latest CSAT", f"{avg_csat:.0f}%" if pd.notna(avg_csat) else "n/a")
    with col3:
        total_tickets = latest_per_rep["Shortcut tickets filed"].fillna(0).sum()
        st.metric("Tickets (latest)", int(total_tickets))
    with col4:
        total_replies = latest_per_rep["Replies sent"].fillna(0).sum()
        st.metric("Replies (latest)", int(total_replies))

    st.divider()

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

    st.markdown("**CSAT trend over time**")
    trend = valid.dropna(subset=["CSAT"]).copy()
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


def _render_rep_trend_chart(rep_weeks: pd.DataFrame, rep_name: str):
    """Per-rep line chart: CSAT % and Conversations reviewed over time, dual axis."""
    if rep_weeks.empty:
        return
    df = rep_weeks.dropna(subset=["_week_dt"]).sort_values("_week_dt")
    if df.empty:
        return

    convs = df["Conversations reviewed"].fillna(df.get("New conversations"))
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["_week_dt"], y=df["CSAT"], mode="lines+markers", name="CSAT %",
        line=dict(color="#00E676", width=2), yaxis="y2",
    ))
    fig.add_trace(go.Bar(
        x=df["_week_dt"], y=convs, name="Conversations",
        marker_color="#42A5F5", opacity=0.55,
    ))
    fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=20, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="#373E47",
        paper_bgcolor="#2D333B",
        font=dict(color="#E0E0E0"),
        xaxis=dict(title="Week of", gridcolor="#4A5160"),
        yaxis=dict(title="Conversations", gridcolor="#4A5160"),
        yaxis2=dict(title="CSAT %", overlaying="y", side="right",
                    range=[0, 105], gridcolor="#4A5160", showgrid=False),
        title=dict(text=f"{rep_name} — CSAT and volume over time", x=0, font=dict(size=14)),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_per_rep_tabs(roster_rows, weekly_df: pd.DataFrame):
    """Tabs: 'All' (team rollups) + one tab per rep."""
    st.subheader("Per-rep coaching detail")
    if not roster_rows:
        st.info("No reps in the roster.")
        return

    # Sort reps alphabetically by name
    sorted_reps = sorted(roster_rows, key=lambda r: r.get("Name", "") or "")
    rep_names = [r.get("Name") or "Unknown" for r in sorted_reps]
    rep_lookup = {r.get("Name") or "Unknown": r for r in sorted_reps}

    if not rep_names:
        st.info("No reps to display.")
        return

    if "_week_dt" not in weekly_df.columns and not weekly_df.empty:
        weekly_df = weekly_df.copy()
        weekly_df["_week_dt"] = pd.to_datetime(weekly_df["Week of"], errors="coerce")

    # "All" tab first, then one tab per rep
    tab_labels = ["All"] + rep_names
    sub_tabs = st.tabs(tab_labels)

    # "All" tab — team rollups
    with sub_tabs[0]:
        _render_team_rollups(weekly_df)

    # Per-rep tabs
    for tab, name in zip(sub_tabs[1:], rep_names):
        with tab:
            rep = rep_lookup[name]
            full_name = rep.get("Full name") or name
            slack = rep.get("Slack handle") or ""
            last_reviewed = rep.get("Last reviewed") or "never"
            latest_overall = rep.get("Latest overall") or "n/a"
            page_url = rep.get("Page URL", "")

            header_cols = st.columns([3, 1])
            with header_cols[0]:
                st.markdown(f"### {full_name}")
                st.caption(
                    f"Slack: `{slack or '—'}` · Last reviewed: {last_reviewed} · "
                    f"Latest overall: **{latest_overall}**"
                )
            with header_cols[1]:
                if page_url:
                    st.link_button("Open in Notion", page_url, use_container_width=True)

            # Filter weekly to this rep + sort newest first
            if not weekly_df.empty:
                rep_weeks = weekly_df[weekly_df["Rep"] == name].copy()
                rep_weeks = rep_weeks.dropna(subset=["_week_dt"]).sort_values("_week_dt", ascending=False)
            else:
                rep_weeks = pd.DataFrame()

            # Latest week metric tiles
            if not rep_weeks.empty:
                latest = rep_weeks.iloc[0]
                m_cols = st.columns(5)

                def _fmt_num(val, suffix=""):
                    return f"{val:.1f}{suffix}" if pd.notna(val) else "n/a"

                def _fmt_int(val):
                    return int(val) if pd.notna(val) else "n/a"

                # Convos tile: prefer "New conversations" (Intercom dashboard semantic),
                # fall back to "Conversations reviewed" if not populated
                new_convs = latest.get("New conversations")
                if not pd.notna(new_convs):
                    new_convs = latest.get("Conversations reviewed")
                m_cols[0].metric("Convos", _fmt_int(new_convs))
                m_cols[1].metric("FRT", _fmt_num(latest.get("Median first response (m)"), " m"))
                m_cols[2].metric("TTC", _fmt_num(latest.get("Median time to close (h)"), " h"))
                m_cols[3].metric(
                    "CSAT",
                    f"{int(latest.get('CSAT'))}%" if pd.notna(latest.get("CSAT")) else "n/a",
                )
                m_cols[4].metric("Tickets", _fmt_int(latest.get("Shortcut tickets filed")))

            # Per-rep trend chart
            if not rep_weeks.empty:
                st.markdown("")
                _render_rep_trend_chart(rep_weeks, name)

            st.divider()

            # Profile body (from rep's roster page)
            st.markdown("#### Profile")
            with st.spinner("Loading profile from Notion..."):
                profile_md = fetch_page_body(rep.get("Page ID", ""))
            if profile_md and profile_md.strip():
                st.markdown(profile_md)
            else:
                st.caption("_No profile written yet._")

            st.divider()

            # Weekly reports — each as expander, with full body
            st.markdown("#### Weekly reports")
            if rep_weeks.empty:
                st.caption("_No weekly reports for this rep in the selected date range._")
            else:
                for _, week_row in rep_weeks.iterrows():
                    week_label = week_row.get("Report") or f"Week of {week_row['_week_dt'].date()}"
                    overall = week_row.get("Overall") or "n/a"
                    with st.expander(f"{week_label} · Overall: {overall}", expanded=False):
                        # Ratings row
                        r_cols = st.columns(5)
                        rating_map = [
                            ("Writing", "Writing & tone"),
                            ("Tech", "Technical accuracy"),
                            ("Escalation", "Escalation judgment"),
                            ("Troubleshooting", "Troubleshooting depth"),
                            ("Overall", "Overall"),
                        ]
                        for col, (label, key) in zip(r_cols, rating_map):
                            val = week_row.get(key) or "n/a"
                            col.markdown(f"**{label}**\n\n{val}")

                        st.markdown("")

                        # Metric row (pre-computed to avoid nested f-strings)
                        nc_val = week_row.get("New conversations")
                        frt_val = week_row.get("Median first response (m)")
                        ttc_val = week_row.get("Median time to close (h)")
                        csat_val = week_row.get("CSAT")
                        tix_val = week_row.get("Shortcut tickets filed")

                        nc_str = str(int(nc_val)) if pd.notna(nc_val) else "n/a"
                        frt_str = f"{frt_val:.1f} m" if pd.notna(frt_val) else "n/a"
                        ttc_str = f"{ttc_val:.1f} h" if pd.notna(ttc_val) else "n/a"
                        csat_str = f"{int(csat_val)}%" if pd.notna(csat_val) else "n/a"
                        tix_str = str(int(tix_val)) if pd.notna(tix_val) else "n/a"

                        m_cols2 = st.columns(5)
                        m_cols2[0].caption(f"New convos: {nc_str}")
                        m_cols2[1].caption(f"FRT: {frt_str}")
                        m_cols2[2].caption(f"TTC: {ttc_str}")
                        m_cols2[3].caption(f"CSAT: {csat_str}")
                        m_cols2[4].caption(f"Tickets: {tix_str}")

                        st.markdown("")

                        # Top strength + opportunity
                        if week_row.get("Top strength"):
                            st.markdown(f"**Top strength:** {week_row['Top strength']}")
                        if week_row.get("Top opportunity"):
                            st.markdown(f"**Top opportunity:** {week_row['Top opportunity']}")

                        st.markdown("")

                        # Full TLDR body from the weekly page
                        weekly_body = fetch_page_body(week_row.get("Page ID", ""))
                        if weekly_body and weekly_body.strip():
                            st.markdown(weekly_body)


def _render_coaching_roster_link():
    st.subheader("Coaching roster")
    st.markdown(
        f"Full coaching history with weekly TLDRs and per-rep profiles is in Notion: "
        f"[Open Support Team Coaching →]({ROSTER_DB_URL})"
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

    # Date filter at the top
    start, end, filtered_df = _render_date_filter(weekly_df)
    if filtered_df is None:
        filtered_df = weekly_df

    st.divider()
    _render_per_rep_tabs(roster_rows, filtered_df)
    st.divider()
    _render_coaching_roster_link()
