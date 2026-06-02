"""
L2 Rep Coaching views — Manager tab and Rep-facing tab.

Data is prepared by l2_dashboard.py and passed in as a DataFrame.

Expected columns (after live-map patching):
  name, category, decision, l2_engineer, l2_involvement,
  _level (int 1-5 or None), _month (str "YYYY-MM" or "NaT"),
  _parsed_date (datetime or NaT), created_at, shortcut_url (optional)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

REPS = ["Jayson", "Sean"]

LEVEL_LABELS = {
    5: "Independent",
    4: "Near-Complete",
    3: "Framework",
    2: "Enrichment",
    1: "Escalated",
}
LEVEL_COLORS = {
    5: "#00E676",
    4: "#69F0AE",
    3: "#FFD740",
    2: "#FF9800",
    1: "#ff5252",
}
_CHART_BASE = dict(
    plot_bgcolor="#373E47",
    paper_bgcolor="#2D333B",
    font=dict(color="#E0E0E0"),
    margin=dict(l=10, r=10, t=20, b=10),
)


def _lc(val):
    """Return hex color for a numeric level (or average)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "#444C56"
    return LEVEL_COLORS.get(int(round(val)), "#636b75")


def _stat_card(label, value, sub=None, color="#E0E0E0"):
    sub_html = f'<div style="color:#9E9E9E;font-size:12px;margin-top:2px;">{sub}</div>' if sub else ""
    return (
        f'<div style="background:#373E47;border:1px solid #444C56;border-radius:10px;'
        f'padding:14px 16px;text-align:center;">'
        f'<div style="color:#9E9E9E;font-size:12px;">{label}</div>'
        f'<div style="color:{color};font-size:26px;font-weight:700;margin:4px 0;">{value}</div>'
        f'{sub_html}</div>'
    )


# ═══════════════════════════════════════════════════════════════════════════
# MANAGER VIEW
# ═══════════════════════════════════════════════════════════════════════════

def render_manager(df: pd.DataFrame):
    if df is None or df.empty:
        st.info("No results data yet — run an analysis first.")
        return

    involved       = df[df["l2_engineer"].isin(REPS)].copy()
    all_supportable = df[df["decision"] == "L2 Can Support"].copy()

    # ── 1. Rep scorecards ────────────────────────────────────────────────
    st.markdown("### Rep overview")
    cols = st.columns(len(REPS))
    for col, rep in zip(cols, REPS):
        rep_df = involved[involved["l2_engineer"] == rep]
        levels = rep_df["_level"].dropna()
        total  = len(rep_df)
        avg    = levels.mean() if len(levels) else 0
        pct_hi = (levels >= 4).sum() / len(levels) * 100 if len(levels) else 0
        pct_lo = (levels <= 1).sum() / len(levels) * 100 if len(levels) else 0

        # Month-over-month delta
        dated = rep_df.dropna(subset=["_month", "_level"])
        months = sorted(dated["_month"].unique())
        delta_html = ""
        if len(months) >= 2:
            prev = dated[dated["_month"] == months[-2]]["_level"].mean()
            curr = dated[dated["_month"] == months[-1]]["_level"].mean()
            if pd.notna(prev) and pd.notna(curr):
                diff = curr - prev
                arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
                clr   = "#00E676" if diff > 0 else ("#ff5252" if diff < 0 else "#9E9E9E")
                delta_html = (
                    f'<div style="color:{clr};font-size:12px;margin-top:6px;">'
                    f'{arrow} {abs(diff):.1f} vs last month</div>'
                )

        col.markdown(
            f'<div style="background:#373E47;border:1px solid #444C56;border-radius:10px;'
            f'padding:18px 20px;">'
            f'<div style="color:#9E9E9E;font-size:13px;font-weight:600;margin-bottom:4px;">{rep}</div>'
            f'<div style="color:#E0E0E0;font-size:36px;font-weight:700;line-height:1;">{total}</div>'
            f'<div style="color:#636b75;font-size:11px;margin-bottom:10px;">tickets</div>'
            f'<div style="color:{_lc(avg)};font-size:20px;font-weight:700;">Avg {avg:.1f}/5</div>'
            f'{delta_html}'
            f'<div style="margin-top:8px;font-size:12px;color:#9E9E9E;">'
            f'🟢 {pct_hi:.0f}% high-impact &nbsp;·&nbsp; 🔴 {pct_lo:.0f}% escalated</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 2. Growth trajectory ────────────────────────────────────────────
    st.markdown("### Support level over time")
    st.caption("Are they improving? Each point is the average support level that month.")
    dated = involved.dropna(subset=["_month", "_level"])
    if not dated.empty:
        fig = go.Figure()
        for rep in REPS:
            sub = (
                dated[dated["l2_engineer"] == rep]
                .groupby("_month")["_level"].mean()
                .reset_index()
                .sort_values("_month")
            )
            fig.add_trace(go.Scatter(
                x=sub["_month"], y=sub["_level"],
                mode="lines+markers", name=rep,
                line=dict(width=2), marker=dict(size=7),
            ))
        fig.update_layout(
            **_CHART_BASE, height=300,
            yaxis=dict(
                range=[0.5, 5.5],
                tickvals=[1, 2, 3, 4, 5],
                ticktext=["1 – Escalated", "2 – Enrichment", "3 – Framework",
                          "4 – Near-Complete", "5 – Independent"],
                gridcolor="#444C56",
            ),
            xaxis=dict(gridcolor="#444C56"),
            legend=dict(orientation="h", y=1.12, x=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("Not enough dated tickets for a trend chart yet.")

    st.divider()

    # ── 3. Category performance matrix ─────────────────────────────────
    st.markdown("### Category performance matrix")
    st.caption(
        "Average support level per rep per category. "
        "Green = handles independently · Yellow = framework-level · Red = needs support."
    )

    if not involved.empty and "category" in involved.columns:
        pivot = (
            involved.dropna(subset=["_level"])
            .pivot_table(index="category", columns="l2_engineer",
                         values="_level", aggfunc="mean")
        )
        counts = involved.groupby("category").size().rename("tickets")
        pivot  = pivot.join(counts).sort_values("tickets", ascending=False)

        header = (
            '<table style="width:100%;border-collapse:collapse;font-size:13px;">'
            '<thead><tr style="border-bottom:2px solid #636b75;">'
            '<th style="padding:8px 12px;text-align:left;color:#9E9E9E;">Category</th>'
        )
        for rep in REPS:
            if rep in pivot.columns:
                header += f'<th style="padding:8px 12px;text-align:center;color:#9E9E9E;">{rep}</th>'
        header += '<th style="padding:8px 12px;text-align:center;color:#9E9E9E;">Tickets</th></tr></thead><tbody>'

        rows_html = ""
        for cat, row in pivot.iterrows():
            rows_html += '<tr style="border-bottom:1px solid #373E47;">'
            rows_html += f'<td style="padding:8px 12px;color:#E0E0E0;">{cat}</td>'
            for rep in REPS:
                val = row.get(rep)
                if pd.notna(val):
                    cell = f'<span style="color:{_lc(val)};font-weight:600;">{val:.1f}</span>'
                else:
                    cell = '<span style="color:#444C56;">—</span>'
                rows_html += f'<td style="padding:8px 12px;text-align:center;">{cell}</td>'
            rows_html += (
                f'<td style="padding:8px 12px;text-align:center;color:#9E9E9E;">'
                f'{int(row.get("tickets", 0))}</td></tr>'
            )

        st.markdown(header + rows_html + "</tbody></table>", unsafe_allow_html=True)
    else:
        st.caption("No category data available.")

    st.divider()

    # ── 4. Falling through the cracks ──────────────────────────────────
    st.markdown("### Falling through the cracks")
    st.caption(
        "L2 Can Support tickets where neither rep got involved. "
        "These are the clearest missed coaching opportunities."
    )

    gap_df = all_supportable[all_supportable["l2_engineer"] == "None"]
    if not gap_df.empty and "category" in gap_df.columns:
        gap_by_cat = (
            gap_df.groupby("category").size()
            .reset_index(name="unclaimed")
            .sort_values("unclaimed", ascending=False)
        )
        g_cols = st.columns(3)
        for i, (_, row) in enumerate(gap_by_cat.iterrows()):
            g_cols[i % 3].markdown(
                f'<div style="background:#2a1515;border:1px solid #ff5252;border-radius:8px;'
                f'padding:12px 14px;margin-bottom:8px;">'
                f'<div style="color:#ff5252;font-size:24px;font-weight:700;">{row["unclaimed"]}</div>'
                f'<div style="color:#E0E0E0;font-size:13px;">{row["category"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    else:
        st.success("No gaps — every supportable ticket has L2 involvement. 🎉")

    st.divider()

    # ── 5. Untouched categories ────────────────────────────────────────
    st.markdown("### Categories neither rep has engaged with")
    st.caption("In-scope for L2 but zero involvement from Jayson or Sean — potential growth roadmap.")

    if "category" in df.columns:
        touched   = set(involved["category"].dropna().unique())
        in_scope  = set(all_supportable["category"].dropna().unique())
        untouched = in_scope - touched
        if untouched:
            unt_counts = (
                all_supportable[all_supportable["category"].isin(untouched)]
                .groupby("category").size().sort_values(ascending=False)
            )
            for cat, cnt in unt_counts.items():
                st.markdown(
                    f'<div style="display:inline-block;background:#2D333B;border:1px solid #636b75;'
                    f'border-radius:6px;padding:4px 12px;margin:4px;font-size:13px;color:#E0E0E0;">'
                    f'{cat} <span style="color:#636b75;">({cnt})</span></div>',
                    unsafe_allow_html=True,
                )
        else:
            st.success("Both reps have engaged with every in-scope category.")


# ═══════════════════════════════════════════════════════════════════════════
# REP-FACING VIEW
# ═══════════════════════════════════════════════════════════════════════════

def render_rep(df: pd.DataFrame):
    if df is None or df.empty:
        st.info("No results data yet — run an analysis first.")
        return

    rep = st.selectbox("Select rep", REPS, key="l2_rep_selector")

    rep_df          = df[df["l2_engineer"] == rep].copy()
    all_supportable = df[df["decision"] == "L2 Can Support"].copy()
    levels          = rep_df["_level"].dropna()

    # ── 1. Stats row ────────────────────────────────────────────────────
    total  = len(rep_df)
    avg    = levels.mean() if len(levels) else 0
    pct_hi = (levels >= 4).sum() / len(levels) * 100 if len(levels) else 0
    pct_lo = (levels <= 1).sum() / len(levels) * 100 if len(levels) else 0

    s1, s2, s3, s4 = st.columns(4)
    s1.markdown(_stat_card("Tickets worked", total), unsafe_allow_html=True)
    s2.markdown(_stat_card("Avg support level", f"{avg:.1f}/5", color=_lc(avg)), unsafe_allow_html=True)
    s3.markdown(_stat_card("High-impact (4–5)", f"{pct_hi:.0f}%", color="#00E676"), unsafe_allow_html=True)
    s4.markdown(_stat_card("Escalated (1)", f"{pct_lo:.0f}%", color="#ff5252"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 2. Level distribution ───────────────────────────────────────────
    st.markdown("### Support level breakdown")
    if not levels.empty:
        lv_counts = [int((levels == i).sum()) for i in range(1, 6)]
        fig = go.Figure(go.Bar(
            x=[LEVEL_LABELS[i] for i in range(1, 6)],
            y=lv_counts,
            marker_color=[LEVEL_COLORS[i] for i in range(1, 6)],
            text=lv_counts, textposition="outside",
        ))
        fig.update_layout(
            **_CHART_BASE, height=260, showlegend=False,
            yaxis=dict(gridcolor="#444C56"),
            xaxis=dict(gridcolor="#444C56"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No level data yet.")

    st.divider()

    # ── 3. Strengths & growth areas ─────────────────────────────────────
    if "category" in rep_df.columns:
        cat_stats = (
            rep_df.dropna(subset=["_level"])
            .groupby("category")
            .agg(avg_level=("_level", "mean"), count=("_level", "size"))
            .reset_index()
        )
        strengths = cat_stats[cat_stats["avg_level"] >= 3.5].sort_values("avg_level", ascending=False)
        growth    = cat_stats[(cat_stats["avg_level"] < 3) & (cat_stats["count"] >= 2)].sort_values("avg_level")

        left, right = st.columns(2)

        with left:
            st.markdown("### ✅ Strengths")
            st.caption("Categories where you consistently land at 3.5 or above.")
            if strengths.empty:
                st.caption("Keep building — more data needed to identify strengths.")
            for _, row in strengths.iterrows():
                c = _lc(row["avg_level"])
                st.markdown(
                    f'<div style="background:#373E47;border-left:4px solid {c};'
                    f'border-radius:6px;padding:10px 14px;margin-bottom:8px;">'
                    f'<div style="display:flex;justify-content:space-between;">'
                    f'<span style="color:#E0E0E0;font-size:13px;">{row["category"]}</span>'
                    f'<span style="color:{c};font-weight:700;">{row["avg_level"]:.1f}</span></div>'
                    f'<div style="color:#636b75;font-size:11px;">{int(row["count"])} tickets</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        with right:
            st.markdown("### 🎯 Growth areas")
            st.caption("Categories you've worked but are averaging below 3 — biggest coaching targets.")
            if growth.empty:
                st.caption("No consistent weak spots identified yet.")
            for _, row in growth.iterrows():
                c = _lc(row["avg_level"])
                st.markdown(
                    f'<div style="background:#373E47;border-left:4px solid {c};'
                    f'border-radius:6px;padding:10px 14px;margin-bottom:8px;">'
                    f'<div style="display:flex;justify-content:space-between;">'
                    f'<span style="color:#E0E0E0;font-size:13px;">{row["category"]}</span>'
                    f'<span style="color:{c};font-weight:700;">{row["avg_level"]:.1f}</span></div>'
                    f'<div style="color:#636b75;font-size:11px;">{int(row["count"])} tickets</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    # ── 4. Within reach — never tried ──────────────────────────────────
    st.markdown("### 🔍 Within reach — haven't tried yet")
    st.caption(
        "Ticket types that are in scope for L2 but you haven't worked on. "
        "Good next challenges to build toward."
    )

    if "category" in df.columns:
        touched    = set(rep_df["category"].dropna().unique())
        in_scope   = set(all_supportable["category"].dropna().unique())
        unexplored = in_scope - touched
        if unexplored:
            exp_counts = (
                all_supportable[all_supportable["category"].isin(unexplored)]
                .groupby("category").size().sort_values(ascending=False)
            )
            for cat, cnt in exp_counts.items():
                st.markdown(
                    f'<div style="display:inline-block;background:#2D333B;border:1px solid #42A5F5;'
                    f'border-radius:6px;padding:5px 14px;margin:4px;font-size:13px;color:#E0E0E0;">'
                    f'{cat} <span style="color:#636b75;">({cnt} tickets)</span></div>',
                    unsafe_allow_html=True,
                )
        else:
            st.success("You've engaged with every in-scope category. Nice work!")

    st.divider()

    # ── 5. Recent activity ──────────────────────────────────────────────
    st.markdown("### Recent tickets")
    recent = rep_df.sort_values("_parsed_date", ascending=False).head(15)
    if not recent.empty:
        show = [c for c in ["created_at", "name", "category", "l2_involvement", "decision", "shortcut_url"]
                if c in recent.columns]
        disp = recent[show].copy()
        rename = {"created_at": "Filed", "name": "Ticket", "category": "Category",
                  "l2_involvement": "Support Level", "decision": "Decision", "shortcut_url": "Link"}
        disp = disp.rename(columns=rename)
        col_cfg = {}
        if "Link" in disp.columns:
            col_cfg["Link"] = st.column_config.LinkColumn("Link", display_text="Open", width="small")
        if "Filed" in disp.columns:
            col_cfg["Filed"] = st.column_config.DateColumn("Filed", width="small")
        st.dataframe(disp, use_container_width=True, hide_index=True, column_config=col_cfg)
    else:
        st.caption("No recent tickets.")
