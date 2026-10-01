"""
Team Capacity view — live "overwhelm score" for the support team.

Score = (open backlog + 4 × human-handled new + 1 × Fin-only new) ÷ reps available

  open backlog      conversations created in the last 7 days that are still open (includes snoozed)
  human-handled new conversations created in the last hour, minus Fin-only ones
  Fin-only new      created in the last hour, Fin participated, no teammate reply yet
  reps available    roster reps not in Intercom away mode

Calibrated Sep 2026 against 6 "very high capacity" alerts from the support team
(Apr to Sep 2026) and 11 same-weekday baselines. Red (40+) caught 5 of 6 alerts
with 1 false positive. Yellow (30 to 40) is a watch zone.

Requires INTERCOM_ACCESS_TOKEN on the Railway environment (read conversations + read admins).
"""

import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

INTERCOM_API = "https://api.intercom.io"
INTERCOM_VERSION = "2.11"

DEFAULT_REPS = [
    "Pablo Coppola",
    "Lauren Stumpf",
    "Taliyah",
    "Dylan",
    "Matthew Hernandez",
    "Lena",
    "Will Hess",
]

BACKLOG_WINDOW = 7 * 24 * 3600
NEW_WINDOW = 3600
HUMAN_WEIGHT = 4.0
FIN_WEIGHT = 1.0  # a quarter of a human-handled conversation
RED_AT = 40
YELLOW_AT = 30

BAND_COLORS = {"red": "#ff5252", "yellow": "#FFD740", "green": "#00E676"}

# Historical calibration points (score at the moment the team flagged high capacity).
CALIBRATION = [
    ("Tue Apr 7, 2:58 PM", 26, 9, 0, 1, 62.0),
    ("Fri Jun 26, 12:27 PM", 38, 9, 1, 4, 18.8),
    ("Thu Aug 13, 12:31 PM", 40, 5, 1, 1, 61.0),
    ("Mon Aug 17, 5:09 PM", 41, 7, 0, 1, 69.0),
    ("Fri Sep 25, 3:39 PM", 31, 12, 2, 2, 40.5),
    ("Mon Sep 28, 4:04 PM", 38, 9, 0, 1, 74.0),
]


def _roster():
    raw = os.environ.get("CAPACITY_REPS", "")
    names = [n.strip() for n in raw.split(",") if n.strip()]
    return names or DEFAULT_REPS


def compute_score(backlog, human_new, fin_new, available):
    """Return (score, band). Score is None when no reps are available."""
    if available <= 0:
        return None, "red"
    score = (backlog + HUMAN_WEIGHT * human_new + FIN_WEIGHT * fin_new) / available
    if score >= RED_AT:
        band = "red"
    elif score >= YELLOW_AT:
        band = "yellow"
    else:
        band = "green"
    return round(score, 1), band


def _headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Intercom-Version": INTERCOM_VERSION,
    }


def _count(token, conditions):
    body = {
        "query": {"operator": "AND", "value": conditions},
        "pagination": {"per_page": 1},
    }
    r = requests.post(f"{INTERCOM_API}/conversations/search", headers=_headers(token), json=body, timeout=20)
    r.raise_for_status()
    return int(r.json().get("total_count", 0))


@st.cache_data(ttl=300, show_spinner=False)
def fetch_snapshot(token, roster):
    now = int(time.time())
    since_backlog = now - BACKLOG_WINDOW
    since_new = now - NEW_WINDOW

    backlog = _count(token, [
        {"field": "created_at", "operator": ">", "value": since_backlog},
        {"field": "open", "operator": "=", "value": True},
    ])
    new_total = _count(token, [
        {"field": "created_at", "operator": ">", "value": since_new},
    ])
    fin_participated = _count(token, [
        {"field": "created_at", "operator": ">", "value": since_new},
        {"field": "ai_agent_participated", "operator": "=", "value": True},
    ])
    fin_with_human = _count(token, [
        {"field": "created_at", "operator": ">", "value": since_new},
        {"field": "ai_agent_participated", "operator": "=", "value": True},
        {"field": "statistics.first_admin_reply_at", "operator": ">", "value": 0},
    ])
    fin_only = max(fin_participated - fin_with_human, 0)
    human_new = max(new_total - fin_only, 0)

    r = requests.get(f"{INTERCOM_API}/admins", headers=_headers(token), timeout=20)
    r.raise_for_status()
    admins = {a.get("name"): a for a in r.json().get("admins", [])}
    available, away, missing = [], [], []
    for name in roster:
        a = admins.get(name)
        if a is None:
            missing.append(name)
        elif a.get("away_mode_enabled"):
            away.append(name)
        else:
            available.append(name)

    return {
        "fetched_at": now,
        "backlog": backlog,
        "human_new": human_new,
        "fin_new": fin_only,
        "available": available,
        "away": away,
        "missing": missing,
    }


def _card(label, value, sub=None, color="#E0E0E0"):
    sub_html = f'<div style="color:#9E9E9E;font-size:12px;margin-top:2px;">{sub}</div>' if sub else ""
    return (
        f'<div style="background:#373E47;border:1px solid #444C56;border-radius:10px;'
        f'padding:14px 16px;text-align:center;">'
        f'<div style="color:#9E9E9E;font-size:12px;">{label}</div>'
        f'<div style="color:{color};font-size:26px;font-weight:700;margin:4px 0;">{value}</div>'
        f'{sub_html}</div>'
    )


def _render_calibration():
    with st.expander("How the score works"):
        st.markdown(
            f"""
**Score = (open backlog + {HUMAN_WEIGHT:g} × human-handled new + {FIN_WEIGHT:g} × Fin-only new) ÷ reps available**

- **Open backlog:** conversations created in the last 7 days that are still open, including snoozed.
- **Human-handled new:** conversations created in the last hour, excluding Fin-only ones.
- **Fin-only new:** created in the last hour, Fin participated, and no teammate has replied yet. Weighted at a quarter of a human-handled conversation.
- **Reps available:** roster reps not in Intercom away mode.

**Bands:** red at {RED_AT}+, yellow {YELLOW_AT} to {RED_AT}, green under {YELLOW_AT}.

Calibrated against the six times the team flagged very high capacity (Apr to Sep 2026) and 11 normal moments at the same weekday and time.
Red caught 5 of 6 alerts. One normal moment (Mon Aug 10, one rep available) scored 42; the rest scored 7 to 34.
Sudden spikes with several reps online (like Jun 26) can still score low.
"""
        )
        rows = [
            {"Alert": a, "Backlog": b, "Human new (1h)": h, "Fin-only new (1h)": f, "Reps available": r, "Score": s}
            for a, b, h, f, r, s in CALIBRATION
        ]
        st.dataframe(rows, hide_index=True, width="stretch")


def render():
    st.markdown("## Team Capacity")

    token = os.environ.get("INTERCOM_ACCESS_TOKEN", "")
    if not token:
        st.warning(
            "Intercom access token not configured. Set `INTERCOM_ACCESS_TOKEN` on the Railway environment "
            "(needs read access to conversations and admins)."
        )
        _render_calibration()
        return

    st_autorefresh(interval=300_000, key="capacity_refresh")
    roster = tuple(_roster())
    col_refresh, _ = st.columns([1, 5])
    with col_refresh:
        if st.button("Refresh now", key="cap_refresh"):
            fetch_snapshot.clear()

    try:
        snap = fetch_snapshot(token, roster)
    except requests.RequestException as e:
        st.error(f"Could not load Intercom data: {e}")
        _render_calibration()
        return

    n_avail = len(snap["available"])
    score, band = compute_score(snap["backlog"], snap["human_new"], snap["fin_new"], n_avail)
    color = BAND_COLORS[band]
    label = {"red": "Overwhelmed", "yellow": "Watch", "green": "OK"}[band]
    score_txt = "No reps available" if score is None else f"{score:g}"

    st.markdown(
        f'<div style="background:#373E47;border:2px solid {color};border-radius:12px;'
        f'padding:20px;text-align:center;margin-bottom:16px;">'
        f'<div style="color:#9E9E9E;font-size:13px;">Overwhelm score</div>'
        f'<div style="color:{color};font-size:48px;font-weight:800;line-height:1.1;">{score_txt}</div>'
        f'<div style="color:{color};font-size:16px;font-weight:600;">{label}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(_card("Open backlog", snap["backlog"], "created in last 7 days"), unsafe_allow_html=True)
    c2.markdown(_card("Human-handled new", snap["human_new"], "last hour"), unsafe_allow_html=True)
    c3.markdown(_card("Fin-only new", snap["fin_new"], "last hour"), unsafe_allow_html=True)
    c4.markdown(
        _card("Reps available", f"{n_avail} of {len(roster)}", ", ".join(snap["available"]) or "none"),
        unsafe_allow_html=True,
    )

    if snap["away"]:
        st.caption("In away mode: " + ", ".join(snap["away"]))
    if snap["missing"]:
        st.caption("Not found in Intercom (check CAPACITY_REPS): " + ", ".join(snap["missing"]))
    st.caption(
        "Updated "
        + datetime.fromtimestamp(snap["fetched_at"], ZoneInfo("America/New_York")).strftime("%-I:%M %p ET")
        + ". Data refreshes every 5 minutes."
    )

    _render_calibration()
