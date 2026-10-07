"""
Team Capacity view — live "overwhelm score" for the support team.

Score = (4 × recent inflow + 2 × open queue + 0.1 × open backlog) ÷ reps available

  recent inflow     new conversations per hour, blended across the last 15 minutes, 1 hour,
                    2 hours, and 4 hours (40/30/20/10 weights, most recent heaviest).
                    Fin-only conversations count as a quarter. Auto-generated emails and spam
                    (closed with no teammate reply and no Fin) don't count.
  open queue        conversations assigned to available reps that are open right now (not snoozed)
  open backlog      conversations created in the last 7 days that are still open (includes snoozed)
  reps available    roster reps not in Intercom away mode

Oct 2026: reweighted so recent volume and what reps actually have open drive the score,
not the 7 day backlog. The Sep 2026 calibration (backlog driven) no longer applies, so the
red and yellow thresholds need re-checking against the new formula.

Requires INTERCOM_ACCESS_TOKEN on the Railway environment (read conversations + read admins).
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
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
NEW_WINDOW = 3600  # window for the "new in the last hour" cards and the alert summary
# (window in seconds, label, weight). Weights sum to 1, most recent heaviest.
INFLOW_WINDOWS = [
    (15 * 60, "15m", 0.4),
    (3600, "1h", 0.3),
    (2 * 3600, "2h", 0.2),
    (4 * 3600, "4h", 0.1),
]
FIN_FRACTION = 0.25  # a Fin-only conversation counts as a quarter of a human-handled one
INFLOW_WEIGHT = 4.0
QUEUE_WEIGHT = 2.0
BACKLOG_WEIGHT = 0.1
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


def inflow_rate(inflow):
    """Blended new conversations per hour. inflow maps window label to (human, fin_only) counts."""
    rate = 0.0
    for seconds, label, weight in INFLOW_WINDOWS:
        human, fin = inflow[label]
        rate += weight * (human + FIN_FRACTION * fin) * 3600 / seconds
    return rate


def compute_score(snap):
    """Return (score, band). Score is None when no reps are available."""
    available = len(snap["available"])
    if available <= 0:
        return None, "red"
    queue = sum(load["open"] for load in snap["rep_load"].values())
    score = (
        INFLOW_WEIGHT * inflow_rate(snap["inflow"])
        + QUEUE_WEIGHT * queue
        + BACKLOG_WEIGHT * snap["backlog"]
    ) / available
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


def _new_counts(token, since):
    """(human-handled, Fin-only) conversations created after `since`.

    Auto-generated emails and spam don't count. Those get closed with no teammate reply and
    no Fin involvement, so anything matching that is dropped before splitting human vs. Fin.
    """
    created = {"field": "created_at", "operator": ">", "value": since}
    fin = {"field": "ai_agent_participated", "operator": "=", "value": True}
    no_fin = {"field": "ai_agent_participated", "operator": "=", "value": False}
    closed = {"field": "state", "operator": "=", "value": "closed"}
    replied = {"field": "statistics.first_admin_reply_at", "operator": ">", "value": 0}
    queries = {
        "total": [created],
        "fin": [created, fin],
        "fin_replied": [created, fin, replied],
        "closed_no_fin": [created, no_fin, closed],
        "closed_no_fin_replied": [created, no_fin, closed, replied],
    }
    with ThreadPoolExecutor(max_workers=len(queries)) as pool:
        futures = {k: pool.submit(_count, token, q) for k, q in queries.items()}
        n = {k: f.result() for k, f in futures.items()}
    fin_only = max(n["fin"] - n["fin_replied"], 0)
    junk = max(n["closed_no_fin"] - n["closed_no_fin_replied"], 0)
    return max(n["total"] - fin_only - junk, 0), fin_only


@st.cache_data(ttl=300, show_spinner=False)
def fetch_snapshot(token, roster):
    return snapshot(token, roster)


def snapshot(token, roster):
    """Live Intercom numbers behind the score. Uncached so capacity_alert.py can reuse it."""
    now = int(time.time())
    since_backlog = now - BACKLOG_WINDOW

    backlog = _count(token, [
        {"field": "created_at", "operator": ">", "value": since_backlog},
        {"field": "open", "operator": "=", "value": True},
    ])
    with ThreadPoolExecutor(max_workers=len(INFLOW_WINDOWS)) as pool:
        futures = {label: pool.submit(_new_counts, token, now - seconds) for seconds, label, _ in INFLOW_WINDOWS}
        inflow = {label: f.result() for label, f in futures.items()}
    human_new, fin_only = inflow["1h"]

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

    # Current load for each available rep: conversations assigned to them right now.
    rep_load = {}
    for name in available:
        aid = admins[name].get("id")
        rep_load[name] = {
            state: _count(token, [
                {"field": "admin_assignee_id", "operator": "=", "value": aid},
                {"field": "state", "operator": "=", "value": state},
            ])
            for state in ("open", "snoozed")
        }

    return {
        "fetched_at": now,
        "backlog": backlog,
        "human_new": human_new,
        "fin_new": fin_only,
        "inflow": inflow,
        "available": available,
        "rep_load": rep_load,
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
**Score = ({INFLOW_WEIGHT:g} × recent inflow + {QUEUE_WEIGHT:g} × open queue + {BACKLOG_WEIGHT:g} × open backlog) ÷ reps available**

- **Recent inflow:** new conversations per hour, blended across the last 15 minutes (40%), 1 hour (30%), 2 hours (20%), and 4 hours (10%). Fin-only conversations count as a quarter. Auto-generated emails and spam (closed with no teammate reply and no Fin) don't count.
- **Open queue:** conversations assigned to available reps that are open right now. Snoozed ones don't count.
- **Open backlog:** conversations created in the last 7 days that are still open, including snoozed. Lightly weighted.
- **Reps available:** roster reps not in Intercom away mode.

**Bands:** red at {RED_AT}+, yellow {YELLOW_AT} to {RED_AT}, green under {YELLOW_AT}.

Reweighted Oct 2026 so recent volume and open work drive the score instead of the 7 day backlog.
The thresholds still come from the Sep 2026 calibration of the old formula and need re-checking.
The table below shows the old formula's scores at the six times the team flagged very high capacity.
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
    score, band = compute_score(snap)
    queue = sum(load["open"] for load in snap["rep_load"].values())
    inflow = snap["inflow"]
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
    c1.markdown(
        _card(
            "New per hour",
            f"{inflow_rate(inflow):.1f}",
            "Human-handled  " + "  ·  ".join(f"{label}: {inflow[label][0]}" for _, label, _ in INFLOW_WINDOWS),
        ),
        unsafe_allow_html=True,
    )
    c2.markdown(_card("Open queue", queue, "open now, snoozed excluded"), unsafe_allow_html=True)
    c3.markdown(_card("Open backlog", snap["backlog"], "last 7 days, lightly weighted"), unsafe_allow_html=True)
    c4.markdown(
        _card("Reps available", f"{n_avail} of {len(roster)}", ", ".join(snap["available"]) or "none"),
        unsafe_allow_html=True,
    )

    if snap["available"]:
        load = snap.get("rep_load", {})
        st.caption(
            "Available mode: "
            + ", ".join(
                f"{n} ({load[n]['open']} open, {load[n]['snoozed']} snoozed)" if n in load else n
                for n in snap["available"]
            )
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
