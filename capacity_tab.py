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

import html
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


# ---------------------------------------------------------------------------
# Per-rep table. Page only (the alert job doesn't need it), cached separately.
# ---------------------------------------------------------------------------

ET = ZoneInfo("America/New_York")
HOURS_START = 7  # hours online count from 7 AM ET unless away mode changed later than that
CSAT_WINDOW = 30 * 24 * 3600


def _search_all(token, conditions, max_pages=10):
    """Every conversation matching `conditions` (150 per page, capped at max_pages)."""
    out, cursor = [], None
    for _ in range(max_pages):
        pagination = {"per_page": 150}
        if cursor:
            pagination["starting_after"] = cursor
        body = {"query": {"operator": "AND", "value": conditions}, "pagination": pagination}
        r = requests.post(f"{INTERCOM_API}/conversations/search", headers=_headers(token), json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("conversations", []))
        cursor = ((data.get("pages") or {}).get("next") or {}).get("starting_after")
        if not cursor:
            break
    return out


def _away_mode_events(token, since):
    """{admin_id: [(created_at, away_mode), ...]} from the teammate activity log, oldest first."""
    events = {}
    url = f"{INTERCOM_API}/admins/activity_logs"
    params = {"created_at_after": since}
    for _ in range(5):
        r = requests.get(url, headers=_headers(token), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for log in data.get("activity_logs", []):
            if log.get("activity_type") != "admin_away_mode_change":
                continue
            away = (log.get("metadata") or {}).get("away_mode")
            aid = str((log.get("performed_by") or {}).get("id", ""))
            if away is None or not aid:
                continue
            events.setdefault(aid, []).append((int(log["created_at"]), bool(away)))
        nxt = (data.get("pages") or {}).get("next")
        if not nxt:
            break
        url, params = (nxt, None) if isinstance(nxt, str) else (url, {**params, "starting_after": nxt.get("starting_after")})
    for evs in events.values():
        evs.sort()
    return events


def _online_time(events, online_now, start, now):
    """(online_since, seconds online since `start`) from a rep's away mode events today."""
    # The first change today flipped the state, so before it the rep was online exactly when
    # that change turned away mode on. With no changes today, the current state held all day.
    online = events[0][1] if events else online_now
    t, total, since = start, 0, (start if online else None)
    for ts, away in events:
        ts = max(ts, start)
        if online:
            total += ts - t
        online, t = (not away), ts
        if online:
            since = ts
    if online:
        total += now - t
    return (since if online else None), total


def _median(values):
    values = sorted(values)
    if not values:
        return None
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


@st.cache_data(ttl=300, show_spinner=False)
def fetch_rep_stats(token, roster):
    now = int(time.time())
    now_et = datetime.fromtimestamp(now, ET)
    midnight = int(now_et.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    hours_start = int(now_et.replace(hour=HOURS_START, minute=0, second=0, microsecond=0).timestamp())

    r = requests.get(f"{INTERCOM_API}/admins", headers=_headers(token), timeout=20)
    r.raise_for_status()
    admins = {a.get("name"): a for a in r.json().get("admins", [])}
    reps = [(name, admins[name]) for name in roster if name in admins]

    def open_convs(aid):
        return _search_all(token, [
            {"field": "admin_assignee_id", "operator": "=", "value": aid},
            {"field": "state", "operator": "=", "value": "open"},
        ], max_pages=2)

    def snoozed(aid):
        return _count(token, [
            {"field": "admin_assignee_id", "operator": "=", "value": aid},
            {"field": "state", "operator": "=", "value": "snoozed"},
        ])

    def optional(fn, *args):
        try:
            return fn(*args)
        except requests.RequestException:
            return None  # shows as n/a; the rest of the table still loads

    online_reps = [(name, a) for name, a in reps if not a.get("away_mode_enabled")]
    window_start = hours_start if now >= hours_start else midnight

    def assigned(aid, *conds):
        return [{"field": "admin_assignee_id", "operator": "=", "value": aid}, *conds]

    # Only online reps get queried, and every query is scoped to that rep.
    jobs = {}
    with ThreadPoolExecutor(max_workers=16) as pool:
        for name, a in online_reps:
            aid = a["id"]
            jobs[name] = {
                "open": pool.submit(_search_all, token, assigned(aid, {"field": "state", "operator": "=", "value": "open"}), 2),
                "snoozed": pool.submit(_count, token, assigned(aid, {"field": "state", "operator": "=", "value": "snoozed"})),
                "today": pool.submit(_search_all, token, assigned(aid, {"field": "created_at", "operator": ">", "value": midnight}), 2),
                "closed": pool.submit(_search_all, token, assigned(aid, {"field": "statistics.last_close_at", "operator": ">", "value": midnight}), 2),
                "rated": pool.submit(optional, _search_all, token,
                                     assigned(aid, {"field": "conversation_rating.replied_at", "operator": ">", "value": now - CSAT_WINDOW}), 2),
            }
        f_events = pool.submit(optional, _away_mode_events, token, window_start) if online_reps else None
        results = {name: {k: f.result() for k, f in fs.items()} for name, fs in jobs.items()}
        events = f_events.result() if f_events else {}

    rows = []
    for name, a in reps:
        if name not in results:
            rows.append({"name": name, "online": False})
            continue
        aid = str(a["id"])
        res = results[name]
        mine = res["today"]
        frts = [
            (c.get("statistics") or {}).get("time_to_admin_reply")
            for c in mine
            if (c.get("statistics") or {}).get("time_to_admin_reply") is not None
        ]
        waits = [now - c["waiting_since"] for c in res["open"] if c.get("waiting_since")]
        closed_today = sum(
            1 for c in res["closed"]
            if str((c.get("statistics") or {}).get("last_closed_by_id")) == aid
        )
        csat = None
        if res["rated"] is not None:
            scores = [(c.get("conversation_rating") or {}).get("rating") for c in res["rated"]]
            scores = [s for s in scores if s is not None]
            csat = (round(100 * sum(1 for s in scores if s >= 4) / len(scores)), len(scores)) if scores else (None, 0)
        if events is None:
            online_since, online_secs = None, None
        else:
            online_since, online_secs = _online_time(events.get(aid, []), True, window_start, now)
        hours = None if online_secs is None else online_secs / 3600
        rows.append({
            "name": name,
            "online": True,
            "open": len(res["open"]),
            "snoozed": res["snoozed"],
            "longest_wait": max(waits) if waits else None,
            "frt": _median(frts),
            "new_15m": sum(1 for c in mine if c["created_at"] > now - 900),
            "new_1h": sum(1 for c in mine if c["created_at"] > now - 3600),
            "today": len(mine),
            "closed": closed_today,
            "online_since": online_since,
            "hours": hours,
            "per_hour": (len(mine) / hours) if hours and hours >= 0.25 else None,
            "csat": csat,
        })
    rows.sort(key=lambda r: (not r["online"], r["name"]))
    missing = [n for n in roster if n not in admins]
    return {"fetched_at": now, "rows": rows, "missing": missing, "activity_log": events is not None}


def _mins(seconds):
    if seconds is None:
        return "n/a"
    m = round(seconds / 60)
    return f"{m} min" if m < 60 else f"{m // 60}h {m % 60:02d}m"


def _render_rep_table(stats):
    cols = [
        ("Rep", "left"), ("Open", "center"), ("Snoozed", "center"), ("Longest wait", "center"),
        ("First response", "center"), ("New 15m", "center"), ("New 1h", "center"), ("Today", "center"),
        ("Closed today", "center"), ("Online since", "center"), ("Hours online", "center"),
        ("Per hour", "center"), ("CSAT 30d", "center"),
    ]
    th = "".join(
        f'<th style="text-align:{align};padding:10px 12px;color:#9E9E9E;font-size:14px;font-weight:600;'
        f'border-bottom:1px solid #444C56;white-space:nowrap;">{label}</th>'
        for label, align in cols
    )
    body = []
    for r in stats["rows"]:
        dot = "🟢" if r["online"] else "🔴"
        name_color = "#E0E0E0" if r["online"] else "#9E9E9E"
        if not r["online"]:
            body.append(
                f'<tr><td style="text-align:left;padding:14px 12px;font-size:22px;border-bottom:1px solid #373E47;'
                f'white-space:nowrap;">{dot} <span style="color:{name_color};font-weight:700;">{html.escape(r["name"])}</span></td>'
                f'<td colspan="{len(cols) - 1}" style="padding:14px 12px;font-size:16px;color:#9E9E9E;'
                f'border-bottom:1px solid #373E47;">Away</td></tr>'
            )
            continue
        since = (
            datetime.fromtimestamp(r["online_since"], ET).strftime("%-I:%M %p") if r["online_since"]
            else ("n/a" if r["hours"] is None else "Away")
        )
        if r["csat"] is None:
            csat = "n/a"
        elif r["csat"][0] is None:
            csat = "No ratings"
        else:
            csat = f'{r["csat"][0]}% <span style="color:#9E9E9E;font-size:14px;">({r["csat"][1]})</span>'
        wait_color = "#ff5252" if (r["longest_wait"] or 0) >= 3600 else ("#FFD740" if (r["longest_wait"] or 0) >= 1800 else "inherit")
        cells = [
            f'{dot} <span style="color:{name_color};font-weight:700;">{html.escape(r["name"])}</span>',
            r["open"], r["snoozed"],
            f'<span style="color:{wait_color};">{_mins(r["longest_wait"])}</span>' if r["longest_wait"] else "None",
            _mins(r["frt"]), r["new_15m"], r["new_1h"], r["today"], r["closed"],
            since,
            "n/a" if r["hours"] is None else f'{r["hours"]:.1f}',
            "n/a" if r["per_hour"] is None else f'{r["per_hour"]:.1f}',
            csat,
        ]
        tds = "".join(
            f'<td style="text-align:{align};padding:14px 12px;font-size:22px;border-bottom:1px solid #373E47;'
            f'white-space:nowrap;">{cell}</td>'
            for cell, (_, align) in zip(cells, cols)
        )
        body.append(f"<tr>{tds}</tr>")
    st.markdown(
        '<div style="overflow-x:auto;background:#2D333B;border:1px solid #444C56;border-radius:12px;'
        'padding:6px 8px;margin-bottom:16px;">'
        f'<table style="width:100%;border-collapse:collapse;color:#E0E0E0;"><thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table></div>',
        unsafe_allow_html=True,
    )
    notes = [
        "Stats load for online reps only. Counts are conversations currently assigned to each rep. First response is today's median.",
        f"Longest wait is the open conversation waiting longest on a reply (yellow 30 min, red 1 hour).",
        f"Hours online count from {HOURS_START} AM ET or the first away mode change today. Per hour is today's tickets ÷ hours online.",
        "CSAT is the share of 4 and 5 star ratings over the last 30 days, with the number of ratings.",
    ]
    if not stats["activity_log"]:
        notes.append("Hours online need the Intercom activity log, which this token can't read yet.")
    st.caption(" ".join(notes))


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
            fetch_rep_stats.clear()

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

    st.markdown("### Reps")
    try:
        _render_rep_table(fetch_rep_stats(token, roster))
    except requests.RequestException as e:
        st.error(f"Could not load rep stats: {e}")

    st.markdown("### Team")
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

    if snap["missing"]:
        st.caption("Not found in Intercom (check CAPACITY_REPS): " + ", ".join(snap["missing"]))
    st.caption(
        "Updated "
        + datetime.fromtimestamp(snap["fetched_at"], ZoneInfo("America/New_York")).strftime("%-I:%M %p ET")
        + ". Data refreshes every 5 minutes."
    )

    _render_calibration()
