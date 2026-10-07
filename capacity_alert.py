"""
Team Capacity Slack alert. Runs on a Railway cron every 30 minutes (see railway.alert.toml).

Weekdays 8am to 6pm ET it computes the same overwhelm score as the Team Capacity page
and posts to a private Slack channel when the band rises:
  yellow (30+)  heads-up
  red (40+)     escalation
  back to green all-clear, once
Each yellow or red alert includes a one-line Claude read of what's coming in.

Every check is logged to the "Capacity Alerts" tab of the dashboard's Google Sheet, which
is also how the job remembers the previous band between runs.

Env vars:
  INTERCOM_ACCESS_TOKEN          same token as the dashboard
  SLACK_CAPACITY_WEBHOOK_URL     incoming webhook for the private alert channel
  ANTHROPIC_API_KEY              for the one-line summary (optional; alert still sends without it)
  GOOGLE_SERVICE_ACCOUNT_JSON    same service account as the dashboard
  DASHBOARD_URL                  base URL of the dashboard, for the link in the alert (optional)
  CAPACITY_REPS                  same roster override as the dashboard (optional)

Flags for testing:
  --force    run outside business hours
  --dry-run  print the Slack message instead of posting, and don't write to the sheet
"""

import html
import json
import os
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import anthropic
import gspread
import requests
from google.oauth2.service_account import Credentials

from capacity_tab import (
    INTERCOM_API,
    NEW_WINDOW,
    _headers,
    _roster,
    compute_score,
    inflow_rate,
    snapshot,
)

ET = ZoneInfo("America/New_York")
GOOGLE_SHEET_ID = "1dRC3DkwOKjhdZveTp2xuSC_roeoxWOUcoP-XWsKQkeo"
STATE_TAB = "Capacity Alerts"
STATE_HEADERS = [
    "checked_at", "score", "band", "backlog", "human_new", "fin_new", "reps_available", "alert_sent",
    "open_queue", "new_per_hour",
]
STATE_MAX_AGE = 2 * 3600  # older than this (overnight, weekend) counts as a fresh start
RANK = {"green": 0, "yellow": 1, "red": 2}


def in_business_hours(now_et):
    return now_et.weekday() < 5 and 8 <= now_et.hour < 18


def state_sheet():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if raw:
        creds = Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
    elif os.path.exists("service_account.json"):
        creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    else:
        return None
    ss = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = ss.worksheet(STATE_TAB)
        if ws.row_values(1) != STATE_HEADERS:
            ws.update(range_name="A1", values=[STATE_HEADERS])  # columns added after the tab was created
        return ws
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=STATE_TAB, rows=2000, cols=len(STATE_HEADERS))
        ws.append_row(STATE_HEADERS)
        return ws


def previous_band(ws, now):
    if ws is None:
        return "green"
    rows = ws.get_all_values()
    if len(rows) < 2:
        return "green"
    last = dict(zip(rows[0], rows[-1]))
    try:
        checked = int(float(last.get("checked_at", 0)))
    except ValueError:
        return "green"
    if now - checked > STATE_MAX_AGE:
        return "green"
    return last.get("band") or "green"


def recent_conversation_lines(token, limit=30):
    body = {
        "query": {"field": "created_at", "operator": ">", "value": int(time.time()) - NEW_WINDOW},
        "pagination": {"per_page": limit},
    }
    r = requests.post(f"{INTERCOM_API}/conversations/search", headers=_headers(token), json=body, timeout=20)
    r.raise_for_status()
    lines = []
    for c in r.json().get("conversations", []):
        src = c.get("source") or {}
        text = c.get("title") or src.get("subject") or src.get("body") or ""
        text = html.unescape(re.sub(r"<[^>]+>", " ", text))
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            lines.append(text[:200])
    return lines


def claude_summary(lines):
    """One-line read of what's driving the volume. Returns None on any failure."""
    if not lines or not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    prompt = (
        "These support conversations were opened in the last hour at an audit software company:\n\n"
        + "\n".join(f"- {l}" for l in lines)
        + "\n\nIn one sentence of 25 words or fewer, say whether several of them are about the same issue "
        "and name it, with a count (for example: 5 of 9 are login errors with code FG-2003). "
        "If there is no clear cluster, say the mix is varied and name the top one or two themes. "
        "Plain text, no dashes."
    )
    try:
        client = anthropic.Anthropic()
        response = client.beta.messages.create(
            model="claude-opus-5-5",
            max_tokens=2000,
            output_config={"effort": "low"},
            betas=["server-side-fallback-2026-07-01"],
            extra_body={"fallbacks": "default"},  # works whether or not the installed SDK knows the field
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIError as e:
        print(f"Claude summary skipped: {e}", file=sys.stderr)
        return None
    if response.stop_reason == "refusal":
        return None
    text = " ".join(b.text for b in response.content if b.type == "text").strip()
    return text or None


def _queue(snap):
    return sum(load["open"] for load in snap["rep_load"].values())


def build_message(band, score, snap, summary):
    n_avail = len(snap["available"])
    score_txt = "no reps available" if score is None else f"{score:g}"
    if band == "red":
        head = f":rotating_light: *Team Capacity is red ({score_txt})*"
    elif band == "yellow":
        head = f":warning: *Team Capacity heads-up: yellow ({score_txt})*"
    else:
        head = f":white_check_mark: *Team Capacity is back to normal ({score_txt})*"

    load = snap.get("rep_load", {})
    avail = ", ".join(
        f"{n} ({load[n]['open']} open)" if n in load else n for n in snap["available"]
    ) or "none"
    lines = [
        head,
        f"Reps available: {n_avail} of {n_avail + len(snap['away'])}: {avail}",
        f"Open queue {_queue(snap)}. New per hour {inflow_rate(snap['inflow']):.1f} "
        f"(last hour: {snap['human_new']} for reps, {snap['fin_new']} Fin only). Backlog {snap['backlog']}.",
    ]
    if summary:
        lines.append(f"What's coming in: {summary}")
    url = os.environ.get("DASHBOARD_URL", "").rstrip("/")
    if url:
        lines.append(f"<{url}/?tab=capacity|Open Team Capacity>")
    return "\n".join(lines)


def main():
    force = "--force" in sys.argv
    dry_run = "--dry-run" in sys.argv
    now = int(time.time())
    now_et = datetime.fromtimestamp(now, ET)

    if not force and not in_business_hours(now_et):
        print(f"Outside business hours ({now_et:%a %H:%M} ET), skipping.")
        return

    token = os.environ.get("INTERCOM_ACCESS_TOKEN")
    webhook = os.environ.get("SLACK_CAPACITY_WEBHOOK_URL")
    if not token or (not webhook and not dry_run):
        sys.exit("INTERCOM_ACCESS_TOKEN and SLACK_CAPACITY_WEBHOOK_URL must be set.")

    snap = snapshot(token, tuple(_roster()))
    score, band = compute_score(snap)

    ws = None if dry_run else state_sheet()
    prev = previous_band(ws, now)

    rising = RANK[band] > RANK[prev]
    recovered = band == "green" and prev != "green"
    message = None
    if rising or recovered:
        summary = None
        if band != "green":
            try:
                summary = claude_summary(recent_conversation_lines(token))
            except requests.RequestException as e:
                print(f"Could not load recent conversations: {e}", file=sys.stderr)
        message = build_message(band, score, snap, summary)

    print(f"{now_et:%a %H:%M} ET score={score} band={band} prev={prev} alert={'yes' if message else 'no'}")

    if message:
        if dry_run:
            print("\n" + message)
        else:
            r = requests.post(webhook, json={"text": message}, timeout=20)
            r.raise_for_status()

    if ws is not None:
        ws.append_row([
            now, "" if score is None else score, band, snap["backlog"], snap["human_new"],
            snap["fin_new"], len(snap["available"]), "yes" if message else "no",
            _queue(snap), round(inflow_rate(snap["inflow"]), 2),
        ])


if __name__ == "__main__":
    main()
