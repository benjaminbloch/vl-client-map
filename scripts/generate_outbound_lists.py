#!/usr/bin/env python3
"""
scripts/generate_outbound_lists.py

Fully automated weekly list generator:
- Runs only at Friday 10:00 AM America/Vancouver (or Thursday 10:00 AM if Friday is a BC stat)
- Idempotent: will not re-run for the same WEEK_ASSIGNED if assignment_history already contains it
- Loads candidates.csv (or you can add an upstream sourcing step)
- Enriches via Apollo, computes FitScore, dedupes, respects 12-month overlap
- Produces two 50-company weekly blocks and appends to Google Sheet tabs 'Evan' and 'Dave'

Environment:
- APOLLO_API_KEY     (GitHub secret)
- GCP_SA_JSON        (GitHub secret with Google Sheets service account JSON) OR workflow writes sa.json
- SHEET_ID           (defaults to your existing sheet)
- WEEK_ASSIGNED      (optional override)
- FORCE_RUN          (optional manual override: '1' / 'true')

Dependencies:
pip install requests pandas gspread google-auth python-dateutil holidays pytz
"""

import os
import re
import json
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
import holidays
import pytz
from google.oauth2.service_account import Credentials
import gspread

# -----------------------
# Config / constants
# -----------------------
SHEET_ID = os.environ.get("SHEET_ID", "1vVFmYqyedxNbzJWKpU4hjJhDbs96q4QqDlcq0bSIlNA")
APOLLO_KEY = os.environ.get("APOLLO_API_KEY", "").strip()
GCP_SA_JSON = os.environ.get("GCP_SA_JSON", "").strip()
CANDIDATES_CSV = "candidates.csv"
ASSIGNMENT_HISTORY = "assignment_history.csv"
PRIOR_GROK_EVAN = "/mnt/data/VL - National Outbound Lists - Evan.csv"
PRIOR_GROK_DAVE = "/mnt/data/VL - National Outbound Lists - Dave.csv"
WEEK_ASSIGNED = os.environ.get("WEEK_ASSIGNED") or datetime.utcnow().strftime("%Y-%m-%d")
TIMEZONE = "America/Vancouver"
APOLLO_PEOPLE_ENDPOINT = "https://api.apollo.io/v1/people/search"

TITLE_KEYWORDS = [
    "Fleet Manager", "Director of Fleet", "Fleet Operations Manager", "Head of Fleet",
    "VP Fleet", "Director of Fleet Operations", "Procurement Manager", "Purchasing Manager",
    "Operations Manager", "Director of Operations", "Logistics Manager", "Director of Logistics",
    "Maintenance Manager", "Fleet Maintenance Manager", "Marketing Manager", "Director of Marketing"
]

# -----------------------
# Sa.json fallback: read sa.json file if env is missing
# -----------------------
if not GCP_SA_JSON:
    if os.path.exists("sa.json"):
        try:
            with open("sa.json", "r", encoding="utf8") as f:
                GCP_SA_JSON = f.read().strip()
            if not (GCP_SA_JSON.startswith("{") and GCP_SA_JSON.endswith("}")):
                print("Warning: sa.json content does not appear to be JSON.")
        except Exception as e:
            print("Warning: failed to read sa.json:", e)

# -----------------------
# Scheduling & idempotency helpers
# -----------------------
def should_run_today_and_hour(target_hour=10, window_minutes=60):
    """
    Return True if:
      - it's Friday (local America/Vancouver) and current local time is within target_hour window,
        AND that Friday is not a BC stat holiday,
      - OR it's Thursday local time and tomorrow is a BC stat holiday (run Thursday in the same local hour).
    """
    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz)
    today_local = now_local.date()
    weekday = today_local.weekday()  # Mon=0 ... Sun=6
    bc_holidays = holidays.CA(prov='BC')

    start = now_local.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    end = start + timedelta(minutes=window_minutes)
    in_window = (now_local >= start) and (now_local < end)

    # Friday and not a BC holiday
    if weekday == 4 and (today_local not in bc_holidays):
        return in_window

    # Thursday and tomorrow is BC holiday -> run Thursday
    if weekday == 3:
        friday = today_local + timedelta(days=1)
        if friday in bc_holidays:
            return in_window

    return False

def already_ran_for_week(week_assigned_str):
    """
    Return True if assignment_history.csv already contains entries for the given WeekAssigned.
    Prevents duplicate runs for the same WEEK_ASSIGNED.
    """
    if not os.path.exists(ASSIGNMENT_HISTORY):
        return False
    try:
        hist = pd.read_csv(ASSIGNMENT_HISTORY, dtype=str)
        if "WeekAssigned" not in hist.columns:
            return False
        return any(hist["WeekAssigned"].astype(str).str.strip() == str(week_assigned_str))
    except Exception as e:
        print("Warning: could not read assignment_history for idempotency check:", e)
        return False

# -----------------------
# Apollo helpers and scoring
# -----------------------
def canonical_domain(url):
    if not isinstance(url, str):
        return ""
    u = url.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return u.split("/")[0].split("?")[0]

def apollo_people_by_domain(domain):
    if not domain or not APOLLO_KEY:
        return []
    headers = {"Authorization": f"Bearer {APOLLO_KEY}"}
    params = {"domain": domain, "page": 1, "per_page": 10}
    try:
        r = requests.get(APOLLO_PEOPLE_ENDPOINT, headers=headers, params=params, timeout=15)
        if r.status_code == 200:
            return r.json().get("people") or []
        else:
            print(f"[Apollo] domain search HTTP {r.status_code} for {domain}: {r.text[:400]}")
    except Exception as e:
        print("Apollo domain search error:", e)
    return []

def apollo_people_by_company(company):
    if not company or not APOLLO_KEY:
        return []
    headers = {"Authorization": f"Bearer {APOLLO_KEY}"}
    params = {"q": company, "page": 1, "per_page": 15}
    try:
        r = requests.get(APOLLO_PEOPLE_ENDPOINT, headers=headers, params=params, timeout=15)
        if r.status_code == 200:
            return r.json().get("people") or []
        else:
            print(f"[Apollo] company search HTTP {r.status_code} for {company}: {r.text[:400]}")
    except Exception as e:
        print("Apollo company search error:", e)
    return []

def pick_decision_maker(people):
    if not people:
        return None
    for kw in TITLE_KEYWORDS:
        for p in people:
            title = (p.get("title") or "").lower()
            if kw.lower() in title:
                return p
    for p in people:
        title = (p.get("title") or "").lower()
        if any(x in title for x in ("director","vp","vice","head","manager")):
            return p
    return people[0]

def compute_fit_score(row):
    score = 0
    industry = str(row.get("Industry","")).lower()
    if any(x in industry for x in ["fleet","transport","logistics","trucking","delivery","distribution"]):
        score += 30
    try:
        emp = float(row.get("EmployeeCount") or 0)
        if emp >= 100:
            score += 25
        elif emp >= 50:
            score += 10
    except:
        score += 5
    if row.get("DM1_Email") and row.get("DM1_DirectPhone"):
        score += 30
    elif row.get("DM1_Email") or row.get("DM1_DirectPhone"):
        score += 10
    return min(100, int(score))

# -----------------------
# Prior Grok and assignment-history helpers
# -----------------------
def read_prior_domains():
    domains = set()
    for path in [PRIOR_GROK_EVAN, PRIOR_GROK_DAVE]:
        try:
            df = pd.read_csv(path, dtype=str)
            if "Website" in df.columns:
                domains |= set(df["Website"].fillna("").apply(canonical_domain).unique())
            if "Domain" in df.columns:
                domains |= set(df["Domain"].fillna("").apply(cano
