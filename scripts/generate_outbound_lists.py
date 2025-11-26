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
                domains |= set(df["Domain"].fillna("").apply(canonical_domain).unique())
        except Exception:
            continue
    return {d for d in domains if d}

def canonicalize_candidates(df):
    if "Website" in df.columns:
        df["Domain"] = df["Website"].fillna("").apply(canonical_domain)
    else:
        df["Domain"] = ""
    return df

def load_assignment_history():
    if os.path.exists(ASSIGNMENT_HISTORY):
        try:
            h = pd.read_csv(ASSIGNMENT_HISTORY, parse_dates=["WeekAssigned"])
            h["domain"] = h["Domain"].fillna("").apply(canonical_domain)
            return h
        except Exception:
            return pd.DataFrame(columns=["Domain","CompanyName","AssignedRep","WeekAssigned","LastDisposition"])
    else:
        return pd.DataFrame(columns=["Domain","CompanyName","AssignedRep","WeekAssigned","LastDisposition"])

def in_12_months(domain, history_df):
    if domain == "" : return False
    cutoff = datetime.utcnow() - timedelta(days=365)
    recent = history_df[(history_df["domain"]==domain) & (pd.to_datetime(history_df["WeekAssigned"], errors="coerce") >= cutoff)]
    return not recent.empty

# -----------------------
# Sheets helper
# -----------------------
def append_weekly_block_to_sheet(rep_tab_name, rows):
    sa_json = GCP_SA_JSON
    if not sa_json and os.path.exists("sa.json"):
        with open("sa.json", "r", encoding="utf8") as f:
            sa_json = f.read().strip()
    if not sa_json:
        raise SystemExit("No Google service account JSON available (GCP_SA_JSON or sa.json).")

    try:
        creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    except Exception as e:
        raise SystemExit(f"Invalid service account JSON: {e}")

    gc = gspread.authorize(creds)
    ss = gc.open_by_key(SHEET_ID)
    try:
        ws = ss.worksheet(rep_tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=rep_tab_name, rows=2000, cols=50)

    today = WEEK_ASSIGNED
    header_row = [f"Week: {today}"]
    columns = ['CompanyName','Website','Domain','HQ_City','HQ_StateProvince','Country','Industry','EmployeeCount','EstimatedFleetSize','GrowthSignalScore','FitScore','DM1_Name','DM1_Title','DM1_LinkedIn','DM1_Email','DM1_Email_Verified','DM1_DirectPhone','DM1_Phone_Verified','Source','Notes','BestCallWindow','AssignedRep','WeekAssigned','LastVerified']
    ws.append_row(header_row, value_input_option="USER_ENTERED")
    ws.append_row(columns, value_input_option="USER_ENTERED")
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    ws.append_row([""], value_input_option="USER_ENTERED")

# -----------------------
# Main flow
# -----------------------
def main():
    # Schedule + idempotency
    if not should_run_today_and_hour():
        print("Not scheduled run time (local 10:00 America/Vancouver). Exiting.")
        return

    if already_ran_for_week(WEEK_ASSIGNED):
        print(f"Weekly lists for {WEEK_ASSIGNED} already created (assignment_history found). Exiting.")
        return

    # Ensure candidates exists
    if not os.path.exists(CANDIDATES_CSV):
        raise SystemExit(f"{CANDIDATES_CSV} not found. Please add candidates.csv in repo root or enable a sourcing step.")

    print("Loading candidates...")
    df = pd.read_csv(CANDIDATES_CSV, dtype=str).fillna("")
    df = canonicalize_candidates(df)

    prior_domains = read_prior_domains()
    history_df = load_assignment_history()

    df["canonical_domain"] = df["Domain"].apply(canonical_domain)
    df = df[~df["canonical_domain"].isin(prior_domains)].copy()

    enriched = []
    print("Enriching candidates with Apollo (people)...")
    for idx, r in df.iterrows():
        company = r.get("Company") or r.get("CompanyName") or r.get("company") or ""
        domain = r.get("canonical_domain","")
        row = {
            "CompanyName": company,
            "Website": r.get("Website",""),
            "Domain": domain,
            "HQ_City": r.get("City",""),
            "HQ_StateProvince": r.get("State",""),
            "Country": r.get("Country",""),
            "Industry": r.get("Industry",""),
            "EmployeeCount": r.get("EmployeeCount",""),
            "EstimatedFleetSize": r.get("EstimatedFleetSize",""),
            "Source": r.get("Source",""),
            "Notes": r.get("Notes","")
        }

        people = []
        if domain:
            people = apollo_people_by_domain(domain)
        if not people:
            people = apollo_people_by_company(company)
        dm = pick_decision_maker(people) if people else None
        if dm:
            row["DM1_Name"] = dm.get("name") or ""
            row["DM1_Title"] = dm.get("title") or ""
            row["DM1_LinkedIn"] = dm.get("linkedin_url") or dm.get("linkedin") or ""
            row["DM1_Email"] = dm.get("email") or ""
            phone = dm.get("phone") or ""
            if isinstance(phone, list) and phone:
                phone = phone[0].get("number") if isinstance(phone[0], dict) else phone[0]
            row["DM1_DirectPhone"] = phone or dm.get("direct_phone") or ""
            row["DM1_Email_Verified"] = "unknown"
            row["DM1_Phone_Verified"] = "unknown"
        else:
            row.update({
                "DM1_Name": "", "DM1_Title":"", "DM1_LinkedIn":"", "DM1_Email":"",
                "DM1_DirectPhone":"", "DM1_Email_Verified":"", "DM1_Phone_Verified":""
            })
        row["GrowthSignalScore"] = ""
        enriched.append(row)
        time.sleep(0.2)

    df_en = pd.DataFrame(enriched)
    df_en["FitScore"] = df_en.apply(compute_fit_score, axis=1)
    df_en = df_en.sort_values("FitScore", ascending=False).drop_duplicates(subset=["Domain","CompanyName"], keep="first").reset_index(drop=True)

    def domain_in_recent(domain):
        if not domain:
            return False
        return in_12_months(domain, history_df)
    df_en["recent_assigned"] = df_en["Domain"].apply(domain_in_recent)
    df_en = df_en[~df_en["recent_assigned"]].copy()

    top100 = df_en.head(100).reset_index(drop=True)
    top100["AssignedRep"] = ["Evan" if i%2==0 else "Dave" for i in range(len(top100))]

    def to_sheet_rows(df_block, rep):
        rows = []
        for _, r in df_block.iterrows():
            rows.append([
                r.get("CompanyName",""),
                r.get("Website",""),
                r.get("Domain",""),
                r.get("HQ_City",""),
                r.get("HQ_StateProvince",""),
                r.get("Country",""),
                r.get("Industry",""),
                r.get("EmployeeCount",""),
                r.get("EstimatedFleetSize",""),
                r.get("GrowthSignalScore",""),
                int(r.get("FitScore") or 0),
                r.get("DM1_Name",""),
                r.get("DM1_Title",""),
                r.get("DM1_LinkedIn",""),
                r.get("DM1_Email",""),
                r.get("DM1_Email_Verified",""),
                r.get("DM1_DirectPhone",""),
                r.get("DM1_Phone_Verified",""),
                r.get("Source",""),
                r.get("Notes",""),
                "",
                rep,
                WEEK_ASSIGNED,
                datetime.utcnow().strftime("%Y-%m-%d")
            ])
        return rows

    evan_df = top100[top100["AssignedRep"]=="Evan"].head(50)
    dave_df = top100[top100["AssignedRep"]=="Dave"].head(50)
    evan_rows = to_sheet_rows(evan_df, "Evan")
    dave_rows = to_sheet_rows(dave_df, "Dave")

    print("Appending to Google Sheet...")
    append_weekly_block_to_sheet("Evan", evan_rows)
    append_weekly_block_to_sheet("Dave", dave_rows)

    hist = load_assignment_history()
    for _, r in top100.iterrows():
        hist = hist.append({
            "Domain": r.get("Domain",""),
            "CompanyName": r.get("CompanyName",""),
            "AssignedRep": r.get("AssignedRep",""),
            "WeekAssigned": WEEK_ASSIGNED,
            "LastDisposition": ""
        }, ignore_index=True)
    hist.to_csv(ASSIGNMENT_HISTORY, index=False)
    print("Completed list generation; Evan and Dave lists appended to sheet.")

if __name__ == "__main__":
    main()
