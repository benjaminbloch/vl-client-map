#!/usr/bin/env python3
"""
scripts/trello_to_repo.py

- Fetch Trello cards from your National Fleets board
- Extract addresses from card descriptions (multiple heuristics)
- Geocode with Nominatim (OpenStreetMap) politely (1+ sec between requests)
  - optional: use Mapbox if MAPBOX_TOKEN is set
- Download card cover images and save thumbnails to assets/logos/{card_id}.png
- Write data/clients.json for the map
- Prints brief summary & warnings for missing data

Environment variables required:
- TRELLO_KEY
- TRELLO_TOKEN
- BOARD_ID
- CONTACT_EMAIL  (email used in User-Agent for Nominatim)
Optional:
- MAPBOX_TOKEN (if you want Mapbox geocoding)
"""
import os
import re
import time
import json
import requests
from io import BytesIO
from datetime import datetime
from PIL import Image

# CONFIG / paths
TRELLO_KEY = os.environ.get("TRELLO_KEY")
TRELLO_TOKEN = os.environ.get("TRELLO_TOKEN")
BOARD_ID = os.environ.get("BOARD_ID")
CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "no-reply@example.com")
MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN")  # optional

OUT_JSON = "data/clients.json"
LOGO_DIR = "assets/logos"
os.makedirs(LOGO_DIR, exist_ok=True)
os.makedirs(os.path.dirname(OUT_JSON) or ".", exist_ok=True)

# Nominatim settings (be polite)
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": f"VinylLabs-Outbound-Map ({CONTACT_EMAIL})"}

# Helpful regexes
US_ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")
CA_POSTAL_RE = re.compile(r"\b[ABCEGHJ-NPRSTVXY]\d[A-Z] ?\d[A-Z]\d\b", re.I)
STREET_KEYWORDS = re.compile(
    r"\b(St|Street|Ave|Avenue|Rd|Road|Blvd|Drive|Dr|Lane|Ln|Suite|Ste|Unit|PO Box|P\.O\. Box|Postal)\b",
    re.I,
)


def check_env():
    missing = []
    for name in ("TRELLO_KEY", "TRELLO_TOKEN", "BOARD_ID"):
        if globals().get(name) is None:
            missing.append(name)
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")


def fetch_cards(board_id, key, token):
    """
    Fetch cards for the board, including attachments and cover metadata.
    """
    url = f"https://api.trello.com/1/boards/{board_id}/cards"
    params = {
        "key": key,
        "token": token,
        "attachments": "true",
        "attachment_fields": "url,mimeType,previews",
        "fields": "name,desc,shortUrl,labels,idMembers,cover",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def canonical_domain(url):
    if not url:
        return ""
    u = url.lower().strip()
    u = re.sub(r"https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return u.split("/")[0].strip()


def extract_address_from_desc(desc):
    """
    Heuristics:
    1) Line containing street keywords or postal code
    2) Last 1-3 lines (often City, State)
    3) Fallback: last line (better than nothing)
    """
    if not desc:
        return ""
    lines = [l.strip() for l in desc.splitlines() if l.strip()]
    # prefer lines with street keywords or postal codes
    for ln in lines:
        if STREET_KEYWORDS.search(ln) or US_ZIP_RE.search(ln) or CA_POSTAL_RE.search(ln):
            return ln
    # check last few lines for "City, ST" style
    for ln in reversed(lines[-3:]):
        if "," in ln and len(ln) < 120:
            return ln
    # fallback: last line
    return lines[-1] if lines else ""


def geocode_mapbox(q):
    """Try Mapbox (if token provided). Return (lat, lon) or (None, None)."""
    if not MAPBOX_TOKEN:
        return None, None
    try:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{requests.utils.requote_uri(q)}.json"
        params = {"access_token": MAPBOX_TOKEN, "limit": 1, "country": "us,ca"}
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            j = r.json()
            if j.get("features"):
                c = j["features"][0]["center"]
                # Mapbox returns [lon, lat]
                return float(c[1]), float(c[0])
    except Exception as e:
        print("[WARN] Mapbox geocode error:", e)
    return None, None


def geocode_nominatim(q):
    """Geocode with Nominatim. Respect rate limits; sleep in caller."""
    try:
        params = {"q": q, "format": "json", "addressdetails": 1, "limit": 1, "countrycodes": "us,ca"}
        r = requests.get(NOMINATIM_URL, params=params, headers=NOMINATIM_HEADERS, timeout=25)
        if r.status_code == 200:
            j = r.json()
            if j:
                return float(j[0]["lat"]), float(j[0]["lon"])
    except Exception as e:
        print("[WARN] Nominatim error for query:", q, e)
    return None, None


def try_geocode_variants(name, address, domain):
    """
    Try multiple strategies:
      1) direct address
      2) last line of address (city/state)
      3) "Company, address"
      4) domain/company only (last resort)
    Returns (lat, lon, used_query)
    """
    # 1) direct address
    if address:
        if MAPBOX_TOKEN:
            lat, lon = geocode_mapbox(address)
            if lat:
                return lat, lon, address
        lat, lon = geocode_nominatim(address)
        if lat:
            return lat, lon, address

    # 2) city/state if possible
    if address and "," in address:
        candidate = address.splitlines()[-1]
        if MAPBOX_TOKEN:
            lat, lon = geocode_mapbox(candidate)
            if lat:
                return lat, lon, candidate
        lat, lon = geocode_nominatim(candidate)
        if lat:
            return lat, lon, candidate

    # 3) company + address
    if name and address:
        candidate = f"{name}, {address}"
        if MAPBOX_TOKEN:
            lat, lon = geocode_mapbox(candidate)
            if lat:
                return lat, lon, candidate
        lat, lon = geocode_nominatim(candidate)
        if lat:
            return lat, lon, candidate

    # 4) domain/company alone (last resort)
    if domain:
        candidate = domain
        if MAPBOX_TOKEN:
            lat, lon = geocode_mapbox(candidate)
            if lat:
                return lat, lon, candidate
        lat, lon = geocode_nominatim(candidate)
        if lat:
            return lat, lon, candidate

    return None, None, None


def choose_cover_attachment(card):
    """
    Try to find the best image for the card:
    - cover.url or cover.scaled[] (if present)
    - attachment with id equal to cover.idAttachment
    - first image attachment in attachments[]
    """
    cover = card.get("cover") or {}
    attachments = card.get("attachments") or []

    # cover has direct url or scaled previews
    if cover.get("url"):
        return {"url": cover.get("url")}
    if cover.get("scaled") and isinstance(cover["scaled"], list) and cover["scaled"]:
        # use the largest preview
        scaled = cover["scaled"][-1]
        if scaled.get("url"):
            return {"url": scaled.get("url")}

    # idAttachment -> find in attachments
    if cover.get("idAttachment"):
        cid = cover["idAttachment"]
        for a in attachments:
            if a.get("id") == cid:
                return a

    # fallback: first image-like attachment
    for a in attachments:
        mt = (a.get("mimeType") or "").lower()
        url = (a.get("url") or "").lower()
        if mt.startswith("image") or url.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            return a

    return None


def download_and_save_image(url, card_id):
    """
    Download an image and save a 64x64 PNG thumbnail to assets/logos/{card_id}.png
    If Trello attachments are private, the script appends key/token when available.
    Returns the relative path (e.g. assets/logos/{card_id}.png) on success or '' on failure.
    """
    try:
        # If the URL looks like a Trello attachment and the API key/token exist,
        # append key/token unless they are already present.
        if "trello.com" in url and "key=" not in url and TRELLO_KEY and TRELLO_TOKEN:
            if "?" in url:
                url = f"{url}&key={TRELLO_KEY}&token={TRELLO_TOKEN}"
            else:
                url = f"{url}?key={TRELLO_KEY}&token={TRELLO_TOKEN}"

        r = requests.get(url, timeout=30, stream=True)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content)).convert("RGBA")
        img.thumbnail((64, 64), Image.ANTIALIAS)
        bg = Image.new("RGBA", (64, 64), (255, 255, 255, 0))
        w, h = img.size
        bg.paste(img, ((64 - w) // 2, (64 - h) // 2), img if img.mode == "RGBA" else None)
        out_path = os.path.join(LOGO_DIR, f"{card_id}.png")
        bg.save(out_path, format="PNG")
        # Return a web-relative path (map expects relative path)
        return out_path.replace("\\", "/")
    except Exception as e:
        print(f"[WARN] Failed to download/save image for card {card_id}: {e}")
        return ""


def process_card(card):
    """
    Build a single record for a Trello card:
    {
      CompanyName, CardURL, Website, Domain, Address,
      Latitude, Longitude, AssignedRep, Notes, LogoFile, LastUpdated
    }
    """
    name = card.get("name", "").strip()
    desc = card.get("desc", "") or ""
    card_url = card.get("shortUrl", "")
    attachments = card.get("attachments") or []
    website = ""
    # try to extract first URL from desc
    m = re.search(r"(https?://[^\s,]+)|(www\.[^\s,]+)", desc, re.I)
    if m:
        website = (m.group(1) or m.group(2)).strip()
    domain = canonical_domain(website)

    # Address heuristics
    address = extract_address_from_desc(desc)
    # Attempt improved geocoding
    lat = lon = None
    used_query = None
    if address:
        lat, lon, used_query = try_geocode_variants(name, address, domain)
    # If still not found, try fallbacks (company + last line)
    if not lat:
        fallback = ""
        lines = [l.strip() for l in desc.splitlines() if l.strip()]
        if lines:
            last_line = lines[-1]
            fallback = f"{name}, {last_line}" if name else last_line
            lat, lon, used_query = try_geocode_variants(name, fallback, domain)
            if lat and not address:
                address = last_line

    # Last resort try company alone or domain
    if not lat and domain:
        lat, lon, used_query = try_geocode_variants(name, "", domain)

    # Download logo (cover or first attachment)
    logo_path = ""
    attach = choose_cover_attachment(card)
    if attach:
        # if attach is a dict with 'url' key (cover), use it; else if it's an attachment object, try its url.
        url = attach.get("url") or attach.get("previews", [{}])[-1].get("url", None)
        if url:
            logo_path = download_and_save_image(url, card.get("id"))
    # If no logo yet, attempt to find any image attachment URL
    if not logo_path:
        for a in attachments:
            url = a.get("url")
            if url and (a.get("mimeType", "").startswith("image") or url.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))):
                logo_path = download_and_save_image(url, card.get("id"))
                if logo_path:
                    break

    if not logo_path:
        # fallback to Clearbit logo if domain exists (not guaranteed)
        if domain:
            logo_path = f"https://logo.clearbit.com/{domain}"

    # AssignedRep: try to infer names from idMembers (we could expand with Trello members endpoint)
    assigned_rep = ""
    if card.get("idMembers"):
        assigned_rep = ",".join(card.get("idMembers"))

    row = {
        "CompanyName": name,
        "CardURL": card_url,
        "Website": website,
        "Domain": domain,
        "Address": address or "",
        "Latitude": lat,
        "Longitude": lon,
        "AssignedRep": assigned_rep,
        "Notes": desc or "",
        "LogoFile": logo_path or "",
        "LastUpdated": datetime.utcnow().isoformat() + "Z",
    }
    return row


def main():
    check_env()
    print("Fetching cards from Trello board:", BOARD_ID)
    try:
        cards = fetch_cards(BOARD_ID, TRELLO_KEY, TRELLO_TOKEN)
    except requests.HTTPError as e:
        print("Failed to fetch cards from Trello:", e)
        raise

    rows = []
    missing_coords = []
    missing_logos = []
    total = len(cards)
    print(f"Found {total} cards, processing...")
    for i, c in enumerate(cards, start=1):
        print(f"[{i}/{total}] {c.get('name')}")
        row = process_card(c)
        rows.append(row)
        if not row.get("Latitude") or not row.get("Longitude"):
            missing_coords.append(row)
        if not row.get("LogoFile"):
            missing_logos.append(row)

    # Write JSON
    with open(OUT_JSON, "w", encoding="utf8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_JSON} with {len(rows)} rows.")

    # Summary
    print("Summary:")
    print("  Missing coords:", len(missing_coords))
    if missing_coords:
        print("  Examples (missing coords):")
        for r in missing_coords[:6]:
            print("   -", r.get("CompanyName"), "| Address:", r.get("Address"), "| CardURL:", r.get("CardURL"))
    print("  Missing logos:", len(missing_logos))
    if missing_logos:
        print("  Examples (missing logos):")
        for r in missing_logos[:6]:
            print("   -", r.get("CompanyName"), "| CardURL:", r.get("CardURL"))

    print("Done.")


if __name__ == "__main__":
    main()
