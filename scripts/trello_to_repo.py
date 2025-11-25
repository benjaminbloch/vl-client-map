#!/usr/bin/env python3
"""
trello_to_repo.py
- Fetch Trello cards from your National Fleets board
- Extract addresses from card descriptions
- Geocode with Nominatim (OpenStreetMap) politely (1+ sec between requests)
- Download card cover images and save thumbnails to assets/logos/{card_id}.png
- Write data/clients.json for the map

Environment variables required (set as GitHub secrets for Actions):
- TRELLO_KEY
- TRELLO_TOKEN
- BOARD_ID
- CONTACT_EMAIL  (email used in User-Agent for Nominatim)
"""
import os, re, time, requests, json
from io import BytesIO
from PIL import Image
from datetime import datetime

# Config (from env)
TRELLO_KEY = os.environ.get('TRELLO_KEY')
TRELLO_TOKEN = os.environ.get('TRELLO_TOKEN')
BOARD_ID = os.environ.get('BOARD_ID')
OUT_JSON = 'data/clients.json'
LOGO_DIR = 'assets/logos'
USER_EMAIL = os.environ.get('CONTACT_EMAIL', 'your-email@example.com')

# Make folders if not present
os.makedirs(LOGO_DIR, exist_ok=True)
os.makedirs(os.path.dirname(OUT_JSON) or '.', exist_ok=True)

# Nominatim
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {'User-Agent': f'VinylLabs-Outbound-Map ({USER_EMAIL})'}

def geocode(query):
    if not query:
        return None, None, None
    params = {'q': query, 'format': 'json', 'addressdetails': 1, 'limit': 1, 'countrycodes': 'us,ca'}
    try:
        r = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=30)
        time.sleep(1.1)  # polite: >1s between requests
        if r.status_code == 200:
            j = r.json()
            if j:
                return float(j[0]['lat']), float(j[0]['lon']), j[0].get('display_name')
    except Exception as e:
        print("Geocode error:", e)
    return None, None, None

def canonical_domain(url):
    if not url: return ''
    u = url.lower().strip()
    u = re.sub(r'https?://','',u)
    u = re.sub(r'www\.', '', u)
    return u.split('/')[0] if u else ''

def choose_cover_attachment(card):
    # prefer cover attachment id or first image attachment
    attachments = card.get('attachments', [])
    cover = card.get('cover') or {}
    if cover.get('idAttachment'):
        cid = cover['idAttachment']
        for a in attachments:
            if a.get('id') == cid:
                return a
    for a in attachments:
        url = a.get('url','').lower()
        if any(url.endswith(ext) for ext in ('.png','.jpg','.jpeg','.gif','.webp')):
            return a
    return None

def download_and_save_image(url, card_id):
    # add key/token if Trello attachment requires it
    req_url = url
    if 'key=' not in req_url and TRELLO_KEY and TRELLO_TOKEN:
        if '?' in req_url:
            req_url = f"{req_url}&key={TRELLO_KEY}&token={TRELLO_TOKEN}"
        else:
            req_url = f"{req_url}?key={TRELLO_KEY}&token={TRELLO_TOKEN}"
    try:
        r = requests.get(req_url, timeout=30)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content)).convert('RGBA')
        img.thumbnail((64,64), Image.ANTIALIAS)
        bg = Image.new('RGBA', (64,64), (255,255,255,0))
        w,h = img.size
        bg.paste(img, ((64-w)//2, (64-h)//2), img if img.mode=='RGBA' else None)
        out_path = f"{LOGO_DIR}/{card_id}.png"
        bg.save(out_path, format='PNG')
        return out_path
    except Exception as e:
        print("Logo download failed for", card_id, e)
        return ''

def fetch_cards(board_id, key, token):
    url = f"https://api.trello.com/1/boards/{board_id}/cards"
    params = {
        'key': key, 'token': token,
        'attachments': 'true',
        'attachment_fields': 'url,mimeType',
        'fields': 'name,desc,shortUrl,labels,idMembers,cover'
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_address_from_desc(desc):
    if not desc: return ''
    lines = [l.strip() for l in desc.splitlines() if l.strip()]
    # prefer lines with street words or postal codes
    for ln in lines:
        if re.search(r'\b(St|Street|Ave|Avenue|Rd|Road|Blvd|Drive|Dr|Lane|Ln|Suite|Ste|Unit|PO Box|P\.O\. Box|Postal)\b', ln, re.I):
            return ln
        if re.search(r'\b\d{5}(?:-\d{4})?\b', ln) or re.search(r'\b[ABCEGHJ-NPRSTVXY]\d[A-Z] ?\d[A-Z]\d\b', ln, re.I):
            return ln
    return lines[-1] if lines else ''

def main():
    if not (TRELLO_KEY and TRELLO_TOKEN and BOARD_ID):
        raise SystemExit("Missing TRELLO_KEY, TRELLO_TOKEN or BOARD_ID env vars.")
    cards = fetch_cards(BOARD_ID, TRELLO_KEY, TRELLO_TOKEN)
    rows = []
    for c in cards:
        name = c.get('name','').strip()
        desc = c.get('desc','') or ''
        card_url = c.get('shortUrl','')
        address = extract_address_from_desc(desc)
        lat = lon = None
        if address:
            lat, lon, disp = geocode(address)
        if not lat:
            fallback = f"{name}, {desc.splitlines()[-1] if desc.splitlines() else ''}"
            if fallback.strip():
                lat, lon, disp = geocode(fallback)
                if lat and not address:
                    address = fallback
        # logo
        logo_path = ''
        cover = choose_cover_attachment(c)
        if cover:
            logo_path = download_and_save_image(cover.get('url'), c.get('id'))
        # website detection in desc
        website = ''
        m = re.search(r'(https?://[^\s,]+)|(www\.[^\s,]+)', desc, re.I)
        if m:
            website = (m.group(1) or m.group(2)).strip()
        domain = canonical_domain(website)
        rows.append({
            'CompanyName': name,
            'CardURL': card_url,
            'Website': website,
            'Domain': domain,
            'Address': address or '',
            'Latitude': lat,
            'Longitude': lon,
            'AssignedRep': '',
            'Notes': desc[:8000],
            'LogoFile': logo_path,
            'LastUpdated': datetime.utcnow().isoformat()+'Z'
        })
    # write JSON
    with open(OUT_JSON, 'w', encoding='utf8') as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print("Wrote", OUT_JSON, "with", len(rows), "rows")

if __name__ == '__main__':
    main()
