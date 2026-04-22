#!/usr/bin/env python3
"""
WB-RERA MongoDB Scraper — Production Grade
==========================================
Scrapes ALL projects from https://rera.wb.gov.in and upserts
full details + booking status into MongoDB Atlas.

On every run:
  - Stage 1: Fetch project_details.php for every project ID
  - Stage 2: Fetch project-status.php for every project
  - Upsert each project into `projects` collection
  - Diff old vs new doc and append any changes to `changes` collection
  - Geocode any new projects that don't have lat/lon yet

Collections:
  projects  — one doc per project, keyed on project_id
  changes   — append-only log of field-level diffs

Usage:
  pip install requests beautifulsoup4 lxml pymongo apscheduler
  export MONGO_URI="mongodb+srv://..."
  python scraper_mongo.py            # starts scheduler + runs immediately
  python scraper_mongo.py --once     # single run then exit
"""

import os
import ssl
import re
import sys
import time
import logging
import threading
import warnings
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
import urllib3
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from apscheduler.schedulers.blocking import BlockingScheduler

warnings.filterwarnings("ignore")
urllib3.disable_warnings()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
MONGO_URI           = os.environ.get("MONGO_URI", "mongodb+srv://<user>:<pass>@cluster.mongodb.net/")
MONGO_DB            = "wbrera"
COLLECTION_PROJECTS = "projects"
COLLECTION_CHANGES  = "changes"

DISTRICT_URL        = "https://rera.wb.gov.in/district_project.php?dcode=0"
BASE_URL            = "https://rera.wb.gov.in"
REQUEST_TIMEOUT     = 30
MAX_WORKERS_DETAILS = 8
MAX_WORKERS_STATUS  = 6
MAX_RETRIES         = 3
RETRY_BACKOFF       = 2.0

SCRAPE_HOUR         = 2
SCRAPE_MINUTE       = 0

DIFF_EXCLUDE_FIELDS = {"_id", "last_scraped_at", "first_seen_at", "_result", "_status_result"}

# Project name lines to skip — not real names
INVALID_NAME_PATTERNS = [
    "DEFAULTER",
    "QUARTERLY STATUS UPDATE",
    "PROMOTERS ARE REQUESTED",
    "CLICK HERE",
    "USER MANUAL",
]
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"wbrera_mongo_{datetime.now().strftime('%Y%m%d')}.log"),
    ],
)
log = logging.getLogger("wbrera")


# ──────────────────────────────────────────────────────────────────────────────
# MONGO
# ──────────────────────────────────────────────────────────────────────────────
def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    db = client[MONGO_DB]
    db[COLLECTION_PROJECTS].create_index("project_id", unique=True)
    db[COLLECTION_PROJECTS].create_index("pincode")
    db[COLLECTION_PROJECTS].create_index("project_status")
    db[COLLECTION_CHANGES].create_index([("project_id", 1), ("changed_at", -1)])
    db[COLLECTION_CHANGES].create_index("changed_at")
    log.info("MONGO CONNECTED | db=%s", MONGO_DB)
    return client, db


# ──────────────────────────────────────────────────────────────────────────────
# DIFF
# ──────────────────────────────────────────────────────────────────────────────
def diff_docs(old: dict, new: dict) -> list[dict]:
    changes = []
    now     = datetime.now(timezone.utc)
    pid     = new.get("project_id", "unknown")
    for key in set(old.keys()) | set(new.keys()):
        if key in DIFF_EXCLUDE_FIELDS:
            continue
        if old.get(key) != new.get(key):
            changes.append({
                "project_id": pid,
                "field":      key,
                "old_value":  old.get(key),
                "new_value":  new.get(key),
                "changed_at": now,
            })
    return changes


# ──────────────────────────────────────────────────────────────────────────────
# SSL ADAPTER
# ──────────────────────────────────────────────────────────────────────────────
class LegacySSLAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        try:
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        except AttributeError:
            ctx.options |= 0x4
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)

    def send(self, *args, **kwargs):
        kwargs["verify"] = False
        return super().send(*args, **kwargs)


def make_session() -> requests.Session:
    sess = requests.Session()
    adapter = LegacySSLAdapter(
        max_retries=urllib3.Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
    )
    sess.mount("https://", adapter)
    sess.mount("http://",  adapter)
    sess.headers.update({
        "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return sess


_thread_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = make_session()
    return _thread_local.session


# ──────────────────────────────────────────────────────────────────────────────
# HTTP
# ──────────────────────────────────────────────────────────────────────────────
def fetch(url: str, project_id: str = "") -> Optional[str]:
    sess = get_session()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = sess.get(url, timeout=REQUEST_TIMEOUT, verify=False)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            wait = RETRY_BACKOFF ** attempt
            log.warning("FETCH FAIL | pid=%s | attempt=%d/%d | err=%s | retry in %.1fs",
                        project_id, attempt, MAX_RETRIES, exc, wait)
            if attempt < MAX_RETRIES:
                time.sleep(wait)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# TEXT HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def clean(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    t = " ".join(text.split()).strip()
    return None if t.upper() in ("", "NA", "N/A", "NOT MENTIONED", "-", "--") else t

def safe_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"[\d,]+(?:\.\d+)?", text.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group().replace(",", ""))
    except ValueError:
        return None

def safe_int(text: Optional[str]) -> Optional[int]:
    v = safe_float(text)
    return int(v) if v is not None else None

def lines_of(soup: BeautifulSoup) -> list[str]:
    return [l.strip() for l in soup.get_text("\n").split("\n") if l.strip()]

def is_invalid_name(name: str) -> bool:
    """Return True if the candidate name is actually a notice/warning, not a real project name."""
    upper = name.upper()
    return any(pattern in upper for pattern in INVALID_NAME_PATTERNS)


# ──────────────────────────────────────────────────────────────────────────────
# DISTRICT PAGE
# ──────────────────────────────────────────────────────────────────────────────
def get_all_project_ids(sess: requests.Session) -> list[str]:
    log.info("DISTRICT PAGE LOADING | url=%s", DISTRICT_URL)
    html = fetch(DISTRICT_URL)
    if not html:
        log.error("DISTRICT PAGE FAILED")
        return []
    soup = BeautifulSoup(html, "lxml")
    ids, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "procode=" in href:
            pid = href.split("procode=")[1].strip()
            if pid and pid not in seen:
                seen.add(pid)
                ids.append(pid)
    log.info("DISTRICT PAGE LOADED | total_projects=%d", len(ids))
    return ids


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 1 — project details
# ──────────────────────────────────────────────────────────────────────────────
def parse_details(project_id: str) -> dict:
    url    = f"{BASE_URL}/project_details.php?procode={project_id}"
    html   = fetch(url, project_id)
    result = {"project_id": project_id, "details_url": url, "_result": "FAIL"}

    if html is None:
        return result

    soup  = BeautifulSoup(html, "lxml")
    lines = lines_of(soup)

    # ── Pincode ──
    pincode = None
    for line in lines:
        m = re.search(r"\bPin\s+(\d{6})\b", line)
        if m:
            pincode = m.group(1)
            break
    result["pincode"] = pincode

    # ── Project name ──
    # Walk backwards from "PROJECT STATUS -" or "PROJECT ID:", skip invalid lines
    project_name = None
    for i, line in enumerate(lines):
        if "PROJECT STATUS -" in line or "PROJECT ID:" in line:
            for j in range(i - 1, max(0, i - 6), -1):
                candidate = clean(lines[j])
                if (candidate
                        and len(candidate) < 150
                        and not is_invalid_name(candidate)
                        and not candidate.startswith("PROJECT")
                        and not re.match(r"^\d+$", candidate)):
                    project_name = candidate
                    break
            break
    result["project_name"] = project_name

    # ── Status, IDs, dates ──
    for i, line in enumerate(lines):
        if line.startswith("PROJECT STATUS -"):
            inline = clean(line.replace("PROJECT STATUS -", "").strip())
            result["project_status"] = inline or (
                clean(lines[i + 1]) if i + 1 < len(lines) else None
            )
        elif line.startswith("PROJECT ID:"):
            result["project_id_display"] = clean(line.replace("PROJECT ID:", "").strip())
        elif line.startswith("PROJECT COMPLETION DATE:"):
            result["completion_date"] = clean(line.replace("PROJECT COMPLETION DATE:", "").strip())
        elif line.startswith("RERA REGISTRATION NO.:"):
            result["rera_reg_no"] = clean(line.replace("RERA REGISTRATION NO.:", "").strip())

    # ── Extension date ──
    for i, line in enumerate(lines):
        if "EXTENSION COMPLETION DATE:" in line:
            inline = line.replace("EXTENSION COMPLETION DATE:", "").strip()
            if inline and inline.upper() not in ("NA", "N/A"):
                result["extension_completion_date"] = clean(inline)
            elif i + 1 < len(lines):
                result["extension_completion_date"] = clean(lines[i + 1])
            break

    # ── Project type ──
    in_highlights = False
    for i, line in enumerate(lines):
        ls = line.strip()
        if ls == "Highlights":
            in_highlights = True
            continue
        if in_highlights:
            if ls == "Project Type" and i + 1 < len(lines):
                candidate = clean(lines[i + 1])
                if candidate and len(candidate) < 50 and "No." not in candidate:
                    result["project_type"] = candidate
                break
            if ls == "Specification":
                break

    # ── Specification block ──
    in_spec = False
    for i, line in enumerate(lines):
        ls = line.strip().rstrip(":")
        if ls in ("Residential Details", "Commercial Details", "Mixed Details",
                  "Residential", "Commercial", "Mixed"):
            if i > 0 and "Specification" in lines[i - 1]:
                in_spec = True
                continue
        if not in_spec:
            continue
        if ls == "Land Area" and i + 1 < len(lines):
            result["land_area_sqm"] = safe_float(lines[i + 1])
        elif ls == "Total Built Up Area" and i + 1 < len(lines):
            result["builtup_area_sqm"] = safe_float(lines[i + 1])
        elif ls == "Carpet Area" and i + 1 < len(lines):
            result["carpet_area_sqm"] = safe_float(lines[i + 1])
        elif ls == "No. of Apartments" and i + 1 < len(lines):
            result["total_apartments"] = safe_int(lines[i + 1])
        elif ls in ("Location", "Facilities", "Amenities", "Registered Agents"):
            break

    # ── Parking ──
    in_h2 = False
    for i, line in enumerate(lines):
        ls = line.strip()
        if ls == "Highlights":
            in_h2 = True
            continue
        if not in_h2:
            continue
        if ls == "Specification":
            break
        if ls == "Basement Parking" and i + 1 < len(lines):
            result["basement_parking"] = safe_int(lines[i + 1])
        elif ls == "Covered Car Parking" and i + 1 < len(lines):
            result["covered_parking"] = safe_int(lines[i + 1])
        elif ls == "Mechanical Parking" and i + 1 < len(lines):
            result["mechanical_parking"] = safe_int(lines[i + 1])

    # ── Location ──
    for i, line in enumerate(lines):
        if line.strip().startswith("Pin ") and pincode and pincode in line:
            for j in range(max(0, i - 6), i):
                l = lines[j].strip()
                if l.startswith("Dist."):
                    result["district"] = clean(l.replace("Dist.", "").strip())
                elif re.match(r"^P\.?S\.", l):
                    result["police_station"] = clean(re.sub(r"^P\.?S\.", "", l).strip())
                elif (l and "Location" not in l and "Download" not in l
                      and "Dist." not in l and not re.match(r"^P\.?S\.", l)
                      and not re.match(r"^\d+$", l)):
                    if len(l) > len(result.get("address") or ""):
                        result["address"] = clean(l)
            break

    # ── Commercial units ──
    for i, line in enumerate(lines):
        if re.search(r"No\.?\s+of\s+Commercial", line, re.I) and i + 1 < len(lines):
            result["commercial_units"] = safe_int(lines[i + 1])

    # ── BHK breakdown ──
    flat_sizes = []
    for line in lines:
        if re.search(r"\d+\s*BHK", line, re.I):
            flat_sizes.append(clean(line))
    if flat_sizes:
        result["flat_size_details"] = " | ".join(flat_sizes[:10])

    # ── Developer ──
    for t in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in t.find_all("th")]
        if "Promoter Name" in headers or "Firm Name" in headers:
            rows = t.find_all("tr")[1:]
            if rows:
                cells = rows[0].find_all("td")
                if len(cells) >= 2:
                    result["developer"] = clean(cells[1].get_text(strip=True))
            break

    result["_result"] = "MATCH"
    return result


# ──────────────────────────────────────────────────────────────────────────────
# STAGE 2 — project status
# ──────────────────────────────────────────────────────────────────────────────
def parse_status(project_id: str) -> dict:
    url    = f"{BASE_URL}/project-status.php?procode={project_id}"
    html   = fetch(url, project_id)
    result = {"project_id": project_id, "status_url": url, "_status_result": "FAIL"}

    if html is None:
        return result

    soup  = BeautifulSoup(html, "lxml")
    lines = lines_of(soup)

    for line in lines:
        m  = re.search(r"Quarter ending\s+([\d\-]+)",  line, re.I)
        m2 = re.search(r"Updated as on\s+([\d\-]+)",   line, re.I)
        if m:  result["quarter_ending"] = m.group(1)
        if m2: result["update_date"]    = m2.group(1)

    booking_map = {
        "Residential Apartments Booked": "apartments_booked",
        "Basement Parking Booked":       "basement_parking_booked",
        "Covered Car Parking Booked":    "covered_parking_booked",
        "Mechanical Parking Booked":     "mechanical_parking_booked",
        "Commercial Units Booked":       "commercial_units_booked",
    }
    for i, line in enumerate(lines):
        for kw, field in booking_map.items():
            if kw.lower() in line.lower() and i + 1 < len(lines):
                val = lines[i + 1].strip()
                result[field] = safe_int(val) if val else None

    for t in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in t.find_all("th")]
        if "Block/Building" in headers and "Construction Status" in headers:
            statuses = {}
            for row in t.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    s = cells[2].get_text(strip=True)
                    statuses[s] = statuses.get(s, 0) + 1
            result["construction_status_summary"] = " | ".join(
                f"{v} floors {k}" for k, v in sorted(statuses.items())
            )
            result["construction_details"] = f"{sum(statuses.values())} total floor entries recorded"
            break

    for t in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in t.find_all("th")]
        if "Construction Status" in headers and "Description" in headers:
            rows_data = []
            for row in t.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    status = clean(cells[1].get_text(strip=True))
                    desc   = clean(cells[2].get_text(strip=True))
                    if desc:
                        rows_data.append(f"{desc}: {status or '?'}")
            result["common_area_status"] = " | ".join(rows_data) if rows_data else None
            break

    result["_status_result"] = "OK"
    log.info("STATUS DONE | pid=%s | quarter=%s", project_id, result.get("quarter_ending"))
    return result


# ──────────────────────────────────────────────────────────────────────────────
# ANALYTICS
# ──────────────────────────────────────────────────────────────────────────────
def compute_analytics(rec: dict) -> dict:
    total   = rec.get("total_apartments")
    booked  = rec.get("apartments_booked")
    carpet  = rec.get("carpet_area_sqm")
    covered = rec.get("covered_parking")
    land    = rec.get("land_area_sqm")
    builtup = rec.get("builtup_area_sqm")

    rec["unsold_units"]                = (total - booked) if (total and booked is not None) else None
    rec["booking_rate_pct"]            = round(booked / total * 100, 2) if (total and booked) else None
    rec["avg_carpet_area_per_apt_sqm"] = round(carpet / total, 2)       if (carpet and total) else None
    rec["parking_ratio"]               = round(covered / total, 2)      if (covered and total) else None
    rec["fsi_builtup_land_ratio"]      = round(builtup / land, 2)       if (builtup and land) else None
    return rec


# ──────────────────────────────────────────────────────────────────────────────
# UPSERT + DIFF
# ──────────────────────────────────────────────────────────────────────────────
def upsert_projects(db, records: list[dict]):
    projects_col = db[COLLECTION_PROJECTS]
    changes_col  = db[COLLECTION_CHANGES]
    now          = datetime.now(timezone.utc)
    all_changes  = []
    ops          = []

    pids     = [r["project_id"] for r in records]
    existing = {
        doc["project_id"]: doc
        for doc in projects_col.find({"project_id": {"$in": pids}})
    }

    for rec in records:
        pid     = rec["project_id"]
        old_doc = existing.get(pid, {})

        diffs = diff_docs(old_doc, rec)
        all_changes.extend(diffs)
        if diffs:
            log.info("DIFF | pid=%s | changed_fields=%d", pid, len(diffs))

        update_doc = {**rec, "last_scraped_at": now}
        update_doc.pop("first_seen_at", None)  # prevent $set/$setOnInsert conflict

        ops.append(UpdateOne(
            {"project_id": pid},
            {"$set": update_doc, "$setOnInsert": {"first_seen_at": now}},
            upsert=True,
        ))

    if ops:
        try:
            result = projects_col.bulk_write(ops, ordered=False)
            log.info("UPSERT DONE | upserted=%d | modified=%d | total=%d",
                     result.upserted_count, result.modified_count, len(ops))
        except BulkWriteError as bwe:
            log.error("BULK WRITE ERROR | %s", bwe.details)

    if all_changes:
        changes_col.insert_many(all_changes)
        log.info("CHANGES LOGGED | count=%d", len(all_changes))


# ──────────────────────────────────────────────────────────────────────────────
# GEOCODER — runs after upsert, geocodes new projects only
# ──────────────────────────────────────────────────────────────────────────────
def geocode_new_projects(db):
    """Geocode any projects missing lat/lon using v2 multi-strategy approach."""
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    col   = db["projects"]
    query = {
        "$or": [
            {"lat": {"$exists": False}},
            {"geocode_failed": True},
            # Re-geocode city-center stuck coordinates
            {"lat": {"$gt": 22.570, "$lt": 22.575},
             "lon": {"$gt": 88.360, "$lt": 88.368}},
        ]
    }
    projects = list(col.find(query,
        {"project_id":1,"address":1,"pincode":1,"district":1,"police_station":1}))

    if not projects:
        log.info("GEOCODER | nothing to geocode")
        return

    log.info("GEOCODER START | projects=%d", len(projects))

    # Pre-load pincode cache
    pin_cache  = {}
    cache_lock = threading.Lock()
    for doc in col.find(
        {"lat":{"$exists":True},"geocode_failed":{"$ne":True}},
        {"pincode":1,"lat":1,"lon":1}
    ):
        pin = doc.get("pincode")
        lat, lon = doc.get("lat"), doc.get("lon")
        if pin and lat and lon:
            if not (22.570 < lat < 22.575 and 88.360 < lon < 88.368):
                pin_cache.setdefault(pin, (lat, lon))

    sess_local = threading.local()

    def get_sess():
        if not hasattr(sess_local, "s"):
            s = make_session()
            s.verify = True
            s.headers.update({"User-Agent": "WB-RERA-Dashboard/2.0"})
            sess_local.s = s
        return sess_local.s

    def nom(query_str=None, structured=None):
        time.sleep(1.1)
        try:
            s = get_sess()
            params = {**(structured or {"q": query_str}),
                      "format":"json","limit":1,"countrycodes":"in","addressdetails":0}
            r = s.get("https://nominatim.openstreetmap.org/search",
                       params=params, timeout=10, verify=True)
            data = r.json()
            if data:
                lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
                if not (22.570 < lat < 22.575 and 88.360 < lon < 88.368):
                    return lat, lon
        except Exception as e:
            log.debug("nom error: %s", e)
        return None

    def geocode_one(p):
        pid     = p["project_id"]
        pincode = (p.get("pincode") or "").strip()
        address = (p.get("address") or "").strip()
        district= (p.get("district") or "").strip()
        ps      = (p.get("police_station") or "").strip()
        result  = None
        strategy= None

        # 1. Cache
        if pincode:
            with cache_lock:
                cached = pin_cache.get(pincode)
            if cached:
                return pid, cached[0], cached[1], "cache"

        # 2. Structured pincode
        if pincode:
            r = nom(structured={"postalcode":pincode,"country":"India","state":"West Bengal"})
            if r:
                result, strategy = r, "pincode"
                with cache_lock: pin_cache[pincode] = r

        # 3. Address + pincode
        if not result and address and pincode:
            r = nom(f"{address[:80]}, {pincode}, West Bengal, India")
            if r: result, strategy = r, "address"

        # 4. Police station + district
        if not result and ps and district:
            r = nom(f"{ps}, {district}, West Bengal, India")
            if r:
                result, strategy = r, "ps_district"
                if pincode:
                    with cache_lock: pin_cache.setdefault(pincode, r)

        # 5. District
        if not result and district:
            r = nom(f"{district}, West Bengal, India")
            if r: result, strategy = r, "district"

        if result:
            return pid, result[0], result[1], strategy
        return pid, None, None, "failed"

    ops     = []
    success = 0
    failed  = 0
    now     = datetime.now(timezone.utc)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(geocode_one, p): p for p in projects}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                pid, lat, lon, strategy = fut.result()
                if lat and lon:
                    success += 1
                    ops.append(UpdateOne({"project_id": pid}, {"$set": {
                        "lat": lat, "lon": lon,
                        "geo_strategy": strategy,
                        "geocode_failed": False,
                        "geocoded_at": now,
                        "location": {"type":"Point","coordinates":[lon, lat]},
                    }}))
                else:
                    failed += 1
                    ops.append(UpdateOne({"project_id": pid},
                        {"$set": {"geocode_failed": True, "geocoded_at": now}}))

                if len(ops) >= 50:
                    col.bulk_write(ops, ordered=False)
                    ops = []
                    log.info("GEOCODER | done=%d/%d | ok=%d | fail=%d",
                             i, len(projects), success, failed)
            except Exception as e:
                log.error("GEOCODER exception: %s", e)

    if ops:
        col.bulk_write(ops, ordered=False)
    log.info("GEOCODER END | success=%d | failed=%d", success, failed)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPE RUN
# ──────────────────────────────────────────────────────────────────────────────
def run_scrape():
    log.info("=" * 70)
    log.info("SCRAPE START | timestamp=%s", datetime.now().isoformat())
    log.info("=" * 70)

    client, db = get_db()

    try:
        sess    = make_session()
        all_ids = get_all_project_ids(sess)
        if not all_ids:
            log.error("No project IDs found — aborting.")
            return

        total = len(all_ids)

        # ── Stage 1: Details ──
        log.info("STAGE 1 START | total=%d | workers=%d", total, MAX_WORKERS_DETAILS)
        detail_records: list[dict] = []
        failed_details: list[str]  = []
        done = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAILS) as ex:
            futures = {ex.submit(parse_details, pid): pid for pid in all_ids}
            for fut in as_completed(futures):
                pid  = futures[fut]
                done += 1
                try:
                    rec  = fut.result()
                    flag = rec.pop("_result", "FAIL")
                    if flag == "MATCH":
                        detail_records.append(rec)
                    else:
                        failed_details.append(pid)
                except Exception as e:
                    log.error("EXCEPTION details | pid=%s | err=%s", pid, e)
                    failed_details.append(pid)

                if done % 500 == 0 or done == total:
                    log.info("STAGE 1 PROGRESS | done=%d/%d | matched=%d",
                             done, total, len(detail_records))

        log.info("STAGE 1 COMPLETE | matched=%d | failed=%d",
                 len(detail_records), len(failed_details))

        if not detail_records:
            log.warning("No detail records — aborting.")
            return

        # ── Stage 2: Status ──
        log.info("STAGE 2 START | projects=%d | workers=%d", len(detail_records), MAX_WORKERS_STATUS)
        status_map:    dict[str, dict] = {}
        failed_status: list[str]       = []
        done2  = 0
        total2 = len(detail_records)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_STATUS) as ex:
            futures = {ex.submit(parse_status, pid): pid for pid in [r["project_id"] for r in detail_records]}
            for fut in as_completed(futures):
                pid   = futures[fut]
                done2 += 1
                try:
                    sdata = fut.result()
                    flag  = sdata.pop("_status_result", "FAIL")
                    if flag == "OK":
                        status_map[pid] = sdata
                    else:
                        failed_status.append(pid)
                except Exception as e:
                    log.error("EXCEPTION status | pid=%s | err=%s", pid, e)
                    failed_status.append(pid)

                if done2 % 100 == 0 or done2 == total2:
                    log.info("STAGE 2 PROGRESS | done=%d/%d | ok=%d",
                             done2, total2, len(status_map))

        # ── Merge + analytics ──
        for rec in detail_records:
            if rec["project_id"] in status_map:
                rec.update(status_map[rec["project_id"]])
            compute_analytics(rec)

        # ── Upsert ──
        log.info("MONGO UPSERT START | records=%d", len(detail_records))
        upsert_projects(db, detail_records)

        # ── Geocode new projects ──
        log.info("GEOCODER: checking for new projects to geocode...")
        geocode_new_projects(db)

        log.info("=" * 70)
        log.info("SCRAPE END | total=%d | matched=%d | failed_details=%d | failed_status=%d",
                 total, len(detail_records), len(failed_details), len(failed_status))
        log.info("=" * 70)

    finally:
        client.close()


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if "--once" in sys.argv:
        run_scrape()
    else:
        scheduler = BlockingScheduler(timezone="Asia/Kolkata")
        scheduler.add_job(
            run_scrape,
            trigger="cron",
            hour=SCRAPE_HOUR,
            minute=SCRAPE_MINUTE,
            id="wbrera_daily_scrape",
            max_instances=1,
            misfire_grace_time=3600,
        )
        log.info("SCHEDULER STARTED | next run at %02d:%02d Asia/Kolkata daily",
                 SCRAPE_HOUR, SCRAPE_MINUTE)
        log.info("Running initial scrape on startup...")
        run_scrape()
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("Scheduler stopped.")