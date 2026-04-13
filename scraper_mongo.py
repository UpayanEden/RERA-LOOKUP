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

Collections:
  projects  — one doc per project, keyed on project_id
  changes   — append-only log of field-level diffs

Schedule:
  Runs via APScheduler cron — configure SCRAPE_HOUR / SCRAPE_MINUTE below.
  To run once immediately (no schedule): python wbrera_scraper_mongo.py --once

Usage:
  pip install requests beautifulsoup4 lxml pymongo apscheduler
  Set MONGO_URI environment variable (or edit MONGO_URI below).
  python wbrera_scraper_mongo.py            # starts scheduler
  python wbrera_scraper_mongo.py --once     # single run then exit
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
# CONFIGURATION — edit these as needed
# ─────────────────────────────────────────────
MONGO_URI             = os.environ.get("MONGO_URI", "mongodb+srv://<user>:<pass>@cluster.mongodb.net/")
MONGO_DB              = "wbrera"
COLLECTION_PROJECTS   = "projects"
COLLECTION_CHANGES    = "changes"

DISTRICT_URL          = "https://rera.wb.gov.in/district_project.php?dcode=0"
BASE_URL              = "https://rera.wb.gov.in"
REQUEST_TIMEOUT       = 30
MAX_WORKERS_DETAILS   = 8
MAX_WORKERS_STATUS    = 6
MAX_RETRIES           = 3
RETRY_BACKOFF         = 2.0

# Cron schedule — default: every day at 02:00 local time
SCRAPE_HOUR           = 2
SCRAPE_MINUTE         = 0

# Fields to exclude from change-diff (metadata, not project data)
DIFF_EXCLUDE_FIELDS   = {"_id", "last_scraped_at", "first_seen_at", "_result", "_status_result"}
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f"wbrera_mongo_{datetime.now().strftime('%Y%m%d')}.log"
        ),
    ],
)
log = logging.getLogger("wbrera")


# ──────────────────────────────────────────────────────────────────────────────
# MONGO CLIENT
# ──────────────────────────────────────────────────────────────────────────────
def get_db():
    """Return (client, db) — call once per run, not per thread."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")          # fail fast if URI is wrong
    db = client[MONGO_DB]

    # Indexes (idempotent)
    db[COLLECTION_PROJECTS].create_index("project_id", unique=True)
    db[COLLECTION_PROJECTS].create_index("pincode")
    db[COLLECTION_PROJECTS].create_index("project_status")
    db[COLLECTION_CHANGES].create_index([("project_id", 1), ("changed_at", -1)])
    db[COLLECTION_CHANGES].create_index("changed_at")

    log.info("MONGO CONNECTED | db=%s", MONGO_DB)
    return client, db


# ──────────────────────────────────────────────────────────────────────────────
# DIFF HELPER
# ──────────────────────────────────────────────────────────────────────────────
def diff_docs(old: dict, new: dict) -> list[dict]:
    """
    Compare old and new project dicts.
    Returns a list of change records, one per changed field:
      { project_id, field, old_value, new_value, changed_at }
    """
    changes = []
    now = datetime.now(timezone.utc)
    pid = new.get("project_id", "unknown")
    all_keys = set(old.keys()) | set(new.keys())
    for key in all_keys:
        if key in DIFF_EXCLUDE_FIELDS:
            continue
        old_val = old.get(key)
        new_val = new.get(key)
        if old_val != new_val:
            changes.append({
                "project_id":  pid,
                "field":       key,
                "old_value":   old_val,
                "new_value":   new_val,
                "changed_at":  now,
            })
    return changes


# ──────────────────────────────────────────────────────────────────────────────
# SSL ADAPTER
# ──────────────────────────────────────────────────────────────────────────────
class LegacySSLAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
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
    sess.mount("http://", adapter)
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return sess


_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = make_session()
    return _thread_local.session


# ──────────────────────────────────────────────────────────────────────────────
# HTTP HELPERS
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
            log.warning(
                "FETCH FAIL | pid=%s | attempt=%d/%d | err=%s | retry in %.1fs",
                project_id, attempt, MAX_RETRIES, exc, wait,
            )
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


# ──────────────────────────────────────────────────────────────────────────────
# DISTRICT PAGE — extract project IDs
# ──────────────────────────────────────────────────────────────────────────────
def get_all_project_ids(sess: requests.Session) -> list[str]:
    log.info("DISTRICT PAGE LOADING | url=%s", DISTRICT_URL)
    html = fetch(DISTRICT_URL)
    if not html:
        log.error("DISTRICT PAGE FAILED — cannot proceed")
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
    url = f"{BASE_URL}/project_details.php?procode={project_id}"
    html = fetch(url, project_id)
    result = {"project_id": project_id, "details_url": url, "_result": "FAIL"}

    if html is None:
        return result

    soup = BeautifulSoup(html, "lxml")
    lines = lines_of(soup)

    # Pincode
    pincode = None
    for line in lines:
        m = re.search(r"\bPin\s+(\d{6})\b", line)
        if m:
            pincode = m.group(1)
            break
    result["pincode"] = pincode

    # Project name
    project_name = None
    for i, line in enumerate(lines):
        if "PROJECT STATUS -" in line or "PROJECT ID:" in line:
            if i > 0:
                candidate = clean(lines[i - 1])
                if candidate and len(candidate) < 120:
                    project_name = candidate
            break
    result["project_name"] = project_name

    # Status, ID display, dates, RERA reg
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

    # Extension date
    for i, line in enumerate(lines):
        if "EXTENSION COMPLETION DATE:" in line:
            inline = line.replace("EXTENSION COMPLETION DATE:", "").strip()
            if inline and inline.upper() not in ("NA", "N/A"):
                result["extension_completion_date"] = clean(inline)
            elif i + 1 < len(lines):
                result["extension_completion_date"] = clean(lines[i + 1])
            break

    # Project type from Highlights block
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

    # Specification block
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

    # Parking
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

    # Location block
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

    # Commercial units
    for i, line in enumerate(lines):
        if re.search(r"No\.?\s+of\s+Commercial", line, re.I) and i + 1 < len(lines):
            result["commercial_units"] = safe_int(lines[i + 1])

    # BHK breakdown
    flat_sizes = []
    for line in lines:
        if re.search(r"\d+\s*BHK", line, re.I):
            flat_sizes.append(clean(line))
    if flat_sizes:
        result["flat_size_details"] = " | ".join(flat_sizes[:10])

    # Developer / promoter
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
    url = f"{BASE_URL}/project-status.php?procode={project_id}"
    html = fetch(url, project_id)
    result = {"project_id": project_id, "status_url": url, "_status_result": "FAIL"}

    if html is None:
        return result

    soup = BeautifulSoup(html, "lxml")
    lines = lines_of(soup)

    for line in lines:
        m = re.search(r"Quarter ending\s+([\d\-]+)", line, re.I)
        if m:
            result["quarter_ending"] = m.group(1)
        m2 = re.search(r"Updated as on\s+([\d\-]+)", line, re.I)
        if m2:
            result["update_date"] = m2.group(1)

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

    # Construction status
    for t in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in t.find_all("th")]
        if "Block/Building" in headers and "Construction Status" in headers:
            statuses = {}
            for row in t.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    status = cells[2].get_text(strip=True)
                    statuses[status] = statuses.get(status, 0) + 1
            result["construction_status_summary"] = " | ".join(
                f"{v} floors {k}" for k, v in sorted(statuses.items())
            )
            result["construction_details"] = (
                f"{sum(statuses.values())} total floor entries recorded"
            )
            break

    # Common area status
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
    total  = rec.get("total_apartments")
    booked = rec.get("apartments_booked")
    carpet = rec.get("carpet_area_sqm")
    covered= rec.get("covered_parking")
    land   = rec.get("land_area_sqm")
    builtup= rec.get("builtup_area_sqm")

    rec["unsold_units"]               = (total - booked) if (total and booked is not None) else None
    rec["booking_rate_pct"]           = round(booked / total * 100, 2) if (total and booked) else None
    rec["avg_carpet_area_per_apt_sqm"]= round(carpet / total, 2)       if (carpet and total) else None
    rec["parking_ratio"]              = round(covered / total, 2)      if (covered and total) else None
    rec["fsi_builtup_land_ratio"]     = round(builtup / land, 2)       if (builtup and land) else None
    return rec


# ──────────────────────────────────────────────────────────────────────────────
# UPSERT + DIFF
# ──────────────────────────────────────────────────────────────────────────────
def upsert_projects(db, records: list[dict]):
    """
    Upsert all records into `projects`.
    For each record, diff against the existing doc and write to `changes`.
    """
    projects_col = db[COLLECTION_PROJECTS]
    changes_col  = db[COLLECTION_CHANGES]

    now = datetime.now(timezone.utc)
    all_changes = []
    ops = []

    # Fetch existing docs in one query
    pids = [r["project_id"] for r in records]
    existing = {
        doc["project_id"]: doc
        for doc in projects_col.find({"project_id": {"$in": pids}})
    }

    for rec in records:
        pid = rec["project_id"]
        old_doc = existing.get(pid, {})

        # Compute diffs
        diffs = diff_docs(old_doc, rec)
        all_changes.extend(diffs)
        if diffs:
            log.info("DIFF | pid=%s | changed_fields=%d", pid, len(diffs))

        # Build upsert op — first_seen_at only via $setOnInsert, never in $set
        update_doc = {**rec, "last_scraped_at": now}
        update_doc.pop("first_seen_at", None)   # prevent $set/$setOnInsert conflict

        ops.append(UpdateOne(
            {"project_id": pid},
            {"$set": update_doc, "$setOnInsert": {"first_seen_at": now}},
            upsert=True,
        ))

    # Bulk write projects
    if ops:
        try:
            result = projects_col.bulk_write(ops, ordered=False)
            log.info(
                "UPSERT DONE | upserted=%d | modified=%d | total=%d",
                result.upserted_count, result.modified_count, len(ops),
            )
        except BulkWriteError as bwe:
            log.error("BULK WRITE ERROR | %s", bwe.details)

    # Insert change records
    if all_changes:
        changes_col.insert_many(all_changes)
        log.info("CHANGES LOGGED | count=%d", len(all_changes))


# ──────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPE RUN
# ──────────────────────────────────────────────────────────────────────────────
def run_scrape():
    log.info("=" * 70)
    log.info("SCRAPE START | timestamp=%s", datetime.now().isoformat())
    log.info("=" * 70)

    client, db = get_db()

    try:
        sess = make_session()
        all_ids = get_all_project_ids(sess)
        if not all_ids:
            log.error("No project IDs found — aborting.")
            return

        total = len(all_ids)

        # ── STAGE 1: Details ──
        log.info("STAGE 1 START | total=%d | workers=%d", total, MAX_WORKERS_DETAILS)
        detail_records: list[dict] = []
        failed_details: list[str]  = []
        done = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAILS) as ex:
            futures = {ex.submit(parse_details, pid): pid for pid in all_ids}
            for fut in as_completed(futures):
                pid = futures[fut]
                done += 1
                try:
                    rec = fut.result()
                    flag = rec.pop("_result", "FAIL")
                    if flag == "MATCH":
                        detail_records.append(rec)
                    else:
                        failed_details.append(pid)
                except Exception as e:
                    log.error("EXCEPTION details | pid=%s | err=%s", pid, e)
                    failed_details.append(pid)

                if done % 500 == 0 or done == total:
                    log.info(
                        "STAGE 1 PROGRESS | done=%d/%d | matched=%d",
                        done, total, len(detail_records),
                    )

        log.info(
            "STAGE 1 COMPLETE | matched=%d | failed=%d",
            len(detail_records), len(failed_details),
        )

        if not detail_records:
            log.warning("No detail records scraped — aborting.")
            return

        # ── STAGE 2: Status ──
        log.info("STAGE 2 START | projects=%d | workers=%d", len(detail_records), MAX_WORKERS_STATUS)
        status_map:    dict[str, dict] = {}
        failed_status: list[str]       = []
        done2 = 0
        total2 = len(detail_records)
        match_ids = [r["project_id"] for r in detail_records]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_STATUS) as ex:
            futures = {ex.submit(parse_status, pid): pid for pid in match_ids}
            for fut in as_completed(futures):
                pid = futures[fut]
                done2 += 1
                try:
                    sdata = fut.result()
                    flag = sdata.pop("_status_result", "FAIL")
                    if flag == "OK":
                        status_map[pid] = sdata
                    else:
                        failed_status.append(pid)
                except Exception as e:
                    log.error("EXCEPTION status | pid=%s | err=%s", pid, e)
                    failed_status.append(pid)

                if done2 % 100 == 0 or done2 == total2:
                    log.info(
                        "STAGE 2 PROGRESS | done=%d/%d | ok=%d",
                        done2, total2, len(status_map),
                    )

        # ── Merge + analytics ──
        for rec in detail_records:
            pid = rec["project_id"]
            if pid in status_map:
                rec.update(status_map[pid])
            compute_analytics(rec)

        # ── Upsert to MongoDB ──
        log.info("MONGO UPSERT START | records=%d", len(detail_records))
        upsert_projects(db, detail_records)

        log.info("=" * 70)
        log.info(
            "SCRAPE END | total=%d | matched=%d | failed_details=%d | failed_status=%d",
            total, len(detail_records), len(failed_details), len(failed_status),
        )
        log.info("=" * 70)

    finally:
        client.close()


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if "--once" in sys.argv:
        # Single run — useful for manual trigger / first-time seed
        run_scrape()
    else:
        # Scheduled run via APScheduler
        scheduler = BlockingScheduler(timezone="Asia/Kolkata")
        scheduler.add_job(
            run_scrape,
            trigger="cron",
            hour=SCRAPE_HOUR,
            minute=SCRAPE_MINUTE,
            id="wbrera_daily_scrape",
            max_instances=1,       # prevent overlapping runs
            misfire_grace_time=3600,
        )
        log.info(
            "SCHEDULER STARTED | next run at %02d:%02d Asia/Kolkata daily",
            SCRAPE_HOUR, SCRAPE_MINUTE,
        )
        # Also run once immediately on startup so you don't wait till 2am
        log.info("Running initial scrape on startup...")
        run_scrape()
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("Scheduler stopped.")