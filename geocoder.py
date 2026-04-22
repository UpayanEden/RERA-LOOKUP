#!/usr/bin/env python3
"""
WB-RERA Geocoder v2 — Faster & More Accurate
=============================================
Strategy (in order):
  1. Pincode centroid via postcodes.io / India Post data (most accurate for WB)
  2. Address + pincode via Nominatim
  3. Police station + district via Nominatim
  4. Just pincode via Nominatim

Speed improvements:
  - Parallel workers (4 threads, each respecting 1 req/sec)
  - Pincode cache — same pincode reused across projects
  - Skip city-center fallback coordinates
  - Batch MongoDB writes every 50 records

Usage:
  export MONGO_URI="mongodb+srv://..."
  python geocoder.py              # geocode only missing
  python geocoder.py --force      # re-geocode everything
  python geocoder.py --fix-center # re-geocode city-center stuck projects only
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from pymongo import MongoClient, UpdateOne

MONGO_URI  = os.environ.get("MONGO_URI", "mongodb+srv://...")
MONGO_DB   = "wbrera"
COL        = "projects"

# Nominatim rate limit: 1 req/sec per thread
# With 4 threads = 4 req/sec effective = ~18 mins for 4226 projects
REQUEST_DELAY = 1.1
MAX_WORKERS   = 4
USER_AGENT    = "WB-RERA-Dashboard/2.0 (educational project)"

# Generic city center coordinates to detect bad geocoding
BAD_COORDS = [
    (22.5726, 88.3639),   # Kolkata center
    (22.9868, 87.8550),   # WB center
    (22.0, 88.0),         # Generic WB
]

def is_bad_coord(lat, lon):
    for blat, blon in BAD_COORDS:
        if abs(lat - blat) < 0.005 and abs(lon - blon) < 0.005:
            return True
    return False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"geocode_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger("geocoder")

# ── Thread-local session ──────────────────────────────────────────────────────
_tl = threading.local()
_pin_cache = {}  # pincode → (lat, lon) cache shared across threads
_cache_lock = threading.Lock()

def get_session():
    if not hasattr(_tl, "sess"):
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en"})
        _tl.sess = s
    return _tl.sess

# ── Nominatim query ───────────────────────────────────────────────────────────
def nominatim(query: str, structured: dict = None) -> tuple[float, float] | None:
    """Single Nominatim request. Returns (lat, lon) or None."""
    sess = get_session()
    time.sleep(REQUEST_DELAY)
    try:
        if structured:
            params = {**structured, "format": "json", "limit": 1,
                      "countrycodes": "in", "addressdetails": 0}
        else:
            params = {"q": query, "format": "json", "limit": 1,
                      "countrycodes": "in", "addressdetails": 0}

        resp = sess.get(
            "https://nominatim.openstreetmap.org/search",
            params=params, timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            lat = float(results[0]["lat"])
            lon = float(results[0]["lon"])
            if not is_bad_coord(lat, lon):
                return lat, lon
    except Exception as e:
        log.debug("Nominatim error | query=%s | %s", query[:50], e)
    return None

# ── Per-project geocode ───────────────────────────────────────────────────────
def geocode_project(project: dict) -> dict:
    pid      = project["project_id"]
    pincode  = (project.get("pincode") or "").strip()
    address  = (project.get("address") or "").strip()
    district = (project.get("district") or "").strip()
    ps       = (project.get("police_station") or "").strip()

    result = None
    strategy_used = None

    # ── Strategy 1: Pincode cache ──
    if pincode:
        with _cache_lock:
            cached = _pin_cache.get(pincode)
        if cached:
            result = cached
            strategy_used = "pincode_cache"

    # ── Strategy 2: Structured pincode query ──
    if not result and pincode:
        r = nominatim("", structured={
            "postalcode": pincode,
            "country":    "India",
            "state":      "West Bengal",
        })
        if r:
            result = r
            strategy_used = "pincode_structured"
            with _cache_lock:
                _pin_cache[pincode] = r

    # ── Strategy 3: Address + pincode ──
    if not result and address and pincode:
        # Clean address — remove very long cadastral descriptions
        clean_addr = address[:80] if len(address) > 80 else address
        r = nominatim(f"{clean_addr}, {pincode}, West Bengal, India")
        if r:
            result = r
            strategy_used = "address_pincode"

    # ── Strategy 4: Police station + district ──
    if not result and ps and district:
        r = nominatim(f"{ps} Police Station, {district}, West Bengal, India")
        if r:
            result = r
            strategy_used = "police_station"
            # Cache this as pincode fallback
            if pincode:
                with _cache_lock:
                    _pin_cache.setdefault(pincode, r)

    # ── Strategy 5: District centroid ──
    if not result and district:
        r = nominatim(f"{district} district, West Bengal, India")
        if r:
            result = r
            strategy_used = "district"

    # ── Strategy 6: Plain pincode ──
    if not result and pincode:
        r = nominatim(f"{pincode}, India")
        if r:
            result = r
            strategy_used = "pincode_plain"
            with _cache_lock:
                _pin_cache.setdefault(pincode, r)

    now = datetime.now(timezone.utc)
    if result:
        lat, lon = result
        log.debug("OK | pid=%s | pin=%s | strategy=%s | %.4f,%.4f",
                  pid, pincode, strategy_used, lat, lon)
        return {
            "project_id": pid,
            "update": {
                "lat":          lat,
                "lon":          lon,
                "geo_strategy": strategy_used,
                "geocoded_at":  now,
                "geocode_failed": False,
                "location": {
                    "type":        "Point",
                    "coordinates": [lon, lat],
                },
            },
            "success": True,
        }
    else:
        log.warning("FAIL | pid=%s | pin=%s | addr=%s", pid, pincode, address[:40])
        return {
            "project_id": pid,
            "update": {"geocode_failed": True, "geocoded_at": now},
            "success": False,
        }


# ── Main ──────────────────────────────────────────────────────────────────────
def run_geocoder(force: bool = False, fix_center: bool = False):
    log.info("=" * 60)
    log.info("GEOCODER v2 START | force=%s | fix_center=%s | workers=%d",
             force, fix_center, MAX_WORKERS)
    log.info("=" * 60)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    db  = client[MONGO_DB]
    col = db[COL]

    # Create 2dsphere index
    col.create_index([("location", "2dsphere")], sparse=True)

    if force:
        query = {}
    elif fix_center:
        # Re-geocode projects stuck at city center coordinates
        query = {
            "$or": [
                {"lat": {"$gt": 22.570, "$lt": 22.575},
                 "lon": {"$gt": 88.360, "$lt": 88.368}},
                {"lat": {"$exists": False}},
                {"geocode_failed": True},
            ]
        }
    else:
        query = {"lat": {"$exists": False}}

    projects = list(col.find(
        query,
        {"project_id": 1, "address": 1, "pincode": 1,
         "district": 1, "police_station": 1},
    ))

    total   = len(projects)
    success = 0
    failed  = 0
    ops     = []

    log.info("Projects to geocode: %d", total)
    if total == 0:
        log.info("Nothing to geocode.")
        client.close()
        return

    # Pre-populate pincode cache from already-geocoded projects
    log.info("Pre-loading pincode cache from existing geocoded projects...")
    existing = col.find(
        {"lat": {"$exists": True}, "geocode_failed": {"$ne": True}},
        {"pincode": 1, "lat": 1, "lon": 1}
    )
    for doc in existing:
        pin = doc.get("pincode")
        lat = doc.get("lat")
        lon = doc.get("lon")
        if pin and lat and lon and not is_bad_coord(lat, lon):
            _pin_cache.setdefault(pin, (lat, lon))
    log.info("Pincode cache loaded: %d pincodes", len(_pin_cache))

    done = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(geocode_project, p): p for p in projects}
        for fut in as_completed(futures):
            done += 1
            try:
                res = fut.result()
                if res["success"]:
                    success += 1
                else:
                    failed += 1

                ops.append(UpdateOne(
                    {"project_id": res["project_id"]},
                    {"$set": res["update"]},
                ))

                if len(ops) >= 50:
                    col.bulk_write(ops, ordered=False)
                    ops = []
                    log.info("PROGRESS | done=%d/%d | ok=%d | fail=%d | cache=%d",
                             done, total, success, failed, len(_pin_cache))

            except Exception as e:
                log.error("EXCEPTION | pid=%s | %s", futures[fut].get("project_id"), e)
                failed += 1

    if ops:
        col.bulk_write(ops, ordered=False)

    log.info("=" * 60)
    log.info("GEOCODER END | total=%d | success=%d | failed=%d",
             total, success, failed)
    log.info("=" * 60)
    client.close()


if __name__ == "__main__":
    force      = "--force"       in sys.argv
    fix_center = "--fix-center"  in sys.argv
    run_geocoder(force=force, fix_center=fix_center)