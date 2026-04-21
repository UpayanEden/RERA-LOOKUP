#!/usr/bin/env python3
"""
WB-RERA Geocoder
================
Geocodes all projects in MongoDB using Nominatim (OpenStreetMap).
Free, no API key required. Respects 1 req/sec rate limit.

Adds lat/lon fields to each project document.
Only geocodes projects that don't already have coordinates.

Usage:
  pip install requests pymongo
  export MONGO_URI="mongodb+srv://..."
  python geocoder.py           # geocode all missing
  python geocoder.py --force   # re-geocode everything
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone

import requests
from pymongo import MongoClient, UpdateOne

MONGO_URI  = os.environ.get("MONGO_URI", "mongodb+srv://...")
MONGO_DB   = "wbrera"
COL        = "projects"

NOMINATIM_URL    = "https://nominatim.openstreetmap.org/search"
REQUEST_DELAY    = 1.1   # seconds between requests (Nominatim policy: 1/sec)
USER_AGENT       = "WB-RERA-Dashboard/1.0 (educational project)"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"geocode_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ],
)
log = logging.getLogger("geocoder")


def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    db = client[MONGO_DB]
    # Add geo index for map queries
    db[COL].create_index([("location", "2dsphere")], sparse=True)
    log.info("MONGO CONNECTED | db=%s", MONGO_DB)
    return client, db


def geocode_nominatim(address: str, pincode: str, district: str) -> dict | None:
    """
    Try multiple query strategies from most to least specific.
    Returns {"lat": float, "lon": float, "display_name": str} or None.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    queries = []

    # Strategy 1: full address + pincode + West Bengal
    if address and pincode:
        queries.append(f"{address}, {pincode}, West Bengal, India")

    # Strategy 2: pincode + district + West Bengal
    if pincode and district:
        queries.append(f"{district}, {pincode}, West Bengal, India")

    # Strategy 3: just pincode + West Bengal
    if pincode:
        queries.append(f"{pincode}, West Bengal, India")

    for query in queries:
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.get(
                NOMINATIM_URL,
                params={
                    "q":              query,
                    "format":         "json",
                    "limit":          1,
                    "countrycodes":   "in",
                    "addressdetails": 0,
                },
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json()
            if results:
                r = results[0]
                return {
                    "lat":          float(r["lat"]),
                    "lon":          float(r["lon"]),
                    "display_name": r.get("display_name", ""),
                    "geo_query":    query,
                    "geo_source":   "nominatim",
                }
        except Exception as e:
            log.warning("GEOCODE ERROR | query=%s | %s", query[:60], e)

    return None


def run_geocoder(force: bool = False):
    log.info("=" * 60)
    log.info("GEOCODER START | force=%s | %s", force, datetime.now().isoformat())
    log.info("=" * 60)

    client, db = get_db()
    col = db[COL]

    try:
        # Find projects needing geocoding
        if force:
            query = {}
        else:
            query = {"lat": {"$exists": False}}

        projects = list(col.find(
            query,
            {"project_id": 1, "address": 1, "pincode": 1, "district": 1},
        ))

        total   = len(projects)
        success = 0
        failed  = 0
        ops     = []

        log.info("Projects to geocode: %d", total)

        for i, project in enumerate(projects, 1):
            pid      = project["project_id"]
            address  = project.get("address") or ""
            pincode  = project.get("pincode") or ""
            district = project.get("district") or ""

            result = geocode_nominatim(address, pincode, district)

            if result:
                success += 1
                ops.append(UpdateOne(
                    {"project_id": pid},
                    {"$set": {
                        "lat":          result["lat"],
                        "lon":          result["lon"],
                        "geo_query":    result["geo_query"],
                        "geo_source":   result["geo_source"],
                        "geocoded_at":  datetime.now(timezone.utc),
                        # GeoJSON point for $near queries
                        "location": {
                            "type":        "Point",
                            "coordinates": [result["lon"], result["lat"]],
                        },
                    }},
                ))
                log.debug("OK | pid=%s | pin=%s | lat=%.4f lon=%.4f",
                          pid, pincode, result["lat"], result["lon"])
            else:
                failed += 1
                ops.append(UpdateOne(
                    {"project_id": pid},
                    {"$set": {"geocode_failed": True, "geocoded_at": datetime.now(timezone.utc)}},
                ))
                log.warning("FAIL | pid=%s | pin=%s | addr=%s", pid, pincode, address[:40])

            # Bulk write every 50 records
            if len(ops) >= 50:
                col.bulk_write(ops, ordered=False)
                ops = []
                log.info("PROGRESS | done=%d/%d | ok=%d | fail=%d",
                         i, total, success, failed)

        # Final flush
        if ops:
            col.bulk_write(ops, ordered=False)

        log.info("=" * 60)
        log.info("GEOCODER END | total=%d | success=%d | failed=%d",
                 total, success, failed)
        log.info("=" * 60)

    finally:
        client.close()


if __name__ == "__main__":
    force = "--force" in sys.argv
    run_geocoder(force=force)