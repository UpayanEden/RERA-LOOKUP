#!/usr/bin/env python3
"""
WB-RERA Price Scraper
=====================
Fetches property listings from 99acres and Housing.com for each
project in the `projects` collection, matches by project name,
and stores results in a `prices` collection in MongoDB.

Runs daily via APScheduler (same pattern as the main scraper).

MongoDB collection: prices
  {
    project_id:     str,
    project_name:   str,
    pincode:        str,
    sources: [
      {
        portal:     "99acres" | "housing",
        url:        str,
        bhk:        int | null,
        area_type:  "super_built_up" | "built_up" | "carpet" | null,
        area_min:   float | null,   # sqft
        area_max:   float | null,
        price_min:  float | null,   # INR
        price_max:  float | null,
        price_sqft: float | null,
        listing_count: int,
        fetched_at: datetime,
      }
    ],
    last_fetched_at: datetime,
  }

Usage:
  pip install requests pymongo apscheduler
  export MONGO_URI="mongodb+srv://..."
  python price_scraper.py --once     # single run
  python price_scraper.py            # scheduled daily at 03:00 IST
"""

import os
import sys
import re
import time
import json
import random
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
from pymongo import MongoClient, UpdateOne
from apscheduler.schedulers.blocking import BlockingScheduler

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MONGO_URI   = os.environ.get("MONGO_URI", "mongodb+srv://...")
MONGO_DB    = "wbrera"
COL_PROJECTS= "projects"
COL_PRICES  = "prices"

MAX_WORKERS = 4          # parallel project lookups
DELAY_MIN   = 1.5        # seconds between requests (per thread)
DELAY_MAX   = 3.5
SCRAPE_HOUR = 3          # 3am IST daily
SCRAPE_MIN  = 0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(),
              logging.FileHandler(f"price_scrape_{datetime.now().strftime('%Y%m%d')}.log")],
)
log = logging.getLogger("prices")

# ─────────────────────────────────────────────
# MONGO
# ─────────────────────────────────────────────
def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    db = client[MONGO_DB]
    db[COL_PRICES].create_index("project_id", unique=True)
    db[COL_PRICES].create_index("pincode")
    db[COL_PRICES].create_index("last_fetched_at")
    log.info("MONGO CONNECTED | db=%s", MONGO_DB)
    return client, db

# ─────────────────────────────────────────────
# HTTP SESSION (per thread)
# ─────────────────────────────────────────────
_tl = threading.local()

HEADERS_99ACRES = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.99acres.com/",
    "X-Requested-With": "XMLHttpRequest",
}

HEADERS_HOUSING = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://housing.com/",
    "Origin": "https://housing.com",
}

def get_session() -> requests.Session:
    if not hasattr(_tl, "session"):
        s = requests.Session()
        s.headers.update(HEADERS_99ACRES)
        _tl.session = s
    return _tl.session

def jitter():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

# ─────────────────────────────────────────────
# TEXT HELPERS
# ─────────────────────────────────────────────
def clean_name(name: str) -> str:
    """Normalise project name for fuzzy matching."""
    return re.sub(r"[^a-z0-9\s]", "", name.lower()).strip()

def name_match_score(query: str, candidate: str) -> float:
    """Simple word overlap score 0-1."""
    q_words = set(clean_name(query).split())
    c_words = set(clean_name(candidate).split())
    if not q_words:
        return 0.0
    overlap = q_words & c_words
    return len(overlap) / len(q_words)

def sqft_from_str(text: str) -> Optional[float]:
    """Extract sqft value from strings like '1200 sq.ft', '1,200 - 1,500 sq ft'."""
    if not text:
        return None
    nums = re.findall(r"[\d,]+(?:\.\d+)?", text.replace(",", ""))
    vals = [float(n) for n in nums if n]
    return vals[0] if vals else None

def price_from_str(text: str) -> Optional[float]:
    """Convert price strings like '₹45 L', '1.2 Cr', '45,00,000' to INR float."""
    if not text:
        return None
    text = text.replace(",", "").replace("₹", "").strip()
    m_cr = re.search(r"([\d.]+)\s*[Cc]r", text)
    m_l  = re.search(r"([\d.]+)\s*[Ll]", text)
    m_raw= re.search(r"([\d.]+)", text)
    if m_cr:
        return float(m_cr.group(1)) * 1_00_00_000
    if m_l:
        return float(m_l.group(1)) * 1_00_000
    if m_raw:
        return float(m_raw.group(1))
    return None

def bhk_from_str(text: str) -> Optional[int]:
    m = re.search(r"(\d)\s*[Bb][Hh][Kk]", str(text))
    return int(m.group(1)) if m else None

# ─────────────────────────────────────────────
# 99ACRES SCRAPER
# ─────────────────────────────────────────────
def fetch_99acres(project_name: str, pincode: str) -> list[dict]:
    """
    Uses 99acres internal search API.
    Endpoint discovered via browser DevTools network tab.
    Returns list of normalised listing dicts.
    """
    results = []
    sess = get_session()

    # Step 1: get city/locality suggestion for pincode
    try:
        jitter()
        suggest_url = (
            f"https://www.99acres.com/api/v2/typeahead/suggestions"
            f"?q={pincode}&intent=BUY&type=locality,city,project"
        )
        r = sess.get(suggest_url, timeout=15)
        r.raise_for_status()
        suggestions = r.json().get("data", {}).get("suggestions", [])
        locality_id = None
        for s in suggestions:
            if s.get("type") == "locality" or s.get("type") == "project":
                locality_id = s.get("id")
                break
    except Exception as e:
        log.warning("99ACRES suggest fail | pin=%s | %s", pincode, e)
        return results

    # Step 2: search for project name
    try:
        jitter()
        search_url = "https://www.99acres.com/api/v2/search/property"
        params = {
            "intent":    "BUY",
            "keyword":   project_name,
            "pincode":   pincode,
            "proptype":  "RES",
            "page":      1,
            "pageSize":  20,
            "sortby":    "relevance",
        }
        r = sess.get(search_url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("99ACRES search fail | project=%s | %s", project_name, e)
        return results

    listings = (
        data.get("data", {})
            .get("properties", {})
            .get("items", [])
    )
    if not listings:
        # fallback: try the HTML search page JSON embed
        try:
            jitter()
            fallback_url = (
                f"https://www.99acres.com/search/property/buy/residential"
                f"?keyword={requests.utils.quote(project_name)}"
                f"&pincode={pincode}"
            )
            r2 = sess.get(fallback_url, timeout=20)
            # extract __INITIAL_STATE__ JSON from HTML
            m = re.search(r"window\.__INITIAL_STATE__\s*=\s*({.*?});", r2.text, re.S)
            if m:
                state = json.loads(m.group(1))
                listings = (
                    state.get("searchResult", {})
                         .get("properties", {})
                         .get("items", [])
                ) or []
        except Exception as e2:
            log.warning("99ACRES fallback fail | %s", e2)

    for item in listings:
        try:
            candidate_name = item.get("projectName") or item.get("societyName") or ""
            score = name_match_score(project_name, candidate_name)
            if score < 0.4:
                continue

            price_text  = str(item.get("price") or item.get("priceDisplay") or "")
            area_text   = str(item.get("area") or item.get("areaDisplay") or "")
            config_text = str(item.get("bedroomCount") or item.get("config") or "")
            area_type_raw = str(item.get("areaType") or "").lower()

            area_type = (
                "carpet"        if "carpet" in area_type_raw else
                "built_up"      if "built" in area_type_raw else
                "super_built_up"if "super" in area_type_raw else
                "super_built_up"
            )

            # price range
            price_parts = re.findall(r"[\d.,]+\s*(?:Cr|L|cr|l)?", price_text)
            price_min = price_from_str(price_parts[0]) if len(price_parts) > 0 else None
            price_max = price_from_str(price_parts[1]) if len(price_parts) > 1 else price_min

            # area range
            area_parts = re.findall(r"[\d,]+(?:\.\d+)?", area_text.replace(",", ""))
            area_min = float(area_parts[0]) if len(area_parts) > 0 else None
            area_max = float(area_parts[1]) if len(area_parts) > 1 else area_min

            price_sqft = None
            if price_min and area_min and area_min > 0:
                price_sqft = round(price_min / area_min, 2)

            results.append({
                "portal":         "99acres",
                "matched_name":   candidate_name,
                "match_score":    round(score, 2),
                "url":            f"https://www.99acres.com{item.get('propUrl', '')}",
                "bhk":            bhk_from_str(config_text),
                "area_type":      area_type,
                "area_min":       area_min,
                "area_max":       area_max,
                "price_min":      price_min,
                "price_max":      price_max,
                "price_sqft":     price_sqft,
                "fetched_at":     datetime.now(timezone.utc),
            })
        except Exception as e:
            log.debug("99ACRES parse item error: %s", e)

    log.info("99ACRES | project=%s | found=%d | matched=%d",
             project_name, len(listings), len(results))
    return results

# ─────────────────────────────────────────────
# HOUSING.COM SCRAPER
# ─────────────────────────────────────────────
def fetch_housing(project_name: str, pincode: str) -> list[dict]:
    """
    Uses Housing.com internal search API.
    Endpoint discovered via browser DevTools network tab.
    """
    results = []
    sess = get_session()
    sess.headers.update(HEADERS_HOUSING)

    try:
        jitter()
        # Housing.com search API
        search_url = "https://housing.com/api/v9/listings/search"
        payload = {
            "query":      project_name,
            "pincode":    pincode,
            "category":   "residential",
            "purpose":    "buy",
            "page":       1,
            "per_page":   20,
            "sort_by":    "relevance",
            "city":       "kolkata",
        }
        r = sess.post(search_url, json=payload, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("HOUSING search fail | project=%s | %s", project_name, e)
        # try GET fallback
        try:
            jitter()
            fallback = (
                f"https://housing.com/in/buy/residential/kolkata"
                f"?q={requests.utils.quote(project_name)}&pincode={pincode}"
            )
            r2 = sess.get(fallback, timeout=20)
            # extract JSON from __NEXT_DATA__
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r2.text, re.S)
            if not m:
                return results
            ndata = json.loads(m.group(1))
            listings = (
                ndata.get("props", {})
                     .get("pageProps", {})
                     .get("listings", [])
            ) or []
            data = {"listings": listings}
        except Exception as e2:
            log.warning("HOUSING fallback fail | %s", e2)
            return results

    listings = data.get("listings") or data.get("data", {}).get("listings") or []

    for item in listings:
        try:
            candidate_name = (
                item.get("project_name") or
                item.get("society_name") or
                item.get("name") or ""
            )
            score = name_match_score(project_name, candidate_name)
            if score < 0.4:
                continue

            # price
            price_min = (
                item.get("min_price") or
                item.get("price") or
                price_from_str(str(item.get("price_display") or ""))
            )
            price_max = item.get("max_price") or price_min
            if price_min:
                price_min = float(price_min)
            if price_max:
                price_max = float(price_max)

            # area
            area_min = item.get("min_area") or item.get("area") or sqft_from_str(str(item.get("area_display") or ""))
            area_max = item.get("max_area") or area_min
            if area_min:
                area_min = float(area_min)
            if area_max:
                area_max = float(area_max)

            # area type
            area_type_raw = str(item.get("area_type") or "").lower()
            area_type = (
                "carpet"         if "carpet"  in area_type_raw else
                "built_up"       if "built"   in area_type_raw else
                "super_built_up"
            )

            price_sqft = None
            if price_min and area_min and area_min > 0:
                price_sqft = round(price_min / area_min, 2)

            bhk = (
                item.get("bedrooms") or
                item.get("bhk") or
                bhk_from_str(str(item.get("config") or item.get("bhk_display") or ""))
            )

            slug = item.get("url") or item.get("slug") or ""
            url  = slug if slug.startswith("http") else f"https://housing.com{slug}"

            results.append({
                "portal":        "housing",
                "matched_name":  candidate_name,
                "match_score":   round(score, 2),
                "url":           url,
                "bhk":           int(bhk) if bhk else None,
                "area_type":     area_type,
                "area_min":      area_min,
                "area_max":      area_max,
                "price_min":     price_min,
                "price_max":     price_max,
                "price_sqft":    price_sqft,
                "fetched_at":    datetime.now(timezone.utc),
            })
        except Exception as e:
            log.debug("HOUSING parse item error: %s", e)

    log.info("HOUSING | project=%s | found=%d | matched=%d",
             project_name, len(listings), len(results))

    # restore 99acres headers for next call
    sess.headers.update(HEADERS_99ACRES)
    return results

# ─────────────────────────────────────────────
# PER-PROJECT FETCH
# ─────────────────────────────────────────────
def fetch_prices_for_project(project: dict) -> dict:
    pid          = project["project_id"]
    project_name = project.get("project_name") or ""
    pincode      = project.get("pincode") or ""

    if not project_name or not pincode:
        return {"project_id": pid, "sources": [], "_skip": True}

    log.info("PRICE FETCH | pid=%s | name=%s | pin=%s", pid, project_name, pincode)

    sources = []
    sources += fetch_99acres(project_name, pincode)
    sources += fetch_housing(project_name, pincode)

    # Aggregate by BHK across sources
    aggregated = aggregate_by_bhk(sources)

    return {
        "project_id":      pid,
        "project_name":    project_name,
        "pincode":         pincode,
        "sources":         sources,
        "aggregated":      aggregated,
        "listing_count":   len(sources),
        "last_fetched_at": datetime.now(timezone.utc),
    }

# ─────────────────────────────────────────────
# AGGREGATION HELPER
# ─────────────────────────────────────────────
def aggregate_by_bhk(sources: list[dict]) -> list[dict]:
    """
    Roll up all sources into per-BHK summary:
      { bhk, area_type, area_min, area_max, price_min, price_max,
        avg_price_sqft, portal_count, listing_count }
    """
    from collections import defaultdict
    groups = defaultdict(list)
    for s in sources:
        key = (s.get("bhk"), s.get("area_type"))
        groups[key].append(s)

    result = []
    for (bhk, area_type), items in sorted(groups.items()):
        prices   = [i["price_min"] for i in items if i.get("price_min")]
        areas    = [i["area_min"]  for i in items if i.get("area_min")]
        sqfts    = [i["price_sqft"]for i in items if i.get("price_sqft")]
        portals  = {i["portal"] for i in items}

        result.append({
            "bhk":            bhk,
            "area_type":      area_type,
            "area_min":       min(areas)  if areas  else None,
            "area_max":       max(areas)  if areas  else None,
            "price_min":      min(prices) if prices else None,
            "price_max":      max(prices) if prices else None,
            "avg_price_sqft": round(sum(sqfts)/len(sqfts), 2) if sqfts else None,
            "portals":        list(portals),
            "listing_count":  len(items),
        })
    return result

# ─────────────────────────────────────────────
# UPSERT
# ─────────────────────────────────────────────
def upsert_prices(db, records: list[dict]):
    col = db[COL_PRICES]
    ops = [
        UpdateOne(
            {"project_id": r["project_id"]},
            {"$set": r},
            upsert=True,
        )
        for r in records if not r.get("_skip")
    ]
    if ops:
        result = col.bulk_write(ops, ordered=False)
        log.info("PRICE UPSERT | upserted=%d | modified=%d",
                 result.upserted_count, result.modified_count)

# ─────────────────────────────────────────────
# MAIN RUN
# ─────────────────────────────────────────────
def run_price_scrape():
    log.info("=" * 60)
    log.info("PRICE SCRAPE START | %s", datetime.now().isoformat())
    log.info("=" * 60)

    client, db = get_db()

    try:
        projects = list(
            db[COL_PROJECTS].find(
                {"project_name": {"$exists": True, "$ne": None}},
                {"project_id": 1, "project_name": 1, "pincode": 1},
            )
        )
        log.info("Projects to fetch prices for: %d", len(projects))

        records = []
        done = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(fetch_prices_for_project, p): p for p in projects}
            for fut in as_completed(futures):
                done += 1
                try:
                    rec = fut.result()
                    records.append(rec)
                except Exception as e:
                    log.error("EXCEPTION | pid=%s | %s", futures[fut].get("project_id"), e)

                if done % 100 == 0 or done == len(projects):
                    log.info("PROGRESS | done=%d/%d | records=%d",
                             done, len(projects), len(records))

                    # Flush every 100 to avoid memory buildup
                    upsert_prices(db, records)
                    records = []

        if records:
            upsert_prices(db, records)

        log.info("=" * 60)
        log.info("PRICE SCRAPE END | %s", datetime.now().isoformat())
        log.info("=" * 60)

    finally:
        client.close()

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if "--once" in sys.argv:
        run_price_scrape()
    else:
        scheduler = BlockingScheduler(timezone="Asia/Kolkata")
        scheduler.add_job(
            run_price_scrape,
            trigger="cron",
            hour=SCRAPE_HOUR,
            minute=SCRAPE_MIN,
            id="price_daily",
            max_instances=1,
            misfire_grace_time=3600,
        )
        log.info("PRICE SCHEDULER STARTED | %02d:%02d IST daily", SCRAPE_HOUR, SCRAPE_MIN)
        run_price_scrape()   # run once on startup
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("Scheduler stopped.")