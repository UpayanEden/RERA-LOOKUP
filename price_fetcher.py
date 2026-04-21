"""
WB-RERA Live Price Fetcher
==========================
Fetches live property prices from 99acres and Housing.com
using Playwright (headless Chromium).

Called on-demand when user clicks "Fetch prices" button.
Results cached in MongoDB `prices` collection for 24 hours.

Install:
  pip install playwright
  playwright install chromium --with-deps

Exposed as a FastAPI endpoint — import and call fetch_prices_for_project().
"""

import re
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

log = logging.getLogger("price_fetcher")

CACHE_TTL_HOURS = 24

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def clean_name(name: str) -> str:
    # Remove special chars, phase info, block info
    name = re.sub(r'\(.*?\)', '', name)           # remove anything in brackets
    name = re.sub(r'phase\s*[ivxlcdm\d]+', '', name, flags=re.I)
    name = re.sub(r'block\s*[-–]?\s*[a-z\d]+', '', name, flags=re.I)
    name = re.sub(r'[^a-z0-9\s]', '', name.lower())
    return ' '.join(name.split()).strip()

def name_match_score(query: str, candidate: str) -> float:
    q = set(clean_name(query).split())
    c = set(clean_name(candidate).split())
    # Remove common noise words
    noise = {'the','a','an','and','or','of','in','at','by','for','ltd','pvt','limited','apartment','apartments','residency','enclave','tower','towers','project'}
    q -= noise
    c -= noise
    if not q: return 0.0
    overlap = q & c
    # Score = overlap / query words (how many query words appear in candidate)
    return len(overlap) / len(q)

def parse_price(text: str) -> Optional[float]:
    """Convert '₹45 L', '1.2 Cr', '45,00,000' → float INR."""
    if not text: return None
    t = str(text).replace(",", "").replace("₹", "").strip()
    m_cr = re.search(r"([\d.]+)\s*[Cc]r", t)
    m_l  = re.search(r"([\d.]+)\s*[Ll]", t)
    m_raw= re.search(r"([\d.]+)", t)
    if m_cr: return float(m_cr.group(1)) * 1_00_00_000
    if m_l:  return float(m_l.group(1))  * 1_00_000
    if m_raw:return float(m_raw.group(1))
    return None

def parse_area(text: str) -> Optional[float]:
    """Extract sqft number from '1200 sq.ft', '1,200 - 1,500 sq ft'."""
    if not text: return None
    nums = re.findall(r"[\d]+(?:\.\d+)?", str(text).replace(",", ""))
    return float(nums[0]) if nums else None

def fmt_inr(n: Optional[float]) -> Optional[str]:
    if n is None: return None
    if n >= 1_00_00_000: return f"₹{n/1_00_00_000:.2f} Cr"
    if n >= 1_00_000:    return f"₹{n/1_00_000:.1f} L"
    return f"₹{n:,.0f}"

def area_type_from_text(text: str) -> str:
    t = text.lower()
    if "carpet"  in t: return "carpet"
    if "built up" in t or "builtup" in t: return "built_up"
    return "super_built_up"

# ─────────────────────────────────────────────────────────────────────────────
# 99ACRES SCRAPER
# ─────────────────────────────────────────────────────────────────────────────
async def scrape_99acres(page, project_name: str, pincode: str) -> list[dict]:
    results = []
    url = f"https://www.99acres.com/search/property/buy/residential?keyword={project_name}&pincode={pincode}&intent=BUY"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)  # let JS render

        # Try to extract JSON from __INITIAL_STATE__
        content = await page.content()
        m = re.search(r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});", content, re.S)
        if m:
            try:
                state    = json.loads(m.group(1))
                listings = (
                    state.get("searchResult", {})
                         .get("properties", {})
                         .get("items", [])
                ) or []
                for item in listings[:10]:
                    candidate = item.get("projectName") or item.get("societyName") or ""
                    score     = name_match_score(project_name, candidate)
                    if score < 0.25: continue

                    price_text     = str(item.get("price") or item.get("priceDisplay") or "")
                    area_text      = str(item.get("area")  or item.get("areaDisplay")  or "")
                    area_type_text = str(item.get("areaType") or "super built up")
                    config_text    = str(item.get("bedroomCount") or item.get("config") or "")
                    bhk_m          = re.search(r"(\d)\s*[Bb][Hh][Kk]", config_text)

                    price_parts = re.findall(r"[\d.,]+\s*(?:Cr|L|cr|l)?", price_text)
                    price_min   = parse_price(price_parts[0]) if price_parts else None
                    price_max   = parse_price(price_parts[1]) if len(price_parts) > 1 else price_min
                    area_parts  = re.findall(r"[\d,]+(?:\.\d+)?", area_text.replace(",", ""))
                    area_min    = float(area_parts[0]) if area_parts else None
                    area_max    = float(area_parts[1]) if len(area_parts) > 1 else area_min
                    price_sqft  = round(price_min / area_min, 0) if price_min and area_min else None

                    results.append({
                        "portal":        "99acres",
                        "matched_name":  candidate,
                        "match_score":   round(score, 2),
                        "url":           f"https://www.99acres.com{item.get('propUrl', '')}",
                        "bhk":           int(bhk_m.group(1)) if bhk_m else None,
                        "area_type":     area_type_from_text(area_type_text),
                        "area_type_label": area_type_text.title() or "Super Built Up",
                        "area_min_sqft": area_min,
                        "area_max_sqft": area_max,
                        "price_min":     price_min,
                        "price_max":     price_max,
                        "price_min_fmt": fmt_inr(price_min),
                        "price_max_fmt": fmt_inr(price_max),
                        "price_sqft":    price_sqft,
                        "price_sqft_fmt":f"₹{price_sqft:,.0f}/sqft" if price_sqft else None,
                    })
                log.info("99ACRES | %s | found=%d matched=%d", project_name, len(listings), len(results))
                return results
            except json.JSONDecodeError:
                pass

        # Fallback: scrape DOM cards
        cards = await page.query_selector_all("[data-label='srp-tuple']")
        for card in cards[:10]:
            try:
                name_el  = await card.query_selector("[class*='projectName'], [class*='title']")
                price_el = await card.query_selector("[class*='price'], [class*='Price']")
                area_el  = await card.query_selector("[class*='area'], [class*='Area']")
                bhk_el   = await card.query_selector("[class*='config'], [class*='bhk']")
                link_el  = await card.query_selector("a[href]")

                candidate = await name_el.inner_text()  if name_el  else ""
                score     = name_match_score(project_name, candidate)
                if score < 0.25: continue

                price_text = await price_el.inner_text() if price_el else ""
                area_text  = await area_el.inner_text()  if area_el  else ""
                bhk_text   = await bhk_el.inner_text()   if bhk_el   else ""
                href       = await link_el.get_attribute("href") if link_el else ""
                bhk_m      = re.search(r"(\d)\s*[Bb][Hh][Kk]", bhk_text)

                price_min  = parse_price(price_text)
                area_min   = parse_area(area_text)
                price_sqft = round(price_min / area_min, 0) if price_min and area_min else None

                results.append({
                    "portal":         "99acres",
                    "matched_name":   candidate.strip(),
                    "match_score":    round(score, 2),
                    "url":            href if href.startswith("http") else f"https://www.99acres.com{href}",
                    "bhk":            int(bhk_m.group(1)) if bhk_m else None,
                    "area_type":      "super_built_up",
                    "area_type_label":"Super Built Up",
                    "area_min_sqft":  area_min,
                    "area_max_sqft":  area_min,
                    "price_min":      price_min,
                    "price_max":      price_min,
                    "price_min_fmt":  fmt_inr(price_min),
                    "price_max_fmt":  fmt_inr(price_min),
                    "price_sqft":     price_sqft,
                    "price_sqft_fmt": f"₹{price_sqft:,.0f}/sqft" if price_sqft else None,
                })
            except Exception as e:
                log.debug("99ACRES DOM parse error: %s", e)

    except PWTimeout:
        log.warning("99ACRES timeout | %s", project_name)
    except Exception as e:
        log.warning("99ACRES error | %s | %s", project_name, e)

    log.info("99ACRES DOM | %s | matched=%d", project_name, len(results))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# HOUSING.COM SCRAPER
# ─────────────────────────────────────────────────────────────────────────────
async def scrape_housing(page, project_name: str, pincode: str) -> list[dict]:
    results = []
    url = f"https://housing.com/in/buy/residential/kolkata?q={project_name}&pincode={pincode}"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # Try __NEXT_DATA__ JSON
        content = await page.content()
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', content, re.S)
        if m:
            try:
                ndata    = json.loads(m.group(1))
                listings = (
                    ndata.get("props", {})
                         .get("pageProps", {})
                         .get("listings", [])
                ) or []

                for item in listings[:10]:
                    candidate = (
                        item.get("project_name") or
                        item.get("society_name") or
                        item.get("name") or ""
                    )
                    score = name_match_score(project_name, candidate)
                    if score < 0.25: continue

                    price_min = item.get("min_price") or item.get("price")
                    price_max = item.get("max_price") or price_min
                    area_min  = item.get("min_area")  or item.get("area")
                    area_max  = item.get("max_area")  or area_min
                    area_type_raw = str(item.get("area_type") or "super built up")

                    if price_min: price_min = float(price_min)
                    if price_max: price_max = float(price_max)
                    if area_min:  area_min  = float(area_min)
                    if area_max:  area_max  = float(area_max)

                    price_sqft = round(price_min / area_min, 0) if price_min and area_min else None
                    bhk        = item.get("bedrooms") or item.get("bhk")
                    slug       = item.get("url") or item.get("slug") or ""
                    href       = slug if slug.startswith("http") else f"https://housing.com{slug}"

                    results.append({
                        "portal":         "housing",
                        "matched_name":   candidate,
                        "match_score":    round(score, 2),
                        "url":            href,
                        "bhk":            int(bhk) if bhk else None,
                        "area_type":      area_type_from_text(area_type_raw),
                        "area_type_label":area_type_raw.title() or "Super Built Up",
                        "area_min_sqft":  area_min,
                        "area_max_sqft":  area_max,
                        "price_min":      price_min,
                        "price_max":      price_max,
                        "price_min_fmt":  fmt_inr(price_min),
                        "price_max_fmt":  fmt_inr(price_max),
                        "price_sqft":     price_sqft,
                        "price_sqft_fmt": f"₹{price_sqft:,.0f}/sqft" if price_sqft else None,
                    })
                log.info("HOUSING JSON | %s | found=%d matched=%d", project_name, len(listings), len(results))
                return results
            except json.JSONDecodeError:
                pass

        # Fallback: DOM scrape
        cards = await page.query_selector_all("[class*='listing-card'], [class*='srpCard'], [data-testid='listing']")
        for card in cards[:10]:
            try:
                name_el  = await card.query_selector("[class*='title'], [class*='projectName']")
                price_el = await card.query_selector("[class*='price'], [class*='Price']")
                area_el  = await card.query_selector("[class*='area'], [class*='carpet']")
                link_el  = await card.query_selector("a[href]")

                candidate  = await name_el.inner_text()  if name_el  else ""
                score      = name_match_score(project_name, candidate)
                if score < 0.25: continue

                price_text = await price_el.inner_text() if price_el else ""
                area_text  = await area_el.inner_text()  if area_el  else ""
                href       = await link_el.get_attribute("href") if link_el else ""

                # detect area type from label text
                area_label = area_text.lower()
                area_type  = area_type_from_text(area_label)

                price_min  = parse_price(price_text)
                area_min   = parse_area(area_text)
                price_sqft = round(price_min / area_min, 0) if price_min and area_min else None

                results.append({
                    "portal":         "housing",
                    "matched_name":   candidate.strip(),
                    "match_score":    round(score, 2),
                    "url":            href if href.startswith("http") else f"https://housing.com{href}",
                    "bhk":            None,
                    "area_type":      area_type,
                    "area_type_label":area_text.split("\n")[0].strip() or "Super Built Up",
                    "area_min_sqft":  area_min,
                    "area_max_sqft":  area_min,
                    "price_min":      price_min,
                    "price_max":      price_min,
                    "price_min_fmt":  fmt_inr(price_min),
                    "price_max_fmt":  fmt_inr(price_min),
                    "price_sqft":     price_sqft,
                    "price_sqft_fmt": f"₹{price_sqft:,.0f}/sqft" if price_sqft else None,
                })
            except Exception as e:
                log.debug("HOUSING DOM parse error: %s", e)

    except PWTimeout:
        log.warning("HOUSING timeout | %s", project_name)
    except Exception as e:
        log.warning("HOUSING error | %s | %s", project_name, e)

    log.info("HOUSING DOM | %s | matched=%d", project_name, len(results))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATOR
# ─────────────────────────────────────────────────────────────────────────────
def aggregate_listings(sources: list[dict]) -> list[dict]:
    """Roll up listings by BHK + area_type into summary rows."""
    from collections import defaultdict
    groups = defaultdict(list)
    for s in sources:
        key = (s.get("bhk"), s.get("area_type"))
        groups[key].append(s)

    rows = []
    for (bhk, area_type), items in sorted(groups.items(), key=lambda x: (x[0][0] or 99, x[0][1] or "")):
        prices  = [i["price_min"]    for i in items if i.get("price_min")]
        areas   = [i["area_min_sqft"]for i in items if i.get("area_min_sqft")]
        sqfts   = [i["price_sqft"]   for i in items if i.get("price_sqft")]
        portals = list({i["portal"]  for i in items})
        label   = items[0].get("area_type_label", "Super Built Up")

        rows.append({
            "bhk":              bhk,
            "area_type":        area_type,
            "area_type_label":  label,
            "area_min_sqft":    min(areas)  if areas  else None,
            "area_max_sqft":    max(areas)  if areas  else None,
            "price_min":        min(prices) if prices else None,
            "price_max":        max(prices) if prices else None,
            "price_min_fmt":    fmt_inr(min(prices)) if prices else None,
            "price_max_fmt":    fmt_inr(max(prices)) if prices else None,
            "avg_price_sqft":   round(sum(sqfts)/len(sqfts)) if sqfts else None,
            "avg_price_sqft_fmt": f"₹{round(sum(sqfts)/len(sqfts)):,}/sqft" if sqfts else None,
            "portals":          portals,
            "listing_count":    len(items),
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY — called by FastAPI endpoint
# ─────────────────────────────────────────────────────────────────────────────
async def fetch_live_prices(project_id: str, project_name: str, pincode: str, db) -> dict:
    """
    Fetch live prices for a single project.
    Checks cache first — returns cached result if < 24hrs old.
    Otherwise scrapes 99acres + Housing.com and caches result.
    """
    col = db["prices"]
    now = datetime.now(timezone.utc)

    # ── Check cache ──
    cached = col.find_one({"project_id": project_id})
    if cached:
        cached_time = cached.get("last_fetched_at")
        if cached_time:
            # Make timezone-aware if stored as naive datetime
            if cached_time.tzinfo is None:
                cached_time = cached_time.replace(tzinfo=timezone.utc)
            age = now - cached_time
            if age < timedelta(hours=CACHE_TTL_HOURS):
                log.info("PRICE CACHE HIT | pid=%s | age=%.1fh", project_id, age.total_seconds()/3600)
                cached["_id"] = str(cached["_id"])
                cached["cached"] = True
                cached["cache_age_hours"] = round(age.total_seconds() / 3600, 1)
                return cached
        if age < timedelta(hours=CACHE_TTL_HOURS):
            log.info("PRICE CACHE HIT | pid=%s | age=%.1fh", project_id, age.total_seconds()/3600)
            cached["_id"] = str(cached["_id"])
            cached["cached"] = True
            cached["cache_age_hours"] = round(age.total_seconds() / 3600, 1)
            return cached

    # ── Live fetch ──
    log.info("PRICE LIVE FETCH | pid=%s | name=%s", project_id, project_name)
    sources = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-first-run",
                "--no-zygote",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-IN",
        )
        # Block images/fonts/media to speed up scraping
        await context.route(
            "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,mp4,webm}",
            lambda route: route.abort()
        )

        page = await context.new_page()
        await stealth_async(page)
        try:
            acres   = await scrape_99acres(page, project_name, pincode)
            housing = await scrape_housing(page, project_name, pincode)
            sources = acres + housing
        finally:
            await browser.close()

    aggregated = aggregate_listings(sources)

    record = {
        "project_id":      project_id,
        "project_name":    project_name,
        "pincode":         pincode,
        "sources":         sources,
        "aggregated":      aggregated,
        "listing_count":   len(sources),
        "last_fetched_at": now,
        "cached":          False,
    }

    # ── Save to cache ──
    col.update_one(
        {"project_id": project_id},
        {"$set": record},
        upsert=True,
    )
    log.info("PRICE CACHED | pid=%s | listings=%d", project_id, len(sources))

    record["_id"] = str(col.find_one({"project_id": project_id}, {"_id": 1})["_id"])
    return record