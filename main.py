"""
WB-RERA Dashboard — FastAPI Backend
====================================
Endpoints:
  Auth:
    POST /auth/register        — create account
    POST /auth/login           — get JWT token
    GET  /auth/me              — current user profile

  Projects:
    GET  /projects             — list with filters (pincode, status, district, search, page)
    GET  /projects/meta/filters — distinct filter values for dropdowns
    GET  /projects/{id}        — single project detail

  Favourites:
    GET    /favourites           — list user's favourite pincodes + summary
    POST   /favourites/{pincode} — add pincode
    DELETE /favourites/{pincode} — remove pincode

  Changes:
    GET /changes         — paginated diff log (filter by pincode/project/field)
    GET /changes/summary — 7-day summary by field

  Health:
    GET /health

Setup:
  pip install fastapi uvicorn pymongo python-jose[cryptography] passlib[bcrypt] python-dotenv

  Create a .env file:
    MONGO_URI=mongodb+srv://...
    JWT_SECRET=some-long-random-secret-string
    JWT_EXPIRE_MINUTES=10080

  Run:
    uvicorn main:app --reload --port 8000

  Interactive docs at: http://localhost:8000/docs
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pymongo import MongoClient
from pymongo.collection import Collection
from pydantic import BaseModel, EmailStr, Field
# from price_fetcher import fetch_live_prices
from dotenv import load_dotenv
# At top with other imports
from io import BytesIO
import pandas as pd
from fastapi.responses import StreamingResponse

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MONGO_URI          = os.environ["MONGO_URI"]
MONGO_DB           = "wbrera"
JWT_SECRET         = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM      = "HS256"
JWT_EXPIRE_MINUTES = int(os.environ.get("JWT_EXPIRE_MINUTES", 10080))  # 7 days

# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────
app = FastAPI(title="WB-RERA Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://127.0.0.1:5175",
        "https://rera-dashboard.onrender.com",  # your Render URL
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# MONGO
# ─────────────────────────────────────────────
_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
_db     = _client[MONGO_DB]


def col_projects() -> Collection:
    return _db["projects"]


def col_changes() -> Collection:
    return _db["changes"]


def col_users() -> Collection:
    c = _db["users"]
    c.create_index("email", unique=True)
    return c


# ─────────────────────────────────────────────
# AUTH HELPERS
# ─────────────────────────────────────────────
pwd_ctx      = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def hash_password(password: str) -> str:
    return pwd_ctx.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)


def create_access_token(email: str) -> str:
    payload = {
        "sub": email,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        email: str = payload.get("sub")
        if not email:
            raise exc
    except JWTError:
        raise exc

    user = col_users().find_one({"email": email})
    if not user:
        raise exc
    return user


# ─────────────────────────────────────────────
# SERIALISER
# ─────────────────────────────────────────────
def serialize(doc: dict) -> dict:
    if not doc:
        return {}
    doc["_id"] = str(doc["_id"])
    for k, v in doc.items():
        if isinstance(v, datetime):
            doc[k] = v.isoformat()
    doc.pop("password", None)   # never leak password hash
    return doc


# ─────────────────────────────────────────────
# SCHEMAS
# ─────────────────────────────────────────────
class RegisterRequest(BaseModel):
    name:     str      = Field(min_length=1)
    email:    EmailStr
    password: str      = Field(min_length=6)


class LoginResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    name:         str
    email:        str


class UserProfile(BaseModel):
    name:       str
    email:      str
    favourites: list[str]


class Msg(BaseModel):
    message: str


# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────
@app.post("/auth/register", response_model=LoginResponse, tags=["Auth"])
def register(req: RegisterRequest):
    users = col_users()
    if users.find_one({"email": req.email}):
        raise HTTPException(status_code=400, detail="Email already registered")

    users.insert_one({
        "email":      req.email,
        "name":       req.name,
        "password":   hash_password(req.password),
        "favourites": [],
        "created_at": datetime.now(timezone.utc),
    })
    token = create_access_token(req.email)
    return LoginResponse(access_token=token, name=req.name, email=req.email)


@app.post("/auth/login", response_model=LoginResponse, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends()):
    """Send email as `username` field (OAuth2 standard)."""
    user = col_users().find_one({"email": form.username})
    if not user or not verify_password(form.password, user["password"]):
        raise HTTPException(status_code=401, detail="Incorrect email or password")

    token = create_access_token(user["email"])
    return LoginResponse(access_token=token, name=user["name"], email=user["email"])


@app.get("/auth/me", response_model=UserProfile, tags=["Auth"])
def me(current_user: dict = Depends(get_current_user)):
    return UserProfile(
        name=current_user["name"],
        email=current_user["email"],
        favourites=current_user.get("favourites", []),
    )


# ─────────────────────────────────────────────
# PROJECTS
# ─────────────────────────────────────────────
@app.get("/projects", tags=["Projects"])
def list_projects(
    pincode:  Optional[str] = Query(None),
    status:   Optional[str] = Query(None, description="Partial match on project_status"),
    district: Optional[str] = Query(None, description="Partial match on district"),
    search:   Optional[str] = Query(None, description="Search project name or developer"),
    page:     int           = Query(1, ge=1),
    limit:    int           = Query(20, ge=1, le=100),
):
    """Paginated, filterable project list. All text filters are case-insensitive."""
    query = {}
    if pincode:
        query["pincode"] = pincode.strip()
    if status:
        query["project_status"] = {"$regex": status.strip(), "$options": "i"}
    if district:
        query["district"] = {"$regex": district.strip(), "$options": "i"}
    if search:
        query["$or"] = [
            {"project_name": {"$regex": search.strip(), "$options": "i"}},
            {"developer":    {"$regex": search.strip(), "$options": "i"}},
        ]

    col   = col_projects()
    skip  = (page - 1) * limit
    total = col.count_documents(query)
    docs  = list(col.find(query).skip(skip).limit(limit))

    return {
        "total":   total,
        "page":    page,
        "limit":   limit,
        "pages":   -(-total // limit),
        "results": [serialize(d) for d in docs],
    }
@app.get("/favourites/{pincode}/export", tags=["Favourites"])
def export_pincode_excel(pincode: str, current_user: dict = Depends(get_current_user)):
    """Download all projects under a pincode as Excel."""
    docs = list(col_projects().find({"pincode": pincode}))
    if not docs:
        raise HTTPException(status_code=404, detail="No projects found")

    COLS = [
        "project_name", "developer", "rera_reg_no", "project_type",
        "project_status", "pincode", "district", "police_station", "address",
        "total_apartments", "apartments_booked", "unsold_units", "booking_rate_pct",
        "land_area_sqm", "builtup_area_sqm", "carpet_area_sqm",
        "covered_parking", "basement_parking", "mechanical_parking",
        "completion_date", "extension_completion_date", "quarter_ending",
        "update_date", "construction_status_summary", "details_url",
    ]

    rows = []
    for doc in docs:
        row = {col: doc.get(col) for col in COLS}
        rows.append(row)

    df = pd.DataFrame(rows, columns=COLS)
    df.columns = [c.replace("_", " ").title() for c in df.columns]

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=f"Pincode {pincode}", index=False)
        # Auto-width columns
        ws = writer.sheets[f"Pincode {pincode}"]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=RERA_{pincode}.xlsx"}
    )


@app.get("/projects/meta/filters", tags=["Projects"])
def filter_options():
    col = col_projects()
    return {
        "pincodes":  sorted([p for p in col.distinct("pincode")  if p]),
        "districts": sorted([d for d in col.distinct("district") if d]),
        "statuses":  sorted([s for s in col.distinct("project_status") if s]),
    }


@app.get("/projects/{project_id}", tags=["Projects"])
def get_project(project_id: str):
    doc = col_projects().find_one({"project_id": project_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Project not found")
    return serialize(doc)


# ─────────────────────────────────────────────
# FAVOURITES
# ─────────────────────────────────────────────
@app.get("/favourites", tags=["Favourites"])
def get_favourites(current_user: dict = Depends(get_current_user)):
    """
    Returns each favourite pincode with a quick summary:
    total projects, how many have bookings, and distinct statuses.
    """
    favourites = current_user.get("favourites", [])
    col        = col_projects()
    summary    = []

    for pincode in favourites:
        total  = col.count_documents({"pincode": pincode})
        booked = col.count_documents({"pincode": pincode, "apartments_booked": {"$gt": 0}})
        statuses = col.distinct("project_status", {"pincode": pincode})
        summary.append({
            "pincode":                pincode,
            "total_projects":         total,
            "projects_with_bookings": booked,
            "statuses":               statuses,
        })

    return {"favourites": summary}


@app.post("/favourites/{pincode}", response_model=Msg, tags=["Favourites"])
def add_favourite(pincode: str, current_user: dict = Depends(get_current_user)):
    pincode = pincode.strip()
    if len(pincode) != 6 or not pincode.isdigit():
        raise HTTPException(status_code=400, detail="Pincode must be 6 digits")
    if not col_projects().find_one({"pincode": pincode}):
        raise HTTPException(status_code=404, detail=f"No projects found for pincode {pincode}")

    col_users().update_one(
        {"email": current_user["email"]},
        {"$addToSet": {"favourites": pincode}},
    )
    return {"message": f"Pincode {pincode} added to favourites"}


@app.delete("/favourites/{pincode}", response_model=Msg, tags=["Favourites"])
def remove_favourite(pincode: str, current_user: dict = Depends(get_current_user)):
    col_users().update_one(
        {"email": current_user["email"]},
        {"$pull": {"favourites": pincode}},
    )
    return {"message": f"Pincode {pincode} removed from favourites"}


# ─────────────────────────────────────────────
# CHANGES FEED
# ─────────────────────────────────────────────
@app.get("/changes", tags=["Changes"])
def get_changes(
    pincode:    Optional[str] = Query(None),
    project_id: Optional[str] = Query(None),
    field:      Optional[str] = Query(None, description="Exact field name e.g. apartments_booked"),
    page:       int           = Query(1, ge=1),
    limit:      int           = Query(50, ge=1, le=200),
    current_user: dict        = Depends(get_current_user),
):
    """Paginated change log sorted newest first. Enriched with project name."""
    query = {}
    if project_id:
        query["project_id"] = project_id
    if field:
        query["field"] = field
    if pincode:
        pids = col_projects().distinct("project_id", {"pincode": pincode})
        query["project_id"] = {"$in": pids}

    changes = col_changes()
    skip    = (page - 1) * limit
    total   = changes.count_documents(query)
    docs    = list(changes.find(query).sort("changed_at", -1).skip(skip).limit(limit))

    # Enrich with project name + pincode in one query
    pids_in_page = list({d["project_id"] for d in docs})
    name_map     = {
        p["project_id"]: {"name": p.get("project_name"), "pincode": p.get("pincode")}
        for p in col_projects().find(
            {"project_id": {"$in": pids_in_page}},
            {"project_id": 1, "project_name": 1, "pincode": 1},
        )
    }

    results = []
    for doc in docs:
        d = serialize(doc)
        meta = name_map.get(d["project_id"], {})
        d["project_name"] = meta.get("name")
        d["pincode"]      = meta.get("pincode")
        results.append(d)

    return {
        "total":   total,
        "page":    page,
        "limit":   limit,
        "pages":   -(-total // limit),
        "results": results,
    }


@app.get("/changes/summary", tags=["Changes"])
def changes_summary(current_user: dict = Depends(get_current_user)):
    """Which fields changed most in the last 7 days and how many projects were affected."""
    changes = col_changes()
    since   = datetime.now(timezone.utc) - timedelta(days=7)

    pipeline = [
        {"$match": {"changed_at": {"$gte": since}}},
        {"$group": {
            "_id":               "$field",
            "change_count":      {"$sum": 1},
            "projects_affected": {"$addToSet": "$project_id"},
        }},
        {"$project": {
            "field":             "$_id",
            "change_count":      1,
            "projects_affected": {"$size": "$projects_affected"},
        }},
        {"$sort": {"change_count": -1}},
        {"$limit": 20},
    ]

    by_field = list(changes.aggregate(pipeline))
    for r in by_field:
        r.pop("_id", None)

    return {
        "since":                     since.isoformat(),
        "total_changes":             changes.count_documents({"changed_at": {"$gte": since}}),
        "total_projects_affected":   len(changes.distinct("project_id", {"changed_at": {"$gte": since}})),
        "by_field":                  by_field,
    }


# ─────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────
@app.get("/health", tags=["Health"])
def health():
    try:
        _client.admin.command("ping")
        mongo_ok = "ok"
    except Exception as e:
        mongo_ok = str(e)

    return {
        "status":        "ok",
        "mongo":         mongo_ok,
        "project_count": _db["projects"].estimated_document_count(),
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }

# ─────────────────────────────────────────────────────────────────────────────
# MAP ROUTES — add these to main.py
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/map/projects", tags=["Map"])
def map_projects(
    pincode:  Optional[str] = Query(None),
    district: Optional[str] = Query(None),
    status:   Optional[str] = Query(None),
    lat:      Optional[float] = Query(None, description="Center latitude for radius search"),
    lon:      Optional[float] = Query(None, description="Center longitude for radius search"),
    radius_km: float          = Query(5.0, description="Search radius in km"),
):
    """
    Returns all geocoded projects as GeoJSON FeatureCollection.
    Supports filtering by pincode, district, status, and radius around a point.
    Each feature includes booking_rate_pct for color-coding pins.
    """
    col   = col_projects()
    query = {"lat": {"$exists": True}, "geocode_failed": {"$ne": True}}

    if pincode:
        query["pincode"] = pincode
    if district:
        query["district"] = {"$regex": district, "$options": "i"}
    if status:
        query["project_status"] = {"$regex": status, "$options": "i"}

    # Radius search using MongoDB $nearSphere
    if lat is not None and lon is not None:
        query["location"] = {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [lon, lat]},
                "$maxDistance": int(radius_km * 1000),  # metres
            }
        }

    docs = list(col.find(
        query,
        {
            "project_id": 1, "project_name": 1, "developer": 1,
            "pincode": 1, "district": 1, "project_status": 1,
            "project_type": 1, "total_apartments": 1,
            "apartments_booked": 1, "booking_rate_pct": 1,
            "lat": 1, "lon": 1, "address": 1, "rera_reg_no": 1,
        },
        limit=2000,   # cap to avoid huge payloads
    ))

    features = []
    for doc in docs:
        lat_val = doc.get("lat")
        lon_val = doc.get("lon")
        if lat_val is None or lon_val is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [lon_val, lat_val],
            },
            "properties": {
                "project_id":      doc["project_id"],
                "project_name":    doc.get("project_name"),
                "developer":       doc.get("developer"),
                "pincode":         doc.get("pincode"),
                "district":        doc.get("district"),
                "project_status":  doc.get("project_status"),
                "project_type":    doc.get("project_type"),
                "total_apartments":doc.get("total_apartments"),
                "apartments_booked":doc.get("apartments_booked"),
                "booking_rate_pct":doc.get("booking_rate_pct"),
                "address":         doc.get("address"),
                "rera_reg_no":     doc.get("rera_reg_no"),
            },
        })

    return {
        "type":     "FeatureCollection",
        "count":    len(features),
        "features": features,
    }


@app.get("/map/geocode-status", tags=["Map"])
def geocode_status():
    """How many projects have been geocoded."""
    col   = col_projects()
    total = col.count_documents({})
    geocoded = col.count_documents({"lat": {"$exists": True}, "geocode_failed": {"$ne": True}})
    failed   = col.count_documents({"geocode_failed": True})
    pending  = total - geocoded - failed
    return {
        "total":    total,
        "geocoded": geocoded,
        "failed":   failed,
        "pending":  pending,
        "pct_done": round(geocoded / total * 100, 1) if total else 0,
    }

# ─────────────────────────────────────────────────────────────────────────────
# PROJECT FAVOURITES — add these to main.py alongside the pincode favourites
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/favourites/projects", tags=["Favourites"])
def get_project_favourites(current_user: dict = Depends(get_current_user)):
    """Returns the user's favourite projects with full detail."""
    fav_ids = current_user.get("fav_projects", [])
    if not fav_ids:
        return {"favourites": []}

    docs = list(col_projects().find(
        {"project_id": {"$in": fav_ids}},
        {
            "project_id": 1, "project_name": 1, "developer": 1,
            "pincode": 1, "district": 1, "project_status": 1,
            "total_apartments": 1, "apartments_booked": 1,
            "booking_rate_pct": 1, "completion_date": 1,
            "rera_reg_no": 1, "project_type": 1,
        }
    ))
    return {"favourites": [serialize(d) for d in docs]}


@app.post("/favourites/projects/{project_id}", response_model=Msg, tags=["Favourites"])
def add_project_favourite(project_id: str, current_user: dict = Depends(get_current_user)):
    if not col_projects().find_one({"project_id": project_id}):
        raise HTTPException(status_code=404, detail="Project not found")
    col_users().update_one(
        {"email": current_user["email"]},
        {"$addToSet": {"fav_projects": project_id}},
    )
    return {"message": f"Project {project_id} added to favourites"}


@app.delete("/favourites/projects/{project_id}", response_model=Msg, tags=["Favourites"])
def remove_project_favourite(project_id: str, current_user: dict = Depends(get_current_user)):
    col_users().update_one(
        {"email": current_user["email"]},
        {"$pull": {"fav_projects": project_id}},
    )
    return {"message": f"Project {project_id} removed from favourites"}

# ─────────────────────────────────────────────────────────────────────────────
# LIVE PRICE ENDPOINT — add to main.py
# ─────────────────────────────────────────────────────────────────────────────
# At top of main.py add:
#   # from price_fetcher import fetch_live_prices
#   import asyncio

@app.get("/prices/{project_id}", tags=["Prices"])
async def get_prices(project_id: str):
    """
    Fetch live prices for a project from 99acres and Housing.com.
    Returns cached result if fetched within last 24 hours.
    First call takes 10-20 seconds (live scrape).
    Subsequent calls within 24hrs are instant (cache).
    """
    doc = col_projects().find_one(
        {"project_id": project_id},
        {"project_name": 1, "pincode": 1}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Project not found")

    project_name = doc.get("project_name") or ""
    pincode      = doc.get("pincode") or ""

    if not project_name or not pincode:
        raise HTTPException(status_code=422, detail="Project has no name or pincode")

    try:
        result = await fetch_live_prices(
            project_id   = project_id,
            project_name = project_name,
            pincode      = pincode,
            db           = _db,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Price fetch failed: {str(e)}")


@app.get("/prices/pincode/{pincode}", tags=["Prices"])
def get_prices_by_pincode(
    pincode: str,
    bhk: Optional[int] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    """Returns cached price data for all projects in a pincode."""
    docs = list(_db["prices"].find({"pincode": pincode}))
    results = []
    for doc in docs:
        agg = doc.get("aggregated") or []
        if bhk:
            agg = [a for a in agg if a.get("bhk") == bhk]
        if agg:
            results.append({
                "project_id":    doc["project_id"],
                "project_name":  doc.get("project_name"),
                "listing_count": doc.get("listing_count", 0),
                "last_fetched":  doc.get("last_fetched_at").isoformat() if doc.get("last_fetched_at") else None,
                "aggregated":    agg,
            })
    return {"pincode": pincode, "project_count": len(results), "results": results}


@app.get("/projects/{project_id}/booking-history", tags=["Projects"])
def booking_history(project_id: str):
    """Returns booking timeline from changes collection."""
    changes = list(
        _db["changes"].find(
            {"project_id": project_id, "field": "apartments_booked"},
            {"old_value": 1, "new_value": 1, "changed_at": 1}
        ).sort("changed_at", 1)
    )
    # Build timeline — start from first known value
    timeline = []
    for c in changes:
        changed_at = c["changed_at"]
        if changed_at.tzinfo is None:
            changed_at = changed_at.replace(tzinfo=__import__('datetime').timezone.utc)
        timeline.append({
            "date":      changed_at.isoformat(),
            "old_value": c.get("old_value"),
            "new_value": c.get("new_value"),
        })
    return {"project_id": project_id, "timeline": timeline}

@app.get("/projects/{project_id}/booking-history", tags=["Projects"])
def booking_history(project_id: str):
    changes = list(
        _db["changes"].find(
            {"project_id": project_id, "field": "apartments_booked"},
            {"old_value": 1, "new_value": 1, "changed_at": 1}
        ).sort("changed_at", 1)
    )
    from datetime import timezone as tz
    timeline = []
    for c in changes:
        changed_at = c["changed_at"]
        if changed_at.tzinfo is None:
            changed_at = changed_at.replace(tzinfo=tz.utc)
        timeline.append({
            "date":      changed_at.isoformat(),
            "old_value": c.get("old_value"),
            "new_value": c.get("new_value"),
        })
    return {"project_id": project_id, "timeline": timeline}