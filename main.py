"""
WB-RERA Dashboard — FastAPI Backend
====================================
Endpoints:
  Auth:       POST /auth/register, POST /auth/login, GET /auth/me
  Projects:   GET /projects, GET /projects/meta/filters, GET /projects/{id}
              GET /projects/{id}/booking-history
  Favourites: GET/POST/DELETE /favourites/{pincode}
              GET /favourites/{pincode}/export
              GET/POST/DELETE /favourites/projects/{project_id}
  Changes:    GET /changes, GET /changes/summary
  Map:        GET /map/projects, GET /map/projects/bounds, GET /map/geocode-status
  Health:     GET /health
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from io import BytesIO

from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pymongo import MongoClient
from pymongo.collection import Collection
from pydantic import BaseModel, EmailStr, Field
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
MONGO_URI          = os.environ["MONGO_URI"]
MONGO_DB           = "wbrera"
JWT_SECRET         = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM      = "HS256"
JWT_EXPIRE_MINUTES = int(os.environ.get("JWT_EXPIRE_MINUTES", 10080))

# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────
app = FastAPI(title="WB-RERA Dashboard API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://127.0.0.1:5175",
        "https://rera-dashboard.onrender.com",
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

def col_prices() -> Collection:
    return _db["prices"]


# ─────────────────────────────────────────────
# AUTH HELPERS
# ─────────────────────────────────────────────
pwd_ctx       = CryptContext(schemes=["bcrypt"], deprecated="auto")
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
    doc.pop("password", None)
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
        "fav_projects": [],
        "created_at": datetime.now(timezone.utc),
    })
    token = create_access_token(req.email)
    return LoginResponse(access_token=token, name=req.name, email=req.email)


@app.post("/auth/login", response_model=LoginResponse, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends()):
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
    status:   Optional[str] = Query(None),
    district: Optional[str] = Query(None),
    search:   Optional[str] = Query(None),
    page:     int           = Query(1, ge=1),
    limit:    int           = Query(20, ge=1, le=100),
):
    query = {}
    if pincode:  query["pincode"]         = pincode.strip()
    if status:   query["project_status"]  = {"$regex": status.strip(),   "$options": "i"}
    if district: query["district"]        = {"$regex": district.strip(), "$options": "i"}
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


@app.get("/projects/meta/filters", tags=["Projects"])
def filter_options():
    col = col_projects()
    return {
        "pincodes":  sorted([p for p in col.distinct("pincode")  if p]),
        "districts": sorted([d for d in col.distinct("district") if d]),
        "statuses":  sorted([s for s in col.distinct("project_status") if s]),
    }


@app.get("/projects/{project_id}/booking-history", tags=["Projects"])
def booking_history(project_id: str):
    """Returns booking timeline from changes collection."""
    changes = list(
        _db["changes"].find(
            {"project_id": project_id, "field": "apartments_booked"},
            {"old_value": 1, "new_value": 1, "changed_at": 1}
        ).sort("changed_at", 1)
    )
    timeline = []
    for c in changes:
        changed_at = c["changed_at"]
        if changed_at.tzinfo is None:
            changed_at = changed_at.replace(tzinfo=timezone.utc)
        timeline.append({
            "date":      changed_at.isoformat(),
            "old_value": c.get("old_value"),
            "new_value": c.get("new_value"),
        })
    return {"project_id": project_id, "timeline": timeline}


@app.get("/projects/{project_id}", tags=["Projects"])
def get_project(project_id: str):
    doc = col_projects().find_one({"project_id": project_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Project not found")
    return serialize(doc)


# ─────────────────────────────────────────────
# FAVOURITES — PINCODES
# ─────────────────────────────────────────────
@app.get("/favourites", tags=["Favourites"])
def get_favourites(current_user: dict = Depends(get_current_user)):
    favourites = current_user.get("favourites", [])
    col        = col_projects()
    summary    = []
    for pincode in favourites:
        total    = col.count_documents({"pincode": pincode})
        booked   = col.count_documents({"pincode": pincode, "apartments_booked": {"$gt": 0}})
        statuses = col.distinct("project_status", {"pincode": pincode})
        summary.append({
            "pincode":                pincode,
            "total_projects":         total,
            "projects_with_bookings": booked,
            "statuses":               statuses,
        })
    return {"favourites": summary}


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

    rows = [{col: doc.get(col) for col in COLS} for doc in docs]
    df   = pd.DataFrame(rows, columns=COLS)
    df.columns = [c.replace("_", " ").title() for c in df.columns]

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=f"Pincode {pincode}", index=False)
        ws = writer.sheets[f"Pincode {pincode}"]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=RERA_{pincode}.xlsx"},
    )


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
# FAVOURITES — PROJECTS
# ─────────────────────────────────────────────
@app.get("/favourites/projects", tags=["Favourites"])
def get_project_favourites(current_user: dict = Depends(get_current_user)):
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


# ─────────────────────────────────────────────
# CHANGES FEED
# ─────────────────────────────────────────────
@app.get("/changes", tags=["Changes"])
def get_changes(
    pincode:    Optional[str] = Query(None),
    project_id: Optional[str] = Query(None),
    field:      Optional[str] = Query(None),
    page:       int           = Query(1, ge=1),
    limit:      int           = Query(50, ge=1, le=200),
    current_user: dict        = Depends(get_current_user),
):
    query = {}
    if project_id: query["project_id"] = project_id
    if field:      query["field"]       = field
    if pincode:
        pids = col_projects().distinct("project_id", {"pincode": pincode})
        query["project_id"] = {"$in": pids}

    changes = col_changes()
    skip    = (page - 1) * limit
    total   = changes.count_documents(query)
    docs    = list(changes.find(query).sort("changed_at", -1).skip(skip).limit(limit))

    pids_in_page = list({d["project_id"] for d in docs})
    name_map = {
        p["project_id"]: {"name": p.get("project_name"), "pincode": p.get("pincode")}
        for p in col_projects().find(
            {"project_id": {"$in": pids_in_page}},
            {"project_id": 1, "project_name": 1, "pincode": 1},
        )
    }

    results = []
    for doc in docs:
        d    = serialize(doc)
        meta = name_map.get(d["project_id"], {})
        d["project_name"] = meta.get("name")
        d["pincode"]      = meta.get("pincode")
        results.append(d)

    return {"total": total, "page": page, "limit": limit, "pages": -(-total // limit), "results": results}


@app.get("/changes/summary", tags=["Changes"])
def changes_summary(current_user: dict = Depends(get_current_user)):
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
        "since":                   since.isoformat(),
        "total_changes":           changes.count_documents({"changed_at": {"$gte": since}}),
        "total_projects_affected": len(changes.distinct("project_id", {"changed_at": {"$gte": since}})),
        "by_field":                by_field,
    }


# ─────────────────────────────────────────────
# MAP
# ─────────────────────────────────────────────
MAP_FIELDS = {
    "project_id": 1, "project_name": 1, "developer": 1, "pincode": 1,
    "district": 1, "project_status": 1, "project_type": 1,
    "total_apartments": 1, "apartments_booked": 1, "booking_rate_pct": 1,
    "lat": 1, "lon": 1,
}


def _to_geojson(docs):
    features = []
    for doc in docs:
        lat = doc.get("lat")
        lon = doc.get("lon")
        if lat is None or lon is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {k: doc.get(k) for k in MAP_FIELDS if k not in ("lat", "lon")},
        })
    return features


@app.get("/map/projects/bounds", tags=["Map"])
def map_projects_bounds(
    north:  float          = Query(...),
    south:  float          = Query(...),
    east:   float          = Query(...),
    west:   float          = Query(...),
    status: Optional[str]  = Query(None),
    search: Optional[str]  = Query(None),
    zoom:   int            = Query(10),
):
    """
    Returns geocoded projects within the current map viewport.
    Called on every pan/zoom — no limit needed since only visible area is queried.
    """
    query = {
        "lat": {"$exists": True, "$gte": south, "$lte": north},
        "lon": {"$exists": True, "$gte": west,  "$lte": east},
        "geocode_failed": {"$ne": True},
    }
    if status: query["project_status"] = {"$regex": status, "$options": "i"}
    if search:
        query["$or"] = [
            {"project_name": {"$regex": search, "$options": "i"}},
            {"developer":    {"$regex": search, "$options": "i"}},
        ]

    docs     = list(col_projects().find(query, MAP_FIELDS))
    features = _to_geojson(docs)
    return {"type": "FeatureCollection", "count": len(features), "features": features}


@app.get("/map/projects", tags=["Map"])
def map_projects(
    pincode:   Optional[str]   = Query(None),
    district:  Optional[str]   = Query(None),
    status:    Optional[str]   = Query(None),
    search:    Optional[str]   = Query(None),
    lat:       Optional[float] = Query(None),
    lon:       Optional[float] = Query(None),
    radius_km: float           = Query(5.0),
):
    """Used for radius/pincode/search queries (circle draw, pincode filter)."""
    query = {"lat": {"$exists": True}, "geocode_failed": {"$ne": True}}
    if pincode:  query["pincode"]         = pincode
    if district: query["district"]        = {"$regex": district, "$options": "i"}
    if status:   query["project_status"]  = {"$regex": status,   "$options": "i"}
    if search:
        query["$or"] = [
            {"project_name": {"$regex": search, "$options": "i"}},
            {"developer":    {"$regex": search, "$options": "i"}},
        ]
    if lat is not None and lon is not None:
        query["location"] = {
            "$nearSphere": {
                "$geometry":    {"type": "Point", "coordinates": [lon, lat]},
                "$maxDistance": int(radius_km * 1000),
            }
        }

    docs     = list(col_projects().find(query, MAP_FIELDS))
    features = _to_geojson(docs)
    return {"type": "FeatureCollection", "count": len(features), "features": features}


@app.get("/map/geocode-status", tags=["Map"])
def geocode_status():
    col      = col_projects()
    total    = col.count_documents({})
    geocoded = col.count_documents({"lat": {"$exists": True}, "geocode_failed": {"$ne": True}})
    failed   = col.count_documents({"geocode_failed": True})
    return {
        "total":    total,
        "geocoded": geocoded,
        "failed":   failed,
        "pending":  total - geocoded - failed,
        "pct_done": round(geocoded / total * 100, 1) if total else 0,
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