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
from dotenv import load_dotenv

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
    allow_origins=["https://rera-dashboard.onrender.com"],   # tighten to your Render frontend URL in production
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


@app.get("/projects/meta/filters", tags=["Projects"])
def filter_options():
    """Distinct values for pincode, district, and status — use to populate dropdowns."""
    col = col_projects()
    return {
        "pincodes":  sorted(col.distinct("pincode")),
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