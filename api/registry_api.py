# api/registry_api.py
# -*- coding: utf-8 -*-
"""
Registry/GroupManager API (SQLite, SQLAlchemy, FastAPI)
- Kanonische Assets (1 Eintrag je wirtschaftliches Objekt)
- Listings (mehrere Börsen/Quellen/Ticker pro Asset)
- Tags (n:m)
- Sectors (n:m)
- Custom Gruppen (Members referenzieren asset_id, optional source/mic/exchange, group_tag)
- Profiles (JSON) für Group-Manager/Notifier
- Resolver: asset_id + source/(exchange|mic) -> Symbol
- Suche: einfache Filter + LIKE-Suche (Assets/Listings/Identifiers/Tags)
- Meta: /meta/types|categories|tags|sectors|sources (distinct, filterbar)
- Versionierung: Assets/Groups/Profiles haben opaque Version-Strings (ETag-kompatibel)
- Bulk-Write für Gruppen inkl. Versionscheck

Start:
    uvicorn registry_api:app --reload --port 8098

ENV:
    REGISTRY_DB_URL=sqlite:///./registry.db   (default)
    LOG_LEVEL=DEBUG|INFO                       (default: INFO)
    REGISTRY_API_TOKEN=...                     (optional; schützt schreibende Calls)
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI, Depends, HTTPException, Query, Body, Path as FPath, Header, Response

from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator

from sqlalchemy import (
    create_engine, Column, String, Integer, Text, DateTime, ForeignKey,
    UniqueConstraint, Index, event, or_, and_, distinct, func
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import JSON

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s registry: %(message)s",
)
log = logging.getLogger("registry")

# ──────────────────────────────────────────────────────────────────────────────
# DB Setup (SQLite WAL) – Pfad aus config.REGISTRY_DB (oder ENV)
# ──────────────────────────────────────────────────────────────────────────────
from pathlib import Path
import config as cfg

Path(cfg.REGISTRY_MANAGER_DB).parent.mkdir(parents=True, exist_ok=True)

DB_URL = os.getenv("REGISTRY_DB_URL", f"sqlite:///{cfg.REGISTRY_MANAGER_DB}")
IS_SQLITE = DB_URL.startswith("sqlite")

engine = create_engine(
    DB_URL,
    connect_args={"check_same_thread": False} if IS_SQLITE else {},
    json_serializer=None, json_deserializer=None,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def _now() -> datetime:
    return datetime.utcnow()

def _new_ver() -> str:
    # Opaque, schwach ETag-kompatibel (Weak ETag Syntax W/"...")
    # Zeit + Kurzrandom für schnelle Unterscheidung, kein kryptografischer Anspruch
    return f'W/"{int(datetime.utcnow().timestamp())}-{uuid4().hex[:8]}"'

@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if not IS_SQLITE:
        return
    cur = dbapi_connection.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.close()
    log.debug("[DB] SQLite pragmas set (WAL, synchronous=NORMAL, foreign_keys=ON)")

# ──────────────────────────────────────────────────────────────────────────────
# Auth (optional via REGISTRY_API_TOKEN)
# ──────────────────────────────────────────────────────────────────────────────
API_TOKEN = os.getenv("REGISTRY_API_TOKEN", "").strip()
AUTH_ENABLED = bool(API_TOKEN)

def require_auth(authorization: Optional[str] = Header(None)):
    if not AUTH_ENABLED:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

if AUTH_ENABLED:
    log.info("[AUTH] REGISTRY_API_TOKEN present → write operations require Bearer token")
else:
    log.warning("[AUTH] No REGISTRY_API_TOKEN set → ALL endpoints are open (DEV mode)")

# ──────────────────────────────────────────────────────────────────────────────
# ORM Modelle
# ──────────────────────────────────────────────────────────────────────────────

class AssetType(str, Enum):
    equity = "equity"
    crypto = "crypto"
    commodity = "commodity"
    index = "index"
    forex = "forex"
    etf = "etf"
    bond = "bond"
    other = "other"
    unknown = "unknown"

class AssetStatus(str, Enum):
    active = "active"
    unsorted = "unsorted"
    inactive = "inactive"

class Asset(Base):
    __tablename__ = "assets"
    id = Column(String, primary_key=True)  # "asset:msft" slug oder uuid
    type = Column(String, nullable=False)
    name = Column(String, nullable=True)
    country = Column(String, nullable=True)
    sector = Column(String, nullable=True)  # legacy single sector (bleibt)
    primary_category = Column(String, nullable=False)
    status = Column(String, nullable=False, default="active")
    version = Column(String, nullable=False, default=_new_ver)  # NEU
    created_ts = Column(DateTime, default=_now, nullable=False)
    updated_ts = Column(DateTime, default=_now, onupdate=_now, nullable=False)

    listings = relationship("Listing", back_populates="asset", cascade="all, delete-orphan")
    tags = relationship("AssetTag", back_populates="asset", cascade="all, delete-orphan")
    identifiers = relationship("Identifier", back_populates="asset", cascade="all, delete-orphan")
    sectors = relationship("AssetSector", back_populates="asset", cascade="all, delete-orphan")  # n:m

class Listing(Base):
    __tablename__ = "listings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    source = Column(String, nullable=False)
    exchange = Column(String, nullable=True)
    mic = Column(String, nullable=True)
    symbol = Column(String, nullable=False)
    isin = Column(String, nullable=True)
    cusip = Column(String, nullable=True)
    figi = Column(String, nullable=True)
    note = Column(Text, nullable=True)

    asset = relationship("Asset", back_populates="listings")

    __table_args__ = (
        UniqueConstraint("source", "symbol", "mic", name="uq_listings_source_symbol_mic"),
        Index("idx_listings_symbol", "symbol"),
        Index("idx_listings_source", "source"),
        Index("idx_listings_mic", "mic"),
        Index("idx_listings_exchange", "exchange"),
    )

class Tag(Base):
    __tablename__ = "tags"
    tag = Column(String, primary_key=True)

class AssetTag(Base):
    __tablename__ = "asset_tags"
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True)
    tag = Column(String, ForeignKey("tags.tag", ondelete="CASCADE"), primary_key=True)
    asset = relationship("Asset", back_populates="tags")

# ── Sectors (n:m) ────────────────────────────────────────────────────────────
class Sector(Base):
    __tablename__ = "sectors"
    sector = Column(String, primary_key=True)  # z.B. "Tech", "Energy", "Defense"

class AssetSector(Base):
    __tablename__ = "asset_sectors"
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True)
    sector = Column(String, ForeignKey("sectors.sector", ondelete="CASCADE"), primary_key=True)
    asset = relationship("Asset", back_populates="sectors")

class Identifier(Base):
    __tablename__ = "identifiers"
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True)
    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)
    asset = relationship("Asset", back_populates="identifiers")

    __table_args__ = (
        UniqueConstraint("asset_id", "key", name="uq_identifiers_asset_key"),
        Index("idx_identifiers_value", "value"),
    )

class Group(Base):
    __tablename__ = "groups"
    id = Column(String, primary_key=True)  # "group:my_watchlist" oder UUID/slug
    name = Column(String, nullable=False)
    default_source = Column(String, nullable=True)
    version = Column(String, nullable=False, default=_new_ver)  # NEU
    created_ts = Column(DateTime, default=_now, nullable=False)
    updated_ts = Column(DateTime, default=_now, onupdate=_now, nullable=False)
    members = relationship("GroupMember", back_populates="group", cascade="all, delete-orphan")

class GroupMember(Base):
    __tablename__ = "group_members"
    group_id = Column(String, ForeignKey("groups.id", ondelete="CASCADE"), primary_key=True)
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True)
    source = Column(String, nullable=True)
    exchange = Column(String, nullable=True)
    mic = Column(String, nullable=True)
    position = Column(Integer, nullable=True)
    group_tag = Column(String, nullable=True)  # NULL = untagged
    group = relationship("Group", back_populates="members")
    asset = relationship("Asset")

# ── Profiles (JSON) ──────────────────────────────────────────────────────────
class Profile(Base):
    __tablename__ = "profiles"
    id = Column(String, primary_key=True)               # "profile:<uuid>" oder slug
    name = Column(String, nullable=False)
    payload = Column(JSON, nullable=False, default={})  # freies JSON für UI/Notifier
    version = Column(String, nullable=False, default=_new_ver)  # NEU
    created_ts = Column(DateTime, default=_now, nullable=False)
    updated_ts = Column(DateTime, default=_now, onupdate=_now, nullable=False)

# Zusätzliche Indizes
Index("idx_assets_primary_category", Asset.primary_category)
Index("idx_assets_status", Asset.status)
Index("idx_assets_type", Asset.type)
Index("idx_asset_name", Asset.name)
Index("idx_sector_sector", Sector.sector)
Index("idx_asset_sector_sector", AssetSector.sector)
Index("idx_asset_tag_tag", AssetTag.tag)


# ──────────────────────────────────────────────────────────────────────────────
# Pydantic Schemas
# ──────────────────────────────────────────────────────────────────────────────
class ListingIn(BaseModel):
    source: str = Field(..., description="EODHD|BINANCE|YF|...")
    symbol: str
    exchange: Optional[str] = None
    mic: Optional[str] = None
    isin: Optional[str] = None
    cusip: Optional[str] = None
    figi: Optional[str] = None
    note: Optional[str] = None

class ListingOut(ListingIn):
    id: int

class ListingPatch(BaseModel):
    source: Optional[str] = None
    symbol: Optional[str] = None
    exchange: Optional[str] = None
    mic: Optional[str] = None
    isin: Optional[str] = None
    cusip: Optional[str] = None
    figi: Optional[str] = None
    note: Optional[str] = None

    @validator("*", pre=True)
    def _empty_to_none(cls, v):
        return v if v != "" else None

class IdentifierIn(BaseModel):
    key: str
    value: str

class IdentifierOut(IdentifierIn):
    pass

class AssetIn(BaseModel):
    id: str = Field(..., description='z. B. "asset:msft" (slug) oder UUID')
    type: AssetType
    name: Optional[str] = None
    country: Optional[str] = None
    sector: Optional[str] = None                    # legacy Einzelwert (optional)
    primary_category: str
    status: AssetStatus = AssetStatus.active
    listings: List[ListingIn] = []
    tags: List[str] = []
    identifiers: List[IdentifierIn] = []
    sectors: List[str] = []                         # n:m Liste

class AssetOut(BaseModel):
    id: str
    type: AssetType
    name: Optional[str]
    country: Optional[str]
    sector: Optional[str]              # legacy
    sectors: List[str]                 # n:m
    primary_category: str
    status: AssetStatus
    version: str
    created_ts: datetime
    updated_ts: datetime
    listings: List[ListingOut]
    tags: List[str]
    identifiers: Dict[str, str]

class AssetPatch(BaseModel):
    type: Optional[AssetType] = None
    name: Optional[str] = None
    country: Optional[str] = None
    sector: Optional[str] = None
    primary_category: Optional[str] = None
    status: Optional[AssetStatus] = None

class GroupIn(BaseModel):
    id: str
    name: str
    default_source: Optional[str] = None

class GroupOut(BaseModel):
    id: str
    name: str
    default_source: Optional[str]
    version: str
    created_ts: datetime
    updated_ts: datetime
    members: List[Dict[str, Any]]
    group_tags: List[GroupTagOut] = [] 


class GroupTagOut(BaseModel):
    id: str
    name: Optional[str] = None
    position: int


class GroupMemberIn(BaseModel):
    asset_id: str
    source: Optional[str] = None
    exchange: Optional[str] = None
    mic: Optional[str] = None
    position: Optional[int] = None
    group_tag: Optional[str] = None

class GroupMemberPatch(BaseModel):
    source: Optional[str] = None
    exchange: Optional[str] = None
    mic: Optional[str] = None
    position: Optional[int] = None
    group_tag: Optional[str] = None

class ReorderItem(BaseModel):
    asset_id: str
    position: Optional[int] = None

class ReorderPayload(BaseModel):
    items: List[ReorderItem]

class AssetTagsPut(BaseModel):
    tags: List[str] = Field(default_factory=list)
    version: Optional[str] = None


class TagDeleteReport(BaseModel):
    removed_from_assets: int        # Anzahl Assets, aus denen der Tag entfernt wurde (distinct)
    deleted_links: int              # Anzahl gelöschter AssetTag-Zeilen
    deleted_catalog_entry: bool     # Katalogeintrag aus 'tags' gelöscht?
    bumped_assets: int              # Anzahl Assets, deren Version erhöht wurde

# Bulk
class BulkAddItem(GroupMemberIn):
    pass

class BulkUpdateItem(GroupMemberPatch):
    asset_id: str

class BulkReorderItem(ReorderItem):
    pass

class GroupBulkRequest(BaseModel):
    version: Optional[str] = None
    adds: List[BulkAddItem] = []
    removes: List[str] = []  # asset_ids
    updates: List[BulkUpdateItem] = []
    reorder: List[BulkReorderItem] = []

class GroupBulkResponse(BaseModel):
    version: str
    applied: Dict[str, int]

# Profiles
class ProfileIn(BaseModel):
    id: Optional[str] = Field(None, description='optional; wenn leer → "profile:<uuid>"')
    name: str
    payload: Dict[str, Any] = Field(default_factory=dict)

class ProfileOut(BaseModel):
    id: str
    name: str
    payload: Dict[str, Any]
    version: str
    created_ts: datetime
    updated_ts: datetime

class ProfilePatch(BaseModel):
    name: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    version: Optional[str] = None  # optionaler Versionscheck

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Registry/GroupManager API", version="1.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# DB Dependency
def get_db():
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        log.error(f"[DB][ROLLBACK] {e}")
        raise
    finally:
        db.close()

# Create tables at startup
Base.metadata.create_all(bind=engine)
log.info("[BOOT] Tables ensured.")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _asset_to_out(a: Asset) -> AssetOut:
    return AssetOut(
        id=a.id,
        type=AssetType(a.type) if a.type in AssetType._value2member_map_ else AssetType.unknown,
        name=a.name,
        country=a.country,
        sector=a.sector,
        sectors=[s.sector for s in a.sectors],
        primary_category=a.primary_category,
        status=AssetStatus(a.status) if a.status in AssetStatus._value2member_map_ else AssetStatus.active,
        version=a.version,
        created_ts=a.created_ts,
        updated_ts=a.updated_ts,
        listings=[ListingOut(
            id=l.id, source=l.source, symbol=l.symbol, exchange=l.exchange, mic=l.mic,
            isin=l.isin, cusip=l.cusip, figi=l.figi, note=l.note
        ) for l in a.listings],
        tags=[t.tag for t in a.tags],
        identifiers={i.key: i.value for i in a.identifiers},
    )

def _norm(s: Optional[str]) -> Optional[str]:
    return s.strip().upper() if isinstance(s, str) else s

def resolve_symbol(db: Session, asset_id: str, source: Optional[str], exchange: Optional[str], mic: Optional[str]) -> Dict[str, Any]:
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_id}")

    src = _norm(source); ex = _norm(exchange); mi = _norm(mic)
    candidates: List[Listing] = a.listings

    if src and mi:
        for l in candidates:
            if _norm(l.source) == src and _norm(l.mic) == mi:
                log.debug(f"[RESOLVE] {asset_id} -> source+mic match: {l.symbol}")
                return {"asset_id": asset_id, "source": l.source, "exchange": l.exchange, "mic": l.mic, "symbol": l.symbol}
    if src and ex:
        for l in candidates:
            if _norm(l.source) == src and _norm(l.exchange) == ex:
                log.debug(f"[RESOLVE] {asset_id} -> source+exchange match: {l.symbol}")
                return {"asset_id": asset_id, "source": l.source, "exchange": l.exchange, "mic": l.mic, "symbol": l.symbol}
    if src:
        for l in candidates:
            if _norm(l.source) == src:
                log.debug(f"[RESOLVE] {asset_id} -> source-only match: {l.symbol}")
                return {"asset_id": asset_id, "source": l.source, "exchange": l.exchange, "mic": l.mic, "symbol": l.symbol}
    if candidates:
        l = candidates[0]
        log.debug(f"[RESOLVE] {asset_id} -> default first listing: {l.symbol}")
        return {"asset_id": asset_id, "source": l.source, "exchange": l.exchange, "mic": l.mic, "symbol": l.symbol}

    raise HTTPException(status_code=404, detail=f"No listings for asset: {asset_id}")

# ──────────────────────────────────────────────────────────────────────────────
# Endpunkte: Assets
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/assets", response_model=List[AssetOut])
def list_assets(
    q: Optional[str] = Query(None, description="LIKE-Suche über name und id"),
    type: Optional[AssetType] = Query(None),
    primary_category: Optional[str] = Query(None),
    status: Optional[AssetStatus] = Query(None),
    tag: Optional[str] = Query(None, description="Filter auf einen Tag"),
    sector: Optional[str] = Query(None, description="Filter auf Sektor (n:m oder legacy)"),
    sectors: Optional[List[str]] = Query(None, alias="sectors[]", description="Mehrere Sektoren (OR innerhalb Liste)"),
    sources: Optional[List[str]] = Query(None, alias="sources[]", description="Mehrere Sources (OR innerhalb Liste)"),
    order_by: str = Query("id", regex="^(id|name|updated_ts|created_ts)$"),
    order_dir: str = Query("asc", regex="^(asc|desc)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    qry = db.query(Asset).distinct()

    if q:
        like = f"%{q}%"
        qry = qry.filter(or_(Asset.name.ilike(like), Asset.id.ilike(like)))

    if type:
        qry = qry.filter(Asset.type == type.value)
    if primary_category:
        qry = qry.filter(Asset.primary_category == primary_category)
    if status:
        qry = qry.filter(Asset.status == status.value)

    if tag:
        qry = qry.join(AssetTag).filter(AssetTag.tag == tag)

    # Legacy Einzel-Sektor oder n:m
    if sector:
        qry = qry.outerjoin(AssetSector, AssetSector.asset_id == Asset.id)\
                 .filter(or_(AssetSector.sector == sector, Asset.sector == sector))

    if sectors:
        qry = qry.outerjoin(AssetSector, AssetSector.asset_id == Asset.id)\
                 .filter(or_(AssetSector.sector.in_(sectors), Asset.sector.in_(sectors)))

    if sources:
        qry = qry.join(Listing).filter(Listing.source.in_(sources))

    # Order
    order_col = {"id": Asset.id, "name": Asset.name, "updated_ts": Asset.updated_ts, "created_ts": Asset.created_ts}[order_by]
    if order_dir == "desc":
        order_col = order_col.desc()

    items = qry.order_by(order_col).offset(offset).limit(limit).all()
    log.debug(f"[ASSETS][LIST] n={len(items)} q={q} type={type} cat={primary_category} tag={tag} sector={sector} sectors={sectors} sources={sources} order={order_by} {order_dir} off={offset} lim={limit}")
    return [_asset_to_out(a) for a in items]



def _compute_group_tags(members: List[Dict[str, Any]]) -> List[GroupTagOut]:
    """
    Leitet group_tags (id, position) aus den Member-Zeilen ab.
    - id = der string in member['group_tag'] (z. B. "sec:f4d58299")
    - position = Reihenfolge nach erster Sichtung (stable)
    - name = None (UI/Client kann Name mappen)
    """
    seen_index: Dict[str, int] = {}
    order: List[str] = []

    # stabile Reihenfolge anhand Position & asset_id
    for m in sorted(members, key=lambda x: (x.get("position") or 0, x.get("asset_id") or "")):
        tag = m.get("group_tag")
        if not tag:
            continue
        if tag not in seen_index:
            seen_index[tag] = len(order)
            order.append(tag)

    out: List[GroupTagOut] = []
    for tag in order:
        out.append(GroupTagOut(id=tag, name=None, position=seen_index[tag]))
    log.debug("[GROUP_TAGS] derived=%s", [(gt.id, gt.position) for gt in out])
    return out


@app.post("/assets", response_model=AssetOut, status_code=201, dependencies=[Depends(require_auth)])
def create_asset(payload: AssetIn, db: Session = Depends(get_db)):
    if db.query(Asset).filter(Asset.id == payload.id).first():
        raise HTTPException(status_code=409, detail="Asset id exists")
    a = Asset(
        id=payload.id,
        type=payload.type.value,
        name=payload.name,
        country=payload.country,
        sector=payload.sector,
        primary_category=payload.primary_category,
        status=payload.status.value if isinstance(payload.status, AssetStatus) else str(payload.status or "active"),
        version=_new_ver(),
    )
    db.add(a)

    # listings
    for l in payload.listings:
        db.add(Listing(
            asset_id=a.id, source=l.source, symbol=l.symbol, exchange=l.exchange,
            mic=l.mic, isin=l.isin, cusip=l.cusip, figi=l.figi, note=l.note
        ))

    # tags
    for tg in payload.tags:
        if not db.query(Tag).filter(Tag.tag == tg).first():
            db.add(Tag(tag=tg))
        db.add(AssetTag(asset_id=a.id, tag=tg))

    # identifiers
    for ident in payload.identifiers:
        db.add(Identifier(asset_id=a.id, key=ident.key, value=ident.value))

    # sectors n:m (+Backfill aus legacy single sector)
    seen = set()
    for sec in (payload.sectors or []):
        s = sec.strip()
        if not s or s in seen:
            continue
        seen.add(s)
        if not db.query(Sector).filter(Sector.sector == s).first():
            db.add(Sector(sector=s))
        db.add(AssetSector(asset_id=a.id, sector=s))
    if payload.sector and payload.sector.strip():
        s = payload.sector.strip()
        if s not in seen:
            if not db.query(Sector).filter(Sector.sector == s).first():
                db.add(Sector(sector=s))
            db.add(AssetSector(asset_id=a.id, sector=s))

    db.flush()
    log.info(f"[ASSETS][CREATE] id={a.id} listings={len(a.listings)} tags={len(a.tags)} sectors={len(a.sectors)}")
    db.refresh(a)
    return _asset_to_out(a)

@app.get("/assets/{asset_id}", response_model=AssetOut)
def get_asset(asset_id: str = FPath(...), db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    return _asset_to_out(a)

@app.patch("/assets/{asset_id}", response_model=AssetOut, dependencies=[Depends(require_auth)])
def patch_asset(asset_id: str, payload: AssetPatch, db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    updated = []
    for field in ("type", "name", "country", "sector", "primary_category", "status"):
        val = getattr(payload, field)
        if val is not None:
            setattr(a, field, val.value if isinstance(val, (AssetType, AssetStatus)) else val)
            updated.append(field)
    if updated:
        a.version = _new_ver()
    log.info(f"[ASSETS][PATCH] id={asset_id} fields={updated} -> ver={a.version}")
    db.flush()
    db.refresh(a)
    return _asset_to_out(a)

@app.delete("/assets/{asset_id}", status_code=204, dependencies=[Depends(require_auth)])
def delete_asset(asset_id: str, db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(a)
    log.info(f"[ASSETS][DELETE] id={asset_id}")
    return

# ─ Listings CRUD ─
@app.post("/assets/{asset_id}/listings", response_model=ListingOut, status_code=201, dependencies=[Depends(require_auth)])
def add_listing(asset_id: str, payload: ListingIn, db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Asset not found")
    l = Listing(
        asset_id=asset_id, source=payload.source, symbol=payload.symbol,
        exchange=payload.exchange, mic=payload.mic, isin=payload.isin,
        cusip=payload.cusip, figi=payload.figi, note=payload.note
    )
    db.add(l)
    try:
        db.flush()
    except IntegrityError as e:
        db.rollback()
        log.warning(f"[LISTINGS][ADD][DUP] asset={asset_id} {payload.source}:{payload.symbol} {payload.mic or payload.exchange or ''}")
        raise HTTPException(status_code=409, detail="Listing duplicate (source/symbol/mic)") from e
    # bump version of asset
    a.version = _new_ver()
    db.flush()
    db.refresh(l); db.refresh(a)
    log.info(f"[LISTINGS][ADD] asset={asset_id} -> {payload.source}:{payload.symbol} {payload.mic or payload.exchange or ''} ver={a.version}")
    return ListingOut(**{**payload.dict(), "id": l.id})

@app.patch("/listings/{listing_id}", response_model=ListingOut, dependencies=[Depends(require_auth)])
def patch_listing(listing_id: int, payload: ListingPatch, db: Session = Depends(get_db)):
    l = db.query(Listing).filter(Listing.id == listing_id).first()
    if not l:
        raise HTTPException(status_code=404, detail="Not found")
    before = (l.source, l.symbol, l.mic)
    if payload.source is not None: l.source = payload.source
    if payload.symbol is not None: l.symbol = payload.symbol
    if payload.exchange is not None: l.exchange = payload.exchange
    if payload.mic is not None: l.mic = payload.mic
    if payload.isin is not None: l.isin = payload.isin
    if payload.cusip is not None: l.cusip = payload.cusip
    if payload.figi is not None: l.figi = payload.figi
    if payload.note is not None: l.note = payload.note
    try:
        db.flush()
    except IntegrityError as e:
        db.rollback()
        log.warning(f"[LISTINGS][PATCH][DUP] id={listing_id} tried {l.source}:{l.symbol}:{l.mic}")
        raise HTTPException(status_code=409, detail="Listing duplicate (source/symbol/mic)") from e
    # bump asset version
    a = db.query(Asset).filter(Asset.id == l.asset_id).first()
    if a:
        a.version = _new_ver()
        db.flush()
        db.refresh(a)
    db.refresh(l)
    log.info(f"[LISTINGS][PATCH] id={listing_id} {before} -> {(l.source,l.symbol,l.mic)} asset_ver={a.version if a else 'n/a'}")
    return ListingOut(
        id=l.id, source=l.source, symbol=l.symbol, exchange=l.exchange, mic=l.mic,
        isin=l.isin, cusip=l.cusip, figi=l.figi, note=l.note
    )

@app.delete("/listings/{listing_id}", status_code=204, dependencies=[Depends(require_auth)])
def delete_listing(listing_id: int, db: Session = Depends(get_db)):
    l = db.query(Listing).filter(Listing.id == listing_id).first()
    if not l:
        raise HTTPException(status_code=404, detail="Not found")
    asset_id = l.asset_id
    db.delete(l)
    # bump asset version
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if a:
        a.version = _new_ver()
        db.flush()
        db.refresh(a)
    log.info(f"[LISTINGS][DELETE] id={listing_id} asset={asset_id} ver={a.version if a else 'n/a'}")
    return

# ─ Tags CRUD ─
@app.get("/tags", response_model=List[str])
def list_tags(db: Session = Depends(get_db)):
    return [t.tag for t in db.query(Tag).order_by(Tag.tag).all()]


@app.post("/tags/{tag}", status_code=204, dependencies=[Depends(require_auth)])
def create_tag(tag: str, db: Session = Depends(get_db)):
    t = (tag or "").strip()
    if not t:
        raise HTTPException(status_code=400, detail="Empty tag")
    exists = db.query(Tag).filter(Tag.tag == t).first()
    if not exists:
        db.add(Tag(tag=t))
        log.info(f"[TAGS][CREATE] tag={t}")
    # idempotent: 204, auch wenn's ihn schon gab
    return Response(status_code=204)


@app.post("/assets/{asset_id}/tags/{tag}", status_code=204, dependencies=[Depends(require_auth)])
def add_tag(asset_id: str, tag: str, db: Session = Depends(get_db)):
    if not db.query(Asset).filter(Asset.id == asset_id).first():
        raise HTTPException(status_code=404, detail="Asset not found")
    if not db.query(Tag).filter(Tag.tag == tag).first():
        db.add(Tag(tag=tag))
    if not db.query(AssetTag).filter(AssetTag.asset_id == asset_id, AssetTag.tag == tag).first():
        db.add(AssetTag(asset_id=asset_id, tag=tag))
        a = db.query(Asset).filter(Asset.id == asset_id).first()
        if a:
            a.version = _new_ver()
        log.info(f"[TAGS][ADD] asset={asset_id} tag={tag} ver={a.version if a else 'n/a'}")
    return

@app.delete("/assets/{asset_id}/tags/{tag}", status_code=204, dependencies=[Depends(require_auth)])
def remove_tag(asset_id: str, tag: str, db: Session = Depends(get_db)):
    at = db.query(AssetTag).filter(AssetTag.asset_id == asset_id, AssetTag.tag == tag).first()
    if not at:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(at)
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if a:
        a.version = _new_ver()
    log.info(f"[TAGS][DEL] asset={asset_id} tag={tag} ver={a.version if a else 'n/a'}")
    return

# ─ Sectors CRUD ─
@app.get("/sectors", response_model=List[str])
def list_sectors(db: Session = Depends(get_db)):
    return [s.sector for s in db.query(Sector).order_by(Sector.sector).all()]



# Katalog: Sektor anlegen (idempotent)
@app.post("/sectors/{sector}", status_code=204, dependencies=[Depends(require_auth)])
def create_sector(sector: str, db: Session = Depends(get_db)):
    s = (sector or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="Empty sector")
    exists = db.query(Sector).filter(Sector.sector == s).first()
    if not exists:
        db.add(Sector(sector=s))
        log.info(f"[SECTORS][CREATE] sector={s}")
    # idempotent: 204, auch wenn's ihn schon gab
    return Response(status_code=204)


# Report für globales Löschen
class SectorDeleteReport(BaseModel):
    removed_from_assets: int       # Anzahl Assets, aus denen der Sektor in n:m entfernt wurde (distinct)
    legacy_cleared: int            # Anzahl Assets, bei denen legacy Asset.sector auf NULL gesetzt wurde
    deleted_links: int             # Anzahl gelöschter AssetSector-Zeilen
    deleted_catalog_entry: bool    # Katalogeintrag aus 'sectors' gelöscht?
    bumped_assets: int             # Anzahl Assets, deren Version erhöht wurde


# Katalog: Sektor löschen; optional global purgen
@app.delete("/sectors/{sector}", response_model=SectorDeleteReport, dependencies=[Depends(require_auth)])
def delete_sector(
    sector: str,
    purge: bool = Query(False, alias="purge", description="true = global: aus allen Assets entfernen + Katalogeintrag löschen"),
    db: Session = Depends(get_db),
):
    s = (sector or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="Empty sector")

    # Nutzung zählen
    in_use_nm = db.query(AssetSector).filter(AssetSector.sector == s).count()
    in_use_legacy = db.query(Asset).filter(Asset.sector == s).count()

    # Ohne purge verhindern wir versehentliches globales Löschen
    if not purge and (in_use_nm or in_use_legacy):
        raise HTTPException(
            status_code=409,
            detail={"in_use_nm": in_use_nm, "in_use_legacy": in_use_legacy},
        )

    # Welche Assets sind betroffen?
    affected_ids_nm = [r[0] for r in db.query(AssetSector.asset_id).filter(AssetSector.sector == s).distinct().all()]
    affected_ids_legacy = [r[0] for r in db.query(Asset.id).filter(Asset.sector == s).distinct().all()]
    affected_ids = set(affected_ids_nm) | set(affected_ids_legacy)

    # n:m entfernen
    deleted_links = db.query(AssetSector).filter(AssetSector.sector == s).delete(synchronize_session=False)

    # legacy-Feld nullen
    legacy_cleared = db.query(Asset).filter(Asset.sector == s).update({Asset.sector: None}, synchronize_session=False)

    # Version für alle betroffenen Assets erhöhen
    bumped_assets = 0
    if affected_ids:
        # Achtung: _new_ver() pro Asset erzeugen (sonst überall die gleiche)
        # -> hier mit Einzel-Update je Asset, ist sicherer und klarer.
        for aid in affected_ids:
            a = db.query(Asset).filter(Asset.id == aid).first()
            if a:
                a.version = _new_ver()
                bumped_assets += 1

    # Katalogeintrag löschen (wenn vorhanden)
    deleted_catalog_entry = False
    cat = db.query(Sector).filter(Sector.sector == s).first()
    if cat:
        db.delete(cat)
        deleted_catalog_entry = True

    log.info(
        f"[SECTORS][DELETE] sector={s} purge={purge} "
        f"in_use_nm={in_use_nm} in_use_legacy={in_use_legacy} "
        f"deleted_links={deleted_links} legacy_cleared={legacy_cleared} bumped_assets={bumped_assets} "
        f"catalog_deleted={deleted_catalog_entry}"
    )

    return SectorDeleteReport(
        removed_from_assets=len(affected_ids_nm),
        legacy_cleared=legacy_cleared,
        deleted_links=deleted_links,
        deleted_catalog_entry=deleted_catalog_entry,
        bumped_assets=bumped_assets,
    )


@app.delete("/tags/{tag}", response_model=TagDeleteReport, dependencies=[Depends(require_auth)])
def delete_tag(
    tag: str,
    purge: bool = Query(False, alias="purge", description="true = global: aus allen Assets entfernen + Katalogeintrag löschen"),
    db: Session = Depends(get_db),
):
    t = (tag or "").strip()
    if not t:
        raise HTTPException(status_code=400, detail="Empty tag")

    # Nutzung zählen
    in_use_nm = db.query(AssetTag).filter(AssetTag.tag == t).count()
    in_use_legacy = 0  # für UI-Kompatibilität: selbe Keys wie Sektor-409

    # Ohne purge verhindern wir versehentliches globales Löschen
    if not purge and in_use_nm:
        raise HTTPException(
            status_code=409,
            detail={"in_use_nm": in_use_nm, "in_use_legacy": in_use_legacy},
        )

    # Betroffene Assets (distinct)
    affected_ids = [r[0] for r in db.query(AssetTag.asset_id).filter(AssetTag.tag == t).distinct().all()]

    # n:m entfernen
    deleted_links = db.query(AssetTag).filter(AssetTag.tag == t).delete(synchronize_session=False)

    # Version für alle betroffenen Assets erhöhen
    bumped_assets = 0
    if affected_ids:
        for aid in affected_ids:
            a = db.query(Asset).filter(Asset.id == aid).first()
            if a:
                a.version = _new_ver()
                bumped_assets += 1

    # Katalogeintrag löschen (wenn vorhanden)
    deleted_catalog_entry = False
    cat = db.query(Tag).filter(Tag.tag == t).first()
    if cat:
        db.delete(cat)
        deleted_catalog_entry = True

    log.info(
        f"[TAGS][DELETE] tag={t} purge={purge} in_use_nm={in_use_nm} "
        f"deleted_links={deleted_links} bumped_assets={bumped_assets} catalog_deleted={deleted_catalog_entry}"
    )

    return TagDeleteReport(
        removed_from_assets=len(affected_ids),
        deleted_links=deleted_links,
        deleted_catalog_entry=deleted_catalog_entry,
        bumped_assets=bumped_assets,
    )

@app.put("/assets/{asset_id}/tags", dependencies=[Depends(require_auth)])
def put_asset_tags(asset_id: str, payload: AssetTagsPut, db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Asset not found")

    client_ver = payload.version
    if client_ver and client_ver != a.version:
        log.warning(f"[TAGS][PUT][409] asset={asset_id} client={client_ver} server={a.version}")
        raise HTTPException(status_code=409, detail={"server_version": a.version})

    # Normalisieren + deduplizieren
    new_tags = [s.strip() for s in (payload.tags or []) if isinstance(s, str) and s.strip()]
    new_set = set(new_tags)

    old_tags = {t.tag for t in a.tags}
    to_add = list(new_set - old_tags)
    to_del = list(old_tags - new_set)

    # Upsert Tag-Katalogeinträge
    for t in to_add:
        if not db.query(Tag).filter(Tag.tag == t).first():
            db.add(Tag(tag=t))

    # Apply changes
    if to_del:
        db.query(AssetTag).filter(AssetTag.asset_id == asset_id, AssetTag.tag.in_(to_del)).delete(synchronize_session=False)
    for t in to_add:
        db.add(AssetTag(asset_id=asset_id, tag=t))

    a.version = _new_ver()
    db.flush(); db.refresh(a)
    log.info(f"[TAGS][PUT] asset={asset_id} add={to_add} del={to_del} -> ver={a.version}")
    return {"version": a.version, "tags": sorted(new_set)}




@app.post("/assets/{asset_id}/sectors/{sector}", status_code=204, dependencies=[Depends(require_auth)])
def add_sector(asset_id: str, sector: str, db: Session = Depends(get_db)):
    if not db.query(Asset).filter(Asset.id == asset_id).first():
        raise HTTPException(status_code=404, detail="Asset not found")
    if not db.query(Sector).filter(Sector.sector == sector).first():
        db.add(Sector(sector=sector))
    if not db.query(AssetSector).filter(AssetSector.asset_id == asset_id, AssetSector.sector == sector).first():
        db.add(AssetSector(asset_id=asset_id, sector=sector))
        a = db.query(Asset).filter(Asset.id == asset_id).first()
        if a:
            a.version = _new_ver()
        log.info(f"[SECTORS][ADD] asset={asset_id} sector={sector} ver={a.version if a else 'n/a'}")
    return

@app.delete("/assets/{asset_id}/sectors/{sector}", status_code=204, dependencies=[Depends(require_auth)])
def remove_sector(asset_id: str, sector: str, db: Session = Depends(get_db)):
    asx = db.query(AssetSector).filter(AssetSector.asset_id == asset_id, AssetSector.sector == sector).first()
    if not asx:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(asx)
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if a:
        a.version = _new_ver()
    log.info(f"[SECTORS][DEL] asset={asset_id} sector={sector} ver={a.version if a else 'n/a'}")
    return

# Full-Set PUT für Sektoren mit Versionsschutz
class AssetSectorsPut(BaseModel):
    sectors: List[str] = Field(default_factory=list)
    version: Optional[str] = None



@app.put("/assets/{asset_id}/sectors", dependencies=[Depends(require_auth)])
def put_asset_sectors(asset_id: str, payload: AssetSectorsPut, db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Asset not found")

    client_ver = payload.version
    if client_ver and client_ver != a.version:
        log.warning(f"[SECTORS][PUT][409] asset={asset_id} client={client_ver} server={a.version}")
        raise HTTPException(status_code=409, detail={"server_version": a.version})

    # Normalisieren + deduplizieren
    new_secs = [s.strip() for s in payload.sectors if isinstance(s, str) and s.strip()]
    new_set = set(new_secs)

    old_secs = {s.sector for s in a.sectors}
    to_add = list(new_set - old_secs)
    to_del = list(old_secs - new_set)

    # Upsert Sectors table
    for s in to_add:
        if not db.query(Sector).filter(Sector.sector == s).first():
            db.add(Sector(sector=s))

    # Apply changes
    if to_del:
        db.query(AssetSector).filter(AssetSector.asset_id == asset_id, AssetSector.sector.in_(to_del)).delete(synchronize_session=False)
    for s in to_add:
        db.add(AssetSector(asset_id=asset_id, sector=s))

    a.version = _new_ver()
    db.flush(); db.refresh(a)
    log.info(f"[SECTORS][PUT] asset={asset_id} add={to_add} del={to_del} -> ver={a.version}")
    return {"version": a.version, "sectors": sorted(new_set)}




# ─ Identifiers CRUD ─
@app.post("/assets/{asset_id}/identifiers", status_code=204, dependencies=[Depends(require_auth)])
def upsert_identifier(asset_id: str, payload: IdentifierIn, db: Session = Depends(get_db)):
    if not db.query(Asset).filter(Asset.id == asset_id).first():
        raise HTTPException(status_code=404, detail="Asset not found")
    idt = db.query(Identifier).filter(Identifier.asset_id == asset_id, Identifier.key == payload.key).first()
    if idt:
        idt.value = payload.value
        a = db.query(Asset).filter(Asset.id == asset_id).first()
        if a:
            a.version = _new_ver()
        log.info(f"[IDENT][UPD] asset={asset_id} {payload.key}={payload.value} ver={a.version if a else 'n/a'}")
    else:
        db.add(Identifier(asset_id=asset_id, key=payload.key, value=payload.value))
        a = db.query(Asset).filter(Asset.id == asset_id).first()
        if a:
            a.version = _new_ver()
        log.info(f"[IDENT][ADD] asset={asset_id} {payload.key}={payload.value} ver={a.version if a else 'n/a'}")
    return

@app.delete("/assets/{asset_id}/identifiers/{key}", status_code=204, dependencies=[Depends(require_auth)])
def delete_identifier(asset_id: str, key: str, db: Session = Depends(get_db)):
    idt = db.query(Identifier).filter(Identifier.asset_id == asset_id, Identifier.key == key).first()
    if not idt:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(idt)
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if a:
        a.version = _new_ver()
    log.info(f"[IDENT][DEL] asset={asset_id} {key} ver={a.version if a else 'n/a'}")
    return

# ──────────────────────────────────────────────────────────────────────────────
# Endpunkte: Groups
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/groups", response_model=List[GroupOut])
def list_groups(
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    if limit > 10000:
        limit = 10000

    gs = db.query(Group).order_by(Group.id).offset(offset).limit(limit).all()
    out: List[GroupOut] = []
    for g in gs:
        members_payload = [{
            "asset_id": m.asset_id, "source": m.source, "exchange": m.exchange,
            "mic": m.mic, "position": m.position, "group_tag": m.group_tag
        } for m in sorted(g.members, key=lambda x: (x.position or 0, x.asset_id))]

        out.append(GroupOut(
            id=g.id, name=g.name, default_source=g.default_source, version=g.version,
            created_ts=g.created_ts, updated_ts=g.updated_ts,
            members=members_payload,
            group_tags=_compute_group_tags(members_payload),
        ))
    log.debug(f"[GROUPS][LIST] n={len(out)} off={offset} lim={limit}")
    return out


@app.post("/groups", response_model=GroupOut, status_code=201, dependencies=[Depends(require_auth)])
def create_group(payload: GroupIn, db: Session = Depends(get_db)):
    if db.query(Group).filter(Group.id == payload.id).first():
        raise HTTPException(status_code=409, detail="Group id exists")
    g = Group(id=payload.id, name=payload.name, default_source=payload.default_source, version=_new_ver())
    db.add(g); db.flush(); db.refresh(g)
    log.info(f"[GROUP][CREATE] id={g.id} ver={g.version}")
    return GroupOut(
        id=g.id, name=g.name, default_source=g.default_source, version=g.version,
        created_ts=g.created_ts, updated_ts=g.updated_ts, members=[]
    )

# NEU: Server-generierte Gruppen-ID (alternative zu POST /groups)
class GroupNewIn(BaseModel):
    name: str
    default_source: Optional[str] = None

@app.post("/groups/new", response_model=GroupOut, status_code=201, dependencies=[Depends(require_auth)])
def create_group_new(payload: GroupNewIn, db: Session = Depends(get_db)):
    base = (payload.name or "").strip().lower()
    base = "".join(ch if ch.isalnum() else "-" for ch in base).strip("-") or "group"
    gid = f"group:{base}-{uuid4().hex[:8]}"
    while db.query(Group).filter(Group.id == gid).first() is not None:
        gid = f"group:{base}-{uuid4().hex[:8]}"

    g = Group(id=gid, name=payload.name, default_source=payload.default_source, version=_new_ver())
    db.add(g); db.flush(); db.refresh(g)
    log.info(f"[GROUP][CREATE_NEW] id={g.id} ver={g.version}")
    return GroupOut(
        id=g.id, name=g.name, default_source=g.default_source, version=g.version,
        created_ts=g.created_ts, updated_ts=g.updated_ts, members=[]
    )

@app.get("/groups/{group_id}", response_model=GroupOut)
def get_group(group_id: str, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Not found")
    members_payload = [{
        "asset_id": m.asset_id, "source": m.source, "exchange": m.exchange,
        "mic": m.mic, "position": m.position, "group_tag": m.group_tag
    } for m in sorted(g.members, key=lambda x: (x.position or 0, x.asset_id))]

    return GroupOut(
        id=g.id, name=g.name, default_source=g.default_source, version=g.version,
        created_ts=g.created_ts, updated_ts=g.updated_ts,
        members=members_payload,
        group_tags=_compute_group_tags(members_payload),
    )


@app.post("/groups/{group_id}/members", status_code=204, dependencies=[Depends(require_auth)])
def add_group_member(group_id: str, member: GroupMemberIn, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Group not found")
    if not db.query(Asset).filter(Asset.id == member.asset_id).first():
        raise HTTPException(status_code=404, detail="Asset not found")
    if db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == member.asset_id).first():
        raise HTTPException(status_code=409, detail="Already member")
    db.add(GroupMember(
        group_id=group_id, asset_id=member.asset_id, source=member.source,
        exchange=member.exchange, mic=member.mic, position=member.position,
        group_tag=member.group_tag
    ))
    g.version = _new_ver()
    log.info(f"[GROUP][ADD] group={group_id} asset={member.asset_id} pos={member.position} tag={member.group_tag} ver={g.version}")
    return

@app.patch("/groups/{group_id}/members/{asset_id}", status_code=204, dependencies=[Depends(require_auth)])
def patch_group_member(group_id: str, asset_id: str, payload: GroupMemberPatch, db: Session = Depends(get_db)):
    m = db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == asset_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="Not found")
    before = (m.position, m.source, m.exchange, m.mic, m.group_tag)
    if payload.position is not None: m.position = payload.position
    if payload.source is not None:   m.source = payload.source
    if payload.exchange is not None: m.exchange = payload.exchange
    if payload.mic is not None:      m.mic = payload.mic
    if payload.group_tag is not None: m.group_tag = payload.group_tag
    db.flush()
    g = db.query(Group).filter(Group.id == group_id).first()
    if g:
        g.version = _new_ver()
    log.info(f"[GROUP][MEMBER][PATCH] group={group_id} asset={asset_id} {before} -> {(m.position,m.source,m.exchange,m.mic,m.group_tag)} ver={g.version if g else 'n/a'}")
    return

@app.post("/groups/{group_id}/members/reorder", status_code=204, dependencies=[Depends(require_auth)])
def reorder_group_members(group_id: str, payload: ReorderPayload, db: Session = Depends(get_db)):
    updated = 0
    for item in payload.items:
        m = db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == item.asset_id).first()
        if m:
            m.position = item.position
            updated += 1
    g = db.query(Group).filter(Group.id == group_id).first()
    if g:
        g.version = _new_ver()
    db.flush()
    log.info(f"[GROUP][REORDER] group={group_id} items={len(payload.items)} updated={updated} ver={g.version if g else 'n/a'}")
    return

@app.delete("/groups/{group_id}/members/{asset_id}", status_code=204, dependencies=[Depends(require_auth)])
def remove_group_member(group_id: str, asset_id: str, db: Session = Depends(get_db)):
    m = db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == asset_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(m)
    g = db.query(Group).filter(Group.id == group_id).first()
    if g:
        g.version = _new_ver()
    log.info(f"[GROUP][DEL] group={group_id} asset={asset_id} ver={g.version if g else 'n/a'}")
    return

@app.patch("/groups/{group_id}", response_model=GroupOut, dependencies=[Depends(require_auth)])
def patch_group(group_id: str, payload: GroupIn, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Not found")
    if payload.name:
        g.name = payload.name
    if payload.default_source is not None:
        g.default_source = payload.default_source
    g.version = _new_ver()
    db.flush(); db.refresh(g)
    log.info(f"[GROUP][PATCH] id={group_id} name={g.name} default_source={g.default_source} ver={g.version}")
    members_payload = [{
        "asset_id": m.asset_id, "source": m.source, "exchange": m.exchange,
        "mic": m.mic, "position": m.position, "group_tag": m.group_tag
    } for m in sorted(g.members, key=lambda x: (x.position or 0, x.asset_id))]

    return GroupOut(
        id=g.id, name=g.name, default_source=g.default_source, version=g.version,
        created_ts=g.created_ts, updated_ts=g.updated_ts,
        members=members_payload,
        group_tags=_compute_group_tags(members_payload),
    )


@app.delete("/groups/{group_id}", status_code=204, dependencies=[Depends(require_auth)])
def delete_group(group_id: str, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(g)
    log.info(f"[GROUP][DELETE] id={group_id}")
    return

# Atomarer Bulk-Write mit Versionsprüfung
@app.post("/groups/{group_id}/bulk", response_model=GroupBulkResponse, dependencies=[Depends(require_auth)])
def bulk_group_update(group_id: str, payload: GroupBulkRequest, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Group not found")

    if payload.version and payload.version != g.version:
        log.warning(f"[GROUP][BULK][409] group={group_id} client={payload.version} server={g.version}")
        raise HTTPException(status_code=409, detail={"server_version": g.version})

    applied = {"adds": 0, "removes": 0, "updates": 0, "reorder": 0}

    # removes
    if payload.removes:
        for aid in payload.removes:
            m = db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == aid).first()
            if m:
                db.delete(m); applied["removes"] += 1

    # adds
    for it in payload.adds or []:
        if not db.query(Asset).filter(Asset.id == it.asset_id).first():
            raise HTTPException(status_code=404, detail=f"Asset not found: {it.asset_id}")
        if db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == it.asset_id).first():
            continue
        db.add(GroupMember(
            group_id=group_id, asset_id=it.asset_id,
            source=it.source, exchange=it.exchange, mic=it.mic,
            position=it.position, group_tag=it.group_tag
        ))
        applied["adds"] += 1

    # updates
    for it in payload.updates or []:
        m = db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == it.asset_id).first()
        if not m:
            continue
        if it.source is not None:   m.source = it.source
        if it.exchange is not None: m.exchange = it.exchange
        if it.mic is not None:      m.mic = it.mic
        if it.position is not None: m.position = it.position
        if it.group_tag is not None: m.group_tag = it.group_tag
        applied["updates"] += 1

    # reorder
    for it in payload.reorder or []:
        m = db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == it.asset_id).first()
        if m:
            m.position = it.position
            applied["reorder"] += 1

    # final: bump version
    g.version = _new_ver()
    db.flush(); db.refresh(g)
    log.info(f"[GROUP][BULK] group={group_id} applied={applied} -> ver={g.version}")
    return GroupBulkResponse(version=g.version, applied=applied)

# ──────────────────────────────────────────────────────────────────────────────
# Profiles (JSON) – Speichern/Laden/Löschen für Group-Manager/Notifier
# ──────────────────────────────────────────────────────────────────────────────
from sqlalchemy import text, literal, select
from sqlalchemy.sql import exists

@app.get("/profiles", response_model=List[ProfileOut])
def list_profiles(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    q: Optional[str] = Query(None, description="LIKE auf name/id"),
    group: Optional[str] = Query(None, description="Filter: payload.groups enthält diese gid"),
    db: Session = Depends(get_db),
):
    qry = db.query(Profile)

    if q:
        like = f"%{q}%"
        qry = qry.filter(or_(Profile.name.ilike(like), Profile.id.ilike(like)))

    if group:
        g = group.strip()
        if IS_SQLITE:
            # JSON1: EXISTS(SELECT 1 FROM json_each(profiles.payload,'$.groups') WHERE value = :gid)
            sub = select(literal(1)).select_from(
                text("json_each(profiles.payload, '$.groups')")
            ).where(text("json_each.value = :gid")).params(gid=g)
            qry = qry.filter(exists(sub))
        else:
            # MySQL/MariaDB: JSON_CONTAINS(payload, JSON_QUOTE(:gid), '$.groups')
            # Fallback: LIKE
            try:
                qry = qry.filter(func.json_contains(Profile.payload, func.json_quote(g), '$.groups') == 1)  # MySQL
            except Exception:
                like_g = f'%"{g}"%'
                qry = qry.filter(Profile.payload.cast(String).like(like_g))

    rows = qry.order_by(Profile.name).offset(offset).limit(limit).all()
    out: List[ProfileOut] = [
        ProfileOut(id=p.id, name=p.name, payload=p.payload or {}, version=p.version, created_ts=p.created_ts, updated_ts=p.updated_ts)
        for p in rows
    ]
    log.debug(f"[PROFILES][LIST] n={len(out)} off={offset} lim={limit} q={q} group={group}")
    return out

@app.get("/profiles/by-group/{group_id}", response_model=List[ProfileOut])
def list_profiles_by_group(
    group_id: str,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    return list_profiles(limit=limit, offset=offset, q=None, group=group_id, db=db)

@app.post("/profiles", response_model=ProfileOut, status_code=201, dependencies=[Depends(require_auth)])
def create_profile(payload: ProfileIn, db: Session = Depends(get_db)):
    pid = payload.id.strip() if (payload.id and payload.id.strip()) else f"profile:{uuid4().hex}"
    if db.query(Profile).filter(Profile.id == pid).first():
        raise HTTPException(status_code=409, detail="Profile id exists")
    p = Profile(id=pid, name=payload.name, payload=payload.payload or {}, version=_new_ver())
    db.add(p); db.flush(); db.refresh(p)
    log.info(f"[PROFILES][CREATE] id={p.id} name={p.name} ver={p.version}")
    return ProfileOut(id=p.id, name=p.name, payload=p.payload or {}, version=p.version, created_ts=p.created_ts, updated_ts=p.updated_ts)

@app.get("/profiles/{profile_id}", response_model=ProfileOut)
def get_profile(profile_id: str, db: Session = Depends(get_db)):
    p = db.query(Profile).filter(Profile.id == profile_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Not found")
    return ProfileOut(id=p.id, name=p.name, payload=p.payload or {}, version=p.version, created_ts=p.created_ts, updated_ts=p.updated_ts)

@app.patch("/profiles/{profile_id}", response_model=ProfileOut, dependencies=[Depends(require_auth)])
def patch_profile(profile_id: str, payload: ProfilePatch, db: Session = Depends(get_db)):
    p = db.query(Profile).filter(Profile.id == profile_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Not found")

    if payload.version and payload.version != p.version:
        log.warning(f"[PROFILES][PATCH][409] id={profile_id} client={payload.version} server={p.version}")
        raise HTTPException(status_code=409, detail={"server_version": p.version})

    if payload.name is not None:
        p.name = payload.name
    if payload.payload is not None:
        p.payload = payload.payload

    p.version = _new_ver()
    db.flush(); db.refresh(p)
    log.info(f"[PROFILES][PATCH] id={p.id} name={p.name} ver={p.version}")
    return ProfileOut(id=p.id, name=p.name, payload=p.payload or {}, version=p.version, created_ts=p.created_ts, updated_ts=p.updated_ts)

@app.delete("/profiles/{profile_id}", status_code=204, dependencies=[Depends(require_auth)])
def delete_profile(profile_id: str, db: Session = Depends(get_db)):
    p = db.query(Profile).filter(Profile.id == profile_id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(p)
    log.info(f"[PROFILES][DELETE] id={profile_id}")
    return

# ──────────────────────────────────────────────────────────────────────────────
# Resolver & Suche
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/resolve")
def resolve_endpoint(
    asset_id: str = Query(...),
    source: Optional[str] = Query(None),
    exchange: Optional[str] = Query(None),
    mic: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    res = resolve_symbol(db, asset_id, source, exchange, mic)
    log.debug(f"[RESOLVE][OUT] {res}")
    return res

@app.get("/search")
def search(
    q: str = Query(..., description="LIKE: asset.name/id/country/sector, listing.symbol, identifiers, tags"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    like = f"%{q}%"
    assets = db.query(Asset).filter(
        or_(Asset.name.ilike(like), Asset.id.ilike(like), Asset.country.ilike(like), Asset.sector.ilike(like))
    ).offset(offset).limit(limit).all()

    listings = db.query(Listing).filter(Listing.symbol.ilike(like)).offset(offset).limit(limit).all()
    idents = db.query(Identifier).filter(Identifier.value.ilike(like)).offset(offset).limit(limit).all()
    tags = db.query(Tag).filter(Tag.tag.ilike(like)).offset(offset).limit(limit).all()

    out: Dict[str, Any] = {
        "assets": [a.id for a in assets],
        "listings": [{"asset_id": l.asset_id, "source": l.source, "symbol": l.symbol, "mic": l.mic, "exchange": l.exchange} for l in listings],
        "identifiers": [{"asset_id": i.asset_id, "key": i.key, "value": i.value} for i in idents],
        "tags": [t.tag for t in tags],
        "offset": offset,
        "limit": limit,
    }
    log.debug(f"[SEARCH] q={q} -> a={len(assets)} l={len(listings)} i={len(idents)} t={len(tags)} off={offset} lim={limit}")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "ok": True,
        "db_url": DB_URL,
        "db_path": cfg.REGISTRY_MANAGER_DB if IS_SQLITE else None,
        "engine": "sqlite-wal" if IS_SQLITE else "sqlalchemy",
        "time": datetime.utcnow().isoformat() + "Z",
        "version": "1.3.0",
        "auth": "enabled" if AUTH_ENABLED else "disabled",
    }

# ──────────────────────────────────────────────────────────────────────────────
# Meta: Distinct Types, Categories, Tags, Sectors, Sources (filterbar)
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/meta/types", response_model=List[str])
def meta_types(db: Session = Depends(get_db)):
    rows = db.query(distinct(Asset.type)).order_by(Asset.type).all()
    types = [r[0] for r in rows if r[0]]
    log.debug(f"[META][TYPES] n={len(types)} -> {types}")
    return types

@app.get("/meta/categories", response_model=List[str])
def meta_categories(
    type: Optional[AssetType] = Query(None, description="optional: filter by asset type"),
    db: Session = Depends(get_db)
):
    qry = db.query(distinct(Asset.primary_category))
    if type:
        qry = qry.filter(Asset.type == type.value)
    rows = qry.order_by(Asset.primary_category).all()
    cats = [r[0] for r in rows if r[0]]
    log.debug(f"[META][CATS] type={type} n={len(cats)} -> {cats}")
    return cats

@app.get("/meta/tags", response_model=List[str])
def meta_tags(
    type: Optional[AssetType] = Query(None, description="optional: filter by asset type"),
    primary_category: Optional[str] = Query(None, description="optional: filter by primary_category"),
    db: Session = Depends(get_db)
):
    qry = db.query(distinct(AssetTag.tag)).join(Asset, Asset.id == AssetTag.asset_id)
    if type:
        qry = qry.filter(Asset.type == type.value)
    if primary_category:
        qry = qry.filter(Asset.primary_category == primary_category)
    rows = qry.order_by(AssetTag.tag).all()
    tags = [r[0] for r in rows if r[0]]
    log.debug(f"[META][TAGS] type={type} cat={primary_category} n={len(tags)} -> {tags}")
    return tags

@app.get("/meta/sectors", response_model=List[str])
def meta_sectors(
    type: Optional[AssetType] = Query(None, description="optional: filter by asset type"),
    primary_category: Optional[str] = Query(None, description="optional: filter by primary_category"),
    db: Session = Depends(get_db)
):
    # n:m
    q_nm = db.query(distinct(AssetSector.sector)).join(Asset, Asset.id == AssetSector.asset_id)
    if type: q_nm = q_nm.filter(Asset.type == type.value)
    if primary_category: q_nm = q_nm.filter(Asset.primary_category == primary_category)
    nm = [r[0] for r in q_nm.all() if r[0]]

    # legacy single string
    q_legacy = db.query(distinct(Asset.sector))
    if type: q_legacy = q_legacy.filter(Asset.type == type.value)
    if primary_category: q_legacy = q_legacy.filter(Asset.primary_category == primary_category)
    legacy = [r[0] for r in q_legacy.all() if r[0]]

    merged = sorted({*nm, *legacy}, key=lambda s: s.lower())
    log.debug(f"[META][SECTORS] type={type} cat={primary_category} n={len(merged)}")
    return merged

@app.get("/meta/sources", response_model=List[str])
def meta_sources(
    type: Optional[AssetType] = Query(None, description="optional: filter by asset type"),
    primary_category: Optional[str] = Query(None, description="optional: filter by primary_category"),
    db: Session = Depends(get_db)
):
    # distinct Listing.source, optional via Asset join filterbar
    qry = db.query(distinct(Listing.source)).join(Asset, Asset.id == Listing.asset_id)
    if type:
        qry = qry.filter(Asset.type == type.value)
    if primary_category:
        qry = qry.filter(Asset.primary_category == primary_category)

    rows = qry.order_by(Listing.source).all()
    sources = [r[0] for r in rows if r and r[0]]
    log.debug(f"[META][SOURCES] type={type} cat={primary_category} n={len(sources)} -> {sources}")
    return sources
