# registry_api.py
# -*- coding: utf-8 -*-
"""
Registry/GroupManager API (SQLite, SQLAlchemy, FastAPI)
- Kanonische Assets (1 Eintrag je wirtschaftliches Objekt)
- Listings (mehrere Börsen/Quellen/Ticker pro Asset)
- Tags (n:m)
- Custom Gruppen (Members referenzieren asset_id, optional source/mic/exchange)
- Resolver: asset_id + source/(exchange|mic) -> Symbol
- Suche: einfache Filter + Volltext-ähnliche LIKE-Suche

Start:
    uvicorn registry_api:app --reload --port 8098

ENV:
    REGISTRY_DB_URL=sqlite:///./registry.db   (default)
    LOG_LEVEL=DEBUG|INFO                       (default: INFO)
"""

from __future__ import annotations

import os
import sys
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Depends, HTTPException, Query, Body, Path as FPath
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime

from sqlalchemy import (
    create_engine, Column, String, Integer, Text, DateTime, ForeignKey, UniqueConstraint,
    Index, event
)
from sqlalchemy.orm import (
    sessionmaker, declarative_base, relationship, Session
)
from sqlalchemy.exc import IntegrityError

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
# DB Setup (SQLite WAL) – Pfad aus config.REGISTRY_DB → data/Symbol_Manager/registry.sqlite
# ──────────────────────────────────────────────────────────────────────────────
from pathlib import Path
import config as cfg

# Stelle sicher, dass der Ordner existiert
Path(cfg.REGISTRY_DB).parent.mkdir(parents=True, exist_ok=True)

# ENV override bleibt möglich; sonst nimm den Pfad aus config
DB_URL = os.getenv("REGISTRY_DB_URL", f"sqlite:///{cfg.REGISTRY_DB}")
IS_SQLITE = DB_URL.startswith("sqlite")

# check_same_thread=False: FastAPI worker threads teilen dieselbe ConnectionFactory
engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if IS_SQLITE else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def _now() -> datetime:
    return datetime.utcnow()


@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if not IS_SQLITE:
        return
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    cursor.close()
    log.debug("[DB] SQLite pragmas set (WAL, synchronous=NORMAL, foreign_keys=ON)")

# ──────────────────────────────────────────────────────────────────────────────
# ORM Modelle
# ──────────────────────────────────────────────────────────────────────────────
class Asset(Base):
    __tablename__ = "assets"
    id = Column(String, primary_key=True)  # "asset:msft" slug oder uuid
    type = Column(String, nullable=False)  # equity|crypto|commodity|index|forex|etf|bond|other|unknown
    name = Column(String, nullable=True)
    country = Column(String, nullable=True)
    sector = Column(String, nullable=True)
    primary_category = Column(String, nullable=False)  # genau eine
    status = Column(String, nullable=False, default="active")  # active|unsorted|inactive
    created_ts = Column(DateTime, default=_now, nullable=False)
    updated_ts = Column(DateTime, default=_now, onupdate=_now, nullable=False)

    listings = relationship("Listing", back_populates="asset", cascade="all, delete-orphan")
    tags = relationship("AssetTag", back_populates="asset", cascade="all, delete-orphan")
    identifiers = relationship("Identifier", back_populates="asset", cascade="all, delete-orphan")


class Listing(Base):
    __tablename__ = "listings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    source = Column(String, nullable=False)       # EODHD|BINANCE|YF|...
    exchange = Column(String, nullable=True)      # "NASDAQ"
    mic = Column(String, nullable=True)           # "XNAS"
    symbol = Column(String, nullable=False)       # "MSFT" / "BTCUSDT"
    isin = Column(String, nullable=True)
    cusip = Column(String, nullable=True)
    figi = Column(String, nullable=True)
    note = Column(Text, nullable=True)

    asset = relationship("Asset", back_populates="listings")

    __table_args__ = (
        # Quelle + Symbol i. d. R. eindeutig; Exchange/MIC verbessern Eindeutigkeit
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
    # relation zu Tag nicht nötig für API, spart Join-Overhead


class Identifier(Base):
    __tablename__ = "identifiers"
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True)
    key = Column(String, primary_key=True)    # "isin"|"cusip"|"figi"|...
    value = Column(String, nullable=False)

    asset = relationship("Asset", back_populates="identifiers")


class Group(Base):
    __tablename__ = "groups"
    id = Column(String, primary_key=True)  # "group:my_tech_watchlist" oder UUID
    name = Column(String, nullable=False)
    default_source = Column(String, nullable=True)
    created_ts = Column(DateTime, default=_now, nullable=False)
    updated_ts = Column(DateTime, default=_now, onupdate=_now, nullable=False)

    members = relationship("GroupMember", back_populates="group", cascade="all, delete-orphan")


class GroupMember(Base):
    __tablename__ = "group_members"
    group_id = Column(String, ForeignKey("groups.id", ondelete="CASCADE"), primary_key=True)
    asset_id = Column(String, ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True)
    source = Column(String, nullable=True)    # optional override
    exchange = Column(String, nullable=True)
    mic = Column(String, nullable=True)
    position = Column(Integer, nullable=True)

    group = relationship("Group", back_populates="members")
    asset = relationship("Asset")

# Zusätzliche sinnvolle Indizes
Index("idx_assets_primary_category", Asset.primary_category)
Index("idx_assets_status", Asset.status)
Index("idx_assets_type", Asset.type)
Index("idx_asset_name", Asset.name)

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

class IdentifierIn(BaseModel):
    key: str
    value: str

class IdentifierOut(IdentifierIn):
    pass

class AssetIn(BaseModel):
    id: str = Field(..., description='z. B. "asset:msft" (slug) oder UUID')
    type: str
    name: Optional[str] = None
    country: Optional[str] = None
    sector: Optional[str] = None
    primary_category: str
    status: str = "active"
    listings: List[ListingIn] = []
    tags: List[str] = []
    identifiers: List[IdentifierIn] = []

class AssetOut(BaseModel):
    id: str
    type: str
    name: Optional[str]
    country: Optional[str]
    sector: Optional[str]
    primary_category: str
    status: str
    created_ts: datetime
    updated_ts: datetime
    listings: List[ListingOut]
    tags: List[str]
    identifiers: Dict[str, str]

class AssetPatch(BaseModel):
    type: Optional[str] = None
    name: Optional[str] = None
    country: Optional[str] = None
    sector: Optional[str] = None
    primary_category: Optional[str] = None
    status: Optional[str] = None

class GroupIn(BaseModel):
    id: str
    name: str
    default_source: Optional[str] = None

class GroupOut(BaseModel):
    id: str
    name: str
    default_source: Optional[str]
    created_ts: datetime
    updated_ts: datetime
    members: List[Dict[str, Any]]

class GroupMemberIn(BaseModel):
    asset_id: str
    source: Optional[str] = None
    exchange: Optional[str] = None
    mic: Optional[str] = None
    position: Optional[int] = None

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Registry/GroupManager API", version="1.0.0")
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
        type=a.type,
        name=a.name,
        country=a.country,
        sector=a.sector,
        primary_category=a.primary_category,
        status=a.status,
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
    """
    Resolver: findet das passende Listing & Symbol.
    Strategie:
      1) Wenn source+mic -> exakter Treffer
      2) Wenn source+exchange -> exakter Treffer
      3) Wenn nur source -> erstes Listing dieser Quelle
      4) Sonst: erstes Listing überhaupt
    """
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail=f"Asset not found: {asset_id}")

    src = _norm(source)
    ex = _norm(exchange)
    mi = _norm(mic)

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
    type: Optional[str] = Query(None),
    primary_category: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    tag: Optional[str] = Query(None, description="Filter auf einen Tag"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    qry = db.query(Asset)
    if q:
        like = f"%{q}%"
        qry = qry.filter((Asset.name.ilike(like)) | (Asset.id.ilike(like)))
    if type:
        qry = qry.filter(Asset.type == type)
    if primary_category:
        qry = qry.filter(Asset.primary_category == primary_category)
    if status:
        qry = qry.filter(Asset.status == status)
    if tag:
        qry = qry.join(Asset.tags).filter(AssetTag.tag == tag)
    items = qry.order_by(Asset.id).offset(offset).limit(limit).all()
    log.debug(f"[ASSETS][LIST] n={len(items)} q={q} type={type} cat={primary_category} tag={tag}")
    return [_asset_to_out(a) for a in items]


@app.post("/assets", response_model=AssetOut, status_code=201)
def create_asset(payload: AssetIn, db: Session = Depends(get_db)):
    if db.query(Asset).filter(Asset.id == payload.id).first():
        raise HTTPException(status_code=409, detail="Asset id exists")
    a = Asset(
        id=payload.id,
        type=payload.type,
        name=payload.name,
        country=payload.country,
        sector=payload.sector,
        primary_category=payload.primary_category,
        status=payload.status or "active",
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

    db.flush()
    log.info(f"[ASSETS][CREATE] id={a.id} listings={len(a.listings)} tags={len(a.tags)}")
    db.refresh(a)
    return _asset_to_out(a)


@app.get("/assets/{asset_id}", response_model=AssetOut)
def get_asset(asset_id: str = FPath(...), db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    return _asset_to_out(a)


@app.patch("/assets/{asset_id}", response_model=AssetOut)
def patch_asset(asset_id: str, payload: AssetPatch, db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    updated = []
    for field in ("type", "name", "country", "sector", "primary_category", "status"):
        val = getattr(payload, field)
        if val is not None:
            setattr(a, field, val)
            updated.append(field)
    log.info(f"[ASSETS][PATCH] id={asset_id} fields={updated}")
    db.flush()
    db.refresh(a)
    return _asset_to_out(a)


@app.delete("/assets/{asset_id}", status_code=204)
def delete_asset(asset_id: str, db: Session = Depends(get_db)):
    a = db.query(Asset).filter(Asset.id == asset_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(a)
    log.info(f"[ASSETS][DELETE] id={asset_id}")
    return

# ─ Listings CRUD ─
@app.post("/assets/{asset_id}/listings", response_model=ListingOut, status_code=201)
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
        raise HTTPException(status_code=409, detail="Listing duplicate (source/symbol/mic)") from e
    db.refresh(l)
    log.info(f"[LISTINGS][ADD] asset={asset_id} -> {payload.source}:{payload.symbol} {payload.mic or payload.exchange or ''}")
    return ListingOut(**{**payload.dict(), "id": l.id})

@app.delete("/listings/{listing_id}", status_code=204)
def delete_listing(listing_id: int, db: Session = Depends(get_db)):
    l = db.query(Listing).filter(Listing.id == listing_id).first()
    if not l:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(l)
    log.info(f"[LISTINGS][DELETE] id={listing_id}")
    return

# ─ Tags CRUD ─
@app.get("/tags", response_model=List[str])
def list_tags(db: Session = Depends(get_db)):
    return [t.tag for t in db.query(Tag).order_by(Tag.tag).all()]

@app.post("/assets/{asset_id}/tags/{tag}", status_code=204)
def add_tag(asset_id: str, tag: str, db: Session = Depends(get_db)):
    if not db.query(Asset).filter(Asset.id == asset_id).first():
        raise HTTPException(status_code=404, detail="Asset not found")
    if not db.query(Tag).filter(Tag.tag == tag).first():
        db.add(Tag(tag=tag))
    if not db.query(AssetTag).filter(AssetTag.asset_id == asset_id, AssetTag.tag == tag).first():
        db.add(AssetTag(asset_id=asset_id, tag=tag))
        log.info(f"[TAGS][ADD] asset={asset_id} tag={tag}")
    return

@app.delete("/assets/{asset_id}/tags/{tag}", status_code=204)
def remove_tag(asset_id: str, tag: str, db: Session = Depends(get_db)):
    at = db.query(AssetTag).filter(AssetTag.asset_id == asset_id, AssetTag.tag == tag).first()
    if not at:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(at)
    log.info(f"[TAGS][DEL] asset={asset_id} tag={tag}")
    return

# ─ Identifiers CRUD ─
@app.post("/assets/{asset_id}/identifiers", status_code=204)
def upsert_identifier(asset_id: str, payload: IdentifierIn, db: Session = Depends(get_db)):
    if not db.query(Asset).filter(Asset.id == asset_id).first():
        raise HTTPException(status_code=404, detail="Asset not found")
    idt = db.query(Identifier).filter(Identifier.asset_id == asset_id, Identifier.key == payload.key).first()
    if idt:
        idt.value = payload.value
        log.info(f"[IDENT][UPD] asset={asset_id} {payload.key}={payload.value}")
    else:
        db.add(Identifier(asset_id=asset_id, key=payload.key, value=payload.value))
        log.info(f"[IDENT][ADD] asset={asset_id} {payload.key}={payload.value}")
    return

@app.delete("/assets/{asset_id}/identifiers/{key}", status_code=204)
def delete_identifier(asset_id: str, key: str, db: Session = Depends(get_db)):
    idt = db.query(Identifier).filter(Identifier.asset_id == asset_id, Identifier.key == key).first()
    if not idt:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(idt)
    log.info(f"[IDENT][DEL] asset={asset_id} {key}")
    return

# ──────────────────────────────────────────────────────────────────────────────
# Endpunkte: Groups
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/groups", response_model=List[GroupOut])
def list_groups(db: Session = Depends(get_db)):
    gs = db.query(Group).order_by(Group.id).all()
    out: List[GroupOut] = []
    for g in gs:
        out.append(GroupOut(
            id=g.id, name=g.name, default_source=g.default_source,
            created_ts=g.created_ts, updated_ts=g.updated_ts,
            members=[{
                "asset_id": m.asset_id, "source": m.source, "exchange": m.exchange,
                "mic": m.mic, "position": m.position
            } for m in sorted(g.members, key=lambda x: (x.position or 0, x.asset_id))]
        ))
    return out

@app.post("/groups", response_model=GroupOut, status_code=201)
def create_group(payload: GroupIn, db: Session = Depends(get_db)):
    if db.query(Group).filter(Group.id == payload.id).first():
        raise HTTPException(status_code=409, detail="Group id exists")
    g = Group(id=payload.id, name=payload.name, default_source=payload.default_source)
    db.add(g)
    db.flush()
    db.refresh(g)
    log.info(f"[GROUP][CREATE] id={g.id}")
    return list_groups(db=db)[-1] if list_groups(db=db) else GroupOut(
        id=g.id, name=g.name, default_source=g.default_source, created_ts=g.created_ts,
        updated_ts=g.updated_ts, members=[]
    )

@app.get("/groups/{group_id}", response_model=GroupOut)
def get_group(group_id: str, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Not found")
    return list_groups(db=db)[[x.id for x in db.query(Group).order_by(Group.id).all()].index(group_id)]

@app.post("/groups/{group_id}/members", status_code=204)
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
        exchange=member.exchange, mic=member.mic, position=member.position
    ))
    log.info(f"[GROUP][ADD] group={group_id} asset={member.asset_id}")
    return

@app.delete("/groups/{group_id}/members/{asset_id}", status_code=204)
def remove_group_member(group_id: str, asset_id: str, db: Session = Depends(get_db)):
    m = db.query(GroupMember).filter(GroupMember.group_id == group_id, GroupMember.asset_id == asset_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(m)
    log.info(f"[GROUP][DEL] group={group_id} asset={asset_id}")
    return

@app.patch("/groups/{group_id}", status_code=204)
def patch_group(group_id: str, payload: GroupIn, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Not found")
    g.name = payload.name or g.name
    g.default_source = payload.default_source if payload.default_source is not None else g.default_source
    log.info(f"[GROUP][PATCH] id={group_id}")
    return

@app.delete("/groups/{group_id}", status_code=204)
def delete_group(group_id: str, db: Session = Depends(get_db)):
    g = db.query(Group).filter(Group.id == group_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(g)
    log.info(f"[GROUP][DELETE] id={group_id}")
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
    q: str = Query(..., description="LIKE-Suche über asset.name, listing.symbol, identifiers"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    like = f"%{q}%"
    # Einfacher Ansatz: erst Asset-Namen, dann Listings, dann Identifiers
    assets = db.query(Asset).filter(Asset.name.ilike(like)).limit(limit).all()
    listings = db.query(Listing).filter(Listing.symbol.ilike(like)).limit(limit).all()
    idents = db.query(Identifier).filter(Identifier.value.ilike(like)).limit(limit).all()

    out: Dict[str, Any] = {
        "assets": [a.id for a in assets],
        "listings": [{"asset_id": l.asset_id, "source": l.source, "symbol": l.symbol, "mic": l.mic, "exchange": l.exchange} for l in listings],
        "identifiers": [{"asset_id": i.asset_id, "key": i.key, "value": i.value} for i in idents],
    }
    log.debug(f"[SEARCH] q={q} -> a={len(assets)} l={len(listings)} i={len(idents)}")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "ok": True,
        "db_url": DB_URL,
        "db_path": cfg.REGISTRY_DB if IS_SQLITE else None,
        "engine": "sqlite-wal" if IS_SQLITE else "sqlalchemy",
        "time": datetime.utcnow().isoformat() + "Z",
        "version": "1.0.0"
    }
