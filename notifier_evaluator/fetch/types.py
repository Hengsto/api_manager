# notifier_evaluator/fetch/types.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from notifier_evaluator.models.runtime import ResolvedContext, FetchResult, safe_float


# ──────────────────────────────────────────────────────────────────────────────
# Fetch Types
# - RequestKey: dedupe-key für (indicator, symbol, interval, exchange, params, output, count, mode, as_of)
# - Normalisierung von Responses (price_api /indicator)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RequestKey:
    """
    Unique request identity for deduping.
    mode:
      - "latest": always get most recent
      - "as_of": request as-of a timestamp (for backfills / deterministic runs)

    as_of:
      Optional timestamp string (ISO) if mode == "as_of"
    """
    indicator: str
    symbol: str
    interval: str
    exchange: str
    params_json: str
    output: str
    count: int
    mode: str = "latest"
    as_of: Optional[str] = None

    def short(self) -> str:
        return f"{self.indicator} {self.symbol} {self.interval} {self.output} c={self.count} m={self.mode}"

    @staticmethod
    def from_parts(
        *,
        indicator: str,
        ctx: ResolvedContext,
        params: Dict[str, Any],
        output: Optional[str],
        count: int,
        mode: str = "latest",
        as_of: Optional[str] = None,
    ) -> "RequestKey":
        params_json = stable_json(params or {})
        out = (output or "").strip() or ""  # allow empty output; client may return single-column
        return RequestKey(
            indicator=str(indicator).strip(),
            symbol=ctx.symbol,
            interval=ctx.interval,
            exchange=ctx.exchange,
            params_json=params_json,
            output=out,
            count=int(count),
            mode=str(mode).strip() or "latest",
            as_of=as_of,
        )

    def params(self) -> Dict[str, Any]:
        try:
            return json.loads(self.params_json or "{}")
        except Exception:
            return {}


def stable_json(obj: Dict[str, Any]) -> str:
    """
    Stable JSON for hashing/deduping.
    - sort keys
    - compact separators
    """
    try:
        return json.dumps(obj or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        # if unserializable: fallback to string-ified dict (still stable-ish)
        return json.dumps({"__raw__": str(obj)}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ──────────────────────────────────────────────────────────────────────────────
# Response normalization helpers
# ──────────────────────────────────────────────────────────────────────────────

def normalize_indicator_response(
    payload: Any,
    *,
    key: RequestKey,
) -> FetchResult:
    """
    Normalizes the response from price_api /indicator into FetchResult.

    We accept a few common shapes:
      A) dict with {"rows": [...], "columns": [...], ...}
      B) list of rows (list[dict])
      C) dict that already contains series-like data

    We try to extract:
      - latest_value (float)
      - latest_ts (string)
      - series (optional list[dict])
    """
    try:
        if payload is None:
            return FetchResult(ok=False, error="payload_none", meta={"key": key.short()})

        # shape A: {"rows": [...], "columns": [...], ...}
        if isinstance(payload, dict) and "rows" in payload and isinstance(payload.get("rows"), list):
            rows = payload.get("rows") or []
            cols = payload.get("columns")
            # some APIs return rows already as dict; some as list aligned with columns
            series = _normalize_rows(rows, cols)
            return _from_series(series, key=key, meta={"shape": "dict_rows"})

        # shape B: list[dict] rows
        if isinstance(payload, list):
            series = _normalize_rows(payload, None)
            return _from_series(series, key=key, meta={"shape": "list_rows"})

        # shape C: dict but no "rows" - maybe {"data": [...]} or {"series": [...]}
        if isinstance(payload, dict):
            for cand in ("data", "series", "values"):
                if cand in payload and isinstance(payload.get(cand), list):
                    series = _normalize_rows(payload.get(cand), payload.get("columns"))
                    return _from_series(series, key=key, meta={"shape": f"dict_{cand}"})

            # maybe it’s a single point dict
            # try to treat as one-row series
            series = _normalize_rows([payload], None)
            return _from_series(series, key=key, meta={"shape": "dict_single"})

        # unknown
        return FetchResult(ok=False, error=f"unknown_payload_type:{type(payload)}", meta={"key": key.short()})

    except Exception as e:
        return FetchResult(ok=False, error=f"normalize_exc:{e}", meta={"key": key.short()})


def _normalize_rows(rows: List[Any], columns: Optional[List[str]]) -> List[Dict[str, Any]]:
    """
    Convert rows to list[dict].
    - If row is dict: keep it.
    - If row is list/tuple and columns provided: map zip(columns,row)
    """
    out: List[Dict[str, Any]] = []
    cols = columns if isinstance(columns, list) else None

    for r in rows or []:
        if r is None:
            continue
        if isinstance(r, dict):
            out.append(r)
            continue
        if isinstance(r, (list, tuple)) and cols:
            d = {}
            for i, c in enumerate(cols):
                try:
                    d[str(c)] = r[i]
                except Exception:
                    d[str(c)] = None
            out.append(d)
            continue

        # fallback: string/number etc.
        out.append({"value": r})

    return out


def _from_series(series: List[Dict[str, Any]], *, key: RequestKey, meta: Dict[str, Any]) -> FetchResult:
    if not series:
        return FetchResult(ok=False, error="empty_series", meta={"key": key.short(), **meta})

    # pick last row as latest
    last = series[-1] or {}

    # timestamp candidates
    ts = None
    for k in ("timestamp", "ts", "time", "date", "datetime"):
        if k in last and last.get(k) is not None:
            ts = str(last.get(k))
            break

    # value candidates:
    # if output provided, prefer that
    val = None
    if key.output:
        val = safe_float(last.get(key.output))
    if val is None:
        # try common fields
        for k in ("value", "close", "Close", "price", "Price"):
            if k in last:
                val = safe_float(last.get(k))
                if val is not None:
                    break

    ok = val is not None
    fr = FetchResult(
        ok=ok,
        latest_value=val,
        latest_ts=ts,
        series=series if key.count and key.count > 1 else None,
        error=None if ok else "missing_value",
        meta={"key": key.short(), "output": key.output, **meta},
    )

    # Debug print (noisy by design)
    print(
        "[fetch.types] normalized ok=%s key=%s ts=%s val=%s series_len=%s"
        % (fr.ok, key.short(), fr.latest_ts, fr.latest_value, (len(series) if series else 0))
    )
    return fr
