# notifier_evaluator/fetch/types.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from notifier_evaluator.models.runtime import FetchResult, ResolvedContext, safe_float


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
        # Keep it readable, but include enough to disambiguate.
        # params_json can be huge -> show length only.
        pj_len = len(self.params_json or "")
        return (
            f"{self.indicator} {self.symbol} {self.interval} {self.exchange} "
            f"out={self.output} c={self.count} m={self.mode} asof={self.as_of or '-'} pjson={pj_len}"
        )

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
        ind = (str(indicator).strip() if indicator is not None else "").strip()
        sym = (str(ctx.symbol).strip() if ctx.symbol is not None else "").strip()
        itv = (str(ctx.interval).strip() if ctx.interval is not None else "").strip()
        ex = (str(ctx.exchange).strip() if ctx.exchange is not None else "").strip()

        out = (str(output).strip() if output is not None else "").strip()  # allow empty output
        cnt = int(count) if count is not None else 1
        if cnt <= 0:
            print(f"[fetch.types] WARN count<=0 for indicator={ind} -> forcing count=1")
            cnt = 1

        m = (str(mode).strip() if mode is not None else "").strip().lower() or "latest"
        if m not in ("latest", "as_of"):
            print(f"[fetch.types] WARN unknown mode='{m}' -> default latest")
            m = "latest"

        ao = (str(as_of).strip() if as_of is not None else None)
        if m == "as_of" and not ao:
            print(f"[fetch.types] WARN mode=as_of but as_of missing -> keeping as_of=None (server decides)")
            ao = None

        params_json = stable_json(params or {})

        # Loud sanity debug (you WANT this while developing)
        print(
            "[fetch.types] build_key ind=%s sym=%s itv=%s ex=%s out=%s cnt=%s mode=%s asof=%s params_len=%d"
            % (ind, sym, itv, ex, out, cnt, m, (ao or "-"), len(params_json))
        )

        return RequestKey(
            indicator=ind,
            symbol=sym,
            interval=itv,
            exchange=ex,
            params_json=params_json,
            output=out,
            count=cnt,
            mode=m,
            as_of=ao,
        )

    def params(self) -> Dict[str, Any]:
        try:
            return json.loads(self.params_json or "{}")
        except Exception as e:
            print(f"[fetch.types] WARN params_json decode failed err={e} json_len={len(self.params_json or '')}")
            return {}


def stable_json(obj: Dict[str, Any]) -> str:
    """
    Stable JSON for hashing/deduping.
    - sort keys
    - compact separators
    """
    try:
        return json.dumps(obj or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception as e:
        # if unserializable: fallback to string-ified dict (still stable-ish)
        print(f"[fetch.types] WARN stable_json unserializable err={e} -> fallback __raw__")
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
            return FetchResult(ok=False, latest_value=None, latest_ts=None, error="payload_none", meta={"key": key.short()})

        # If API wraps errors in a dict, try to surface it.
        # IMPORTANT: don't blindly fail if rows/data exist (some APIs include "error" fields alongside data).
        if isinstance(payload, dict):
            if payload.get("ok") is False:
                err = payload.get("error") or payload.get("message") or "api_ok_false"
                return FetchResult(ok=False, latest_value=None, latest_ts=None, error=str(err), meta={"key": key.short(), "shape": "dict_ok_false"})

            http = payload.get("_http") if isinstance(payload.get("_http"), dict) else {}
            status_code = http.get("status_code")
            has_rows = isinstance(payload.get("rows"), list) and len(payload.get("rows") or []) > 0
            has_data = any(isinstance(payload.get(c), list) and len(payload.get(c) or []) > 0 for c in ("data", "series", "values"))

            if "error" in payload and payload.get("error"):
                # If there is usable data, ignore error and continue normalization.
                if has_rows or has_data:
                    print("[fetch.types] WARN payload.error present but data exists -> ignoring error key=%s err=%s" % (key.short(), str(payload.get("error"))[:200]))
                else:
                    err = payload.get("error")
                    # If HTTP status indicates error, treat as fail.
                    if isinstance(status_code, int) and status_code >= 400:
                        return FetchResult(ok=False, latest_value=None, latest_ts=None, error=str(err), meta={"key": key.short(), "shape": "dict_error_http"})
                    return FetchResult(ok=False, latest_value=None, latest_ts=None, error=str(err), meta={"key": key.short(), "shape": "dict_error"})

        # shape A: {"rows": [...], "columns": [...], ...}
        if isinstance(payload, dict) and "rows" in payload and isinstance(payload.get("rows"), list):
            rows = payload.get("rows") or []
            cols = payload.get("columns")
            series = _normalize_rows(rows, cols)
            series = _maybe_sort_series(series)
            return _from_series(series, key=key, meta={"shape": "dict_rows"})

        # shape B: list[dict] rows
        if isinstance(payload, list):
            series = _normalize_rows(payload, None)
            series = _maybe_sort_series(series)
            return _from_series(series, key=key, meta={"shape": "list_rows"})

        # shape C: dict but no "rows" - maybe {"data": [...]} or {"series": [...]}
        if isinstance(payload, dict):
            for cand in ("data", "series", "values"):
                if cand in payload and isinstance(payload.get(cand), list):
                    series = _normalize_rows(payload.get(cand), payload.get("columns"))
                    series = _maybe_sort_series(series)
                    return _from_series(series, key=key, meta={"shape": f"dict_{cand}"})

            # maybe it’s a single point dict -> treat as one-row series
            series = _normalize_rows([payload], None)
            series = _maybe_sort_series(series)
            return _from_series(series, key=key, meta={"shape": "dict_single"})

        # unknown
        return FetchResult(
            ok=False,
            latest_value=None,
            latest_ts=None,
            error=f"unknown_payload_type:{type(payload)}",
            meta={"key": key.short()},
        )

    except Exception as e:
        return FetchResult(ok=False, latest_value=None, latest_ts=None, error=f"normalize_exc:{e}", meta={"key": key.short()})


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
            d: Dict[str, Any] = {}
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


def _extract_ts_from_row(row: Dict[str, Any]) -> Optional[str]:
    for k in ("timestamp", "ts", "time", "date", "datetime"):
        if k in row and row.get(k) is not None:
            try:
                return str(row.get(k))
            except Exception:
                return None
    return None


def _parse_ts_best_effort(ts: str) -> Optional[float]:
    """
    Best-effort parse to epoch for sorting.
    Supports:
      - epoch seconds (int/str)
      - epoch ms (heuristic)
      - ISO-ish strings incl. "Z"
      - "YYYY-MM-DD HH:MM:SS" (space instead of "T")
    If parsing fails, return None.
    """
    s = (ts or "").strip()
    if not s:
        return None

    # numeric epoch string?
    try:
        # handle negatives or floats
        if s.replace(".", "", 1).isdigit() or (s.startswith("-") and s[1:].replace(".", "", 1).isdigit()):
            n = float(s)
            # heuristic: ms if huge
            if n > 10_000_000_000:
                return n / 1000.0
            return n
    except Exception:
        pass

    # ISO-ish / datetime-ish
    try:
        s2 = s
        # common Z suffix
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        # "YYYY-MM-DD HH:MM:SS" -> fromisoformat accepts this in many cases, but be explicit
        # keep as is; fromisoformat can parse " " and "T" variants.
        dt = datetime.fromisoformat(s2)
        return dt.timestamp()
    except Exception:
        return None


def _maybe_sort_series(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    If we can parse timestamps in the series, sort by ts ascending.
    Otherwise keep original order (API order).
    """
    if not series:
        return series

    # find ts for a few rows; if none parseable, skip sorting
    parsed: List[Tuple[float, int]] = []
    for i, row in enumerate(series[:50]):  # don't scan huge lists
        ts = _extract_ts_from_row(row)
        if not ts:
            continue
        ep = _parse_ts_best_effort(ts)
        if ep is None:
            continue
        parsed.append((ep, i))

    if not parsed:
        return series

    def _k(idx_row: Tuple[int, Dict[str, Any]]) -> Tuple[int, float, int]:
        idx, row = idx_row
        ts = _extract_ts_from_row(row)
        ep = _parse_ts_best_effort(ts) if ts else None
        if ep is None:
            return (1, 0.0, idx)  # unknowns at end, stable
        return (0, float(ep), idx)

    sorted_series = [row for _, row in sorted(list(enumerate(series)), key=_k)]
    return sorted_series


def _pick_latest_row(series: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Pick the best "latest" row.
    - If timestamps parseable: pick max epoch ts
    - else: fallback to last row (API order)
    """
    best_row: Optional[Dict[str, Any]] = None
    best_ep: Optional[float] = None

    for row in series or []:
        ts = _extract_ts_from_row(row)
        ep = _parse_ts_best_effort(ts) if ts else None
        if ep is None:
            continue
        if best_ep is None or ep > best_ep:
            best_ep = ep
            best_row = row

    if best_row is not None:
        print("[fetch.types] latest_row chosen_by_ts epoch=%s" % best_ep)
        return best_row

    return (series[-1] if series else {}) or {}


def _from_series(series: List[Dict[str, Any]], *, key: RequestKey, meta: Dict[str, Any]) -> FetchResult:
    if not series:
        return FetchResult(ok=False, latest_value=None, latest_ts=None, error="empty_series", meta={"key": key.short(), **meta})

    # pick best latest row (not blindly last)
    last = _pick_latest_row(series) or {}

    # timestamp candidates
    ts = _extract_ts_from_row(last)

    # value candidates:
    # if output provided, prefer that
    val = None
    if key.output:
        try:
            val = safe_float(last.get(key.output))
        except Exception:
            val = None

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
