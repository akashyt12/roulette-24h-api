#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import time
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, Response

UPSTREAM = "https://api-cs.casino.org/svc-evolution-game-events/api/autoroulette"
POLL_SECONDS = max(5.0, float(os.environ.get("POLL_SECONDS", "30")))
MAX_STALE = max(5.0, float(os.environ.get("MAX_STALE", "60")))
FETCH_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "20"))
CSV_FILE = Path(os.environ.get("ROULETTE_CSV_FILE", str(Path(__file__).with_name("roulette_24h.csv"))))

ZONES = {
    "Z1": {0, 32, 15, 19, 4, 21, 2, 25, 17},
    "Z2": {9, 22, 18, 29, 7, 28, 12, 35, 3, 26},
    "Z3": {10, 5, 24, 16, 33, 1, 20, 14, 31},
    "Z4": {34, 6, 27, 13, 36, 11, 30, 8, 23},
}
RED_NUMBERS = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
FIELDS = ["id", "settled_at", "number", "colour", "parity", "zone", "column", "dozen"]

app = FastAPI(title="Roulette 24H Collector API", docs_url=None, redoc_url=None)
_spins: list[dict[str, Any]] = []
_meta: dict[str, Any] = {
    "last_fetch": 0.0,
    "fetch_ms": None,
    "error": None,
    "total_header": None,
    "poll_count": 0,
}
_lock = asyncio.Lock()


def _zone_of(number: int) -> str:
    for name, nums in ZONES.items():
        if number in nums:
            return name
    return "?"


def _column_of(number: int) -> str:
    if number == 0:
        return "C0"
    return f"C{((number - 1) % 3) + 1}"


def _dozen_of(number: int) -> str:
    if number == 0:
        return "D0"
    return f"D{((number - 1) // 12) + 1}"


def _normalize(item: dict[str, Any]) -> dict[str, Any]:
    data = item.get("data") if isinstance(item.get("data"), dict) else {}
    outcome = data.get("result", {}).get("outcome", {}) if isinstance(data.get("result"), dict) else {}
    number = int(outcome.get("number", -1))
    colour = str(outcome.get("color") or ("GREEN" if number == 0 else "RED" if number in RED_NUMBERS else "BLACK")).upper()
    parity = str(outcome.get("type") or "-").upper()
    return {
        "id": str(item.get("id", "")),
        "settled_at": str(data.get("settledAt", "")),
        "number": number,
        "colour": colour,
        "parity": parity,
        "zone": _zone_of(number),
        "column": _column_of(number),
        "dozen": _dozen_of(number),
    }


def _fetch_all() -> tuple[list[dict[str, Any]], str | None]:
    req = urllib.request.Request(
        f"{UPSTREAM}?size=5000",
        headers={"User-Agent": "roulette-24h-collector/1.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as response:
        raw = response.read().decode("utf-8")
        total_header = response.headers.get("X-Total-Count")
    data = json.loads(raw)
    if not isinstance(data, list):
        raise RuntimeError("unexpected upstream response")
    return [_normalize(item) for item in data if isinstance(item, dict)], total_header


def _append_csv(spins: list[dict[str, Any]]) -> None:
    if not spins:
        return
    try:
        existed = CSV_FILE.exists()
        known: set[str] = set()
        if existed:
            with CSV_FILE.open("r", newline="", encoding="utf-8") as handle:
                known = {row.get("id", "") for row in csv.DictReader(handle)}
        fresh = [spin for spin in spins if spin["id"] not in known]
        if not fresh:
            return
        with CSV_FILE.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            if not existed or handle.tell() == 0:
                writer.writeheader()
            for spin in reversed(fresh):
                writer.writerow({key: spin[key] for key in FIELDS})
    except Exception:
        pass


async def _refresh() -> dict[str, Any]:
    async with _lock:
        started = time.perf_counter()
        spins, total_header = await asyncio.get_running_loop().run_in_executor(None, _fetch_all)
        _spins[:] = spins
        await asyncio.get_running_loop().run_in_executor(None, _append_csv, spins)
        _meta["last_fetch"] = time.time()
        _meta["fetch_ms"] = round((time.perf_counter() - started) * 1000, 1)
        _meta["error"] = None
        _meta["total_header"] = total_header
        _meta["poll_count"] += 1
    return _summary()


def _summary() -> dict[str, Any]:
    return {
        "total": len(_spins),
        "oldest": _spins[-1]["settled_at"] if _spins else None,
        "newest": _spins[0]["settled_at"] if _spins else None,
        "last_fetch": _meta["last_fetch"],
        "fetch_ms": _meta["fetch_ms"],
        "poll_count": _meta["poll_count"],
        "total_header": _meta["total_header"],
        "error": _meta["error"],
    }


def _distribution(key: str, spins: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    total = len(spins) or 1
    return {
        name: {"count": count, "pct": round(count * 100 / total, 1)}
        for name, count in Counter(spin[key] for spin in spins).most_common()
    }


def _stats(spins: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total": len(spins),
        "oldest": spins[-1]["settled_at"] if spins else None,
        "newest": spins[0]["settled_at"] if spins else None,
        "colour": _distribution("colour", spins),
        "parity": _distribution("parity", spins),
        "zone": _distribution("zone", spins),
        "column": _distribution("column", spins),
        "dozen": _distribution("dozen", spins),
        "top_numbers": [
            {"number": number, "count": count}
            for number, count in Counter(spin["number"] for spin in spins).most_common(10)
        ],
    }


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def _ensure_fresh() -> None:
    stale = not _spins or (time.time() - _meta["last_fetch"]) > MAX_STALE
    if stale:
        try:
            await _refresh()
        except Exception as exc:
            _meta["error"] = str(exc)[:200]


async def _poller() -> None:
    while True:
        started = time.perf_counter()
        try:
            await _refresh()
        except Exception as exc:
            _meta["error"] = str(exc)[:200]
        await asyncio.sleep(max(1.0, POLL_SECONDS - (time.perf_counter() - started)))


@app.on_event("startup")
async def _startup() -> None:
    app.state.poller = asyncio.create_task(_poller())


@app.on_event("shutdown")
async def _shutdown() -> None:
    task = getattr(app.state, "poller", None)
    if task:
        task.cancel()


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "Roulette 24H Collector API",
        "endpoints": ["/health", "/count", "/stats", "/latest", "/history", "/range", "/export.csv", "/refresh"],
        "poll_seconds": POLL_SECONDS,
        "max_stale_seconds": MAX_STALE,
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok" if _spins else "starting", **_summary()}


@app.get("/count")
async def count() -> dict[str, Any]:
    await _ensure_fresh()
    return _summary()


@app.get("/stats")
async def stats() -> dict[str, Any]:
    await _ensure_fresh()
    return _stats(_spins)


@app.get("/latest")
async def latest(n: int = Query(10, ge=1, le=500)) -> dict[str, Any]:
    await _ensure_fresh()
    return {"count": min(n, len(_spins)), "data": _spins[:n]}


@app.get("/history")
async def history(
    limit: int = Query(100, ge=1, le=1954),
    offset: int = Query(0, ge=0),
    order: str = Query("desc", pattern="^(asc|desc)$"),
) -> dict[str, Any]:
    await _ensure_fresh()
    rows = _spins if order == "desc" else list(reversed(_spins))
    return {"total": len(rows), "limit": limit, "offset": offset, "order": order, "data": rows[offset:offset + limit]}


@app.get("/range")
async def date_range(
    start: str | None = Query(None, alias="from"),
    end: str | None = Query(None, alias="to"),
    limit: int = Query(500, ge=1, le=1954),
) -> dict[str, Any]:
    await _ensure_fresh()
    rows = _spins
    if start:
        begin = _parse_ts(start)
        rows = [spin for spin in rows if _parse_ts(spin["settled_at"]) >= begin]
    if end:
        finish = _parse_ts(end)
        rows = [spin for spin in rows if _parse_ts(spin["settled_at"]) <= finish]
    return {"total": len(rows), "limit": limit, "from": start, "to": end, "data": rows[:limit]}


@app.get("/export.csv")
async def export_csv() -> Response:
    await _ensure_fresh()
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=FIELDS)
    writer.writeheader()
    for spin in _spins:
        writer.writerow({key: spin[key] for key in FIELDS})
    return Response(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=roulette_24h.csv"},
    )


@app.post("/refresh")
async def refresh() -> Any:
    try:
        return await _refresh()
    except Exception as exc:
        return JSONResponse(status_code=502, content={"error": "refresh failed", "detail": str(exc)[:200]})
