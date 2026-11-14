"""FastAPI entry point: start / monitor / stop a single collection round."""

from __future__ import annotations

import os
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from .crawler import Crawler

BASE_URL = os.getenv("TARGET_BASE_URL", "http://procurement-site:8080")
SECTIONS = {
    "招标公告": os.getenv("SECTION_TENDER_PATH", "/tenders"),
    "中标公告": os.getenv("SECTION_AWARD_PATH", "/awards"),
}

app = FastAPI(title="Procurement Announcement Collector", version="1.0.0")
crawler = Crawler(base_url=BASE_URL, sections=SECTIONS)


def _summary() -> dict:
    """Round snapshot with the remainder split into two disjoint reports.

    ``failed_pages`` are pages that were actually requested and did not
    succeed; each keeps its page number/code and attempt count.
    ``unrequested_pages`` are pages that were never requested, reported as
    contiguous ranges plus a total count.  ``success`` is only true for a
    completed round, never for a stopped one.
    """
    state = crawler.snapshot()
    return {
        "round_id": state["round_id"],
        "status": state["status"],
        "success": state["success"],
        "base_url": state["base_url"],
        "started_at": state["started_at"],
        "finished_at": state["finished_at"],
        "stop_requested": state["stop_requested"],
        "cooldown_remaining_seconds": state["cooldown_remaining_seconds"],
        "pages_enqueued": state["pages_enqueued"],
        "pages_requested": state["pages_requested"],
        "pages_completed": state["pages_completed"],
        "pages_failed": state["pages_failed"],
        "records_count": state["records_count"],
        "sections": state["sections"],
        "failed_pages": state["failed_pages"],
        "unrequested_pages": state["unrequested_pages"],
        "unrequested_count": state["unrequested_count"],
        "unrequested_count_incomplete": state["unrequested_count_incomplete"],
        "events": state["events"],
    }


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/rounds")
async def start_round() -> dict:
    try:
        round_id = await crawler.start_round()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"round_id": round_id, "state": crawler.snapshot()}


@app.post("/api/rounds/stop")
async def stop_round() -> dict:
    # stop_round only returns once it has accepted the stop, aborted in-flight
    # requests, recycled the connection pool and wound the round down.
    await crawler.stop_round()
    return {"state": crawler.snapshot()}


@app.get("/api/state")
async def get_state() -> dict:
    # Keeps the complete round snapshot, including both remainder reports.
    return crawler.snapshot()


@app.get("/api/rounds/current")
async def current_round() -> dict:
    return _summary()


@app.get("/api/records")
async def get_records() -> dict:
    state = crawler.snapshot()
    return {
        "round_id": state["round_id"],
        "status": state["status"],
        "success": state["success"],
        "count": len(crawler.records()),
        "records": crawler.records(),
    }


@app.get("/api/records/download")
async def download_records():
    state = crawler.snapshot()
    if state["round_id"] is None:
        raise HTTPException(status_code=404, detail="no collection round has run yet")
    payload = {
        "round_id": state["round_id"],
        "status": state["status"],
        # A stopped round is never a successful collection, even if some pages
        # happened to finish before the stop was accepted.
        "success": state["success"],
        "base_url": state["base_url"],
        "started_at": state["started_at"],
        "finished_at": state["finished_at"],
        "stop_requested": state["stop_requested"],
        "records_count": len(crawler.records()),
        # Requested but unsuccessful: page number / code and attempts kept.
        "failed_pages": state["failed_pages"],
        # Never requested: contiguous locatable ranges plus total count.
        "unrequested_pages": state["unrequested_pages"],
        "unrequested_count": state["unrequested_count"],
        "unrequested_count_incomplete": state["unrequested_count_incomplete"],
        "records": crawler.records(),
    }
    headers = {
        "Content-Disposition": f'attachment; filename="procurement-{state["round_id"]}.json"'
    }
    return JSONResponse(content=payload, headers=headers)


# Browser entry to the procurement site: same-origin proxy so the operator can
# open both the console and the target site from the collector's port.
_PROXY_CLIENT: httpx.AsyncClient | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _PROXY_CLIENT
    _PROXY_CLIENT = httpx.AsyncClient(base_url=BASE_URL, timeout=20.0)


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _PROXY_CLIENT is not None:
        await _PROXY_CLIENT.aclose()


@app.api_route("/site/{path:path}", methods=["GET", "POST"])
async def site_proxy(path: str, request: Request):
    assert _PROXY_CLIENT is not None
    # Pass query params exactly once (httpx merges params into the URL).
    proxy_headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    try:
        upstream = await _PROXY_CLIENT.request(
            request.method,
            path,
            params=list(request.query_params.multi_items()),
            content=await request.body(),
            headers=proxy_headers,
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"procurement site unreachable: {exc}") from exc
    excluded = {"content-encoding", "transfer-encoding", "connection"}
    headers = {k: v for k, v in upstream.headers.items() if k.lower() not in excluded}
    content_type = upstream.headers.get("content-type", "")
    if "application/json" in content_type:
        return JSONResponse(upstream.json(), status_code=upstream.status_code, headers=headers)
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=headers,
        media_type=content_type or None,
    )


# Browser UI (mounted last so /api routes take precedence).
_static_dir = Path(__file__).resolve().parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="ui")
