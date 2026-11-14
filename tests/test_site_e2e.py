"""End-to-end test: real mock site + real collector API, no Docker required.

Both FastAPI apps are served on one asyncio loop with uvicorn so the test can
start and stop them deterministically.

Run: python -I tests/test_site_e2e.py
"""

import asyncio
import importlib.util
import json
import os
import socket
import sys
from pathlib import Path

import httpx
import uvicorn

ROOT = Path("/workspace")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def load_app(package: str, app_dir: Path):
    """Load ``app`` from a directory under a unique package name.

    Both services ship a package called ``app``; loading each under its own
    name keeps their relative imports independent.
    """
    package_dir = app_dir / "app"
    spec = importlib.util.spec_from_file_location(
        package, package_dir / "__init__.py",
        submodule_search_locations=[str(package_dir)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[package] = module
    spec.loader.exec_module(module)

    main_name = f"{package}.main"
    main_spec = importlib.util.spec_from_file_location(main_name, package_dir / "main.py")
    main = importlib.util.module_from_spec(main_spec)
    sys.modules[main_name] = main
    main_spec.loader.exec_module(main)
    return main.app


async def start_server(app, port: int):
    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="warning", lifespan="on",
    ))
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.05)
    if not server.started:
        raise RuntimeError(f"server never started on port {port}")
    return server, task


async def stop_server(server, task) -> None:
    server.should_exit = True
    try:
        await asyncio.wait_for(task, timeout=10)
    except asyncio.TimeoutError:  # pragma: no cover - defensive
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def main() -> None:
    site_port = free_port()
    collector_port = free_port()
    os.environ.update({
        "SITE_TOTAL_PAGES": "200000",
        "SITE_LATENCY_MS": "80",
        "SITE_RATE_LIMIT": "6",
        "SITE_RATE_WINDOW": "2",
        "SITE_RETRY_AFTER": "1",
        "TARGET_BASE_URL": f"http://127.0.0.1:{site_port}",
        "SECTION_TENDER_PATH": "/tenders",
        "SECTION_AWARD_PATH": "/awards",
    })
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    site_app = load_app("e2e_site_app", ROOT / "site")
    collector_app = load_app("e2e_collector_app", ROOT / "collector")

    site_server, site_task = await start_server(site_app, site_port)
    collector_server, collector_task = await start_server(collector_app, collector_port)
    base = f"http://127.0.0.1:{collector_port}"
    site_base = f"http://127.0.0.1:{site_port}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            health = (await client.get(f"{site_base}/healthz")).json()
            assert health["total_pages_override"] == 200000, health

            # Browser UI and proxied site must both be reachable.
            ui = await client.get(base + "/")
            assert ui.status_code == 200 and "未请求页面".encode() in ui.content
            proxied = await client.get(base + "/site/tenders?page=1")
            assert proxied.status_code == 200
            assert 'data-total-pages="200000"' in proxied.text

            # Run for a moment; progress must stay small and responsive.
            started = await client.post(base + "/api/rounds")
            assert started.status_code == 200, started.text
            await asyncio.sleep(2.0)
            state = (await client.get(base + "/api/state")).json()
            assert state["status"] == "running", state["status"]
            assert state["pages_enqueued"] < 200, state["pages_enqueued"]
            assert state["sections"]["招标公告"]["list_total_pages"] == 200000

            # Stop must be prompt and must abort in-flight traffic.
            import time
            t0 = time.monotonic()
            stopped = await client.post(base + "/api/rounds/stop")
            elapsed = time.monotonic() - t0
            assert stopped.status_code == 200, stopped.text
            assert elapsed < 3.0, f"stop was not prompt: {elapsed:.2f}s"
            state = (await client.get(base + "/api/state")).json()
            print(f"[e2e] status={state['status']} requested={state['pages_requested']} "
                  f"failed={len(state['failed_pages'])} "
                  f"unrequested_ranges={len(state['unrequested_ranges'])} "
                  f"stop={elapsed:.2f}s")
            assert state["status"] == "stopped"
            assert state["success"] is False
            assert len(state["unrequested_ranges"]) <= 4
            assert all(r["count"] is None or r["count"] > 1000
                       for r in state["unrequested_ranges"] if r["page_type"] == "list")
            for page in state["failed_pages"]:
                assert page["attempts"] >= 1

            # JSON download must separate failed from unrequested and deny success.
            download = await client.get(base + "/api/records/download")
            assert download.status_code == 200
            payload = download.json()
            assert payload["status"] == "stopped"
            assert payload["success"] is False
            assert "failed_pages" in payload and "unrequested_pages" in payload
            assert payload["unrequested_pages"]["count"] == state["unrequested_count"]
            assert payload["failed_pages"] == state["failed_pages"]
            print(f"[e2e] download success={payload['success']} "
                  f"failed={len(payload['failed_pages'])} "
                  f"unrequested={payload['unrequested_pages']['count']}")

            # A fresh round after stop works and resets state.
            started = await client.post(base + "/api/rounds")
            assert started.status_code == 200, started.text
            new_round = started.json()["round_id"]
            await asyncio.sleep(0.5)
            await client.post(base + "/api/rounds/stop")
            state = (await client.get(base + "/api/state")).json()
            assert state["round_id"] == new_round
            assert state["status"] == "stopped"
            print("[e2e] fresh round after stop is isolated")
    finally:
        await stop_server(collector_server, collector_task)
        await stop_server(site_server, site_task)

    print("\nE2E TEST PASSED")


if __name__ == "__main__":
    asyncio.run(main())
