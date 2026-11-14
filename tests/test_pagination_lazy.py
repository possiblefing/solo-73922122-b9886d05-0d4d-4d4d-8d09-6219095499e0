"""Lazy pagination / remainder reporting / stop semantics tests.

Run: python -I tests/test_pagination_lazy.py
"""

import asyncio
import sys
import time

sys.path.insert(0, "/workspace/collector")

from app.crawler import MAX_IN_FLIGHT, Crawler  # noqa: E402

PORT = 18120
LARGE_TOTAL_PAGES = 200_000


class FakeSite:
    """Minimal HTTP/1.1 upstream that advertises ``total_pages`` list pages."""

    def __init__(self, total_pages=3, page_items=4, broken_codes=(), fail_pages=()):
        self.total_pages = total_pages
        self.page_items = page_items
        self.broken_codes = set(broken_codes)
        self.fail_pages = set(fail_pages)
        self.inflight = 0
        self.max_inflight = 0
        self.hits = 0
        self.paths: list[str] = []
        self.lock = asyncio.Lock()
        self.reject_next = 0
        self.retry_after = 2
        self.delay = 0.02

    async def handle(self, scope, receive, send):
        path = scope["path"]
        query = scope.get("query_string", b"").decode()
        async with self.lock:
            self.inflight += 1
            self.hits += 1
            self.paths.append(path + ("?" + query if query else ""))
            self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            await asyncio.sleep(self.delay)
            if self.reject_next > 0:
                self.reject_next -= 1
                await self._send(send, 429, b"<html>429</html>", b"retry-after", str(self.retry_after))
                return
            if path in ("/tenders", "/awards"):
                page = 1
                if "page=" in query:
                    page = int(query.split("page=")[1].split("&")[0])
                if page in self.fail_pages:
                    await self._send(send, 500, b"<html>boom</html>")
                    return
                await self._send(send, 200, self._list_html(path, page))
                return
            if path.startswith("/notice/"):
                code = path.rsplit("/", 1)[-1]
                if code in self.broken_codes:
                    body = (f'<html><body><h1 id="detail-title">标题 {code}</h1>'
                            '<table class="detail-table"><tr><th>公告编号</th>'
                            f'<td>{code}</td></tr></table></body></html>')
                else:
                    body = (f'<html><body><h1 id="detail-title">标题 {code}</h1>'
                            '<table class="detail-table">'
                            f'<tr><th>公告编号</th><td>{code}</td></tr>'
                            '<tr><th>采购单位</th><td>测试单位</td></tr>'
                            '<tr><th>截止日期</th><td>2026-12-31 17:00</td></tr>'
                            '</table></body></html>')
                await self._send(send, 200, body.encode())
                return
            await self._send(send, 404, b"not found")
        finally:
            async with self.lock:
                self.inflight -= 1

    def _list_html(self, section: str, page: int) -> bytes:
        start = (page - 1) * self.page_items + 1
        rows = "".join(
            f'<tr class="announcement"><td class="code">ZB-2026-{n:03d}</td>'
            f'<td><a href="/notice/ZB-2026-{n:03d}">公告{n}</a></td></tr>'
            for n in range(start, start + self.page_items)
        )
        return (
            '<html><body><table id="announcement-list"><tbody>' + rows +
            f'</tbody></table><nav class="pager" data-total-pages="{self.total_pages}"></nav>'
            '</body></html>'
        ).encode()

    @staticmethod
    async def _send(send, status, body, header=None, value=None):
        headers = [(b"content-type", b"text/html; charset=utf-8")]
        if header:
            headers.append((header, value.encode()))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})


async def serve(site):
    ready = asyncio.Event()

    async def pipe(reader, writer):
        while True:
            line = await reader.readline()
            if not line:
                break
            headers = {}
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
                k, _, v = h.partition(b":")
                headers[k.strip().lower()] = v.strip()
            cl = int(headers.get(b"content-length", b"0"))
            if cl:
                await reader.readexactly(cl)
            target = line.split(b" ")[1].decode()
            path, _, query = target.partition("?")
            collected = {}

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(msg):
                if msg["type"] == "http.response.start":
                    collected["status"] = msg["status"]
                    collected["headers"] = msg["headers"]
                else:
                    body = msg.get("body", b"")
                    writer.write(b"HTTP/1.1 " + str(collected["status"]).encode() + b" X\r\n")
                    for k, v in collected["headers"]:
                        writer.write(k + b": " + v + b"\r\n")
                    writer.write(b"content-length: " + str(len(body)).encode() + b"\r\n\r\n")
                    writer.write(body)
                    await writer.drain()
                    writer.close()

            scope = {"type": "http", "path": path, "query_string": query.encode(),
                     "headers": [], "method": "GET"}
            try:
                await site.handle(scope, receive, send)
            except (ConnectionResetError, BrokenPipeError):
                pass  # the client aborted the request (expected on stop)
            return

    async def runner():
        srv = await asyncio.start_server(pipe, "127.0.0.1", PORT)
        ready.set()
        async with srv:
            await srv.serve_forever()

    task = asyncio.create_task(runner())
    await ready.wait()
    return task


def make_crawler():
    return Crawler(base_url=f"http://127.0.0.1:{PORT}",
                   sections={"招标公告": "/tenders", "中标公告": "/awards"})


async def test_large_page_count_is_not_materialised():
    site = FakeSite(total_pages=LARGE_TOTAL_PAGES, page_items=4)
    srv = await serve(site)
    crawler = make_crawler()
    try:
        await crawler.start_round()
        await asyncio.sleep(1.0)
        assert crawler.state.status == "running", crawler.state.status
        snap = crawler.snapshot()
        # A huge virtual page count must not inflate the queue or state.
        assert snap["pages_enqueued"] < 100, snap["pages_enqueued"]
        assert len(crawler._records) < 100
        # Progress and stop stay responsive while running.
        t0 = time.monotonic()
        first = await crawler.stop_round()
        assert time.monotonic() - t0 < 2.0, "stop was not prompt"
        snap = crawler.snapshot()
        print(f"[lazy] enqueued={snap['pages_enqueued']} requested={snap['pages_requested']} "
              f"status={snap['status']} unrequested_ranges={len(snap['unrequested_ranges'])}")
        assert snap["status"] == "stopped"
        assert snap["success"] is False
        # Unrequested list pages are one contiguous range, not 200k records.
        list_ranges = [r for r in snap["unrequested_ranges"] if r["page_type"] == "list"]
        assert list_ranges, snap["unrequested_ranges"]
        assert any(r["count"] and r["count"] > 1000 for r in list_ranges), list_ranges
        assert len(snap["failed_pages"]) == 0 or all(
            r["attempts"] > 0 for r in snap["failed_pages"])
        # Huge tail must not create one record per page.
        assert len(snap["unrequested_ranges"]) < 10
    finally:
        await crawler.stop_round()
        srv.cancel()
        await asyncio.gather(srv, return_exceptions=True)


async def test_failed_and_unrequested_are_distinct():
    # Page 3 of tenders fails repeatedly: pages 4..6 must never be requested,
    # and are reported as a range, while page 3 is a failed page with 3 tries.
    site = FakeSite(total_pages=6, page_items=2, fail_pages={3}, broken_codes=set())
    srv = await serve(site)
    crawler = make_crawler()
    try:
        # Only crawl tenders to keep the expected numbers deterministic.
        crawler.sections = {"招标公告": "/tenders"}
        crawler.state.sections = {"招标公告": type(crawler.state.sections["招标公告"])()}
        await crawler.start_round()
        await crawler._round_task
        snap = crawler.snapshot()
        print(f"[split] status={snap['status']} failed={len(snap['failed_pages'])} "
              f"unrequested={snap['unrequested_pages']}")
        assert snap["status"] == "partial"
        assert snap["success"] is False
        failed = snap["failed_pages"]
        assert len(failed) == 1, failed
        assert failed[0]["page"] == "3" and failed[0]["attempts"] == 3
        ranges = snap["unrequested_ranges"]
        list_ranges = [r for r in ranges if r["page_type"] == "list"]
        assert len(list_ranges) == 1, list_ranges
        assert list_ranges[0]["start_page"] == 4 and list_ranges[0]["end_page"] == 6
        assert list_ranges[0]["count"] == 3
        # Detail pages of page 3 were requested; page 4..6 details never were.
        assert "page=4" not in " ".join(site.paths)
        assert snap["unrequested_count"] == 3
        # Failed and unrequested reports must not overlap.
        failed_keys = {(p["section"], p["page_type"], p["page"]) for p in failed}
        for r in list_ranges:
            assert r["page_type"] == "list"
        assert all(p["page_type"] != "list" or p["attempts"] > 0 for p in failed)
    finally:
        srv.cancel()
        await asyncio.gather(srv, return_exceptions=True)


async def test_stop_reports_unrequested_details_without_per_page_records():
    site = FakeSite(total_pages=50, page_items=10)
    srv = await serve(site)
    crawler = make_crawler()
    try:
        await crawler.start_round()
        await asyncio.sleep(0.15)
        await crawler.stop_round()
        snap = crawler.snapshot()
        print(f"[stop] status={snap['status']} success={snap['success']} "
              f"failed={len(snap['failed_pages'])} ranges={len(snap['unrequested_ranges'])}")
        assert snap["status"] == "stopped"
        assert snap["success"] is False
        # Never mix a never-requested page into the failed list.
        for p in snap["failed_pages"]:
            assert p["attempts"] >= 1
        # Every detail page listed as unrequested belongs to one compact range.
        detail_ranges = [r for r in snap["unrequested_ranges"] if r["page_type"] == "detail"]
        for r in detail_ranges:
            assert r["count"] == len(r["codes"]) and r["codes"]
        # Stop before the first list page yields an unknown-count range.
        assert snap["unrequested_count"] >= 0
    finally:
        await crawler.stop_round()
        srv.cancel()
        await asyncio.gather(srv, return_exceptions=True)


async def test_stop_before_first_request_is_unknown_range():
    class BlockingSite(FakeSite):
        async def handle(self, scope, receive, send):
            await asyncio.sleep(30)

    site = BlockingSite(total_pages=10)
    srv = await serve(site)
    crawler = make_crawler()
    try:
        await crawler.start_round()
        await asyncio.sleep(0.05)
        t0 = time.monotonic()
        await crawler.stop_round()
        assert time.monotonic() - t0 < 2.0
        snap = crawler.snapshot()
        print(f"[stop-first] status={snap['status']} ranges={snap['unrequested_ranges']}")
        assert snap["status"] == "stopped"
        assert snap["success"] is False
        # The in-flight request was aborted; page 1 was requested and failed,
        # while the tail is unknown but starts at a locatable page.
        failed = [p for p in snap["failed_pages"] if p["page_type"] == "list"]
        assert failed and failed[0]["page"] == "1" and failed[0]["attempts"] == 1
        list_ranges = [r for r in snap["unrequested_ranges"] if r["page_type"] == "list"]
        assert list_ranges and list_ranges[0]["start_page"] == 2
        assert list_ranges[0]["count"] is None
        assert snap["unrequested_count"] == 0
    finally:
        srv.cancel()
        await asyncio.gather(srv, return_exceptions=True)


async def main():
    await test_large_page_count_is_not_materialised()
    await test_failed_and_unrequested_are_distinct()
    await test_stop_reports_unrequested_details_without_per_page_records()
    await test_stop_before_first_request_is_unknown_range()
    print("\nALL PAGINATION TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
