"""Crawler engine.

Concurrency, pagination and rate-limit contract:

* Only one collection round may run at a time (``round_lock``).
* At most two HTTP requests are in flight at any moment, across both columns
  (a single shared ``asyncio.Semaphore(2)``).
* Pagination is *lazy*.  A list page never materialises every page number the
  source site advertises (which may be hundreds of thousands of pages).  Each
  column walks its pages in order; a list page schedules at most its own detail
  pages plus the next list page.  Memory and progress snapshots therefore stay
  bounded by the amount of work actually performed.  Pages that were never
  requested are reported as compact, contiguous ``start_page..end_page``
  ranges instead of one record per page.
* A 429 with a ``Retry-After`` integer header pauses the *whole site*: every
  later request, including the retry itself and requests of the other column,
  waits in :meth:`_pass_gate`. Further 429s only extend the pause. Requests
  already in flight are allowed to finish.
* Every page (list or detail) is attempted at most three times.  A page that
  was requested but did not succeed is recorded in ``failed_pages`` with its
  page number / announcement code and the number of attempts made.  Pages that
  were never requested are not mixed into that list; they are summarised in
  ``unrequested_ranges`` as contiguous ranges with a count.  A round with
  either kind of remainder is never reported as successful.
* Stop blocks queued/cooling requests from being sent, actively cancels the
  in-flight HTTP requests, closes the connection pool, and only after all of
  that has been awaited flips the state to stopped and permits a new round.
  A round id guards against a late response from an old round being written
  into the new one.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import httpx

from . import parser
from .parser import ListParseResult, ParseError

MAX_ATTEMPTS = 3
MAX_IN_FLIGHT = 2
GATE_POLL_SECONDS = 0.25
PROGRESS_POLL_SECONDS = 0.05
EVENT_LOG_LIMIT = 200
DEFAULT_RETRY_AFTER_SECONDS = 5


class RoundStopped(Exception):
    """Raised inside a round when cooperative stop has been requested."""


@dataclass
class SectionProgress:
    list_total_pages: int = 0
    list_requested: int = 0
    list_completed: int = 0
    list_failed: int = 0
    # Highest list page number for which a request was actually started.  List
    # pages are scheduled strictly in order, so this single number is enough to
    # describe which list pages were requested and which were not.
    list_highest_requested: int = 0
    detail_requested: int = 0
    detail_enqueued: int = 0
    detail_completed: int = 0
    detail_failed: int = 0
    duplicated_skipped: int = 0


@dataclass
class FailedPage:
    """A page that was requested but never succeeded within the attempt cap."""

    section: str
    page_type: str  # "list" or "detail"
    page: str  # list page number, or announcement code for detail pages
    list_page: int | None  # list page that referenced a detail page
    url: str
    reason: str
    attempts: int


@dataclass
class UnrequestedRange:
    """A contiguous run of pages that was never requested.

    ``end_page``/``count`` are ``None`` when the site's total page count is not
    known (for example the first list page was never fetched).  The start page
    is still locatable, so the remainder can be reported without inventing
    per-page records.
    """

    section: str
    page_type: str  # "list" or "detail"
    start_page: int
    end_page: int | None
    count: int | None
    url: str | None = None
    codes: list[str] = field(default_factory=list)


@dataclass
class RoundState:
    round_id: str | None = None
    status: str = "idle"  # idle | running | stopping | stopped | completed | partial
    base_url: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    stop_requested: bool = False
    pages_enqueued: int = 0
    pages_requested: int = 0
    pages_completed: int = 0
    pages_failed: int = 0
    records_count: int = 0
    unrequested_count: int = 0
    # True when at least one unrequested range has an unknown count (the site
    # never disclosed its total page count), so the total is a lower bound.
    unrequested_count_incomplete: bool = False
    cooldown_remaining_seconds: float = 0.0
    sections: dict[str, SectionProgress] = field(default_factory=dict)
    failed_pages: list[FailedPage] = field(default_factory=list)
    unrequested_ranges: list[UnrequestedRange] = field(default_factory=list)
    events: list[str] = field(default_factory=list)


class Crawler:
    def __init__(self, base_url: str, sections: dict[str, str], timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        # section display name -> list path
        self.sections = sections
        self.timeout = timeout

        self._round_lock = asyncio.Lock()
        self._round_task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

        # Per-round runtime flags (reset when a round starts).
        self._round_id: str | None = None
        self._stop_event = asyncio.Event()
        self._inflight = asyncio.Semaphore(MAX_IN_FLIGHT)
        self._request_tasks: set[asyncio.Task] = set()
        # Detail pages that were discovered but never requested, grouped by
        # (section, source list page).  At most one list page's worth of detail
        # jobs can be waiting, so this stays bounded.
        self._unrequested_details: dict[tuple[str, int], list[tuple[str, str]]] = {}
        self._unrequested_codes: set[str] = set()

        # Site-wide 429 cooldown (one clock for the whole crawler).
        self._cooldown_until = 0.0

        self.state = RoundState(
            base_url=self.base_url,
            sections={name: SectionProgress() for name in sections},
        )
        self._records: dict[str, dict] = {}
        self._seen_codes: set[str] = set()

    # ------------------------------------------------------------------ state
    def snapshot(self) -> dict:
        now = time.monotonic()
        remaining = max(0.0, self._cooldown_until - now)
        self.state.cooldown_remaining_seconds = round(remaining, 1)
        data = asdict(self.state)
        # Backwards-compatible name used by earlier clients/tests.  It contains
        # only requested-but-failed pages; unrequested pages are separate.
        data["unfinished"] = data["failed_pages"]
        data["success"] = self.state.status == "completed"
        data["unrequested_pages"] = {
            "count": data["unrequested_count"],
            "ranges": data["unrequested_ranges"],
        }
        return data

    def records(self) -> list[dict]:
        return list(self._records.values())

    def _log(self, message: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.state.events.append(f"{stamp} {message}")
        del self.state.events[:-EVENT_LOG_LIMIT]

    # ------------------------------------------------------------- lifecycle
    async def start_round(self) -> str:
        async with self._round_lock:
            if self._round_task is not None and not self._round_task.done():
                raise RuntimeError("a collection round is already running")

            round_id = uuid.uuid4().hex[:12]
            self._round_id = round_id
            self._stop_event = asyncio.Event()
            self._inflight = asyncio.Semaphore(MAX_IN_FLIGHT)
            self._request_tasks = set()
            self._unrequested_details = {}
            self._unrequested_codes = set()
            self._cooldown_until = 0.0
            self._records: dict[str, dict] = {}
            self._seen_codes: set[str] = set()

            self.state = RoundState(
                round_id=round_id,
                status="running",
                base_url=self.base_url,
                started_at=datetime.now(timezone.utc).isoformat(),
                sections={name: SectionProgress() for name in self.sections},
            )
            # Limits created before this round do not apply to the new one:
            # a fresh client also guarantees a fresh connection pool.
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers={"User-Agent": "procurement-collector/1.0"},
                follow_redirects=True,
            )
            self._log(f"round {round_id} started; columns={list(self.sections)}")
            self._round_task = asyncio.create_task(self._run_round(round_id))
            return round_id

    async def stop_round(self) -> None:
        async with self._round_lock:
            task = self._round_task
            client = self._client
            if task is None or task.done():
                return

            # 1) Accept the stop synchronously: every later gate check fails,
            #    so no new request can be created from this point on.
            self._stop_event.set()
            self.state.stop_requested = True
            self.state.status = "stopping"
            self._log("stop accepted: no new requests will be sent")

            # 2) Actively abort in-flight requests and wait for their sockets
            #    to be released before reporting the round as stopped.
            await self._cancel_inflight_requests()
            if client is not None:
                try:
                    await client.aclose()
                except Exception:  # pragma: no cover - defensive
                    pass

            # 3) Wait until the round coroutine has fully wound down.
            try:
                await task
            except asyncio.CancelledError:  # pragma: no cover - defensive
                raise
            except Exception:
                pass

            # Rebuild a closed client so the next round owns a clean pool.
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers={"User-Agent": "procurement-collector/1.0"},
                follow_redirects=True,
            )
            self._log("in-flight requests aborted and connections recycled; round stopped")

    async def _cancel_inflight_requests(self) -> None:
        tasks = list(self._request_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ----------------------------------------------------------------- round
    async def _run_round(self, round_id: str) -> None:
        # Mutable holder so both the coordinator and workers can track the
        # number of jobs queued or being processed.  Only the initial list page
        # of each column starts queued: further list pages are scheduled one at
        # a time as their predecessor settles.
        pending = {"count": 0}
        queue: asyncio.Queue = asyncio.Queue()
        for name, path in self.sections.items():
            queue.put_nowait(
                {"section": name, "kind": "list", "page": 1, "list_page": 1,
                 "url": path, "attempts": 0, "requested": False}
            )
            pending["count"] += 1
            self.state.pages_enqueued += 1

        workers = [
            asyncio.create_task(self._worker(round_id, queue, i, pending))
            for i in range(MAX_IN_FLIGHT)
        ]

        try:
            # Coordinator: finish once every job (and every job it spawned)
            # has been settled. Stop interrupts the wait promptly.
            while pending["count"] > 0 and not self._stop_event.is_set():
                await asyncio.sleep(PROGRESS_POLL_SECONDS)
            for _ in workers:
                queue.put_nowait(None)
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            for w in workers:
                if not w.done():
                    w.cancel()
            for w in workers:
                try:
                    await w
                except asyncio.CancelledError:
                    pass
            # Any job still queued when the round ends was never requested.
            # It is reported as part of a contiguous range, not page by page.
            while not queue.empty():
                leftover = queue.get_nowait()
                if isinstance(leftover, dict):
                    self._settle_unrequested(leftover)
            # Defensive: stop_round already did this, but a round can also end
            # by itself.  Never leave an aborted request task behind.
            await self._cancel_inflight_requests()
            # Close the client used by this round (idempotent if stopped).
            if self._client is not None:
                try:
                    await self._client.aclose()
                except Exception:
                    pass

        if round_id != self._round_id:
            # A new round superseded this one; do not touch its state.
            return

        self._finalize_round(round_id)

    def _finalize_round(self, round_id: str) -> None:
        self._build_unrequested_ranges()
        self.state.unrequested_count = sum(
            r.count for r in self.state.unrequested_ranges if r.count is not None
        )
        self.state.unrequested_count_incomplete = any(
            r.count is None for r in self.state.unrequested_ranges
        )
        self.state.finished_at = datetime.now(timezone.utc).isoformat()
        if self._stop_event.is_set():
            # A stopped round is never a success, even if every page that was
            # requested happened to finish first.
            self.state.status = "stopped"
        elif self.state.failed_pages or self.state.unrequested_ranges:
            self.state.status = "partial"
        else:
            self.state.status = "completed"
        self.state.records_count = len(self._records)
        self._log(
            f"round ended: status={self.state.status}, "
            f"records={self.state.records_count}, "
            f"failed_pages={len(self.state.failed_pages)}, "
            f"unrequested={self.state.unrequested_count} pages in "
            f"{len(self.state.unrequested_ranges)} range(s)"
        )

    async def _worker(
        self, round_id: str, queue: asyncio.Queue, worker_no: int, pending: dict
    ) -> None:
        while True:
            job = await queue.get()
            try:
                if job is None:
                    return
                if self._stop_event.is_set():
                    self._settle_unrequested(job)
                    pending["count"] -= 1
                    return
                try:
                    if job["kind"] == "list":
                        await self._handle_list(round_id, job, queue, pending)
                    else:
                        await self._handle_detail(round_id, job, pending)
                except RoundStopped:
                    # Stop while waiting/cooling: no further request is sent.
                    if job.get("requested"):
                        self._note_failure(job, "round stopped before this page succeeded")
                    else:
                        self._settle_unrequested(job)
                    pending["count"] -= 1
                    return
                except Exception as exc:  # noqa: BLE001 - never wedge a round
                    # An unexpected handler bug must not leave ``pending``
                    # unbalanced (which would hang the coordinator).  Account
                    # for the job and surface it as a failed page.
                    self._note_failure(job, f"internal error: {exc!r}")
                    self._log(f"[{job['section']}] {job['kind']} {job['page']} internal error: {exc!r}")
                    pending["count"] -= 1
            finally:
                queue.task_done()

    # -------------------------------------------------------------- handlers
    async def _handle_list(
        self, round_id: str, job: dict, queue: asyncio.Queue, pending: dict
    ) -> None:
        section = job["section"]
        progress = self.state.sections[section]
        try:
            result: ListParseResult = await self._get_parsed_page(
                round_id, job, parser.parse_list_page
            )
        except RoundStopped:
            raise
        except Exception as exc:
            self._note_failure(job, str(exc))
            self._log(f"[{section}] list page {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return

        if round_id != self._round_id:
            raise RoundStopped("round superseded after list response")

        progress.list_completed += 1
        self.state.pages_completed += 1

        page_no = int(job["page"])
        # Never trust a shrinking/stale total to hide the page we just read.
        progress.list_total_pages = max(
            progress.list_total_pages, result.total_pages, page_no
        )

        # If stop was accepted while this response was being parsed, none of
        # the children were requested.  Record the detail items compactly and
        # let the list range cover all later list pages.
        if self._stop_event.is_set():
            self._record_unrequested_details(
                section,
                page_no,
                # Codes already claimed on earlier pages (including the other
                # column) were handled; they must not reappear as unrequested.
                [(item.code, item.url) for item in result.items
                 if item.code not in self._seen_codes],
            )
            pending["count"] -= 1
            return

        # ``pending`` is incremented before each enqueue and decremented once
        # for this job at the end, so an exception mid-loop can never leave the
        # coordinator waiting on a job that was never counted.
        for item in result.items:
            if item.code in self._seen_codes:
                progress.duplicated_skipped += 1
                continue
            self._seen_codes.add(item.code)
            queue.put_nowait(
                {
                    "section": section,
                    "kind": "detail",
                    "page": item.code,
                    "list_page": page_no,
                    "url": item.url,
                    "attempts": 0,
                    "requested": False,
                }
            )
            progress.detail_enqueued += 1
            self.state.pages_enqueued += 1
            pending["count"] += 1

        # Schedule only the next list page, never the whole tail.  A page that
        # failed does not schedule its successor: the unrequested remainder is
        # then a clean, locatable range after the failure.
        if page_no < progress.list_total_pages:
            queue.put_nowait(
                {
                    "section": section,
                    "kind": "list",
                    "page": page_no + 1,
                    "list_page": page_no + 1,
                    "url": self._list_url(section, page_no + 1),
                    "attempts": 0,
                    "requested": False,
                }
            )
            self.state.pages_enqueued += 1
            pending["count"] += 1

        # This job is settled; the children it enqueued keep the round alive.
        pending["count"] -= 1

    async def _handle_detail(self, round_id: str, job: dict, pending: dict) -> None:
        section = job["section"]
        progress = self.state.sections[section]
        try:
            detail = await self._get_parsed_page(round_id, job, parser.parse_detail_page)
        except RoundStopped:
            raise
        except Exception as exc:
            self._note_failure(job, str(exc))
            self._log(f"[{section}] detail {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return

        if round_id != self._round_id:
            raise RoundStopped("round superseded after detail response")

        progress.detail_completed += 1
        self.state.pages_completed += 1
        self._records[detail["code"]] = {
            "announcement_code": detail["code"],
            "title": detail["title"],
            "procurement_unit": detail["unit"],
            "deadline": detail["deadline"],
            "section": section,
            "detail_url": self._absolute(job["url"]),
        }
        self.state.records_count = len(self._records)
        pending["count"] -= 1

    # ------------------------------------------------------------- transport
    async def _get_parsed_page(self, round_id: str, job: dict, parse_func):
        """Fetch one URL with up to three attempts, then parse it.

        An attempt is counted only when a request is actually about to be sent;
        waiting for the cooldown (or stopping) does not consume an attempt.
        Transport errors (including an active stop abort), HTTP error statuses
        and parse failures all consume attempts.  A page whose HTML never
        parses within three attempts surfaces the last :class:`ParseError`.
        """
        last_error: Exception | None = None
        while job["attempts"] < MAX_ATTEMPTS:
            await self._pass_gate(round_id)  # may raise RoundStopped
            if round_id != self._round_id:
                raise RoundStopped("round superseded before request")
            if self._stop_event.is_set():
                raise RoundStopped("stop requested before request could be sent")

            try:
                # Acquiring the slot is not an attempt: waiting here (or for
                # the cooldown) must not consume one of the three tries.
                async with self._inflight:
                    if self._stop_event.is_set() or round_id != self._round_id:
                        raise RoundStopped("stop requested before request could be sent")
                    # From here to task creation there is no await, so a stop
                    # accepted concurrently either precedes this request or
                    # finds it registered for cancellation.
                    job["attempts"] += 1
                    if not job.get("requested"):
                        job["requested"] = True
                        self._note_request_started(job)
                    response = await self._send_request(round_id, job["url"])
            except RoundStopped:
                raise
            except asyncio.CancelledError:
                if self._stop_event.is_set() or round_id != self._round_id:
                    raise RoundStopped("in-flight request aborted by stop") from None
                raise
            except httpx.RequestError as exc:
                last_error = exc
                if self._stop_event.is_set() or round_id != self._round_id:
                    raise RoundStopped("in-flight request aborted by stop") from exc
                self._log(
                    f"request error ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}: {exc}"
                )
                continue

            if round_id != self._round_id:
                raise RoundStopped("round superseded after response")

            # The request itself has finished: 429 only gates *later* traffic.
            if response.status_code == 429:
                wait_seconds = self._retry_after_seconds(response)
                self._extend_cooldown(wait_seconds)
                last_error = httpx.HTTPStatusError(
                    f"429 Too Many Requests; site-wide cooldown {wait_seconds}s",
                    request=response.request,
                    response=response,
                )
                self._log(
                    f"429 on {job['url']}; whole site pauses {wait_seconds}s "
                    f"(attempt {job['attempts']}/{MAX_ATTEMPTS})"
                )
                continue

            if response.status_code >= 400:
                last_error = httpx.HTTPStatusError(
                    f"HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                self._log(
                    f"HTTP {response.status_code} ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}"
                )
                continue

            try:
                return parse_func(response.text)
            except ParseError as exc:
                last_error = exc
                self._log(
                    f"parse error ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}: {exc}"
                )
                continue

        raise last_error if last_error else ParseError("unknown failure")

    async def _send_request(self, round_id: str, url: str) -> httpx.Response:
        """Create the HTTP request as a cancellable task.

        The stop check and task registration happen without an ``await`` in
        between, so once :meth:`stop_round` sets the stop event no new request
        can slip past it.
        """
        if self._stop_event.is_set() or round_id != self._round_id:
            raise RoundStopped("stop requested before request could be sent")
        assert self._client is not None
        task = asyncio.create_task(self._client.get(url))
        self._request_tasks.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            if self._stop_event.is_set() or round_id != self._round_id:
                raise RoundStopped("in-flight request aborted by stop") from None
            raise
        finally:
            self._request_tasks.discard(task)

    async def _pass_gate(self, round_id: str) -> None:
        """Block while stop is requested or a site-wide cooldown is active."""
        while True:
            if self._stop_event.is_set() or round_id != self._round_id:
                raise RoundStopped("stop requested before request could be sent")
            remaining = self._cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.sleep(min(GATE_POLL_SECONDS, remaining))
            except asyncio.CancelledError:  # pragma: no cover - defensive
                raise

    def _extend_cooldown(self, seconds: int) -> None:
        # Later 429s can only extend the existing wait.
        self._cooldown_until = max(self._cooldown_until, time.monotonic() + seconds)

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> int:
        raw = response.headers.get("Retry-After", "").strip()
        if raw.isdigit():
            return max(0, int(raw))
        return DEFAULT_RETRY_AFTER_SECONDS

    # ---------------------------------------------------------------- helpers
    def _note_request_started(self, job: dict) -> None:
        progress = self.state.sections[job["section"]]
        self.state.pages_requested += 1
        if job["kind"] == "list":
            progress.list_requested += 1
            page_no = int(job["page"])
            progress.list_highest_requested = max(progress.list_highest_requested, page_no)
        else:
            progress.detail_requested += 1

    def _note_failure(self, job: dict, reason: str) -> None:
        """Record a page that was requested but did not succeed."""
        progress = self.state.sections[job["section"]]
        if job["kind"] == "list":
            progress.list_failed += 1
        else:
            progress.detail_failed += 1
        self.state.pages_failed += 1
        self.state.failed_pages.append(
            FailedPage(
                section=job["section"],
                page_type=job["kind"],
                page=str(job["page"]),
                list_page=job.get("list_page"),
                url=self._absolute(job["url"]),
                reason=reason,
                attempts=job.get("attempts", 0),
            )
        )

    def _settle_unrequested(self, job: dict) -> None:
        """Account for a job that was scheduled but never requested."""
        if job["kind"] == "detail":
            self._record_unrequested_details(
                job["section"], int(job.get("list_page") or 0), [(job["page"], job["url"])]
            )
        # List jobs are represented by the contiguous range derived from
        # list_highest_requested and list_total_pages; one entry per page would
        # defeat the purpose of lazy scheduling.

    def _record_unrequested_details(
        self, section: str, list_page: int, items: list[tuple[str, str]]
    ) -> None:
        if not items:
            return
        bucket = self._unrequested_details.setdefault((section, list_page), [])
        for code, url in items:
            # Deduplicate within the unrequested report only.  Enqueued detail
            # codes live in ``_seen_codes``; they must not suppress a genuinely
            # unrequested code that happens to share its number across columns.
            if code in self._unrequested_codes:
                continue
            self._unrequested_codes.add(code)
            bucket.append((code, url))

    def _build_unrequested_ranges(self) -> None:
        ranges: list[UnrequestedRange] = []
        for name, progress in self.state.sections.items():
            start = progress.list_highest_requested + 1
            if progress.list_total_pages > 0:
                end = progress.list_total_pages
                if start <= end:
                    ranges.append(
                        UnrequestedRange(
                            section=name,
                            page_type="list",
                            start_page=start,
                            end_page=end,
                            count=end - start + 1,
                            url=self._absolute(self._list_url(name, start)),
                        )
                    )
            else:
                # The site never told us its total page count (the last
                # attempted list page was never parsed).  The next page is
                # still locatable, but neither its end nor its count is known.
                start = progress.list_highest_requested + 1
                ranges.append(
                    UnrequestedRange(
                        section=name,
                        page_type="list",
                        start_page=start,
                        end_page=None,
                        count=None,
                        url=self._absolute(self._list_url(name, start)),
                    )
                )

        for (section, list_page), items in sorted(self._unrequested_details.items()):
            seen: set[str] = set()
            unique: list[tuple[str, str]] = []
            for code, url in items:
                if code in seen:
                    continue
                seen.add(code)
                unique.append((code, url))
            if not unique:
                continue
            ranges.append(
                UnrequestedRange(
                    section=section,
                    page_type="detail",
                    start_page=list_page,
                    end_page=list_page,
                    count=len(unique),
                    url=self._absolute(unique[0][1]),
                    codes=[code for code, _ in unique],
                )
            )

        self.state.unrequested_ranges = ranges

    def _list_url(self, section: str, page: int) -> str:
        path = self.sections[section]
        separator = "&" if "?" in path else "?"
        return f"{path}{separator}page={page}"

    def _absolute(self, url: str) -> str:
        if url.startswith(("http://", "https://")):
            return url
        return f"{self.base_url}/{url.lstrip('/')}"
