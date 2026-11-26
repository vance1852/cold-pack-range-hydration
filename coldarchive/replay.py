"""Replay the timed event scripts shipped under ``fixtures/``.

This is both an operator demo and an executable specification.  It drives the
normal service API at the event timestamps and prints a JSON report.
"""
from __future__ import annotations

import argparse
import json
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from .indexing import IndexRegistry
from .media import HttpMediaClient, MediaReply
from .server import ColdReaderHttpServer
from .service import ColdReaderService


class ScriptedFixtureMediaClient(HttpMediaClient):
    """Skip POST calls omitted from a synthetic packet recording.

    The generic HTTP client correctly tries POST first.  For exact timeline
    replay, events.json itself tells us which device calls have scripted
    responses; omitted POSTs would only add an unmatched 404 round-trip.
    """

    def __init__(self, base_url: str, packets: list[dict[str, Any]], **kwargs: Any):
        super().__init__(base_url, **kwargs)
        self.scripted_post_paths = {p["path"] for p in packets if p.get("method") == "POST"}

    def apply_read(self, pack: str, segment: str):
        from urllib.parse import quote

        path = f"/media/{quote(pack, safe='')}/read/{quote(segment, safe='')}"
        if path not in self.scripted_post_paths:
            return MediaReply(202, {"state": "accepted", "scripted_recording": "post_omitted"})
        return super().apply_read(pack, segment)


class EventReplay:
    def __init__(
        self,
        fixture_dir: str | Path,
        *,
        media_url: str | None = None,
        state_dir: str | Path | None = None,
        start_http: int | None = None,
    ):
        self.fixture_dir = Path(fixture_dir)
        self.record = json.loads((self.fixture_dir / "events.json").read_text(encoding="utf-8"))
        _, self.policy = IndexRegistry.from_fixture(self.fixture_dir)
        self.media_url = media_url
        self.state_dir = Path(state_dir or tempfile.mkdtemp(prefix="cold-replay-"))
        self.service: ColdReaderService | None = None
        self.httpd: ColdReaderHttpServer | None = None
        self.http_thread: threading.Thread | None = None
        self.start_http = start_http
        self.results: dict[str, Any] = {}
        self._threads: list[threading.Thread] = []
        self._start = 0.0

    def start(self) -> str | None:
        if self.media_url is None:
            from device_fixture import FixtureServer

            self.fixture_server = FixtureServer(("127.0.0.1", 0), self.fixture_dir)
            fixture_thread = threading.Thread(target=self.fixture_server.serve_forever, daemon=True)
            fixture_thread.start()
            self.media_url = f"http://127.0.0.1:{self.fixture_server.server_port}"
        else:
            self.fixture_server = None

        media = ScriptedFixtureMediaClient(self.media_url, self.record.get("packets", []), timeout=3.0)
        self.service = ColdReaderService(media, policy=self.policy, state_dir=self.state_dir, poll_interval=0.005)
        http_url = None
        if self.start_http is not None:
            self.httpd = ColdReaderHttpServer(("127.0.0.1", self.start_http), self.service)
            self.http_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
            self.http_thread.start()
            http_url = f"http://127.0.0.1:{self.httpd.server_port}"
        return http_url

    def close(self) -> None:
        for thread in self._threads:
            thread.join(timeout=3)
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
        if self.service:
            self.service.close()
        if self.fixture_server:
            self.fixture_server.shutdown()
            self.fixture_server.server_close()

    def run(self) -> dict[str, Any]:
        http_url = self.start()
        assert self.service is not None
        self._start = time.monotonic()
        timers: list[threading.Timer] = []
        for event in self.record["events"]:
            delay = max(0.0, float(event["at"]) / 1000.0)
            timer = threading.Timer(delay, self._handle, args=(event,))
            timer.daemon = True
            timers.append(timer)
            timer.start()
        for timer in timers:
            timer.join()
        for thread in list(self._threads):
            thread.join(timeout=5)

        report = {
            "http_url": http_url,
            "media_url": self.media_url,
            "state_dir": str(self.state_dir),
            "results": self.results,
            "status": self.service.status(include_audit=100),
        }
        self.close()
        return report

    def _handle(self, event: dict[str, Any]) -> None:
        action = event["action"]
        if action == "read":
            thread = threading.Thread(target=self._run_read, args=(event,), daemon=True)
            self._threads.append(thread)
            thread.start()
        elif action in {"cancel", "close", "abort"}:
            rid = event["request"]
            if action == "cancel":
                snap = self.service.cancel(rid)
            elif action == "abort":
                snap = self.service.abort(rid)
            else:
                snap = self.service.close(rid)
            self.results[f"{action}:{rid}"] = self._summary(snap)
        elif action in {"resume", "continue"}:
            rid = event["request"]
            snap = self.service.poll(rid, wait_ms=min(100, float(event.get("wait_ms", 0))), cursor=0)
            self.results[f"{action}:{rid}:{event['at']}"] = self._summary(snap)
        elif action == "index":
            result = self.service.set_current_index(event["current"])
            self.results[f"index:{event['at']}"] = result
        elif action == "restart":
            old = self.service
            self.service = old.restart()
            self.results[f"restart:{event['at']}"] = self.service.status()["recovery"]
        else:
            self.results[f"unknown:{action}:{event['at']}"] = event

    def _run_read(self, event: dict[str, Any]) -> None:
        rid = event["request"]
        snap = self.service.read(
            request_id=rid,
            version=event.get("version"),
            offset=int(event["offset"]),
            length=int(event["length"]),
            wait_ms=float(event.get("wait_ms", 1000)),
            priority=event.get("priority", "normal"),
        )
        self.results[f"read:{rid}"] = self._summary(snap)

    @staticmethod
    def _summary(snap: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": snap.get("id"),
            "state": snap.get("state"),
            "complete": snap.get("complete"),
            "next_cursor": snap.get("next_cursor"),
            "remaining_ms": snap.get("remaining_ms"),
            "chunks": [
                {"seq": c["seq"], "offset": c["offset"], "length": c["length"], "sha256": c["sha256"]}
                for c in snap.get("chunks", [])
            ],
            "blocked": snap.get("blocked", []),
            "items": [
                {
                    "segment": i.get("segment"),
                    "state": i.get("state"),
                    "category": i.get("category"),
                    "blocker": i.get("blocker"),
                    "reason": i.get("reason"),
                }
                for i in snap.get("items", [])
            ],
        }


def run_replay(fixture_dir: str | Path, **kwargs: Any) -> dict[str, Any]:
    return EventReplay(fixture_dir, **kwargs).run()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--media-url", help="existing fixture/media server; starts an in-process fixture if omitted")
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--http-port", type=int)
    args = parser.parse_args(argv)
    replay = EventReplay(args.fixture, media_url=args.media_url, state_dir=args.state_dir, start_http=args.http_port)
    print(json.dumps(replay.run(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
