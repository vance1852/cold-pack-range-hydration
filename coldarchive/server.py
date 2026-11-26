"""HTTP API for the cold archive reader service."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .indexing import IndexRegistry, VersionNotFound
from .media import HttpMediaClient
from .service import ColdReaderService, RequestExists, RequestNotFound


def json_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Remove in-process ``bytes`` fields; base64 fields remain for JSON."""
    for chunk in snapshot.get("chunks", []):
        chunk.pop("data", None)
    return snapshot


def create_service_from_fixture(
    fixture: str | Path,
    *,
    media_url: str | None = None,
    state_dir: str | Path | None = None,
    port: int | None = None,
    poll_interval: float = 0.01,
) -> ColdReaderService:
    fixture = Path(fixture)
    registry, policy = IndexRegistry.from_fixture(fixture)
    if media_url is None:
        media_url = f"http://127.0.0.1:{port or 8090}"
    media = HttpMediaClient(media_url)
    return ColdReaderService(
        media,
        policy=policy,
        current_version=registry.current,
        state_dir=state_dir,
        poll_interval=poll_interval,
    )


class ColdReaderHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], service: ColdReaderService):
        super().__init__(address, ColdReaderHandler)
        self.service = service


class ColdReaderHandler(BaseHTTPRequestHandler):
    server: ColdReaderHttpServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        # Tests can configure logging; default stdlib logging is quieter than stderr.
        return

    @property
    def service(self) -> ColdReaderService:
        return self.server.service

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, error: str, **extra: Any) -> None:
        payload = {"ok": False, "error": error}
        payload.update(extra)
        self._send_json(status, payload)

    def _read_json_body(self) -> dict[str, Any]:
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            size = 0
        if size < 0 or size > 4 * 1024 * 1024:
            raise ValueError("invalid Content-Length")
        raw = self.rfile.read(size) if size else b"{}"
        if not raw:
            return {}
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value

    @staticmethod
    def _query_one(query: dict[str, list[str]], name: str, default: str | None = None) -> str | None:
        values = query.get(name)
        return values[0] if values else default

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlsplit(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            if path in {"/healthz", "/health"}:
                self._send_json(200, {"ok": True})
                return
            if path in {"/", "/v1", "/v1/reads"}:
                if path == "/v1/reads":
                    self._send_json(200, {"ok": True, "request_ids": list(self.service.state["requests"])})
                else:
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "service": "coldarchive",
                            "endpoints": [
                                "POST /v1/reads",
                                "GET /v1/reads/{id}?wait_ms=&cursor=",
                                "POST /v1/reads/{id}/poll",
                                "POST /v1/reads/{id}/close",
                                "POST /v1/reads/{id}/cancel",
                                "GET /v1/ops/status",
                            ],
                        },
                    )
                return

            parts = path.split("/")
            # /v1/reads/{id}
            if len(parts) == 4 and parts[1:3] == ["v1", "reads"]:
                rid = parts[3]
                wait_ms = float(self._query_one(query, "wait_ms", "0") or 0)
                cursor = int(self._query_one(query, "cursor", "0") or 0)
                snap = self.service.poll(rid, wait_ms=wait_ms, cursor=cursor)
                status = 200 if snap["complete"] or snap["state"] in {"canceled", "closed", "expired"} else 202
                self._send_json(status, {"ok": True, "snapshot": json_snapshot(snap)})
                return

            if path in {"/v1/status", "/v1/ops/status"}:
                self._send_json(200, {"ok": True, **self.service.status()})
                return
            if path == "/v1/ops/media":
                self._send_json(200, {"ok": True, "media": self.service.media_status()})
                return
            if path == "/v1/ops/hot":
                self._send_json(200, {"ok": True, "hot": self.service.hot_status()})
                return
            if path == "/v1/ops/readers":
                self._send_json(200, {"ok": True, "readers": self.service.readers_status()})
                return
            self._send_error_json(404, "not_found", path=path)
        except RequestNotFound as exc:
            self._send_error_json(404, "request_not_found", detail=str(exc))
        except VersionNotFound as exc:
            self._send_error_json(404, "version_not_found", detail=str(exc))
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            self._send_error_json(400, "bad_request", error_type=type(exc).__name__, detail=str(exc))
        except Exception as exc:
            self._send_error_json(500, "internal_error", error_type=type(exc).__name__, detail=str(exc))

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlsplit(self.path)
            path = parsed.path.rstrip("/")
            query = parse_qs(parsed.query)
            body = self._read_json_body()

            if path == "/v1/reads":
                merged = dict(body)
                for name in ("wait_ms", "priority", "version", "request_id", "cursor", "offset", "length"):
                    value = self._query_one(query, name)
                    if value is not None:
                        merged.setdefault(name, value)
                if "offset" not in merged or "length" not in merged:
                    raise ValueError("offset and length are required")
                try:
                    offset = int(merged["offset"])
                    length = int(merged["length"])
                    wait_ms = float(merged.get("wait_ms", 1000))
                    cursor = int(merged.get("cursor", 0))
                except (TypeError, ValueError) as exc:
                    raise ValueError("offset, length, wait_ms and cursor must be numeric") from exc
                if offset < 0 or length < 0 or wait_ms < 0 or cursor < 0:
                    raise ValueError("offset, length, wait_ms and cursor must be non-negative")
                snap = self.service.submit(
                    offset=offset,
                    length=length,
                    version=merged.get("version"),
                    wait_ms=wait_ms,
                    priority=str(merged.get("priority", "normal")),
                    request_id=merged.get("request_id") or merged.get("id"),
                    object_id=merged.get("object_id") or merged.get("object"),
                )
                # If wait_ms is supplied on creation, behave like read().
                if merged.get("block") in {True, "true", "1"} or "wait_ms" in merged:
                    snap = self.service.read(request_id=snap["id"], wait_ms=wait_ms, cursor=cursor)
                status_code = 200 if snap.get("complete") or snap.get("state") in {"canceled", "closed", "expired"} else 202
                self._send_json(status_code, {"ok": True, "snapshot": json_snapshot(snap)})
                return

            parts = path.split("/")
            if len(parts) == 5 and parts[1:3] == ["v1", "reads"]:
                rid = parts[3]
                action = parts[4]
                if action == "poll":
                    wait_ms = float(body.get("wait_ms", self._query_one(query, "wait_ms", "0") or 0))
                    cursor = int(body.get("cursor", self._query_one(query, "cursor", "0") or 0))
                    snap = self.service.poll(rid, wait_ms=wait_ms, cursor=cursor)
                    self._send_json(200, {"ok": True, "snapshot": json_snapshot(snap)})
                    return
                if action in {"close", "cancel", "abort", "finish"}:
                    if action == "close":
                        snap = self.service.close(rid)
                    elif action == "cancel":
                        snap = self.service.cancel(rid)
                    else:
                        snap = self.service.abort(rid)
                    self._send_json(200, {"ok": True, "snapshot": json_snapshot(snap)})
                    return
                if action in {"resume", "continue"}:
                    wait_ms = float(body.get("wait_ms", 0))
                    cursor = int(body.get("cursor", 0))
                    snap = self.service.poll(rid, wait_ms=wait_ms, cursor=cursor)
                    self._send_json(200, {"ok": True, "snapshot": json_snapshot(snap)})
                    return

            if path in {"/v1/ops/index", "/v1/index/current"}:
                version = body.get("version") or body.get("current")
                if not version:
                    raise ValueError("version is required")
                result = self.service.set_current_index(str(version))
                self._send_json(200, {"ok": True, "index": result})
                return
            if path in {"/v1/ops/corrupt/clear", "/v1/ops/segments/clear-corrupt"}:
                result = self.service.clear_corrupt(body.get("key"), digest=body.get("digest"))
                self._send_json(200, {"ok": True, "result": result})
                return
            self._send_error_json(404, "not_found", path=path)
        except RequestNotFound as exc:
            self._send_error_json(404, "request_not_found", detail=str(exc))
        except RequestExists as exc:
            self._send_error_json(409, "request_exists", detail=str(exc))
        except VersionNotFound as exc:
            self._send_error_json(404, "version_not_found", detail=str(exc))
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            self._send_error_json(400, "bad_request", error_type=type(exc).__name__, detail=str(exc))
        except Exception as exc:
            self._send_error_json(500, "internal_error", error_type=type(exc).__name__, detail=str(exc))


def serve_cli(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Cold archive range reader HTTP service")
    parser.add_argument("--fixture", type=Path, help="fixture directory or events.json containing policy/indexes")
    parser.add_argument("--policy", type=Path, help="JSON policy file if not using --fixture")
    parser.add_argument("--media-url", default="http://127.0.0.1:8090")
    parser.add_argument("--object-prefix", default="/fixture/objects")
    parser.add_argument("--state-dir", type=Path, default=Path(".cold-reader-state"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--poll-interval", type=float, default=0.01)
    args = parser.parse_args(argv)

    if args.fixture:
        _, policy = IndexRegistry.from_fixture(args.fixture)
    elif args.policy:
        policy = json.loads(args.policy.read_text(encoding="utf-8"))
    else:
        parser.error("one of --fixture or --policy is required")

    media = HttpMediaClient(args.media_url, object_prefix=args.object_prefix)
    service = ColdReaderService(
        media,
        policy=policy,
        state_dir=args.state_dir,
        poll_interval=args.poll_interval,
    )
    httpd = ColdReaderHttpServer((args.host, args.port), service)
    print(f"cold reader listening on http://{args.host}:{args.port}", flush=True)
    try:
        httpd.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        service.close()
