"""Media clients for the cold-archive read protocol."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import http.client
import json
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Protocol


class MediaError(RuntimeError):
    """Definitive or transport-level media failure."""


class ReceiptLost(MediaError):
    """The request bytes reached the device but no HTTP response was received."""


class MediaResponseError(MediaError):
    def __init__(self, status: int, code: str | None, payload: Any):
        self.status = status
        self.code = code
        self.payload = payload
        super().__init__(f"media HTTP {status}: {code or payload!r}")


@dataclass
class MediaReply:
    status: int
    payload: dict[str, Any]

    @property
    def state(self) -> str:
        return str(self.payload.get("state", "unknown"))

    @property
    def file(self) -> str | None:
        value = self.payload.get("file")
        return None if value is None else str(value)


class MediaClient(Protocol):
    def apply_read(self, pack: str, segment: str) -> MediaReply: ...

    def query_read(self, pack: str, segment: str) -> MediaReply: ...

    def fetch_file(self, file_ref: str, destination: Path) -> tuple[str, int]: ...


class HttpMediaClient:
    """Standard-library HTTP adapter.

    ``object_prefix`` defaults to the repository fixture's raw object endpoint.
    A real deployment can pass an absolute ``object_url_template`` or an
    object prefix matching its gateway.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 3.0,
        object_prefix: str = "/fixture/objects",
        object_url_template: str | None = None,
        user_agent: str = "coldarchive/1.0",
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.object_prefix = object_prefix.rstrip("/")
        self.object_url_template = object_url_template
        self.user_agent = user_agent
        self._lock = threading.Lock()
        # Fixture paths already known to omit POST recordings.  This is scoped
        # to the exact physical segment; it must not suppress a scripted POST
        # for some later segment.
        self.post_unrecorded_keys: set[tuple[str, str]] = set()

    def _url(self, path: str) -> str:
        return self.base_url + path

    def _request_json(self, method: str, path: str) -> MediaReply:
        request = urllib.request.Request(
            self._url(path),
            method=method,
            headers={"Accept": "application/json", "User-Agent": self.user_agent},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                status = response.status
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            payload = self._json(raw)
            code = payload.get("state") if isinstance(payload, dict) else None
            raise MediaResponseError(exc.code, code, payload) from exc
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            ConnectionError,
            socket.timeout,
            OSError,
        ) as exc:
            # A dropped response after the server read the request is exactly
            # the receipt-lost case.  URLError wraps RemoteDisconnected.
            reason = getattr(exc, "reason", exc)
            receipt_lost_types = (
                http.client.RemoteDisconnected,
                ConnectionResetError,
                BrokenPipeError,
                TimeoutError,
                socket.timeout,
            )
            if isinstance(reason, receipt_lost_types) or isinstance(exc, receipt_lost_types):
                raise ReceiptLost(str(reason)) from exc
            raise MediaError(f"{method} {path} failed: {reason}") from exc

        return MediaReply(status, self._json(raw))

    @staticmethod
    def _json(raw: bytes) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MediaError(f"media returned non-JSON reply: {raw[:80]!r}") from exc
        return value if isinstance(value, dict) else {"value": value}

    def apply_read(self, pack: str, segment: str) -> MediaReply:
        quoted = f"/media/{urllib.parse.quote(pack, safe='')}/read/{urllib.parse.quote(segment, safe='')}"
        identity = (pack, segment)
        with self._lock:
            skip_post = identity in self.post_unrecorded_keys
        if skip_post:
            return MediaReply(200, {"state": "unmatched", "learned_get_fallback": True})
        try:
            return self._request_json("POST", quoted)
        except MediaResponseError as exc:
            if exc.status == 404 and exc.code == "unmatched":
                with self._lock:
                    self.post_unrecorded_keys.add(identity)
            raise

    def query_read(self, pack: str, segment: str) -> MediaReply:
        quoted = f"/media/{urllib.parse.quote(pack, safe='')}/read/{urllib.parse.quote(segment, safe='')}"
        return self._request_json("GET", quoted)

    def file_url(self, file_ref: str) -> str:
        if re_match_url(file_ref):
            return file_ref
        if self.object_url_template:
            return self.object_url_template.format(file=urllib.parse.quote(file_ref))
        if file_ref.startswith("/"):
            return self.base_url + file_ref
        if file_ref.startswith("fixture/") or file_ref.startswith("media/"):
            return self.base_url + "/" + file_ref
        if file_ref.startswith("objects/"):
            return self.base_url + "/fixture/" + file_ref
        return self.base_url + self.object_prefix + "/" + urllib.parse.quote(file_ref)

    def fetch_file(self, file_ref: str, destination: Path) -> tuple[str, int]:
        url = self.file_url(file_ref)
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        digest = hashlib.sha256()
        size = 0
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response, destination.open("wb") as out:
                while True:
                    block = response.read(128 * 1024)
                    if not block:
                        break
                    out.write(block)
                    digest.update(block)
                    size += len(block)
                out.flush()
        except Exception:
            try:
                destination.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return digest.hexdigest(), size


def re_match_url(value: str) -> bool:
    parsed = urllib.parse.urlsplit(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


class StaticMediaClient:
    """In-memory media port useful in unit tests."""

    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects = dict(objects or {})
        self.applied: list[tuple[str, str]] = []
        self.queries: list[tuple[str, str]] = []
        self.files: list[str] = []

    def apply_read(self, pack: str, segment: str) -> MediaReply:
        self.applied.append((pack, segment))
        return MediaReply(202, {"state": "accepted"})

    def query_read(self, pack: str, segment: str) -> MediaReply:
        self.queries.append((pack, segment))
        key = f"{pack}/{segment}"
        if key in self.objects:
            return MediaReply(200, {"state": "ready", "file": key})
        return MediaReply(200, {"state": "accepted"})

    def fetch_file(self, file_ref: str, destination: Path) -> tuple[str, int]:
        self.files.append(file_ref)
        try:
            data = self.objects[file_ref]
        except KeyError as exc:
            raise MediaResponseError(404, "absent", {"state": "absent"}) from exc
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        return hashlib.sha256(data).hexdigest(), len(data)
