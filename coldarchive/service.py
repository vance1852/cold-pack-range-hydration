"""Thread-safe cold-segment reading, hot/cold accounting and recovery."""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from .indexing import IndexRegistry, VersionNotFound
from .media import MediaError, MediaResponseError

LOG = logging.getLogger("coldarchive")

NEW = "new"
WAITING_CAPACITY = "waiting_capacity"
WAITING_MEDIA = "waiting_media"
POSTING = "posting"
POLLING = "polling"
FETCHING = "fetching"
VERIFYING = "verifying"
RECONCILING = "reconciling"
VERIFIED = "verified"
CORRUPT = "corrupt"
MEDIA_FAILED = "media_failed"
EVICTED = "evicted"
CORRUPT_CLEARED = "corrupt_cleared"

ACTIVE_MEDIA_STATES = {POSTING, POLLING, FETCHING, VERIFYING}
SPACE_STATES = {
    WAITING_MEDIA,
    POSTING,
    POLLING,
    FETCHING,
    VERIFYING,
    RECONCILING,
    MEDIA_FAILED,
    VERIFIED,
    CORRUPT,
}
ZERO_SPACE_STATES = {NEW, WAITING_CAPACITY, EVICTED, CORRUPT_CLEARED}

PRIORITY_RANK = {"field": 0, "urgent": 0, "emergency": 0, "high": 0, "normal": 1, "low": 2}
FIELD_PRIORITIES = {0}

REASON_ZH = {
    "verified": "已校验，可读取",
    "hot_capacity": "热盘容量不足，正在排队或等待读者释放",
    "evicted": "未被读取者占住的已校验段已逐出，需要重新回温",
    "corrupt": "段校验失败，数据损坏",
    "checksum_mismatch": "段校验失败，数据损坏",
    "corrupt_cleared": "损坏数据已清除，需要重新取回",
    "parallel_limit": "介质并行处理份额已满",
    "cold_window_quota": "冷读时间窗字节份额不足",
    "media_dispatch_queued": "已预留热盘位置，等待介质分派份额",
    "posting": "正在申请介质取回",
    "polling": "介质仍在回传/回温中",
    "fetching": "正在下载已就绪段",
    "verifying": "正在校验段摘要",
    "in_transit": "段仍在途/回温中",
    "reconciling": "重启后正在与介质对账取回结果",
    "receipt_lost_reconciling": "受理回执丢失，已计入介质消耗，正在通过 GET 对账",
    "media_error": "介质请求失败，将重试",
    "not_indexed": "该范围在索引版本中没有映射",
    "digest_conflict": "索引摘要与已核验字节冲突",
}


class RequestNotFound(KeyError):
    pass


class RequestExists(RuntimeError):
    pass


class RealClock:
    def wall(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class ClockAdapter:
    """Allow test clocks that only implement ``now()``/``sleep()``."""

    def __init__(self, clock: Any):
        self._clock = clock

    def wall(self) -> float:
        if hasattr(self._clock, "wall"):
            return float(self._clock.wall())
        if hasattr(self._clock, "time"):
            return float(self._clock.time())
        return float(self._clock.now())

    def monotonic(self) -> float:
        if hasattr(self._clock, "monotonic"):
            return float(self._clock.monotonic())
        if hasattr(self._clock, "now"):
            return float(self._clock.now())
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        if hasattr(self._clock, "sleep"):
            self._clock.sleep(seconds)
        else:
            time.sleep(seconds)


class ColdReaderService:
    """Coalesce physical reads while keeping reader promises explicit.

    The service is safe to call from multiple threads.  A scheduler thread and
    short-lived media workers are daemon threads; call :meth:`close` for a clean
    shutdown.
    """

    def __init__(
        self,
        media: Any,
        policy: dict[str, Any] | None = None,
        state_dir: str | Path | None = None,
        *,
        indexes: dict[str, Any] | None = None,
        current_version: str | None = None,
        clock: Any | None = None,
        poll_interval: float = 0.01,
        media_timeout: float = 10.0,
        retry_backoff: float = 0.05,
        reclaim_corrupt_under_pressure: bool = True,
        preempt_normal_reservations: bool = False,
        synchronous: bool = False,
        start: bool = True,
    ):
        self.media = media
        self.clock = ClockAdapter(clock) if clock is not None else RealClock()
        self.poll_interval = poll_interval
        self.media_timeout = media_timeout
        self.retry_backoff = retry_backoff
        self.reclaim_corrupt_under_pressure = reclaim_corrupt_under_pressure
        self.preempt_normal_reservations = preempt_normal_reservations
        self.synchronous = synchronous
        self.state_dir = Path(state_dir or tempfile.mkdtemp(prefix="cold-reader-"))
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "tmp").mkdir(exist_ok=True)
        (self.state_dir / "verified").mkdir(exist_ok=True)
        (self.state_dir / "quarantine").mkdir(exist_ok=True)
        self.state_path = self.state_dir / "cold_reader_state.json"

        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._stop_event = threading.Event()
        self._closed = False
        self._workers: set[threading.Thread] = set()
        self._scheduler: threading.Thread | None = None

        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text(encoding="utf-8"))
            persisted_policy = dict(self.state.get("policy") or {})
            if policy is not None:
                # Explicit constructor overrides win over persisted tuning.
                # This keeps test/ops tuning of hot/cold/parallel limits usable
                # across a restart against the same state directory.
                persisted_policy.update(policy)
            self.policy = persisted_policy
            raw_indexes = indexes if indexes is not None else policy.get("indexes") if policy is not None else None
            if raw_indexes is None:
                raw_indexes = self.state["indexes"]
            self.state["policy"] = self._jsonable_policy()
            self.state["indexes"] = raw_indexes
            self.registry = IndexRegistry.from_dict(raw_indexes, current_version or self.state.get("current_version"))
        else:
            if policy is None:
                if indexes is None:
                    raise ValueError("policy or indexes is required for a new cold reader state")
                policy = {}
            self.policy = dict(policy)
            raw_indexes = indexes if indexes is not None else policy.get("indexes")
            if not raw_indexes:
                raise ValueError("policy.indexes or indexes is required")
            self.registry = IndexRegistry.from_dict(raw_indexes, current_version)
            self.state = {
                "schema": 1,
                "policy": self._jsonable_policy(),
                "indexes": raw_indexes,
                "current_version": self.registry.current,
                "seq": 0,
                "requests": {},
                "segments": {},
                "meter_events": [],
                "lifetime_committed_bytes": 0,
                "media_calls": {"post": 0, "get": 0, "fetch": 0, "receipt_lost": 0},
                "audit": [],
                "recovery": None,
                "created_wall": self.clock.wall(),
            }
            self._save_locked()

        self.cold_window_ms = int(self.policy.get("cold_window_ms", 1000))
        self.cold_bytes = int(self.policy.get("cold_bytes", 1 << 30))
        self.parallel = max(1, int(self.policy.get("parallel", 1)))
        self.hot_bytes = max(1, int(self.policy.get("hot_bytes", 1 << 30)))

        self._recover_locked()
        if start and not self.synchronous:
            self.start()

    # ---------- lifecycle ----------

    def start(self) -> None:
        with self._lock:
            if self._scheduler and self._scheduler.is_alive():
                return
            self._stop_event.clear()
            self._scheduler = threading.Thread(target=self._schedule_loop, name="cold-reader-scheduler", daemon=True)
            self._scheduler.start()
        self.pump()

    def __enter__(self) -> "ColdReaderService":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def restart(self, **overrides: Any) -> "ColdReaderService":
        """Close and reopen durable state through a new service instance."""
        init = dict(
            media=self.media,
            state_dir=self.state_dir,
            clock=self.clock,
            poll_interval=self.poll_interval,
            media_timeout=self.media_timeout,
            retry_backoff=self.retry_backoff,
            reclaim_corrupt_under_pressure=self.reclaim_corrupt_under_pressure,
            preempt_normal_reservations=self.preempt_normal_reservations,
            synchronous=self.synchronous,
            start=True,
        )
        init.update(overrides)
        self.close()
        return ColdReaderService(**init)

    # ---------- persistence / recovery ----------

    def _jsonable_policy(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.policy, default=str))

    def _save_locked(self) -> None:
        if getattr(self, "_closed", False):
            # A timed-out worker from a previous generation must not overwrite
            # state written by a reopened service on the same directory.
            return
        self.state["current_version"] = self.registry.current
        fd, tmp_name = tempfile.mkstemp(prefix=".state.", suffix=".tmp", dir=self.state_dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.state, handle, ensure_ascii=False, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.state_path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def _audit_locked(self, event: str, **fields: Any) -> None:
        entry = {"at_wall": self.clock.wall(), "event": event}
        entry.update(fields)
        audit = self.state.setdefault("audit", [])
        audit.append(entry)
        if len(audit) > 1000:
            del audit[:-1000]

    def _recover_locked(self) -> None:
        now = self.clock.wall()
        self._expire_requests_locked(now)
        reusable: list[str] = []
        reconciling: list[str] = []
        corrupt: list[str] = []

        # Rebuild promises from durable active requests rather than trusting
        # a potentially stale promise list.
        for seg in self.state["segments"].values():
            seg["promises"] = []
        for req in self.state["requests"].values():
            req_active = req.get("state") == "active" and float(req.get("deadline_wall", 0)) > now
            if req_active:
                for item in req.get("items", []):
                    # Crash/manual recovery can leave a terminal marker without
                    # a durable chunk sequence.  Rebind it to the verified file.
                    if item.get("terminal") == "verified" and item.get("chunk_seq") is None:
                        item["terminal"] = None
                        item["reason"] = None
                    key = item.get("key")
                    if key and item.get("mapped", True) and key in self.state["segments"]:
                        promises = self.state["segments"][key].setdefault("promises", [])
                        if req["id"] not in promises:
                            promises.append(req["id"])

        for key, seg in self.state["segments"].items():
            state = seg.get("state")
            rel = seg.get("file")
            path = self.state_dir / rel if rel else None
            if state == VERIFIED:
                if path and path.exists():
                    seg["bytes_on_disk"] = path.stat().st_size
                    seg["state"] = VERIFIED
                    reusable.append(key)
                else:
                    seg["state"] = RECONCILING
                    seg["file"] = None
                    seg["recovery_reason"] = "verified_file_missing"
                    for req in self.state.get("requests", {}).values():
                        for item in req.get("items", []):
                            if item.get("key") == key and item.get("chunk_seq") is not None:
                                item["missing_chunk_seq"] = item["chunk_seq"]
                                item["chunk_seq"] = None
                                if item.get("terminal") == "verified":
                                    item["terminal"] = None
                    reconciling.append(key)
            elif state == CORRUPT:
                if path and path.exists():
                    seg["bytes_on_disk"] = path.stat().st_size
                else:
                    seg["bytes_on_disk"] = 0
                corrupt.append(key)
            elif state in {NEW, WAITING_CAPACITY, EVICTED, CORRUPT_CLEARED, WAITING_MEDIA}:
                # WAITING_MEDIA has a hot reservation but had not necessarily
                # consumed a media/cold-window dispatch token.
                pass
            elif state == MEDIA_FAILED and not seg.get("meter_event_id"):
                seg["next_retry_wall"] = now
            else:
                # POST may have been lost, process may have died during GET or
                # verification.  Do not blindly POST again: reconcile via GET.
                seg["state"] = RECONCILING
                seg["recovery_reason"] = f"restart_from_{state}"
                seg["recovered_wall"] = now
                reconciling.append(key)

        self.state["recovery"] = {
            "at_wall": now,
            "verified_reusable": sorted(reusable),
            "reconciling": sorted(reconciling),
            "corrupt": sorted(corrupt),
        }
        self._save_locked()

    # ---------- index / request API ----------

    def list_indexes(self) -> list[str]:
        return self.registry.names()

    def current_index(self) -> str:
        return self.registry.current

    def set_current_index(self, version: str) -> dict[str, Any]:
        with self._cond:
            old = self.registry.current
            self.registry.set_current(version)
            self.state["current_version"] = version
            self._audit_locked("index_changed", old=old, new=version)
            self._save_locked()
            self._cond.notify_all()
            return {"old": old, "current": version}

    @staticmethod
    def _priority_rank(priority: str | int | None) -> int:
        if priority is None:
            return PRIORITY_RANK["normal"]
        if isinstance(priority, int):
            return priority
        return PRIORITY_RANK.get(str(priority).lower(), PRIORITY_RANK["normal"])

    def submit(
        self,
        *,
        offset: int,
        length: int,
        version: str | None = None,
        wait_ms: int | float = 1000,
        priority: str = "normal",
        request_id: str | None = None,
        object_id: str | None = None,
    ) -> dict[str, Any]:
        with self._cond:
            now = self.clock.wall()
            rid = request_id or f"req-{self.state['seq'] + 1}"
            if rid in self.state["requests"]:
                existing = self.state["requests"][rid]
                if (
                    int(existing["offset"]) != int(offset)
                    or int(existing["length"]) != int(length)
                    or (version is not None and existing["version"] != version)
                ):
                    raise RequestExists(f"request {rid} already exists with different parameters")
                return self.snapshot(rid, cursor=0)

            self.state["seq"] += 1
            ver, resolved = self.registry.resolve(offset, length, version)
            rank = self._priority_rank(priority)
            req = {
                "id": rid,
                "seq": int(self.state["seq"]),
                "object_id": object_id,
                "version": ver.version,
                "pack": ver.pack,
                "offset": int(offset),
                "length": int(length),
                "priority": "field" if rank == 0 else "normal",
                "requested_priority": str(priority),
                "priority_rank": rank,
                "wait_ms": float(wait_ms),
                "created_wall": now,
                "deadline_wall": now + max(0.0, float(wait_ms)) / 1000.0,
                "state": "active",
                "chunk_count": 0,
                "items": [],
            }

            for position, item in enumerate(resolved):
                runtime = item.to_dict()
                runtime.update(
                    {
                        "position": position,
                        "chunk_seq": None,
                        "terminal": None if item.mapped else "unmapped",
                        "reason": None if item.mapped else "not_indexed",
                    }
                )
                req["items"].append(runtime)
                if not item.mapped or item.segment is None:
                    continue
                seg = self.state["segments"].setdefault(
                    item.key,
                    {
                        "key": item.key,
                        "pack": item.pack,
                        "segment": item.segment.name,
                        "length": item.segment.length,
                        "expected_sha256": item.sha256,
                        "state": NEW,
                        "promises": [],
                        "charged_bytes": 0,
                        "bytes_on_disk": 0,
                        "attempts": 0,
                        "created_wall": now,
                    },
                )
                if rid not in seg.setdefault("promises", []):
                    seg["promises"].append(rid)
                if seg.get("state") == VERIFIED:
                    self._assign_chunk_locked(req, runtime, seg)
                elif seg.get("state") == CORRUPT:
                    runtime["terminal"] = "corrupt"
                    runtime["reason"] = "checksum_mismatch"

            self.state["requests"][rid] = req
            self._audit_locked(
                "request_submitted",
                request=rid,
                version=ver.version,
                offset=offset,
                length=length,
                priority=req["priority"],
                wait_ms=wait_ms,
                segments=[i["key"] for i in req["items"] if i.get("mapped")],
            )
            self._save_locked()
            self._cond.notify_all()
        self.pump()
        return self.snapshot(rid, cursor=0)

    def read(self, *, wait_ms: int | float | None = None, cursor: int = 0, **submit_kwargs: Any) -> dict[str, Any]:
        """Submit (or reuse ``request_id``) and collect until settled/deadline."""
        rid = submit_kwargs.get("request_id")
        if rid is None or rid not in self.state["requests"]:
            if wait_ms is not None:
                submit_kwargs.setdefault("wait_ms", wait_ms)
            snap = self.submit(**submit_kwargs)
            rid = snap["id"]
        total_wait = float(wait_ms if wait_ms is not None else submit_kwargs.get("wait_ms", 1000))
        end = self.clock.wall() + max(0.0, total_wait) / 1000.0
        current_cursor = cursor
        while True:
            remaining_ms = max(0.0, (end - self.clock.wall()) * 1000.0)
            snap = self.poll(rid, wait_ms=min(max(total_wait, 0.0), remaining_ms), cursor=current_cursor)
            current_cursor = snap.get("next_cursor", current_cursor)
            if snap.get("complete") or snap.get("state") in {"canceled", "closed", "expired"}:
                break
            if self.clock.wall() >= end:
                break
        return self.snapshot(rid, cursor=0)

    def poll(self, request_id: str, wait_ms: float = 0, cursor: int = 0) -> dict[str, Any]:
        with self._cond:
            if request_id not in self.state["requests"]:
                raise RequestNotFound(request_id)
            end = self.clock.wall() + max(0.0, float(wait_ms)) / 1000.0
            while True:
                self._expire_requests_locked()
                snap = self._snapshot_locked(request_id, cursor=cursor)
                new_chunks = [c for c in snap["chunks"] if int(c["seq"]) > int(cursor)]
                if (
                    new_chunks
                    or snap["complete"]
                    or snap["state"] in {"canceled", "closed", "expired"}
                    or self.clock.wall() >= end
                ):
                    return snap
                timeout = min(max(0.001, self.poll_interval), max(0.0, end - self.clock.wall()))
                self._cond.wait(timeout)

    def snapshot(self, request_id: str, cursor: int = 0, *, include_data: bool = True) -> dict[str, Any]:
        with self._cond:
            self._expire_requests_locked()
            return self._snapshot_locked(request_id, cursor=cursor, include_data=include_data)

    def _assign_chunk_locked(self, req: dict[str, Any], item: dict[str, Any], seg: dict[str, Any]) -> bool:
        if item.get("chunk_seq") is not None:
            return True
        actual = seg.get("sha256")
        expected = (item.get("sha256") or seg.get("expected_sha256") or "").lower()
        if seg.get("state") != VERIFIED or not actual:
            return False
        if expected and actual.lower() != expected:
            # Never reuse bytes under one index digest for a request that was
            # resolved against a conflicting index version.
            item["terminal"] = "digest_mismatch"
            item["reason"] = "index_digest_conflict"
            item["detail"] = f"item expects {expected}, verified segment is {actual}"
            return False
        req["chunk_count"] = int(req.get("chunk_count", 0)) + 1
        item["chunk_seq"] = req["chunk_count"]
        item["terminal"] = "verified"
        item["reason"] = None
        item["verified_sha256"] = actual
        return True

    def _read_chunk_locked(self, item: dict[str, Any]) -> bytes:
        key = item["key"]
        seg = self.state["segments"][key]
        rel = seg.get("file")
        if not rel:
            raise FileNotFoundError(f"segment {key} has no readable hot-store file")
        path = self.state_dir / rel
        data = path.read_bytes()
        start = int(item["slice_offset"])
        return data[start : start + int(item["slice_length"])]

    def _snapshot_locked(self, request_id: str, cursor: int = 0, *, include_data: bool = True) -> dict[str, Any]:
        req = self.state["requests"].get(request_id)
        if req is None:
            raise RequestNotFound(request_id)
        now = self.clock.wall()
        items: list[dict[str, Any]] = []
        chunks: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        complete = True
        error_count = 0

        active_media = self._active_media_count_locked()
        window_used = self._meter_used_locked(now)

        for item in req["items"]:
            key = item["key"]
            seg = self.state["segments"].get(key) if item.get("mapped", True) else None
            if item.get("chunk_seq") is None and not item.get("terminal") and seg and seg.get("state") == VERIFIED:
                # A verification may have won the race with lease expiry.
                # Recording the chunk is safe; lease state only controls pins.
                self._assign_chunk_locked(req, item, seg)
            category, reason, detail = self._reason_locked(req, item, seg, now, active_media, window_used)
            blocker = {
                "corrupt": "corruption",
                "in_transit": "in_flight",
                "ok": "ready",
            }.get(category, category)
            view = {
                "position": item["position"],
                "key": key,
                "pack": item.get("pack"),
                "segment": item.get("segment"),
                "version": req["version"],
                "logical_offset": item["logical_offset"],
                "logical_length": item["logical_length"],
                "chunk_seq": item.get("chunk_seq"),
                "state": "verified" if item.get("chunk_seq") is not None else (item.get("terminal") or reason),
                "category": category,
                "blocker": blocker,
                "reason": reason,
                "message_zh": REASON_ZH.get(reason) or REASON_ZH.get(category) or detail,
                "detail": detail,
                "sha256": item.get("verified_sha256") or item.get("sha256"),
            }
            items.append(view)

            if item.get("chunk_seq") is not None:
                if int(item["chunk_seq"]) > int(cursor):
                    chunk = {
                        "seq": item["chunk_seq"],
                        "key": key,
                        "version": req["version"],
                        "pack": item.get("pack"),
                        "segment": item.get("segment"),
                        "offset": item["logical_offset"],
                        "length": item["logical_length"],
                        "sha256": item.get("verified_sha256"),
                    }
                    if include_data:
                        try:
                            data = self._read_chunk_locked(item)
                            chunk["data"] = data
                            chunk["data_b64"] = base64.b64encode(data).decode("ascii")
                        except OSError as exc:
                            chunk["data_error"] = str(exc)
                    chunks.append(chunk)
            elif item.get("terminal"):
                error_count += 1
            else:
                complete = False
                blocked.append(
                    {
                        "request": request_id,
                        "priority": req.get("priority"),
                        "version": req["version"],
                        "offset": item["logical_offset"],
                        "length": item["logical_length"],
                        "key": key,
                        "category": category,
                        "blocker": blocker,
                        "reason": reason,
                        "message_zh": REASON_ZH.get(reason) or REASON_ZH.get(category) or detail,
                        "detail": detail,
                    }
                )

        chunks.sort(key=lambda c: c["seq"])
        chunk_bytes = sum(int(c["length"]) for c in chunks if "data" in c)
        if req["state"] != "active":
            state = req["state"]
        elif complete:
            state = "complete" if error_count == 0 else "complete_with_errors"
        elif req.get("chunk_count"):
            state = "streaming"
        else:
            state = "waiting"

        deadline = float(req["deadline_wall"])
        return {
            "id": request_id,
            "state": state,
            "request_state": req["state"],
            "active": req["state"] == "active",
            "complete": complete,
            "version": req["version"],
            "pack": req.get("pack"),
            "offset": req["offset"],
            "length": req["length"],
            "priority": req.get("priority"),
            "cursor": cursor,
            "next_cursor": int(req.get("chunk_count", 0)),
            "chunk_count": int(req.get("chunk_count", 0)),
            "returned_bytes_in_page": chunk_bytes,
            "items": items,
            "chunks": chunks,
            "blocked": blocked,
            "error_count": error_count,
            "created_wall": req.get("created_wall"),
            "deadline_wall": deadline,
            "remaining_ms": max(0.0, (deadline - now) * 1000.0),
        }

    def _reason_locked(
        self,
        req: dict[str, Any],
        item: dict[str, Any],
        seg: dict[str, Any] | None,
        now: float,
        active_media: int,
        window_used: int,
    ) -> tuple[str, str, str | None]:
        if item.get("chunk_seq") is not None:
            return "ok", "verified", None
        if item.get("terminal") == "unmapped":
            return "index", "not_indexed", "range has no segment in this index version"
        if item.get("terminal") == "digest_mismatch":
            return "index", "digest_conflict", item.get("detail") or "index digest does not match verified bytes"
        if item.get("terminal") == "corrupt" or (seg and seg.get("state") == CORRUPT):
            return "corrupt", "checksum_mismatch", (seg or {}).get("last_error_detail") or "sha256 verification failed"
        if seg is None:
            if req.get("state") != "active":
                return "request", req["state"], "reader promise ended"
            return "in_transit", "unknown", "segment record unavailable"

        state = seg.get("state")
        if state == WAITING_CAPACITY or state == NEW:
            return "capacity", "hot_capacity", f"hot disk limit {self.hot_bytes} bytes"
        if state == EVICTED:
            return "capacity", "evicted", "verified segment was reclaimed and will be refetched"
        if state == CORRUPT_CLEARED:
            return "corrupt", "corrupt_cleared", "quarantined bytes were cleared; refetch required"
        if state == MEDIA_FAILED:
            detail = seg.get("last_error_detail") or seg.get("last_error") or "media request failed"
            if "quota" in str(seg.get("last_error", "")):
                return "quota", "media_quota", detail
            return "media_error", "media_error", detail
        if state == WAITING_MEDIA:
            charged = int(seg.get("charged_bytes") or seg.get("length") or 0)
            if active_media >= self.parallel:
                return "quota", "parallel_limit", f"{active_media}/{self.parallel} media slots busy"
            if window_used + charged > self.cold_bytes:
                return "quota", "cold_window_quota", f"{window_used}+{charged} > {self.cold_bytes} in {self.cold_window_ms}ms"
            return "quota", "media_dispatch_queued", seg.get("dispatch_reason")
        if state == RECONCILING:
            return "in_transit", "reconciling", seg.get("recovery_reason") or "restart recovery pending"
        if state in ACTIVE_MEDIA_STATES:
            if seg.get("post_state") == "lost":
                return "in_transit", "receipt_lost_reconciling", "POST receipt was lost; GET is reconciling"
            return "in_transit", state, seg.get("last_error_detail")
        if state == VERIFIED:
            return "ok", "verified", None
        return "in_transit", str(state), None

    # ---------- cancellation / expiry ----------

    def cancel(self, request_id: str) -> dict[str, Any]:
        return self._finish_request(request_id, "canceled")

    def close(self, request_id: str | None = None, timeout: float = 5.0) -> dict[str, Any] | None:  # type: ignore[override]
        # Supports both service.close() shutdown and handle.close(request_id).
        if request_id is None:
            self._shutdown(timeout=timeout)
            return None
        return self._finish_request(request_id, "closed")

    def abort(self, request_id: str) -> dict[str, Any]:
        return self._finish_request(request_id, "aborted")

    def _finish_request(self, request_id: str, terminal_state: str) -> dict[str, Any]:
        with self._cond:
            req = self.state["requests"].get(request_id)
            if req is None:
                raise RequestNotFound(request_id)
            if req.get("state") == "active":
                req["state"] = terminal_state
                req["ended_wall"] = self.clock.wall()
                self._release_promises_locked(request_id)
                self._audit_locked("request_finished", request=request_id, state=terminal_state)
                self._save_locked()
                self._cond.notify_all()
            snap = self._snapshot_locked(request_id, cursor=0, include_data=False)
        self.pump()
        return snap

    def _release_promises_locked(self, request_id: str) -> None:
        req = self.state["requests"].get(request_id)
        if not req:
            return
        for item in req.get("items", []):
            key = item.get("key")
            seg = self.state["segments"].get(key) if key else None
            if not seg or request_id not in seg.get("promises", []):
                continue
            seg["promises"].remove(request_id)
            if seg.get("promises"):
                continue
            state = seg.get("state")
            # A queued hot reservation has no bytes/media operation that must
            # run to completion.  Release it when the last reader promise ends.
            if state in {WAITING_MEDIA, NEW, WAITING_CAPACITY, MEDIA_FAILED}:
                seg["state"] = WAITING_CAPACITY
                seg["charged_bytes"] = 0
                seg["reservation_released"] = self.clock.wall()
                seg["dispatch_reason"] = "promise_released"
            # Active POST/GET/fetch jobs are not killed mid-call.  When they
            # terminate they become unpinned verified/corrupt cache and can be
            # reclaimed by the next admission pass.

    def _expire_requests_locked(self, now: float | None = None) -> None:
        now = self.clock.wall() if now is None else now
        changed = False
        for rid, req in self.state["requests"].items():
            if req.get("state") == "active" and now >= float(req.get("deadline_wall", 0)):
                req["state"] = "expired"
                req["ended_wall"] = now
                self._release_promises_locked(rid)
                self._audit_locked("request_expired", request=rid)
                changed = True
        if changed:
            self._save_locked()
            self._cond.notify_all()

    # The shutdown implementation is named separately because close() has the
    # overloaded request-handle form used by the HTTP/JSON API.
    def _shutdown(self, timeout: float = 5.0) -> None:
        if hasattr(self, "_stop_event"):
            self._stop_event.set()
        scheduler = getattr(self, "_scheduler", None)
        if scheduler:
            scheduler.join(timeout=timeout)
        join_deadline = time.monotonic() + max(0.05, float(timeout))
        with self._lock:
            try:
                self._expire_requests_locked()
                self._save_locked()
            except Exception:
                LOG.exception("save during close failed")
            # After this point, late workers from a timed-out generation must
            # not write state that could clobber a reopened service.
            self._closed = True
            workers = [t for t in self._workers if t is not threading.current_thread()]
        for thread in workers:
            remaining = max(0.0, join_deadline - time.monotonic())
            thread.join(timeout=remaining)
        # Workers discard themselves on exit; do a short bounded drain for the
        # final bookkeeping thread section.  pump() will not spawn after stop.
        while time.monotonic() < join_deadline:
            with self._lock:
                alive = [t for t in self._workers if t.is_alive() and t is not threading.current_thread()]
            if not alive:
                break
            for thread in alive:
                thread.join(timeout=0.02)

    # ---------- scheduling / capacity ----------

    def _schedule_loop(self) -> None:
        while not self._stop_event.wait(self.poll_interval):
            try:
                self.pump()
            except Exception:
                LOG.exception("cold reader pump failed")

    def pump(self) -> list[dict[str, str]]:
        """Run one scheduling pass and start media workers."""
        if self._stop_event.is_set() or self._closed:
            return []
        with self._cond:
            if self._stop_event.is_set() or self._closed:
                return []
            commands = self._plan_locked()
            self._save_locked()
            self._cond.notify_all()

        started: list[dict[str, str]] = []
        for command in commands:
            if self.synchronous:
                self._worker_safe(command["mode"], command["key"])
            else:
                thread = threading.Thread(
                    target=self._worker_safe,
                    args=(command["mode"], command["key"]),
                    name=f"cold-media-{command['mode']}-{command['key'][:48]}",
                    daemon=True,
                )
                with self._lock:
                    self._workers.add(thread)
                thread.start()
            started.append(command)
        return started

    def _worker_safe(self, mode: str, key: str) -> None:
        try:
            if mode == "reconcile":
                self._reconcile(key)
            else:
                self._retrieve(key)
        except Exception as exc:
            LOG.exception("media worker failed for %s", key)
            self._media_failure(key, exc, rollback_meter=False)
        finally:
            with self._lock:
                self._cond.notify_all()
            try:
                self.pump()
            except Exception:
                LOG.exception("post-worker pump failed")
            finally:
                # Discard only after the final pump/save, so close() can join
                # every thread that may still write durable state.
                with self._lock:
                    self._workers.discard(threading.current_thread())
                    self._cond.notify_all()

    def _sorted_active_requests_locked(self) -> list[dict[str, Any]]:
        reqs = [r for r in self.state["requests"].values() if r.get("state") == "active"]
        return sorted(reqs, key=lambda r: (int(r.get("priority_rank", 1)), int(r.get("seq", 0))))

    def _plan_locked(self) -> list[dict[str, str]]:
        now = self.clock.wall()
        self._expire_requests_locked(now)

        for req in self._sorted_active_requests_locked():
            rank = int(req.get("priority_rank", 1))
            for item in req.get("items", []):
                if item.get("chunk_seq") is not None or item.get("terminal"):
                    continue
                key = item["key"]
                if not item.get("mapped", True):
                    item["terminal"] = "unmapped"
                    item["reason"] = "not_indexed"
                    continue
                seg = self.state["segments"].get(key)
                if seg is None:
                    continue
                if seg.get("state") == VERIFIED:
                    self._assign_chunk_locked(req, item, seg)
                    continue
                if seg.get("state") == CORRUPT:
                    item["terminal"] = "corrupt"
                    item["reason"] = "checksum_mismatch"
                    continue
                if seg.get("state") == MEDIA_FAILED:
                    if now < float(seg.get("next_retry_wall", 0)):
                        continue
                    seg["state"] = WAITING_MEDIA
                if seg.get("state") in {NEW, WAITING_CAPACITY, EVICTED, CORRUPT_CLEARED}:
                    self._admit_locked(seg, req["id"], rank, now)

        commands: list[dict[str, str]] = []
        active_media = self._active_media_count_locked()
        window_used = self._meter_used_locked(now)

        candidates = list(self.state["segments"].values())
        candidates.sort(key=lambda s: (*self._segment_priority_key_locked(s), s.get("created_wall", 0), s["key"]))

        for seg in candidates:
            state = seg.get("state")
            key = seg["key"]
            if state == RECONCILING:
                if active_media >= self.parallel:
                    seg["dispatch_reason"] = "parallel_limit"
                    continue
                seg["state"] = POLLING
                seg["recovery_mode"] = True
                seg["poll_started_wall"] = now
                active_media += 1
                commands.append({"mode": "reconcile", "key": key})
                continue

            if state != WAITING_MEDIA:
                continue
            charged = int(seg.get("charged_bytes") or seg.get("length") or 0)
            if active_media >= self.parallel:
                seg["dispatch_reason"] = "parallel_limit"
                continue
            if window_used + charged > self.cold_bytes:
                seg["dispatch_reason"] = "cold_window_quota"
                continue

            event_id = self.state["seq"] = int(self.state["seq"]) + 1
            event = {"id": f"meter-{event_id}", "at_wall": now, "key": key, "bytes": charged}
            self.state["meter_events"].append(event)
            self.state["lifetime_committed_bytes"] = int(self.state.get("lifetime_committed_bytes", 0)) + charged
            seg["state"] = POSTING
            seg["meter_event_id"] = event["id"]
            seg["dispatched_wall"] = now
            seg["attempts"] = int(seg.get("attempts", 0)) + 1
            seg["dispatch_reason"] = None
            active_media += 1
            window_used += charged
            commands.append({"mode": "retrieve", "key": key})

        return commands

    def _admit_locked(self, seg: dict[str, Any], request_id: str, priority_rank: int, now: float) -> bool:
        size = int(seg.get("length") or 0)
        if size <= 0:
            seg["state"] = MEDIA_FAILED
            seg["last_error"] = "invalid_segment_length"
            return False
        if seg.get("state") in SPACE_STATES and int(seg.get("charged_bytes") or 0) > 0:
            if seg.get("state") == MEDIA_FAILED:
                seg["state"] = WAITING_MEDIA
            return True

        if self._hot_used_locked(now) + size > self.hot_bytes:
            self._reclaim_hot_locked(size, priority_rank, request_id, now)

        if self._hot_used_locked(now) + size <= self.hot_bytes:
            seg["state"] = WAITING_MEDIA
            seg["charged_bytes"] = size
            seg["waiting_since"] = now
            seg["admitted_for"] = request_id
            seg["dispatch_reason"] = None
            return True

        seg["state"] = WAITING_CAPACITY
        seg["charged_bytes"] = 0
        seg["dispatch_reason"] = "hot_capacity"
        return False

    def _reclaim_hot_locked(self, need: int, priority_rank: int, current_rid: str, now: float) -> int:
        reclaimed = 0
        candidates = [s for s in self.state["segments"].values() if s.get("state") == VERIFIED and not s.get("promises")]
        candidates.sort(key=lambda s: float(s.get("last_used_wall") or s.get("verified_wall") or 0))
        for seg in candidates:
            if self._hot_available_locked(now) >= need:
                break
            reclaimed += self._delete_segment_file_locked(seg, EVICTED, "evicted_unpinned_lru")

        if self.reclaim_corrupt_under_pressure:
            candidates = [s for s in self.state["segments"].values() if s.get("state") == CORRUPT]
            candidates.sort(key=lambda s: float(s.get("corrupt_wall") or 0))
            for seg in candidates:
                if self._hot_available_locked(now) >= need:
                    break
                reclaimed += self._clear_corrupt_record_locked(seg, "pressure_reclaim")

        if priority_rank == 0 and self.preempt_normal_reservations:
            candidates = [
                s
                for s in self.state["segments"].values()
                if s.get("state") == WAITING_MEDIA and self._segment_priority_rank_locked(s) > 0
            ]
            candidates.sort(key=lambda s: float(s.get("waiting_since") or 0), reverse=True)
            for seg in candidates:
                if self._hot_available_locked(now) >= need:
                    break
                freed = int(seg.get("charged_bytes") or 0)
                seg["state"] = WAITING_CAPACITY
                seg["charged_bytes"] = 0
                seg["preempted_for"] = current_rid
                seg["dispatch_reason"] = "preempted_by_field_request"
                self._audit_locked("reservation_preempted", key=seg["key"], freed_bytes=freed)
                reclaimed += freed

        return reclaimed

    def _delete_segment_file_locked(self, seg: dict[str, Any], new_state: str, reason: str) -> int:
        size = int(seg.get("bytes_on_disk") or 0)
        rel = seg.get("file")
        if rel:
            try:
                (self.state_dir / rel).unlink(missing_ok=True)
            except OSError:
                LOG.exception("failed deleting %s", rel)
        seg["file"] = None
        seg["bytes_on_disk"] = 0
        seg["state"] = new_state
        seg["evict_reason"] = reason
        seg["evicted_wall"] = self.clock.wall()
        # Historical snapshots must not point at a deleted file.  Clear chunk
        # markers; the item will now report the evicted/capacity/reconcile
        # state instead of pretending verified bytes are still readable.
        for req in self.state.get("requests", {}).values():
            for item in req.get("items", []):
                if item.get("key") == seg.get("key") and item.get("chunk_seq") is not None:
                    item["evicted_chunk_seq"] = item["chunk_seq"]
                    item["chunk_seq"] = None
                    if item.get("terminal") == "verified":
                        item["terminal"] = None
        self._audit_locked("segment_evicted", key=seg.get("key"), bytes=size, reason=reason)
        return size

    def _clear_corrupt_record_locked(self, seg: dict[str, Any], reason: str) -> int:
        size = self._delete_segment_file_locked(seg, CORRUPT_CLEARED, reason)
        seg["corrupt_cleared_wall"] = self.clock.wall()
        self._audit_locked("corrupt_cleared", key=seg.get("key"), bytes=size, reason=reason)
        return size

    def clear_corrupt(self, key: str | None = None, *, digest: str | None = None) -> dict[str, Any]:
        with self._cond:
            matches = []
            for seg_key, seg in self.state["segments"].items():
                if seg.get("state") != CORRUPT:
                    continue
                if key is not None and seg_key != key:
                    continue
                if digest is not None and seg.get("sha256") != digest.lower():
                    continue
                matches.append(seg)
            if not matches:
                raise RequestNotFound(f"corrupt segment not found: {key or digest}")
            freed = 0
            for seg in matches:
                freed += self._clear_corrupt_record_locked(seg, "operator_clear")
            self._save_locked()
            self._cond.notify_all()
            result = {"cleared": [s["key"] for s in matches], "freed_bytes": freed}
        self.pump()
        return result

    def _hot_available_locked(self, now: float | None = None) -> int:
        return max(0, self.hot_bytes - self._hot_used_locked(now))

    def _segment_space_locked(self, seg: dict[str, Any]) -> int:
        state = seg.get("state")
        if state in {VERIFIED, CORRUPT}:
            return int(seg.get("bytes_on_disk") or 0)
        if state in ZERO_SPACE_STATES:
            return 0
        if state == MEDIA_FAILED:
            # A failed slot may have been explicitly released even though the
            # segment length is retained for diagnostics.
            return max(int(seg.get("charged_bytes") or 0), int(seg.get("bytes_on_disk") or 0))
        return max(int(seg.get("charged_bytes") or seg.get("length") or 0), int(seg.get("bytes_on_disk") or 0))

    def _hot_used_locked(self, now: float | None = None) -> int:
        return sum(self._segment_space_locked(seg) for seg in self.state["segments"].values())

    def _active_media_count_locked(self) -> int:
        return sum(1 for seg in self.state["segments"].values() if seg.get("state") in ACTIVE_MEDIA_STATES)

    def _meter_used_locked(self, now: float | None = None) -> int:
        now = self.clock.wall() if now is None else now
        cutoff = now - self.cold_window_ms / 1000.0
        events = self.state.get("meter_events", [])
        live = [e for e in events if float(e.get("at_wall", 0)) >= cutoff]
        if len(live) != len(events):
            self.state["meter_events"] = live
        return sum(int(e.get("bytes") or 0) for e in live)

    def _segment_priority_rank_locked(self, seg: dict[str, Any]) -> int:
        ranks = []
        for rid in seg.get("promises", []):
            req = self.state["requests"].get(rid)
            if req and req.get("state") == "active":
                ranks.append(int(req.get("priority_rank", 1)))
        return min(ranks) if ranks else 9

    def _segment_priority_key_locked(self, seg: dict[str, Any]) -> tuple[int, int]:
        rank = self._segment_priority_rank_locked(seg)
        seq = 999999
        for rid in seg.get("promises", []):
            req = self.state["requests"].get(rid)
            if req and req.get("state") == "active":
                seq = min(seq, int(req.get("seq", 0)))
        return rank, seq

    # ---------- media workers ----------

    def _key_parts(self, key: str) -> tuple[str, str]:
        seg = self.state["segments"].get(key)
        if seg:
            return str(seg["pack"]), str(seg["segment"])
        pack, _, name = key.partition("/")
        return pack, name

    def _increment_media_call(self, kind: str) -> None:
        with self._lock:
            counts = self.state.setdefault("media_calls", {})
            counts[kind] = int(counts.get(kind, 0)) + 1

    def _retrieve(self, key: str) -> None:
        pack, segment = self._key_parts(key)
        meter_event_id = None
        with self._lock:
            seg = self.state["segments"][key]
            meter_event_id = seg.get("meter_event_id")

        try:
            self._increment_media_call("post")
            reply = self.media.apply_read(pack, segment)
        except Exception as exc:
            if isinstance(exc, MediaResponseError) and exc.code == "unmatched":
                # Sparse fixture recordings may omit ordinary POST replies; the
                # idempotent GET script still models acceptance/result.
                with self._cond:
                    seg = self.state["segments"].get(key)
                    if seg is not None:
                        seg["post_state"] = "fixture_unmatched_get_fallback"
                        self._audit_locked("apply_unmatched_fallback_get", key=key, status=exc.status)
                        self._save_locked()
                        self._cond.notify_all()
            elif isinstance(exc, MediaError) and "ReceiptLost" in type(exc).__name__:
                self._increment_media_call("receipt_lost")
                with self._cond:
                    seg = self.state["segments"].get(key)
                    if seg is not None:
                        seg["post_state"] = "lost"
                        seg["indoubt"] = True
                        seg["last_error"] = "receipt_lost"
                        seg["last_error_detail"] = str(exc)
                        self._audit_locked("apply_receipt_lost", key=key, meter_event_id=meter_event_id)
                        self._save_locked()
                        self._cond.notify_all()
            else:
                # Definitive pre-acceptance failure: the dispatch did not
                # consume the cold-byte window.
                self._rollback_meter(meter_event_id)
                self._media_failure(key, exc, rollback_meter=False)
                return
        else:
            with self._cond:
                seg = self.state["segments"].get(key)
                if seg is not None:
                    seg["post_state"] = reply.state or "accepted"
                    seg["apply_status"] = reply.status
                    self._save_locked()

        self._poll_and_fetch(key, initial_reconcile=False)

    def _reconcile(self, key: str) -> None:
        self._poll_and_fetch(key, initial_reconcile=True)

    def _rollback_meter(self, event_id: str | None) -> None:
        if not event_id:
            return
        with self._cond:
            event = next((e for e in self.state["meter_events"] if e.get("id") == event_id), None)
            if event is not None:
                self.state["meter_events"] = [e for e in self.state["meter_events"] if e.get("id") != event_id]
                committed = int(self.state.get("lifetime_committed_bytes", 0))
                self.state["lifetime_committed_bytes"] = max(0, committed - int(event.get("bytes") or 0))
            self._save_locked()

    def _poll_and_fetch(self, key: str, *, initial_reconcile: bool) -> None:
        pack, segment = self._key_parts(key)
        deadline = self.clock.wall() + self.media_timeout
        last_error: Exception | None = None

        while not self._stop_event.is_set() and self.clock.wall() < deadline:
            with self._lock:
                seg = self.state["segments"].get(key)
                if seg is None or seg.get("state") not in ACTIVE_MEDIA_STATES:
                    return
                seg["state"] = POLLING
            try:
                self._increment_media_call("get")
                reply = self.media.query_read(pack, segment)
            except MediaResponseError as exc:
                last_error = exc
                if exc.status == 404 and initial_reconcile:
                    # The device has no accepted retrieval record.  Route back
                    # through the scheduler so the fresh POST takes a cold
                    # quota/parallel slot instead of bypassing accounting.
                    with self._cond:
                        seg = self.state["segments"].get(key)
                        if seg is not None:
                            seg["state"] = WAITING_MEDIA
                            seg["recovery_absent"] = True
                            seg["next_retry_wall"] = 0
                            seg["dispatch_reason"] = "reconcile_absent_reapply"
                            self._save_locked()
                    return
                self.clock.sleep(self.poll_interval)
                continue
            except Exception as exc:
                last_error = exc
                self.clock.sleep(self.poll_interval)
                continue

            state = reply.state
            if state == "ready" and reply.file:
                self._fetch_and_verify(key, reply.file)
                return
            if state in {"error", "failed", "absent", "expired", "canceled"}:
                self._media_failure(key, MediaError(f"media state {state}: {reply.payload}"), rollback_meter=False)
                return
            # accepted/processing/warming/pending/... keep polling.
            self.clock.sleep(self.poll_interval)

        self._media_failure(key, MediaError(f"timed out reconciling {key}: {last_error}"), rollback_meter=False)

    def _fetch_and_verify(self, key: str, file_ref: str) -> None:
        with self._lock:
            seg = self.state["segments"][key]
            expected = str(seg["expected_sha256"]).lower()
            expected_length = int(seg.get("length") or 0)
            attempt = int(seg.get("attempts", 1))
            temp_rel = f"tmp/{key.replace('/', '_')}-{os.getpid()}-{threading.get_ident()}-{attempt}.part"
            temp_path = self.state_dir / temp_rel
            seg["state"] = FETCHING
            seg["temp_file"] = temp_rel
            self._save_locked()

        try:
            self._increment_media_call("fetch")
            actual_sha, actual_size = self.media.fetch_file(file_ref, temp_path)
        except Exception as exc:
            self._media_failure(key, exc, rollback_meter=False)
            return

        with self._lock:
            seg = self.state["segments"].get(key)
            if seg is None:
                return
            seg["state"] = VERIFYING
            seg["bytes_on_disk"] = actual_size
            expected_digests = {str(expected).lower()}
            for rid in seg.get("promises", []):
                req = self.state["requests"].get(rid)
                if not req:
                    continue
                for item in req.get("items", []):
                    if item.get("key") == key and item.get("sha256"):
                        expected_digests.add(str(item["sha256"]).lower())
            length_ok = expected_length <= 0 or actual_size == expected_length
            digest_ok = actual_sha.lower() in expected_digests
            if not digest_ok or not length_ok:
                q_rel = f"quarantine/{actual_sha or 'nohash'}-{os.getpid()}-{attempt}.bin"
                q_path = self.state_dir / q_rel
                q_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    os.replace(temp_path, q_path)
                except OSError:
                    LOG.exception("quarantine move failed")
                seg["state"] = CORRUPT
                seg["file"] = q_rel
                seg["sha256"] = actual_sha
                seg["bytes_on_disk"] = actual_size if q_path.exists() else 0
                seg["corrupt_wall"] = self.clock.wall()
                seg["last_error"] = "checksum_mismatch" if not digest_ok else "length_mismatch"
                seg["last_error_detail"] = (
                    f"expected sha256={expected} length={expected_length}; "
                    f"actual sha256={actual_sha} length={actual_size}"
                )
                for rid in list(seg.get("promises", [])):
                    req = self.state["requests"].get(rid)
                    if not req:
                        continue
                    for item in req.get("items", []):
                        if item.get("key") == key and item.get("terminal") is None:
                            item["terminal"] = "corrupt"
                            item["reason"] = seg["last_error"]
                self._audit_locked(
                    "segment_corrupt",
                    key=key,
                    expected_sha256=expected,
                    actual_sha256=actual_sha,
                    bytes=actual_size,
                    file=q_rel,
                )
                self._save_locked()
                self._cond.notify_all()
                return

            # Segment-specific path: identical bytes under two physical
            # pack/segment identities must not let evicting one key remove a
            # file referenced by another segment record.
            key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
            final_rel = f"verified/{key_hash}-{actual_sha}.bin"
            final_path = self.state_dir / final_rel
            final_path.parent.mkdir(parents=True, exist_ok=True)
            if final_path.exists():
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            else:
                os.replace(temp_path, final_path)
            seg["state"] = VERIFIED
            seg["file"] = final_rel
            seg["sha256"] = actual_sha
            seg["bytes_on_disk"] = final_path.stat().st_size if final_path.exists() else actual_size
            seg["verified_wall"] = self.clock.wall()
            seg["last_used_wall"] = seg["verified_wall"]
            seg["last_error"] = None
            seg["last_error_detail"] = None
            seg["indoubt"] = False
            for rid in list(seg.get("promises", [])):
                req = self.state["requests"].get(rid)
                if not req or req.get("state") != "active":
                    continue
                for item in req.get("items", []):
                    if item.get("key") == key and item.get("chunk_seq") is None and not item.get("terminal"):
                        self._assign_chunk_locked(req, item, seg)
            self._audit_locked("segment_verified", key=key, sha256=actual_sha, bytes=actual_size)
            self._save_locked()
            self._cond.notify_all()

    def _media_failure(self, key: str, exc: Exception, *, rollback_meter: bool) -> None:
        with self._cond:
            seg = self.state["segments"].get(key)
            if seg is None:
                return
            event_id = seg.get("meter_event_id")
            if rollback_meter:
                self.state["meter_events"] = [e for e in self.state["meter_events"] if e.get("id") != event_id]
            # Keep hot reservation charged: the active reader still has a
            # promised slot unless its lease was released.
            seg["state"] = MEDIA_FAILED
            seg["last_error"] = type(exc).__name__
            seg["last_error_detail"] = str(exc)
            seg["next_retry_wall"] = self.clock.wall() + self.retry_backoff
            self._audit_locked("media_failure", key=key, error=type(exc).__name__, detail=str(exc))
            self._save_locked()
            self._cond.notify_all()

    # ---------- operations / observability ----------

    def status(self, *, include_audit: int = 50) -> dict[str, Any]:
        with self._cond:
            now = self.clock.wall()
            self._expire_requests_locked(now)
            window_used = self._meter_used_locked(now)
            active_media = self._active_media_count_locked()

            verified_bytes = corrupt_bytes = in_flight_bytes = reserved_bytes = 0
            in_flight_remaining_bytes = media_failed_bytes = other_charged_bytes = 0
            segment_views = []
            for key, seg in self.state["segments"].items():
                space = self._segment_space_locked(seg)
                state = seg.get("state")
                actual = int(seg.get("bytes_on_disk") or 0)
                if state == VERIFIED:
                    verified_bytes += actual
                elif state == CORRUPT:
                    corrupt_bytes += actual
                elif state in ACTIVE_MEDIA_STATES or state == RECONCILING:
                    in_flight_bytes += space
                    in_flight_remaining_bytes += max(0, space - actual)
                elif state == WAITING_MEDIA:
                    reserved_bytes += space
                elif state == MEDIA_FAILED:
                    media_failed_bytes += space
                elif state not in ZERO_SPACE_STATES:
                    other_charged_bytes += space
                segment_views.append(
                    {
                        "key": key,
                        "pack": seg.get("pack"),
                        "segment": seg.get("segment"),
                        "state": state,
                        "length": seg.get("length"),
                        "space_bytes": space,
                        "bytes_on_disk": actual,
                        "charged_bytes": seg.get("charged_bytes"),
                        "promises": list(seg.get("promises", [])),
                        "promise_count": len(seg.get("promises", [])),
                        "sha256": seg.get("sha256"),
                        "expected_sha256": seg.get("expected_sha256"),
                        "post_state": seg.get("post_state"),
                        "last_error": seg.get("last_error"),
                        "last_error_detail": seg.get("last_error_detail"),
                        "dispatch_reason": seg.get("dispatch_reason"),
                        "attempts": seg.get("attempts"),
                    }
                )

            readers = []
            pinned_by_reader: dict[str, int] = {}
            for rid, req in self.state["requests"].items():
                if req.get("state") != "active":
                    continue
                keys = []
                pinned = 0
                required = 0
                for item in req.get("items", []):
                    if not item.get("mapped", True):
                        continue
                    key = item["key"]
                    if key in keys:
                        continue
                    keys.append(key)
                    seg = self.state["segments"].get(key)
                    required += int((seg or {}).get("length") or item.get("segment_length") or 0)
                    pinned += self._segment_space_locked(seg) if seg else 0
                pinned_by_reader[rid] = pinned
                readers.append(
                    {
                        "id": rid,
                        "priority": req.get("priority"),
                        "version": req.get("version"),
                        "offset": req.get("offset"),
                        "length": req.get("length"),
                        "state": req.get("state"),
                        "remaining_ms": max(0.0, (float(req.get("deadline_wall", now)) - now) * 1000.0),
                        "segment_keys": keys,
                        "hot_space_bytes_attributed": pinned,
                        "full_segment_required_bytes": required,
                        "chunk_count": req.get("chunk_count", 0),
                    }
                )

            blocked = []
            for rid in self.state["requests"]:
                if self.state["requests"][rid].get("state") != "active":
                    continue
                snap = self._snapshot_locked(rid, cursor=0, include_data=False)
                blocked.extend(snap["blocked"])

            audit = list(self.state.get("audit", []))[-include_audit:]
            return {
                "ok": True,
                "time_wall": now,
                "current_index": self.registry.current,
                "indexes": self.registry.names(),
                "recovery": self.state.get("recovery"),
                "media": {
                    "cold_window_ms": self.cold_window_ms,
                    "cold_bytes_limit": self.cold_bytes,
                    "window_committed_bytes": window_used,
                    "window_available_bytes": max(0, self.cold_bytes - window_used),
                    "parallel_limit": self.parallel,
                    "parallel_active": active_media,
                    "parallel_available": max(0, self.parallel - active_media),
                    "lifetime_committed_bytes": self.state.get("lifetime_committed_bytes", 0),
                    "calls": dict(self.state.get("media_calls", {})),
                    "meter_events": list(self.state.get("meter_events", [])),
                },
                "hot": {
                    "capacity_bytes": self.hot_bytes,
                    "used_bytes": self._hot_used_locked(now),
                    "available_bytes": self._hot_available_locked(now),
                    "verified_bytes": verified_bytes,
                    "corrupt_uncleared_bytes": corrupt_bytes,
                    "in_flight_bytes": in_flight_bytes,
                    "in_flight_remaining_reservation_bytes": in_flight_remaining_bytes,
                    "reserved_waiting_dispatch_bytes": reserved_bytes,
                    "media_failed_reserved_bytes": media_failed_bytes,
                    "other_charged_bytes": other_charged_bytes,
                    "partition_sum_bytes": (
                        verified_bytes
                        + corrupt_bytes
                        + in_flight_bytes
                        + reserved_bytes
                        + media_failed_bytes
                        + other_charged_bytes
                    ),
                    "pinned_bytes_by_active_reader": pinned_by_reader,
                },
                "readers": readers,
                "blocked_ranges": blocked,
                "segments": segment_views,
                "request_ids": list(self.state["requests"]),
                "audit": audit,
                "state_dir": str(self.state_dir),
            }

    def media_status(self) -> dict[str, Any]:
        status = self.status(include_audit=0)
        return status["media"]

    def hot_status(self) -> dict[str, Any]:
        status = self.status(include_audit=0)
        return status["hot"]

    def readers_status(self) -> list[dict[str, Any]]:
        return self.status(include_audit=0)["readers"]

    # Common semantic aliases used by embedders/tests.
    submit_read = submit
    read_range = read
    run_once = pump
    tick = pump
    step = pump
    get_request = snapshot
    get_snapshot = snapshot
    metrics = status
    inspect = status

    def release(self, request_id: str) -> dict[str, Any] | None:
        return self.close(request_id)

    def abort_request(self, request_id: str) -> dict[str, Any]:
        return self.abort(request_id)

    def health(self) -> dict[str, Any]:
        return {"ok": True, "hot": self.hot_status(), "media": self.media_status()}


