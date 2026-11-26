from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from cold_reader import ColdReaderService, HttpMediaClient, IndexRegistry, StaticMediaClient
from coldarchive.server import ColdReaderHttpServer
from device_fixture import FixtureServer

ROOT = Path(__file__).parent
SHARED = ROOT / "fixtures" / "shared-segment-and-expiring-reader"
HOT = ROOT / "fixtures" / "enough-medium-no-hot-space"
GOOD_A = (ROOT / "fixtures/shared-segment-and-expiring-reader/objects/a.bin").read_bytes()
GOOD_B = (ROOT / "fixtures/shared-segment-and-expiring-reader/objects/b.bin").read_bytes()
GOOD_B2 = (ROOT / "fixtures/shared-segment-and-expiring-reader/objects/b2.bin").read_bytes()
GOOD_R = (ROOT / "fixtures/enough-medium-no-hot-space/objects/r.bin").read_bytes()
GOOD_S = (ROOT / "fixtures/enough-medium-no-hot-space/objects/s.bin").read_bytes()


class FixtureHarness:
    def __init__(self, folder: Path):
        self.folder = folder
        self.server = FixtureServer(("127.0.0.1", 0), folder)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.005}, daemon=True)
        self.state_dir = tempfile.TemporaryDirectory()
        self.service: ColdReaderService | None = None

    def __enter__(self):
        self.thread.start()
        _, policy = IndexRegistry.from_fixture(self.folder)
        self.policy = policy
        self.media = HttpMediaClient(self.url, timeout=2.0)
        self.service = ColdReaderService(
            self.media,
            policy=policy,
            state_dir=self.state_dir.name,
            poll_interval=0.005,
        )
        return self

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def journal(self):
        with urllib.request.urlopen(self.url + "/fixture/journal", timeout=2) as response:
            return json.loads(response.read())

    def make_service(self, **policy_overrides):
        policy = dict(self.policy)
        policy.update(policy_overrides)
        return ColdReaderService(
            HttpMediaClient(self.url, timeout=2.0),
            policy=policy,
            state_dir=self.state_dir.name,
            poll_interval=0.005,
        )

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.service:
                self.service.close()
        finally:
            self.server.shutdown()
            self.thread.join(timeout=3)
            self.server.server_close()
            self.state_dir.cleanup()


def wait_for(predicate, timeout=2.0, interval=0.005):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError("condition timed out")


class IndexResolutionTest(unittest.TestCase):
    def test_versioned_overlap_resolution_is_immutable_shape(self):
        registry, _ = IndexRegistry.from_fixture(SHARED)
        version, items = registry.resolve(10, 4, "map/v1")
        self.assertEqual(version.pack, "P1")
        self.assertEqual([i.key for i in items], ["P1/b"])
        self.assertEqual((items[0].slice_offset, items[0].slice_length), (2, 4))

        _, whole = registry.resolve(0, 24, "map/v1")
        self.assertEqual([i.segment.name for i in whole], ["a", "b", "c"])


class HotSpaceTest(unittest.TestCase):
    def test_normal_range_waits_until_field_reader_closes(self):
        with FixtureHarness(HOT) as h:
            svc = h.service
            one = svc.read(request_id="one", version="map/v3", offset=0, length=8, wait_ms=300, priority="field")
            self.assertEqual(one["state"], "complete")
            self.assertEqual(one["chunks"][0]["data"], GOOD_R)

            two = svc.submit(request_id="two", version="map/v3", offset=8, length=8, wait_ms=1000)
            snap = svc.poll("two", wait_ms=20)
            self.assertEqual(snap["blocked"][0]["category"], "capacity")
            self.assertEqual(snap["blocked"][0]["reason"], "hot_capacity")
            hot = svc.hot_status()
            self.assertEqual(hot["used_bytes"], 8)
            self.assertIn("one", hot["pinned_bytes_by_active_reader"])

            svc.close("one")
            result = svc.read(request_id="two", wait_ms=300)
            self.assertEqual(result["state"], "complete")
            self.assertEqual(result["chunks"][0]["data"], GOOD_S)
            self.assertNotIn("one", svc.hot_status()["pinned_bytes_by_active_reader"])


class SharedSegmentTest(unittest.TestCase):
    def test_cancel_one_reader_keeps_shared_segment_for_other(self):
        with FixtureHarness(SHARED) as h:
            svc = h.service
            svc.submit(request_id="bulk", version="map/v1", offset=0, length=24, wait_ms=1000)
            wait_for(lambda: svc.snapshot("bulk")["chunk_count"] >= 1)

            crew = svc.submit(
                request_id="crew", version="map/v1", offset=10, length=4, wait_ms=1000, priority="field"
            )
            time.sleep(0.035)
            canceled = svc.cancel("bulk")
            self.assertEqual(canceled["state"], "canceled")

            result = svc.read(request_id="crew", wait_ms=500)
            self.assertEqual(result["state"], "complete")
            self.assertEqual(result["chunks"][0]["data"], GOOD_B[2:6])

            status = svc.status()
            b_seg = next(s for s in status["segments"] if s["key"] == "P1/b")
            self.assertEqual(b_seg["state"], "verified")
            self.assertEqual(b_seg["promises"], ["crew"])
            journal = h.journal()
            self.assertEqual([j["path"] for j in journal if j["method"] == "POST"].count("/media/P1/read/b"), 1)

    def test_dropped_post_is_counted_and_reconciled_by_get(self):
        with FixtureHarness(SHARED) as h:
            svc = h.service
            result = svc.read(request_id="a", version="map/v1", offset=0, length=8, wait_ms=500)
            self.assertEqual(result["chunks"][0]["data"], GOOD_A)
            media = svc.media_status()
            self.assertEqual(media["calls"]["receipt_lost"], 1)
            self.assertGreaterEqual(media["window_committed_bytes"], 8)
            journal = h.journal()
            self.assertTrue(any(j["method"] == "POST" and j["path"] == "/media/P1/read/a" and j["drop"] for j in journal))
            self.assertTrue(any(j["method"] == "GET" and j["path"] == "/media/P1/read/a" for j in journal))

    def test_corrupt_segment_does_not_invalidate_adjacent_verified_ranges(self):
        with FixtureHarness(SHARED) as h:
            # Give all three 8-byte segments hot room so one request observes
            # both successful chunks and the isolated checksum failure.
            svc = h.service
            svc.hot_bytes = 24
            result = svc.read(request_id="all", version="map/v1", offset=0, length=24, wait_ms=700)
            self.assertEqual(result["state"], "complete_with_errors")
            payload = {c["segment"]: c["data"] for c in result["chunks"]}
            self.assertEqual(payload["a"], GOOD_A)
            self.assertEqual(payload["b"], GOOD_B)
            bad = next(i for i in result["items"] if i["segment"] == "c")
            self.assertEqual(bad["category"], "corrupt")
            self.assertEqual(bad["reason"], "checksum_mismatch")
            status = svc.status()
            self.assertGreater(status["hot"]["corrupt_uncleared_bytes"], 0)
            svc.clear_corrupt("P1/c")
            self.assertEqual(svc.hot_status()["corrupt_uncleared_bytes"], 0)
            svc.close()

    def test_request_is_bound_to_original_pack_version(self):
        with FixtureHarness(SHARED) as h:
            svc = h.service
            old = svc.submit(request_id="old", version="map/v1", offset=8, length=8, wait_ms=1000)
            self.assertEqual(old["pack"], "P1")
            svc.set_current_index("map/v2")
            old_result = svc.read(request_id="old", wait_ms=500)
            self.assertEqual(old_result["chunks"][0]["data"], GOOD_B)

            new_result = svc.read(
                request_id="new",
                offset=8,
                length=8,
                wait_ms=500,
            )
            self.assertEqual(new_result["version"], "map/v2")
            self.assertEqual(new_result["chunks"][0]["data"], GOOD_B2)
            paths = [(j["method"], j["path"]) for j in h.journal()]
            self.assertIn(("GET", "/media/P1/read/b"), paths)
            self.assertIn(("GET", "/media/P2/read/b"), paths)

    def test_restart_reuses_verified_segment_without_media_get(self):
        with FixtureHarness(SHARED) as h:
            svc = h.service
            result = svc.read(request_id="a-durable", version="map/v1", offset=0, length=8, wait_ms=5000)
            self.assertEqual(result["chunks"][0]["data"], GOOD_A)
            svc.close()

            before = len([j for j in h.journal() if j["path"] == "/media/P1/read/a"])
            restarted = ColdReaderService(
                HttpMediaClient(h.url, timeout=2),
                state_dir=h.state_dir.name,
                poll_interval=0.005,
            )
            try:
                recovery = restarted.status()["recovery"]
                self.assertIn("P1/a", recovery["verified_reusable"])
                snap = restarted.snapshot("a-durable")
                self.assertEqual(snap["chunks"][0]["data"], GOOD_A)
                time.sleep(0.05)
                after = len([j for j in h.journal() if j["path"] == "/media/P1/read/a"])
                self.assertEqual(before, after)
            finally:
                restarted.close()


class RecoveryStateTest(unittest.TestCase):
    def test_posting_state_reconciles_with_get_after_restart(self):
        with tempfile.TemporaryDirectory() as state_dir:
            data = b"RECONCILED"
            digest = __import__("hashlib").sha256(data).hexdigest()
            media = StaticMediaClient({"P9/x": data})
            policy = {
                "cold_window_ms": 1000,
                "cold_bytes": 64,
                "parallel": 1,
                "hot_bytes": 64,
                "indexes": {
                    "map/x": {
                        "pack": "P9",
                        "segments": [["x", 0, len(data), digest]],
                    }
                },
            }
            svc = ColdReaderService(media, policy=policy, state_dir=state_dir, start=False)
            svc.submit(request_id="rx", version="map/x", offset=0, length=len(data), wait_ms=10_000)
            svc.close()

            state_path = Path(state_dir) / "cold_reader_state.json"
            state = json.loads(state_path.read_text())
            seg = state["segments"]["P9/x"]
            seg["state"] = "posting"
            seg["file"] = None
            req = state["requests"]["rx"]
            req["chunk_count"] = 0
            req["items"][0]["chunk_seq"] = None
            req["items"][0]["terminal"] = None
            req["items"][0]["reason"] = None
            state_path.write_text(json.dumps(state))

            media2 = StaticMediaClient({"P9/x": data})
            restarted = ColdReaderService(media2, state_dir=state_dir, poll_interval=0.005)
            try:
                snap = restarted.read(request_id="rx", wait_ms=1000)
                self.assertEqual(snap["chunks"][0]["data"], data)
                self.assertEqual(media2.applied, [])
                self.assertTrue(any(p == ("P9", "x") for p in media2.queries))
                self.assertIn("P9/x", restarted.status()["recovery"]["reconciling"])
            finally:
                restarted.close()


class QuotaAccountingTest(unittest.TestCase):
    def test_cold_window_full_segment_charge_blocks_next_dispatch(self):
        data_x = b"X" * 8
        data_y = b"Y" * 8
        media = StaticMediaClient({"P/x": data_x, "P/y": data_y})
        policy = {
            "cold_window_ms": 60_000,
            "cold_bytes": 8,
            "parallel": 4,
            "hot_bytes": 32,
            "indexes": {
                "map/q": {
                    "pack": "P",
                    "segments": [
                        ["x", 0, 8, __import__("hashlib").sha256(data_x).hexdigest()],
                        ["y", 8, 8, __import__("hashlib").sha256(data_y).hexdigest()],
                    ],
                }
            },
        }
        with tempfile.TemporaryDirectory() as state_dir:
            svc = ColdReaderService(media, policy=policy, state_dir=state_dir, poll_interval=0.005)
            try:
                snap = svc.submit(request_id="q", version="map/q", offset=0, length=16, wait_ms=10_000)
                deadline = time.monotonic() + 1
                while time.monotonic() < deadline:
                    snap = svc.snapshot("q")
                    reasons = {i["segment"]: i for i in snap["items"]}
                    if reasons["y"]["category"] == "quota":
                        break
                    time.sleep(0.005)
                y = next(i for i in svc.snapshot("q")["items"] if i["segment"] == "y")
                self.assertEqual(y["category"], "quota")
                self.assertEqual(y["reason"], "cold_window_quota")
                status = svc.media_status()
                self.assertEqual(status["window_committed_bytes"], 8)
                self.assertEqual(status["cold_bytes_limit"], 8)
            finally:
                svc.close()


class ReviewRegressionTest(unittest.TestCase):
    def test_snapshot_after_lru_eviction_does_not_crash(self):
        data_x, data_y = b"X" * 8, b"Y" * 8
        media = StaticMediaClient({"P/x": data_x, "P/y": data_y})
        policy = {
            "cold_window_ms": 60_000,
            "cold_bytes": 64,
            "parallel": 2,
            "hot_bytes": 8,
            "indexes": {
                "map/e": {
                    "pack": "P",
                    "segments": [
                        ["x", 0, 8, __import__("hashlib").sha256(data_x).hexdigest()],
                        ["y", 8, 8, __import__("hashlib").sha256(data_y).hexdigest()],
                    ]
                }
            },
        }
        with tempfile.TemporaryDirectory() as state_dir:
            svc = ColdReaderService(media, policy=policy, state_dir=state_dir, poll_interval=0.005)
            try:
                first = svc.read(request_id="rx", version="map/e", offset=0, length=8, wait_ms=1000)
                self.assertEqual(first["chunks"][0]["data"], data_x)
                svc.close("rx")
                second = svc.read(request_id="ry", version="map/e", offset=8, length=8, wait_ms=1000)
                self.assertEqual(second["chunks"][0]["data"], data_y)
                old = svc.snapshot("rx")  # must not raise Path/None or OSError
                self.assertEqual(old["chunks"], [])
                hot = svc.hot_status()
                self.assertEqual(hot["partition_sum_bytes"], hot["used_bytes"])
            finally:
                svc.close()

    def test_cancel_releases_queued_hot_reservation(self):
        data_x, data_y = b"X" * 8, b"Y" * 8
        media = StaticMediaClient({"P/x": data_x, "P/y": data_y})
        policy = {
            "cold_window_ms": 60_000,
            "cold_bytes": 8,
            "parallel": 2,
            "hot_bytes": 16,
            "indexes": {
                "map/q2": {
                    "pack": "P",
                    "segments": [
                        ["x", 0, 8, __import__("hashlib").sha256(data_x).hexdigest()],
                        ["y", 8, 8, __import__("hashlib").sha256(data_y).hexdigest()],
                    ]
                }
            },
        }
        with tempfile.TemporaryDirectory() as state_dir:
            svc = ColdReaderService(media, policy=policy, state_dir=state_dir, poll_interval=0.005)
            try:
                svc.read(request_id="rx", version="map/q2", offset=0, length=8, wait_ms=1000)
                svc.submit(request_id="ry", version="map/q2", offset=8, length=8, wait_ms=10_000)
                seg_y = svc.state["segments"]["P/y"]
                self.assertEqual(seg_y["state"], "waiting_media")
                svc.cancel("ry")
                self.assertEqual(seg_y["state"], "waiting_capacity")
                self.assertEqual(int(seg_y["charged_bytes"]), 0)
                self.assertEqual(svc.hot_status()["used_bytes"], 8)
            finally:
                svc.close()


class HttpApiTest(unittest.TestCase):
    def test_http_submit_poll_and_ops(self):
        with FixtureHarness(HOT) as h:
            httpd = ColdReaderHttpServer(("127.0.0.1", 0), h.service)
            thread = threading.Thread(target=httpd.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{httpd.server_port}"
                req = urllib.request.Request(
                    base + "/v1/reads",
                    data=json.dumps(
                        {
                            "request_id": "http-one",
                            "version": "map/v3",
                            "offset": 0,
                            "length": 8,
                            "wait_ms": 300,
                            "priority": "field",
                        }
                    ).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=3) as response:
                    payload = json.loads(response.read())
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["snapshot"]["chunks"][0]["data_b64"], __import__("base64").b64encode(GOOD_R).decode())

                with urllib.request.urlopen(base + "/v1/ops/status", timeout=3) as response:
                    status = json.loads(response.read())
                self.assertTrue(status["ok"])
                self.assertIn("hot", status)

                bad = urllib.request.Request(
                    base + "/v1/reads",
                    data=b"{}",
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(bad, timeout=3)
                self.assertEqual(ctx.exception.code, 400)
            finally:
                httpd.shutdown()
                httpd.server_close()
                thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
