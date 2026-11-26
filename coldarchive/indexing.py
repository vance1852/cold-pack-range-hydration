"""Index parsing and immutable range resolution for cold archive reads."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any, Mapping


class IndexProtocolError(ValueError):
    """The archive index or requested range does not follow the protocol."""


class VersionNotFound(KeyError):
    """Requested index version is not present."""


@dataclass(frozen=True)
class SegmentSpec:
    name: str
    offset: int
    length: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "offset": self.offset,
            "length": self.length,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class IndexVersion:
    version: str
    pack: str
    segments: tuple[SegmentSpec, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "pack": self.pack,
            "segments": [s.to_dict() for s in self.segments],
        }


@dataclass(frozen=True)
class RangeItem:
    """One segment intersection, or an unmapped hole."""

    key: str
    pack: str | None
    segment: SegmentSpec | None
    logical_offset: int
    logical_length: int
    slice_offset: int
    slice_length: int
    sha256: str | None
    mapped: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "pack": self.pack,
            "segment": self.segment.name if self.segment else None,
            "logical_offset": self.logical_offset,
            "logical_length": self.logical_length,
            "slice_offset": self.slice_offset,
            "slice_length": self.slice_length,
            "sha256": self.sha256,
            "mapped": self.mapped,
        }


class IndexRegistry:
    """All known index versions.

    A read stores the version and resolved :class:`RangeItem` objects it was
    created with, so later :meth:`set_current` cannot splice new-index bytes
    into an old request.
    """

    def __init__(self, versions: Mapping[str, IndexVersion], current: str | None = None):
        if not versions:
            raise IndexProtocolError("at least one archive index is required")
        self._versions = dict(versions)
        if current is None:
            current = next(iter(self._versions))
        if current not in self._versions:
            raise VersionNotFound(current)
        self.current = current

    @classmethod
    def from_dict(cls, indexes: Mapping[str, Any], current: str | None = None) -> "IndexRegistry":
        versions: dict[str, IndexVersion] = {}
        for version, raw in indexes.items():
            try:
                pack = str(raw["pack"])
                segments = []
                for row in raw.get("segments", []):
                    if isinstance(row, Mapping):
                        name = row.get("name") or row.get("segment") or row.get("id")
                        offset = row["offset"]
                        length = row["length"]
                        digest = row.get("sha256") or row.get("digest")
                    else:
                        name, offset, length, digest = row
                    offset = int(offset)
                    length = int(length)
                    if offset < 0 or length <= 0:
                        raise IndexProtocolError(f"invalid segment range in {version}: {row!r}")
                    digest = str(digest).lower()
                    if len(digest) != 64:
                        raise IndexProtocolError(f"invalid sha256 in {version}: {row!r}")
                    segments.append(SegmentSpec(str(name), offset, length, digest))
                segments.sort(key=lambda s: s.offset)
                versions[version] = IndexVersion(version, pack, tuple(segments))
            except (KeyError, TypeError, ValueError) as exc:
                if isinstance(exc, IndexProtocolError):
                    raise
                raise IndexProtocolError(f"bad index {version}: {exc}") from exc
        return cls(versions, current)

    @classmethod
    def from_fixture(cls, path: str | Path, current: str | None = None) -> tuple["IndexRegistry", dict[str, Any]]:
        path = Path(path)
        events_path = path / "events.json" if path.is_dir() else path
        record = json.loads(events_path.read_text(encoding="utf-8"))
        policy = record["policy"]
        return cls.from_dict(policy["indexes"], current), policy

    def set_current(self, version: str) -> None:
        if version not in self._versions:
            raise VersionNotFound(version)
        self.current = version

    def get(self, version: str | None = None) -> IndexVersion:
        version = version or self.current
        try:
            return self._versions[version]
        except KeyError as exc:
            raise VersionNotFound(version) from exc

    def names(self) -> list[str]:
        return list(self._versions)

    def resolve(self, offset: int, length: int, version: str | None = None) -> tuple[IndexVersion, list[RangeItem]]:
        if not isinstance(offset, int) or offset < 0:
            raise IndexProtocolError("offset must be a non-negative integer")
        if not isinstance(length, int) or length < 0:
            raise IndexProtocolError("length must be a non-negative integer")
        ver = self.get(version)
        if length == 0:
            return ver, []
        end = offset + length
        items: list[RangeItem] = []
        cursor = offset

        for seg in ver.segments:
            seg_end = seg.offset + seg.length
            if seg_end <= offset:
                continue
            if seg.offset >= end:
                break
            overlap_start = max(offset, seg.offset)
            overlap_end = min(end, seg_end)
            if overlap_start > cursor:
                gap_end = min(seg.offset, end)
                if gap_end > cursor:
                    items.append(
                        RangeItem(
                            key=f"unmapped:{ver.version}:{cursor}:{gap_end}",
                            pack=None,
                            segment=None,
                            logical_offset=cursor,
                            logical_length=gap_end - cursor,
                            slice_offset=0,
                            slice_length=0,
                            sha256=None,
                            mapped=False,
                        )
                    )
                    cursor = gap_end
            if overlap_end > overlap_start:
                items.append(
                    RangeItem(
                        key=f"{ver.pack}/{seg.name}",
                        pack=ver.pack,
                        segment=seg,
                        logical_offset=overlap_start,
                        logical_length=overlap_end - overlap_start,
                        slice_offset=overlap_start - seg.offset,
                        slice_length=overlap_end - overlap_start,
                        sha256=seg.sha256,
                    )
                )
                cursor = overlap_end

        if cursor < end:
            items.append(
                RangeItem(
                    key=f"unmapped:{ver.version}:{cursor}:{end}",
                    pack=None,
                    segment=None,
                    logical_offset=cursor,
                    logical_length=end - cursor,
                    slice_offset=0,
                    slice_length=0,
                    sha256=None,
                    mapped=False,
                )
            )
        return ver, items
