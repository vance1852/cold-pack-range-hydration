"""Cold-archive range reading service."""
from .indexing import IndexRegistry, IndexVersion, RangeItem, SegmentSpec, VersionNotFound
from .media import HttpMediaClient, MediaError, MediaResponseError, ReceiptLost, StaticMediaClient
from .service import ColdReaderService, RequestExists, RequestNotFound

__all__ = [
    "ColdReaderService",
    "HttpMediaClient",
    "StaticMediaClient",
    "IndexRegistry",
    "IndexVersion",
    "RangeItem",
    "SegmentSpec",
    "VersionNotFound",
    "MediaError",
    "MediaResponseError",
    "ReceiptLost",
    "RequestExists",
    "RequestNotFound",
]
