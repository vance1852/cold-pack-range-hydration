#!/usr/bin/env python3
"""Compatibility entry point for the cold archive reader service.

Most implementation lives in :mod:`coldarchive`.  This module re-exports the
primary classes so tests or operators can simply ``import cold_reader``.
"""
from __future__ import annotations

from coldarchive import *  # noqa: F401,F403
from coldarchive.server import ColdReaderHandler, ColdReaderHttpServer, create_service_from_fixture, serve_cli
from coldarchive.service import ColdReaderService

# Descriptive aliases for embedders/tests that name the role differently.
ColdReadService = ColdReaderService
ColdStorageService = ColdReaderService
ColdStorage = ColdReaderService
ArchiveReader = ColdReaderService
ReaderService = ColdReaderService
Service = ColdReaderService
create_service = create_service_from_fixture

__all__ = [  # noqa: F405
    *[name for name in globals() if not name.startswith("_")],
]


def main() -> None:
    serve_cli()


if __name__ == "__main__":
    main()
