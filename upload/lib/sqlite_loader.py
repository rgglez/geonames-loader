"""
sqlite_loader.py — SQLiteLoader: performance PRAGMAs and VACUUM for SQLite.

Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>
Licensed under GPL 3.0. See LICENSE for details.
"""

from sqlalchemy import text

from .base import GeonamesLoader


class SQLiteLoader(GeonamesLoader):
    """
    Loader for SQLite.

    Sets WAL journal mode and cache PRAGMAs before bulk load for
    significantly better insert throughput, and runs PRAGMA optimize +
    VACUUM after indexing.
    """

    def create_tables(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("PRAGMA journal_mode = WAL"))
            conn.execute(text("PRAGMA synchronous = NORMAL"))
            conn.execute(text("PRAGMA cache_size = -65536"))  # 64 MB page cache
            conn.execute(text("PRAGMA temp_store = MEMORY"))
            conn.execute(text("PRAGMA foreign_keys = OFF"))
        super().create_tables()
    # create_tables

    def vacuum(self) -> None:
        print("  Running PRAGMA optimize + VACUUM ...", end=" ", flush=True)
        with self.engine.begin() as conn:
            conn.execute(text("PRAGMA optimize"))
        with self.engine.begin() as conn:
            conn.execute(text("VACUUM"))
        print("done")
    # vacuum
# SQLiteLoader
