"""
postgresql_loader.py — PostgreSQLLoader: COPY bulk load, CASCADE DROP,
unaccent(), earthdistance GIST indexes, and VACUUM ANALYZE.

Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>
Licensed under GPL 3.0. See LICENSE for details.
"""

from pathlib import Path

import tqdm
from sqlalchemy import Table, text

from .base import GeonamesLoader, _DROP_ORDER, _has_extension, _iter_tsv_rows
from . import models


class PostgreSQLLoader(GeonamesLoader):
    """
    Loader for standard PostgreSQL.

    Uses COPY for bulk load, CASCADE on DROP, unaccent() for accent
    stripping, earthdistance GIST indexes, and runs VACUUM ANALYZE
    after indexing.
    """

    def drop_tables(self) -> None:
        with self.engine.begin() as conn:
            for tbl in _DROP_ORDER:
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl.name} CASCADE"))
    # drop_tables

    def create_extension(self) -> None:
        # unaccent is used for accent-stripped name variants; not available
        # on all managed PostgreSQL services — fall back gracefully.
        with self.engine.begin() as conn:
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS unaccent"))
            except Exception:
                pass
    # create_extension

    def _bulk_load(self, table: Table, columns: list[str], filepath: Path) -> None:
        """Fast COPY-based bulk load using PostgreSQL's native COPY protocol."""
        cols = ", ".join(columns)
        sql = (
            f"COPY {table.name} ({cols}) FROM STDIN "
            f"WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE E'\\x01')"
        )
        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cur, \
                 open(filepath, "r", encoding="utf-8", errors="replace") as f, \
                 tqdm.tqdm.wrapattr(
                     f, "read",
                     total=filepath.stat().st_size,
                     desc=f"  {table.name}",
                     unit="B", unit_scale=True, unit_divisor=1024,
                 ) as fwrapped:
                cur.copy_expert(sql, fwrapped)
            raw_conn.commit()
        finally:
            raw_conn.close()
    # _bulk_load

    def _enrich_nameascii(self) -> None:
        with self.engine.connect() as conn:
            use_unaccent = _has_extension(conn, "unaccent")
        if use_unaccent:
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE postalcodes SET"
                    " admin1nameascii = unaccent(admin1name),"
                    " admin2nameascii = unaccent(admin2name),"
                    " admin3nameascii = unaccent(admin3name)"
                ))
        else:
            super()._enrich_nameascii()
    # _enrich_nameascii

    def _add_pk_fk_constraints(self) -> None:
        pk_stmts = [
            "ALTER TABLE ONLY alternatename"
            " ADD CONSTRAINT alternatenameid_pkey PRIMARY KEY (alternatenameid)",
            "ALTER TABLE ONLY geoname"
            " ADD CONSTRAINT geonameid_pkey PRIMARY KEY (geonameid)",
            "ALTER TABLE ONLY countryinfo"
            " ADD CONSTRAINT iso_alpha2_pkey PRIMARY KEY (iso_alpha2)",
        ]
        fk_stmts = [
            "ALTER TABLE ONLY countryinfo"
            " ADD CONSTRAINT countryinfo_geonameid_fkey"
            " FOREIGN KEY (geonameid) REFERENCES geoname(geonameid)",
            "ALTER TABLE ONLY alternatename"
            " ADD CONSTRAINT alternatename_geonameid_fkey"
            " FOREIGN KEY (geonameid) REFERENCES geoname(geonameid)",
        ]
        with self.engine.begin() as conn:
            for stmt in pk_stmts + fk_stmts:
                conn.execute(text(stmt))
    # _add_pk_fk_constraints

    def _add_spatial_indexes(self) -> None:
        """
        Create geospatial GIST indexes specific to a spatial plugin.
        No-op on plain PostgreSQL; overridden by Ganos and PostGIS subclasses.
        """
    # _add_spatial_indexes

    def _add_geo_indexes(self) -> None:
        # earthdistance GIST indexes — fallback strategy when neither Ganos
        # nor PostGIS is installed.  Wrapped in try/except because cube and
        # earthdistance are not available on all managed PostgreSQL services
        # (e.g. Aliyun Apsara RDS when the Ganos suite is used instead).
        try:
            with self.engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS cube"))
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS earthdistance"))
                conn.execute(text(
                    "CREATE INDEX geoname_geo_idx ON geoname"
                    " USING GIST (ll_to_earth(latitude, longitude))"
                ))
                conn.execute(text(
                    "CREATE INDEX postalcodes_geo_idx ON postalcodes"
                    " USING GIST (ll_to_earth(latitude, longitude))"
                ))
            print("  [PostgreSQL: GIST geospatial indexes created"
                  " via cube + earthdistance]")
        except Exception as exc:
            print(f"  [earthdistance GIST indexes skipped: {exc}]")

        self._add_spatial_indexes()
    # _add_geo_indexes

    def vacuum(self) -> None:
        print("  Running VACUUM ANALYZE ...", end=" ", flush=True)
        # VACUUM cannot run inside a transaction block
        with self.engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text("VACUUM ANALYZE"))
        print("done")
    # vacuum
