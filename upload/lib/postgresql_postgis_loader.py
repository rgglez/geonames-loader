"""
postgresql_postgis_loader.py — PostgreSQLPostGISLoader: PostGIS spatial indexes.

Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>
Licensed under GPL 3.0. See LICENSE for details.
"""

from sqlalchemy import text

from .postgresql_loader import PostgreSQLLoader


class PostgreSQLPostGISLoader(PostgreSQLLoader):
    """PostgreSQL loader with standard PostGIS geospatial indexes."""

    def _add_spatial_indexes(self) -> None:
        geo_stmts = [
            "CREATE INDEX IF NOT EXISTS geoname_postgis_idx ON geoname"
            " USING GIST (ST_MakePoint(longitude, latitude)::geography)",
            "CREATE INDEX IF NOT EXISTS postalcodes_postgis_idx ON postalcodes"
            " USING GIST (ST_MakePoint(longitude, latitude)::geography)",
        ]
        try:
            with self.engine.begin() as conn:
                for stmt in geo_stmts:
                    conn.execute(text(stmt))
            print("  [PostgreSQL: PostGIS GIST indexes created]")
        except Exception as exc:
            print(f"  [PostGIS GIST indexes skipped: {exc}]")
    # _add_spatial_indexes
