"""
postgresql_ganos_loader.py — PostgreSQLGanosLoader: Aliyun Ganos spatial indexes.

Uses ST_SetSRID(ST_MakePoint(...), 4326) because Ganos registers the geography
type in pg_type but does not support the geometry::geography cast.

Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>
Licensed under GPL 3.0. See LICENSE for details.
"""

from sqlalchemy import text

from .postgresql_loader import PostgreSQLLoader


class PostgreSQLGanosLoader(PostgreSQLLoader):
    """
    PostgreSQL loader with Aliyun Ganos (ganos_spatialref) geospatial indexes.
    """

    def _add_spatial_indexes(self) -> None:
        geo_stmts = [
            "CREATE INDEX IF NOT EXISTS geoname_postgis_idx ON geoname"
            " USING GIST (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326))",
            "CREATE INDEX IF NOT EXISTS postalcodes_postgis_idx ON postalcodes"
            " USING GIST (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326))",
        ]
        try:
            with self.engine.begin() as conn:
                for stmt in geo_stmts:
                    conn.execute(text(stmt))
            print("  [PostgreSQL: Ganos/ganos_spatialref GIST indexes created]")
        except Exception as exc:
            print(f"  [Ganos GIST indexes skipped: {exc}]")
    # _add_spatial_indexes
