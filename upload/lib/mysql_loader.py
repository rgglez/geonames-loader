"""
mysql_loader.py — MySQLLoader: adds PK and FK constraints for MySQL/MariaDB.

Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>
Licensed under GPL 3.0. See LICENSE for details.
"""

from sqlalchemy import text

from .base import GeonamesLoader


class MySQLLoader(GeonamesLoader):
    """Loader for MySQL / MariaDB — adds PK and FK constraints."""

    def _add_pk_fk_constraints(self) -> None:
        pk_stmts = [
            "ALTER TABLE alternatename"
            " ADD CONSTRAINT alternatenameid_pkey PRIMARY KEY (alternatenameid)",
            "ALTER TABLE geoname"
            " ADD CONSTRAINT geonameid_pkey PRIMARY KEY (geonameid)",
            "ALTER TABLE countryinfo"
            " ADD CONSTRAINT iso_alpha2_pkey PRIMARY KEY (iso_alpha2)",
        ]
        fk_stmts = [
            "ALTER TABLE countryinfo"
            " ADD CONSTRAINT countryinfo_geonameid_fkey"
            " FOREIGN KEY (geonameid) REFERENCES geoname(geonameid)",
            "ALTER TABLE alternatename"
            " ADD CONSTRAINT alternatename_geonameid_fkey"
            " FOREIGN KEY (geonameid) REFERENCES geoname(geonameid)",
        ]
        with self.engine.begin() as conn:
            for stmt in pk_stmts + fk_stmts:
                conn.execute(text(stmt))
    # _add_pk_fk_constraints
