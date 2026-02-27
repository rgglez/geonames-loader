#!/usr/bin/env python3

"""
    load_geonames.py
    Creates the Geonames schema in an existing database and loads
    data from the locally-downloaded files.

    Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

    ---------------------------------------------------------------------------

    Configuration is read from config/config.yaml (or --config argument).
    The database must already exist; this script only creates tables and
    populates them.

    Usage:
        python load_geonames.py [--config CONFIG_FILE] [--skip-indexes] [-o]

    The config 'database' section accepts either a SQLAlchemy URL:

        database:
        url: "postgresql+psycopg2://user:pass@host:5432/mydb"

    or legacy PostgreSQL components (a postgresql+psycopg2 URL is built automatically):

        database:
        host: localhost
        port: 5432
        user: myuser
        password: mypassword
        dbname: mydb
"""

import argparse
import csv
import sys
import unicodedata
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import yaml
from sqlalchemy import (
    BigInteger, Boolean, CHAR, Column, Date, DateTime, Float, Index,
    Integer, MetaData, Numeric, SmallInteger, String, Table, Text,
    create_engine, func, text, update,
)
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Schema — table definitions via SQLAlchemy Core
# ---------------------------------------------------------------------------

metadata = MetaData()

t_geoname = Table(
    "geoname", metadata,
    Column("geonameid",      Integer,      nullable=True),
    Column("name",           String(200),  nullable=True),
    Column("asciiname",      String(200),  nullable=True),
    Column("alternatenames", Text,         nullable=True),
    Column("latitude",       Float,        nullable=True),
    Column("longitude",      Float,        nullable=True),
    Column("fclass",         CHAR(1),      nullable=True),
    Column("fcode",          String(10),   nullable=True),
    Column("country",        String(3),    nullable=True),
    Column("cc2",            Text,         nullable=True),
    Column("admin1",         String(20),   nullable=True),
    Column("admin2",         String(80),   nullable=True),
    Column("admin3",         String(20),   nullable=True),
    Column("admin4",         String(20),   nullable=True),
    Column("population",     BigInteger,   nullable=True),
    Column("elevation",      Integer,      nullable=True),
    Column("gtopo30",        Integer,      nullable=True),
    Column("timezone",       String(40),   nullable=True),
    Column("moddate",        Date,         nullable=True),
)

t_alternatename = Table(
    "alternatename", metadata,
    Column("alternatenameid", Integer,      nullable=True),
    Column("geonameid",       Integer,      nullable=True),
    Column("isolanguage",     String(7),    nullable=True),
    Column("alternatename",   String(500),  nullable=True),
    Column("ispreferredname", Boolean,      nullable=True),
    Column("isshortname",     Boolean,      nullable=True),
    Column("iscolloquial",    Boolean,      nullable=True),
    Column("ishistoric",      Boolean,      nullable=True),
)

t_countryinfo = Table(
    "countryinfo", metadata,
    Column("iso_alpha2",           CHAR(2),     nullable=True),
    Column("iso_alpha3",           CHAR(3),     nullable=True),
    Column("iso_numeric",          Integer,     nullable=True),
    Column("fips_code",            String(3),   nullable=True),
    Column("country",              String(200), nullable=True),
    Column("capital",              String(200), nullable=True),
    Column("areainsqkm",           Float,       nullable=True),
    Column("population",           Integer,     nullable=True),
    Column("continent",            CHAR(3),     nullable=True),
    Column("tld",                  CHAR(10),    nullable=True),
    Column("currency_code",        CHAR(3),     nullable=True),
    Column("currency_name",        CHAR(25),    nullable=True),
    Column("phone",                String(20),  nullable=True),
    Column("postal",               String(60),  nullable=True),
    Column("postalregex",          String(200), nullable=True),
    Column("languages",            String(200), nullable=True),
    Column("geonameid",            Integer,     nullable=True),
    Column("neighbours",           String(50),  nullable=True),
    Column("equivalent_fips_code", String(3),   nullable=True),
)

t_iso_languagecodes = Table(
    "iso_languagecodes", metadata,
    Column("iso_639_3",     CHAR(4),     nullable=True),
    Column("iso_639_2",     String(50),  nullable=True),
    Column("iso_639_1",     String(50),  nullable=True),
    Column("language_name", String(200), nullable=True),
)

t_admin1codesascii = Table(
    "admin1codesascii", metadata,
    Column("code",        CHAR(20),   nullable=True),
    Column("name",        Text,       nullable=True),
    Column("nameascii",   Text,       nullable=True),
    Column("geonameid",   Integer,    nullable=True),
    # Derived: first 2 chars of code (populated during enrichment)
    Column("countrycode", String(25), nullable=True),
)

t_admin2codesascii = Table(
    "admin2codesascii", metadata,
    Column("code",        CHAR(80),   nullable=True),
    Column("name",        Text,       nullable=True),
    Column("nameascii",   Text,       nullable=True),
    Column("geonameid",   Integer,    nullable=True),
    # Derived: first 2 chars of code (populated during enrichment)
    Column("countrycode", String(25), nullable=True),
)

t_featurecodes = Table(
    "featurecodes", metadata,
    Column("code",        CHAR(7),     nullable=True),
    Column("name",        String(200), nullable=True),
    Column("description", Text,        nullable=True),
)

t_timezones = Table(
    "timezones", metadata,
    Column("countrycode", CHAR(20),       nullable=True),
    Column("timezoneid",  String(200),    nullable=True),
    Column("gmt_offset",  Numeric(3, 1),  nullable=True),
    Column("dst_offset",  Numeric(3, 1),  nullable=True),
    Column("raw_offset",  Numeric(3, 1),  nullable=True),
)

t_continentcodes = Table(
    "continentcodes", metadata,
    Column("code",      CHAR(2),    nullable=True),
    Column("name",      String(20), nullable=True),
    Column("geonameid", Integer,    nullable=True),
)

t_postalcodes = Table(
    "postalcodes", metadata,
    Column("countrycode",     CHAR(2),      nullable=True),
    Column("postalcode",      String(20),   nullable=True),
    Column("placename",       String(180),  nullable=True),
    Column("admin1name",      String(100),  nullable=True),
    Column("admin1code",      String(20),   nullable=True),
    Column("admin2name",      String(100),  nullable=True),
    Column("admin2code",      String(20),   nullable=True),
    Column("admin3name",      String(100),  nullable=True),
    Column("admin3code",      String(20),   nullable=True),
    Column("latitude",        Float,        nullable=True),
    Column("longitude",       Float,        nullable=True),
    Column("accuracy",        SmallInteger, nullable=True),
    # Derived columns (populated during enrichment)
    Column("admin1code_full", String(100),  nullable=True),
    Column("admin2code_full", String(100),  nullable=True),
    Column("admin3code_full", String(100),  nullable=True),
    Column("admin1nameascii", String(100),  nullable=True),
    Column("admin2nameascii", String(100),  nullable=True),
    Column("admin3nameascii", String(100),  nullable=True),
)

t_meta = Table(
    "meta", metadata,
    Column("version",       Text,     nullable=True),
    Column("data_uri",      Text,     nullable=True),
    Column("data_version",  Text,     nullable=True),
    Column("date_accessed", DateTime, nullable=True),
)

# Drop order that respects FK dependencies (dependents first)
_DROP_ORDER = [
    t_alternatename, t_countryinfo, t_geoname,
    t_postalcodes, t_admin1codesascii, t_admin2codesascii,
    t_iso_languagecodes, t_featurecodes, t_timezones,
    t_continentcodes, t_meta,
]


# ---------------------------------------------------------------------------
# Engine / config helpers
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
# load_config


# -----------------------------------------------------------------------------


def build_engine(cfg: dict) -> Engine:
    """Build a SQLAlchemy engine from the 'database' section of the config."""
    db = cfg["database"]
    if "url" in db:
        return create_engine(db["url"])
    # Legacy format: individual PostgreSQL components
    return create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['dbname']}"
    )
# build_engine


# -----------------------------------------------------------------------------


def is_postgresql(engine: Engine) -> bool:
    return engine.dialect.name == "postgresql"
# is_postgresql


def _has_extension(conn, name: str) -> bool:
    """Return True if the named PostgreSQL extension is installed."""
    count = conn.execute(
        text("SELECT count(*) FROM pg_extension WHERE extname = :n"),
        {"n": name},
    ).scalar()
    return bool(count)
# _has_extension


# ---------------------------------------------------------------------------
# Table management
# ---------------------------------------------------------------------------

def drop_and_create_tables(engine: Engine) -> None:
    """Drop all tables (handling FK dependencies) and recreate them."""
    with engine.begin() as conn:
        if is_postgresql(engine):
            # CASCADE handles FK-dependent tables automatically
            for tbl in _DROP_ORDER:
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl.name} CASCADE"))
            # unaccent is used for accent-stripped name variants; not available
            # on all managed PostgreSQL services — fall back gracefully.
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS unaccent"))
            except Exception:
                pass  # enrich_admin_codes() will fall back to Python stripping
        else:
            for tbl in _DROP_ORDER:
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl.name}"))
    metadata.create_all(engine)
# drop_and_create_tables


# ---------------------------------------------------------------------------
# Bulk-load helpers
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 10_000


def _iter_tsv_rows(filepath: Path, columns: list[str]) -> Iterator[dict]:
    """Stream a tab-delimited file as dicts, skipping comment/blank lines."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter="\t", quotechar="\x01")
        for line in reader:
            if not line:
                continue
            if len(line) == 1 and line[0].startswith("#"):
                continue
            if len(line) < len(columns):
                line += [""] * (len(columns) - len(line))
            yield {
                col: (v if v != "" else None)
                for col, v in zip(columns, line)
            }
# _iter_tsv_rows


# -----------------------------------------------------------------------------


def _copy_pg(engine: Engine, table: Table, columns: list[str],
             filepath: Path) -> None:
    """Fast COPY-based bulk load using PostgreSQL's native COPY protocol."""
    cols = ", ".join(columns)
    sql = (
        f"COPY {table.name} ({cols}) FROM STDIN "
        f"WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE E'\\x01')"
    )
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur, \
             open(filepath, "r", encoding="utf-8", errors="replace") as f:
            cur.copy_expert(sql, f)
        raw_conn.commit()
    finally:
        raw_conn.close()
# _copy_pg


# -----------------------------------------------------------------------------


def _insert_chunks(engine: Engine, table: Table, columns: list[str],
                   filepath: Path) -> int:
    """Chunked INSERT bulk load for non-PostgreSQL engines (streams the file)."""
    count = 0
    chunk: list[dict] = []
    with engine.begin() as conn:
        for row in _iter_tsv_rows(filepath, columns):
            chunk.append(row)
            if len(chunk) >= _CHUNK_SIZE:
                conn.execute(table.insert(), chunk)
                count += len(chunk)
                chunk = []
        if chunk:
            conn.execute(table.insert(), chunk)
            count += len(chunk)
    return count
# _insert_chunks


# -----------------------------------------------------------------------------


def load_file(engine: Engine, table: Table, columns: list[str],
              filepath: Path) -> None:
    """Load a TSV data file into a table using the best method for the dialect."""
    print(f"  Loading {table.name} from {filepath.name} ...", end=" ", flush=True)
    if is_postgresql(engine):
        _copy_pg(engine, table, columns, filepath)
        print("done")
    else:
        count = _insert_chunks(engine, table, columns, filepath)
        print(f"done ({count:,} rows)")
# load_file


# ---------------------------------------------------------------------------
# Admin-codes enrichment
# ---------------------------------------------------------------------------

def _strip_accents(s: str | None) -> str | None:
    """Remove combining accent marks from a Unicode string."""
    if s is None:
        return None
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
# _strip_accents


# -----------------------------------------------------------------------------


def _enrich_nameascii_python(engine: Engine) -> None:
    """
    Populate admin*nameascii columns via Python-side accent stripping.
    Used on non-PostgreSQL engines where unaccent() is unavailable.
    Updates in batches grouped by distinct name values.
    """
    pc = t_postalcodes.c
    for src_col, dst_col in [
        (pc.admin1name, pc.admin1nameascii),
        (pc.admin2name, pc.admin2nameascii),
        (pc.admin3name, pc.admin3nameascii),
    ]:
        with engine.connect() as conn:
            names = {
                row[0]
                for row in conn.execute(
                    t_postalcodes.select().with_only_columns(src_col).distinct()
                )
                if row[0] is not None
            }
        if not names:
            continue
        with engine.begin() as conn:
            for name in names:
                conn.execute(
                    update(t_postalcodes)
                    .where(src_col == name)
                    .values({dst_col.key: _strip_accents(name)})
                )
# _enrich_nameascii_python


# -----------------------------------------------------------------------------


def enrich_admin_codes(engine: Engine) -> None:
    """Populate all derived columns after the initial bulk load."""
    print("  Enriching admin-codes tables ...", end=" ", flush=True)

    with engine.begin() as conn:
        # Fix nulls: admin1codesascii.name ← nameascii where name is NULL
        conn.execute(
            update(t_admin1codesascii)
            .where(t_admin1codesascii.c.name.is_(None))
            .where(t_admin1codesascii.c.nameascii.is_not(None))
            .values(name=t_admin1codesascii.c.nameascii)
        )

        # countrycode = first 2 chars of code
        # func.substr() renders portably across PostgreSQL, MySQL, SQLite
        conn.execute(
            update(t_admin1codesascii)
            .values(countrycode=func.substr(t_admin1codesascii.c.code, 1, 2))
        )
        conn.execute(
            update(t_admin2codesascii)
            .values(countrycode=func.substr(t_admin2codesascii.c.code, 1, 2))
        )

        # Composite admin code fields using SQLAlchemy string concatenation.
        # The + operator on String columns renders as || (PostgreSQL/SQLite)
        # or CONCAT() (MySQL) automatically via SQLAlchemy's type system.
        pc = t_postalcodes.c
        conn.execute(
            update(t_postalcodes)
            .where(pc.admin1code.is_not(None))
            .where(pc.admin1code != "")
            .values(admin1code_full=pc.countrycode + "." + pc.admin1code)
        )
        conn.execute(
            update(t_postalcodes)
            .where(pc.admin1code.is_not(None)).where(pc.admin1code != "")
            .where(pc.admin2code.is_not(None)).where(pc.admin2code != "")
            .values(admin2code_full=(
                pc.countrycode + "." + pc.admin1code + "." + pc.admin2code
            ))
        )
        conn.execute(
            update(t_postalcodes)
            .where(pc.admin1code.is_not(None)).where(pc.admin1code != "")
            .where(pc.admin2code.is_not(None)).where(pc.admin2code != "")
            .where(pc.admin3code.is_not(None)).where(pc.admin3code != "")
            .values(admin3code_full=(
                pc.countrycode + "." + pc.admin1code
                + "." + pc.admin2code + "." + pc.admin3code
            ))
        )

    # Accent-stripped name variants
    if is_postgresql(engine):
        with engine.connect() as conn:
            use_unaccent = _has_extension(conn, "unaccent")
        if use_unaccent:
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE postalcodes SET"
                    " admin1nameascii = unaccent(admin1name),"
                    " admin2nameascii = unaccent(admin2name),"
                    " admin3nameascii = unaccent(admin3name)"
                ))
        else:
            _enrich_nameascii_python(engine)
    else:
        _enrich_nameascii_python(engine)

    print("done")
# enrich_admin_codes


# ---------------------------------------------------------------------------
# Indexes and constraints (applied after bulk load for speed)
# ---------------------------------------------------------------------------

def create_indexes(engine: Engine) -> None:
    """Add primary keys, foreign keys, and indexes after bulk load."""
    dialect = engine.dialect.name

    # --- Primary keys ---
    # ALTER TABLE ADD CONSTRAINT PRIMARY KEY works on PostgreSQL and MySQL/MariaDB.
    # SQLite does not support this syntax; PKs must be declared at CREATE TABLE.
    if dialect in ("postgresql", "mysql", "mariadb"):
        only = "ONLY " if dialect == "postgresql" else ""
        pk_stmts = [
            f"ALTER TABLE {only}alternatename"
            " ADD CONSTRAINT alternatenameid_pkey PRIMARY KEY (alternatenameid)",
            f"ALTER TABLE {only}geoname"
            " ADD CONSTRAINT geonameid_pkey PRIMARY KEY (geonameid)",
            f"ALTER TABLE {only}countryinfo"
            " ADD CONSTRAINT iso_alpha2_pkey PRIMARY KEY (iso_alpha2)",
        ]
        with engine.begin() as conn:
            for stmt in pk_stmts:
                conn.execute(text(stmt))
    else:
        print("  [Primary key constraints skipped: not supported by this dialect]")

    # --- Indexes via SQLAlchemy Index objects (portable DDL) ---
    indexes = [
        # countryinfo
        Index("countryinfo_geonameid_idx",             t_countryinfo.c.geonameid),
        # alternatename
        Index("alternatename_geonameid_idx",            t_alternatename.c.geonameid),
        Index("alternatename_isolanguage_idx",          t_alternatename.c.isolanguage),
        Index("alternatename_alternatename_idx",        t_alternatename.c.alternatename),
        Index("alternatename_ispreferredname_idx",      t_alternatename.c.ispreferredname),
        Index("alternatename_isshortname_idx",          t_alternatename.c.isshortname),
        Index("alternatename_iscolloquial_idx",         t_alternatename.c.iscolloquial),
        Index("alternatename_ishistoric_idx",           t_alternatename.c.ishistoric),
        # geoname
        Index("geoname_name_idx",                      t_geoname.c.name),
        Index("geoname_asciiname_idx",                 t_geoname.c.asciiname),
        Index("geoname_fclass_idx",                    t_geoname.c.fclass),
        Index("geoname_fcode_idx",                     t_geoname.c.fcode),
        Index("geoname_country_idx",                   t_geoname.c.country),
        Index("geoname_cc2_idx",                       t_geoname.c.cc2),
        Index("geoname_admin1_idx",                    t_geoname.c.admin1),
        Index("geoname_admin2_idx",                    t_geoname.c.admin2),
        Index("geoname_admin3_idx",                    t_geoname.c.admin3),
        Index("geoname_admin4_idx",                    t_geoname.c.admin4),
        # postalcodes — base columns
        Index("postalcodes_countrycode_idx",           t_postalcodes.c.countrycode),
        Index("postalcodes_admin1name_idx",            t_postalcodes.c.admin1name),
        Index("postalcodes_admin1code_idx",            t_postalcodes.c.admin1code),
        Index("postalcodes_admin2name_idx",            t_postalcodes.c.admin2name),
        Index("postalcodes_admin2code_idx",            t_postalcodes.c.admin2code),
        Index("postalcodes_admin3name_idx",            t_postalcodes.c.admin3name),
        Index("postalcodes_admin3code_idx",            t_postalcodes.c.admin3code),
        # postalcodes — enrichment columns
        Index("postalcodes_admin1code_full_idx",       t_postalcodes.c.admin1code_full),
        Index("postalcodes_admin2code_full_idx",       t_postalcodes.c.admin2code_full),
        Index("postalcodes_admin3code_full_idx",       t_postalcodes.c.admin3code_full),
        Index("postalcodes_admin1nameascii_idx",       t_postalcodes.c.admin1nameascii),
        Index("postalcodes_admin2nameascii_idx",       t_postalcodes.c.admin2nameascii),
        Index("postalcodes_admin3nameascii_idx",       t_postalcodes.c.admin3nameascii),
        # admin1codesascii
        Index("admin1codesascii_countrycode_idx",      t_admin1codesascii.c.countrycode),
        Index("admin1codesascii_name_idx",             t_admin1codesascii.c.name),
        Index("admin1codesascii_nameascii_idx",        t_admin1codesascii.c.nameascii),
        Index("admin1codesascii_code_idx",             t_admin1codesascii.c.code),
        # admin2codesascii
        Index("admin2codesascii_countrycode_idx",      t_admin2codesascii.c.countrycode),
        Index("admin2codesascii_name_idx",             t_admin2codesascii.c.name),
        Index("admin2codesascii_nameascii_idx",        t_admin2codesascii.c.nameascii),
        Index("admin2codesascii_code_idx",             t_admin2codesascii.c.code),
        # geoname + postalcodes — coordinate columns (B-tree)
        # Enable bounding-box pre-filtering on all dialects
        Index("geoname_latitude_idx",      t_geoname.c.latitude),
        Index("geoname_longitude_idx",     t_geoname.c.longitude),
        Index("postalcodes_latitude_idx",  t_postalcodes.c.latitude),
        Index("postalcodes_longitude_idx", t_postalcodes.c.longitude),
        # postalcodes — composite index for nearest-postal-code correlated subquery:
        # equality on countrycode + range on latitude allows the DB to scan only
        # postal codes in the right country within a lat/lon bounding box instead
        # of performing a full country scan for every geoname result row.
        Index("postalcodes_cc_lat_lon_idx",
              t_postalcodes.c.countrycode,
              t_postalcodes.c.latitude,
              t_postalcodes.c.longitude),
    ]
    for idx in indexes:
        idx.create(engine)

    # --- Foreign keys (PostgreSQL and MySQL/MariaDB) ---
    if dialect in ("postgresql", "mysql", "mariadb"):
        only = "ONLY " if dialect == "postgresql" else ""
        fk_stmts = [
            f"ALTER TABLE {only}countryinfo"
            " ADD CONSTRAINT countryinfo_geonameid_fkey"
            " FOREIGN KEY (geonameid) REFERENCES geoname(geonameid)",
            f"ALTER TABLE {only}alternatename"
            " ADD CONSTRAINT alternatename_geonameid_fkey"
            " FOREIGN KEY (geonameid) REFERENCES geoname(geonameid)",
        ]
        with engine.begin() as conn:
            for stmt in fk_stmts:
                conn.execute(text(stmt))
    else:
        print("  [Foreign key constraints skipped: not supported by this dialect]")

    # --- Geospatial GIST indexes (PostgreSQL only) ---
    # On other dialects the B-tree indexes above are the best available.
    if dialect == "postgresql":
        # earthdistance GIST indexes — fallback strategy when neither Ganos
        # nor PostGIS is installed.  Wrapped in try/except because cube and
        # earthdistance are not available on all managed PostgreSQL services
        # (e.g. Aliyun Apsara RDS when the Ganos suite is used instead).
        try:
            with engine.begin() as conn:
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
            print(
                "  [PostgreSQL: GIST geospatial indexes created"
                " via cube + earthdistance]"
            )
        except Exception as exc:
            print(f"  [earthdistance GIST indexes skipped: {exc}]")

        # --- ST_MakePoint geography GIST indexes (Ganos or PostGIS) ---
        # Required by the ST_DWithin / ST_Distance query path used when
        # ganos_spatialref (Aliyun Apsara RDS) or postgis is installed.
        # These coexist with the earthdistance indexes when both are present;
        # the query planner picks the appropriate one for each query.
        with engine.connect() as conn:
            has_ganos = _has_extension(conn, "ganos_spatialref")
            has_postgis = _has_extension(conn, "postgis")

        if has_ganos or has_postgis:
            label = "Ganos/ganos_spatialref" if has_ganos else "PostGIS"
            geo_stmts = [
                "CREATE INDEX IF NOT EXISTS geoname_postgis_idx ON geoname"
                " USING GIST (ST_MakePoint(longitude, latitude)::geography)",
                "CREATE INDEX IF NOT EXISTS postalcodes_postgis_idx ON postalcodes"
                " USING GIST (ST_MakePoint(longitude, latitude)::geography)",
            ]
            try:
                with engine.begin() as conn:
                    for stmt in geo_stmts:
                        conn.execute(text(stmt))
                print(f"  [PostgreSQL: {label} GIST indexes created]")
            except Exception as exc:
                # geography type may be absent even when ganos_spatialref /
                # postgis is installed (e.g. ganos_geometry not loaded via
                # CASCADE).  Fall back to earthdistance indexes already created.
                print(f"  [{label} GIST indexes skipped: {exc}]")
        else:
            print("  [Ganos/PostGIS indexes skipped: neither extension is available]")
    else:
        print(
            "  [GIST geospatial indexes skipped:"
            " earthdistance is PostgreSQL-only]"
        )
# create_indexes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Geonames data into a relational database via SQLAlchemy."
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML file (default: config/config.yaml)",
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip creating indexes and constraints (useful for faster testing)",
    )
    parser.add_argument(
        "-o", "--overwrite",
        action="store_true",
        help="Drop and recreate all tables before loading (overwrite existing data)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    dl = config["download"]
    meta_cfg = config.get("meta", {})

    data_dir = Path(dl["data_dir"])
    postal_dir = data_dir / dl["postal_subdir"]

    engine = build_engine(config)
    db_url = engine.url

    print("=" * 60)
    print("Geonames database loader")
    print(f"  Engine  : {db_url.get_dialect().name}")
    print(f"  Host    : {db_url.host}:{db_url.port}")
    print(f"  Database: {db_url.database}")
    print(f"  Data dir: {data_dir.resolve()}")
    print("=" * 60)

    # Verify required files exist
    required = {
        "allCountries.txt":             data_dir / "allCountries.txt",
        "alternateNames.txt":           data_dir / "alternateNames.txt",
        "admin1CodesASCII.txt":         data_dir / "admin1CodesASCII.txt",
        "admin2Codes.txt":              data_dir / "admin2Codes.txt",
        "featureCodes_en.txt":          data_dir / "featureCodes_en.txt",
        "iso-languagecodes.txt.tmp":    data_dir / "iso-languagecodes.txt.tmp",
        "timeZones.txt.tmp":            data_dir / "timeZones.txt.tmp",
        "countryInfo.txt.tmp":          data_dir / "countryInfo.txt.tmp",
        "postalcodes/allCountries.txt": postal_dir / "allCountries.txt",
    }
    missing = [name for name, path in required.items() if not path.exists()]
    if missing:
        print("\nERROR: Missing required data files. Run download_geonames.py first.")
        for m in missing:
            print(f"  - {m}")
        sys.exit(1)

    download_timestamp = datetime.now(timezone.utc)

    try:
        # ---------------------------------------------------------------- #
        # 1. Create tables (drop first if --overwrite was requested)
        # ---------------------------------------------------------------- #
        if args.overwrite:
            print("\nDropping and recreating tables ...")
            drop_and_create_tables(engine)
            print("  Tables created.")
        else:
            print("\nCreating tables (if not exist) ...")
            if is_postgresql(engine):
                with engine.begin() as conn:
                    try:
                        conn.execute(
                            text("CREATE EXTENSION IF NOT EXISTS unaccent")
                        )
                    except Exception:
                        pass  # enrich_admin_codes() will fall back to Python stripping
            metadata.create_all(engine)
            print("  Tables ready.")

        # ---------------------------------------------------------------- #
        # 2. Load data
        # ---------------------------------------------------------------- #
        print("\nLoading data:")

        load_file(
            engine, t_geoname,
            ["geonameid", "name", "asciiname", "alternatenames", "latitude",
             "longitude", "fclass", "fcode", "country", "cc2", "admin1",
             "admin2", "admin3", "admin4", "population", "elevation",
             "gtopo30", "timezone", "moddate"],
            data_dir / "allCountries.txt",
        )
        load_file(
            engine, t_alternatename,
            ["alternatenameid", "geonameid", "isolanguage", "alternatename",
             "ispreferredname", "isshortname", "iscolloquial", "ishistoric"],
            data_dir / "alternateNames.txt",
        )
        load_file(
            engine, t_timezones,
            ["countrycode", "timezoneid", "gmt_offset", "dst_offset", "raw_offset"],
            data_dir / "timeZones.txt.tmp",
        )
        load_file(
            engine, t_featurecodes,
            ["code", "name", "description"],
            data_dir / "featureCodes_en.txt",
        )
        load_file(
            engine, t_admin1codesascii,
            ["code", "name", "nameascii", "geonameid"],
            data_dir / "admin1CodesASCII.txt",
        )
        load_file(
            engine, t_admin2codesascii,
            ["code", "name", "nameascii", "geonameid"],
            data_dir / "admin2Codes.txt",
        )
        load_file(
            engine, t_iso_languagecodes,
            ["iso_639_3", "iso_639_2", "iso_639_1", "language_name"],
            data_dir / "iso-languagecodes.txt.tmp",
        )
        load_file(
            engine, t_countryinfo,
            ["iso_alpha2", "iso_alpha3", "iso_numeric", "fips_code", "country",
             "capital", "areainsqkm", "population", "continent", "tld",
             "currency_code", "currency_name", "phone", "postal", "postalregex",
             "languages", "geonameid", "neighbours", "equivalent_fips_code"],
            data_dir / "countryInfo.txt.tmp",
        )
        load_file(
            engine, t_postalcodes,
            ["countrycode", "postalcode", "placename", "admin1name", "admin1code",
             "admin2name", "admin2code", "admin3name", "admin3code",
             "latitude", "longitude", "accuracy"],
            postal_dir / "allCountries.txt",
        )

        # Continent codes are static — insert directly
        print("  Loading continentcodes ...", end=" ", flush=True)
        with engine.begin() as conn:
            conn.execute(t_continentcodes.insert(), [
                {"code": "AF", "name": "Africa",        "geonameid": 6255146},
                {"code": "AS", "name": "Asia",          "geonameid": 6255147},
                {"code": "EU", "name": "Europe",        "geonameid": 6255148},
                {"code": "NA", "name": "North America", "geonameid": 6255149},
                {"code": "OC", "name": "Oceania",       "geonameid": 6255150},
                {"code": "SA", "name": "South America", "geonameid": 6255151},
                {"code": "AN", "name": "Antarctica",    "geonameid": 6255152},
            ])
        print("done")

        # ---------------------------------------------------------------- #
        # 3. Admin-codes enrichment
        # ---------------------------------------------------------------- #
        print("\nEnriching admin-codes tables:")
        enrich_admin_codes(engine)

        # ---------------------------------------------------------------- #
        # 4. Metadata
        # ---------------------------------------------------------------- #
        print("\nInserting metadata ...")
        with engine.begin() as conn:
            conn.execute(t_meta.insert().values(
                version=meta_cfg.get("version", ""),
                data_uri=dl["url_data"],
                data_version=meta_cfg.get("data_version", ""),
                date_accessed=download_timestamp,
            ))
        print("  Metadata inserted.")

        # ---------------------------------------------------------------- #
        # 5. Indexes and constraints
        # ---------------------------------------------------------------- #
        if not args.skip_indexes:
            print("\nBuilding indexes and constraints (this may take a while) ...")
            create_indexes(engine)
            print("  Indexes created.")

            if is_postgresql(engine):
                print("  Running VACUUM ANALYZE ...", end=" ", flush=True)
                # VACUUM cannot run inside a transaction block
                with engine.connect() as conn:
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                    conn.execute(text("VACUUM ANALYZE"))
                print("done")
        else:
            print("\n  [Skipping indexes as requested]")

    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
    finally:
        engine.dispose()

    print("\nLoad complete.")
# main

# -----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
# __main__
