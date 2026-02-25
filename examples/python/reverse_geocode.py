#!/usr/bin/env python3

"""
    reverse_geocode.py
    Given a latitude and longitude, finds the nearest postal address
    and named place in the GeoNames database.

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

    Usage:
        python reverse_geocode.py --lat 19.4326 --lon -99.1332
        python reverse_geocode.py --lat 48.8566 --lon 2.3522 --results 5
        python reverse_geocode.py --lat 51.5074 --lon -0.1278 \\
            --url "postgresql+psycopg2://user:pass@host/db"
        python reverse_geocode.py --lat 48.8566 --lon 2.3522 --country FR

    The --config flag points to the same YAML used by load_geonames.py.
    The --url flag accepts any SQLAlchemy connection URL and overrides --config.

    Distance strategy (chosen automatically):
    - PostgreSQL: uses the earthdistance extension (cube + ll_to_earth) with
        a GIST index for fast KNN lookup (requires load_geonames.py to have
        been run without --skip-indexes).
    - Other dialects: Haversine formula executed server-side via SQL math
        functions (sin, cos, asin, sqrt). Available on MySQL/MariaDB and
        SQLite >= 3.35 (Python 3.8+). Falls back to a full table scan.
"""

import argparse
import math
import sys

import yaml
from sqlalchemy import (
    BigInteger, CHAR, Column, Float, Integer,
    MetaData, String, Table,
    create_engine, func, select, text,
)
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Table definitions (subset of columns needed for the query)
# ---------------------------------------------------------------------------

metadata = MetaData()

t_postalcodes = Table(
    "postalcodes", metadata,
    Column("countrycode",     CHAR(2),      nullable=True),
    Column("postalcode",      String(20),   nullable=True),
    Column("placename",       String(180),  nullable=True),
    Column("admin1name",      String(100),  nullable=True),
    Column("admin2name",      String(100),  nullable=True),
    Column("admin3name",      String(100),  nullable=True),
    Column("latitude",        Float,        nullable=True),
    Column("longitude",       Float,        nullable=True),
    Column("admin1code_full", String(100),  nullable=True),
    Column("admin2code_full", String(100),  nullable=True),
)

t_geoname = Table(
    "geoname", metadata,
    Column("geonameid",  Integer,      nullable=True),
    Column("name",       String(200),  nullable=True),
    Column("asciiname",  String(200),  nullable=True),
    Column("latitude",   Float,        nullable=True),
    Column("longitude",  Float,        nullable=True),
    Column("fclass",     CHAR(1),      nullable=True),
    Column("fcode",      String(10),   nullable=True),
    Column("country",    String(3),    nullable=True),
    Column("admin1",     String(20),   nullable=True),
    Column("admin2",     String(80),   nullable=True),
    Column("population", BigInteger,   nullable=True),
)


# ---------------------------------------------------------------------------
# Engine helpers
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)
# load_config


# -----------------------------------------------------------------------------


def build_engine(args: argparse.Namespace) -> Engine:
    """Build a SQLAlchemy engine from --url or the config file."""
    if args.url:
        return create_engine(args.url)
    cfg = load_config(args.config)
    db = cfg["database"]
    if "url" in db:
        return create_engine(db["url"])
    return create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['dbname']}"
    )
# build_engine


# -----------------------------------------------------------------------------


def is_postgresql(engine: Engine) -> bool:
    return engine.dialect.name == "postgresql"
# is_postgresql


# ---------------------------------------------------------------------------
# Haversine distance expression (non-PostgreSQL dialects)
# ---------------------------------------------------------------------------

_EARTH_RADIUS_KM = 6371.0
_RAD = math.pi / 180.0


def _haversine_km(lat: float, lon: float, col_lat, col_lon):
    """
    Return a SQLAlchemy column expression for the Haversine distance (km)
    between a fixed point (lat, lon) and the row's (col_lat, col_lon).
    Uses only basic SQL math functions available on all supported dialects.
    """
    dlat = (col_lat - lat) * _RAD
    dlon = (col_lon - lon) * _RAD
    cos_lat1 = math.cos(lat * _RAD)   # constant — evaluated in Python

    a = (
        func.power(func.sin(dlat / 2), 2)
        + cos_lat1
        * func.cos(col_lat * _RAD)
        * func.power(func.sin(dlon / 2), 2)
    )
    return (2 * _EARTH_RADIUS_KM * func.asin(func.sqrt(a))).label(
        "distance_km"
    )
# _haversine_km


# ---------------------------------------------------------------------------
# PostgreSQL earthdistance queries (use GIST index via earth_box pre-filter)
# ---------------------------------------------------------------------------

# Initial search radius for earth_box() pre-filter.
# Increase if you expect the nearest result to be farther than this.
_GEO_RADIUS_M = 500_000   # 500 km


def _query_postal_pg(conn, lat: float, lon: float, limit: int,
                     country: str = None) -> list:
    """
    Postal-code query using PostgreSQL earthdistance extension.
    earth_box() enables GIST index pre-filtering;
    earth_distance() provides accurate ordering.
    """
    country_clause = "   AND countrycode = :country" if country else ""
    stmt = text(
        "SELECT countrycode, postalcode, placename,"
        "       admin1name, admin2name, admin3name,"
        "       latitude, longitude,"
        "       earth_distance("
        "           ll_to_earth(latitude, longitude),"
        "           ll_to_earth(:lat, :lon)"
        "       ) / 1000.0 AS distance_km"
        " FROM postalcodes"
        " WHERE latitude IS NOT NULL"
        "   AND longitude IS NOT NULL"
        "   AND earth_box(ll_to_earth(:lat, :lon), :radius)"
        "       @> ll_to_earth(latitude, longitude)"
        f" {country_clause}"
        " ORDER BY distance_km"
        " LIMIT :limit"
    )
    params = {"lat": lat, "lon": lon, "radius": _GEO_RADIUS_M, "limit": limit}
    if country:
        params["country"] = country
    return conn.execute(stmt, params).fetchall()
# _query_postal_pg


# -----------------------------------------------------------------------------


def _query_geo_pg(conn, lat: float, lon: float, limit: int,
                  country: str = None) -> list:
    """
    Geoname query using PostgreSQL earthdistance extension.
    """
    country_clause = "   AND country = :country" if country else ""
    stmt = text(
        "SELECT geonameid, name, fclass, fcode, country,"
        "       admin1, admin2, population, latitude, longitude,"
        "       earth_distance("
        "           ll_to_earth(latitude, longitude),"
        "           ll_to_earth(:lat, :lon)"
        "       ) / 1000.0 AS distance_km"
        " FROM geoname"
        " WHERE latitude IS NOT NULL"
        "   AND longitude IS NOT NULL"
        "   AND earth_box(ll_to_earth(:lat, :lon), :radius)"
        "       @> ll_to_earth(latitude, longitude)"
        f" {country_clause}"
        " ORDER BY distance_km"
        " LIMIT :limit"
    )
    params = {"lat": lat, "lon": lon, "radius": _GEO_RADIUS_M, "limit": limit}
    if country:
        params["country"] = country
    return conn.execute(stmt, params).fetchall()
# _query_geo_pg


# ---------------------------------------------------------------------------
# Public query dispatchers
# ---------------------------------------------------------------------------

def query_postalcodes(
    engine: Engine, conn, lat: float, lon: float, limit: int,
    country: str = None,
):
    """Return the closest rows from postalcodes ordered by distance."""
    if is_postgresql(engine):
        return _query_postal_pg(conn, lat, lon, limit, country)
    pc = t_postalcodes.c
    dist = _haversine_km(lat, lon, pc.latitude, pc.longitude)
    stmt = (
        select(
            pc.countrycode,
            pc.postalcode,
            pc.placename,
            pc.admin1name,
            pc.admin2name,
            pc.admin3name,
            pc.latitude,
            pc.longitude,
            dist,
        )
        .where(pc.latitude.is_not(None))
        .where(pc.longitude.is_not(None))
        .order_by(dist)
        .limit(limit)
    )
    if country:
        stmt = stmt.where(pc.countrycode == country)
    return conn.execute(stmt).fetchall()
# query_postalcodes


# -----------------------------------------------------------------------------


def query_geoname(
    engine: Engine, conn, lat: float, lon: float, limit: int,
    country: str = None,
):
    """Return the closest rows from geoname ordered by distance."""
    if is_postgresql(engine):
        return _query_geo_pg(conn, lat, lon, limit, country)
    g = t_geoname.c
    dist = _haversine_km(lat, lon, g.latitude, g.longitude)
    stmt = (
        select(
            g.geonameid,
            g.name,
            g.fclass,
            g.fcode,
            g.country,
            g.admin1,
            g.admin2,
            g.population,
            g.latitude,
            g.longitude,
            dist,
        )
        .where(g.latitude.is_not(None))
        .where(g.longitude.is_not(None))
        .order_by(dist)
        .limit(limit)
    )
    if country:
        stmt = stmt.where(g.country == country)
    return conn.execute(stmt).fetchall()
# query_geoname


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _print_postal(rows) -> None:
    print(f"Nearest postal-code entries ({len(rows)} result(s)):\n")
    for r in rows:
        print(f"  Country     : {r.countrycode}")
        print(f"  Postal code : {r.postalcode}")
        print(f"  Place       : {r.placename}")
        if r.admin3name:
            print(f"  Admin 3     : {r.admin3name}")
        if r.admin2name:
            print(f"  Admin 2     : {r.admin2name}")
        if r.admin1name:
            print(f"  Admin 1     : {r.admin1name}")
        print(f"  Coordinates : {r.latitude}, {r.longitude}")
        print(f"  Distance    : {r.distance_km:.3f} km")
        print()
# _print_postal


# -----------------------------------------------------------------------------


def _print_geoname(rows) -> None:
    print(f"Nearest geoname entries ({len(rows)} result(s)):\n")
    for r in rows:
        print(f"  GeoName ID  : {r.geonameid}")
        print(f"  Name        : {r.name}")
        print(f"  Country     : {r.country}")
        print(f"  Feature     : {r.fclass}/{r.fcode}")
        print(f"  Population  : {r.population or 0:,}")
        print(f"  Coordinates : {r.latitude}, {r.longitude}")
        print(f"  Distance    : {r.distance_km:.3f} km")
        print()
# _print_geoname


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Reverse geocode: find the nearest address for coordinates "
            "using the GeoNames database."
        )
    )
    parser.add_argument(
        "--lat",
        type=float,
        required=True,
        help="Latitude in decimal degrees (e.g. 19.4326)",
    )
    parser.add_argument(
        "--lon",
        type=float,
        required=True,
        help="Longitude in decimal degrees (e.g. -99.1332)",
    )
    parser.add_argument(
        "--config",
        default="../config/config.yaml",
        help=(
            "Path to config YAML file "
            "(default: ../config/config.yaml)"
        ),
    )
    parser.add_argument(
        "--url",
        help=(
            "SQLAlchemy connection URL — overrides --config. "
            "Example: postgresql+psycopg2://user:pass@host/db"
        ),
    )
    parser.add_argument(
        "--results",
        type=int,
        default=3,
        metavar="N",
        help="Number of nearest results to return (default: 3)",
    )
    parser.add_argument(
        "--country",
        default=None,
        metavar="CC",
        help=(
            "Restrict results to this ISO 3166-1 alpha-2 country code "
            "(e.g. MX, FR, DE). If omitted, all countries are searched."
        ),
    )
    args = parser.parse_args()

    if not (-90 <= args.lat <= 90):
        print("ERROR: --lat must be between -90 and 90.")
        sys.exit(1)
    if not (-180 <= args.lon <= 180):
        print("ERROR: --lon must be between -180 and 180.")
        sys.exit(1)

    engine = build_engine(args)

    print("=" * 60)
    print("GeoNames reverse geocoder")
    print(f"  Latitude  : {args.lat}")
    print(f"  Longitude : {args.lon}")
    print(f"  Results   : {args.results}")
    if args.country:
        print(f"  Country   : {args.country}")
    print("=" * 60)
    print()

    try:
        with engine.connect() as conn:
            postal_rows = query_postalcodes(
                engine, conn, args.lat, args.lon, args.results, args.country
            )
            if postal_rows:
                _print_postal(postal_rows)
            else:
                print(
                    "No postal-code data found for these coordinates.\n"
                )

            print("-" * 60)
            print()

            geo_rows = query_geoname(
                engine, conn, args.lat, args.lon, args.results, args.country
            )
            if geo_rows:
                _print_geoname(geo_rows)
            else:
                print("No geoname entries found.\n")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        engine.dispose()
# main


# -----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
# __main__
