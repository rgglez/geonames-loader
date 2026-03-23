"""
base.py — GeonamesLoader base class and shared utilities.

Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>
Licensed under GPL 3.0. See LICENSE for details.
"""

import argparse
import csv
import sys
import unicodedata
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import tqdm
import yaml
from sqlalchemy import Table, create_engine, func, text, update
from sqlalchemy.engine import Engine

from . import models


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 10_000

_DROP_ORDER = [
    models.t_alternatename, models.t_countryinfo, models.t_geoname,
    models.t_postalcodes, models.t_admin1codesascii, models.t_admin2codesascii,
    models.t_iso_languagecodes, models.t_featurecodes, models.t_timezones,
    models.t_continentcodes, models.t_meta,
]


# ---------------------------------------------------------------------------
# Config / engine helpers
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
# load_config


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


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

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


def _strip_accents(s: str | None) -> str | None:
    """Remove combining accent marks from a Unicode string."""
    if s is None:
        return None
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
# _strip_accents


def _has_extension(conn, name: str) -> bool:
    """Return True if the named PostgreSQL extension is installed."""
    count = conn.execute(
        text("SELECT count(*) FROM pg_extension WHERE extname = :n"),
        {"n": name},
    ).scalar()
    return bool(count)
# _has_extension


# ---------------------------------------------------------------------------
# Base loader class
# ---------------------------------------------------------------------------

class GeonamesLoader:
    """
    Base loader — works on any SQLAlchemy-supported dialect (generic INSERT).

    Subclasses override individual methods to specialise behaviour for a
    specific database engine or plugin.  Use the factory method
    GeonamesLoader.create(engine) to obtain the right instance.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    # --- Factory ---

    @classmethod
    def create(cls, engine: Engine) -> "GeonamesLoader":
        """Return the most-specific loader for the connected database."""
        # Lazy imports avoid circular dependencies between loader modules.
        from .mysql_loader import MySQLLoader
        from .postgresql_loader import PostgreSQLLoader
        from .postgresql_ganos_loader import PostgreSQLGanosLoader
        from .postgresql_postgis_loader import PostgreSQLPostGISLoader

        dialect = engine.dialect.name
        if dialect == "postgresql":
            with engine.connect() as conn:
                has_ganos = _has_extension(conn, "ganos_spatialref")
                has_postgis = _has_extension(conn, "postgis")
            if has_ganos:
                return PostgreSQLGanosLoader(engine)
            if has_postgis:
                return PostgreSQLPostGISLoader(engine)
            return PostgreSQLLoader(engine)
        if dialect in ("mysql", "mariadb"):
            return MySQLLoader(engine)
        if dialect == "sqlite":
            from .sqlite_loader import SQLiteLoader
            return SQLiteLoader(engine)
        return cls(engine)
    # create

    # --- Schema management ---

    def drop_tables(self) -> None:
        with self.engine.begin() as conn:
            for tbl in _DROP_ORDER:
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl.name}"))
    # drop_tables

    def create_extension(self) -> None:
        """Install any required database extensions. No-op on generic engines."""
    # create_extension

    def create_tables(self) -> None:
        self.create_extension()
        models.metadata.create_all(self.engine)
    # create_tables

    # --- Bulk load ---

    def _bulk_load(self, table: Table, columns: list[str], filepath: Path) -> None:
        """Chunked INSERT — default for non-PostgreSQL engines."""
        chunk: list[dict] = []
        with self.engine.begin() as conn, \
             tqdm.tqdm(
                 _iter_tsv_rows(filepath, columns),
                 desc=f"  {table.name}",
                 unit=" rows", unit_scale=True, mininterval=0.5,
             ) as pbar:
            for row in pbar:
                chunk.append(row)
                if len(chunk) >= _CHUNK_SIZE:
                    conn.execute(table.insert(), chunk)
                    chunk = []
            if chunk:
                conn.execute(table.insert(), chunk)
    # _bulk_load

    def load_file(self, table: Table, columns: list[str], filepath: Path) -> None:
        """Load a TSV data file into a table using the best method for the dialect."""
        self._bulk_load(table, columns, filepath)
    # load_file

    # --- Enrichment ---

    def _enrich_nameascii(self) -> None:
        """Populate admin*nameascii columns via Python-side accent stripping."""
        pc = models.t_postalcodes.c
        for src_col, dst_col in [
            (pc.admin1name, pc.admin1nameascii),
            (pc.admin2name, pc.admin2nameascii),
            (pc.admin3name, pc.admin3nameascii),
        ]:
            with self.engine.connect() as conn:
                names = {
                    row[0]
                    for row in conn.execute(
                        models.t_postalcodes.select()
                        .with_only_columns(src_col).distinct()
                    )
                    if row[0] is not None
                }
            if not names:
                continue
            with self.engine.begin() as conn:
                for name in tqdm.tqdm(names, desc=f"  {src_col.key}", unit=" names"):
                    conn.execute(
                        update(models.t_postalcodes)
                        .where(src_col == name)
                        .values({dst_col.key: _strip_accents(name)})
                    )
    # _enrich_nameascii

    def enrich_admin_codes(self) -> None:
        """Populate all derived columns after the initial bulk load."""
        print("  Enriching admin-codes tables ...")

        with self.engine.begin() as conn:
            # Fix nulls: admin1codesascii.name ← nameascii where name is NULL
            conn.execute(
                update(models.t_admin1codesascii)
                .where(models.t_admin1codesascii.c.name.is_(None))
                .where(models.t_admin1codesascii.c.nameascii.is_not(None))
                .values(name=models.t_admin1codesascii.c.nameascii)
            )

            # countrycode = first 2 chars of code
            # func.substr() renders portably across PostgreSQL, MySQL, SQLite
            conn.execute(
                update(models.t_admin1codesascii)
                .values(countrycode=func.substr(models.t_admin1codesascii.c.code, 1, 2))
            )
            conn.execute(
                update(models.t_admin2codesascii)
                .values(countrycode=func.substr(models.t_admin2codesascii.c.code, 1, 2))
            )

            # Composite admin code fields using SQLAlchemy string concatenation.
            # The + operator on String columns renders as || (PostgreSQL/SQLite)
            # or CONCAT() (MySQL) automatically via SQLAlchemy's type system.
            pc = models.t_postalcodes.c
            conn.execute(
                update(models.t_postalcodes)
                .where(pc.admin1code.is_not(None))
                .where(pc.admin1code != "")
                .values(admin1code_full=pc.countrycode + "." + pc.admin1code)
            )
            conn.execute(
                update(models.t_postalcodes)
                .where(pc.admin1code.is_not(None)).where(pc.admin1code != "")
                .where(pc.admin2code.is_not(None)).where(pc.admin2code != "")
                .values(admin2code_full=(
                    pc.countrycode + "." + pc.admin1code + "." + pc.admin2code
                ))
            )
            conn.execute(
                update(models.t_postalcodes)
                .where(pc.admin1code.is_not(None)).where(pc.admin1code != "")
                .where(pc.admin2code.is_not(None)).where(pc.admin2code != "")
                .where(pc.admin3code.is_not(None)).where(pc.admin3code != "")
                .values(admin3code_full=(
                    pc.countrycode + "." + pc.admin1code
                    + "." + pc.admin2code + "." + pc.admin3code
                ))
            )

        self._enrich_nameascii()
    # enrich_admin_codes

    # --- Indexes and constraints ---

    def _add_pk_fk_constraints(self) -> None:
        """Add primary key and foreign key constraints. No-op on generic engines."""
        print("  [Primary key / foreign key constraints skipped:"
              " not supported by this dialect]")
    # _add_pk_fk_constraints

    def _add_btree_indexes(self) -> None:
        """Create portable B-tree indexes defined in models.indexes."""
        with tqdm.tqdm(models.indexes, desc="  indexes", unit=" idx") as pbar:
            for idx in pbar:
                pbar.set_postfix_str(idx.name)
                idx.create(self.engine)
    # _add_btree_indexes

    def _add_geo_indexes(self) -> None:
        """Create geospatial indexes. No-op on non-PostgreSQL engines."""
        print("  [GIST geospatial indexes skipped: PostgreSQL-only]")
    # _add_geo_indexes

    def create_indexes(self) -> None:
        """Add all indexes and constraints after bulk load."""
        self._add_pk_fk_constraints()
        self._add_btree_indexes()
        self._add_geo_indexes()
    # create_indexes

    def vacuum(self) -> None:
        """Run post-load maintenance. No-op on non-PostgreSQL engines."""
    # vacuum

    # --- Full orchestration ---

    def run(self, args: argparse.Namespace, config: dict) -> None:
        dl = config["download"]
        meta_cfg = config.get("meta", {})
        data_dir = Path(dl["data_dir"])
        postal_dir = data_dir / dl["postal_subdir"]
        db_url = self.engine.url

        print("=" * 60)
        print("Geonames database loader")
        print(f"  Engine  : {db_url.get_dialect().name}")
        if db_url.host:
            print(f"  Host    : {db_url.host}:{db_url.port}")
            print(f"  Database: {db_url.database}")
        else:
            print(f"  File    : {db_url.database}")
        print(f"  Data dir: {data_dir.resolve()}")
        print("=" * 60)

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
            print("\nERROR: Missing required data files."
                  " Run download_geonames.py first.")
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
                self.drop_tables()
                print("  Tables dropped.")
            else:
                print("\nCreating tables (if not exist) ...")
            self.create_tables()
            print("  Tables ready.")

            # ---------------------------------------------------------------- #
            # 2. Load data
            # ---------------------------------------------------------------- #
            print("\nLoading data:")

            self.load_file(
                models.t_geoname,
                ["geonameid", "name", "asciiname", "alternatenames", "latitude",
                 "longitude", "fclass", "fcode", "country", "cc2", "admin1",
                 "admin2", "admin3", "admin4", "population", "elevation",
                 "gtopo30", "timezone", "moddate"],
                data_dir / "allCountries.txt",
            )
            self.load_file(
                models.t_alternatename,
                ["alternatenameid", "geonameid", "isolanguage", "alternatename",
                 "ispreferredname", "isshortname", "iscolloquial", "ishistoric"],
                data_dir / "alternateNames.txt",
            )
            self.load_file(
                models.t_timezones,
                ["countrycode", "timezoneid", "gmt_offset", "dst_offset", "raw_offset"],
                data_dir / "timeZones.txt.tmp",
            )
            self.load_file(
                models.t_featurecodes,
                ["code", "name", "description"],
                data_dir / "featureCodes_en.txt",
            )
            self.load_file(
                models.t_admin1codesascii,
                ["code", "name", "nameascii", "geonameid"],
                data_dir / "admin1CodesASCII.txt",
            )
            self.load_file(
                models.t_admin2codesascii,
                ["code", "name", "nameascii", "geonameid"],
                data_dir / "admin2Codes.txt",
            )
            self.load_file(
                models.t_iso_languagecodes,
                ["iso_639_3", "iso_639_2", "iso_639_1", "language_name"],
                data_dir / "iso-languagecodes.txt.tmp",
            )
            self.load_file(
                models.t_countryinfo,
                ["iso_alpha2", "iso_alpha3", "iso_numeric", "fips_code", "country",
                 "capital", "areainsqkm", "population", "continent", "tld",
                 "currency_code", "currency_name", "phone", "postal", "postalregex",
                 "languages", "geonameid", "neighbours", "equivalent_fips_code"],
                data_dir / "countryInfo.txt.tmp",
            )
            self.load_file(
                models.t_postalcodes,
                ["countrycode", "postalcode", "placename", "admin1name", "admin1code",
                 "admin2name", "admin2code", "admin3name", "admin3code",
                 "latitude", "longitude", "accuracy"],
                postal_dir / "allCountries.txt",
            )

            # Continent codes are static — insert directly
            print("  Loading continentcodes ...", end=" ", flush=True)
            with self.engine.begin() as conn:
                conn.execute(models.t_continentcodes.insert(), [
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
            self.enrich_admin_codes()

            # ---------------------------------------------------------------- #
            # 4. Metadata
            # ---------------------------------------------------------------- #
            print("\nInserting metadata ...")
            with self.engine.begin() as conn:
                conn.execute(models.t_meta.insert().values(
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
                print("\nBuilding indexes and constraints"
                      " (this may take a while) ...")
                self.create_indexes()
                print("  Indexes created.")
                self.vacuum()
            else:
                print("\n  [Skipping indexes as requested]")

        except Exception as e:
            print(f"\nError: {e}")
            sys.exit(1)
        finally:
            self.engine.dispose()

        print("\nLoad complete.")
    # run
