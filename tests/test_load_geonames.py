"""
Tests for src/load_geonames.py

Database-touching tests use SQLite in-memory via SQLAlchemy so no external
database is required.  PostgreSQL-only code paths (COPY, CASCADE DROP,
GIST indexes) are not tested here.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, select, text

import load_geonames as lg


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sqlite_engine():
    """Fresh SQLite in-memory engine with all tables created."""
    engine = create_engine("sqlite:///:memory:")
    lg.metadata.create_all(engine)
    yield engine
    engine.dispose()


def _write_tsv(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_returns_parsed_dict(self, tmp_path):
        cfg = tmp_path / "config.yaml"
        cfg.write_text("database:\n  url: sqlite:///test.db\n")
        result = lg.load_config(str(cfg))
        assert result == {"database": {"url": "sqlite:///test.db"}}

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises((FileNotFoundError, OSError)):
            lg.load_config(str(tmp_path / "missing.yaml"))


# ---------------------------------------------------------------------------
# build_engine
# ---------------------------------------------------------------------------

class TestBuildEngine:
    def test_url_format(self):
        cfg = {"database": {"url": "sqlite:///:memory:"}}
        engine = lg.build_engine(cfg)
        assert engine.url.drivername == "sqlite"
        engine.dispose()

    def test_legacy_postgresql_format(self):
        cfg = {"database": {
            "user": "u", "password": "p",
            "host": "localhost", "port": 5432, "dbname": "mydb",
        }}
        engine = lg.build_engine(cfg)
        assert "postgresql" in engine.url.drivername
        assert engine.url.host == "localhost"
        assert engine.url.database == "mydb"
        engine.dispose()


# ---------------------------------------------------------------------------
# is_postgresql
# ---------------------------------------------------------------------------

class TestIsPostgresql:
    def test_true_for_postgresql_dialect(self):
        engine = MagicMock()
        engine.dialect.name = "postgresql"
        assert lg.is_postgresql(engine) is True

    def test_false_for_sqlite(self):
        engine = create_engine("sqlite:///:memory:")
        assert lg.is_postgresql(engine) is False
        engine.dispose()

    def test_false_for_mysql(self):
        engine = MagicMock()
        engine.dialect.name = "mysql"
        assert lg.is_postgresql(engine) is False


# ---------------------------------------------------------------------------
# _iter_tsv_rows
# ---------------------------------------------------------------------------

class TestIterTsvRows:
    def test_basic_row(self, tmp_path):
        f = tmp_path / "data.txt"
        _write_tsv(f, ["val1\tval2\tval3"])
        rows = list(lg._iter_tsv_rows(f, ["a", "b", "c"]))
        assert rows == [{"a": "val1", "b": "val2", "c": "val3"}]

    def test_empty_string_becomes_none(self, tmp_path):
        f = tmp_path / "data.txt"
        _write_tsv(f, ["val1\t\tval3"])
        rows = list(lg._iter_tsv_rows(f, ["a", "b", "c"]))
        assert rows[0]["b"] is None

    def test_skips_comment_only_line(self, tmp_path):
        f = tmp_path / "data.txt"
        _write_tsv(f, ["# comment", "val1\tval2"])
        rows = list(lg._iter_tsv_rows(f, ["a", "b"]))
        assert len(rows) == 1
        assert rows[0]["a"] == "val1"

    def test_pads_short_lines_with_none(self, tmp_path):
        f = tmp_path / "data.txt"
        _write_tsv(f, ["val1"])
        rows = list(lg._iter_tsv_rows(f, ["a", "b", "c"]))
        assert rows[0] == {"a": "val1", "b": None, "c": None}

    def test_multiple_rows(self, tmp_path):
        f = tmp_path / "data.txt"
        _write_tsv(f, ["a1\tb1", "a2\tb2", "a3\tb3"])
        rows = list(lg._iter_tsv_rows(f, ["a", "b"]))
        assert len(rows) == 3
        assert rows[2] == {"a": "a3", "b": "b3"}


# ---------------------------------------------------------------------------
# _strip_accents
# ---------------------------------------------------------------------------

class TestStripAccents:
    def test_removes_accents(self):
        assert lg._strip_accents("Café") == "Cafe"
        assert lg._strip_accents("naïve") == "naive"
        assert lg._strip_accents("über") == "uber"
        assert lg._strip_accents("résumé") == "resume"

    def test_none_returns_none(self):
        assert lg._strip_accents(None) is None

    def test_ascii_string_unchanged(self):
        assert lg._strip_accents("hello world") == "hello world"

    def test_empty_string(self):
        assert lg._strip_accents("") == ""


# ---------------------------------------------------------------------------
# drop_and_create_tables  (SQLite path)
# ---------------------------------------------------------------------------

class TestDropAndCreateTables:
    def test_creates_all_expected_tables(self):
        engine = create_engine("sqlite:///:memory:")
        lg.drop_and_create_tables(engine)
        expected = {
            "geoname", "alternatename", "countryinfo", "iso_languagecodes",
            "admin1codesascii", "admin2codesascii", "featurecodes", "timezones",
            "continentcodes", "postalcodes", "meta",
        }
        with engine.connect() as conn:
            existing = {
                row[0]
                for row in conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                )
            }
        assert expected.issubset(existing)
        engine.dispose()

    def test_tables_are_empty_after_creation(self):
        engine = create_engine("sqlite:///:memory:")
        lg.drop_and_create_tables(engine)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT count(*) FROM geoname")).scalar()
        assert count == 0
        engine.dispose()


# ---------------------------------------------------------------------------
# _insert_chunks  (SQLite path)
# ---------------------------------------------------------------------------

class TestInsertChunks:
    # Use t_featurecodes (code, name, description) — all String columns,
    # no Date/numeric types that SQLite would reject from raw string values.
    _COLS = ["code", "name", "description"]

    def test_inserts_single_row(self, tmp_path, sqlite_engine):
        f = tmp_path / "data.txt"
        _write_tsv(f, ["A.PPL\tPopulated place\tA populated place"])
        count = lg._insert_chunks(sqlite_engine, lg.t_featurecodes, self._COLS, f)
        assert count == 1
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_featurecodes)).fetchone()
        assert row.name == "Populated place"
        assert row.code == "A.PPL"

    def test_inserts_multiple_rows(self, tmp_path, sqlite_engine):
        rows = [
            "A.ADM1\tFirst-order admin\tFirst-order administrative division",
            "A.ADM2\tSecond-order admin\tSecond-order administrative division",
        ]
        f = tmp_path / "data.txt"
        _write_tsv(f, rows)
        count = lg._insert_chunks(sqlite_engine, lg.t_featurecodes, self._COLS, f)
        assert count == 2

    def test_returns_row_count(self, tmp_path, sqlite_engine):
        rows = [f"X.FC{i}\tFeature {i}\tDesc {i}" for i in range(5)]
        f = tmp_path / "data.txt"
        _write_tsv(f, rows)
        count = lg._insert_chunks(sqlite_engine, lg.t_featurecodes, self._COLS, f)
        assert count == 5


# ---------------------------------------------------------------------------
# enrich_admin_codes  (SQLite path — _enrich_nameascii_python branch)
# ---------------------------------------------------------------------------

class TestEnrichAdminCodes:
    def test_countrycode_derived_from_admin1_code(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            conn.execute(lg.t_admin1codesascii.insert(), [
                {"code": "US.CA", "name": "California", "nameascii": "California", "geonameid": 1},
            ])
        lg.enrich_admin_codes(sqlite_engine)
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_admin1codesascii)).fetchone()
        assert row.countrycode == "US"

    def test_countrycode_derived_from_admin2_code(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            conn.execute(lg.t_admin2codesascii.insert(), [
                {"code": "MX.OA.001", "name": "Oaxaca Centro", "nameascii": "Oaxaca Centro", "geonameid": 2},
            ])
        lg.enrich_admin_codes(sqlite_engine)
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_admin2codesascii)).fetchone()
        assert row.countrycode == "MX"

    def test_null_name_filled_from_nameascii(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            conn.execute(lg.t_admin1codesascii.insert(), [
                {"code": "MX.OA", "name": None, "nameascii": "Oaxaca", "geonameid": 3},
            ])
        lg.enrich_admin_codes(sqlite_engine)
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_admin1codesascii)).fetchone()
        assert row.name == "Oaxaca"

    def test_postal_admin1code_full(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            conn.execute(lg.t_postalcodes.insert(), [_postal_row("US", "90210", "CA")])
        lg.enrich_admin_codes(sqlite_engine)
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_postalcodes)).fetchone()
        assert row.admin1code_full == "US.CA"

    def test_postal_admin2code_full(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            conn.execute(lg.t_postalcodes.insert(), [_postal_row("US", "90210", "CA", admin2code="001")])
        lg.enrich_admin_codes(sqlite_engine)
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_postalcodes)).fetchone()
        assert row.admin2code_full == "US.CA.001"

    def test_postal_admin3code_full(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            conn.execute(lg.t_postalcodes.insert(), [
                _postal_row("US", "90210", "CA", admin2code="001", admin3code="X01")
            ])
        lg.enrich_admin_codes(sqlite_engine)
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_postalcodes)).fetchone()
        assert row.admin3code_full == "US.CA.001.X01"

    def test_postal_nameascii_accent_stripping(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            conn.execute(lg.t_postalcodes.insert(), [
                _postal_row("MX", "68000", "OA", admin1name="Oaxaçá"),
            ])
        lg.enrich_admin_codes(sqlite_engine)
        with sqlite_engine.connect() as conn:
            row = conn.execute(select(lg.t_postalcodes)).fetchone()
        assert row.admin1nameascii == "Oaxaca"


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------

def _postal_row(
    countrycode: str,
    postalcode: str,
    admin1code: str,
    admin1name: str = "State",
    admin2code: str | None = None,
    admin2name: str | None = None,
    admin3code: str | None = None,
    admin3name: str | None = None,
) -> dict:
    return {
        "countrycode": countrycode,
        "postalcode": postalcode,
        "placename": "City",
        "admin1name": admin1name,
        "admin1code": admin1code,
        "admin2name": admin2name,
        "admin2code": admin2code,
        "admin3name": admin3name,
        "admin3code": admin3code,
        "latitude": 0.0,
        "longitude": 0.0,
        "accuracy": 1,
        "admin1code_full": None,
        "admin2code_full": None,
        "admin3code_full": None,
        "admin1nameascii": None,
        "admin2nameascii": None,
        "admin3nameascii": None,
    }
