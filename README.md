# geonames-loader

[![License](https://img.shields.io/badge/GitHub-GPL--3.0-informational)](https://www.gnu.org/licenses/gpl-3.0.html)
![GitHub all releases](https://img.shields.io/github/downloads/rgglez/geonames-loader/total)
![GitHub issues](https://img.shields.io/github/issues/rgglez/geonames-loader)
![GitHub commit activity](https://img.shields.io/github/commit-activity/y/rgglez/geonames-loader)
![GitHub stars](https://img.shields.io/github/stars/rgglez/geonames-loader?style=social)
![GitHub forks](https://img.shields.io/github/forks/rgglez/geonames-loader?style=social)

Python scripts to download the [GeoNames](https://www.geonames.org/) data files and upload them into relational databases.

> *The GeoNames geographical database covers all countries and contains over eleven million placenames that are available for download free of charge.*

---

## Requirements

- Python 3.10+
- An existing database (PostgreSQL, MySQL/MariaDB, or SQLite)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

| Package | Version | Notes |
|---------|---------|-------|
| `SQLAlchemy` | ≥ 2.0 | ORM / database abstraction |
| `PyYAML` | ≥ 6.0 | Config file parsing |
| `requests` | ≥ 2.31 | HTTP downloads |
| `tqdm` | ≥ 4.66 | Download progress bars |
| `psycopg2-binary` | ≥ 2.9 | PostgreSQL driver |
| `pymysql` | ≥ 1.1 | MySQL / MariaDB driver |

SQLite uses Python's built-in `sqlite3` module — no extra driver needed.
If you prefer the C-based MySQL driver, replace `pymysql` with `mysqlclient>=2.2`
(see the comment in `requirements.txt`).

---

## Configuration

Both scripts read `config/config.yaml` by default. Use `--config` to point to a
different file.

```yaml
database:
  # Option A — single SQLAlchemy URL (any supported dialect)
  url: "postgresql+psycopg2://user:pass@localhost:5432/geonames"

  # Option B — individual PostgreSQL fields (builds the URL automatically)
  # host: localhost
  # port: 5432
  # user: myuser
  # password: mypassword
  # dbname: geonames

download:
  data_dir: data                  # local directory for downloaded files
  postal_subdir: postalcodes      # subdirectory inside data_dir for postal data
  url_data: "https://download.geonames.org/export/dump"
  url_postal: "https://download.geonames.org/export/zip"
  files:
    - allCountries.zip
    - alternateNames.zip
    - admin1CodesASCII.txt
    - admin2Codes.txt
    - featureCodes_en.txt
    - iso-languagecodes.txt
    - timeZones.txt
    - countryInfo.txt

meta:
  version: "1.0"
  data_version: "2025"
```

Supported SQLAlchemy URL formats:

```
postgresql+psycopg2://user:pass@host:5432/dbname
mysql+pymysql://user:pass@host:3306/dbname
sqlite:///path/to/file.db
```

> The database must exist before running either script. Only tables and data
> are created/populated — the database itself is never created automatically.

---

## Usage

### 1. Download data — `download_geonames.py`

Downloads all GeoNames data files from [geonames.org](https://www.geonames.org)
into the local `data_dir` defined in the config. Files are skipped if they are
already up to date (size check). ZIP archives are extracted automatically.
Several files receive post-processing (header stripping, comment removal) to
make them ready for bulk loading.

```bash
src/download_geonames.py [--config CONFIG_FILE]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `config/config.yaml` | Path to the YAML configuration file |

**Examples:**

```bash
# Download using the default config
python src/download_geonames.py

# Use a custom config file
python src/download_geonames.py --config /etc/geonames/config.yaml
```

---

### 2. Load data — `load_geonames.py`

Creates the GeoNames schema in the target database and bulk-loads all
downloaded files. On PostgreSQL the load uses the native `COPY` protocol for
maximum speed; on other dialects it falls back to chunked `INSERT` statements.

After loading, derived columns are populated (composite admin codes,
ASCII-normalised name variants) and, unless `--skip-indexes` is passed, all
indexes, primary keys, foreign keys, and geospatial GIST indexes are created.

```bash
src/load_geonames.py [--config CONFIG_FILE] [--skip-indexes] [-o]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `config/config.yaml` | Path to the YAML configuration file |
| `--skip-indexes` | off | Skip creating indexes and constraints (useful for faster testing) |
| `-o`, `--overwrite` | off | Drop and recreate all tables before loading (overwrites existing data) |

**Examples:**

```bash
# Full load (download first, then load)
python src/download_geonames.py
python src/load_geonames.py

# Overwrite an existing database
python src/load_geonames.py --overwrite

# Load quickly without indexes (e.g. for development/testing)
python src/load_geonames.py --skip-indexes

# Use a custom config file
python src/load_geonames.py --config /etc/geonames/config.yaml

# Custom config + overwrite + skip indexes
python src/load_geonames.py --config /etc/geonames/config.yaml --overwrite --skip-indexes
```

> **Note:** `--skip-indexes` disables the geospatial GIST indexes on
> PostgreSQL. The reverse geocoding examples will still work, but they will
> fall back to a full table scan instead of using the fast KNN index.

---

## Distance strategy

The strategy is chosen automatically based on the database dialect:

| Dialect | Strategy | Notes |
|---------|----------|-------|
| PostgreSQL | `earthdistance` extension + GIST index (`earth_box`) | Fast KNN. Requires `load_geonames.py` to have been run **without** `--skip-indexes`. |
| MySQL / MariaDB | Haversine formula in SQL | Full table scan (no spatial index equivalent). |
| SQLite | Haversine formula in SQL | Requires SQLite ≥ 3.35 and CGO enabled (Go). |

---

## Reverse geocoding examples

Both examples (`examples/go/` and `examples/python/`) accept the same set of
arguments and connect to the same database populated by `load_geonames.py`.
They query the `postalcodes` and `geoname` tables and return the nearest
entries to a given coordinate pair.

### Arguments

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--lat` | float | yes | Latitude in decimal degrees (−90 … 90) |
| `--lon` | float | yes | Longitude in decimal degrees (−180 … 180) |
| `--results` | int | no (default: 3) | Number of nearest results to return |
| `--country` | string | no | Restrict results to an [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) country code (e.g. `MX`, `FR`, `DE`) |
| `--config` | path | no | Path to the YAML config file used by `load_geonames.py` |
| `--url` | string | no | SQLAlchemy / GORM connection URL — overrides `--config` |

Accepted URL formats for `--url`:

```
postgresql+psycopg2://user:pass@host:5432/db   # Python (SQLAlchemy)
postgres://user:pass@host:5432/db              # Go (GORM) / Python
mysql://user:pass@host:3306/db
sqlite:///path/to/file.db
```

---

### Python

```bash
cd examples/python

# Nearest places to Mexico City (uses config/config.yaml by default)
python reverse_geocode.py --lat 19.4326 --lon -99.1332

# Return 5 results near Paris
python reverse_geocode.py --lat 48.8566 --lon 2.3522 --results 5

# Restrict to French territory
python reverse_geocode.py --lat 48.8566 --lon 2.3522 --country FR

# Explicit connection URL (overrides config file)
python reverse_geocode.py --lat 51.5074 --lon -0.1278 \
    --url "postgresql+psycopg2://user:pass@localhost/geonames"

# MySQL / MariaDB
python reverse_geocode.py --lat 19.4326 --lon -99.1332 \
    --url "mysql+pymysql://user:pass@localhost/geonames"

# SQLite
python reverse_geocode.py --lat 19.4326 --lon -99.1332 \
    --url "sqlite:///path/to/geonames.db"

# Custom config file path
python reverse_geocode.py --lat 19.4326 --lon -99.1332 \
    --config /etc/geonames/config.yaml

# Combined: 10 results, restricted to Germany, explicit URL
python reverse_geocode.py --lat 52.5200 --lon 13.4050 \
    --results 10 --country DE \
    --url "postgres://user:pass@localhost/geonames"
```

---

### Go

First-time setup (resolves and downloads dependencies):

```bash
cd examples/go
go mod tidy
```

Run directly with `go run`:

```bash
# Nearest places to Mexico City
go run . --lat 19.4326 --lon -99.1332

# Return 5 results near Paris
go run . --lat 48.8566 --lon 2.3522 --results 5

# Restrict to French territory
go run . --lat 48.8566 --lon 2.3522 --country FR

# Explicit PostgreSQL connection URL
go run . --lat 51.5074 --lon -0.1278 \
    --url "postgres://user:pass@localhost/geonames"

# MySQL / MariaDB
go run . --lat 19.4326 --lon -99.1332 \
    --url "mysql://user:pass@localhost/geonames"

# SQLite
go run . --lat 19.4326 --lon -99.1332 \
    --url "sqlite:///path/to/geonames.db"

# Custom config file path
go run . --lat 19.4326 --lon -99.1332 \
    --config /etc/geonames/config.yaml

# Combined: 10 results, restricted to Germany, explicit URL
go run . --lat 52.5200 --lon 13.4050 \
    --results 10 --country DE \
    --url "postgres://user:pass@localhost/geonames"
```

Build a standalone binary:

```bash
go build -o reverse_geocode .
./reverse_geocode --lat 19.4326 --lon -99.1332
./reverse_geocode --lat 48.8566 --lon 2.3522 --country FR --results 5
```

---

## Notes

GeoNames data is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

---

## License

Copyright (C) 2026 Rodolfo González González.

Licensed under [GPL 3.0](https://www.gnu.org/licenses/gpl-3.0.html). Read the [LICENSE](LICENSE) file for more information.


