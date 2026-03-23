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

from upload.lib import GeonamesLoader, load_config, build_engine


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
    engine = build_engine(config)
    GeonamesLoader.create(engine).run(args, config)
# main

# -----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
# __main__
