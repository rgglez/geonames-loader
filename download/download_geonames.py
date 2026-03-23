#!/usr/bin/env python3

"""
    download_geonames.py
    Downloads Geonames data files from geonames.org.

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

    Configuration is read from config/config.yaml.

    Usage:
        python download_geonames.py [--config CONFIG_FILE]
"""

import argparse
import zipfile
from pathlib import Path

import requests
import yaml
from tqdm import tqdm

# -----------------------------------------------------------------------------


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
# load_config

# -----------------------------------------------------------------------------


def download_file(url: str, dest_path: Path) -> bool:
    """Download a file with progress bar. Returns True if file was (re)downloaded."""
    response = requests.head(url, allow_redirects=True, timeout=30)
    if response.status_code != 200:
        print(f"  ERROR: Cannot reach {url} (HTTP {response.status_code})")
        return False

    remote_size = int(response.headers.get("Content-Length", 0))

    # Check if local file is already up to date
    if dest_path.exists() and remote_size and dest_path.stat().st_size == remote_size:
        print(f"  {dest_path.name}: already up to date, skipping.")
        return False

    print(f"  Downloading {dest_path.name} ...")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total = int(response.headers.get("Content-Length", 0))
    with open(dest_path, "wb") as f, tqdm(
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=f"    {dest_path.name}",
        leave=False,
    ) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))

    return True
# download_file


# -----------------------------------------------------------------------------


def unzip_file(zip_path: Path, dest_dir: Path) -> None:
    print(f"  Extracting {zip_path.name} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_dir)
# unzip_file


# -----------------------------------------------------------------------------


def strip_header(src: Path, dest: Path, lines_to_skip: int = 1) -> None:
    """Write dest with the first N lines removed from src."""
    with open(src, "r", encoding="utf-8", errors="replace") as fin, open(
        dest, "w", encoding="utf-8"
    ) as fout:
        for i, line in enumerate(fin):
            if i >= lines_to_skip:
                fout.write(line)
# strip_header


# -----------------------------------------------------------------------------


def strip_comments_and_tail(src: Path, dest: Path, tail_lines: int = 2) -> None:
    """Remove lines starting with '#' and strip the last N lines (countryInfo.txt)."""
    with open(src, "r", encoding="utf-8", errors="replace") as fin:
        lines = [line for line in fin if not line.startswith("#")]
    lines = lines[: len(lines) - tail_lines] if tail_lines else lines
    with open(dest, "w", encoding="utf-8") as fout:
        fout.writelines(lines)
# strip_comments_and_tail


# -----------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Geonames data files.")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML file (default: config/config.yaml)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    dl = config["download"]

    data_dir = Path(dl["data_dir"])
    data_dir.mkdir(parents=True, exist_ok=True)

    postal_dir = data_dir / dl["postal_subdir"]
    postal_dir.mkdir(parents=True, exist_ok=True)

    base_url = dl["url_data"].rstrip("/")
    postal_url = dl["url_postal"].rstrip("/")
    files = dl["files"]

    print("=" * 60)
    print("Geonames data downloader")
    print(f"  Data directory : {data_dir.resolve()}")
    print(f"  Source URL     : {base_url}")
    print("=" * 60)

    # ------------------------------------------------------------------ #
    # Main data files
    # ------------------------------------------------------------------ #
    print("\nDownloading main data files:")
    for filename in files:
        url = f"{base_url}/{filename}"
        dest = data_dir / filename
        changed = download_file(url, dest)

        if filename.endswith(".zip") and (changed or not (data_dir / filename.replace(".zip", ".txt")).exists()):
            unzip_file(dest, data_dir)

    # ------------------------------------------------------------------ #
    # Post-processing: strip headers / comments from selected files
    # ------------------------------------------------------------------ #
    print("\nPost-processing files:")

    iso_src = data_dir / "iso-languagecodes.txt"
    iso_tmp = data_dir / "iso-languagecodes.txt.tmp"
    if iso_src.exists():
        print(f"  Stripping header from {iso_src.name} ...")
        strip_header(iso_src, iso_tmp, lines_to_skip=1)

    tz_src = data_dir / "timeZones.txt"
    tz_tmp = data_dir / "timeZones.txt.tmp"
    if tz_src.exists():
        print(f"  Stripping header from {tz_src.name} ...")
        strip_header(tz_src, tz_tmp, lines_to_skip=1)

    ci_src = data_dir / "countryInfo.txt"
    ci_tmp = data_dir / "countryInfo.txt.tmp"
    if ci_src.exists():
        print(f"  Stripping comments from {ci_src.name} ...")
        strip_comments_and_tail(ci_src, ci_tmp, tail_lines=2)

    # ------------------------------------------------------------------ #
    # Postal codes
    # ------------------------------------------------------------------ #
    print("\nDownloading postal codes:")
    pc_zip_url = f"{postal_url}/allCountries.zip"
    pc_zip_dest = postal_dir / "allCountries.zip"
    changed = download_file(pc_zip_url, pc_zip_dest)

    pc_txt = postal_dir / "allCountries.txt"
    if changed or not pc_txt.exists():
        unzip_file(pc_zip_dest, postal_dir)

    print("\nDownload complete.")
    print(f"Data directory: {data_dir.resolve()}")
# main

# -----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
# __main__
