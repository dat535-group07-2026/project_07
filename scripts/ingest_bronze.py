#!/usr/bin/env python3
"""Download and verify BTS/NOAA Bronze inputs without cleaning their records."""

from __future__ import annotations

import argparse
import copy
import csv
import fcntl
import hashlib
import io
import json
import logging
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


LOG = logging.getLogger("bronze")
STATIONS = {"JFK": "USW00094789", "LGA": "USW00014732"}
BTS_BASE = "https://transtats.bts.gov/PREZIP"
NOAA_BASE = "https://www.ncei.noaa.gov/oa/local-climatological-data/v2/access"
REQUIRED = {
    "bts": {"FlightDate", "Origin", "Dest", "CRSDepTime", "DepTime", "DepDelay", "Cancelled", "Diverted"},
    "noaa": {"STATION", "DATE", "REPORT_TYPE", "HourlyDryBulbTemperature", "HourlyPrecipitation", "HourlyWindSpeed", "HourlyVisibility"},
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def month(value):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError("Use YYYY-MM, for example 2025-06")
    if int(value[:4]) < 1987:
        raise argparse.ArgumentTypeError("BTS reporting data starts in 1987")
    return value


def next_month(value):
    year, number = map(int, value.split("-"))
    return f"{year + (number == 12):04d}-{number % 12 + 1:02d}"


def month_range(start, end):
    if start > end:
        raise ValueError("Start month must not follow end month")
    result = []
    while start <= end:
        result.append(start)
        start = next_month(start)
    return result


def previous_month():
    now = datetime.now(timezone.utc)
    return f"{now.year - (now.month == 1):04d}-{(now.month - 2) % 12 + 1:02d}"


def flight_source(period, base=BTS_BASE):
    year, number = map(int, period.split("-"))
    name = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{number}.zip"
    return {"id": f"bts:{period}", "kind": "bts", "period": period, "name": name, "url": f"{base.rstrip('/')}/{name}"}


def weather_source(year, station):
    name = f"LCD_{station}_{year}.csv"
    return {"id": f"noaa:{station}:{year}", "kind": "noaa", "period": str(year), "station": station,
            "name": name, "url": f"{NOAA_BASE}/{year}/{name}"}


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def safe_path(root, relative):
    value = Path(relative)
    if value.is_absolute() or ".." in value.parts:
        raise ValueError(f"Unsafe manifest path: {relative}")
    result = (root / value).resolve()
    if not result.is_relative_to(root.resolve()):
        raise ValueError(f"Manifest path escapes Bronze: {relative}")
    return result


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, indent=2)
        stream.write("\n")
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def add_version(catalog, asset_id, version):
    versions = catalog["assets"].setdefault(asset_id, [])
    for existing in versions:
        if existing["sha256"] == version["sha256"]:
            return existing
    versions.append(version)
    return version


def load_catalog(bronze):
    path = bronze / "catalog.json"
    if path.exists():
        catalog = json.loads(path.read_text(encoding="utf-8"))
        if catalog.get("schema_version") != 1:
            raise ValueError("Unsupported Bronze catalog version")
        return catalog
    catalog = {"schema_version": 1, "assets": {}}
    # Import the original verified download manifest without moving or copying data.
    for legacy_path in sorted(bronze.glob("*/manifest.json")):
        legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
        if legacy.get("status") != "complete":
            continue
        for item in legacy.get("files", []):
            name = Path(item["path"]).name
            flight = re.fullmatch(r"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_(\d{4})_(\d{1,2})\.zip", name)
            weather = re.fullmatch(r"LCD_(USW\d{8})_(\d{4})\.csv", name)
            if flight:
                source = flight_source(f"{int(flight[1]):04d}-{int(flight[2]):02d}")
            elif weather:
                source = weather_source(int(weather[2]), weather[1])
            else:
                continue
            version = {**source, "path": str((legacy_path.parent / item["path"]).relative_to(bronze)),
                       "size_bytes": item["size_bytes"], "sha256": item["sha256"],
                       "retrieved_at_utc": item.get("retrieved_at_utc"),
                       "http_etag": item.get("http_etag"), "http_last_modified": item.get("http_last_modified"),
                       "transport": item.get("transport"),
                       "columns": item.get("columns"), "imported_manifest": str(legacy_path.relative_to(bronze)),
                       "csvs": []}
            for extracted in item.get("extracted_csvs", []):
                csv_record = {**extracted, "path": str((legacy_path.parent / extracted["path"]).relative_to(bronze))}
                if extracted.get("source_archive"):
                    csv_record["source_archive"] = str((legacy_path.parent / extracted["source_archive"]).relative_to(bronze))
                version["csvs"].append(csv_record)
            add_version(catalog, source["id"], version)
    return catalog


def header(stream, kind, expected=None):
    columns = next(csv.reader(stream), None)
    names = [name for name in (columns or []) if name]
    if not columns or len(names) != len(set(names)) or not REQUIRED[kind].issubset(names):
        raise ValueError(f"Unexpected {kind} CSV header: {columns}")
    if expected is not None and columns != expected:
        raise ValueError(f"{kind} CSV schema changed. Review the new header before ingestion.")
    return columns


def validate_raw(path, kind, expected=None):
    if kind == "noaa":
        with path.open(encoding="utf-8-sig", newline="") as stream:
            return header(stream, kind, expected)
    with zipfile.ZipFile(path) as archive:
        members = [item for item in archive.infolist() if not item.is_dir() and item.filename.lower().endswith(".csv")]
        if not members:
            raise ValueError("BTS ZIP contains no CSV files")
        if any(Path(item.filename).is_absolute() or ".." in Path(item.filename).parts for item in members):
            raise ValueError("Unsafe CSV member path in BTS ZIP")
        names = [Path(item.filename).name for item in members]
        if len(names) != len(set(names)):
            raise ValueError("BTS archive contains colliding CSV filenames")
        if archive.testzip() is not None:
            raise ValueError("BTS ZIP failed its CRC check")
        columns = expected
        for item in members:
            with archive.open(item) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
                columns = header(stream, kind, columns)
        return columns


class HTTPClient:
    def __init__(self, timeout=30, attempts=3):
        self.timeout, self.attempts = timeout, attempts

    def _request(self, url, method="GET"):
        return urllib.request.urlopen(urllib.request.Request(url, method=method, headers={
            "User-Agent": "DAT535-Group07-Bronze/1.0"}), timeout=self.timeout)

    def available(self, url):
        for attempt in range(self.attempts):
            try:
                with self._request(url, "HEAD"):
                    return True
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    return False
                if error.code < 500 and error.code != 429:
                    raise
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt + 1 == self.attempts:
                    raise
            if attempt + 1 == self.attempts:
                raise RuntimeError(f"Availability check failed: {url}")
            time.sleep(attempt + 1)

    def download(self, url, target):
        for attempt in range(self.attempts):
            try:
                with self._request(url) as response, target.open("wb") as output:
                    metadata = {"resolved_url": response.geturl(), "http_etag": response.headers.get("ETag"),
                                "http_last_modified": response.headers.get("Last-Modified")}
                    expected = response.headers.get("Content-Length")
                    shutil.copyfileobj(response, output, length=1024 * 1024)
                if not target.stat().st_size or (expected and target.stat().st_size != int(expected)):
                    raise ValueError("Empty or incomplete download")
                return {**metadata, "retrieved_at_utc": utc_now()}
            except Exception as error:
                target.unlink(missing_ok=True)
                retryable = not isinstance(error, urllib.error.HTTPError) or error.code >= 500 or error.code == 429
                if not retryable or attempt + 1 == self.attempts:
                    raise
                LOG.warning("Download failed (%s), retrying %s", error, url)
                time.sleep(attempt + 1)


@contextmanager
def ingestion_lock(bronze):
    bronze.mkdir(parents=True, exist_ok=True)
    with (bronze / ".ingest.lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another Bronze ingestion run is active") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


class Ingestor:
    def __init__(self, bronze, catalog, client, offline=False):
        self.bronze, self.catalog, self.client, self.offline = bronze, catalog, client, offline

    def verify(self, version):
        for record in [version, *version.get("csvs", [])]:
            path = safe_path(self.bronze, record["path"])
            if path.stat().st_size != record["size_bytes"] or sha256(path) != record["sha256"]:
                raise ValueError(f"Checksum/size mismatch: {path}")
        path = safe_path(self.bronze, version["path"])
        version["columns"] = validate_raw(path, version["kind"], version.get("columns"))

    def schema_for(self, kind):
        for versions in self.catalog["assets"].values():
            for version in reversed(versions):
                if version["kind"] == kind:
                    columns = version.get("columns")
                    if not columns and version.get("csvs"):
                        columns = version["csvs"][0].get("columns")
                    if columns:
                        return columns
        return None

    def extract(self, version):
        if version["kind"] != "bts" or version.get("csvs"):
            return
        archive_path = safe_path(self.bronze, version["path"])
        parent = self.bronze / "extracted" / "bts" / version["period"]
        parent.mkdir(parents=True, exist_ok=True)
        destination = parent / version["sha256"]
        with tempfile.TemporaryDirectory(prefix=".extract-", dir=parent) as temporary:
            csvs = []
            with zipfile.ZipFile(archive_path) as archive:
                for item in archive.infolist():
                    if item.is_dir() or not item.filename.lower().endswith(".csv"):
                        continue
                    path = Path(temporary) / Path(item.filename).name
                    with archive.open(item) as source, path.open("xb") as output:
                        shutil.copyfileobj(source, output, length=1024 * 1024)
                    csvs.append({"path": str((destination / path.name).relative_to(self.bronze)),
                                 "source_member": item.filename, "size_bytes": path.stat().st_size,
                                 "sha256": sha256(path), "columns": version["columns"]})
            if destination.exists():
                actual_files = {path.name for path in destination.iterdir()}
                if actual_files != {Path(item["path"]).name for item in csvs}:
                    raise ValueError(f"Existing extraction has unexpected files: {destination}")
                for item in csvs:
                    path = safe_path(self.bronze, item["path"])
                    if not path.is_file() or sha256(path) != item["sha256"]:
                        raise ValueError(f"Existing extraction is corrupt: {path}")
            else:
                Path(temporary).rename(destination)
        version["csvs"] = csvs

    def obtain(self, source, refresh=False):
        versions = self.catalog["assets"].get(source["id"], [])
        if versions and not refresh:
            version = versions[-1]
            self.verify(version)
            self.extract(version)
            LOG.info("Reused %s", source["id"])
            return version, False
        if self.offline:
            raise ValueError(f"No verified cached source available offline: {source['id']}")
        parent = self.bronze / "sources" / source["kind"] / source["period"]
        if source.get("station"):
            parent = parent / source["station"]
        parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".download-", dir=parent) as temporary:
            staged = Path(temporary) / source["name"]
            metadata = self.client.download(source["url"], staged)
            columns = validate_raw(staged, source["kind"], self.schema_for(source["kind"]))
            digest = sha256(staged)
            for version in versions:
                if version["sha256"] == digest:
                    self.verify(version)
                    self.extract(version)
                    versions.remove(version)
                    versions.append(version)
                    LOG.info("Unchanged %s", source["id"])
                    return version, False
            destination = parent / (digest + Path(source["name"]).suffix)
            if destination.exists():
                if sha256(destination) != digest:
                    raise ValueError(f"Existing snapshot is corrupt: {destination}")
            else:
                staged.rename(destination)
            version = {**source, **metadata, "path": str(destination.relative_to(self.bronze)),
                       "size_bytes": destination.stat().st_size, "sha256": digest, "columns": columns, "csvs": []}
        self.extract(version)
        add_version(self.catalog, source["id"], version)
        LOG.info("Downloaded %s", source["id"])
        return version, True


def run(args, client=None):
    bronze = args.data_root.resolve() / "bronze"
    catalog = load_catalog(bronze)
    if args.mode == "manual":
        periods = month_range(args.start, args.end)
    else:
        known = sorted(key.split(":", 1)[1] for key in catalog["assets"] if key.startswith("bts:"))
        pending = sorted(catalog.get("pending_months", []))
        start = args.since or (next_month(known[-1]) if known else pending[0] if pending else None)
        if start is None:
            raise ValueError("Use --since YYYY-MM for the first new-release run")
        end = args.through or previous_month()
        periods = month_range(start, end) if start <= end else []
        if args.since is None:
            periods = sorted(set(periods) | {value for value in catalog.get("pending_months", []) if value <= end})
    if args.offline and args.refresh:
        raise ValueError("--offline and --refresh cannot be combined")
    if args.mode == "new" and args.offline:
        raise ValueError("New-release discovery needs network access. Use --dry-run to preview without requests.")
    if args.dry_run:
        plan = {"mode": args.mode, "bronze": str(bronze), "candidate_months": periods,
                "note": "Plan only. Source availability was not checked and no files were written."}
        print(json.dumps(plan, indent=2))
        return plan
    client = client or HTTPClient(args.timeout, args.attempts)
    with ingestion_lock(bronze):
        # Reload after locking so a completed concurrent run cannot be overwritten.
        catalog = copy.deepcopy(load_catalog(bronze))
        worker = Ingestor(bronze, catalog, client, args.offline)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        report_path = bronze / "runs" / (run_id + ".json")
        report = {"schema_version": 1, "run_id": run_id, "mode": args.mode, "started_at_utc": utc_now(),
                  "requested_months": periods, "unavailable_months": [], "sources": [], "status": "running"}
        write_json(report_path, report)
        try:
            years = set()
            changed = False
            for period in periods:
                source = flight_source(period, args.bts_base_url)
                if args.mode == "new" and source["id"] not in catalog["assets"] and not client.available(source["url"]):
                    LOG.info("Not released: %s", period)
                    report["unavailable_months"].append(period)
                    catalog["pending_months"] = sorted(set(catalog.get("pending_months", [])) | {period})
                    continue
                version, downloaded = worker.obtain(source, args.refresh)
                report["sources"].append(copy.deepcopy(version))
                catalog["pending_months"] = [value for value in catalog.get("pending_months", []) if value != period]
                changed |= downloaded
                years.add(int(period[:4]))
            for year in sorted(years):
                for station in STATIONS.values():
                    version, downloaded = worker.obtain(weather_source(year, station),
                                                        args.refresh or (args.mode == "new" and changed))
                    report["sources"].append(copy.deepcopy(version))
                    changed |= downloaded
            write_json(bronze / "catalog.json", catalog)
            report["status"] = "complete"
            report["new_or_changed_sources"] = changed
            report["message"] = "New or changed inputs ready" if changed else "No new or changed inputs"
            LOG.info(report["message"])
        except Exception as error:
            report["status"] = "failed"
            report["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            report["finished_at_utc"] = utc_now()
            write_json(report_path, report)
            LOG.info("Run manifest: %s", report_path)
        return report


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    modes = result.add_subparsers(dest="mode", required=True)
    for name in ("manual", "new"):
        mode_parser = modes.add_parser(name)
        mode_parser.add_argument("--data-root", type=Path, default=Path(__file__).resolve().parents[2] / "project-data")
        mode_parser.add_argument("--offline", action="store_true", help="Verify and reuse cached files only")
        mode_parser.add_argument("--refresh", action="store_true", help="Fetch sources again and retain changed versions")
        mode_parser.add_argument("--dry-run", action="store_true", help="Preview months without downloads, network requests or writes")
        mode_parser.add_argument("--timeout", type=int, default=30)
        mode_parser.add_argument("--attempts", type=int, default=3)
        mode_parser.add_argument("--bts-base-url", default=BTS_BASE, help="BTS ZIP endpoint (override for controlled tests)")
        if name == "manual":
            mode_parser.add_argument("--start", type=month, required=True)
            mode_parser.add_argument("--end", type=month, required=True)
        else:
            mode_parser.add_argument("--since", type=month, help="First month to check, otherwise month after latest ingested flight month")
            mode_parser.add_argument("--through", type=month, help="Last month to check, otherwise previous calendar month")
    return result


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parser().parse_args(argv)
    if args.timeout <= 0 or args.attempts <= 0:
        parser().error("--timeout and --attempts must be positive")
    try:
        run(args)
    except Exception as error:
        LOG.error("Bronze ingestion failed: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
