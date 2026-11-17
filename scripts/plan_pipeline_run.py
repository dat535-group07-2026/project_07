"""Decide which Silver and Gold runs are needed, by comparing what is published with its inputs.

Prints the keys to run, separated by spaces, so the workflow can loop over them.
Nothing is printed when everything is up to date, so running the pipeline twice
in a row does nothing the second time.

  flights     months whose Bronze BTS file is newer than the published Silver run,
              or whose Silver run used older flight rules, or that are not published
  weather     years whose Bronze NOAA files (or set of stations) changed, or whose
              Silver run used older weather rules, or that are not published
  departures  months whose inputs were republished since: the flights of that month
              and the month before, or a weather year; or older departure rules
  gold        prints "yes" if any Gold input was republished since the last Gold run

Only months from --since (default 2025-01) are considered. Call the layers in this
order, after the earlier layer has run, because each looks at what is published now.

Example:
    python -m scripts.plan_pipeline_run flights --env prod --data-root /home/ubuntu/project-data
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from project07.silver import chain, common, flights, weather

LAYERS = ("flights", "weather", "departures", "gold")
GOLD_TABLE = "daily_airport"
GOLD_TABLES = ("daily_airport", "airport_hour", "departure_chain")


def bronze_versions(bronze_root: Path) -> dict:
    """Latest version hash of every Bronze asset: {"bts": {month: sha}, "noaa": {year: {station: sha}}}."""
    catalog = json.loads((Path(bronze_root) / "catalog.json").read_text(encoding="utf-8"))
    result: dict = {"bts": {}, "noaa": {}}
    for asset_id, versions in catalog.get("assets", {}).items():
        latest = versions[-1]
        if latest["kind"] == "bts":
            result["bts"][latest["period"]] = latest["sha256"]
        else:
            result["noaa"].setdefault(latest["period"], {})[latest["station"]] = latest["sha256"]
    return result


def _run_id(root: Path, table: str, key: str | None = None) -> str | None:
    return (common.read_pointer(root, table, key) or {}).get("run_id")


def _input_shas(report: dict) -> dict[str, str]:
    return {item["asset_id"]: item["version_sha256"] for item in report.get("inputs", [])}


def stale_flight_months(bronze: dict, silver_root: Path, since: str) -> list[str]:
    stale = []
    for month, sha in sorted(bronze["bts"].items()):
        if month < since:
            continue
        report = common.read_manifest(silver_root, "flights", month)
        if (report is None or report.get("rule_version") != flights.RULE_VERSION
                or _input_shas(report).get(f"bts:{month}") != sha):
            stale.append(month)
    return stale


def stale_weather_years(bronze: dict, silver_root: Path, since: str) -> list[str]:
    stations = set(weather.STATION_AIRPORTS)
    stale = []
    for year, files in sorted(bronze["noaa"].items()):
        if year < since[:4]:
            continue
        expected = {f"noaa:{station}:{year}": sha for station, sha in files.items() if station in stations}
        report = common.read_manifest(silver_root, "weather_hourly", year)
        if (report is None or report.get("rule_version") != weather.RULE_VERSION
                or set(report.get("stations", [])) != stations or _input_shas(report) != expected):
            stale.append(year)
    return stale


def _weather_run_ids(silver_root: Path) -> dict[str, str]:
    return {year: _run_id(silver_root, "weather_hourly", year)
            for year in common.published_keys(silver_root, "weather_hourly")}


def stale_departure_months(silver_root: Path, since: str) -> list[str]:
    published = common.published_keys(silver_root, "flights")
    weather_now = _weather_run_ids(silver_root)
    stale = []
    for month in published:
        if month < since:
            continue
        report = common.read_manifest(silver_root, "departures_enriched", month)
        context = [m for m in (common.previous_month(month), month) if m in published]
        flights_now = {m: _run_id(silver_root, "flights", m) for m in context}
        if report is None or report.get("rule_version") != chain.RULE_VERSION:
            stale.append(month)
            continue
        inputs = report.get("inputs", {})
        weather_then = {y: p["run_id"] for y, p in inputs.get("weather_hourly", {}).items()}
        if inputs.get("flights") != flights_now or weather_then != weather_now:
            stale.append(month)
    return stale


def gold_needed(gold_root: Path, silver_root: Path, since: str) -> bool:
    report = common.read_manifest(gold_root, GOLD_TABLE)
    if report is None:
        return True
    # One Gold run publishes all three tables. If a run stopped half-way, they differ.
    if any(_run_id(gold_root, table) != report["run_id"] for table in GOLD_TABLES):
        return True
    months = [m for m in common.published_keys(silver_root, "departures_enriched") if m >= since]
    if report.get("months") != months:
        return True
    inputs = report.get("inputs", {})
    for month in months:
        for table in ("flights", "departures_enriched"):
            if inputs.get(f"{table} {month}", {}).get("run_id") != _run_id(silver_root, table, month):
                return True
    weather_then = {y: p["run_id"] for y, p in inputs.get("weather_hourly", {}).items()}
    return weather_then != _weather_run_ids(silver_root)


def plan(layer: str, bronze_root: Path, silver_root: Path, gold_root: Path, since: str) -> list[str]:
    if layer == "flights":
        return stale_flight_months(bronze_versions(bronze_root), silver_root, since)
    if layer == "weather":
        return stale_weather_years(bronze_versions(bronze_root), silver_root, since)
    if layer == "departures":
        return stale_departure_months(silver_root, since)
    return ["yes"] if gold_needed(gold_root, silver_root, since) else []


def main(argv: Sequence[str] | None = None) -> int:
    from project07 import config as cfg_mod

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("layer", choices=LAYERS)
    cfg_mod.add_config_args(parser)
    parser.add_argument("--since", default="2025-01", help="First month to keep up to date (YYYY-MM).")
    args = parser.parse_args(argv)
    cfg = cfg_mod.resolve_config(env=args.env, data_root=args.data_root, dev_user=args.dev_user)
    print(" ".join(plan(args.layer, cfg.bronze_root, cfg.silver_root, cfg.gold_root, args.since)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
