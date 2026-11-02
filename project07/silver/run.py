"""Command-line entry point for Silver cleaning runs (weather and flights)."""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from typing import Sequence

from project07 import config as cfg_mod
from project07 import spark as spark_mod
from project07.silver import common, flights, weather

LOG = logging.getLogger(__name__)


def _validate_month(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-(?:0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError("Month must be formatted as YYYY-MM")
    return value


def _validate_year(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}", value):
        raise argparse.ArgumentTypeError("Year must be formatted as YYYY")
    return value


def run_weather(
    cfg: cfg_mod.PipelineConfig,
    *,
    year: str = "2025",
    stations: Sequence[str] = tuple(weather.STATION_AIRPORTS.keys()),
    allow_dirty_git: bool = False,
) -> dict:
    """Execute the Silver weather cleaning and hourly aggregation pipeline."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    spark_mod.check_spark_env()
    silver_root = cfg.silver_root
    bronze_root = cfg.bronze_root
    commit = common.clean_git_commit(allow_dirty=allow_dirty_git)
    started = time.monotonic()
    run_id = common.new_run_id()

    asset_ids = [f"noaa:{station}:{year}" for station in stations]
    assets = common.select_inputs(bronze_root, asset_ids)
    spark = spark_mod.get_spark(app_name=f"project07-weather-{year}", env=cfg.env)

    classified = None
    paths = {
        table: silver_root / table / "runs" / run_id
        for table in ("weather_observations", "weather_quarantine", "weather_hourly", "audit")
    }
    try:
        raw = weather.read_weather(spark, assets, bronze_root, run_id)
        observed_raw = raw.count()
        tables = weather.clean_weather(raw)
        classified = tables.classified.persist(StorageLevel.DISK_ONLY)

        counts = tables.audit.first().asDict()
        checks = common.check_accounting(counts, observed_raw)

        hourly_df = tables.hourly
        hourly_count = hourly_df.count()
        counts["hourly_rows"] = hourly_count

        # Verify hourly key uniqueness: at most one row per (station_id, weather_hour_utc).
        distinct_hourly_keys = hourly_df.select("station_id", "weather_hour_utc").distinct().count()
        checks["hourly_keys_unique"] = hourly_count == distinct_hourly_keys

        source_asset_summary = ",".join(asset_ids)
        audit_df = common.build_audit_df(
            spark, run_id, weather.RULE_VERSION, source_asset_summary, counts
        )

        expected = {
            "weather_observations": counts["retained"],
            "weather_quarantine": counts["quarantined"],
            "weather_hourly": hourly_count,
            "audit": len(counts),
        }
        outputs = {
            "weather_observations": (tables.retained, ("year", "month")),
            "weather_quarantine": (tables.quarantined, ()),
            "weather_hourly": (hourly_df, ("year", "month")),
            "audit": (audit_df, ()),
        }

        for name, (frame, part_cols) in outputs.items():
            checks[f"{name}_readback_count"] = common.write_parquet_and_verify(
                spark,
                frame,
                paths[name],
                expected[name],
                partition_by=part_cols,
            )

        if not all(checks.values()):
            raise ValueError(f"Weather checks failed: {checks}")

        report = {
            "run_id": run_id,
            "git_commit": commit,
            "rule_version": weather.RULE_VERSION,
            "env": cfg.env,
            "dev_user": cfg.dev_user,
            "year": year,
            "stations": list(stations),
            "inputs": common.input_snapshot(assets, bronze_root),
            "counts": counts,
            "checks": checks,
            "spark_version": spark.version,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "outputs": {
                name: common.output_summary(path, silver_root) for name, path in paths.items()
            },
        }
        for table_name in ("weather_observations", "weather_quarantine", "weather_hourly"):
            manifest_path = paths[table_name] / "_READY.json"
            common.publish_ready(
                silver_root,
                table_name,
                manifest_path,
                report,
                extra_pointer_fields={"year": year, "stations": list(stations)},
            )
        print(json.dumps(report, indent=2))
        return report
    finally:
        try:
            if classified is not None:
                classified.unpersist()
        finally:
            spark.stop()


def run_flights_month(
    cfg: cfg_mod.PipelineConfig,
    *,
    month: str,
    allow_dirty_git: bool = False,
) -> dict:
    """Execute the Silver flight cleaning pipeline for one month using shared config and publication."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    spark_mod.check_spark_env()
    silver_root = cfg.silver_root
    bronze_root = cfg.bronze_root
    commit = common.clean_git_commit(allow_dirty=allow_dirty_git)
    started = time.monotonic()
    run_id = common.new_run_id()

    assets = common.select_inputs(bronze_root, [f"bts:{month}"])
    spark = spark_mod.get_spark(app_name=f"project07-flights-{month}", env=cfg.env)

    classified = None
    paths = {
        table: silver_root / table / "runs" / run_id
        for table in ("flights", "flights_quarantine", "audit")
    }
    try:
        raw = flights.read_flights(spark, assets, bronze_root, run_id)
        observed_raw = raw.count()
        tables = flights.clean_flights(raw)
        classified = tables.classified.persist(StorageLevel.DISK_ONLY)
        counts = tables.audit.first().asDict()
        checks = common.check_accounting(counts, observed_raw)
        checks["retained_period_matches_asset"] = not tables.retained.filter(
            F.date_format("flight_date", "yyyy-MM") != month
        ).take(1)

        audit_df = common.build_audit_df(
            spark, run_id, flights.RULE_VERSION, assets[0].asset_id, counts
        )
        expected = {
            "flights": counts["retained"],
            "flights_quarantine": counts["quarantined"],
            "audit": len(counts),
        }
        outputs = {
            "flights": (tables.retained, ("year", "month")),
            "flights_quarantine": (tables.quarantined, ()),
            "audit": (audit_df, ()),
        }
        for name, (frame, part_cols) in outputs.items():
            checks[f"{name}_readback_count"] = common.write_parquet_and_verify(
                spark,
                frame,
                paths[name],
                expected[name],
                partition_by=part_cols,
            )
        if not all(checks.values()):
            raise ValueError(f"Flight checks failed: {checks}")

        report = {
            "run_id": run_id,
            "git_commit": commit,
            "rule_version": flights.RULE_VERSION,
            "env": cfg.env,
            "dev_user": cfg.dev_user,
            "month": month,
            "inputs": common.input_snapshot(assets, bronze_root),
            "counts": counts,
            "checks": checks,
            "spark_version": spark.version,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "outputs": {
                name: common.output_summary(path, silver_root) for name, path in paths.items()
            },
        }
        common.publish_ready(
            silver_root,
            "flights",
            paths["flights"] / "_READY.json",
            report,
            pointer_key=month,
            extra_pointer_fields={"month": month},
        )
        print(json.dumps(report, indent=2))
        return report
    finally:
        try:
            if classified is not None:
                classified.unpersist()
        finally:
            spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="dataset", required=True)

    weather_parser = subparsers.add_parser("weather", help="Clean NOAA LCD v2 station files.")
    cfg_mod.add_config_args(weather_parser)
    weather_parser.add_argument(
        "--year",
        default="2025",
        type=_validate_year,
        help="Four-digit NOAA station year (default: 2025).",
    )
    weather_parser.add_argument(
        "--stations",
        nargs="+",
        default=list(weather.STATION_AIRPORTS.keys()),
        choices=list(weather.STATION_AIRPORTS.keys()),
        help="Station identifiers to process (default: both JFK and LGA).",
    )

    flights_parser = subparsers.add_parser("flights", help="Clean one BTS monthly flight file.")
    cfg_mod.add_config_args(flights_parser)
    flights_parser.add_argument(
        "--month",
        required=True,
        type=_validate_month,
        help="BTS flight month formatted as YYYY-MM.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        cfg = cfg_mod.resolve_config(
            env=args.env, data_root=args.data_root, dev_user=args.dev_user
        )
        if args.dataset == "weather":
            run_weather(cfg, year=args.year, stations=args.stations)
        elif args.dataset == "flights":
            run_flights_month(cfg, month=args.month)
    except Exception:
        LOG.exception("Silver %s run failed", args.dataset)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

