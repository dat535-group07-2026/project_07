"""Build the Gold tables from published Silver departures_enriched, flights and weather.

Three tables, each with its own pointer:
  - daily_airport:   one row per departure airport and day (flights, delays, daily weather);
  - airport_hour:    one row per departure airport and hour (flights, delays, that hour's weather);
  - departure_chain: linked departures by how late the inbound plane was and the weather at both ends.

By default Gold uses every published month of departures_enriched; --start/--end
limit the period. Weather is read for every published year.

Example (dev, June-October 2025):
    python -m project07.gold.run --env dev --dev-user alkval --start 2025-06 --end 2025-10
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Sequence

from project07 import config as cfg_mod
from project07 import spark as spark_mod
from project07.gold import chain, daily, hourly
from project07.silver import common
from project07.silver.enrichment import AIRPORTS

LOG = logging.getLogger(__name__)
TABLES = ("daily_airport", "airport_hour", "departure_chain")
# Gold fails if a month has fewer flights matched to weather than this. The lowest
# real months are about 92% (NOAA gaps on 30-31 Aug and 1 Sep 2025), so 80% only
# catches real errors, such as a weather year that was not published.
MIN_WEATHER_COVERAGE = 0.80


def select_months(published: Sequence[str], start: str | None = None, end: str | None = None) -> list[str]:
    """Published months between start and end (both optional, inclusive)."""
    months = [m for m in sorted(published) if (start is None or m >= start) and (end is None or m <= end)]
    if not months:
        raise ValueError(f"No published months between {start} and {end}")
    return months


def rule_versions(silver_root, table: str, months: Sequence[str]) -> dict[str, str]:
    """Rule version of each published month of a table, read from its _READY.json."""
    versions = {}
    for month in months:
        pointer = _pointer(silver_root, table, month)
        manifest = json.loads((silver_root / table / pointer["ready_manifest"]).read_text(encoding="utf-8"))
        versions[month] = manifest["rule_version"]
    return versions


def flight_rule_versions(silver_root, months: Sequence[str]) -> dict[str, str]:
    return rule_versions(silver_root, "flights", months)


def weather_coverage_per_month(enriched) -> dict[str, float]:
    """Share of flights per month that matched an hourly weather report."""
    rows = enriched.selectExpr(
        "date_format(flight_date, 'yyyy-MM') AS month",
        "CASE WHEN observation_utc IS NULL THEN 0 ELSE 1 END AS matched",
    ).groupBy("month").agg({"matched": "avg"}).collect()
    return {row["month"]: round(row["avg(matched)"], 4) for row in sorted(rows)}


def _pointer(silver_root, table, key=None) -> dict:
    """The current pointer of an input table, recorded in the Gold manifest."""
    return json.loads(common.current_pointer(silver_root, table, key).read_text(encoding="utf-8"))


def _rows_per_month(frame) -> dict[str, int]:
    rows = frame.selectExpr("date_format(flight_date, 'yyyy-MM') AS month").groupBy("month").count()
    return {row["month"]: row["count"] for row in rows.orderBy("month").collect()}


def _read_months(spark, silver_root, table: str, months: Sequence[str], inputs: dict):
    frame = None
    for month in months:
        part = common.read_ready_table(spark, silver_root, table, pointer_key=month)
        frame = part if frame is None else frame.unionByName(part)
        inputs[f"{table} {month}"] = _pointer(silver_root, table, month)
    return frame


def _total(frame, column: str) -> int:
    from pyspark.sql import functions as F

    return frame.agg(F.sum(column)).first()[0] or 0


def run_gold(
    cfg: cfg_mod.PipelineConfig,
    flights_cfg: cfg_mod.PipelineConfig,
    weather_cfg: cfg_mod.PipelineConfig,
    *,
    start: str | None = None,
    end: str | None = None,
    allow_dirty_git: bool = False,
) -> dict:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    spark_mod.check_spark_env()
    commit = common.clean_git_commit(allow_dirty=allow_dirty_git)
    started = time.monotonic()
    run_id = common.new_run_id()
    gold_root = cfg.gold_root
    silver_root = flights_cfg.silver_root
    spark = spark_mod.get_spark(app_name="project07-gold", env=cfg.env)
    departures = None
    try:
        # 1. Read the published inputs through their pointers. Each table's months must
        #    come from one rule version, or their columns differ.
        months = select_months(common.published_keys(silver_root, "departures_enriched"), start, end)
        for table in ("flights", "departures_enriched"):
            versions = rule_versions(silver_root, table, months)
            if len(set(versions.values())) > 1:
                raise ValueError(f"{table} months were built with different rule versions, re-run Silver: {versions}")
        inputs: dict = {}
        flights = _read_months(spark, silver_root, "flights", months, inputs)
        departures = _read_months(spark, silver_root, "departures_enriched", months, inputs) \
            .persist(StorageLevel.DISK_ONLY)
        weather_hourly, inputs["weather_hourly"] = common.read_ready_tables(
            spark, weather_cfg.silver_root, "weather_hourly")

        # 2. Build the three tables (daily weather only for the departure airports).
        tables = {
            "daily_airport": daily.daily_airport(
                departures, weather_hourly.filter(F.col("airport_code").isin(*AIRPORTS))),
            "airport_hour": hourly.airport_hour(departures),
            "departure_chain": chain.departure_chain(departures),
        }
        rows = {name: frame.count() for name, frame in tables.items()}
        coverage = weather_coverage_per_month(departures)

        # 3. Reconcile before publishing: no flight may be lost or counted twice.
        silver_per_month = _rows_per_month(flights.filter(F.col("origin").isin(*AIRPORTS)))
        departures_per_month = _rows_per_month(departures)
        departure_rows = sum(departures_per_month.values())
        linked = departures.filter("link_status = 'linked'").count()
        counts = {
            "silver_jfk_lga_flights": silver_per_month,
            "departures_enriched": departures_per_month,
            "linked_departures": linked,
            "rows": rows,
            "daily_airport_flights": _total(tables["daily_airport"], "flights"),
            "airport_hour_flights": _total(tables["airport_hour"], "flights"),
            "departure_chain_departures": _total(tables["departure_chain"], "departures"),
            "flights_with_weather": _total(tables["daily_airport"], "flights_with_weather"),
            "days_without_weather": tables["daily_airport"].filter(F.col("weather_hours").isNull()).count(),
            "weather_coverage_per_month": coverage,
        }
        checks = {
            "departures_match_silver_per_month": departures_per_month == silver_per_month,
            "daily_airport_flights_match": counts["daily_airport_flights"] == departure_rows,
            "airport_hour_flights_match": counts["airport_hour_flights"] == departure_rows,
            "departure_chain_matches_linked": counts["departure_chain_departures"] == linked,
            "weather_coverage_every_month": all(v >= MIN_WEATHER_COVERAGE for v in coverage.values()),
        }
        if not all(checks.values()):
            raise ValueError(f"Gold reconciliation failed, nothing published: {checks} {counts}")

        # 4. Write and read back every table, then publish the pointers last.
        paths = {name: gold_root / name / "runs" / run_id for name in TABLES}
        for name, frame in tables.items():
            checks[f"{name}_readback_count"] = common.write_parquet_and_verify(
                spark, frame, paths[name], rows[name])
        if not all(checks.values()):
            raise ValueError(f"Gold read-back failed, nothing published: {checks}")
        report = {
            "run_id": run_id,
            "git_commit": commit,
            "env": cfg.env,
            "dev_user": cfg.dev_user,
            "months": list(months),
            "inputs": inputs,
            "counts": counts,
            "checks": checks,
            "spark_version": spark.version,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "outputs": {name: common.output_summary(path, gold_root) for name, path in paths.items()},
        }
        for name in TABLES:
            common.publish_ready(gold_root, name, paths[name] / "_READY.json", report)
        print(json.dumps(report, indent=2))
        return report
    finally:
        try:
            if departures is not None:
                departures.unpersist()
        finally:
            spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    cfg_mod.add_config_args(parser)
    parser.add_argument("--flights-user", help="Dev only: whose Silver flights and departures to read (default --dev-user).")
    parser.add_argument("--weather-user", help="Dev only: whose Silver weather to read (default --dev-user).")
    parser.add_argument("--start", help="First month, YYYY-MM (default: first published).")
    parser.add_argument("--end", help="Last month, YYYY-MM (default: last published).")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        cfg = cfg_mod.resolve_config(env=args.env, data_root=args.data_root, dev_user=args.dev_user)
        if args.env == "prod" and (args.flights_user or args.weather_user):
            raise ValueError("--flights-user and --weather-user are only for --env dev")

        def inputs_from(user):
            if args.env == "prod":
                return cfg
            return cfg_mod.resolve_config(env="dev", data_root=args.data_root, dev_user=user or args.dev_user)

        run_gold(cfg, inputs_from(args.flights_user), inputs_from(args.weather_user),
                 start=args.start, end=args.end)
    except Exception:
        LOG.exception("Gold run failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
