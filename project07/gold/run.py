"""Build the Gold daily_airport table from published Silver flights and weather.

By default Gold uses every published flight month; --start/--end limit the period.
Weather is read for every published year.

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
from project07.gold import daily
from project07.silver import common
from project07.silver.enrichment import AIRPORTS, enrich_flights

LOG = logging.getLogger(__name__)
TABLE = "daily_airport"
# Gold fails if a month has fewer flights matched to weather than this. The lowest
# real months are about 92% (NOAA gaps on 30-31 Aug and 1 Sep 2025), so 80% only
# catches real errors, such as a weather year that was not published.
MIN_WEATHER_COVERAGE = 0.80


def select_months(published: Sequence[str], start: str | None = None, end: str | None = None) -> list[str]:
    """Published flight months between start and end (both optional, inclusive)."""
    months = [m for m in sorted(published) if (start is None or m >= start) and (end is None or m <= end)]
    if not months:
        raise ValueError(f"No published flight months between {start} and {end}")
    return months


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
    run_path = gold_root / TABLE / "runs" / run_id
    spark = spark_mod.get_spark(app_name="project07-gold", env=cfg.env)
    enriched = None
    try:
        # 1. Read the published inputs through their pointers.
        months = select_months(common.published_keys(flights_cfg.silver_root, "flights"), start, end)
        inputs = {}
        flights = None
        for month in months:
            frame = common.read_ready_table(spark, flights_cfg.silver_root, "flights", pointer_key=month)
            flights = frame if flights is None else flights.unionByName(frame)
            inputs[f"flights {month}"] = _pointer(flights_cfg.silver_root, "flights", month)
        # Weather is published per year; read every published year.
        weather_hourly, inputs["weather_hourly"] = common.read_ready_tables(
            spark, weather_cfg.silver_root, "weather_hourly")

        # 2. Join, and keep the result on disk so it is not recomputed for each step.
        enriched = enrich_flights(flights, weather_hourly).persist(StorageLevel.DISK_ONLY)

        # 3. Build the daily table (weather only for the departure airports).
        table = daily.daily_airport(enriched, weather_hourly.filter(F.col("airport_code").isin(*AIRPORTS)))
        gold_rows = table.count()
        coverage = weather_coverage_per_month(enriched)

        # 4. Reconcile before publishing: no flight may be lost or counted twice.
        silver_per_month = _rows_per_month(flights.filter(F.col("origin").isin(*AIRPORTS)))
        enriched_per_month = _rows_per_month(enriched)
        enriched_rows = sum(enriched_per_month.values())
        gold_flights = table.agg(F.sum("flights")).first()[0]
        counts = {
            "silver_jfk_lga_flights": silver_per_month,
            "enriched_flights": enriched_per_month,
            "gold_flights": gold_flights,
            "gold_rows": gold_rows,
            "flights_with_weather": table.agg(F.sum("flights_with_weather")).first()[0],
            "days_without_weather": table.filter(F.col("weather_hours").isNull()).count(),
            "weather_coverage_per_month": coverage,
        }
        checks = {
            "enriched_matches_silver_per_month": enriched_per_month == silver_per_month,
            "gold_flights_match_enriched": gold_flights == enriched_rows,
            "weather_coverage_every_month": all(v >= MIN_WEATHER_COVERAGE for v in coverage.values()),
        }
        if not all(checks.values()):
            raise ValueError(f"Gold reconciliation failed, nothing published: {checks} {counts}")

        # 5. Write, read back, then publish the pointer last.
        checks["readback_count"] = common.write_parquet_and_verify(spark, table, run_path, gold_rows)
        if not checks["readback_count"]:
            raise ValueError(f"Gold read-back count differs from {gold_rows}, nothing published")
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
            "output": common.output_summary(run_path, gold_root),
        }
        common.publish_ready(gold_root, TABLE, run_path / "_READY.json", report)
        print(json.dumps(report, indent=2))
        return report
    finally:
        try:
            if enriched is not None:
                enriched.unpersist()
        finally:
            spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    cfg_mod.add_config_args(parser)
    parser.add_argument("--flights-user", help="Dev only: whose Silver flights to read (default --dev-user).")
    parser.add_argument("--weather-user", help="Dev only: whose Silver weather to read (default --dev-user).")
    parser.add_argument("--start", help="First flight month, YYYY-MM (default: first published).")
    parser.add_argument("--end", help="Last flight month, YYYY-MM (default: last published).")
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
