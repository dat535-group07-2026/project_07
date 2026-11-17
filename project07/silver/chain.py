"""Departures with their inbound flight and the weather at both ends.

For each JFK/LGA departure this module finds the inbound flight that brought the
same plane (same tail number) to that airport, and adds:
  - the local weather in the departure's scheduled UTC hour (enrichment.py),
  - the inbound flight: where it came from, when it was due, how late it was,
  - the weather at the inbound flight's origin in the hour it was due to leave.

The link rule, in plain words:
  - only departures that were not cancelled get a link: a cancelled departure did
    not use the plane, and its replacement flight should get the inbound flight;
  - same tail number, and the inbound flight lands at the airport the departure leaves from;
  - the inbound flight was not cancelled or diverted;
  - it was due to arrive before the departure's scheduled time, at most 12 hours earlier;
  - it arrived after the plane's previous departure from that airport (otherwise the
    plane had already left again, and the inbound flight belongs to that departure);
  - if several fit, the latest one; ties are broken by record_hash.

All times are compared in UTC. An inbound flight's scheduled arrival is its
scheduled departure in UTC (origin time zone from the reference table) plus its
scheduled elapsed minutes, so overnight and cross-time-zone flights need no
date guessing.
"""

from __future__ import annotations

from itertools import chain as _chain
from typing import TYPE_CHECKING

from project07.silver.enrichment import AIRPORTS, enrich_flights

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

RULE_VERSION = "departures-0.1"
LINK_WINDOW_HOURS = 12
ORIGIN_WEATHER_COLUMNS = (
    "observation_utc", "temperature_c", "precipitation_mm", "precipitation_trace",
    "wind_speed_mps", "wind_gust_mps", "visibility_km", "thunderstorm",
)
LINK_STATUSES = ("linked", "cancelled", "no_tail_number", "no_inbound_found")


def _time_zones(airports: dict):
    from pyspark.sql import functions as F

    pairs = [(a.code, a.time_zone) for a in airports.values()]
    return F.create_map(*[F.lit(v) for v in _chain.from_iterable(pairs)])


def with_utc_times(flights: DataFrame, airports: dict) -> DataFrame:
    """Add scheduled departure, scheduled arrival and actual arrival in UTC.

    The departure clock is in the origin's local time, so it is converted with the
    origin's time zone. Airports missing from the reference table get null times.
    """
    from pyspark.sql import functions as F

    origin_zone = _time_zones(airports)[F.col("origin")]
    return (
        flights.withColumn("origin_time_zone", origin_zone)
        .withColumn("scheduled_dep_utc", F.when(
            F.col("origin_time_zone").isNotNull(),
            F.to_utc_timestamp(F.col("scheduled_dep_local").cast("timestamp"), F.col("origin_time_zone"))))
        .withColumn("scheduled_arr_utc", F.expr("timestampadd(MINUTE, scheduled_elapsed_min, scheduled_dep_utc)"))
        # BTS arrival delay is measured against the scheduled arrival.
        .withColumn("actual_arr_utc", F.expr("timestampadd(MINUTE, arr_delay_min, scheduled_arr_utc)"))
    )


def inbound_candidates(context_flights: DataFrame, airports: dict) -> DataFrame:
    """Completed flights into JFK/LGA with a tail number, with UTC times."""
    from pyspark.sql import functions as F

    arrivals = context_flights.filter(
        F.col("dest").isin(*AIRPORTS) & ~F.col("cancelled") & ~F.col("diverted")
        & F.col("tail_number").isNotNull()
    )
    return with_utc_times(arrivals, airports)


def unknown_inbound_airports(candidates: DataFrame) -> list[str]:
    """Origins of inbound flights that are missing from the reference table."""
    rows = candidates.filter("origin_time_zone IS NULL").select("origin").distinct().collect()
    return sorted(row["origin"] for row in rows)


def previous_departure(context_flights: DataFrame) -> DataFrame:
    """For every JFK/LGA departure: the same plane's previous non-cancelled departure in UTC."""
    from pyspark.sql import Window, functions as F
    from project07.silver.enrichment import LOCAL_TIME_ZONE

    departures = context_flights.filter(F.col("origin").isin(*AIRPORTS)).withColumn(
        "_dep_utc", F.to_utc_timestamp(F.col("scheduled_dep_local").cast("timestamp"), LOCAL_TIME_ZONE))
    earlier = (Window.partitionBy("tail_number", "origin")
               .orderBy("_dep_utc", "record_hash").rowsBetween(Window.unboundedPreceding, -1))
    return departures.select(
        "record_hash",
        F.max(F.when(~F.col("cancelled"), F.col("_dep_utc"))).over(earlier).alias("previous_dep_utc"),
    )


def link_inbound(departures: DataFrame, candidates: DataFrame) -> DataFrame:
    """Add the inbound flight (inbound_* columns) and link_status to each departure."""
    from pyspark.sql import Window, functions as F

    inbound = candidates.select(
        F.col("record_hash").alias("inbound_record_hash"),
        F.col("tail_number").alias("_in_tail"),
        F.col("dest").alias("_in_dest"),
        F.col("origin").alias("inbound_origin"),
        F.col("flight_date").alias("inbound_flight_date"),
        F.col("reporting_airline").alias("inbound_reporting_airline"),
        F.col("flight_number").alias("inbound_flight_number"),
        F.col("scheduled_dep_utc").alias("inbound_scheduled_dep_utc"),
        F.col("scheduled_arr_utc").alias("inbound_scheduled_arr_utc"),
        F.col("actual_arr_utc").alias("inbound_actual_arr_utc"),
        F.col("dep_delay_min").alias("inbound_dep_delay_min"),
        F.col("arr_delay_min").alias("inbound_arr_delay_min"),
    ).filter(F.col("inbound_scheduled_arr_utc").isNotNull())

    d, i = departures.alias("d"), inbound.alias("i")
    condition = (
        ~F.col("d.cancelled")
        & (F.col("d.tail_number") == F.col("i._in_tail"))
        & (F.col("d.origin") == F.col("i._in_dest"))
        & (F.col("i.inbound_scheduled_arr_utc") <= F.col("d.scheduled_dep_utc"))
        & (F.col("i.inbound_scheduled_arr_utc")
           >= F.col("d.scheduled_dep_utc") - F.expr(f"INTERVAL {LINK_WINDOW_HOURS} HOURS"))
        & (F.col("d.previous_dep_utc").isNull()
           | (F.col("i.inbound_scheduled_arr_utc") > F.col("d.previous_dep_utc")))
    )
    latest = Window.partitionBy("d.record_hash").orderBy(
        F.col("i.inbound_scheduled_arr_utc").desc_nulls_last(), F.col("i.inbound_record_hash").asc())
    linked = (
        d.join(i, condition, "left")
        .withColumn("_rank", F.row_number().over(latest))
        .filter("_rank = 1")
        .drop("_rank", "_in_tail", "_in_dest")
    )
    return (
        linked.withColumn("link_status", F.when(F.col("cancelled"), "cancelled")
                          .when(F.col("tail_number").isNull(), "no_tail_number")
                          .when(F.col("inbound_record_hash").isNotNull(), "linked")
                          .otherwise("no_inbound_found"))
        .withColumn("scheduled_ground_min", F.when(
            F.col("inbound_record_hash").isNotNull(),
            ((F.unix_timestamp("scheduled_dep_utc") - F.unix_timestamp("inbound_scheduled_arr_utc")) / 60)
            .cast("int")))
    )


def add_origin_weather(departures: DataFrame, weather_hourly: DataFrame) -> DataFrame:
    """Weather at the inbound flight's origin in the hour it was due to leave (origin_* columns)."""
    from pyspark.sql import functions as F

    weather = weather_hourly.select(
        F.col("airport_code").alias("inbound_origin"),
        F.col("weather_hour_utc").alias("origin_weather_hour_utc"),
        *[F.col(c).alias(f"origin_{c}") for c in ORIGIN_WEATHER_COLUMNS],
    )
    stations = weather_hourly.select(F.col("airport_code").alias("inbound_origin")).distinct() \
        .withColumn("origin_has_station", F.lit(True))
    return (
        departures.withColumn("origin_weather_hour_utc", F.date_trunc("hour", "inbound_scheduled_dep_utc"))
        .join(weather, ["inbound_origin", "origin_weather_hour_utc"], "left")
        .join(F.broadcast(stations), "inbound_origin", "left")
        .withColumn("origin_has_station", F.coalesce("origin_has_station", F.lit(False)))
    )


def build_departures(month_flights: DataFrame, context_flights: DataFrame,
                     weather_hourly: DataFrame, airports: dict) -> DataFrame:
    """One row per JFK/LGA departure of month_flights, with inbound flight and weather at both ends.

    context_flights must contain month_flights and the month before it, so that a
    plane that arrived the evening before the first of the month is found.
    """
    from pyspark.sql import functions as F

    departures = (
        enrich_flights(month_flights, weather_hourly)
        # Exact actual departure: scheduled UTC + delay. This avoids the one-hour error
        # that local clock arithmetic gives across a daylight-saving change (D3).
        .withColumn("actual_dep_utc", F.expr("timestampadd(MINUTE, dep_delay_min, scheduled_dep_utc)"))
        .join(previous_departure(context_flights), "record_hash", "left")
    )
    linked = link_inbound(departures, inbound_candidates(context_flights, airports))
    return add_origin_weather(linked, weather_hourly).drop("previous_dep_utc")
