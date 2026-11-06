"""Attach hourly weather to scheduled JFK and LGA departures.

BTS clocks are local civil time, so they are converted with New York's rules.
weather_hourly is already in UTC, converted from fixed UTC-5 (decision D5).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

AIRPORTS = ("JFK", "LGA")
LOCAL_TIME_ZONE = "America/New_York"
WEATHER_COLUMNS = (
    "station_id", "observation_utc", "temperature_c", "precipitation_mm", "precipitation_trace",
    "wind_speed_mps", "wind_direction_deg", "wind_direction_variable", "wind_gust_mps",
    "visibility_km", "visibility_variable",
)


def enrich_flights(flights: DataFrame, weather_hourly: DataFrame) -> DataFrame:
    """Return one row per JFK/LGA departure, with weather for its scheduled UTC hour.

    Flights without a weather row are kept with null weather columns, which
    observation_utc shows. Cancelled flights are kept because the join uses the
    scheduled time. Weather is not filtered by month, because a late June
    departure can fall in July in UTC.
    """
    from pyspark.sql import functions as F

    if flights.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Enrichment requires explicit session timeZone=UTC")
    # A repeated key would silently multiply flights in the join.
    if weather_hourly.groupBy("airport_code", "weather_hour_utc").count().filter("count > 1").take(1):
        raise ValueError("weather_hourly has more than one row for an airport and hour")

    departures = flights.filter(F.col("origin").isin(*AIRPORTS)).withColumn(
        "scheduled_dep_utc",
        F.to_utc_timestamp(F.col("scheduled_dep_local").cast("timestamp"), LOCAL_TIME_ZONE),
    ).withColumn("weather_hour_utc", F.date_trunc("hour", "scheduled_dep_utc"))
    # Flights have their own record_hash and quality_flags, so these two are renamed.
    weather = weather_hourly.select(
        F.col("airport_code").alias("origin"), "weather_hour_utc", *WEATHER_COLUMNS,
        F.col("record_hash").alias("weather_record_hash"),
        F.col("quality_flags").alias("weather_quality_flags"),
    )
    return departures.join(weather, ["origin", "weather_hour_utc"], "left")
