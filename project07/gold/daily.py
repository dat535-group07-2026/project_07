"""Daily Gold table for JFK and LGA departures, written in Spark SQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# weather_coverage is the share of flights matched to hourly weather.
# observation_utc is only null when the join found no weather row.
# Both delay metrics use non-cancelled flights with a known delay, diverted included,
# so they share the same denominator. AVG skips the nulls.
DAILY_SUMMARY_SQL = """
SELECT flight_date, origin,
       COUNT(*) AS flights,
       SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) AS cancelled_flights,
       SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) / COUNT(*) AS cancellation_rate,
       COUNT(observation_utc) AS flights_with_weather,
       COUNT(observation_utc) / COUNT(*) AS weather_coverage,
       AVG(CASE WHEN NOT cancelled THEN dep_delay_min END) AS avg_dep_delay_min,
       AVG(CASE WHEN NOT cancelled AND dep_delay_min IS NOT NULL
                THEN CASE WHEN dep_delay_min >= 15 THEN 1 ELSE 0 END END) AS delayed_15_rate
FROM enriched_flights
GROUP BY flight_date, origin
"""

# The weather day is the New York date, so it lines up with the local flight_date.
# precipitation_mm is null for trace and for missing hours, so the sum only covers
# reported hourly amounts. precipitation_hours and trace_hours show how complete it is.
DAILY_WEATHER_SQL = """
SELECT to_date(from_utc_timestamp(weather_hour_utc, 'America/New_York')) AS flight_date,
       airport_code AS origin,
       COUNT(*) AS weather_hours,
       AVG(temperature_c) AS avg_temperature_c,
       SUM(precipitation_mm) AS reported_precipitation_mm,
       COUNT(precipitation_mm) AS precipitation_hours,
       SUM(CASE WHEN precipitation_trace THEN 1 ELSE 0 END) AS trace_hours,
       MAX(wind_speed_mps) AS max_wind_speed_mps,
       MAX(wind_gust_mps) AS max_wind_gust_mps,
       MIN(visibility_km) AS min_visibility_km
FROM weather_hourly
GROUP BY 1, 2
"""


def daily_summary(enriched: DataFrame) -> DataFrame:
    """Return one row per scheduled date and origin airport."""
    enriched.createOrReplaceTempView("enriched_flights")
    return enriched.sparkSession.sql(DAILY_SUMMARY_SQL)


def daily_weather(weather_hourly: DataFrame) -> DataFrame:
    """Return one row per New York date and airport."""
    weather_hourly.createOrReplaceTempView("weather_hourly")
    return weather_hourly.sparkSession.sql(DAILY_WEATHER_SQL)


def daily_airport(enriched: DataFrame, weather_hourly: DataFrame) -> DataFrame:
    """Flight summary per date and airport, with that day's weather added."""
    return daily_summary(enriched).join(
        daily_weather(weather_hourly), ["flight_date", "origin"], "left"
    )
