"""Gold airport_hour: one row per departure airport and UTC hour, in Spark SQL.

Built from Silver departures_enriched. Every departure in the hour was joined to
the same weather row, so the weather columns are taken with MAX (they are equal
within a group, or all null when the hour had no weather report).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

AIRPORT_HOUR_SQL = """
SELECT origin,
       weather_hour_utc,
       to_date(from_utc_timestamp(weather_hour_utc, 'America/New_York')) AS local_date,
       hour(from_utc_timestamp(weather_hour_utc, 'America/New_York')) AS local_hour,
       COUNT(*) AS flights,
       SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) AS cancelled_flights,
       AVG(CASE WHEN NOT cancelled THEN dep_delay_min END) AS avg_dep_delay_min,
       AVG(CASE WHEN NOT cancelled AND dep_delay_min IS NOT NULL
                THEN CASE WHEN dep_delay_min >= 15 THEN 1 ELSE 0 END END) AS delayed_15_rate,
       -- Counts and sums, so hours can be added up over any period.
       COUNT(CASE WHEN NOT cancelled THEN dep_delay_min END) AS flights_with_delay,
       SUM(CASE WHEN NOT cancelled AND dep_delay_min >= 15 THEN 1 ELSE 0 END) AS delayed_15,
       SUM(CASE WHEN NOT cancelled THEN dep_delay_min END) AS total_dep_delay_min,
       MAX(observation_utc) IS NOT NULL AS has_weather,
       MAX(temperature_c) AS temperature_c,
       MAX(precipitation_mm) AS precipitation_mm,
       MAX(precipitation_trace) AS precipitation_trace,
       MAX(wind_speed_mps) AS wind_speed_mps,
       MAX(wind_gust_mps) AS wind_gust_mps,
       MAX(visibility_km) AS visibility_km,
       MAX(thunderstorm) AS thunderstorm
FROM departures
GROUP BY origin, weather_hour_utc
"""


def airport_hour(departures: DataFrame) -> DataFrame:
    """Flights, cancellations, delays and weather per departure airport and hour."""
    departures.createOrReplaceTempView("departures")
    return departures.sparkSession.sql(AIRPORT_HOUR_SQL)
