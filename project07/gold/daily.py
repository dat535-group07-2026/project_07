"""Daily Gold summary of JFK and LGA departures, written in Spark SQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# weather_coverage is the share of flights matched to hourly weather.
# observation_utc is only null when the join found no weather row.
DAILY_SUMMARY_SQL = """
SELECT flight_date, origin,
       COUNT(*) AS flights,
       SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) AS cancelled_flights,
       SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) / COUNT(*) AS cancellation_rate,
       COUNT(observation_utc) AS flights_with_weather,
       COUNT(observation_utc) / COUNT(*) AS weather_coverage
FROM enriched_flights
GROUP BY flight_date, origin
"""


def daily_summary(enriched: DataFrame) -> DataFrame:
    """Return one row per scheduled date and origin airport."""
    enriched.createOrReplaceTempView("enriched_flights")
    return enriched.sparkSession.sql(DAILY_SUMMARY_SQL)
