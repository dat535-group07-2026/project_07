"""Gold departure_chain: linked departures by how late the inbound plane was and the weather at both ends.

One row per (flight_date, origin, inbound_delay_band, local_weather, origin_weather).
Only departures that Silver linked to an inbound flight are counted. The table
keeps sums and counts, not averages, so rows can be added up over any period
(average = total / count).

"Bad" weather in an hour means a thunderstorm, visibility under 5 km, or a
measurable amount of rain. Blank rain hours count as dry, as in the rain
analysis. "unknown" means there was no weather report for that hour (or no
station at the inbound flight's origin).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

INBOUND_DELAY_BANDS = ("under 15", "15-44", "45-89", "90+", "unknown")
WEATHER_GROUPS = ("good", "bad", "unknown")


def _weather_group(prefix: str) -> str:
    return f"""CASE WHEN {prefix}observation_utc IS NULL THEN 'unknown'
            WHEN COALESCE({prefix}thunderstorm, false) OR {prefix}visibility_km < 5
                 OR {prefix}precipitation_mm > 0 THEN 'bad'
            ELSE 'good' END"""


DEPARTURE_CHAIN_SQL = f"""
WITH linked AS (
  SELECT *,
         CASE WHEN inbound_arr_delay_min IS NULL THEN 'unknown'
              WHEN inbound_arr_delay_min < 15 THEN 'under 15'
              WHEN inbound_arr_delay_min < 45 THEN '15-44'
              WHEN inbound_arr_delay_min < 90 THEN '45-89'
              ELSE '90+' END AS inbound_delay_band,
         {_weather_group('')} AS local_weather,
         {_weather_group('origin_')} AS origin_weather
  FROM departures
  WHERE link_status = 'linked'
)
SELECT flight_date, origin, inbound_delay_band, local_weather, origin_weather,
       COUNT(*) AS departures,
       COUNT(dep_delay_min) AS departures_with_delay,
       SUM(CASE WHEN dep_delay_min >= 15 THEN 1 ELSE 0 END) AS delayed_15,
       SUM(dep_delay_min) AS total_dep_delay_min,
       SUM(inbound_arr_delay_min) AS total_inbound_arr_delay_min,
       SUM(scheduled_ground_min) AS total_scheduled_ground_min,
       SUM(late_aircraft_delay_min) AS bts_late_aircraft_delay_min,
       SUM(weather_delay_min) AS bts_weather_delay_min
FROM linked
GROUP BY flight_date, origin, inbound_delay_band, local_weather, origin_weather
"""


def departure_chain(departures: DataFrame) -> DataFrame:
    """Counts and delay sums of linked departures per day, airport, inbound delay and weather."""
    departures.createOrReplaceTempView("departures")
    return departures.sparkSession.sql(DEPARTURE_CHAIN_SQL)
