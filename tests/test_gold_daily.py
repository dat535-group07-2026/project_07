"""Hand-made flights and weather hours with hand-calculated daily results."""

import unittest
from datetime import date

from project07.gold.daily import daily_summary, daily_weather


def rounded(rows):
    return [tuple(round(v, 4) if isinstance(v, float) else v for v in row) for row in rows]


class DailySummaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("project07-gold-fixtures")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")
        cls.addClassCleanup(cls.spark.stop)

    def test_daily_rows_match_hand_calculation(self):
        rows = [
            ("A", "2025-06-15", "JFK", False, False, 30, "2025-06-15 14:20:00", "2025-06-15 14:51:00"),
            ("B", "2025-06-15", "JFK", False, False, -6, "2025-06-15 15:10:00", "2025-06-15 15:51:00"),
            ("C", "2025-06-15", "JFK", True, False, None, "2025-06-15 16:05:00", "2025-06-15 16:51:00"),
            ("D", "2025-06-15", "JFK", True, False, None, "2025-06-15 17:30:00", None),
            # Diverted is not cancelled, so its delay counts.
            ("E", "2025-06-15", "JFK", False, True, 15, "2025-06-15 18:45:00", None),
            ("F", "2025-06-15", "LGA", False, False, 0, "2025-06-15 14:20:00", "2025-06-15 14:51:00"),
            # Not cancelled, but the delay is unknown, so it is left out of both delay metrics.
            ("G", "2025-06-15", "LGA", False, False, None, "2025-06-15 19:00:00", "2025-06-15 19:51:00"),
            # These two stay on 30 June, although their UTC time is 1 July.
            ("H", "2025-06-30", "JFK", False, False, 45, "2025-07-01 04:00:00", "2025-07-01 04:51:00"),
            ("I", "2025-06-30", "JFK", True, False, None, "2025-07-01 01:30:00", None),
        ]
        enriched = self.spark.createDataFrame(
            rows,
            "flight string, flight_date string, origin string, cancelled boolean, diverted boolean, "
            "dep_delay_min int, scheduled_dep_utc string, observation_utc string",
        ).selectExpr(
            "flight", "cast(flight_date as date) flight_date", "origin", "cancelled", "diverted",
            "dep_delay_min", "cast(scheduled_dep_utc as timestamp) scheduled_dep_utc",
            "cast(observation_utc as timestamp) observation_utc",
        )
        actual = daily_summary(enriched).orderBy("flight_date", "origin").collect()
        self.assertEqual(rounded(actual), [
            # date, airport, flights, cancelled, cancellation rate, with weather, coverage,
            # average delay, delayed 15+ rate
            (date(2025, 6, 15), "JFK", 5, 2, 0.40, 3, 0.60, 13.0, 0.6667),  # (30-6+15)/3, 2 of 3
            (date(2025, 6, 15), "LGA", 2, 0, 0.00, 2, 1.00, 0.0, 0.0),      # only F has a delay
            (date(2025, 6, 30), "JFK", 2, 1, 0.50, 1, 0.50, 45.0, 1.0),
        ])

    def test_daily_weather_uses_new_york_date_and_counts_precipitation_hours(self):
        rows = [
            # airport, UTC hour, temperature, precipitation, trace, wind, gust, visibility
            ("JFK", "2025-06-15 14:00:00", 20.0, 1.5, False, 5.0, None, 16.0),
            ("JFK", "2025-06-15 18:00:00", 24.0, None, True, 7.0, 12.0, 10.0),
            # 02:00 UTC on 16 June is 22:00 on 15 June in New York.
            ("JFK", "2025-06-16 02:00:00", 22.0, 0.5, False, 3.0, None, 8.0),
            # 04:00 UTC on 16 June is midnight on 16 June in New York.
            ("JFK", "2025-06-16 04:00:00", 18.0, 2.0, False, 4.0, 9.0, 5.0),
            # Missing precipitation: not a trace, and not a number.
            ("LGA", "2025-06-15 15:00:00", 21.0, None, False, 6.0, None, 16.0),
        ]
        hourly = self.spark.createDataFrame(
            rows,
            "airport_code string, weather_hour_utc string, temperature_c double, "
            "precipitation_mm double, precipitation_trace boolean, wind_speed_mps double, "
            "wind_gust_mps double, visibility_km double",
        ).selectExpr(
            "airport_code", "cast(weather_hour_utc as timestamp) weather_hour_utc", "temperature_c",
            "precipitation_mm", "precipitation_trace", "wind_speed_mps", "wind_gust_mps", "visibility_km",
        )
        actual = daily_weather(hourly).orderBy("flight_date", "origin").collect()
        self.assertEqual(rounded(actual), [
            # date, airport, hours, avg temp, reported precipitation, precipitation hours,
            # trace hours, max wind, max gust, min visibility
            (date(2025, 6, 15), "JFK", 3, 22.0, 2.0, 2, 1, 7.0, 12.0, 8.0),
            (date(2025, 6, 15), "LGA", 1, 21.0, None, 0, 0, 6.0, None, 16.0),
            (date(2025, 6, 16), "JFK", 1, 18.0, 2.0, 1, 0, 4.0, 9.0, 5.0),
        ])
