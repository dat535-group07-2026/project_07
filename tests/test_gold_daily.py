"""Nine hand-made flights with hand-calculated daily results."""

import unittest
from datetime import date

from project07.gold.daily import daily_summary


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
            ("A", "2025-06-15", "JFK", False, False, "2025-06-15 14:20:00", "2025-06-15 14:51:00"),
            ("B", "2025-06-15", "JFK", False, False, "2025-06-15 15:10:00", "2025-06-15 15:51:00"),
            ("C", "2025-06-15", "JFK", True, False, "2025-06-15 16:05:00", "2025-06-15 16:51:00"),
            ("D", "2025-06-15", "JFK", True, False, "2025-06-15 17:30:00", None),
            # Diverted is not cancelled.
            ("E", "2025-06-15", "JFK", False, True, "2025-06-15 18:45:00", None),
            ("F", "2025-06-15", "LGA", False, False, "2025-06-15 14:20:00", "2025-06-15 14:51:00"),
            ("G", "2025-06-15", "LGA", False, False, "2025-06-15 19:00:00", "2025-06-15 19:51:00"),
            # These two stay on 30 June, although their UTC time is 1 July.
            ("H", "2025-06-30", "JFK", False, False, "2025-07-01 04:00:00", "2025-07-01 04:51:00"),
            ("I", "2025-06-30", "JFK", True, False, "2025-07-01 01:30:00", None),
        ]
        enriched = self.spark.createDataFrame(
            rows,
            "flight string, flight_date string, origin string, cancelled boolean, diverted boolean, "
            "scheduled_dep_utc string, observation_utc string",
        ).selectExpr(
            "flight", "cast(flight_date as date) flight_date", "origin", "cancelled", "diverted",
            "cast(scheduled_dep_utc as timestamp) scheduled_dep_utc",
            "cast(observation_utc as timestamp) observation_utc",
        )
        actual = daily_summary(enriched).orderBy("flight_date", "origin").collect()
        self.assertEqual([tuple(row) for row in actual], [
            # date, airport, flights, cancelled, cancellation rate, with weather, coverage
            (date(2025, 6, 15), "JFK", 5, 2, 0.40, 3, 0.60),
            (date(2025, 6, 15), "LGA", 2, 0, 0.00, 2, 1.00),
            (date(2025, 6, 30), "JFK", 2, 1, 0.50, 1, 0.50),
        ])
