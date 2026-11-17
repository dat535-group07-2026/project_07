"""Gold over many months: month selection, weather coverage, the year boundary."""

import unittest
from datetime import date, datetime

from project07.gold import run
from project07.silver.enrichment import enrich_flights
from tests.spark_case import SparkTestCase


class MonthSelectionTests(unittest.TestCase):
    PUBLISHED = ["2026-01", "2025-06", "2025-12", "2025-07"]

    def test_all_published_months_by_default(self):
        self.assertEqual(run.select_months(self.PUBLISHED), ["2025-06", "2025-07", "2025-12", "2026-01"])

    def test_start_and_end_are_inclusive(self):
        self.assertEqual(run.select_months(self.PUBLISHED, "2025-07", "2025-12"), ["2025-07", "2025-12"])

    def test_no_month_in_range_fails(self):
        with self.assertRaisesRegex(ValueError, "No published flight months"):
            run.select_months(self.PUBLISHED, "2024-01", "2024-12")


class CoverageAndYearBoundaryTests(SparkTestCase):
    FLIGHTS = "flight_date date, origin string, scheduled_dep_local timestamp_ntz, cancelled boolean"
    WEATHER = "airport_code string, weather_hour_utc timestamp, observation_utc timestamp, record_hash string, quality_flags array<string>"

    def weather_rows(self, rows):
        from pyspark.sql import functions as F
        from project07.silver.enrichment import WEATHER_COLUMNS

        frame = self.spark.createDataFrame(rows, self.WEATHER)
        for column in WEATHER_COLUMNS:
            if column not in frame.columns:
                frame = frame.withColumn(column, F.lit(None).cast("string"))
        return frame

    def test_coverage_per_month(self):
        flights = self.spark.createDataFrame([
            (date(2025, 6, 1), "JFK", datetime(2025, 6, 1, 10, 0), False),
            (date(2025, 6, 2), "JFK", datetime(2025, 6, 2, 10, 0), False),
            (date(2025, 7, 1), "JFK", datetime(2025, 7, 1, 10, 0), False),
        ], self.FLIGHTS)
        weather = self.weather_rows([
            ("JFK", datetime(2025, 6, 1, 14), datetime(2025, 6, 1, 14, 51), "a", []),
            ("JFK", datetime(2025, 7, 1, 14), datetime(2025, 7, 1, 14, 51), "b", []),
        ])
        coverage = run.weather_coverage_per_month(enrich_flights(flights, weather))
        self.assertEqual(coverage, {"2025-06": 0.5, "2025-07": 1.0})
        self.assertLess(coverage["2025-06"], run.MIN_WEATHER_COVERAGE)

    def test_new_year_eve_flight_uses_weather_from_the_old_year_file(self):
        # 31 Dec 2025 23:30 New York (winter, UTC-5) is 1 Jan 2026 04:30 UTC. NOAA's 2025
        # file ends at 23:59 local standard time, which is 04:59 UTC on 1 January.
        flights = self.spark.createDataFrame(
            [(date(2025, 12, 31), "JFK", datetime(2025, 12, 31, 23, 30), False)], self.FLIGHTS)
        weather = self.weather_rows([("JFK", datetime(2026, 1, 1, 4), datetime(2026, 1, 1, 4, 51), "a", [])])
        row = enrich_flights(flights, weather).selectExpr(
            "date_format(weather_hour_utc, 'yyyy-MM-dd HH:mm') AS hour", "observation_utc IS NOT NULL AS matched"
        ).first()
        self.assertEqual(row.hour, "2026-01-01 04:00")
        self.assertTrue(row.matched)


if __name__ == "__main__":
    unittest.main()
