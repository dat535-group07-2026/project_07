"""Small fixtures for the flight and weather join."""

import unittest

from project07.silver.enrichment import enrich_flights


class EnrichmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("project07-enrichment-fixtures")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")
        cls.addClassCleanup(cls.spark.stop)

    def flights(self, *rows):
        """Rows are (flight_number, origin, scheduled local time, cancelled)."""
        return self.spark.createDataFrame(
            rows, "flight_number string, origin string, local string, cancelled boolean",
        ).selectExpr(
            "flight_number", "origin", "cast(local as timestamp_ntz) scheduled_dep_local", "cancelled",
            "cast(null as timestamp_ntz) actual_dep_local",
            "'flight-hash' record_hash", "array('FLIGHT_FLAG') quality_flags", "6 month",
            "'flights.csv' source_file",
        )

    def weather(self, *rows):
        """Rows are (airport, UTC hour, temperature)."""
        return self.spark.createDataFrame(
            rows, "airport_code string, hour string, temperature_c double",
        ).selectExpr(
            "airport_code", "concat('station-', airport_code) station_id",
            "cast(hour as timestamp) weather_hour_utc",
            "cast(hour as timestamp) + interval 51 minutes observation_utc", "temperature_c",
            "cast(null as double) precipitation_mm", "cast(null as boolean) precipitation_trace",
            "cast(null as double) wind_speed_mps", "cast(null as int) wind_direction_deg",
            "cast(null as boolean) wind_direction_variable", "cast(null as double) wind_gust_mps",
            "cast(null as double) visibility_km", "cast(null as boolean) visibility_variable",
            "'weather-hash' record_hash", "array('WEATHER_FLAG') quality_flags",
            "month(cast(hour as timestamp)) month", "'weather.csv' source_file",
        )

    def by_flight(self, frame):
        """Collect times as text, so the result does not depend on the machine's time zone."""
        from pyspark.sql import functions as F

        rows = frame.withColumn(
            "utc", F.date_format("scheduled_dep_utc", "yyyy-MM-dd HH:mm"),
        ).withColumn("hour", F.date_format("weather_hour_utc", "yyyy-MM-dd HH:mm")).collect()
        self.assertEqual(len(rows), len({row.flight_number for row in rows}))
        return {row.flight_number: row for row in rows}

    def test_scheduled_time_is_converted_to_utc_before_joining(self):
        from pyspark.sql import functions as F

        # The 12:40 local actual departure falls in the 16:00 UTC hour, which must not be used.
        flights = self.flights(("1", "JFK", "2025-06-15 10:20:00", False)).withColumn(
            "actual_dep_local", F.lit("2025-06-15 12:40:00").cast("timestamp_ntz"),
        )
        rows = self.by_flight(enrich_flights(
            flights,
            self.weather(
                ("JFK", "2025-06-15 10:00:00", 10.0),
                ("JFK", "2025-06-15 14:00:00", 14.0),
                ("JFK", "2025-06-15 16:00:00", 16.0),
            ),
        ))
        self.assertEqual(rows["1"].utc, "2025-06-15 14:20")
        self.assertEqual(rows["1"].hour, "2025-06-15 14:00")
        self.assertEqual(rows["1"].temperature_c, 14.0)

    def test_late_june_departures_use_july_weather(self):
        rows = self.by_flight(enrich_flights(
            self.flights(
                ("evening", "LGA", "2025-06-30 21:30:00", False),
                # A scheduled clock of 2400 on 30 June is stored as midnight on 1 July.
                ("midnight", "LGA", "2025-07-01 00:00:00", False),
            ),
            self.weather(("LGA", "2025-07-01 01:00:00", 21.0), ("LGA", "2025-07-01 04:00:00", 24.0)),
        ))
        self.assertEqual(rows["evening"].utc, "2025-07-01 01:30")
        self.assertEqual(rows["evening"].temperature_c, 21.0)
        self.assertEqual(rows["midnight"].utc, "2025-07-01 04:00")
        self.assertEqual(rows["midnight"].temperature_c, 24.0)
        self.assertEqual(rows["evening"].month, 6)

    def test_cancelled_flight_gets_weather_from_its_scheduled_time(self):
        rows = self.by_flight(enrich_flights(
            self.flights(("1", "JFK", "2025-06-15 10:20:00", True)),
            self.weather(("JFK", "2025-06-15 14:00:00", 14.0)),
        ))
        self.assertTrue(rows["1"].cancelled)
        self.assertIsNone(rows["1"].actual_dep_local)
        self.assertEqual(rows["1"].temperature_c, 14.0)

    def test_flight_without_weather_is_kept_with_null_weather(self):
        rows = self.by_flight(enrich_flights(
            self.flights(("1", "JFK", "2025-06-15 10:20:00", False)),
            self.weather(("JFK", "2025-06-15 15:00:00", 15.0)),
        ))
        self.assertIsNone(rows["1"].observation_utc)
        self.assertIsNone(rows["1"].temperature_c)
        self.assertIsNone(rows["1"].weather_record_hash)
        self.assertEqual(rows["1"].hour, "2025-06-15 14:00")

    def test_duplicate_weather_key_fails(self):
        with self.assertRaisesRegex(ValueError, "more than one row"):
            enrich_flights(
                self.flights(("1", "JFK", "2025-06-15 10:20:00", False)),
                self.weather(("JFK", "2025-06-15 14:00:00", 14.0), ("JFK", "2025-06-15 14:00:00", 15.0)),
            )

    def test_only_jfk_lga_departures_are_kept_and_matched_by_airport(self):
        rows = self.by_flight(enrich_flights(
            self.flights(
                ("jfk", "JFK", "2025-06-15 10:20:00", False),
                ("lga", "LGA", "2025-06-15 10:20:00", False),
                ("bos", "BOS", "2025-06-15 10:20:00", False),
            ),
            # The same hour at two airports is not a duplicate key.
            self.weather(("JFK", "2025-06-15 14:00:00", 14.0), ("LGA", "2025-06-15 14:00:00", 16.0)),
        ))
        self.assertEqual(set(rows), {"jfk", "lga"})
        self.assertEqual(rows["jfk"].temperature_c, 14.0)
        self.assertEqual(rows["lga"].temperature_c, 16.0)

    def test_columns_that_exist_on_both_sides_stay_separate(self):
        row = self.by_flight(enrich_flights(
            self.flights(("1", "JFK", "2025-06-15 10:20:00", False)),
            self.weather(("JFK", "2025-06-15 14:00:00", 14.0)),
        ))["1"]
        self.assertEqual(row.record_hash, "flight-hash")
        self.assertEqual(row.weather_record_hash, "weather-hash")
        self.assertEqual(row.quality_flags, ["FLIGHT_FLAG"])
        self.assertEqual(row.weather_quality_flags, ["WEATHER_FLAG"])
        self.assertEqual(row.source_file, "flights.csv")
