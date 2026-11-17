"""Linking departures to the inbound flight of the same plane, with weather at both ends.

Times in June 2025: New York is UTC-4, Chicago UTC-5, Los Angeles UTC-7.
"""

from datetime import date, datetime

from project07 import reference
from project07.silver import chain
from project07.silver.enrichment import WEATHER_COLUMNS
from tests.spark_case import SparkTestCase

FLIGHTS = ("record_hash string, flight_date date, reporting_airline string, flight_number string, "
           "origin string, dest string, tail_number string, cancelled boolean, diverted boolean, "
           "scheduled_dep_local timestamp_ntz, scheduled_elapsed_min int, dep_delay_min int, "
           "arr_delay_min int, quality_flags array<string>")
D = date(2025, 6, 1)


def flight(key, origin, dest, tail, local, elapsed=75, dep_delay=0, arr_delay=0, cancelled=False,
           flight_date=None):
    when = datetime.fromisoformat(local)
    return (key, flight_date or when.date(), "AA", key, origin, dest, tail, cancelled, False,
            when, elapsed, dep_delay, arr_delay, [])


ROWS = [
    flight("a1", "BOS", "JFK", "T1", "2025-06-01 08:00", arr_delay=20),
    flight("d1", "JFK", "ORD", "T1", "2025-06-01 10:00", dep_delay=5),
    # Overnight: Los Angeles 22:30 PDT = 05:30 UTC on 2 June, plus 330 minutes = 07:00 EDT.
    flight("a2", "LAX", "JFK", "T2", "2025-06-01 22:30", elapsed=330),
    flight("d2", "JFK", "BOS", "T2", "2025-06-02 08:00"),
    flight("d3", "JFK", "BOS", "T3", "2025-06-01 10:00"),
    flight("d4", "JFK", "BOS", None, "2025-06-01 10:00"),
    flight("a5", "BOS", "JFK", "T5", "2025-06-01 08:00", cancelled=True),
    flight("d5", "JFK", "BOS", "T5", "2025-06-01 10:00"),
    flight("a6x", "DCA", "JFK", "T6", "2025-06-01 05:00", elapsed=60),
    flight("a6y", "BOS", "JFK", "T6", "2025-06-01 07:00", elapsed=60),
    flight("d6", "JFK", "BOS", "T6", "2025-06-01 09:00"),
    flight("a7", "BOS", "LGA", "T7", "2025-06-01 08:00"),
    flight("d7", "JFK", "BOS", "T7", "2025-06-01 10:00"),
    flight("a8", "BOS", "JFK", "T8", "2025-06-01 08:00"),
    flight("d8a", "JFK", "BOS", "T8", "2025-06-01 10:00"),
    flight("d8b", "JFK", "BOS", "T8", "2025-06-01 15:00"),
    # Arrived on 31 May (previous month), leaves on 1 June.
    flight("a9", "BOS", "JFK", "T9", "2025-05-31 21:00"),
    flight("d9", "JFK", "BOS", "T9", "2025-06-01 06:00"),
    # Chicago 07:00 CDT = 12:00 UTC; Chicago weather for that hour is in the fixture.
    flight("a10", "ORD", "JFK", "T10", "2025-06-01 07:00", elapsed=140),
    flight("d10", "JFK", "ORD", "T10", "2025-06-01 13:00"),
    flight("a11", "XXX", "JFK", "T11", "2025-06-01 07:00"),
    # A cancelled departure does not use the plane; its replacement gets the inbound flight.
    flight("a12", "BOS", "JFK", "T12", "2025-06-01 08:00"),
    flight("d12a", "JFK", "BOS", "T12", "2025-06-01 10:00", cancelled=True),
    flight("d12b", "JFK", "BOS", "T12", "2025-06-01 11:00"),
]


class ChainTests(SparkTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from pyspark.sql import functions as F

        cls.airports = reference.load_airports()
        context = cls.spark.createDataFrame(ROWS, FLIGHTS)
        month = context.filter(F.date_format("flight_date", "yyyy-MM") == "2025-06")
        weather = cls.spark.createDataFrame([
            ("ORD", datetime(2025, 6, 1, 12), datetime(2025, 6, 1, 12, 51), 18.5),
            ("JFK", datetime(2025, 6, 1, 14), datetime(2025, 6, 1, 14, 51), 25.0),
        ], "airport_code string, weather_hour_utc timestamp, observation_utc timestamp, temperature_c double")
        for column in WEATHER_COLUMNS:
            if column not in weather.columns:
                weather = weather.withColumn(column, F.lit(None).cast("string"))
        weather = weather.withColumn("record_hash", F.lit("w")).withColumn("quality_flags", F.array())
        cls.candidates = chain.inbound_candidates(context, cls.airports)
        result = chain.build_departures(month, context, weather, cls.airports)
        text = "yyyy-MM-dd HH:mm"
        cls.rows = {
            r.record_hash: r for r in result
            .withColumn("dep_utc", F.date_format("scheduled_dep_utc", text))
            .withColumn("actual_utc", F.date_format("actual_dep_utc", text))
            .withColumn("in_arr_utc", F.date_format("inbound_scheduled_arr_utc", text))
            .withColumn("in_actual_utc", F.date_format("inbound_actual_arr_utc", text))
            .collect()
        }

    def assert_link(self, departure, inbound, ground=None):
        row = self.rows[departure]
        self.assertEqual(row.link_status, "linked")
        self.assertEqual(row.inbound_record_hash, inbound)
        if ground is not None:
            self.assertEqual(row.scheduled_ground_min, ground)
        return row

    def assert_no_link(self, departure, status="no_inbound_found"):
        row = self.rows[departure]
        self.assertEqual(row.link_status, status)
        self.assertIsNone(row.inbound_record_hash)

    def test_one_row_per_departure_of_the_month(self):
        self.assertEqual(sorted(self.rows), sorted(r[0] for r in ROWS if r[4] == "JFK"))

    def test_normal_link_with_utc_times(self):
        row = self.assert_link("d1", "a1", ground=45)
        self.assertEqual(row.dep_utc, "2025-06-01 14:00")
        self.assertEqual(row.actual_utc, "2025-06-01 14:05")
        self.assertEqual(row.in_arr_utc, "2025-06-01 13:15")
        self.assertEqual(row.in_actual_utc, "2025-06-01 13:35")
        self.assertEqual(row.inbound_origin, "BOS")
        self.assertEqual(row.inbound_arr_delay_min, 20)

    def test_overnight_inbound_from_another_time_zone(self):
        row = self.assert_link("d2", "a2", ground=60)
        self.assertEqual(row.in_arr_utc, "2025-06-02 11:00")

    def test_no_inbound_and_no_tail_number(self):
        self.assert_no_link("d3")
        self.assert_no_link("d4", "no_tail_number")

    def test_cancelled_inbound_is_skipped(self):
        self.assert_no_link("d5")

    def test_latest_of_two_candidates_wins(self):
        self.assert_link("d6", "a6y", ground=60)

    def test_plane_at_another_airport_is_ignored(self):
        self.assert_no_link("d7")

    def test_inbound_is_not_reused_after_the_plane_left(self):
        self.assert_link("d8a", "a8")
        self.assert_no_link("d8b")

    def test_inbound_from_the_previous_month(self):
        self.assert_link("d9", "a9", ground=465)

    def test_weather_at_both_ends(self):
        row = self.assert_link("d10", "a10")
        self.assertTrue(row.origin_has_station)
        self.assertEqual(row.origin_temperature_c, 18.5)
        self.assertEqual(self.rows["d1"].temperature_c, 25.0)
        self.assertFalse(self.rows["d1"].origin_has_station)
        self.assertIsNone(self.rows["d1"].origin_observation_utc)

    def test_cancelled_departure_leaves_the_inbound_for_its_replacement(self):
        self.assert_no_link("d12a", "cancelled")
        self.assert_link("d12b", "a12", ground=105)

    def test_unknown_inbound_airport_is_reported(self):
        self.assertEqual(chain.unknown_inbound_airports(self.candidates), ["XXX"])
