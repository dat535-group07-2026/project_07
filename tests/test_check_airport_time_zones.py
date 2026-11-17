import datetime as dt
import unittest

from project07.reference import Airport
from scripts import check_airport_time_zones as check
from tests.spark_case import SparkTestCase

SCHEMA = ("origin string, dest string, flight_date date, scheduled_dep_hhmm string, "
          "scheduled_arr_hhmm string, scheduled_elapsed_min int")
AIRPORTS = {
    "JFK": Airport("JFK", "New York", "America/New_York", None),
    "ORD": Airport("ORD", "Chicago", "America/Chicago", None),
    "LAX": Airport("LAX", "Los Angeles", "America/Los_Angeles", None),
}
SUMMER = dt.date(2025, 7, 1)
WINTER = dt.date(2025, 1, 15)


class ImpliedOffsetTests(SparkTestCase):
    def implied(self, rows, airports=AIRPORTS):
        flights = self.spark.createDataFrame(rows, SCHEMA)
        result = check.implied_offsets(flights, airports)
        return sorted((r["airport"], r["implied_offset_min"], r["expected_offset_min"]) for r in result.collect())

    def test_inbound_flight_gives_origin_offset(self):
        # Chicago 13:00 CDT -> New York 16:20 EDT, 140 minutes: Chicago is UTC-5 in summer.
        rows = [("ORD", "JFK", SUMMER, "1300", "1620", 140)]
        self.assertEqual(self.implied(rows), [("ORD", -300, -300)])

    def test_outbound_flight_gives_destination_offset(self):
        # New York 08:00 EDT -> Los Angeles 11:30 PDT, 390 minutes: Los Angeles is UTC-7.
        rows = [("JFK", "LAX", SUMMER, "0800", "1130", 390)]
        self.assertEqual(self.implied(rows), [("LAX", -420, -420)])

    def test_overnight_flight_wraps_around_midnight(self):
        # Los Angeles 22:30 PDT -> New York 07:00 EDT next day, 330 minutes.
        rows = [("LAX", "JFK", SUMMER, "2230", "0700", 330)]
        self.assertEqual(self.implied(rows), [("LAX", -420, -420)])

    def test_winter_offsets_use_standard_time(self):
        rows = [("ORD", "JFK", WINTER, "1300", "1620", 140)]
        self.assertEqual(self.implied(rows), [("ORD", -360, -360)])

    def test_wrong_time_zone_is_reported(self):
        wrong = {**AIRPORTS, "ORD": Airport("ORD", "Chicago", "America/Denver", None)}
        rows = [("ORD", "JFK", SUMMER, "1300", "1620", 140), ("ORD", "JFK", SUMMER, "0700", "1020", 140)]
        summary = check.summarise(check.implied_offsets(self.spark.createDataFrame(rows, SCHEMA), wrong))
        self.assertEqual(summary[0]["airport"], "ORD")
        self.assertEqual(summary[0]["matching"], 0)
        self.assertFalse(summary[0]["ok"])
        self.assertEqual(summary[0]["most_common_implied_offset_min"], -300)

    def test_flights_without_jfk_or_lga_or_missing_clock_are_ignored(self):
        rows = [("ORD", "LAX", SUMMER, "1300", "1500", 240), ("ORD", "JFK", SUMMER, None, "1620", 140)]
        self.assertEqual(self.implied(rows), [])


if __name__ == "__main__":
    unittest.main()
