import tempfile
import unittest
from pathlib import Path

from project07 import reference
from project07.silver import enrichment, weather
from scripts import ingest_bronze

HEADER = "airport,city,time_zone,noaa_station\n"


def write_table(text: str) -> Path:
    folder = Path(tempfile.mkdtemp())
    path = folder / "airports.csv"
    path.write_text(text, encoding="utf-8")
    return path


class AirportTableTests(unittest.TestCase):
    def setUp(self):
        self.airports = reference.load_airports()

    def test_table_covers_jfk_lga_and_their_inbound_airports(self):
        self.assertEqual(len(self.airports), 113)
        for code in ("JFK", "LGA", "ORD", "HNL", "SJU", "STT", "ANC"):
            self.assertIn(code, self.airports)

    def test_jfk_and_lga_match_the_values_used_today(self):
        expected = {"JFK": "USW00094789", "LGA": "USW00014732"}
        for code, station in expected.items():
            self.assertEqual(self.airports[code].noaa_station, station)
            self.assertEqual(self.airports[code].time_zone, enrichment.LOCAL_TIME_ZONE)
            self.assertEqual(weather.STATION_AIRPORTS[station], code)
            self.assertEqual(ingest_bronze.STATIONS[code], station)

    def test_thirty_two_airports_have_a_weather_station(self):
        stations = reference.weather_stations()
        self.assertEqual(len(stations), 32)
        self.assertEqual(stations["USW00094846"], "ORD")
        self.assertEqual(stations["RQW00011641"], "SJU")

    def test_standard_offsets(self):
        expected = {
            "JFK": -5, "ORD": -6, "DEN": -7, "PHX": -7, "LAX": -8,
            "ANC": -9, "HNL": -10, "SJU": -4, "STT": -4, "IND": -5, "SDF": -5,
        }
        for code, hours in expected.items():
            with self.subTest(code=code):
                self.assertEqual(reference.standard_offset_hours(self.airports[code].time_zone), hours)


class LoaderRejectsBadTablesTests(unittest.TestCase):
    def assert_rejected(self, rows: str, message: str):
        with self.assertRaisesRegex(ValueError, message):
            reference.load_airports(write_table(HEADER + rows))

    def test_comments_and_empty_station_are_allowed(self):
        path = write_table("# comment\n" + HEADER + 'AAA,"A, AA",America/Chicago,\n')
        self.assertIsNone(reference.load_airports(path)["AAA"].noaa_station)

    def test_wrong_header(self):
        with self.assertRaisesRegex(ValueError, "header"):
            reference.load_airports(write_table("code,zone\nAAA,America/Chicago\n"))

    def test_duplicate_airport(self):
        self.assert_rejected("AAA,a,America/Chicago,\nAAA,a,America/Chicago,\n", "listed twice")

    def test_invalid_airport_code(self):
        self.assert_rejected("aa1,a,America/Chicago,\n", "invalid airport code")

    def test_unknown_time_zone(self):
        self.assert_rejected("AAA,a,America/Nowhere,\n", "unknown time zone")

    def test_invalid_station(self):
        self.assert_rejected("AAA,a,America/Chicago,USW123\n", "invalid NOAA station")

    def test_duplicate_station(self):
        self.assert_rejected(
            "AAA,a,America/Chicago,USW00000001\nBBB,b,America/Chicago,USW00000001\n", "listed twice"
        )

    def test_wrong_number_of_fields(self):
        self.assert_rejected("AAA,a,America/Chicago\n", "expected 4 fields")


if __name__ == "__main__":
    unittest.main()
