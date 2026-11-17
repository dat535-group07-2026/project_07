"""Weather for many stations: per-station UTC shift, usual report minute,
thunderstorm flag, synoptic-hour check and per-year pointers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from project07.silver import common, weather
from tests.spark_case import SparkTestCase

JFK, ORD, SJU, PHX = "USW00094789", "USW00094846", "RQW00011641", "USW00023183"


def row(label: str, station: str, date: str, report_type: str = "FM-15", **changes) -> dict:
    values = dict.fromkeys(weather.RAW_FIELDS)
    values.update(STATION=station, DATE=date, REPORT_TYPE=report_type, SOURCE="7",
                  HourlyDryBulbTemperature="10.0", HourlyVisibility="16.093", REM=label)
    values.update(changes)
    return values


class StationShiftTests(unittest.TestCase):
    def test_shift_comes_from_the_reference_time_zone(self):
        expected = {JFK: 5, ORD: 6, SJU: 4, PHX: 7, "USW00023174": 8}  # LAX
        for station, hours in expected.items():
            self.assertEqual(weather.STATION_UTC_SHIFT_HOURS[station], hours)
        self.assertEqual(len(weather.STATION_AIRPORTS), 32)
        self.assertEqual(weather.STATION_AIRPORTS[ORD], "ORD")


class ManyStationSparkTests(SparkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        from pyspark.sql import functions as F

        rows = [
            row("ord-winter", ORD, "2025-01-15T12:53:00"),
            row("sju", SJU, "2025-01-15T12:53:00"),
            row("phx-summer", PHX, "2025-07-15T12:51:00"),
            # ORD reports at :53; one hour also has an extra FM-15 at :51.
            row("ord-13-53", ORD, "2025-01-15T13:53:00"),
            row("ord-14-51", ORD, "2025-01-15T14:51:00"),
            row("ord-14-53", ORD, "2025-01-15T14:53:00"),
            row("storm", JFK, "2025-06-30T23:51:00", HourlyPresentWeatherType="TS -RA:02 BR:1 |TS RA BR |RA"),
            row("vicinity-storm", JFK, "2025-06-30T22:51:00", HourlyPresentWeatherType="VCTS |VCTS |"),
            row("rain-only", JFK, "2025-06-30T21:51:00", HourlyPresentWeatherType="-RA:02 |RA |RA"),
            row("no-weather", JFK, "2025-06-30T20:51:00"),
            # FM-12 at 12:00 LST in Chicago is 18:00 UTC (a synoptic hour); 13:00 LST is not.
            row("ord-fm12-on", ORD, "2025-01-15T12:00:00", "FM-12"),
            row("ord-fm12-off", ORD, "2025-01-15T13:00:00", "FM-12"),
            row("unknown-station", "USW99999999", "2025-01-15T12:53:00"),
        ]
        raw = cls.spark.createDataFrame([[r[c] for c in weather.RAW_FIELDS] for r in rows], weather.raw_schema())
        for name, value in zip(weather.PROVENANCE, ("noaa:test", "test.csv", "c" * 64, "run", weather.RULE_VERSION)):
            raw = raw.withColumn(name, F.lit(value))
        cls.tables = weather.clean_weather(raw)
        utc = "yyyy-MM-dd'T'HH:mm"
        cls.by_label = {
            r.original.REM: r
            for r in cls.tables.classified.withColumn("utc", F.date_format("observation_utc", utc)).collect()
        }
        cls.hourly = {
            (r.station_id, r.hour): r
            for r in cls.tables.hourly.withColumn("hour", F.date_format("weather_hour_utc", utc)).collect()
        }
        cls.synoptic = weather.synoptic_hour_shares(cls.tables.retained)

    def test_each_station_uses_its_own_shift(self):
        self.assertEqual(self.by_label["ord-winter"].utc, "2025-01-15T18:53")
        self.assertEqual(self.by_label["ord-winter"].airport_code, "ORD")
        self.assertEqual(self.by_label["sju"].utc, "2025-01-15T16:53")
        self.assertEqual(self.by_label["phx-summer"].utc, "2025-07-15T19:51")
        self.assertEqual(self.by_label["storm"].utc, "2025-07-01T04:51")

    def test_unknown_station_is_quarantined_without_utc_time(self):
        record = self.by_label["unknown-station"]
        self.assertEqual(record.outcome, "quarantined")
        self.assertIn("W_KEY_INVALID", record.rejection_reasons)
        self.assertIsNone(record.observation_utc)

    def test_hourly_row_prefers_the_station_usual_minute(self):
        selected = self.hourly[(ORD, "2025-01-15T20:00")]
        self.assertEqual(selected.usual_report_minute, 53)
        self.assertEqual(selected.raw_date, "2025-01-15T14:53:00")
        self.assertEqual(selected.fm15_reports_in_hour, 2)
        self.assertEqual(self.hourly[(JFK, "2025-07-01T04:00")].usual_report_minute, 51)

    def test_thunderstorm_flag(self):
        for label, expected in {"storm": True, "vicinity-storm": True, "rain-only": False, "no-weather": False}.items():
            with self.subTest(label=label):
                self.assertIs(self.by_label[label].thunderstorm, expected)
        self.assertIn("thunderstorm", self.tables.hourly.columns)

    def test_synoptic_hour_shares(self):
        self.assertEqual(self.synoptic[ORD], {"fm12_reports": 2, "on_synoptic_hour": 1, "share": 0.5})
        self.assertNotIn(JFK, self.synoptic)


class YearPointerTests(SparkTestCase):
    def test_every_published_year_is_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "silver"
            for year, rows in (("2025", 3), ("2026", 2)):
                run = root / "weather_hourly" / "runs" / f"run-{year}"
                self.spark.range(rows).write.parquet(str(run))
                common.publish_ready(root, "weather_hourly", run / "_READY.json", {"run_id": f"run-{year}"},
                                     pointer_key=year)
            self.assertEqual(common.published_keys(root, "weather_hourly"), ["2025", "2026"])
            frame, pointers = common.read_ready_tables(self.spark, root, "weather_hourly")
            self.assertEqual(frame.count(), 5)
            self.assertEqual(pointers["2026"]["run_id"], "run-2026")

    def test_no_published_year_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "No published keys"):
                common.read_ready_tables(self.spark, Path(directory), "weather_hourly")


if __name__ == "__main__":
    unittest.main()
