"""Unit and Spark fixture tests for NOAA LCD v2 Silver weather cleaning and hourly selection."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from project07.silver import common, weather
from tests.spark_case import SparkTestCase


def weather_fixture(label: str, date_str: str, **changes) -> dict:
    """Create a synthetic 126-column NOAA LCD v2 row with REM carrying the fixture label."""
    row = dict.fromkeys(weather.RAW_FIELDS)
    row.update(
        STATION="USW00094789",
        DATE=date_str,
        REPORT_TYPE="FM-15",
        SOURCE="343",
        HourlyDryBulbTemperature="20.0",
        HourlyPrecipitation="0.0",
        HourlyWindSpeed="5.1",
        HourlyWindDirection="180",
        HourlyWindGustSpeed=None,
        HourlyVisibility="16.093",
        REM=label,
    )
    row.update(changes)
    return row


class WeatherSparkTests(SparkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        from pyspark import StorageLevel
        from pyspark.sql import functions as F

        rows = [
            # 1. Standard routine FM-15 observation (known METAR match from D5: 2025-07-15T00:51 LST -> 05:51 UTC)
            weather_fixture(
                "normal-jfk",
                "2025-07-15T00:51:00",
                HourlyDryBulbTemperature="24.4",
                HourlyPrecipitation="0.0",
                HourlyWindSpeed="4.6",
                HourlyWindDirection="210",
                HourlyVisibility="16.093",
            ),
            # 2. Negative temperature + trace precipitation + variable wind direction (LGA)
            weather_fixture(
                "winter-trace-vrb",
                "2025-01-20T06:51:00",
                STATION="USW00014732",
                HourlyDryBulbTemperature="-12.2",
                HourlyPrecipitation="T",
                HourlyWindSpeed="3.1",
                HourlyWindDirection="VRB",
            ),
            # 3. Missing optional measurements (blank precipitation, wind, visibility)
            weather_fixture(
                "missing-optionals",
                "2025-06-02T01:51:00",
                HourlyPrecipitation=" ",
                HourlyWindSpeed="",
                HourlyWindDirection=None,
                HourlyVisibility=None,
            ),
            # 4. Suspect 's' suffix and variable 'V' visibility suffix
            weather_fixture(
                "suspect-and-variable-vis",
                "2025-06-03T02:51:00",
                HourlyWindSpeed="17.0s",
                HourlyVisibility="0.402Vs",
            ),
            # 5. Sensor-missing '*' visibility and unparseable temperature token
            weather_fixture(
                "unparseable-tokens",
                "2025-06-04T03:51:00",
                HourlyDryBulbTemperature="bad-temp",
                HourlyVisibility="*",
            ),
            # 6. Physical range violation and gust below sustained wind speed
            weather_fixture(
                "physical-and-gust-warning",
                "2025-06-05T04:51:00",
                HourlyDryBulbTemperature="75.0",
                HourlyWindSpeed="15.0",
                HourlyWindGustSpeed="10.0",
            ),
            # 7. Month rollover between LST source date (June 30 21:51) and UTC hour (July 1 02:00)
            weather_fixture(
                "month-rollover",
                "2025-06-30T21:51:00",
                HourlyDryBulbTemperature="22.0",
            ),
            # 8 & 9. Two valid FM-15 reports in the same UTC hour (23:15 and 23:51 LST -> 04:15 and 04:51 UTC)
            weather_fixture(
                "multi-fm15-early",
                "2025-03-16T23:15:00",
                SOURCE="223",
                HourlyDryBulbTemperature="8.0",
            ),
            weather_fixture(
                "multi-fm15-routine",
                "2025-03-16T23:51:00",
                SOURCE="343",
                HourlyDryBulbTemperature="7.5",
            ),
            # 10 & 11. Non-FM-15 valid observation types (FM-16 SPECI and FM-12 SYNOP)
            weather_fixture(
                "speci-fm16",
                "2025-06-10T10:20:00",
                REPORT_TYPE="FM-16",
            ),
            weather_fixture(
                "synop-fm12",
                "2025-06-10T01:00:00",
                REPORT_TYPE="FM-12",
            ),
            # 12 & 13. Scope-excluded daily (SOD) and monthly (SOM) summary rows
            weather_fixture(
                "summary-sod",
                "2025-06-10T23:59:00",
                REPORT_TYPE="SOD",
                SOURCE="",
                HourlyDryBulbTemperature="",
                HourlyPrecipitation="",
                HourlyWindSpeed="",
                HourlyWindDirection="",
                HourlyVisibility="",
            ),
            weather_fixture(
                "summary-som",
                "2025-06-30T23:59:00",
                REPORT_TYPE="SOM",
                SOURCE="",
                HourlyDryBulbTemperature="",
                HourlyPrecipitation="",
                HourlyWindSpeed="",
                HourlyWindDirection="",
                HourlyVisibility="",
            ),
            # 14 & 15. Conflicting DST duplicate label (2025-03-08T21:51:00 with 5.0 C vs 4.4 C)
            weather_fixture(
                "dst-conflict-a",
                "2025-03-08T21:51:00",
                HourlyDryBulbTemperature="5.0",
            ),
            weather_fixture(
                "dst-conflict-b",
                "2025-03-08T21:51:00",
                HourlyDryBulbTemperature="4.4",
            ),
            # 16. Exact duplicate row (will be duplicated below)
            weather_fixture(
                "exact-duplicate",
                "2025-06-15T12:51:00",
            ),
            # 17. Missing key (blank DATE)
            weather_fixture(
                "key-missing",
                " ",
            ),
            # 18. Invalid station key
            weather_fixture(
                "invalid-station",
                "2025-06-16T12:51:00",
                STATION="USW99999999",
            ),
            # 19. Invalid timestamp key
            weather_fixture(
                "invalid-date",
                "2025-02-30T12:51:00",
            ),
            # 20. Malformed CSV record
            weather_fixture(
                "malformed-csv",
                "2025-06-17T12:51:00",
                _corrupt_record='"unclosed quote',
            ),
        ]
        # Append an exact copy of row 16 ("exact-duplicate")
        rows.append(dict(rows[15]))

        cls.fixture_rows = rows
        data = [[r[c] for c in weather.RAW_FIELDS] for r in rows]
        cls.raw = cls.spark.createDataFrame(data, weather.raw_schema())
        for name, val in zip(
            weather.PROVENANCE,
            (
                "noaa:USW00094789:2025",
                "fixtures_weather.csv",
                "b" * 64,
                "weather-fixture-run",
                weather.RULE_VERSION,
            ),
        ):
            cls.raw = cls.raw.withColumn(name, F.lit(val))

        def with_utc_strings(frame, *columns: str):
            for col_name in columns:
                frame = frame.withColumn(
                    f"{col_name}_str",
                    F.date_format(col_name, "yyyy-MM-dd'T'HH:mm:ss"),
                )
            return frame

        cls.tables = weather.clean_weather(cls.raw)
        cls.cached = cls.tables.classified.persist(StorageLevel.MEMORY_AND_DISK)
        cls.addClassCleanup(cls.cached.unpersist)
        # Format Spark TIMESTAMP columns in session timeZone (UTC) before collecting,
        # because PySpark converts collected timestamps using the host OS time zone.
        cls.records = with_utc_strings(cls.cached, "observation_utc", "weather_hour_utc").collect()
        cls.by_label = {r.original.REM: r for r in cls.records}
        cls.counts = cls.tables.audit.first().asDict()
        cls.hourly_rows = with_utc_strings(cls.tables.hourly, "weather_hour_utc").collect()

    def test_row_accounting_reconciles_with_scope_exclusions_and_duplicates(self) -> None:
        checks = common.check_accounting(self.counts, observed_raw=len(self.fixture_rows))
        self.assertTrue(all(checks.values()))
        self.assertEqual(self.counts["raw"], 21)
        self.assertEqual(self.counts["duplicate_extras"], 1)
        self.assertEqual(self.counts["unique"], 20)
        self.assertEqual(self.counts["scope_excluded"], 2)  # SOD and SOM
        self.assertEqual(self.counts["quarantined"], 6)  # 2 DST conflict + missing + bad station + bad date + malformed
        self.assertEqual(self.counts["retained"], 12)
        self.assertEqual(self.counts["W_SUMMARY_REPORT"], 2)
        self.assertEqual(self.counts["W_DST_LABEL_CONFLICT"], 2)

    def test_utc_conversion_adds_five_hours_to_lst(self) -> None:
        from pyspark.sql.types import TimestampNTZType, TimestampType

        self.assertIsInstance(self.cached.schema["observation_lst"].dataType, TimestampNTZType)
        self.assertIsInstance(self.cached.schema["observation_utc"].dataType, TimestampType)
        self.assertIsInstance(self.cached.schema["weather_hour_utc"].dataType, TimestampType)

        row = self.by_label["normal-jfk"]
        self.assertEqual(row.outcome, "retained")
        self.assertEqual(row.airport_code, "JFK")
        self.assertEqual(row.raw_date, "2025-07-15T00:51:00")
        self.assertEqual(row.observation_lst, datetime(2025, 7, 15, 0, 51, 0))
        self.assertEqual(row.observation_utc_str, "2025-07-15T05:51:00")
        self.assertEqual(row.weather_hour_utc_str, "2025-07-15T05:00:00")
        self.assertAlmostEqual(row.temperature_c, 24.4)
        self.assertAlmostEqual(row.precipitation_mm, 0.0)
        self.assertFalse(row.precipitation_trace)
        self.assertEqual(row.quality_flags, [])

    def test_negative_temperature_trace_precipitation_and_variable_wind(self) -> None:
        row = self.by_label["winter-trace-vrb"]
        self.assertEqual(row.outcome, "retained")
        self.assertEqual(row.airport_code, "LGA")
        self.assertAlmostEqual(row.temperature_c, -12.2)
        self.assertIsNone(row.precipitation_mm)
        self.assertTrue(row.precipitation_trace)
        self.assertIsNone(row.wind_direction_deg)
        self.assertTrue(row.wind_direction_variable)
        self.assertEqual(row.quality_flags, [])

    def test_missing_optional_measurements_remain_null_without_warnings(self) -> None:
        row = self.by_label["missing-optionals"]
        self.assertEqual(row.outcome, "retained")
        self.assertIsNone(row.precipitation_mm)
        self.assertIsNone(row.precipitation_trace)
        self.assertIsNone(row.wind_speed_mps)
        self.assertIsNone(row.wind_direction_deg)
        self.assertIsNone(row.wind_direction_variable)
        self.assertIsNone(row.visibility_km)
        self.assertIsNone(row.visibility_variable)
        self.assertEqual(row.quality_flags, [])

    def test_suspect_and_variable_visibility_suffixes_parsed_and_flagged(self) -> None:
        row = self.by_label["suspect-and-variable-vis"]
        self.assertEqual(row.outcome, "retained")
        self.assertAlmostEqual(row.wind_speed_mps, 17.0)
        self.assertAlmostEqual(row.visibility_km, 0.402)
        self.assertTrue(row.visibility_variable)
        self.assertEqual(row.quality_flags, ["W_SUSPECT_VALUE"])

    def test_unparseable_and_physical_range_warnings(self) -> None:
        unparseable = self.by_label["unparseable-tokens"]
        self.assertEqual(unparseable.outcome, "retained")
        self.assertIsNone(unparseable.temperature_c)
        self.assertIsNone(unparseable.visibility_km)
        self.assertEqual(unparseable.quality_flags, ["W_TOKEN_UNPARSEABLE"])

        phys = self.by_label["physical-and-gust-warning"]
        self.assertEqual(phys.outcome, "retained")
        self.assertEqual(
            set(phys.quality_flags),
            {"W_PHYSICAL_RANGE", "W_WIND_GUST_BELOW_SPEED"},
        )

    def test_summary_reports_are_scope_excluded_not_quarantined(self) -> None:
        for label in ("summary-sod", "summary-som"):
            row = self.by_label[label]
            self.assertEqual(row.outcome, "scope_excluded")
            self.assertEqual(row.scope_reasons, ["W_SUMMARY_REPORT"])
            self.assertEqual(row.rejection_reasons, [])

    def test_dst_conflicting_labels_and_invalid_keys_are_quarantined(self) -> None:
        for label in ("dst-conflict-a", "dst-conflict-b"):
            row = self.by_label[label]
            self.assertEqual(row.outcome, "quarantined")
            self.assertEqual(row.rejection_reasons, ["W_DST_LABEL_CONFLICT"])

        self.assertEqual(self.by_label["key-missing"].rejection_reasons, ["W_KEY_MISSING"])
        self.assertEqual(self.by_label["invalid-station"].rejection_reasons, ["W_KEY_INVALID"])
        self.assertEqual(self.by_label["invalid-date"].rejection_reasons, ["W_KEY_INVALID"])
        self.assertEqual(
            self.by_label["malformed-csv"].rejection_reasons, ["W_MALFORMED_RECORD"]
        )

    def test_hourly_table_uniqueness_deterministic_selection_and_utc_partitioning(self) -> None:
        # 12 retained observations: 2 are FM-16/FM-12, 10 are FM-15 across 9 distinct UTC hours
        self.assertEqual(len(self.hourly_rows), 9)
        keys = [(r.station_id, r.weather_hour_utc_str) for r in self.hourly_rows]
        self.assertEqual(len(keys), len(set(keys)))

        by_hour = {r.weather_hour_utc_str: r for r in self.hourly_rows}
        # In the 2025-03-17 04:00 UTC hour, the :51 routine report (7.5 C) must win over :15 (8.0 C)
        multi = by_hour["2025-03-17T04:00:00"]
        self.assertEqual(multi.fm15_reports_in_hour, 2)
        self.assertEqual(multi.raw_date, "2025-03-16T23:51:00")
        self.assertAlmostEqual(multi.temperature_c, 7.5)

        # Month rollover: 2025-06-30T21:51 LST is partitioned in June in observations,
        # but 2025-07-01 02:00 UTC is partitioned in July in weather_hourly.
        obs_rollover = self.by_label["month-rollover"]
        self.assertEqual((obs_rollover.year, obs_rollover.month), (2025, 6))
        hourly_rollover = by_hour["2025-07-01T02:00:00"]
        self.assertEqual((hourly_rollover.year, hourly_rollover.month), (2025, 7))

    def test_parquet_roundtrip_for_observations_quarantine_and_hourly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            obs_ok = common.write_parquet_and_verify(
                self.spark,
                self.tables.retained,
                root / "obs",
                expected_count=self.counts["retained"],
                partition_by=("year", "month"),
            )
            quar_ok = common.write_parquet_and_verify(
                self.spark,
                self.tables.quarantined,
                root / "quar",
                expected_count=self.counts["quarantined"],
            )
            hourly_ok = common.write_parquet_and_verify(
                self.spark,
                self.tables.hourly,
                root / "hourly",
                expected_count=len(self.hourly_rows),
                partition_by=("year", "month"),
            )
            self.assertTrue(obs_ok)
            self.assertTrue(quar_ok)
            self.assertTrue(hourly_ok)


if __name__ == "__main__":
    unittest.main()

