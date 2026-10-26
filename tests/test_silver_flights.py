"""Flight fixtures plus tests for the temporary Bronze catalog adapter."""

import csv
import io
import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from project07.silver import flights


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.archive = self.root / "archive.zip"
        self.archive.write_bytes(b"archive fixture, not opened by the catalog reader")
        self.csv = self.root / "flights.csv"
        self.csv.write_bytes(b"fixture bytes")

    def record(self, path):
        return {"path": path.name, "sha256": flights.file_hash(path),
                "size_bytes": path.stat().st_size}

    def catalog(self, version=None):
        version = version or {**self.record(self.archive), "kind": "bts",
                              "csvs": [self.record(self.csv)]}
        payload = {"schema_version": 1, "assets": {"bts:2025-06": [version]}}
        (self.root / "catalog.json").write_text(json.dumps(payload))
        return payload

    def test_selected_asset_signature_and_original_files_unchanged(self):
        self.catalog()
        before = {p: p.read_bytes() for p in (self.csv, self.archive, self.root / "catalog.json")}
        selected = flights.select_inputs(self.root, ["bts:2025-06"])[0]
        self.assertEqual(selected.asset_id, "bts:2025-06")
        self.assertEqual(selected.kind, "bts")
        self.assertEqual(selected.version_sha256, flights.file_hash(self.archive))
        self.assertEqual(selected.csvs, [flights.SelectedCsv(
            self.csv.resolve(), flights.file_hash(self.csv), self.csv.stat().st_size)])
        for path, content in before.items():
            self.assertEqual(path.read_bytes(), content)

    def test_last_version_is_selected(self):
        payload = self.catalog()
        other = self.root / "revised.csv"
        other.write_bytes(b"new version")
        payload["assets"]["bts:2025-06"].append({**payload["assets"]["bts:2025-06"][0],
                                                 "csvs": [self.record(other)]})
        (self.root / "catalog.json").write_text(json.dumps(payload))
        self.assertEqual(flights.select_inputs(self.root, ["bts:2025-06"])[0].csvs[0].path, other.resolve())

    def test_missing_asset_and_unknown_schema_fail(self):
        payload = self.catalog()
        with self.assertRaisesRegex(ValueError, "Missing Bronze asset"):
            flights.select_inputs(self.root, ["bts:2025-07"])
        payload["schema_version"] = 2
        (self.root / "catalog.json").write_text(json.dumps(payload))
        with self.assertRaisesRegex(ValueError, "schema"):
            flights.select_inputs(self.root, ["bts:2025-06"])

    def test_checksum_and_size_mismatch_fail(self):
        self.catalog()
        self.csv.write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError, "mismatch"):
            flights.select_inputs(self.root, ["bts:2025-06"])
        self.catalog()
        self.csv.write_bytes(b"BAD")
        with self.assertRaisesRegex(ValueError, "mismatch"):
            flights.select_inputs(self.root, ["bts:2025-06"])

    def test_unsafe_paths_and_duplicate_selection_fail(self):
        payload = self.catalog()
        payload["assets"]["bts:2025-06"][0]["csvs"][0]["path"] = "../outside.csv"
        (self.root / "catalog.json").write_text(json.dumps(payload))
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            flights.select_inputs(self.root, ["bts:2025-06"])
        with self.assertRaisesRegex(ValueError, "distinct"):
            flights.select_inputs(self.root, ["bts:2025-06", "bts:2025-06"])

    def test_failed_atomic_pointer_replacement_preserves_previous_value(self):
        pointer = self.root / "current.json"
        flights._atomic_json(pointer, {"run_id": "old"})
        with patch("project07.silver.flights.os.replace", side_effect=OSError("fixture failure")):
            with self.assertRaises(OSError):
                flights._atomic_json(pointer, {"run_id": "new"})
        self.assertEqual(json.loads(pointer.read_text()), {"run_id": "old"})
        self.assertEqual(sorted(p.name for p in self.root.iterdir()),
                         ["archive.zip", "current.json", "flights.csv"])

    def test_publishing_one_month_keeps_other_months_current(self):
        output = self.root / "silver" / "dev" / "alkval"
        june, july = (flights.current_pointer(output, m) for m in ("2025-06", "2025-07"))
        self.assertEqual(june, output / "flights" / "current" / "2025-06.json")
        flights._atomic_json(june, {"month": "2025-06", "run_id": "june-run"})
        flights._atomic_json(july, {"month": "2025-07", "run_id": "july-run"})
        self.assertEqual(json.loads(june.read_text())["run_id"], "june-run")
        self.assertEqual(json.loads(july.read_text())["run_id"], "july-run")
        with self.assertRaises(Exception):
            flights.current_pointer(output, "2025-6")

    def test_accounting_failure_is_not_accepted(self):
        counts = {"raw": 3, "duplicate_extras": 1, "unique": 2,
                  "scope_excluded": 0, "retained": 1, "quarantined": 1}
        self.assertTrue(all(flights.check_accounting(counts, 3).values()))
        with self.assertRaisesRegex(ValueError, "accounting"):
            flights.check_accounting(counts, 4)

    def test_protected_output_roots_and_symlink_redirects_fail(self):
        for path in (flights.DEFAULT_DATA_ROOT / "bronze",
                     flights.DEFAULT_DATA_ROOT / "silver" / "prod",
                     flights.DEFAULT_DATA_ROOT / "silver" / "dev" / "ckolle"):
            with self.assertRaisesRegex(ValueError, "Data root"):
                flights.dev_output_root(path)
        output = self.root / "silver" / "dev" / "alkval"
        output.parent.mkdir(parents=True)
        output.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "Symlink"):
            flights.dev_output_root(self.root)


def fixture(label, number, **changes):
    row = dict.fromkeys(flights.RAW_FIELDS)
    row.update(FlightDate="2025-06-01", Reporting_Airline="AA",
               Flight_Number_Reporting_Airline=str(number), Origin="JFK", Dest="LGA",
               Tail_Number=label, OriginCityName="New York, NY", CRSDepTime="1000",
               DepTime="1005", DepDelay="5.00", DepDelayMinutes="5.00",
               CRSArrTime="1200", ArrTime="1205", ArrDelay="5.00", ArrDelayMinutes="5.00",
               Cancelled="0.00", Diverted="0.00", CRSElapsedTime="120.00",
               ActualElapsedTime="120.00", AirTime="90.00", TaxiIn="10.00", TaxiOut="20.00")
    row.update(changes)
    if "DepDelay" in changes and "DepDelayMinutes" not in changes:
        # Keep the clipped delay consistent with BTS unless a fixture tests it.
        try:
            row["DepDelayMinutes"] = f"{max(float(changes['DepDelay']), 0):.2f}"
        except (TypeError, ValueError):
            row["DepDelayMinutes"] = None if changes["DepDelay"] is None else row["DepDelayMinutes"]
    return row


class FlightSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark import StorageLevel
        from pyspark.sql import SparkSession, functions as F

        cls.spark = (SparkSession.builder.master("local[2]").appName("project07-flight-fixtures")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.ansi.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")
        cls.addClassCleanup(cls.spark.stop)
        rows = [
            fixture("normal", 1),
            fixture("blank-key", 2, Origin=" "),
            fixture("bad-flag", 3, Cancelled="2.00"),
            fixture("bad-number", 4, DepDelay="not-a-number"),
            fixture("negative", 5, DepTime="0955", DepDelay="-5.00", DepDelayMinutes="0.00"),
            fixture("previous-day", 6, CRSDepTime="0010", DepTime="2355", DepDelay="-15.00"),
            fixture("scheduled-2400", 7, CRSDepTime="2400", DepTime="0005"),
            fixture("actual-2400", 8, CRSDepTime="2350", DepTime="2400", DepDelay="10.00"),
            fixture("overnight", 9, CRSDepTime="2350", DepTime="0010", DepDelay="20.00"),
            fixture("multi-day", 10, DepTime="1600", DepDelay="1800.00"),
            fixture("month-rollover", 11, FlightDate="2025-06-30", CRSDepTime="2350", DepTime="0010", DepDelay="20.00"),
            fixture("year-rollover", 12, FlightDate="2025-12-31", CRSDepTime="2350", DepTime="0010", DepDelay="20.00"),
            fixture("cancelled-no-departure", 13, Cancelled="1.00", CancellationCode="A", DepTime=None,
                    DepDelay=None, ArrTime=None, ArrDelay=None, ActualElapsedTime=None,
                    AirTime=None, TaxiIn=None, TaxiOut=None),
            fixture("cancelled-departed-no-delay", 14, Cancelled="1.00", CancellationCode="B", DepDelay=None),
            fixture("cancelled-departed", 15, Cancelled="1.00", CancellationCode="A"),
            fixture("diverted", 16, Diverted="1.00", ArrTime=None, ArrDelay=None,
                    ActualElapsedTime=None, AirTime=None, TaxiIn=None),
            fixture("duplicate", 17),
            fixture("duplicate-invalid", 18, Origin=None),
            fixture("conflict-a", 19),
            fixture("conflict-b", 19, DepTime="1010", DepDelay="10.00"),
            fixture("duration-mismatch", 20, ActualElapsedTime="121.00"),
            fixture("many-errors", 21, Origin="12", Cancelled="2.00"),
            fixture("missing-completed", 22, ArrTime=None),
            fixture("clock-mismatch", 23, DepTime="1010"),
            fixture("state-conflict", 24, Cancelled="1.00", Diverted="1.00", CancellationCode="A"),
            fixture("cancel-code-mismatch", 25, CancellationCode="A"),
            fixture("malformed", 26, _corrupt_record='"unterminated'),
            fixture("trailing-value", 27, _trailing_empty="unexpected"),
            fixture("bad-date", 28, FlightDate="2025-02-30"),
            fixture("overflow", 29, DepDelay="2147483648.00"),
            fixture("invalid-clock", 30, DepTime="2561"),
            fixture("pruned-difference", 31, CarrierDelay=None),
            fixture("pruned-difference", 31, CarrierDelay="null"),
            # Air time is negative but the components still sum to the elapsed time.
            fixture("negative-duration", 32, AirTime="-10.00", TaxiOut="120.00"),
            fixture("clip-mismatch", 33, DepDelayMinutes="0.00"),
            fixture("negative-clipped", 34, DepTime="0955", DepDelay="-5.00", DepDelayMinutes="-5.00"),
        ]
        rows.extend([dict(rows[16]), dict(rows[17])])
        cls.fixture_rows = rows
        data = [[row[c] for c in flights.RAW_FIELDS] for row in rows]
        cls.raw = cls.spark.createDataFrame(data, flights.raw_schema())
        for name, value in zip(flights.PROVENANCE,
                               ("bts:2025-06", "fixtures.csv", "a" * 64, "fixture-run", flights.RULE_VERSION)):
            cls.raw = cls.raw.withColumn(name, F.lit(value))
        cls.tables = flights.clean_flights(cls.raw)
        cls.cached = cls.tables.classified.persist(StorageLevel.MEMORY_AND_DISK)
        cls.addClassCleanup(cls.cached.unpersist)
        cls.records = cls.cached.collect()
        cls.by_label = {}
        for row in cls.records:
            cls.by_label.setdefault(row.tail_number, []).append(row)
        cls.counts = cls.tables.audit.first().asDict()

    def row(self, label):
        self.assertEqual(len(self.by_label[label]), 1)
        return self.by_label[label][0]

    def outcome(self, label, reasons=(), warnings=()):
        row = self.row(label)
        self.assertEqual(row.outcome, "quarantined" if reasons else "retained")
        self.assertEqual(set(row.rejection_reasons), set(reasons))
        self.assertEqual(set(row.quality_flags), set(warnings))
        return row

    def test_normal_flight(self):
        row = self.outcome("normal")
        self.assertEqual(row.flight_date, date(2025, 6, 1))
        self.assertEqual(row.dep_delay_min, 5)
        self.assertEqual(row.actual_dep_local, datetime(2025, 6, 1, 10, 5))
        self.assertEqual(row.reporting_airline, "AA")
        self.assertEqual(row.flight_number, "1")
        self.assertIs(row.cancelled, False)
        self.assertEqual(row.dep_day_offset, 0)

    def test_blank_key_and_invalid_flag(self):
        self.outcome("blank-key", ["F_KEY_MISSING"])
        self.outcome("bad-flag", ["F_FLAG_INVALID"])

    def test_malformed_number_and_overflow_become_null_with_warnings(self):
        for label in ("bad-number", "overflow"):
            row = self.outcome(label, warnings=["F_NUMERIC_UNPARSEABLE", "F_COMPLETED_FIELD_MISSING"])
            self.assertIsNone(row.dep_delay_min)
            self.assertIsNone(row.actual_dep_local)

    def test_negative_delay_and_previous_date(self):
        self.assertEqual(self.outcome("negative").actual_dep_local, datetime(2025, 6, 1, 9, 55))
        row = self.outcome("previous-day")
        self.assertEqual(row.actual_dep_local, datetime(2025, 5, 31, 23, 55))
        self.assertEqual(row.dep_day_offset, -1)
        self.assertEqual((row.year, row.month), (2025, 6))

    def test_2400_anchors_scheduled_midnight_and_normalizes_actual_clock(self):
        row = self.outcome("scheduled-2400")
        self.assertEqual(row.scheduled_dep_local, datetime(2025, 6, 2))
        self.assertEqual(row.actual_dep_local, datetime(2025, 6, 2, 0, 5))
        row = self.outcome("actual-2400")
        self.assertEqual(row.actual_dep_local, datetime(2025, 6, 2))
        self.assertEqual(row.dep_day_offset, 1)

    def test_overnight_multi_day_month_and_year_boundaries(self):
        for label, timestamp in {
            "overnight": datetime(2025, 6, 2, 0, 10),
            "multi-day": datetime(2025, 6, 2, 16, 0),
            "month-rollover": datetime(2025, 7, 1, 0, 10),
            "year-rollover": datetime(2026, 1, 1, 0, 10),
        }.items():
            row = self.outcome(label)
            self.assertEqual(row.actual_dep_local, timestamp)
            self.assertEqual(row.dep_day_offset, 1)
        self.assertEqual(self.row("year-rollover").month, 12)

    def test_cancellations_with_and_without_departure(self):
        self.outcome("cancelled-no-departure")
        self.outcome("cancelled-departed")
        row = self.outcome("cancelled-departed-no-delay", warnings=["F_CANCELLED_DEPTIME_NO_DELAY"])
        self.assertIsNone(row.actual_dep_local)
        self.assertIsNone(row.dep_day_offset)

    def test_diversion_nulls_are_not_completed_flight_errors(self):
        self.outcome("diverted")

    def test_exact_duplicate_and_invalid_duplicate_accounting(self):
        from pyspark.sql.types import LongType

        self.assertEqual(self.row("duplicate")._occurrences, 2)
        row = self.outcome("duplicate-invalid", ["F_KEY_MISSING"])
        self.assertEqual(row._occurrences, 2)
        self.assertEqual(self.counts["duplicate_extras"], 2)
        for table, expected in (
            (self.tables.retained, {"duplicate": 2, "normal": 1}),
            (self.tables.quarantined, {"duplicate-invalid": 2, "blank-key": 1}),
        ):
            self.assertIsInstance(table.schema["source_occurrences"].dataType, LongType)
            self.assertNotIn("_occurrences", table.columns)
            actual = {row.tail_number: row.source_occurrences
                      for row in table.select("tail_number", "source_occurrences").collect()}
            for label, occurrences in expected.items():
                self.assertEqual(actual[label], occurrences)

    def test_key_conflicts_quarantine_every_representative(self):
        for label in ("conflict-a", "conflict-b"):
            self.outcome(label, ["F_KEY_CONFLICT"])

    def test_dedup_uses_pruned_fields_and_hash_preserves_null(self):
        rows = self.by_label["pruned-difference"]
        self.assertEqual(len(rows), 2)
        self.assertEqual(len({r.record_hash for r in rows}), 2)
        for row in rows:
            self.assertEqual(row.rejection_reasons, ["F_KEY_CONFLICT"])
            self.assertEqual(row._occurrences, 1)

    def test_duration_clock_missing_fields_and_code_warnings(self):
        for label, code in {
            "duration-mismatch": "F_DURATION_MISMATCH",
            "clock-mismatch": "F_CLOCK_DELAY_MISMATCH",
            "missing-completed": "F_COMPLETED_FIELD_MISSING",
            "cancel-code-mismatch": "F_CANCEL_CODE_MISMATCH",
        }.items():
            self.outcome(label, warnings=[code])

    def test_minute_domain_violations_are_flagged_but_retained(self):
        for label in ("negative-duration", "clip-mismatch", "negative-clipped"):
            self.outcome(label, warnings=["F_MINUTE_DOMAIN"])
        # A valid early departure clips to zero and is not flagged.
        self.assertEqual(self.outcome("negative").dep_delay_nonnegative_min, 0)
        self.assertEqual(self.counts["F_MINUTE_DOMAIN"], 3)

    def test_malformed_record_and_trailing_field(self):
        row = self.outcome("malformed", ["F_MALFORMED_RECORD"])
        self.assertEqual(row.original._corrupt_record, '"unterminated')
        self.outcome("trailing-value", ["F_MALFORMED_RECORD"])

    def test_invalid_date_and_clock(self):
        self.outcome("bad-date", ["F_KEY_INVALID"])
        self.outcome("invalid-clock", warnings=["F_NUMERIC_UNPARSEABLE", "F_COMPLETED_FIELD_MISSING"])

    def test_state_conflict_and_overlapping_rules(self):
        self.outcome("state-conflict", ["F_STATE_CONFLICT"])
        self.outcome("many-errors", ["F_KEY_INVALID", "F_FLAG_INVALID"])
        self.assertEqual(self.counts["F_KEY_INVALID"], 2)
        self.assertEqual(self.counts["F_FLAG_INVALID"], 2)

    def test_every_fixture_has_a_defined_outcome_and_counts_reconcile(self):
        self.assertTrue(all(flights.check_accounting(self.counts, len(self.fixture_rows)).values()))
        self.assertEqual(self.counts["unique"], len(self.records))
        self.assertEqual(self.counts["scope_excluded"], 0)
        for row in self.records:
            self.assertEqual(bool(row.rejection_reasons), row.outcome == "quarantined")
            self.assertTrue(set(row.quality_flags).issubset(flights.RULES))

    def test_local_datetimes_are_timestamp_ntz(self):
        from pyspark.sql.types import TimestampNTZType

        for name in ("scheduled_dep_local", "actual_dep_local"):
            self.assertIsInstance(self.tables.classified.schema[name].dataType, TimestampNTZType)

    def test_reader_preserves_quoted_commas_and_exact_catalog_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / "flights_(2025_6).csv"
            row = fixture("quoted", 99)
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow([*flights.BTS_COLUMNS, ""])
            writer.writerow([row[name] for name in (*flights.BTS_COLUMNS, "_trailing_empty")])
            path.write_text(buffer.getvalue())
            source = flights.SelectedCsv(path, flights.file_hash(path), path.stat().st_size)
            asset = flights.SelectedAsset("bts:2025-06", "bts", "a" * 64, [source])
            frame = flights.read_flights(self.spark, [asset], root, "reader-test")
            actual = frame.first()
            self.assertEqual(actual.OriginCityName, "New York, NY")
            self.assertEqual(actual.source_file, path.name)
            self.assertEqual(actual.source_sha256, source.sha256)
            self.assertIsNone(actual._corrupt_record)

    def test_reader_handles_doubled_quotes_without_shifting_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / "doubled_quotes.csv"
            row = fixture("doubled-quote", 98, OriginCityName='Say "Hi", NY')
            buffer = io.StringIO()
            writer = csv.writer(buffer)  # Writes the embedded quotes as "".
            writer.writerow([*flights.BTS_COLUMNS, ""])
            writer.writerow([row[name] for name in (*flights.BTS_COLUMNS, "_trailing_empty")])
            path.write_text(buffer.getvalue())
            source = flights.SelectedCsv(path, flights.file_hash(path), path.stat().st_size)
            asset = flights.SelectedAsset("bts:2025-06", "bts", "a" * 64, [source])
            actual = flights.read_flights(self.spark, [asset], root, "quote-test").first()
            self.assertEqual(actual.OriginCityName, 'Say "Hi", NY')
            self.assertEqual((actual.Origin, actual.Dest, actual.CRSDepTime), ("JFK", "LGA", "1000"))
            self.assertIsNone(actual._trailing_empty)
            self.assertIsNone(actual._corrupt_record)

    def test_reader_rejects_short_and_long_lines_with_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / "wrong_width.csv"
            row = fixture("width", 99)
            values = [row[name] for name in (*flights.BTS_COLUMNS, "_trailing_empty")]
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow([*flights.BTS_COLUMNS, ""])
            writer.writerow(values)
            writer.writerow(values[:-1])
            writer.writerow([*values, "extra"])
            path.write_text(buffer.getvalue())
            source = flights.SelectedCsv(path, flights.file_hash(path), path.stat().st_size)
            asset = flights.SelectedAsset("bts:2025-06", "bts", "a" * 64, [source])
            with self.assertRaisesRegex(ValueError, "2 lines deviate from 110 fields"):
                flights.read_flights(self.spark, [asset], root, "width-test")

    def test_retained_parquet_preserves_partition_types_and_occurrences(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, LongType

        for name in ("year", "month"):
            self.assertIsInstance(self.tables.retained.schema[name].dataType, IntegerType)
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "retained")
            self.tables.retained.write.mode("errorifexists").partitionBy("year", "month").parquet(path)
            readback = self.spark.read.parquet(path)
            self.assertEqual(readback.count(), self.counts["retained"])
            for name in ("year", "month"):
                self.assertIsInstance(readback.schema[name].dataType, IntegerType)
            self.assertIsInstance(readback.schema["source_occurrences"].dataType, LongType)
            duplicate = readback.filter(F.col("tail_number") == "duplicate").first()
            self.assertEqual((duplicate.year, duplicate.month, duplicate.source_occurrences), (2025, 6, 2))
            self.assertEqual(readback.filter(F.col("tail_number") == "normal").first().source_occurrences, 1)

    def test_quarantine_parquet_preserves_original_values(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType

        with tempfile.TemporaryDirectory() as directory:
            self.tables.quarantined.write.mode("errorifexists").parquet(str(Path(directory) / "quarantine"))
            readback = self.spark.read.parquet(str(Path(directory) / "quarantine"))
            self.assertEqual(readback.count(), self.counts["quarantined"])
            row = readback.filter(F.col("tail_number") == "malformed").first()
            self.assertEqual(row.original._corrupt_record, '"unterminated')
            self.assertEqual(row.rejection_reasons, ["F_MALFORMED_RECORD"])
            self.assertEqual(row.source_occurrences, 1)
            self.assertIsInstance(readback.schema["source_occurrences"].dataType, LongType)
            duplicate = readback.filter(F.col("tail_number") == "duplicate-invalid").first()
            self.assertEqual(duplicate.source_occurrences, 2)


if __name__ == "__main__":
    unittest.main()
