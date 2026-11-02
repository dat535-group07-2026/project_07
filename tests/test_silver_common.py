"""Tests for shared Silver configuration, Spark session, catalog reader, raw reader and publication."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from project07 import config, spark as spark_mod
from project07.silver import common
from tests.spark_case import SparkTestCase


class ConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def test_dev_and_prod_paths_resolve_cleanly(self) -> None:
        dev_cfg = config.resolve_config(env="dev", data_root=self.root, dev_user="christopher")
        self.assertEqual(dev_cfg.bronze_root, self.root / "bronze")
        self.assertEqual(dev_cfg.silver_root, self.root / "silver" / "dev" / "christopher")
        self.assertEqual(dev_cfg.gold_root, self.root / "gold" / "dev" / "christopher")

        prod_cfg = config.resolve_config(env="prod", data_root=self.root)
        self.assertEqual(prod_cfg.silver_root, self.root / "silver" / "prod")
        self.assertEqual(prod_cfg.gold_root, self.root / "gold" / "prod")

    def test_dev_user_defaults_to_env_and_prod_rejects_dev_user(self) -> None:
        with patch.dict(os.environ, {"USER": "testuser"}):
            cfg = config.resolve_config(env="dev", data_root=self.root)
            self.assertEqual(cfg.dev_user, "testuser")

        with self.assertRaisesRegex(ValueError, "dev-user"):
            config.resolve_config(env="prod", data_root=self.root, dev_user="christopher")

        with self.assertRaisesRegex(ValueError, "Invalid dev_user"):
            config.resolve_config(env="dev", data_root=self.root, dev_user="../escape")

    def test_symlinked_output_paths_are_rejected(self) -> None:
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        (self.root / "silver").mkdir()
        (self.root / "silver" / "dev").symlink_to(elsewhere, target_is_directory=True)
        cfg = config.resolve_config(env="dev", data_root=self.root, dev_user="christopher")
        with self.assertRaisesRegex(ValueError, "symlink"):
            _ = cfg.silver_root

    def test_add_config_args_parses_cli_flags(self) -> None:
        parser = argparse.ArgumentParser()
        config.add_config_args(parser)
        args = parser.parse_args(
            ["--env", "dev", "--data-root", str(self.root), "--dev-user", "christopher"]
        )
        cfg = config.resolve_config(args.env, args.data_root, args.dev_user)
        self.assertEqual(cfg.silver_root, self.root / "silver" / "dev" / "christopher")


class SparkEnvTests(unittest.TestCase):
    def test_check_spark_env_rejects_pyspark_driver_python_and_bad_java_home(self) -> None:
        with patch.dict(os.environ, {"PYSPARK_DRIVER_PYTHON": "jupyter"}, clear=False):
            with self.assertRaisesRegex(ValueError, "PYSPARK_DRIVER_PYTHON"):
                spark_mod.check_spark_env()

        with patch.dict(
            os.environ,
            {
                "PYSPARK_DRIVER_PYTHON": "",
                "PYSPARK_DRIVER_PYTHON_OPTS": "",
                "JAVA_HOME": "/nonexistent/java",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "JAVA_HOME"):
                spark_mod.check_spark_env()

    @unittest.skipUnless(spark_mod.VM_JAVA8_HOME.exists(), "VM Java 8 path not present")
    def test_check_spark_env_accepts_vm_java8(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PYSPARK_DRIVER_PYTHON": "",
                "PYSPARK_DRIVER_PYTHON_OPTS": "",
                "JAVA_HOME": str(spark_mod.VM_JAVA8_HOME),
            },
            clear=False,
        ):
            spark_mod.check_spark_env()


class CatalogAndPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.archive = self.root / "archive.zip"
        self.archive.write_bytes(b"zip archive bytes")
        self.bts_csv = self.root / "flights.csv"
        self.bts_csv.write_bytes(b"bts csv bytes")
        self.noaa_csv = self.root / "LCD_USW00094789_2025.csv"
        self.noaa_csv.write_bytes(b"STATION,DATE\nUSW00094789,2025-06-01T00:51:00\n")

    def _record(self, path: Path) -> dict:
        return {
            "path": path.name,
            "sha256": common.file_hash(path),
            "size_bytes": path.stat().st_size,
        }

    def _write_catalog(self, payload: dict | None = None) -> dict:
        if payload is None:
            payload = {
                "schema_version": 1,
                "assets": {
                    "bts:2025-06": [
                        {
                            **self._record(self.archive),
                            "kind": "bts",
                            "csvs": [self._record(self.bts_csv)],
                        }
                    ],
                    "noaa:USW00094789:2025": [
                        {
                            **self._record(self.noaa_csv),
                            "kind": "noaa",
                            "csvs": [],
                        }
                    ],
                },
            }
        (self.root / "catalog.json").write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def test_select_inputs_supports_bts_and_noaa_without_mutating_files(self) -> None:
        self._write_catalog()
        before = {
            p: p.read_bytes()
            for p in (self.archive, self.bts_csv, self.noaa_csv, self.root / "catalog.json")
        }
        selected = common.select_inputs(
            self.root, ["bts:2025-06", "noaa:USW00094789:2025"]
        )
        self.assertEqual(len(selected), 2)
        self.assertEqual(selected[0].asset_id, "bts:2025-06")
        self.assertEqual(selected[0].kind, "bts")
        self.assertEqual(selected[0].version_sha256, common.file_hash(self.archive))
        self.assertEqual(selected[0].csvs[0].path, self.bts_csv.resolve())

        self.assertEqual(selected[1].asset_id, "noaa:USW00094789:2025")
        self.assertEqual(selected[1].kind, "noaa")
        self.assertEqual(selected[1].version_sha256, common.file_hash(self.noaa_csv))
        self.assertEqual(selected[1].csvs[0].path, self.noaa_csv.resolve())

        for path, raw_bytes in before.items():
            self.assertEqual(path.read_bytes(), raw_bytes)

    def test_select_inputs_picks_latest_version_and_rejects_invalid_catalog(self) -> None:
        payload = self._write_catalog()
        revised = self.root / "LCD_USW00094789_2025_v2.csv"
        revised.write_bytes(b"revised weather csv")
        payload["assets"]["noaa:USW00094789:2025"].append(
            {**self._record(revised), "kind": "noaa", "csvs": []}
        )
        self._write_catalog(payload)
        selected = common.select_inputs(self.root, ["noaa:USW00094789:2025"])[0]
        self.assertEqual(selected.csvs[0].path, revised.resolve())

        with self.assertRaisesRegex(ValueError, "Missing Bronze asset"):
            common.select_inputs(self.root, ["noaa:USW00014732:2025"])

        with self.assertRaisesRegex(ValueError, "distinct"):
            common.select_inputs(self.root, ["bts:2025-06", "bts:2025-06"])

        payload["schema_version"] = 2
        self._write_catalog(payload)
        with self.assertRaisesRegex(ValueError, "schema"):
            common.select_inputs(self.root, ["bts:2025-06"])

    def test_select_inputs_rejects_unsafe_paths_and_hash_mismatches(self) -> None:
        payload = self._write_catalog()
        self.noaa_csv.write_bytes(b"corrupted content")
        with self.assertRaisesRegex(ValueError, "mismatch"):
            common.select_inputs(self.root, ["noaa:USW00094789:2025"])

        payload = self._write_catalog()
        payload["assets"]["noaa:USW00094789:2025"][0]["path"] = "../outside.csv"
        self._write_catalog(payload)
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            common.select_inputs(self.root, ["noaa:USW00094789:2025"])

    def test_check_accounting_enforces_both_equations(self) -> None:
        valid = {
            "raw": 10,
            "duplicate_extras": 2,
            "unique": 8,
            "scope_excluded": 3,
            "retained": 4,
            "quarantined": 1,
        }
        checks = common.check_accounting(valid, observed_raw=10)
        self.assertTrue(all(checks.values()))

        with self.assertRaisesRegex(ValueError, "accounting failed"):
            common.check_accounting(valid, observed_raw=9)

        bad_unique = {**valid, "retained": 5}
        with self.assertRaisesRegex(ValueError, "accounting failed"):
            common.check_accounting(bad_unique, observed_raw=10)

    def test_publish_ready_writes_manifest_then_pointer_atomically(self) -> None:
        silver_root = self.root / "silver" / "dev" / "christopher"
        pointer = common.current_pointer(silver_root, "weather_observations")
        common.atomic_json(pointer, {"table": "weather_observations", "run_id": "old-run"})

        manifest = silver_root / "weather_observations" / "runs" / "new-run" / "_READY.json"
        report = {"run_id": "new-run", "status": "ready"}

        with patch("project07.silver.common.atomic_json", side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                common.publish_ready(silver_root, "weather_observations", manifest, report)
        self.assertEqual(json.loads(pointer.read_text(encoding="utf-8"))["run_id"], "old-run")

        order: list[Path] = []
        real_atomic = common.atomic_json
        with patch(
            "project07.silver.common.atomic_json",
            side_effect=lambda p, data: (order.append(Path(p)), real_atomic(p, data)),
        ):
            common.publish_ready(silver_root, "weather_observations", manifest, report)

        self.assertEqual(order, [manifest, pointer])
        pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
        self.assertEqual(pointer_payload["run_id"], "new-run")
        self.assertEqual(pointer_payload["ready_manifest"], "runs/new-run/_READY.json")


class CommonSparkTests(SparkTestCase):
    def test_session_conventions_enforced(self) -> None:
        self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")
        self.assertEqual(self.spark.conf.get("spark.sql.ansi.enabled").lower(), "false")
        common.verify_session_conventions(self.spark)

    def test_read_raw_csvs_and_deduplicate_with_provenance(self) -> None:
        columns = ("STATION", "DATE", "REPORT_TYPE", "HourlyDryBulbTemperature", "REM")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            csv_path = root / "weather.csv"
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            writer.writerow(["USW00094789", "2025-06-01T00:51:00", "FM-15", "18.3", 'METAR "OK", JFK'])
            writer.writerow(["USW00094789", "2025-06-01T00:51:00", "FM-15", "18.3", 'METAR "OK", JFK'])
            writer.writerow(["USW00094789", "2025-06-01T01:51:00", "FM-15", "", "null"])
            csv_path.write_text(buf.getvalue(), encoding="utf-8")

            source = common.SelectedCsv(
                csv_path, common.file_hash(csv_path), csv_path.stat().st_size
            )
            asset = common.SelectedAsset(
                "noaa:USW00094789:2025", "noaa", source.sha256, [source]
            )
            raw = common.read_raw_csvs(
                self.spark,
                [asset],
                root,
                run_id="run-001",
                rule_version="test-1.0",
                columns=columns,
                expected_kind="noaa",
                trailing_empty=False,
            )
            self.assertEqual(raw.count(), 3)
            raw_fields = (*columns, "_corrupt_record")
            deduped = common.deduplicate_with_provenance(raw, raw_fields)
            rows = {r.DATE: r for r in deduped.collect()}
            self.assertEqual(len(rows), 2)
            first = rows["2025-06-01T00:51:00"]
            self.assertEqual(first._occurrences, 2)
            self.assertEqual(first.REM, 'METAR "OK", JFK')
            self.assertEqual(first.source_asset_id, "noaa:USW00094789:2025")
            self.assertEqual(first.source_file, "weather.csv")
            self.assertEqual(len(first.record_hash), 64)

    def test_read_raw_csvs_rejects_wrong_line_width(self) -> None:
        columns = ("STATION", "DATE", "REPORT_TYPE")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            csv_path = root / "bad_width.csv"
            csv_path.write_text(
                "STATION,DATE,REPORT_TYPE\nUSW00094789,2025-06-01T00:51:00,FM-15\nUSW00094789,2025-06-01T01:51:00\n",
                encoding="utf-8",
            )
            source = common.SelectedCsv(
                csv_path, common.file_hash(csv_path), csv_path.stat().st_size
            )
            asset = common.SelectedAsset(
                "noaa:USW00094789:2025", "noaa", source.sha256, [source]
            )
            with self.assertRaisesRegex(ValueError, "1 lines deviate from 3 fields"):
                common.read_raw_csvs(
                    self.spark,
                    [asset],
                    root,
                    run_id="run-002",
                    rule_version="test-1.0",
                    columns=columns,
                    expected_kind="noaa",
                )

    def test_write_publish_and_read_ready_table_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            silver_root = Path(tmp).resolve() / "silver" / "dev" / "christopher"
            audit_df = common.build_audit_df(
                self.spark,
                run_id="run-123",
                rule_version="v1",
                source_asset_id="noaa:USW00094789:2025",
                counts={"raw": 5, "retained": 5},
            )
            run_dir = silver_root / "audit" / "runs" / "run-123"
            ok = common.write_parquet_and_verify(
                self.spark, audit_df, run_dir, expected_count=2
            )
            self.assertTrue(ok)
            manifest = run_dir / "_READY.json"
            common.publish_ready(
                silver_root, "audit", manifest, {"run_id": "run-123", "counts": {"raw": 5}}
            )
            readback = common.read_ready_table(self.spark, silver_root, "audit")
            self.assertEqual(readback.count(), 2)


if __name__ == "__main__":
    unittest.main()

