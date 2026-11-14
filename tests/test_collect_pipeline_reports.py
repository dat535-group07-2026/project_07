"""Tests for pipeline manifest collection, log archiving, and summary generation."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from project07.silver import common
from scripts import collect_pipeline_reports as collector


class CollectPipelineReportsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.data_root = self.root / "project-data"
        self.artifact_dir = self.root / "artifacts"
        self.archive_dir = self.data_root / "logs" / "pipeline" / "100-1"
        self.marker = self.root / "pipeline-start"
        self.marker.write_text("", encoding="utf-8")
        os.utime(self.marker, (1000.0, 1000.0))

    def test_collects_manifests_archives_logs_and_writes_summary(self) -> None:
        bronze_runs = self.data_root / "bronze" / "runs"
        bronze_runs.mkdir(parents=True)
        old_run = bronze_runs / "old.json"
        old_run.write_text(json.dumps({"run_id": "old", "status": "complete"}), encoding="utf-8")
        os.utime(old_run, (900.0, 900.0))

        new_run = bronze_runs / "new.json"
        new_run.write_text(
            json.dumps(
                {
                    "run_id": "new-run",
                    "status": "complete",
                    "sources": [{"id": "bts:2025-06"}],
                    "unavailable_months": [],
                    "message": "No new or changed inputs",
                }
            ),
            encoding="utf-8",
        )
        os.utime(new_run, (1100.0, 1100.0))
        (self.data_root / "bronze" / "catalog.json").write_text('{"schema_version": 1}', encoding="utf-8")

        silver_root = self.data_root / "silver" / "prod"
        common.publish_ready(
            silver_root,
            "flights",
            silver_root / "flights" / "runs" / "f-1" / "_READY.json",
            {
                "run_id": "f-1",
                "git_commit": "abcdef1234567",
                "counts": {"raw": 100, "retained": 98, "quarantined": 2},
                "elapsed_seconds": 12.3,
            },
            pointer_key="2025-06",
            extra_pointer_fields={"month": "2025-06"},
        )
        common.publish_ready(
            silver_root,
            "weather_hourly",
            silver_root / "weather_hourly" / "runs" / "w-1" / "_READY.json",
            {
                "run_id": "w-1",
                "git_commit": "abcdef1234567",
                "counts": {"raw": 20, "retained": 18, "quarantined": 1},
                "elapsed_seconds": 4.5,
            },
        )

        gold_root = self.data_root / "gold" / "prod"
        common.publish_ready(
            gold_root,
            "daily_airport",
            gold_root / "daily_airport" / "runs" / "g-1" / "_READY.json",
            {
                "run_id": "g-1",
                "git_commit": "abcdef1234567",
                "counts": {
                    "gold_rows": 306,
                    "gold_flights": 102640,
                    "flights_with_weather": 99591,
                    "days_without_weather": 6,
                },
                "elapsed_seconds": 9.1,
            },
        )

        logs_dir = self.artifact_dir / "logs"
        logs_dir.mkdir(parents=True)
        (logs_dir / "04-gold.log").write_text("gold ok\n", encoding="utf-8")
        summary_file = self.root / "summary.md"

        rc = collector.main(
            [
                "--env",
                "prod",
                "--data-root",
                str(self.data_root),
                "--artifact-dir",
                str(self.artifact_dir),
                "--archive-dir",
                str(self.archive_dir),
                "--since-marker",
                str(self.marker),
                "--summary-file",
                str(summary_file),
                "--event-name",
                "schedule",
            ]
        )
        self.assertEqual(rc, 0)

        # Old Bronze run excluded; new Bronze run and catalog copied.
        self.assertFalse((self.artifact_dir / "manifests" / "bronze" / "old.json").exists())
        self.assertTrue((self.artifact_dir / "manifests" / "bronze" / "new.json").is_file())
        self.assertTrue((self.artifact_dir / "manifests" / "bronze" / "catalog.json").is_file())

        # Silver and Gold pointers and _READY.json manifests copied and archived.
        for base in (self.artifact_dir, self.archive_dir):
            self.assertTrue((base / "logs" / "04-gold.log").is_file())
            self.assertTrue(
                (base / "manifests" / "silver" / "flights" / "current" / "2025-06.json").is_file()
            )
            self.assertTrue(
                (base / "manifests" / "silver" / "flights" / "runs" / "f-1" / "_READY.json").is_file()
            )
            self.assertTrue(
                (base / "manifests" / "gold" / "daily_airport" / "runs" / "g-1" / "_READY.json").is_file()
            )

        summary = summary_file.read_text(encoding="utf-8")
        self.assertIn("Event: `schedule`", summary)
        self.assertIn("Run `new-run`: **complete**", summary)
        self.assertIn("`flights (2025-06)` (`f-1`, `abcdef1`): raw=100, retained=98, quarantined=2", summary)
        self.assertIn("`daily_airport` (`g-1`, `abcdef1`): rows=306, flights=102640, with_weather=99591", summary)

    def test_ignores_unsafe_ready_manifest_path_outside_table(self) -> None:
        outside = self.root / "secret.json"
        outside.write_text('{"secret": true}', encoding="utf-8")
        silver_root = self.data_root / "silver" / "prod"
        pointer = silver_root / "flights" / "current" / "2025-06.json"
        common.atomic_json(
            pointer,
            {"table": "flights", "run_id": "bad", "ready_manifest": "../../../../secret.json"},
        )

        entries = collector.collect_layer_manifests(silver_root, self.artifact_dir / "manifests" / "silver")
        self.assertEqual(len(entries), 1)
        self.assertIsNone(entries[0][2])
        self.assertFalse((self.artifact_dir / "secret.json").exists())


if __name__ == "__main__":
    unittest.main()
