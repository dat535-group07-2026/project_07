#!/usr/bin/env python3
"""Collect Bronze, Silver, and Gold manifests and archive pipeline logs."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Sequence

from project07 import config as cfg_mod


def collect_bronze_manifests(
    bronze_root: Path,
    out_dir: Path,
    *,
    since_mtime: float,
) -> list[dict]:
    """Copy Bronze run manifests modified since `since_mtime` and catalog.json."""
    out_dir.mkdir(parents=True, exist_ok=True)
    reports: list[dict] = []
    runs_dir = bronze_root / "runs"
    if runs_dir.is_dir():
        for run_file in sorted(runs_dir.glob("*.json")):
            if run_file.stat().st_mtime >= since_mtime:
                shutil.copy2(run_file, out_dir / run_file.name)
                reports.append(json.loads(run_file.read_text(encoding="utf-8")))
    catalog_file = bronze_root / "catalog.json"
    if catalog_file.is_file():
        shutil.copy2(catalog_file, out_dir / "catalog.json")
    return reports


def collect_layer_manifests(
    layer_root: Path,
    out_dir: Path,
) -> list[tuple[str, dict, dict | None]]:
    """Copy current pointers and their validated _READY.json manifests for a Silver/Gold layer."""
    collected: list[tuple[str, dict, dict | None]] = []
    if not layer_root.is_dir():
        return collected

    pointer_files = sorted(layer_root.glob("*/current.json")) + sorted(
        layer_root.glob("*/current/*.json")
    )
    for pointer_path in pointer_files:
        rel_pointer = pointer_path.relative_to(layer_root)
        dest_pointer = out_dir / rel_pointer
        dest_pointer.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(pointer_path, dest_pointer)
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))

        table = pointer.get("table")
        manifest_rel = pointer.get("ready_manifest")
        manifest_data: dict | None = None
        if isinstance(table, str) and isinstance(manifest_rel, str):
            table_dir = (layer_root / table).resolve()
            manifest_path = (table_dir / manifest_rel).resolve()
            if manifest_path.is_relative_to(table_dir) and manifest_path.is_file():
                dest_manifest = (out_dir / table / manifest_rel).resolve()
                if dest_manifest.is_relative_to(out_dir.resolve()):
                    dest_manifest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(manifest_path, dest_manifest)
                    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))

        collected.append((str(rel_pointer), pointer, manifest_data))
    return collected


def archive_artifacts(artifact_dir: Path, archive_dir: Path) -> None:
    """Copy collected logs and manifests to the persistent VM archive directory."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    for sub in ("logs", "manifests"):
        src = artifact_dir / sub
        if not src.exists():
            continue
        dst = archive_dir / sub
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)


def format_summary(
    *,
    event_name: str,
    target_env: str,
    operation: str,
    start_month: str,
    end_month: str,
    weather_year: str,
    archive_dir: Path,
    bronze_reports: Sequence[dict],
    silver_entries: Sequence[tuple[str, dict, dict | None]],
    gold_entries: Sequence[tuple[str, dict, dict | None]],
) -> str:
    """Render a concise Markdown summary of the Bronze-to-Gold run."""
    lines = [
        "## Bronze to Gold pipeline",
        "",
        f"- Event: `{event_name}`",
        f"- Environment: `{target_env}`",
        f"- Bronze operation: `{operation}` ({start_month} to {end_month})",
        f"- Weather year: `{weather_year}`",
        f"- VM archive: `{archive_dir}`",
        "",
        "### Bronze",
        "",
    ]
    if not bronze_reports:
        lines.append("No Bronze run manifest was created.")
    for report in bronze_reports:
        lines.append(
            f"- Run `{report['run_id']}`: **{report['status']}** "
            f"(sources={len(report.get('sources', []))}, "
            f"unavailable={len(report.get('unavailable_months', []))}) — "
            f"{report.get('message', '')}"
        )

    lines.extend(["", "### Silver", ""])
    if not silver_entries:
        lines.append("No Silver `current` pointers found.")
    seen_silver: set[tuple[str, str]] = set()
    for rel, pointer, manifest in silver_entries:
        if not manifest:
            lines.append(f"- `{rel}`: missing `_READY.json`")
            continue
        key = (pointer["table"], manifest["run_id"])
        if key in seen_silver:
            continue
        seen_silver.add(key)
        counts = manifest.get("counts", {})
        label = f"{pointer['table']}" + (f" ({pointer['key']})" if "key" in pointer else "")
        commit = str(manifest.get("git_commit", ""))[:7]
        lines.append(
            f"- `{label}` (`{manifest['run_id']}`, `{commit}`): "
            f"raw={counts.get('raw')}, retained={counts.get('retained')}, "
            f"quarantined={counts.get('quarantined')}, "
            f"elapsed={manifest.get('elapsed_seconds')}s"
        )

    lines.extend(["", "### Gold", ""])
    if not gold_entries:
        lines.append("No Gold `current.json` pointer found.")
    for rel, pointer, manifest in gold_entries:
        if not manifest:
            lines.append(f"- `{rel}`: missing `_READY.json`")
            continue
        counts = manifest.get("counts", {})
        commit = str(manifest.get("git_commit", ""))[:7]
        lines.append(
            f"- `{pointer['table']}` (`{manifest['run_id']}`, `{commit}`): "
            f"rows={counts.get('gold_rows')}, flights={counts.get('gold_flights')}, "
            f"with_weather={counts.get('flights_with_weather')}, "
            f"days_without_weather={counts.get('days_without_weather')}, "
            f"elapsed={manifest.get('elapsed_seconds')}s"
        )

    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    cfg_mod.add_config_args(parser)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--since-marker", type=Path, required=True)
    parser.add_argument("--summary-file", type=Path, default=None)
    parser.add_argument("--event-name", default="manual")
    parser.add_argument("--operation", default="verify")
    parser.add_argument("--start-month", default="2025-06")
    parser.add_argument("--end-month", default="2025-10")
    parser.add_argument("--weather-year", default="2025")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = cfg_mod.resolve_config(env=args.env, data_root=args.data_root, dev_user=args.dev_user)
    manifests_dir = args.artifact_dir / "manifests"
    since_mtime = args.since_marker.stat().st_mtime

    bronze_reports = collect_bronze_manifests(
        cfg.bronze_root, manifests_dir / "bronze", since_mtime=since_mtime
    )
    silver_entries = collect_layer_manifests(cfg.silver_root, manifests_dir / "silver")
    gold_entries = collect_layer_manifests(cfg.gold_root, manifests_dir / "gold")
    archive_artifacts(args.artifact_dir, args.archive_dir)

    summary = format_summary(
        event_name=args.event_name,
        target_env=cfg.env,
        operation=args.operation,
        start_month=args.start_month,
        end_month=args.end_month,
        weather_year=args.weather_year,
        archive_dir=args.archive_dir,
        bronze_reports=bronze_reports,
        silver_entries=silver_entries,
        gold_entries=gold_entries,
    )
    if args.summary_file is not None:
        args.summary_file.parent.mkdir(parents=True, exist_ok=True)
        with args.summary_file.open("a", encoding="utf-8") as stream:
            stream.write(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
