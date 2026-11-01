"""Shared Silver catalog reader, raw CSV reader, provenance, accounting and publication."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

PROVENANCE: tuple[str, ...] = (
    "source_asset_id",
    "source_file",
    "source_sha256",
    "silver_run_id",
    "rule_version",
)


@dataclass(frozen=True)
class SelectedCsv:
    path: Path
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class SelectedAsset:
    asset_id: str
    kind: str
    version_sha256: str
    csvs: list[SelectedCsv]


def file_hash(path: Path) -> str:
    """Compute the SHA-256 hex digest of a file."""
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _verified_file(root: Path, record: dict) -> SelectedCsv:
    relative = Path(record["path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Unsafe catalog path: {relative}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError(f"Missing file or path outside Bronze: {relative}")
    digest = record.get("sha256", "")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"Invalid catalog checksum: {relative}")
    size_bytes = record.get("size_bytes")
    if not isinstance(size_bytes, int) or path.stat().st_size != size_bytes or file_hash(path) != digest:
        raise ValueError(f"Catalog size/checksum mismatch: {relative}")
    return SelectedCsv(path, digest, size_bytes)


def select_inputs(bronze_root: Path, asset_ids: list[str]) -> list[SelectedAsset]:
    """Snapshot the selected (latest) version of each requested Bronze asset and verify hashes."""
    root = Path(bronze_root).resolve()
    catalog_path = root / "catalog.json"
    if not catalog_path.is_file():
        raise ValueError(f"Missing Bronze catalog: {catalog_path}")
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    if catalog.get("schema_version") != 1:
        raise ValueError("Unsupported Bronze catalog schema")
    if not asset_ids or len(set(asset_ids)) != len(asset_ids):
        raise ValueError("Select a nonempty list of distinct assets")

    selected: list[SelectedAsset] = []
    for asset_id in asset_ids:
        versions = catalog.get("assets", {}).get(asset_id)
        if not versions:
            raise ValueError(f"Missing Bronze asset: {asset_id}")
        version = versions[-1]
        kind = version.get("kind")
        if kind not in {"bts", "noaa"} or asset_id.split(":", 1)[0] != kind:
            raise ValueError(f"Unexpected asset kind: {asset_id}")
        _verified_file(root, version)
        csv_records = version.get("csvs", []) if kind == "bts" else [version]
        if not csv_records:
            raise ValueError(f"Selected asset has no extracted CSVs: {asset_id}")
        verified = [_verified_file(root, record) for record in csv_records]
        if len({record.path for record in verified}) != len(verified):
            raise ValueError(f"Asset lists the same CSV more than once: {asset_id}")
        selected.append(SelectedAsset(asset_id, kind, version["sha256"], verified))
    return selected


def raw_string_schema(raw_fields: Sequence[str]) -> StructType:
    """Build an explicit all-string Spark schema for raw CSV ingestion."""
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(name, StringType(), True) for name in raw_fields])


def read_raw_csvs(
    spark: SparkSession,
    assets: Sequence[SelectedAsset],
    bronze_root: Path,
    run_id: str,
    rule_version: str,
    columns: Sequence[str],
    *,
    expected_kind: str | None = None,
    trailing_empty: bool = False,
) -> DataFrame:
    """Read selected CSVs with explicit all-string schema, line-width check, and provenance literals."""
    from pyspark.sql import functions as F

    root = Path(bronze_root).resolve()
    expected_header = [*columns, ""] if trailing_empty else list(columns)
    raw_fields = (*columns, "_trailing_empty", "_corrupt_record") if trailing_empty else (*columns, "_corrupt_record")
    schema = raw_string_schema(raw_fields)
    expected_commas = len(expected_header) - 1

    frames = []
    for asset in assets:
        if expected_kind is not None and asset.kind != expected_kind:
            raise ValueError(f"Reader accepts only {expected_kind!r} assets; got {asset.kind!r}")
        for source in asset.csvs:
            with source.path.open(encoding="utf-8-sig", newline="") as stream:
                actual_header = next(csv.reader(stream), None)
            if actual_header != expected_header:
                raise ValueError(f"Unexpected {asset.kind.upper()} header: {source.path}")

            # Verify physical line field width before PERMISSIVE parsing so short/long rows
            # cannot silently pad nulls or drop trailing columns.
            lines = spark.read.text(str(source.path))
            unquoted = F.regexp_replace(F.col("value"), r'"[^"]*"', "")
            commas = F.length(unquoted) - F.length(F.regexp_replace(unquoted, ",", ""))
            invalid_lines = lines.filter(commas != expected_commas).count()
            if invalid_lines:
                raise ValueError(
                    f"{asset.kind.upper()} CSV field count: {invalid_lines} lines deviate from "
                    f"{len(expected_header)} fields ({expected_commas} commas): {source.path}"
                )

            frame = (
                spark.read.schema(schema)
                .option("header", True)
                .option("mode", "PERMISSIVE")
                .option("escape", '"')
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .csv(str(source.path))
            )
            provenance_values = (
                asset.asset_id,
                str(source.path.relative_to(root)),
                source.sha256,
                run_id,
                rule_version,
            )
            frames.append(
                frame.select(
                    "*",
                    *[F.lit(val).alias(name) for name, val in zip(PROVENANCE, provenance_values)],
                )
            )
    if not frames:
        raise ValueError("No CSVs selected")
    return reduce(lambda left, right: left.unionByName(right), frames)


def verify_session_conventions(spark: SparkSession) -> None:
    """Confirm that the active SparkSession obeys the Silver contract (UTC and ANSI=false)."""
    if spark.conf.get("spark.sql.ansi.enabled").lower() != "false":
        raise ValueError("Silver parsing requires explicit spark.sql.ansi.enabled=false")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Silver requires explicit spark.sql.session.timeZone=UTC")


def deduplicate_with_provenance(raw: DataFrame, raw_fields: Sequence[str]) -> DataFrame:
    """Collapse exact duplicates across all original fields and compute deterministic record_hash."""
    from pyspark.sql import functions as F

    required = set(raw_fields) | set(PROVENANCE)
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Raw columns missing: {sorted(missing)}")
    verify_session_conventions(raw.sparkSession)

    deduped = (
        raw.select(
            F.struct(*[F.col(c) for c in raw_fields]).alias("original"),
            F.struct(*[F.col(c) for c in PROVENANCE]).alias("_provenance"),
        )
        .groupBy("original")
        .agg(
            F.count("*").alias("_occurrences"),
            F.min("_provenance").alias("_provenance"),
        )
        .select("original", "original.*", "_occurrences", "_provenance.*")
    )
    return deduped.withColumn(
        "record_hash",
        F.sha2(F.to_json("original", {"ignoreNullFields": "false"}), 256),
    )


def check_accounting(counts: dict[str, int], observed_raw: int) -> dict[str, bool]:
    """Verify the Silver row-accounting equations and fail if any equation does not hold."""
    checks = {
        "raw_matches_reader": counts["raw"] == observed_raw,
        "raw_reconciles": counts["raw"] == counts["duplicate_extras"] + counts["unique"],
        "unique_reconciles": (
            counts["unique"]
            == counts["scope_excluded"] + counts["retained"] + counts["quarantined"]
        ),
    }
    if not all(checks.values()):
        raise ValueError(f"Row accounting failed: {checks}")
    return checks


def build_audit_df(
    spark: SparkSession,
    run_id: str,
    rule_version: str,
    source_asset_id: str,
    counts: dict[str, int],
) -> DataFrame:
    """Create the standardized Silver audit DataFrame from a metrics dictionary."""
    from pyspark.sql import types as T

    audit_schema = T.StructType(
        [
            T.StructField("silver_run_id", T.StringType(), False),
            T.StructField("rule_version", T.StringType(), False),
            T.StructField("source_asset_id", T.StringType(), False),
            T.StructField("metric", T.StringType(), False),
            T.StructField("value", T.LongType(), False),
        ]
    )
    rows = [
        (run_id, rule_version, source_asset_id, metric, int(value))
        for metric, value in counts.items()
    ]
    return spark.createDataFrame(rows, audit_schema)


def new_run_id() -> str:
    """Generate a Silver run identifier formatted as YYYYMMDDTHHMMSSZ-<8 hex>."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]


def clean_git_commit(repo_root: Path | None = None, *, allow_dirty: bool = False) -> str:
    """Return the current HEAD commit SHA, failing if the checkout has uncommitted changes."""
    repository = repo_root or Path(__file__).resolve().parents[2]
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repository, text=True
    ).strip()
    if not allow_dirty:
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=repository, text=True
        ).strip()
        if status:
            raise ValueError(
                "Publish from a clean committed checkout so the manifest identifies the code"
            )
    return commit


def atomic_json(path: Path, payload: dict) -> None:
    """Atomically write a JSON file by writing a sibling temporary file and replacing."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=path.parent, encoding="utf-8", delete=False
        ) as stream:
            temporary = Path(stream.name)
            json.dump(payload, stream, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def current_pointer(silver_root: Path, table: str, key: str | None = None) -> Path:
    """Return the readiness pointer path for a Silver table (optionally scoped by period/station key)."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", table):
        raise ValueError(f"Invalid table name: {table!r}")
    table_dir = Path(silver_root) / table
    if key is None:
        return table_dir / "current.json"
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", key):
        raise ValueError(f"Invalid pointer key: {key!r}")
    return table_dir / "current" / f"{key}.json"


def input_snapshot(assets: Sequence[SelectedAsset], bronze_root: Path) -> list[dict]:
    """Serialize selected Bronze assets and relative file paths for _READY.json manifests."""
    root = Path(bronze_root).resolve()
    return [
        {
            "asset_id": asset.asset_id,
            "kind": asset.kind,
            "version_sha256": asset.version_sha256,
            "csvs": [
                {
                    "path": str(csv_file.path.relative_to(root)),
                    "sha256": csv_file.sha256,
                    "size_bytes": csv_file.size_bytes,
                }
                for csv_file in asset.csvs
            ],
        }
        for asset in assets
    ]


def output_summary(path: Path, silver_root: Path) -> dict:
    """Summarize written Parquet files and total byte size under a run directory."""
    files = list(Path(path).rglob("*.parquet"))
    return {
        "path": str(Path(path).relative_to(silver_root)),
        "parquet_files": len(files),
        "bytes": sum(p.stat().st_size for p in files),
    }


def write_parquet_and_verify(
    spark: SparkSession,
    frame: DataFrame,
    run_path: Path,
    expected_count: int,
    *,
    partition_by: Sequence[str] = (),
) -> bool:
    """Write an immutable Parquet run output and verify its read-back row count."""
    writer = frame.write.mode("errorifexists")
    if partition_by and expected_count > 0:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(run_path))
    actual_count = spark.read.parquet(str(run_path)).count()
    return actual_count == expected_count


def publish_ready(
    silver_root: Path,
    table: str,
    manifest_path: Path,
    report: dict,
    *,
    pointer_key: str | None = None,
    extra_pointer_fields: dict | None = None,
) -> Path:
    """Write _READY.json first, then atomically move the current pointer as the final step."""
    silver_root = Path(silver_root)
    manifest_path = Path(manifest_path)
    pointer = current_pointer(silver_root, table, pointer_key)
    atomic_json(manifest_path, report)
    pointer_payload = {
        "table": table,
        "run_id": report["run_id"],
        "ready_manifest": str(manifest_path.relative_to(silver_root / table)),
    }
    if pointer_key is not None:
        pointer_payload["key"] = pointer_key
    if extra_pointer_fields:
        pointer_payload.update(extra_pointer_fields)
    atomic_json(pointer, pointer_payload)
    return pointer


def read_ready_table(
    spark: SparkSession,
    silver_root: Path,
    table: str,
    *,
    pointer_key: str | None = None,
) -> DataFrame:
    """Read a published Silver table through its current pointer after verifying _READY.json."""
    silver_root = Path(silver_root)
    pointer = current_pointer(silver_root, table, pointer_key)
    if not pointer.is_file():
        raise ValueError(f"No ready pointer found for {table}: {pointer}")
    pointer_data = json.loads(pointer.read_text(encoding="utf-8"))
    manifest_rel = pointer_data.get("ready_manifest")
    if not manifest_rel:
        raise ValueError(f"Pointer missing ready_manifest: {pointer}")
    manifest_path = (silver_root / table / manifest_rel).resolve()
    if not manifest_path.is_relative_to((silver_root / table).resolve()) or not manifest_path.is_file():
        raise ValueError(f"Missing or invalid _READY.json manifest: {manifest_path}")
    run_dir = manifest_path.parent
    return spark.read.parquet(str(run_dir))

