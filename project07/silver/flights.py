"""First national BTS flight slice, using PR #3's working assumptions.

The severities below are proposals for Monday's meeting, not BTS requirements.
The BTS directive defines the clocks, minute fields and operational states:
https://www.bts.gov/topics/airlines-and-airports/number-31-technical-directive-time-reporting-effective-jan-1-2019

Only this module contains temporary catalog/runtime/publication adapters. Replace
them with Christopher's shared implementations when their interfaces are ready.
No national time zone is assigned. Local datetimes use Spark TIMESTAMP_NTZ.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


LOG = logging.getLogger(__name__)
DEFAULT_DATA_ROOT = Path("/home/ubuntu/project-data")
RULE_VERSION = "flights-0.2-working"
PROVENANCE = (
    "source_asset_id", "source_file", "source_sha256", "silver_run_id", "rule_version",
)
KEY_FIELDS = (
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "CRSDepTime",
)
BTS_COLUMNS = tuple("""
Year Quarter Month DayofMonth DayOfWeek FlightDate Reporting_Airline
DOT_ID_Reporting_Airline IATA_CODE_Reporting_Airline Tail_Number
Flight_Number_Reporting_Airline OriginAirportID OriginAirportSeqID
OriginCityMarketID Origin OriginCityName OriginState OriginStateFips
OriginStateName OriginWac DestAirportID DestAirportSeqID DestCityMarketID Dest
DestCityName DestState DestStateFips DestStateName DestWac CRSDepTime DepTime
DepDelay DepDelayMinutes DepDel15 DepartureDelayGroups DepTimeBlk TaxiOut
WheelsOff WheelsOn TaxiIn CRSArrTime ArrTime ArrDelay ArrDelayMinutes ArrDel15
ArrivalDelayGroups ArrTimeBlk Cancelled CancellationCode Diverted CRSElapsedTime
ActualElapsedTime AirTime Flights Distance DistanceGroup CarrierDelay WeatherDelay
NASDelay SecurityDelay LateAircraftDelay FirstDepTime TotalAddGTime LongestAddGTime
DivAirportLandings DivReachedDest DivActualElapsedTime DivArrDelay DivDistance
Div1Airport Div1AirportID Div1AirportSeqID Div1WheelsOn Div1TotalGTime
Div1LongestGTime Div1WheelsOff Div1TailNum Div2Airport Div2AirportID Div2AirportSeqID
Div2WheelsOn Div2TotalGTime Div2LongestGTime Div2WheelsOff Div2TailNum Div3Airport
Div3AirportID Div3AirportSeqID Div3WheelsOn Div3TotalGTime Div3LongestGTime
Div3WheelsOff Div3TailNum Div4Airport Div4AirportID Div4AirportSeqID Div4WheelsOn
Div4TotalGTime Div4LongestGTime Div4WheelsOff Div4TailNum Div5Airport Div5AirportID
Div5AirportSeqID Div5WheelsOn Div5TotalGTime Div5LongestGTime Div5WheelsOff Div5TailNum
""".split())
RAW_FIELDS = (*BTS_COLUMNS, "_trailing_empty", "_corrupt_record")
MINUTE_FIELDS = {
    "DepDelay": "dep_delay_min", "DepDelayMinutes": "dep_delay_nonnegative_min",
    "ArrDelay": "arr_delay_min", "ArrDelayMinutes": "arr_delay_nonnegative_min",
    "CRSElapsedTime": "scheduled_elapsed_min", "ActualElapsedTime": "actual_elapsed_min",
    "AirTime": "air_time_min", "TaxiIn": "taxi_in_min", "TaxiOut": "taxi_out_min",
}
CLOCK_FIELDS = {
    "CRSDepTime": "scheduled_dep_hhmm", "DepTime": "dep_hhmm",
    "CRSArrTime": "scheduled_arr_hhmm", "ArrTime": "arr_hhmm",
}
COMPLETED_REQUIRED = (
    "dep_hhmm", "dep_delay_min", "arr_hhmm", "arr_delay_min",
    "actual_elapsed_min", "air_time_min", "taxi_in_min", "taxi_out_min",
)
RULES = {
    "F_MALFORMED_RECORD": ("fatal", "Parser-corrupt record or nonempty trailing field"),
    "F_KEY_MISSING": ("fatal", "Blank candidate-key component"),
    "F_KEY_INVALID": ("fatal", "Invalid date, identifier shape or scheduled departure clock"),
    "F_FLAG_INVALID": ("fatal", "Cancellation/diversion flag is not an encoded zero or one"),
    "F_STATE_CONFLICT": ("fatal", "Flight is both cancelled and diverted"),
    "F_KEY_CONFLICT": ("fatal", "Different source records share a valid candidate key"),
    "F_CLOCK_DELAY_MISMATCH": ("warning", "Reported departure clock differs from reconstruction"),
    "F_CANCELLED_DEPTIME_NO_DELAY": ("warning", "Cancellation with departure clock but blank delay"),
    "F_COMPLETED_FIELD_MISSING": ("warning", "Completed flight lacks a parsed required actual field"),
    "F_DURATION_MISMATCH": ("warning", "Completed elapsed minutes differ from air plus taxi minutes"),
    "F_CANCEL_CODE_MISMATCH": ("warning", "Cancellation code disagrees with a valid cancellation flag"),
    "F_NUMERIC_UNPARSEABLE": ("warning", "Nonblank optional minute/clock token cannot be parsed"),
    "F_MINUTE_DOMAIN": ("warning", "Negative duration or clipped delay, or clipped delay differs from signed delay floored at zero"),
}
NONNEGATIVE_MINUTES = (
    "dep_delay_nonnegative_min", "arr_delay_nonnegative_min", "scheduled_elapsed_min",
    "actual_elapsed_min", "air_time_min", "taxi_in_min", "taxi_out_min",
)
CLIPPED_DELAYS = (
    ("dep_delay_min", "dep_delay_nonnegative_min"),
    ("arr_delay_min", "arr_delay_nonnegative_min"),
)


# Temporary common.select_inputs stand-in. Keep the PR #3 dataclass signature.
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
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _verified_file(root: Path, record: dict) -> SelectedCsv:
    relative = Path(record["path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Unsafe catalog path: {relative}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError(f"Missing file or path outside Bronze: {relative}")
    digest = record["sha256"]
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"Invalid catalog checksum: {relative}")
    if path.stat().st_size != record["size_bytes"] or file_hash(path) != digest:
        raise ValueError(f"Catalog size/checksum mismatch: {relative}")
    return SelectedCsv(path, digest, record["size_bytes"])


def select_inputs(bronze_root: Path, asset_ids: list[str]) -> list[SelectedAsset]:
    """Read one catalog snapshot; verify selected source and extracted files."""
    root = Path(bronze_root).resolve()
    catalog = json.loads((root / "catalog.json").read_text(encoding="utf-8"))
    if catalog.get("schema_version") != 1:
        raise ValueError("Unsupported Bronze catalog schema")
    if not asset_ids or len(set(asset_ids)) != len(asset_ids):
        raise ValueError("Select a nonempty list of distinct assets")
    selected = []
    paths = set()
    for asset_id in asset_ids:
        versions = catalog.get("assets", {}).get(asset_id)
        if not versions:
            raise ValueError(f"Missing Bronze asset: {asset_id}")
        version = versions[-1]
        kind = version.get("kind")
        if kind not in {"bts", "noaa"} or asset_id.split(":", 1)[0] != kind:
            raise ValueError(f"Unexpected asset kind: {asset_id}")
        _verified_file(root, version)
        csvs = version.get("csvs", []) if kind == "bts" else [version]
        if not csvs:
            raise ValueError(f"Selected asset has no extracted CSVs: {asset_id}")
        verified = [_verified_file(root, record) for record in csvs]
        for record in verified:
            if record.path in paths:
                raise ValueError(f"CSV selected more than once: {record.path}")
            paths.add(record.path)
        selected.append(SelectedAsset(asset_id, kind, version["sha256"], verified))
    return selected


def raw_schema():
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(name, StringType(), True) for name in RAW_FIELDS])


# Temporary raw-reader adapter; move into common.py after Christopher's PR.
def read_flights(spark: SparkSession, assets: list[SelectedAsset], bronze_root: Path,
                 run_id: str) -> DataFrame:
    from pyspark.sql import functions as F

    frames = []
    root = Path(bronze_root).resolve()
    for asset in assets:
        if asset.kind != "bts":
            raise ValueError("The flight reader accepts only BTS assets")
        for source in asset.csvs:
            with source.path.open(encoding="utf-8-sig", newline="") as stream:
                actual = next(csv.reader(stream), None)
            if actual != [*BTS_COLUMNS, ""]:
                raise ValueError(f"Unexpected BTS header: {source.path}")
            # The June-October 2025 BTS files were checked with csv.reader:
            # each record occupies one physical line, including the header.
            # PERMISSIVE does not flag every short/extra-field record, so check
            # widths separately. This is not a multiline CSV validator.
            lines = spark.read.text(str(source.path)).withColumn(
                "_line_id", F.monotonically_increasing_id())
            # Each file is read separately; ID zero is its first physical line.
            lines = lines.filter(F.col("_line_id") != 0)
            unquoted = F.regexp_replace(F.col("value"), r'"[^"]*"', "")
            commas = F.length(unquoted) - F.length(F.regexp_replace(unquoted, ",", ""))
            invalid_lines = lines.filter(commas != 109).count()
            if invalid_lines:
                raise ValueError(
                    f"BTS CSV field count: {invalid_lines} lines deviate from "
                    f"110 fields (109 commas): {source.path}")
            frame = (spark.read.schema(raw_schema()).option("header", True)
                     .option("mode", "PERMISSIVE")
                     # RFC 4180 doubles quotes inside quoted fields, as Python's
                     # csv module and the width check expect. Spark's default
                     # escape is a backslash, which would shift such fields.
                     .option("escape", '"')
                     .option("columnNameOfCorruptRecord", "_corrupt_record")
                     .option("ignoreLeadingWhiteSpace", False)
                     .option("ignoreTrailingWhiteSpace", False).csv(str(source.path)))
            provenance = (asset.asset_id, str(source.path.relative_to(root)), source.sha256,
                          run_id, RULE_VERSION)
            frames.append(frame.select("*", *[
                F.lit(value).alias(name) for name, value in zip(PROVENANCE, provenance)
            ]))
    if not frames:
        raise ValueError("No flight CSVs selected")
    return reduce(lambda left, right: left.unionByName(right), frames)


def _token(name):
    from pyspark.sql import functions as F

    value = F.trim(F.col(name))
    return F.when(F.length(value) > 0, value)


def _clock_valid(token):
    from pyspark.sql import functions as F

    return F.coalesce(
        token.rlike(r"^(?:[01][0-9]|2[0-3])[0-5][0-9]$") | (token == "2400"),
        F.lit(False),
    )


def _minute_value(token):
    """Whole source minutes, including .00, bounded to Spark INTEGER storage."""
    from pyspark.sql import functions as F

    number = F.when(token.rlike(r"^[+-]?[0-9]+(?:\.0+)?$"),
                    token.cast("decimal(20,0)"))
    return F.when(number.between(-2147483648, 2147483647), number.cast("int"))


@dataclass
class FlightTables:
    """DataFrame-only result; the caller controls materialization and writes."""

    classified: DataFrame

    @property
    def retained(self) -> DataFrame:
        return (self.classified.filter("outcome = 'retained'")
                .withColumnRenamed("_occurrences", "source_occurrences")
                .drop("original", "rejection_reasons", "outcome"))

    @property
    def quarantined(self) -> DataFrame:
        return (self.classified.filter("outcome = 'quarantined'")
                .withColumnRenamed("_occurrences", "source_occurrences")
                .drop("outcome"))

    @property
    def audit(self) -> DataFrame:
        from pyspark.sql import functions as F

        def n(condition):
            return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).cast("long")

        expressions = [
            F.coalesce(F.sum("_occurrences"), F.lit(0)).cast("long").alias("raw"),
            F.count("*").alias("unique"),
            F.coalesce(F.sum(F.col("_occurrences") - 1), F.lit(0)).cast("long").alias("duplicate_extras"),
            F.lit(0).cast("long").alias("scope_excluded"),
            n(F.col("outcome") == "retained").alias("retained"),
            n(F.col("outcome") == "quarantined").alias("quarantined"),
        ]
        for code, (severity, _) in RULES.items():
            field = "rejection_reasons" if severity == "fatal" else "quality_flags"
            expressions.append(n(F.array_contains(F.col(field), code)).alias(code))
        return self.classified.agg(*expressions)


def clean_flights(raw: DataFrame) -> FlightTables:
    """Classify all national input rows using built-in Spark expressions only.

    Policy assumptions: two alphanumeric reporting-carrier characters, one to
    four flight-number digits (not all zero), three-letter airport codes and
    integral minute values. These are format checks, not reference-data lookups.
    This slice has no scope exclusions. No valid airport is filtered out.
    """
    from pyspark.sql import Window, functions as F

    required = set(RAW_FIELDS) | set(PROVENANCE)
    if required - set(raw.columns):
        raise ValueError(f"Raw columns missing: {sorted(required - set(raw.columns))}")
    if raw.sparkSession.conf.get("spark.sql.ansi.enabled").lower() != "false":
        raise ValueError("Flight parsing requires explicit ANSI=false")
    if raw.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Flight tests and runner require explicit session timeZone=UTC")

    # Group on the complete raw struct, not on its hash or a pruned column set.
    # Choose provenance deterministically if identical rows occur in several files.
    frame = raw.select(
        F.struct(*[F.col(c) for c in RAW_FIELDS]).alias("original"),
        F.struct(*[F.col(c) for c in PROVENANCE]).alias("_provenance"),
    ).groupBy("original").agg(
        F.count("*").alias("_occurrences"), F.min("_provenance").alias("_provenance"),
    ).select("original", "original.*", "_occurrences", "_provenance.*")
    frame = frame.withColumn("record_hash", F.sha2(
        F.to_json("original", {"ignoreNullFields": "false"}), 256))
    # JSON null is a fixed encoding distinct from the literal string "null".
    frame = frame.withColumn("flight_date", F.when(
        _token("FlightDate").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
        F.to_date(_token("FlightDate"), "yyyy-MM-dd")))
    for source, target in (
        ("Reporting_Airline", "reporting_airline"),
        ("Flight_Number_Reporting_Airline", "flight_number"),
        ("Origin", "origin"), ("Dest", "dest"), ("Tail_Number", "tail_number"),
        ("CancellationCode", "cancellation_code"),
    ):
        frame = frame.withColumn(target, _token(source))
    for source, target in CLOCK_FIELDS.items():
        token = _token(source)
        frame = frame.withColumn(target, F.when(_clock_valid(token), token))
    for source, target in MINUTE_FIELDS.items():
        frame = frame.withColumn(target, _minute_value(_token(source)))
    for source, target in (("Cancelled", "cancelled"), ("Diverted", "diverted")):
        token = _token(source)
        frame = frame.withColumn(target, F.when(
            token.rlike(r"^[01](?:\.0+)?$"), token.cast("decimal(2,0)") == 1))

    key_missing = reduce(lambda a, b: a | b, [_token(c).isNull() for c in KEY_FIELDS])
    shape_checks = (
        (_token("FlightDate").isNotNull(), F.col("flight_date").isNotNull()),
        (_token("Reporting_Airline").isNotNull(), F.col("reporting_airline").rlike(r"^[A-Z0-9]{2}$")),
        (_token("Flight_Number_Reporting_Airline").isNotNull(),
         F.col("flight_number").rlike(r"^[0-9]{1,4}$") & ~F.col("flight_number").rlike(r"^0+$")),
        (_token("Origin").isNotNull(), F.col("origin").rlike(r"^[A-Z]{3}$")),
        (_token("Dest").isNotNull(), F.col("dest").rlike(r"^[A-Z]{3}$")),
        (_token("CRSDepTime").isNotNull(), F.col("scheduled_dep_hhmm").isNotNull()),
    )
    key_invalid = reduce(lambda a, b: a | b, [
        present & ~F.coalesce(valid, F.lit(False)) for present, valid in shape_checks
    ])
    frame = frame.withColumn("_key_valid", ~(key_missing | key_invalid))
    key = ["flight_date", "reporting_airline", "flight_number", "origin", "dest", "scheduled_dep_hhmm"]
    frame = frame.withColumn("_key_count", F.count("*").over(Window.partitionBy(*key)))
    clock = F.col("scheduled_dep_hhmm")
    frame = frame.withColumn("_scheduled_minutes", F.when(clock == "2400", 1440).otherwise(
        F.substring(clock, 1, 2).cast("int") * 60 + F.substring(clock, 3, 2).cast("int")))
    frame = frame.withColumn("scheduled_dep_local", F.expr(
        "timestampadd(MINUTE, _scheduled_minutes, CAST(flight_date AS TIMESTAMP_NTZ))"))
    frame = frame.withColumn("actual_dep_local", F.when(
        F.col("dep_hhmm").isNotNull() & F.col("_key_valid"), F.expr(
            "timestampadd(MINUTE, dep_delay_min, scheduled_dep_local)")))
    frame = frame.withColumn("dep_day_offset", F.datediff(F.to_date("actual_dep_local"), "flight_date"))
    frame = frame.withColumn("year", F.year("flight_date"))
    frame = frame.withColumn("month", F.month("flight_date"))

    completed = (~F.col("cancelled")) & (~F.col("diverted"))
    minute_invalid = [
        _token(source).isNotNull() & F.col(target).isNull()
        for source, target in MINUTE_FIELDS.items()
    ]
    clock_invalid = [
        _token(source).isNotNull() & F.col(target).isNull()
        for source, target in CLOCK_FIELDS.items() if source != "CRSDepTime"
    ]
    duration_fields = ["actual_elapsed_min", "air_time_min", "taxi_in_min", "taxi_out_min"]
    duration_present = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in duration_fields])
    code = F.col("cancellation_code")
    conditions = {
        "F_MALFORMED_RECORD": F.col("_corrupt_record").isNotNull() | _token("_trailing_empty").isNotNull(),
        "F_KEY_MISSING": key_missing,
        "F_KEY_INVALID": key_invalid,
        "F_FLAG_INVALID": F.col("cancelled").isNull() | F.col("diverted").isNull(),
        "F_STATE_CONFLICT": F.col("cancelled") & F.col("diverted"),
        "F_KEY_CONFLICT": F.col("_key_valid") & (F.col("_key_count") > 1),
        "F_CLOCK_DELAY_MISMATCH": F.col("actual_dep_local").isNotNull() & (
            F.date_format("actual_dep_local", "HHmm") != F.when(
                F.col("dep_hhmm") == "2400", "0000").otherwise(F.col("dep_hhmm"))),
        "F_CANCELLED_DEPTIME_NO_DELAY": F.col("cancelled") & _token("DepTime").isNotNull() & _token("DepDelay").isNull(),
        "F_COMPLETED_FIELD_MISSING": completed & reduce(
            lambda a, b: a | b, [F.col(c).isNull() for c in COMPLETED_REQUIRED]),
        "F_DURATION_MISMATCH": completed & duration_present & (
            F.col("actual_elapsed_min").cast("long") != F.col("air_time_min").cast("long")
            + F.col("taxi_in_min").cast("long") + F.col("taxi_out_min").cast("long")),
        "F_CANCEL_CODE_MISMATCH": (F.col("cancelled") & ~F.coalesce(code.isin("A", "B", "C", "D"), F.lit(False)))
            | ((~F.col("cancelled")) & code.isNotNull()),
        "F_NUMERIC_UNPARSEABLE": reduce(lambda a, b: a | b, minute_invalid + clock_invalid),
        # Definitional limits, not thresholds: durations and clipped delays cannot
        # be negative, and a clipped delay is the signed delay floored at zero.
        "F_MINUTE_DOMAIN": reduce(lambda a, b: a | b, [
            F.col(c) < 0 for c in NONNEGATIVE_MINUTES
        ] + [
            # greatest() skips nulls, so compare only when the signed delay exists.
            F.col(signed).isNotNull() & (F.col(clipped) != F.greatest(F.col(signed), F.lit(0)))
            for signed, clipped in CLIPPED_DELAYS
        ]),
    }
    for severity, output in (("fatal", "rejection_reasons"), ("warning", "quality_flags")):
        frame = frame.withColumn(output, F.filter(F.array(*[
            F.when(F.coalesce(conditions[code], F.lit(False)), F.lit(code))
            for code, (level, _) in RULES.items() if level == severity
        ]), lambda value: value.isNotNull()))
    frame = frame.withColumn("outcome", F.when(
        F.size("rejection_reasons") > 0, "quarantined").otherwise("retained"))
    return FlightTables(frame.select(
        *PROVENANCE, "record_hash", "flight_date", "reporting_airline", "flight_number",
        "origin", "dest", "tail_number", "cancelled", "diverted", "cancellation_code",
        *CLOCK_FIELDS.values(), *MINUTE_FIELDS.values(), "scheduled_dep_local",
        "actual_dep_local", "dep_day_offset", "year", "month", "quality_flags",
        "rejection_reasons", "outcome", "original", "_occurrences",
    ))


def check_accounting(counts: dict, observed_raw: int) -> dict:
    checks = {
        "raw_matches_reader": counts["raw"] == observed_raw,
        "raw_reconciles": counts["raw"] == counts["duplicate_extras"] + counts["unique"],
        "unique_reconciles": counts["unique"] == counts["scope_excluded"] + counts["retained"] + counts["quarantined"],
    }
    if not all(checks.values()):
        raise ValueError(f"Flight row accounting failed: {checks}")
    return checks


# Temporary publication helpers. No overwrites of immutable runs or other roots.
def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=path.parent, encoding="utf-8", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(payload, stream, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def dev_output_root(data_root: Path) -> Path:
    """Reject protected roots and symlink redirects before creating any output."""
    root = data_root.resolve()
    protected = [DEFAULT_DATA_ROOT / "bronze", DEFAULT_DATA_ROOT / "silver" / "prod",
                 DEFAULT_DATA_ROOT / "silver" / "dev" / "ckolle"]
    if any(root.is_relative_to(path.resolve()) for path in protected):
        raise ValueError("Data root is inside Bronze, prod or Christopher's workspace")
    output = root / "silver" / "dev" / "alkval"
    for path in [output, *[output / name for name in ("flights", "flights_quarantine", "audit")]]:
        if path.resolve() != path:
            raise ValueError(f"Symlink redirects a dev output path: {path}")
        runs = path / "runs"
        if runs.resolve() != runs:
            raise ValueError(f"Symlink redirects a run path: {runs}")
    return output


def current_pointer(output: Path, month: str) -> Path:
    """One pointer per source month, so publishing one month leaves the others current."""
    return output / "flights" / "current" / f"{_month(month)}.json"


def _input_snapshot(assets, root):
    return [{"asset_id": a.asset_id, "kind": a.kind, "version_sha256": a.version_sha256,
             "csvs": [{"path": str(c.path.relative_to(root)), "sha256": c.sha256,
                       "size_bytes": c.size_bytes} for c in a.csvs]} for a in assets]


def _clean_commit() -> str:
    repository = Path(__file__).resolve().parents[2]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=repository, text=True).strip():
        raise ValueError("Publish from a clean committed checkout so the manifest identifies the code")
    return commit


def _month(value):
    if not re.fullmatch(r"[0-9]{4}-(?:0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError("Use YYYY-MM")
    return value


def run_month(args) -> dict:
    """Temporary dev-only entry point; shared run.py will replace this adapter."""
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession, functions as F, types as T

    if args.env != "dev" or args.dev_user != "alkval":
        raise ValueError("This temporary flight runner only writes --env dev --dev-user alkval")
    if Path(os.environ.get("JAVA_HOME", "")).resolve() != Path("/usr/lib/jvm/java-8-openjdk-amd64").resolve():
        raise ValueError("Set JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64")
    if os.environ.get("PYSPARK_DRIVER_PYTHON") or os.environ.get("PYSPARK_DRIVER_PYTHON_OPTS"):
        raise ValueError("Unset PYSPARK_DRIVER_PYTHON and PYSPARK_DRIVER_PYTHON_OPTS")
    root = args.data_root.resolve()
    bronze = root / "bronze"
    output = dev_output_root(root)
    commit = _clean_commit()
    started = time.monotonic()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    assets = select_inputs(bronze, [f"bts:{args.month}"])
    # Local mode runs tasks inside the driver JVM. Spark's 1 GB default heap ran
    # out of memory on June 2025 (about 277 MB of CSV), so set it before the JVM starts.
    spark = (SparkSession.builder.master("local[*]").appName(f"project07-flights-{args.month}")
             .config("spark.driver.memory", "3g")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    classified = None
    paths = {table: output / table / "runs" / run_id
             for table in ("flights", "flights_quarantine", "audit")}
    try:
        # The raw rows are read twice (one count, one classification) instead of
        # cached. The classified rows are reused by the audit, checks and three
        # writes, so they are cached on disk: building in-memory columnar batches
        # of the wide original struct exhausted the heap.
        raw = read_flights(spark, assets, bronze, run_id)
        observed_raw = raw.count()
        tables = clean_flights(raw)
        classified = tables.classified.persist(StorageLevel.DISK_ONLY)
        counts = tables.audit.first().asDict()
        checks = check_accounting(counts, observed_raw)
        checks["quarantine_has_reasons"] = not tables.quarantined.filter(
            F.size("rejection_reasons") == 0).take(1)
        checks["retained_period_matches_asset"] = not tables.retained.filter(
            F.date_format("flight_date", "yyyy-MM") != args.month).take(1)
        if not all(checks.values()):
            raise ValueError(f"Flight validation failed: {checks}")
        snapshot = _input_snapshot(assets, bronze)
        csvs = assets[0].csvs
        # Audit rows describe run-level aggregates; source_file/hash are null
        # when several CSVs contribute. The full input snapshot is in _READY.
        audit_rows = [(assets[0].asset_id,
                       str(csvs[0].path.relative_to(bronze)) if len(csvs) == 1 else None,
                       csvs[0].sha256 if len(csvs) == 1 else None,
                       run_id, RULE_VERSION, metric, value) for metric, value in counts.items()]
        audit_schema = T.StructType([
            *[T.StructField(c, T.StringType(), True) for c in PROVENANCE],
            T.StructField("metric", T.StringType(), False),
            T.StructField("value", T.LongType(), False),
        ])
        audit = spark.createDataFrame(audit_rows, audit_schema).withColumn(
            "record_hash", F.sha2(F.to_json(F.struct("silver_run_id", "metric", "value")), 256)
        ).withColumn("quality_flags", F.array().cast("array<string>"))
        expected = {"flights": counts["retained"], "flights_quarantine": counts["quarantined"],
                    "audit": len(counts)}
        outputs = {"flights": tables.retained, "flights_quarantine": tables.quarantined, "audit": audit}
        for name, frame in outputs.items():
            writer = frame.write.mode("errorifexists")
            if name == "flights" and expected[name]:
                writer = writer.partitionBy("year", "month")
            writer.parquet(str(paths[name]))
            checks[f"{name}_readback_count"] = spark.read.parquet(str(paths[name])).count() == expected[name]
        # Check exact selected bytes again, even if the catalog now selects a
        # newer version. Never alter or reselect Bronze during a Silver run.
        for asset in assets:
            for source in asset.csvs:
                _verified_file(bronze, {"path": str(source.path.relative_to(bronze)),
                                       "sha256": source.sha256, "size_bytes": source.size_bytes})
        checks["input_files_unchanged"] = True
        if not all(checks.values()):
            raise ValueError(f"Flight output checks failed: {checks}")
        files = {name: [{"path": str(p.relative_to(output)), "size_bytes": p.stat().st_size}
                        for p in sorted(path.rglob("*.parquet"))] for name, path in paths.items()}
        samples = {
            "retained": [r.asDict(recursive=True) for r in tables.retained.orderBy(
                "record_hash").limit(3).collect()],
            "quarantined": [r.asDict(recursive=True) for r in tables.quarantined.orderBy(
                "record_hash").limit(3).collect()],
        }
        report = {"run_id": run_id, "git_commit": commit, "rule_version": RULE_VERSION,
                  "env": "dev", "dev_user": "alkval", "month": args.month,
                  "inputs": snapshot, "counts": counts, "checks": checks,
                  "elapsed_seconds": round(time.monotonic() - started, 3), "files": files,
                  "spark_version": spark.version,
                  "assumptions": "PR #3 defaults and flights-0.1-working; awaiting Monday review",
                  "outputs": {name: str(path.relative_to(output)) for name, path in paths.items()}}
        # Prepare all companion tables before committing one authoritative pointer.
        # Temporary runner: only the month's flights pointer is advanced. Its
        # readiness manifest identifies quarantine and audit from this exact run.
        # Separate table pointers would permit a partial cross-table publication
        # on failure. Agree the final pointer interface with the common.py owner.
        for name, path in paths.items():
            _atomic_json(path / "_READY.json", {**report, "table": name})
        _atomic_json(current_pointer(output, args.month), {
            "month": args.month,
            "run_id": run_id,
            "ready_manifest": str((paths["flights"] / "_READY.json").relative_to(output / "flights")),
        })
        print(json.dumps({**report, "samples": samples}, indent=2, default=str))
        return report
    finally:
        if classified is not None:
            classified.unpersist()
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, type=_month)
    parser.add_argument("--env", required=True, choices=["dev"])
    parser.add_argument("--dev-user", required=True, choices=["alkval"])
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run_month(args)
    except Exception:
        LOG.exception("Flight slice failed; inspect any incomplete run directories")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
