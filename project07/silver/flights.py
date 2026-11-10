"""BTS On-Time Performance Silver cleaner for the national monthly flight files.

The BTS directive defines the clocks, minute fields and operational states:
https://www.bts.gov/topics/airlines-and-airports/number-31-technical-directive-time-reporting-effective-jan-1-2019
The rule severities are our own choices, not BTS requirements.

No time zone is assigned here, because the file covers every US airport.
Local datetimes use Spark TIMESTAMP_NTZ. Reading the catalog, running and
publishing are in common.py and run.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from project07.silver import common

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


RULE_VERSION = "flights-0.2-working"
PROVENANCE = common.PROVENANCE
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


def raw_schema():
    """Return the explicit all-string schema for the BTS CSV."""
    return common.raw_string_schema(RAW_FIELDS)


def read_flights(spark: SparkSession, assets: Sequence[common.SelectedAsset], bronze_root: Path,
                 run_id: str) -> DataFrame:
    """Read verified BTS CSVs from Bronze as text and attach the provenance columns.

    Each BTS line ends with a comma, so the header has an empty 110th field.
    """
    return common.read_raw_csvs(
        spark, assets, bronze_root, run_id=run_id, rule_version=RULE_VERSION,
        columns=BTS_COLUMNS, expected_kind="bts", trailing_empty=True,
    )


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
    There are no scope exclusions, so no valid airport is filtered out.
    """
    from pyspark.sql import Window, functions as F

    # Exact duplicates are grouped on the complete raw row, not on a hash or a
    # pruned column set. This also checks the ANSI and time zone settings.
    frame = common.deduplicate_with_provenance(raw, RAW_FIELDS)
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
