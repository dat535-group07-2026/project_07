"""NOAA Local Climatological Data (LCD v2) Silver cleaner for JFK and LaGuardia.

Units in NOAA LCD v2 bulk CSVs are already metric (degrees Celsius, millimetres,
metres per second, kilometres, whole degrees). Timestamps in the bulk CSV `DATE`
column are recorded in Local Standard Time (fixed UTC-5 year-round without DST
shifts, per decision D5) and are converted to UTC via `DATE + INTERVAL 5 HOURS`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from project07.silver import common

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

RULE_VERSION = "weather-0.1"
PROVENANCE = common.PROVENANCE

STATION_AIRPORTS: dict[str, str] = {
    "USW00094789": "JFK",
    "USW00014732": "LGA",
}
OBSERVATION_REPORT_TYPES: tuple[str, ...] = ("FM-15", "FM-16", "FM-12")
SUMMARY_REPORT_TYPES: tuple[str, ...] = ("SOD", "SOM")
ALL_REPORT_TYPES: tuple[str, ...] = (*OBSERVATION_REPORT_TYPES, *SUMMARY_REPORT_TYPES)

NOAA_COLUMNS: tuple[str, ...] = tuple(
    """
STATION DATE LATITUDE LONGITUDE ELEVATION NAME REPORT_TYPE SOURCE
HourlyAltimeterSetting HourlyDewPointTemperature HourlyDryBulbTemperature
HourlyPrecipitation HourlyPresentWeatherType HourlyPressureChange
HourlyPressureTendency HourlyRelativeHumidity HourlySkyConditions
HourlySeaLevelPressure HourlyStationPressure HourlyVisibility
HourlyWetBulbTemperature HourlyWindDirection HourlyWindGustSpeed
HourlyWindSpeed Sunrise Sunset DailyAverageDewPointTemperature
DailyAverageDryBulbTemperature DailyAverageRelativeHumidity
DailyAverageSeaLevelPressure DailyAverageStationPressure
DailyAverageWetBulbTemperature DailyAverageWindSpeed DailyCoolingDegreeDays
DailyDepartureFromNormalAverageTemperature DailyHeatingDegreeDays
DailyMaximumDryBulbTemperature DailyMinimumDryBulbTemperature
DailyPeakWindDirection DailyPeakWindSpeed DailyPrecipitation DailySnowDepth
DailySnowfall DailySustainedWindDirection DailySustainedWindSpeed DailyWeather
MonthlyAverageRH MonthlyDaysWithGT001Precip MonthlyDaysWithGT010Precip
MonthlyDaysWithGT32Temp MonthlyDaysWithGT90Temp MonthlyDaysWithLT0Temp
MonthlyDaysWithLT32Temp MonthlyDepartureFromNormalAverageTemperature
MonthlyDepartureFromNormalCoolingDegreeDays
MonthlyDepartureFromNormalHeatingDegreeDays
MonthlyDepartureFromNormalMaximumTemperature
MonthlyDepartureFromNormalMinimumTemperature
MonthlyDepartureFromNormalPrecipitation MonthlyDewpointTemperature
MonthlyGreatestPrecip MonthlyGreatestPrecipDate MonthlyGreatestSnowDepth
MonthlyGreatestSnowDepthDate MonthlyGreatestSnowfall
MonthlyGreatestSnowfallDate MonthlyMaxSeaLevelPressureValue
MonthlyMaxSeaLevelPressureValueDate MonthlyMaxSeaLevelPressureValueTime
MonthlyMaximumTemperature MonthlyMeanTemperature
MonthlyMinSeaLevelPressureValue MonthlyMinSeaLevelPressureValueDate
MonthlyMinSeaLevelPressureValueTime MonthlyMinimumTemperature
MonthlySeaLevelPressure MonthlyStationPressure MonthlyTotalLiquidPrecipitation
MonthlyTotalSnowfall MonthlyWetBulb MonthlyAverageWindSpeed
CoolingDegreeDaysSeasonToDate MonthlyCoolingDegreeDays
MonthlyNumberDaysWithSnowfall HeatingDegreeDaysSeasonToDate
MonthlyHeatingDegreeDays MonthlyNumberDaysWithThunderstorms
MonthlyNumberDaysWithHeavyFog NormalsCoolingDegreeDay NormalsHeatingDegreeDay
ShortDurationEndDate005 ShortDurationEndDate010 ShortDurationEndDate015
ShortDurationEndDate020 ShortDurationEndDate030 ShortDurationEndDate045
ShortDurationEndDate060 ShortDurationEndDate080 ShortDurationEndDate100
ShortDurationEndDate120 ShortDurationEndDate150 ShortDurationEndDate180
ShortDurationPrecipitationValue005 ShortDurationPrecipitationValue010
ShortDurationPrecipitationValue015 ShortDurationPrecipitationValue020
ShortDurationPrecipitationValue030 ShortDurationPrecipitationValue045
ShortDurationPrecipitationValue060 ShortDurationPrecipitationValue080
ShortDurationPrecipitationValue100 ShortDurationPrecipitationValue120
ShortDurationPrecipitationValue150 ShortDurationPrecipitationValue180 REM
BackupDirection BackupDistance BackupDistanceUnit BackupElements
BackupElevation BackupEquipment BackupLatitude BackupLongitude BackupName
WindEquipmentChangeDate
""".split()
)
RAW_FIELDS: tuple[str, ...] = (*NOAA_COLUMNS, "_corrupt_record")

MEASUREMENT_COLUMNS: tuple[str, ...] = (
    "HourlyDryBulbTemperature",
    "HourlyPrecipitation",
    "HourlyWindSpeed",
    "HourlyWindDirection",
    "HourlyWindGustSpeed",
    "HourlyVisibility",
)

RULES: dict[str, tuple[str, str]] = {
    "W_MALFORMED_RECORD": ("fatal", "Parser-corrupt CSV record"),
    "W_KEY_MISSING": ("fatal", "Blank station, timestamp or report type"),
    "W_KEY_INVALID": (
        "fatal",
        "Station outside JFK/LGA scope, invalid ISO timestamp or unknown report type",
    ),
    "W_DST_LABEL_CONFLICT": (
        "fatal",
        "Conflicting observations share (station_id, raw_date, report_type), including DST label defects",
    ),
    "W_SUMMARY_REPORT": (
        "scope",
        "Daily (SOD) or monthly (SOM) summary row excluded from observation model",
    ),
    "W_SUSPECT_VALUE": (
        "warning",
        "Measurement carries LCD v2 suspect value flag 's'",
    ),
    "W_TOKEN_UNPARSEABLE": (
        "warning",
        "Nonblank measurement token cannot be parsed or is sensor-missing '*'",
    ),
    "W_PHYSICAL_RANGE": (
        "warning",
        "Parsed measurement falls outside physical domain bounds",
    ),
    "W_WIND_GUST_BELOW_SPEED": (
        "warning",
        "Reported wind gust speed is smaller than sustained wind speed",
    ),
}


def raw_schema():
    """Return the explicit 126-column all-string schema for NOAA LCD v2 CSVs."""
    return common.raw_string_schema(RAW_FIELDS)


def read_weather(
    spark: SparkSession,
    assets: Sequence[common.SelectedAsset],
    bronze_root: Path,
    run_id: str,
) -> DataFrame:
    """Read verified NOAA LCD v2 CSVs from Bronze and attach Silver provenance literals."""
    return common.read_raw_csvs(
        spark,
        assets,
        bronze_root,
        run_id=run_id,
        rule_version=RULE_VERSION,
        columns=NOAA_COLUMNS,
        expected_kind="noaa",
        trailing_empty=False,
    )


def _token(name: str):
    from pyspark.sql import functions as F

    trimmed = F.trim(F.col(name))
    return F.when(F.length(trimmed) > 0, trimmed)


def _numeric_part(token, pattern: str = r"^([+-]?[0-9]+(?:\.[0-9]+)?)[sV]*$"):
    from pyspark.sql import functions as F

    extracted = F.regexp_extract(token, pattern, 1)
    return F.when(token.rlike(pattern) & (F.length(extracted) > 0), extracted)


@dataclass
class WeatherTables:
    """DataFrame-only result of weather cleaning and hourly selection."""

    classified: DataFrame

    @property
    def retained(self) -> DataFrame:
        return (
            self.classified.filter("outcome = 'retained'")
            .withColumnRenamed("_occurrences", "source_occurrences")
            .drop("original", "rejection_reasons", "scope_reasons", "outcome")
        )

    @property
    def quarantined(self) -> DataFrame:
        return (
            self.classified.filter("outcome = 'quarantined'")
            .withColumnRenamed("_occurrences", "source_occurrences")
            .drop("scope_reasons", "outcome")
        )

    @property
    def scope_excluded(self) -> DataFrame:
        return (
            self.classified.filter("outcome = 'scope_excluded'")
            .withColumnRenamed("_occurrences", "source_occurrences")
            .drop("rejection_reasons", "outcome")
        )

    @property
    def hourly(self) -> DataFrame:
        return build_hourly_weather(self.retained)

    @property
    def audit(self) -> DataFrame:
        from pyspark.sql import functions as F

        def n(condition):
            return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).cast("long")

        expressions = [
            F.coalesce(F.sum("_occurrences"), F.lit(0)).cast("long").alias("raw"),
            F.count("*").alias("unique"),
            F.coalesce(F.sum(F.col("_occurrences") - 1), F.lit(0))
            .cast("long")
            .alias("duplicate_extras"),
            n(F.col("outcome") == "scope_excluded").alias("scope_excluded"),
            n(F.col("outcome") == "retained").alias("retained"),
            n(F.col("outcome") == "quarantined").alias("quarantined"),
            n((F.col("outcome") == "retained") & (F.col("report_type") == "FM-15")).alias(
                "retained_fm15"
            ),
            n((F.col("outcome") == "retained") & (F.col("report_type") == "FM-16")).alias(
                "retained_fm16"
            ),
            n((F.col("outcome") == "retained") & (F.col("report_type") == "FM-12")).alias(
                "retained_fm12"
            ),
        ]
        for code, (severity, _) in RULES.items():
            if severity == "fatal":
                field = "rejection_reasons"
            elif severity == "scope":
                field = "scope_reasons"
            else:
                field = "quality_flags"
            expressions.append(n(F.array_contains(F.col(field), code)).alias(code))
        return self.classified.agg(*expressions)


def clean_weather(raw: DataFrame) -> WeatherTables:
    """Classify NOAA LCD v2 records, parse metric measurements, and convert LST to UTC."""
    from functools import reduce
    from pyspark.sql import Window, functions as F

    frame = common.deduplicate_with_provenance(raw, RAW_FIELDS)

    station_tok = _token("STATION")
    date_tok = _token("DATE")
    report_tok = _token("REPORT_TYPE")
    source_tok = _token("SOURCE")

    frame = (
        frame.withColumn("station_id", station_tok)
        .withColumn(
            "airport_code",
            F.when(station_tok == "USW00094789", F.lit("JFK")).when(
                station_tok == "USW00014732", F.lit("LGA")
            ),
        )
        .withColumn("raw_date", date_tok)
        .withColumn("report_type", report_tok)
        .withColumn("source_code", source_tok)
    )

    # Parse ISO timestamp YYYY-MM-DDTHH:MM:SS as TIMESTAMP_NTZ (Local Standard Time, UTC-5).
    iso_regex = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$"
    frame = frame.withColumn(
        "observation_lst",
        F.when(
            date_tok.rlike(iso_regex),
            F.expr("try_to_timestamp(raw_date, \"yyyy-MM-dd'T'HH:mm:ss\")").cast("timestamp_ntz"),
        ),
    )
    # Convert Local Standard Time (fixed UTC-5) to UTC instant: observation_lst + 5 hours.
    # Because spark.sql.session.timeZone=UTC, casting TIMESTAMP_NTZ to TIMESTAMP interprets
    # the shifted clock components directly as UTC.
    frame = frame.withColumn(
        "observation_utc",
        F.when(
            F.col("observation_lst").isNotNull(),
            F.expr(" timestampadd(HOUR, 5, observation_lst) ").cast("timestamp"),
        ),
    )
    frame = frame.withColumn(
        "weather_hour_utc",
        F.when(F.col("observation_utc").isNotNull(), F.date_trunc("hour", F.col("observation_utc"))),
    )
    # Partition observations by year and month of the source DATE (contract item 9).
    frame = frame.withColumn("year", F.year("observation_lst")).withColumn(
        "month", F.month("observation_lst")
    )

    # Parse measurements and special tokens.
    temp_tok = _token("HourlyDryBulbTemperature")
    temp_num = _numeric_part(temp_tok, r"^([+-]?[0-9]+(?:\.[0-9]+)?)s?$")
    frame = frame.withColumn("temperature_c", temp_num.cast("double"))

    precip_tok = _token("HourlyPrecipitation")
    precip_num = _numeric_part(precip_tok, r"^([+-]?[0-9]+(?:\.[0-9]+)?)s?$")
    frame = frame.withColumn("precipitation_mm", precip_num.cast("double")).withColumn(
        "precipitation_trace",
        F.when(precip_tok == "T", F.lit(True)).when(precip_num.isNotNull(), F.lit(False)),
    )

    ws_tok = _token("HourlyWindSpeed")
    ws_num = _numeric_part(ws_tok, r"^([+-]?[0-9]+(?:\.[0-9]+)?)s?$")
    frame = frame.withColumn("wind_speed_mps", ws_num.cast("double"))

    wd_tok = _token("HourlyWindDirection")
    wd_num = _numeric_part(wd_tok, r"^([+-]?[0-9]+(?:\.0+)?)[sV]*$")
    wd_int = wd_num.cast("decimal(10,0)").cast("int")
    frame = frame.withColumn("wind_direction_deg", wd_int).withColumn(
        "wind_direction_variable",
        F.when(wd_tok == "VRB", F.lit(True)).when(wd_int.isNotNull(), F.lit(False)),
    )

    wg_tok = _token("HourlyWindGustSpeed")
    wg_num = _numeric_part(wg_tok, r"^([+-]?[0-9]+(?:\.[0-9]+)?)s?$")
    frame = frame.withColumn("wind_gust_mps", wg_num.cast("double"))

    vis_tok = _token("HourlyVisibility")
    vis_num = _numeric_part(vis_tok, r"^([+-]?[0-9]+(?:\.[0-9]+)?)[sV]*$")
    frame = frame.withColumn("visibility_km", vis_num.cast("double")).withColumn(
        "visibility_variable",
        F.when(vis_num.isNotNull(), vis_tok.contains("V")),
    )

    # Identify unparseable tokens vs suspect flags vs physical range violations.
    temp_unparseable = temp_tok.isNotNull() & temp_num.isNull()
    precip_unparseable = precip_tok.isNotNull() & precip_num.isNull() & (precip_tok != "T")
    ws_unparseable = ws_tok.isNotNull() & ws_num.isNull()
    wd_unparseable = wd_tok.isNotNull() & wd_int.isNull() & (wd_tok != "VRB")
    wg_unparseable = wg_tok.isNotNull() & wg_num.isNull()
    vis_unparseable = vis_tok.isNotNull() & vis_num.isNull()

    suspect_flags = [
        (temp_num.isNotNull() & temp_tok.contains("s")),
        (precip_num.isNotNull() & precip_tok.contains("s")),
        (ws_num.isNotNull() & ws_tok.contains("s")),
        (wd_int.isNotNull() & wd_tok.contains("s")),
        (wg_num.isNotNull() & wg_tok.contains("s")),
        (vis_num.isNotNull() & vis_tok.contains("s")),
    ]

    physical_checks = [
        F.col("temperature_c").isNotNull() & ~F.col("temperature_c").between(-60.0, 60.0),
        F.col("precipitation_mm").isNotNull() & ~F.col("precipitation_mm").between(0.0, 500.0),
        F.col("wind_speed_mps").isNotNull() & ~F.col("wind_speed_mps").between(0.0, 100.0),
        F.col("wind_direction_deg").isNotNull() & ~F.col("wind_direction_deg").between(0, 360),
        F.col("wind_gust_mps").isNotNull() & ~F.col("wind_gust_mps").between(0.0, 120.0),
        F.col("visibility_km").isNotNull() & ~F.col("visibility_km").between(0.0, 100.0),
    ]

    key_missing = station_tok.isNull() | date_tok.isNull() | report_tok.isNull()
    key_invalid = (
        (station_tok.isNotNull() & F.col("airport_code").isNull())
        | (date_tok.isNotNull() & F.col("observation_lst").isNull())
        | (report_tok.isNotNull() & ~report_tok.isin(*ALL_REPORT_TYPES))
    )
    key_valid = ~(key_missing | key_invalid)
    is_summary = key_valid & F.col("report_type").isin(*SUMMARY_REPORT_TYPES)

    # Detect duplicate/conflicting observation keys (station_id, raw_date, report_type),
    # which capture the March and November DST timestamp collisions in LCD v2.
    obs_key_window = Window.partitionBy("station_id", "raw_date", "report_type")
    frame = frame.withColumn("_obs_key_count", F.count("*").over(obs_key_window))

    conditions = {
        "W_MALFORMED_RECORD": F.col("_corrupt_record").isNotNull(),
        "W_KEY_MISSING": key_missing,
        "W_KEY_INVALID": key_invalid,
        "W_DST_LABEL_CONFLICT": key_valid & (~is_summary) & (F.col("_obs_key_count") > 1),
        "W_SUMMARY_REPORT": is_summary,
        "W_SUSPECT_VALUE": reduce(lambda a, b: a | b, suspect_flags),
        "W_TOKEN_UNPARSEABLE": reduce(
            lambda a, b: a | b,
            [
                temp_unparseable,
                precip_unparseable,
                ws_unparseable,
                wd_unparseable,
                wg_unparseable,
                vis_unparseable,
            ],
        ),
        "W_PHYSICAL_RANGE": reduce(lambda a, b: a | b, physical_checks),
        "W_WIND_GUST_BELOW_SPEED": (
            F.col("wind_speed_mps").isNotNull()
            & F.col("wind_gust_mps").isNotNull()
            & (F.col("wind_gust_mps") < F.col("wind_speed_mps"))
        ),
    }

    for severity, col_name in (
        ("fatal", "rejection_reasons"),
        ("scope", "scope_reasons"),
        ("warning", "quality_flags"),
    ):
        frame = frame.withColumn(
            col_name,
            F.filter(
                F.array(
                    *[
                        F.when(F.coalesce(conditions[code], F.lit(False)), F.lit(code))
                        for code, (level, _) in RULES.items()
                        if level == severity
                    ]
                ),
                lambda x: x.isNotNull(),
            ),
        )

    frame = frame.withColumn(
        "outcome",
        F.when(F.size("rejection_reasons") > 0, F.lit("quarantined"))
        .when(F.size("scope_reasons") > 0, F.lit("scope_excluded"))
        .otherwise(F.lit("retained")),
    )

    return WeatherTables(
        frame.select(
            *PROVENANCE,
            "record_hash",
            "station_id",
            "airport_code",
            "raw_date",
            "observation_lst",
            "observation_utc",
            "weather_hour_utc",
            "report_type",
            "source_code",
            "temperature_c",
            "precipitation_mm",
            "precipitation_trace",
            "wind_speed_mps",
            "wind_direction_deg",
            "wind_direction_variable",
            "wind_gust_mps",
            "visibility_km",
            "visibility_variable",
            "year",
            "month",
            "quality_flags",
            "rejection_reasons",
            "scope_reasons",
            "outcome",
            "original",
            "_occurrences",
        )
    )


def build_hourly_weather(retained_observations: DataFrame) -> DataFrame:
    """Build the unique hourly weather table from retained routine (FM-15) observations.

    Produces at most one row per (station_id, weather_hour_utc) using a deterministic
    ordering that prefers the routine :51 observation, then the latest observation in
    the hour, then source_code descending, then record_hash ascending.
    Partitions by year and month of weather_hour_utc per contract item 9.
    """
    from pyspark.sql import Window, functions as F

    fm15 = retained_observations.filter(F.col("report_type") == "FM-15")
    hour_window = Window.partitionBy("station_id", "weather_hour_utc")
    rank_window = hour_window.orderBy(
        F.abs(F.minute("observation_utc") - F.lit(51)).asc(),
        F.col("observation_utc").desc(),
        F.coalesce(F.col("source_code"), F.lit("")).desc(),
        F.col("record_hash").asc(),
    )

    selected = (
        fm15.withColumn("fm15_reports_in_hour", F.count("*").over(hour_window).cast("int"))
        .withColumn("_rank", F.row_number().over(rank_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn("year", F.year("weather_hour_utc"))
        .withColumn("month", F.month("weather_hour_utc"))
    )

    return selected.select(
        *PROVENANCE,
        "record_hash",
        "station_id",
        "airport_code",
        "weather_hour_utc",
        "observation_utc",
        "observation_lst",
        "raw_date",
        "report_type",
        "source_code",
        "fm15_reports_in_hour",
        "temperature_c",
        "precipitation_mm",
        "precipitation_trace",
        "wind_speed_mps",
        "wind_direction_deg",
        "wind_direction_variable",
        "wind_gust_mps",
        "visibility_km",
        "visibility_variable",
        "year",
        "month",
        "quality_flags",
        "source_occurrences",
    )

