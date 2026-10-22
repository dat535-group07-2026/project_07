# Project decisions

This log records the project-wide choices that Silver and Gold build on, and the
evidence behind them. It was started on 22 October 2026. Each entry records:

- its status: agreed, or proposed and awaiting confirmation
- how its evidence was checked: once, or independently re-checked in a second
  review

Add new decisions at the end. Do not rewrite old ones; mark them as superseded
instead.

Project deadline, including the report: 20 November 2026.

## D1. Spark runtime: Spark 3.5.0 on Java 8

Status: agreed by Alexander. Awaiting confirmation by Christopher.

- Use the project virtual environment `/home/ubuntu/project_07/.venv`
  (Python 3.11.15, PySpark 3.5.0). It matches `/opt/spark` (Spark 3.5.0,
  Scala 2.12, Hadoop 3.3.4) and the only installed JDK, OpenJDK 1.8.0_502.
- Do not use or modify the shared `/home/ubuntu/spark-env`. It contains
  PySpark 4.2.0, which needs Java 17. Starting a session there fails with
  `UnsupportedClassVersionError` (class file version 61).
- Before running Spark, set `JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64` and
  unset `PYSPARK_DRIVER_PYTHON` and `PYSPARK_DRIVER_PYTHON_OPTS`. The VM's
  `~/.bashrc` points them at Jupyter.
- The VM has 4 cores and about 7 GB of RAM.

Smoke tests on the VM (checked once, 22 October 2026). Configuration: session
time zone UTC, ANSI mode off.

- Parquet write and read-back: types, values, nulls, negative numbers, empty
  arrays, exact UTC timestamps and `year=/month=` partition directories all
  survive.
  - Nullability does not survive. Every field, including array elements, is
    read back as nullable. This is documented Spark behaviour for Parquet.
  - Consequence: Silver must enforce required fields with explicit
    validation, not rely on the schema.
- Reading the selected October 2025 BTS CSV with Spark: 605,844 rows, matching
  an independent Python count, with 0 corrupt records and no misaligned
  fields.
  - Schema used: all strings, with the unnamed 110th column named
    `_trailing_empty`, plus `_corrupt_record`; `PERMISSIVE` mode.
  - Every `OriginCityName` value contains a quoted comma.

Still open: whether parsing runs with ANSI mode on. Either way, parsing rules
should validate tokens explicitly and not rely on silent casts to null.

## D2. Bronze schedule paused

Status: agreed. Implemented in pull request #1, merged on 22 October 2026.

- The monthly cron is commented out in `.github/workflows/bronze-ingestion.yml`.
  The workflow runs only when dispatched manually.
- Reason: an unbounded `new` run on 1 November would have checked every month
  from 2025-11 onward and downloaded the 2025 NOAA files again. That could have
  changed the selected weather version while Silver is developed on fixed
  inputs.
- Re-enable the schedule only with a bounded `--since`/`--through` policy.

## D3. Evaluation window: June to October 2025

Status: agreed by Alexander. Awaiting confirmation by Christopher.

| Input | Catalog assets | Rows | Raw CSV bytes |
| --- | --- | --- | --- |
| BTS flights | `bts:2025-06` to `bts:2025-10` | 3,013,664 | 1,365,048,769 |
| NOAA weather | `noaa:USW00094789:2025`, `noaa:USW00014732:2025` | not yet counted by Silver | 18,072,054 |
| Total | | | 1,383,120,823 |

The byte total was independently re-checked.

- National Silver flights cover all airports. Weather enrichment and Gold cover
  JFK and LGA.
- Daylight saving time:
  - JFK and LGA scheduled clocks in this window are all on Eastern Daylight
    Time (UTC-4).
  - National flights span several time zones, and some of those zones do not
    observe daylight saving time.
  - Actual departure times must be placed on their actual date. A flight
    scheduled in late October that is delayed past 2 November 2025 at 02:00
    departs on standard time. This edge case needs a fixture test.
- Weather Silver keeps the full annual observations. Limiting them to June to
  October happens at enrichment as a documented scope filter, not as a
  rejection.

## D4. Silver work split

Status: as written in the local Silver planning document. Awaiting
confirmation by Christopher.

| Work package | Owner | Reviewer |
| --- | --- | --- |
| National BTS cleaning (`flights.py`) and tests | Alexander Kvalvaag | Christopher Liebich Kolle |
| NOAA cleaning (`weather.py`) and tests | Christopher Liebich Kolle | Alexander Kvalvaag |
| Catalog reader and output/run contract (`common.py`) | Christopher Liebich Kolle | Alexander Kvalvaag |
| Airport mapping, time alignment, enrichment | Both, after both cleaners work | Both |

The weather package together with `common.py` is the larger share.
Rebalance after the first thin implementations if needed.

## D5. NOAA LCD v2 `DATE` is Local Standard Time (fixed UTC-5)

Status: supported by evidence. Awaiting confirmation by Christopher, who owns
weather.

Evidence:

1. FM-12 (SYNOP) reports are issued every three hours in UTC. In both station
   files they appear at `DATE` hours 01, 04, 07, ..., 22 throughout the year.
   That is a constant UTC-5 offset with no seasonal shift.
   - Independently re-checked for June to October at both stations.
   - March and November show exceptions (see below).
2. JFK FM-15 temperatures were compared with the Iowa Environmental Mesonet
   METAR archive, which is in UTC. They match at +5 hours in January and in
   July. Checked once.
   - For example, `DATE` 2025-07-15T00:51 has 24.4 °C, which matches UTC
     05:51 (24.44 °C), not UTC 04:51 (23.89 °C).

Rules that follow:

- Convert with `observation_utc = DATE + 5 hours`. Do not convert `DATE` with
  `America/New_York`.
- BTS clocks are local civil time. JFK and LGA are on UTC-4 throughout the
  evaluation window. Aligning flights and weather on local clock hours would
  therefore be off by one hour. The alignment tests must cover this case.

Source defect around the daylight-saving changes (JFK, checked once against
METAR):

- 8 to 9 March 2025:
  - `2025-03-08T21:51` appears twice, with 5.0 °C and 4.4 °C.
  - `2025-03-09T02:51` is missing.
  - About five labels are offset by 6 hours instead of 5.
- 1 to 2 November 2025:
  - `2025-11-02T01:51` appears twice.
  - The labels just before it are offset by 4 hours instead of 5.
- Silver should detect these labels with rules and flag or quarantine them
  with a stable reason code. It must never keep an arbitrary first row.
- Both periods fall outside D3, but tests and the audit should still cover
  them.

Still to confirm against the LCD v2 documentation:

- The documentation states that the CSV files use metric units. Take each
  field's unit from the CSV-specific table, not from descriptions of the PDF
  forms.
- Which period `HourlyPrecipitation` covers.
- What the flags `s`, `V`, `*`, `T` and `VRB` mean.
- How the report types (FM-12, FM-15, FM-16, SOD, SOM) are defined.
- The LGA file has not been profiled in detail.

## D6. Measured BTS data quality

Status: measurements. They were taken by one full scan with Python's `csv`
module and have not been independently re-checked.

| Check (June to October 2025) | Result |
| --- | --- |
| Fields per row | 110 in every row |
| Exact duplicate rows | 0 |
| Candidate-key collisions (FlightDate, Reporting_Airline, Flight_Number_Reporting_Airline, Origin, Dest, CRSDepTime) | 0 |
| `ActualElapsedTime` vs `AirTime + TaxiIn + TaxiOut`, completed flights | 0 mismatches |
| `Cancelled`/`Diverted` values other than `0.00`/`1.00` | 0 |
| Departure clock inconsistent with scheduled time plus delay | 2 (August) |
| Completed flight missing a duration, arrival or delay field | 1 (September) |
| Cancelled flight with `DepTime` but blank `DepDelay` | 168 |
| `DepTime` = `2400`, per month | 44 to 103 |
| Departure delay of 24 hours or more, per month | 42 to 82 |

- The README's figure of 12 elapsed-time discrepancies came from a June 2024
  inspection. It does not describe the 2025 inputs.
- A low flight rejection rate is a valid result. Report it as measured rather
  than adding rules to reject more rows.
- Most of the flight work is reconstructing dates and times, modelling the
  cancelled and diverted states, and reconciling row counts.

## D7. Self-hosted runner exposure

Status: agreed. Implemented on 22 October 2026.

- The repository's fork pull-request approval policy is set to require
  approval for all external contributors.
- This stops fork pull requests from running automatically. Approved code still
  runs on the VM with the `ubuntu` user's access, so read any workflow change
  before approving it.
