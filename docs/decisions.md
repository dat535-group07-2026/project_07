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

Status: agreed by both.

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

Status: agreed by both.

| Input | Catalog assets | Rows | Raw CSV bytes |
| --- | --- | --- | --- |
| BTS flights | `bts:2025-06` to `bts:2025-10` | 3,013,664 | 1,365,048,769 |
| NOAA weather | `noaa:USW00094789:2025`, `noaa:USW00014732:2025` | 26,185 | 18,072,054 |
| Total | | 3,039,849 | 1,383,120,823 |

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
    departs on standard time.
  - Known limitation: Silver adds the delay to the local scheduled clock
    without a time zone, so such a flight would be one hour off and get the
    `F_CLOCK_DELAY_MISMATCH` warning. No flight in our data crosses that
    change, and Gold uses the scheduled time, not the actual time. We did not
    add a fixture test for it.
- Weather Silver keeps the full annual observations. Limiting them to June to
  October happens at enrichment as a documented scope filter, not as a
  rejection.

## D4. Silver work split

Status: agreed. Final ownership:

- Alexander: Bronze, flights, enrichment and Gold.
- Christopher: weather, `common.py`, config/Spark, the Silver runner and CI.

## D5. NOAA LCD v2 `DATE` is Local Standard Time (fixed UTC-5)

Status: supported by evidence and agreed by both. Christopher built the weather
Silver on it.

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

## D8. Measured NOAA gaps and the Gold precipitation field

Status: measured on 8 November 2026 from the 2025 LCD v2 files and the first
Gold dev run (`20261108T174718Z-fa21d594`).

| Check, JFK and LGA | Result |
| --- | --- |
| FM-15 reports on 30 and 31 August and 1 September 2025 | 0 at both stations |
| Gold airport-days without any weather | 6 (those three days at both airports) |
| Flights matched to hourly weather, June to October | 99,591 of 102,640 |
| `HourlyPrecipitation` with a value, September (JFK / LGA) | 86 of 670 / 72 of 670 |
| `HourlyPrecipitation` with a value, October (JFK / LGA) | 98 of 739 / 76 of 738 |

- Precipitation reporting changes at raw `DATE` 2025-08-26T23:51:00. Both
  stations report `0.00` at 22:51 and a blank at 23:51. After that, neither
  station has an explicit zero in September or October.
- NOAA treats a blank as missing or unreported, not as zero. The files do not
  explain why reporting changed.
- `reported_precipitation_mm` sums the available numeric hourly precipitation
  reports. Trace amounts are counted separately in `trace_hours`. Missing
  reports stay missing, so an incomplete day does not give a complete daily
  rainfall total. `precipitation_hours` shows how many hours the sum covers.
- Correlations are calculated with SQL `CORR`, which skips missing pairs.
  Spark's `DataFrame.stat.corr` replaces nulls with zero, so it is only used
  after the missing pairs are removed.
- Daily reported precipitation against average departure delay, both airports
  together: r = 0.5438 over the 206 airport-days with a value, and r = 0.4366
  over the 168 airport-days where all 24 hours have a numeric or trace report.
  These are exploratory associations, not causes.

## D9. Performance measurement

Status: measured on 10 November 2026. Use 8 shuffle partitions for VM batch jobs.

- VM: 4 vCPUs, 7.76 GiB RAM, no swap; Java 8, Python 3.11.15,
  PySpark 3.5.0. The jobs used `local[*]`, a 3 GB driver, UTC, ANSI off
  and adaptive query execution enabled.
- Input: all national flights for June 2025, selected from the Bronze catalog.
  The benchmark used the cleaners from main `091a606` without changing rules.
- `scripts/benchmark_flights.py` runs each configuration three times, with a
  fresh Python process and Spark JVM each time. It reads only: no Silver or
  Gold tables, manifests or pointers are written. Spark uses temporary scratch
  space and a disk-only cache of the classified rows, as the Silver runner does.

Reproduce from the checkout, with the Java environment set as in the README:

```sh
.venv/bin/python -m scripts.benchmark_flights --shuffle-partitions 200
.venv/bin/python -m scripts.benchmark_flights --shuffle-partitions 8
```

Times in seconds:

| Step | 200 partitions, min | 200, median | 8 partitions, min | 8, median |
| --- | ---: | ---: | ---: | ---: |
| Select and hash-check inputs | 0.310 | 0.436 | 0.303 | 0.320 |
| Read flights, including field-width check | 7.494 | 8.157 | 7.386 | 7.870 |
| Clean and materialize classified rows | 35.805 | 36.276 | 33.247 | 33.253 |
| Aggregate the audit from cached rows | 1.318 | 1.343 | 1.280 | 1.376 |
| Total measured steps | 45.716 | 46.891 | 42.816 | 42.993 |

The total is calculated for each run before taking the median. It excludes
Spark startup, shutdown, plan printing and output writes, so it is not a full
Silver run time. Reading the CSV payload is lazy: the read step checks line
widths and builds the reader; parsing its rows happens during materialization.
The classified DataFrame is persisted before `count()`, forcing Spark to
compute all output columns and validation rules rather than prune them away.

Classification was the largest measured step. The formatted execution plan
shows a shuffle for full-row duplicate grouping, followed by a shuffle and
sort for the candidate-key window. It also includes the parsing and validation
expressions. The plan explains the work involved, but does not time those
operations separately. Adaptive execution coalesces some shuffle partitions,
so the configured partition count is not always the final task count.

We tested one change: reducing `spark.sql.shuffle.partitions` from 200 to 8,
with two partitions per VM core as a starting point for this monthly input.
All six comparison runs returned 611575 classified rows and the same 19 audit
counts, including all 13 rule counts: 30 cancellation-with-departure-time
warnings, no other rule flags, no duplicate copies and no quarantined rows.

The median measured total fell from 46.891 to 42.993 seconds (8.3%), and
classification from 36.276 to 33.253 seconds (8.3%). All three eight-partition
runs (42.816–43.281 seconds) were faster than all three baseline runs
(45.716–47.099 seconds). We kept the change in `get_spark` for non-test jobs;
`local[2]` and two shuffle partitions are unchanged in tests. Weather and Gold
use this batch default too, but their performance was not measured here.

These are three runs per configuration on a shared VM, not a general scaling
result. File caches were left warm and configurations were not interleaved.
An initial baseline overlapped dependency installation and was excluded and
repeated; neither configuration in the table overlapped our other Spark jobs
or dependency installation. We did not stop other users' services. A larger
input or a different machine may need a different partition count.

The comparison's per-run results and execution plans remain on the VM in
`/home/ubuntu/dev/alkval/benchmark-flights-baseline.log` and
`/home/ubuntu/dev/alkval/benchmark-flights-shuffle8.log`. The excluded initial
baseline is kept separately as `benchmark-flights-baseline-initial.log`.

Full-run check, separate from the benchmark above: on 10 November we also ran
complete development runs, including output writes and read-back checks, once
with each setting. With 200 versus eight shuffle partitions, June flights took
59.744 versus 55.073 seconds and produced seven versus eight flight Parquet
files. Both retained 611,575 rows, with identical values after excluding
`silver_run_id`. The following Gold runs took 26.061 versus 22.039 seconds and
each produced one file with 306 rows, covering 102,640 flights and 99,591
weather matches. All checks passed. These single runs support keeping eight
partitions on our VM, but do not establish a repeatable full-run speed-up.
Weather cleaning was not measured.
