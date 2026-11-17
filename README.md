# BTS Flight Operations and Airport Weather

DAT535 course project (Group 07) by Alexander Kvalvaag and Christopher Liebich Kolle.

We are building a PySpark batch pipeline that combines Bureau of Transportation Statistics (BTS) flight operations with National Oceanic and Atmospheric Administration (NOAA) airport weather data. For every departure from John F. Kennedy International Airport (JFK) and LaGuardia Airport (LGA) it finds the weather at departure, the inbound flight that brought the same plane, and the weather where that flight came from. Gold summarises this per day, per hour and by how late the inbound plane was, with data-quality and weather-coverage metrics.

The primary focus is on data engineering within a Medallion architecture: robust ingestion, explicit domain modelling, anomaly handling, consistency verification, scalable Spark transformations, and reproducible automated runs.

---

## Current Status

Status as of 17 November 2026 (deadline, including the report: 20 November 2026):

- **Window: January 2025 – August 2026** (D10), extended from June–October 2025 after the presentation feedback.
- **Bronze: implemented and tested.** 20 national BTS months (11,730,351 flight rows) and NOAA LCDv2 files for 32 stations, 2025 and 2026 (780,471 rows), ingested, checksummed and catalogued. See [docs/bronze-ingestion.md](docs/bronze-ingestion.md).
- **Silver: implemented and tested.** Flights (all national rows, including BTS delay causes) and weather (32 stations, each with its own UTC offset) are cleaned, validated, quarantined where needed and published with row accounting. `departures_enriched` links every JFK/LGA departure to the inbound flight of the same plane, with weather at both ends (D13).
- **Gold: implemented and tested.** Three tables: `daily_airport`, `airport_hour` and `departure_chain` (D15), plus `scripts/gold_figures.py` for the report figures.
- **Dev runs (17 November 2026):** 397,146 JFK/LGA departures, 98.9% matched to hourly weather, 325,198 linked to their inbound flight (85.1% of the departures that were not cancelled). Every Silver and Gold check passed.
- **Automation:** `bronze-to-gold.yml` runs monthly and only re-runs what changed (D15).
- **Pending:** the first production (`prod`) run of the extended pipeline and its first scheduled run.
- **Decisions and evidence:** runtime, windows, time conventions, the airport reference table, the chaining rule and measured data quality are recorded in [docs/decisions.md](docs/decisions.md).
- **Historical inspection:** An earlier inspection of June 2024 inputs (611,132 flights; 5,465 next-day departures, 84 `2400` departures, 12 elapsed-time discrepancies) guided the design. These figures do not describe the 2025 inputs.

---

## Scope and Boundaries

### In Scope
- **Flight cleaning & validation:** Full national monthly BTS Reporting Carrier On-Time Performance files.
- **Weather enrichment & analysis:** Station-level observations for JFK, LGA and the 30 airports that send the most planes to them.
- **Chaining:** each JFK/LGA departure linked to the inbound flight of the same plane (by tail number).
- **Temporal coverage:** January 2025 – August 2026, kept up to date monthly.
- **Outputs:** Daily and hourly flight operations, delay propagation from inbound flights, weather at both ends, cancellation statistics, and data quality/coverage reports.

### Out of Scope (Non-Goals)
- No predictive machine learning models or forecasting.
- No real-time streaming pipeline (batch processing only).
- No web application or interactive user dashboard.
- No reconstruction of multi-leg diversion itineraries.
- No causal claims: the project documents empirical associations between weather metrics and delays, not causality.

---

## Datasets and Sources

| Source | Dataset | Role | Primary Fields |
| --- | --- | --- | --- |
| **BTS** | Reporting Carrier On-Time Performance | Flight operations, delays, and cancellations | Flight date, carrier ID, flight number, origin, destination, scheduled/actual local times, departure delays, elapsed times, taxi times, cancellation codes, diversion indicators |
| **NOAA** | Local Climatological Data version 2 (LCDv2) | Airport weather observations | Station identifier, timestamp, report type, temperature, wind speed/direction, visibility, precipitation |

### Data Management Principles
- **Off-repository storage:** Large raw archives and Parquet tables are kept outside Git in dedicated data directories.
- **Provenance:** Official bulk releases, URLs, retrieval timestamps, file sizes, and SHA-256 checksums are tracked in run manifests.
- **Airport reference table:** `project07/reference/airports.csv` gives each of 113 airports its IANA time zone and, for 32 of them, its NOAA LCDv2 station (for example JFK `USW00094789`, LGA `USW00014732`, San Juan `RQW00011641`). Bronze, Silver weather and chaining all read it. The time zones are checked against BTS with `scripts/check_airport_time_zones.py` (D11).
- **Clocks:** BTS clock times are local civil time with daylight saving. LCDv2 `DATE` values are each station's local standard time all year, without daylight saving (D5, D11).

---

## Architecture and Pipeline

The pipeline follows a three-stage Medallion architecture using PySpark:

```
[ BTS Monthly Files ]      [ NOAA LCDv2 Stations ]
          │                           │
          ▼                           ▼
┌──────────────────────────────────────────────────┐
│              Bronze (Preservation)               │
│  - Raw archives preserved unchanged             │
│  - Manifests: URLs, timestamps, SHA-256 hashes   │
│  - Strict schema & corruption validation        │
└─────────────────────────┬────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│          Silver (Cleaning & Enrichment)          │
│  - Exact-duplicate removal & row accounting      │
│  - Conflicting candidate keys quarantined        │
│  - Operational states (cancellations/diversions) │
│  - Departure date & 2400/rollover reconstruction │
│  - UTC conversion per station and airport        │
│  - Elapsed time & delay-cause consistency checks │
│  - Hourly routine weather parsing (trace/tokens) │
│  - departures_enriched: weather at departure,    │
│    inbound flight of the same plane, and the     │
│    weather where it came from                    │
│  - Quarantined records & audit tables            │
└─────────────────────────┬────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│        Gold (Daily, Hourly, Chain Tables)        │
│  - daily_airport, airport_hour, departure_chain  │
│  - Delay metrics and cancellation counts         │
│  - Reconciled counts, Parquet & manifests        │
└──────────────────────────────────────────────────┘
```

### Bronze: Ingestion and Preservation

- Raw BTS `.zip` archives and NOAA LCDv2 CSV files are stored unmodified in the Bronze storage layer.
- A **catalog** records every source version with its URL, retrieval timestamp, file size, and SHA-256 checksum. A **run manifest** records each run's mode, requested months, selected inputs, and outcome.
- Archive members are inventoried during extraction, keeping unpacked working copies separated.
- **Failure policy:** Missing source files, corrupt downloads, or unexpected schemas trigger immediate job failures rather than silently dropping records or degrading coverage. In new-month mode, an availability-check 404 records an unreleased month as pending.
- **Idempotence & Updates:** The monthly Bronze-to-Gold workflow checks for newly released months from 2025-01 (D15). Cached files are verified and reused unless a refresh is requested; the growing NOAA year files are refreshed when a new flight month arrives. Refreshes download the source before comparing hashes. Backfills and manual runs process historical periods on demand.
- **Stations:** one NOAA file per station and year for every station in the airport reference table (32 stations).

### Silver: Cleaning, Model, and Verification

Code: `project07/silver/flights.py`, `weather.py`, `enrichment.py`, `chain.py`, with shared reading, accounting and publishing in `common.py`, the airport reference table in `project07/reference/`, and the command-line runner in `run.py`.

Silver models the domain, cleans raw fields, isolates invalid data, and joins flight operations with weather observations:

- **Reading:** Every column is read as text with an explicit schema. Before parsing, the reader checks the file's checksum, the exact header and the number of fields on every line.
- **Data model and keys:**
  - BTS does not provide a single universal flight ID. We use a composite candidate key: `FlightDate`, `Reporting_Airline`, `Flight_Number_Reporting_Airline`, `Origin`, `Dest`, and `CRSDepTime`.
  - Exactly identical rows (every original field equal) are collapsed into one, and the number of copies is kept.
  - Different records that share a valid candidate key are quarantined rather than resolved arbitrarily.
  - Clean tables, quarantined records (with the original payload and reasons) and an audit table are stored as Parquet with source provenance.
- **Flight rules:** 14 rules (rule version `flights-0.3`). Six are fatal and send the row to quarantine: a malformed record, a missing or invalid key, an invalid cancelled/diverted flag, a flight that is both cancelled and diverted, and a key conflict. Eight are warnings that keep the row with a quality flag, for example a completed flight that lacks an actual-time field, an unparseable optional number, a departure clock that disagrees with the rebuilt departure, or BTS delay-cause minutes that are only partly filled or do not add up to the arrival delay.
- **Delay causes:** BTS's carrier, weather, NAS, security and late-aircraft delay minutes are kept as typed columns.
- **Operational states and typing:**
  - Explicit parsing of dates, whole minutes, clocks and flags. Unparseable values become null and are flagged, never zero-filled.
  - Cancelled flights may legitimately report departure times, and diverted flights legitimately lack arrival and elapsed-time values.
- **Departure time reconstruction:**
  - BTS writes clock times as `HHMM`, where `2400` means midnight at the end of the day. The parser accepts `0000`–`2359` and `2400`.
  - The actual departure is the scheduled local time plus the signed departure delay, checked against the reported actual clock. This handles midnight rollovers, multi-day delays and early departures.
  - For JFK and LGA, the scheduled departure is converted to UTC with New York's daylight-saving rules during enrichment.
- **Consistency checks:**
  - For completed flights, `ActualElapsedTime == AirTime + TaxiIn + TaxiOut` is checked. The Silver runs for January 2025 – August 2026 found no discrepancies (the June 2024 inspection found 12).
- **Weather preprocessing:**
  - LCDv2 rows mix routine hourly (`FM-15`), special (`FM-16`), synoptic (`FM-12`) and daily/monthly summary (`SOD`, `SOM`) reports. Summary rows are counted as out of scope.
  - `weather_hourly` keeps one `FM-15` report per station and UTC hour: the one closest to the station's usual report minute (51 at JFK and LGA, 51–56 elsewhere; D12). It is published with one pointer per year.
  - Token handling: trace precipitation (`T`) is kept as a flag with a null amount, suspect (`s`) values are flagged, variable wind directions and visibilities are marked, and `thunderstorm` is set when the present-weather codes contain `TS`.
  - Timestamps: `DATE` is converted to UTC with the station's own standard-time offset (+5 hours for New York), not with daylight-saving rules. Every run checks the offset from the data: FM-12 reports must land on 3-hourly UTC hours (D11). Rows that share the same station, `DATE` and report type, such as duplicated labels at the daylight-saving changes, are quarantined, and so are unknown report types (for example NOAA's new `FM94_1` from April 2026). Shifted or missing labels are not detected.
  - Measured gaps (D8): no hourly reports on 30–31 August and 1 September 2025 at JFK or LGA, and hourly precipitation is mostly blank from 26 August 2025 (D14).
- **Enrichment and chaining (`departures_enriched`, D13):**
  - JFK and LGA departures are left-joined to `weather_hourly` on airport and the UTC hour of the scheduled departure, so a flight scheduled for 14:10 UTC can match a 14:51 observation from the same hour. Flights without a weather match are kept with empty weather columns. The join fails if `weather_hourly` has two rows for the same airport and hour, so flights cannot be multiplied.
  - Each departure that was not cancelled is linked to the inbound flight of the same plane: same tail number, landed at the same airport, not cancelled or diverted, due at most 12 hours before and after the plane's previous departure; the latest wins. Times are compared in UTC using each airport's time zone.
  - The weather at the inbound flight's origin is added for the hour it was due to leave.
  - Checks before publishing: every departure once, every inbound flight used at most once, every inbound airport in the reference table.
- **Verification and quality reporting:**
  - Complete accounting: raw rows = unique rows + duplicate copies, and unique rows = retained + quarantined + out of scope. A run publishes nothing if these do not hold.
  - Overlapping rule flags are tracked per rule without inflating the row counts.
  - Small hand-made test fixtures cover `2400`, early departures, multi-day delays, cancellations, diversion nulls, key conflicts, weather tokens and the UTC conversion.

### Spark and Scaling Considerations

- Transform data using the DataFrame API and Spark SQL, preferring native built-in functions over Python UDFs for performance and catalyst optimization.
- Keep all original fields through exact deduplication, then select the fields needed downstream.
- The main Silver tables (`flights`, `weather_observations`, `weather_hourly`) are partitioned by year and month. `departures_enriched` (about 20,000 rows per month), quarantine, audit and Gold tables are small and not partitioned.
- The classified rows are cached on disk (`DISK_ONLY`) because each run reuses them for the audit, the checks and several writes; the driver uses 3 GB of memory.
- Measured run times on the VM: about 55 seconds per national flight month, 70–80 seconds per weather year (32 stations), 25 seconds per departures month and 35 seconds for Gold over 20 months. Shuffle partitions are set to 8 from a measured benchmark (D9).

### Gold: Daily, Hourly and Chain Tables

Code: `project07/gold/daily.py`, `hourly.py`, `chain.py` (Spark SQL) and `project07/gold/run.py`.

Gold reads `departures_enriched` and publishes three tables for JFK and LGA, each with its own pointer and read-back check:

- **`airport_hour`:** one row per departure airport and UTC hour (with the New York date and hour): flights, cancellations, delays and that hour's weather, plus counts and sums so any period can be added up.
- **`departure_chain`:** linked departures per day, airport, inbound delay band (under 15, 15–44, 45–89, 90+ minutes) and good/bad/unknown weather at JFK/LGA and at the inbound origin. "Bad" means a thunderstorm, visibility under 5 km or measurable rain (D14). Counts and sums only, including BTS late-aircraft and weather delay minutes.
- **`daily_airport`**, as follows.

- **Grain:** One row per scheduled flight date and origin airport.
- **Flight metrics:** `flights`, `cancelled_flights`, `cancellation_rate`, `avg_dep_delay_min` and `delayed_15_rate` (share of flights 15 minutes or more late). Both delay metrics use flights that were not cancelled and have a known delay; diverted flights are included.
- **Coverage:** `flights_with_weather` and `weather_coverage`, the share of flights matched to an hourly weather report.
- **Daily weather:** `weather_hours`, `avg_temperature_c`, `reported_precipitation_mm`, `precipitation_hours`, `trace_hours`, `max_wind_speed_mps`, `max_wind_gust_mps` and `min_visibility_km`, grouped by New York date.
- **Unbiased weather aggregation:** Weather is aggregated to daily values from `weather_hourly` before it is joined to the daily flight summary, so it is not weighted by the number of flights.
- **Precipitation limit:** `reported_precipitation_mm` is the sum of the reported hourly amounts, not a complete daily rainfall total. Missing hours stay missing; `precipitation_hours` shows how many hours the sum covers (D8).
- **Checks before publishing:** `departures_enriched` must equal the Silver JFK/LGA flights for each month; each table's flight total must equal the departures (and `departure_chain` the linked departures); every month must have at least 80% of flights matched to weather; and all months must come from one rule version. Otherwise nothing is published.
- **Period:** every published month by default, or `--start`/`--end`; every published weather year.
- **Delivery:** Parquet, with a `_READY.json` manifest that records the code version, the Silver runs it read, the counts and the checks.
- **Figures:** `scripts/gold_figures.py` reads only the three Gold tables and prints every number it draws.

---

## Execution and Deployment

The pipeline executes on the project's dedicated Spark VM environment:

- **Automation:**
  - `.github/workflows/bronze-to-gold.yml` runs the Bronze → Silver → Gold pipeline (`--env prod`) on the dedicated self-hosted VM runner monthly (the 5th at 06:17 UTC) and via manual `workflow_dispatch`. Bronze runs in `new` mode from 2025-01; `scripts/plan_pipeline_run.py` then picks only the months and years whose inputs changed, so a second run right after the first does nothing (D15). Run logs and Bronze/Silver/Gold manifests are archived on the VM (`/home/ubuntu/project-data/logs/pipeline/`) and as GitHub Actions artifacts.
  - `.github/workflows/bronze-ingestion.yml` remains available for manual Bronze-only runs; its own schedule stays paused (D2).
- **Auditability:** Bronze writes a run manifest and keeps the source catalog. Every Silver and Gold run writes to a new `runs/<run_id>/` folder and reads its output back. Published tables then get a `_READY.json` manifest, and their `current` pointer is moved last. The exceptions are the audit tables and `flights_quarantine`: they are written to the run folder but have no pointer. Reruns add new run folders; nothing is skipped or deleted automatically.
- **Continuous Integration:** `.github/workflows/ci.yml` runs all unit tests on GitHub-hosted runners for every pull request and every push to `main`. The self-hosted runner is not exposed to pull requests (D7).
- **Environments:** `--env dev --dev-user <name>` writes to `silver/dev/<name>/` and `gold/dev/<name>/`; `--env prod` writes to `silver/prod/` and `gold/prod/`. Pending: executing the first production run.

---

## Environment and VM Setup

### Directory Conventions

```text
<your checkout>/project_07/       Repository code, configuration, tests, and documentation
/home/ubuntu/project-data/        External data storage (excluded from Git)
├── bronze/                      Raw BTS archives and NOAA station downloads
├── silver/<env>/                Cleaned Parquet tables, quarantine, audit tables and manifests
└── gold/<env>/                  daily_airport, airport_hour, departure_chain Parquet files and manifests
```

### Setup Instructions

Clone the repository:

```bash
git clone https://github.com/dat535-group07-2026/project_07.git
cd project_07
```

The project uses Spark 3.5.0 on Java 8, matching `/opt/spark` on the VM. Use a virtual environment inside the checkout; do not install into the shared `/home/ubuntu/spark-env`, which contains an incompatible PySpark 4 (see [docs/decisions.md](docs/decisions.md), D1):

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

Before running Spark jobs, use Java 8 and clear the VM's Jupyter driver settings:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
unset PYSPARK_DRIVER_PYTHON PYSPARK_DRIVER_PYTHON_OPTS
```

Bronze ingestion and its tests need only Python 3.11 or later; see [docs/bronze-ingestion.md](docs/bronze-ingestion.md).

### Running the Pipeline

Make sure Java 8 is active and the VM's Jupyter driver variables are unset (from the setup step above), then run all tests:

```bash
.venv/bin/python -m unittest discover -s tests
```

Verify the Bronze catalog and raw files offline (see [docs/bronze-ingestion.md](docs/bronze-ingestion.md) for `manual` downloads and bounded `new` checks):

```bash
.venv/bin/python scripts/ingest_bronze.py manual --start 2025-01 --end 2026-08 --offline
```

Silver and Gold refuse to publish from a checkout with uncommitted changes, so the manifest always names the exact code. Run them in this order (`dev` example; replace `<name>`). Departures need the flights of the month and the month before, and every weather year:

```bash
for year in 2025 2026; do
  .venv/bin/python -m project07.silver.run weather --env dev --dev-user <name> --year "$year"
done
for month in 2025-01 2025-02 2025-03; do   # ... up to the last released month
  .venv/bin/python -m project07.silver.run flights --env dev --dev-user <name> --month "$month"
done
for month in 2025-01 2025-02 2025-03; do
  .venv/bin/python -m project07.silver.run departures --env dev --dev-user <name> --month "$month"
done
.venv/bin/python -m project07.gold.run --env dev --dev-user <name>
```

For production, replace `--env dev --dev-user <name>` with `--env prod`. Instead of listing months by hand, ask the planner what is out of date, layer by layer (it prints the months or years to run, or nothing):

```bash
.venv/bin/python -m scripts.plan_pipeline_run flights --env prod
.venv/bin/python -m scripts.plan_pipeline_run weather --env prod
.venv/bin/python -m scripts.plan_pipeline_run departures --env prod
.venv/bin/python -m scripts.plan_pipeline_run gold --env prod
```

In `dev`, Gold can read another developer's Silver tables with `--flights-user` and `--weather-user`, and `--start`/`--end` limit its months. `--data-root` defaults to `/home/ubuntu/project-data`.

Each run prints its manifest. The published tables are read through `current` pointers, for example `silver/prod/flights/current/2025-06.json`, `silver/prod/weather_hourly/current/2025.json` and `gold/prod/daily_airport/current.json` (or under `silver/dev/<name>/` and `gold/dev/<name>/` for `dev`).

Check the airport time zones in the reference table against published flights:

```bash
.venv/bin/python -m scripts.check_airport_time_zones --env prod --months 2025-01 2025-07 2026-01
```

Generate the report figures and monthly summary table from published `prod` Gold tables (or pass `--env dev --dev-user <name>`):

```bash
.venv/bin/python -m scripts.gold_figures --env prod --out /tmp/gold-figures
```

Reproduce the June 2025 flight-cleaning shuffle-partition benchmark (see [docs/decisions.md](docs/decisions.md), D9):

```bash
.venv/bin/python -m scripts.benchmark_flights --shuffle-partitions 8
```

---

## References and Licences

### Source Data References
1. **BTS Field Definitions:** [BTS TranStats On-Time Performance Fields](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ)
2. **BTS Time Reporting Directive:** [BTS Number 31 Technical Directive: Time Reporting Instructions](https://www.bts.gov/topics/airlines-and-airports/number-31-technical-directive-time-reporting-effective-jan-1-2019)
3. **NOAA Weather Documentation:** [NOAA Local Climatological Data (LCD) v2 Documentation](https://www.ncei.noaa.gov/oa/local-climatological-data/v2/doc/lcdv2_DOCUMENTATION.pdf)

### Project Licence
The project code is licensed under [GNU General Public License v3.0](LICENSE). Source datasets remain subject to their respective government open-data terms and conditions.
