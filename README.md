# BTS Flight Operations and Airport Weather

DAT535 course project (Group 07) by Alexander Kvalvaag and Christopher Liebich Kolle.

We are building a PySpark batch pipeline that combines Bureau of Transportation Statistics (BTS) flight operations with National Oceanic and Atmospheric Administration (NOAA) airport weather data. The pipeline outputs daily departure-delay and cancellation summaries for John F. Kennedy International Airport (JFK) and LaGuardia Airport (LGA), accompanied by data-quality and weather-coverage metrics.

The primary focus is on data engineering within a Medallion architecture: robust ingestion, explicit domain modelling, anomaly handling, consistency verification, scalable Spark transformations, and reproducible automated runs.

---

## Current Status

Status as of 10 November 2026 (deadline, including the report: 20 November 2026):

- **Bronze: implemented and tested.** National BTS months June–October 2025 and the 2025 LCDv2 files for JFK and LGA are ingested, checksummed, and catalogued (3,013,664 flight rows; 1.38 GB raw CSV). See [docs/bronze-ingestion.md](docs/bronze-ingestion.md).
- **Silver: implemented and tested.** Flights (all national rows) and weather (JFK and LGA) are cleaned, validated, quarantined where needed and published with row accounting. Flights for JFK and LGA are joined to hourly weather.
- **Gold: implemented and tested.** One daily table per date and airport, `daily_airport`, with flight, delay, cancellation, weather and coverage metrics.
- **Runs so far:** development runs only. The first Gold dev run covered 102,640 JFK/LGA departures, 99,591 of them matched to hourly weather.
- **Pending:** a production run, the scheduled Bronze-to-Gold workflow, an analysis of the Gold table (the Gold consumer) and performance measurements.
- **Evaluation window:** June–October 2025.
- **Decisions and evidence:** Runtime, evaluation window, time conventions, and measured data quality are recorded in [docs/decisions.md](docs/decisions.md).
- **Historical inspection:** An earlier inspection of June 2024 inputs (611,132 flights; 5,465 next-day departures, 84 `2400` departures, 12 elapsed-time discrepancies) guided the design. These figures do not describe the 2025 inputs.

---

## Scope and Boundaries

### In Scope
- **Flight cleaning & validation:** Full national monthly BTS Reporting Carrier On-Time Performance files.
- **Weather enrichment & analysis:** Station-level observations for JFK and LGA.
- **Temporal coverage:** June–October 2025 for development and evaluation.
- **Outputs:** Daily flight operations, departure delay summaries, cancellation statistics, and data quality/coverage reports.

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
- **Airport-to-station mapping:** `project07/silver/weather.py` maps the JFK and LGA airport codes to their NOAA LCDv2 station identifiers (`USW00094789`, `USW00014732`). BTS clock times are local civil time (Eastern Time with daylight saving). LCDv2 `DATE` values are Local Standard Time, a fixed UTC−5 offset without daylight saving (see [docs/decisions.md](docs/decisions.md), D5).

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
│  - UTC timestamp conversion                      │
│  - Elapsed time consistency checks               │
│  - Hourly routine weather parsing (trace/tokens) │
│  - Scheduled departure hour left join            │
│  - Quarantined records & audit tables            │
└─────────────────────────┬────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│             Gold (Daily Summaries)               │
│  - Daily aggregations for JFK & LGA              │
│  - Decoupled daily weather summary join          │
│  - Delay metrics and cancellation counts         │
│  - Reconciled counts, Parquet & manifest         │
└──────────────────────────────────────────────────┘
```

### Bronze: Ingestion and Preservation

- Raw BTS `.zip` archives and NOAA LCDv2 CSV files are stored unmodified in the Bronze storage layer.
- A **catalog** records every source version with its URL, retrieval timestamp, file size, and SHA-256 checksum. A **run manifest** records each run's mode, requested months, selected inputs, and outcome.
- Archive members are inventoried during extraction, keeping unpacked working copies separated.
- **Failure policy:** Missing source files, corrupt downloads, or unexpected schemas trigger immediate job failures rather than silently dropping records or degrading coverage. In new-month mode, an availability-check 404 records an unreleased month as pending.
- **Idempotence & Updates:** A monthly GitHub Actions check for new data months exists but is paused (D2). Cached files are verified and reused unless a refresh is requested. Refreshes download the source before comparing hashes. Backfills and manual runs process historical periods on demand.

### Silver: Cleaning, Model, and Verification

Code: `project07/silver/flights.py`, `weather.py`, `enrichment.py`, with shared reading, accounting and publishing in `common.py` and the command-line runner in `run.py`.

Silver models the domain, cleans raw fields, isolates invalid data, and joins flight operations with weather observations:

- **Reading:** Every column is read as text with an explicit schema. Before parsing, the reader checks the file's checksum, the exact header and the number of fields on every line.
- **Data model and keys:**
  - BTS does not provide a single universal flight ID. We use a composite candidate key: `FlightDate`, `Reporting_Airline`, `Flight_Number_Reporting_Airline`, `Origin`, `Dest`, and `CRSDepTime`.
  - Exactly identical rows (every original field equal) are collapsed into one, and the number of copies is kept.
  - Different records that share a valid candidate key are quarantined rather than resolved arbitrarily.
  - Clean tables, quarantined records (with the original payload and reasons) and an audit table are stored as Parquet with source provenance.
- **Flight rules:** 13 rules. Six are fatal and send the row to quarantine: a malformed record, a missing or invalid key, an invalid cancelled/diverted flag, a flight that is both cancelled and diverted, and a key conflict. Seven are warnings that keep the row with a quality flag, for example a completed flight that lacks an actual-time field, an unparseable optional number, or a departure clock that disagrees with the rebuilt departure.
- **Operational states and typing:**
  - Explicit parsing of dates, whole minutes, clocks and flags. Unparseable values become null and are flagged, never zero-filled.
  - Cancelled flights may legitimately report departure times, and diverted flights legitimately lack arrival and elapsed-time values.
- **Departure time reconstruction:**
  - BTS writes clock times as `HHMM`, where `2400` means midnight at the end of the day. The parser accepts `0000`–`2359` and `2400`.
  - The actual departure is the scheduled local time plus the signed departure delay, checked against the reported actual clock. This handles midnight rollovers, multi-day delays and early departures.
  - For JFK and LGA, the scheduled departure is converted to UTC with New York's daylight-saving rules during enrichment.
- **Consistency checks:**
  - For completed flights, `ActualElapsedTime == AirTime + TaxiIn + TaxiOut` is checked. A full scan of June–October 2025 found no discrepancies (the June 2024 inspection found 12).
- **Weather preprocessing:**
  - LCDv2 rows mix routine hourly (`FM-15`), special (`FM-16`), synoptic (`FM-12`) and daily/monthly summary (`SOD`, `SOM`) reports. Summary rows are counted as out of scope.
  - `weather_hourly` keeps one `FM-15` report per station and UTC hour: the one closest to minute 51.
  - Token handling: trace precipitation (`T`) is kept as a flag with a null amount, suspect (`s`) values are flagged, and variable wind directions and visibilities are marked.
  - Timestamps: `DATE` is converted to UTC with a fixed +5 hour offset (Local Standard Time), not with daylight-saving rules. Rows that share the same station, `DATE` and report type, such as duplicated labels at the daylight-saving changes, are quarantined. Shifted or missing labels are not detected.
  - Measured gaps (D8): no hourly reports on 30–31 August and 1 September 2025 at either station, and hourly precipitation is mostly blank from 26 August 2025.
- **Enrichment:**
  - JFK and LGA departures are left-joined to `weather_hourly` on airport and the UTC hour of the scheduled departure, so a flight scheduled for 14:10 UTC can match a 14:51 observation from the same hour.
  - Joining on the scheduled time allows cancelled flights to receive weather when a match exists.
  - Flights without a weather match are kept with empty weather columns. The join fails if `weather_hourly` has two rows for the same airport and hour, so flights cannot be multiplied.
- **Verification and quality reporting:**
  - Complete accounting: raw rows = unique rows + duplicate copies, and unique rows = retained + quarantined + out of scope. A run publishes nothing if these do not hold.
  - Overlapping rule flags are tracked per rule without inflating the row counts.
  - Small hand-made test fixtures cover `2400`, early departures, multi-day delays, cancellations, diversion nulls, key conflicts, weather tokens and the UTC conversion.

### Spark and Scaling Considerations

- Transform data using the DataFrame API and Spark SQL, preferring native built-in functions over Python UDFs for performance and catalyst optimization.
- Keep all original fields through exact deduplication, then select the fields needed downstream.
- The main Silver tables (`flights`, `weather_observations`, `weather_hourly`) are partitioned by year and month. Quarantine, audit and Gold tables are small and not partitioned.
- The classified rows are cached on disk (`DISK_ONLY`) because each run reuses them for the audit, the checks and several writes; the driver uses 3 GB of memory.
- Pending: measure execution plans and stage times, and test one performance change.

### Gold: Daily Airport Summaries

Code: `project07/gold/daily.py` (Spark SQL) and `project07/gold/run.py`.

Gold produces one table, `daily_airport`, for JFK and LGA:

- **Grain:** One row per scheduled flight date and origin airport.
- **Flight metrics:** `flights`, `cancelled_flights`, `cancellation_rate`, `avg_dep_delay_min` and `delayed_15_rate` (share of flights 15 minutes or more late). Both delay metrics use flights that were not cancelled and have a known delay; diverted flights are included.
- **Coverage:** `flights_with_weather` and `weather_coverage`, the share of flights matched to an hourly weather report.
- **Daily weather:** `weather_hours`, `avg_temperature_c`, `reported_precipitation_mm`, `precipitation_hours`, `trace_hours`, `max_wind_speed_mps`, `max_wind_gust_mps` and `min_visibility_km`, grouped by New York date.
- **Unbiased weather aggregation:** Weather is aggregated to daily values from `weather_hourly` before it is joined to the daily flight summary, so it is not weighted by the number of flights.
- **Precipitation limit:** `reported_precipitation_mm` is the sum of the reported hourly amounts, not a complete daily rainfall total. Missing hours stay missing; `precipitation_hours` shows how many hours the sum covers (D8).
- **Checks before publishing:** the joined rows must equal the Silver JFK/LGA flights for each month, and Gold's total flights must equal the joined rows. Otherwise nothing is published.
- **Delivery:** Parquet, with a `_READY.json` manifest that records the code version, the Silver runs it read, the counts and the checks.

---

## Execution and Deployment

The pipeline executes on the project's dedicated Spark VM environment:

- **Automation:** Bronze ingestion runs via manually dispatched GitHub Actions on a dedicated self-hosted runner; its monthly schedule is paused (D2). Pending: a scheduled run of the full Bronze-to-Gold pipeline.
- **Auditability:** Bronze writes a run manifest and keeps the source catalog. Every Silver and Gold run writes to a new `runs/<run_id>/` folder, reads its output back, writes a `_READY.json` manifest and moves the table's `current` pointer last. Silver also writes an audit table. Reruns add new run folders; nothing is skipped or deleted automatically.
- **Continuous Integration:** `.github/workflows/ci.yml` runs all unit tests on GitHub-hosted runners for every pull request and every push to `main`. The self-hosted runner is not exposed to pull requests (D7).
- **Environments:** `--env dev --dev-user <name>` writes to `silver/dev/<name>/` and `gold/dev/<name>/`; `--env prod` writes to `silver/prod/` and `gold/prod/`. Pending: the first production run.

---

## Environment and VM Setup

### Directory Conventions

```text
<your checkout>/project_07/       Repository code, configuration, tests, and documentation
/home/ubuntu/project-data/        External data storage (excluded from Git)
├── bronze/                      Raw BTS archives and NOAA station downloads
├── silver/<env>/                Cleaned Parquet tables, quarantine, audit tables and manifests
└── gold/<env>/                  daily_airport Parquet files and manifests
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

Run all tests:

```bash
.venv/bin/python -m unittest discover -s tests
```

Silver and Gold refuse to publish from a checkout with uncommitted changes, so the manifest always names the exact code. Run them in this order (dev example; replace `<name>`):

```bash
.venv/bin/python -m project07.silver.run weather --env dev --dev-user <name> --year 2025
for month in 2025-06 2025-07 2025-08 2025-09 2025-10; do
  .venv/bin/python -m project07.silver.run flights --env dev --dev-user <name> --month "$month"
done
.venv/bin/python -m project07.gold.run --env dev --dev-user <name>
```

For production, replace `--env dev --dev-user <name>` with `--env prod`. In dev, Gold can read another developer's Silver tables with `--flights-user` and `--weather-user`. `--data-root` defaults to `/home/ubuntu/project-data`.

Each run prints its manifest. The published tables are read through `current` pointers, for example `silver/dev/<name>/flights/current/2025-06.json` and `gold/dev/<name>/daily_airport/current.json`.

---

## References and Licences

### Source Data References
1. **BTS Field Definitions:** [BTS TranStats On-Time Performance Fields](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ)
2. **BTS Time Reporting Directive:** [BTS Number 31 Technical Directive: Time Reporting Instructions](https://www.bts.gov/topics/airlines-and-airports/number-31-technical-directive-time-reporting-effective-jan-1-2019)
3. **NOAA Weather Documentation:** [NOAA Local Climatological Data (LCD) v2 Documentation](https://www.ncei.noaa.gov/oa/local-climatological-data/v2/doc/lcdv2_DOCUMENTATION.pdf)

### Project Licence
The project code is licensed under [GNU General Public License v3.0](LICENSE). Source datasets remain subject to their respective government open-data terms and conditions.
