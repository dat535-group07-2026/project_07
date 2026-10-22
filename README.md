# BTS Flight Operations and Airport Weather

DAT535 course project (Group 07) by Alexander Kvalvaag and Christopher Liebich Kolle.

We are building a PySpark batch pipeline that combines Bureau of Transportation Statistics (BTS) flight operations with National Oceanic and Atmospheric Administration (NOAA) airport weather data. The pipeline outputs daily departure-delay and cancellation summaries for John F. Kennedy International Airport (JFK) and LaGuardia Airport (LGA), accompanied by data-quality and weather-coverage metrics.

The primary focus is on data engineering within a Medallion architecture: robust ingestion, explicit domain modelling, anomaly handling, consistency verification, scalable Spark transformations, and reproducible automated runs.

---

## Current Status

Status as of 22 October 2026 (deadline, including the report: 20 November 2026):

- **Bronze: implemented and tested.** National BTS months June–October 2025 and the 2025 LCDv2 files for JFK and LGA are ingested, checksummed, and catalogued (3,013,664 flight rows; 1.38 GB raw CSV). See [docs/bronze-ingestion.md](docs/bronze-ingestion.md).
- **Silver and Gold: designed, not yet implemented.** The Silver and Gold sections below describe the planned design.
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
- **Airport reference mapping (planned for Silver):** A static reference table maps JFK and LGA airport codes to their NOAA LCDv2 station identifiers (`USW00094789`, `USW00014732`) and time conventions. BTS clock times are local civil time (Eastern Time with daylight saving). LCDv2 `DATE` values are Local Standard Time, a fixed UTC−5 offset without daylight saving (see [docs/decisions.md](docs/decisions.md), D5).

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
│  - Candidate key deduplication & audit counts    │
│  - Operational states (cancellations/diversions) │
│  - Departure date & 2400/rollover reconstruction │
│  - UTC timestamp conversion                      │
│  - Elapsed time consistency checks               │
│  - Hourly routine weather parsing (trace/tokens) │
│  - Scheduled departure hour left join            │
│  - Quarantined records & quality reports         │
└─────────────────────────┬────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│             Gold (Daily Summaries)               │
│  - Daily aggregations for JFK & LGA              │
│  - Decoupled daily weather summary join          │
│  - Delay metrics, eligible counts, cancellations │
│  - Parquet, CSV, & analysis notebook             │
└──────────────────────────────────────────────────┘
```

### Bronze: Ingestion and Preservation

- Raw BTS `.zip` archives and NOAA LCDv2 CSV files are stored unmodified in the Bronze storage layer.
- A **catalog** records every source version with its URL, retrieval timestamp, file size, and SHA-256 checksum. A **run manifest** records each run's mode, requested months, selected inputs, and outcome.
- Archive members are inventoried during extraction, keeping unpacked working copies separated.
- **Failure policy:** Missing source files, corrupt downloads, or unexpected schemas trigger immediate job failures rather than silently dropping records or degrading coverage.
- **Idempotence & Updates:** A monthly GitHub Actions check for new data months exists but is paused during Silver development. Runs without new or modified source data exit without rebuilding existing outputs. Backfills and manual runs process historical periods on demand.

### Silver: Cleaning, Model, and Verification

*Planned design; not yet implemented.*

Silver models the domain, cleans raw fields, isolates invalid data, and joins flight operations with weather observations:

- **Data model and keys:**
  - BTS does not provide a single universal flight ID. We evaluate a composite candidate key: `FlightDate`, `Reporting_Airline`, `Flight_Number_Reporting_Airline`, `Origin`, `Dest`, and `CRSDepTime`.
  - Exact duplicate rows are deduplicated with an audit count.
  - Conflicting keys (different records sharing the same candidate key) are quarantined for review rather than resolved arbitrarily.
  - Validated clean tables, quarantined records, and quality metrics are stored as Parquet with source provenance retained.
- **Operational states and typing:**
  - Explicit parsing of dates, integers, floats, and categorical flags.
  - Cancelled and diverted flights are treated according to valid operational states:
    - Cancelled flights may legitimately report departure times (e.g., gate departures prior to cancellation).
    - Diverted flights legitimately lack standard arrival or elapsed-time values.
  - Blanket null filtering and arbitrary zero-filling are strictly avoided. Unparseable required values are routed to quarantine with the original payload and error reason. Suspicious but interpretable values receive quality flags.
- **Departure time reconstruction:**
  - BTS formats clock times as integer `HHMM` (from `0001` to `2400`, where `2400` denotes midnight).
  - Actual departure dates and timestamps are reconstructed using scheduled date/time plus signed departure delay, cross-checked against the reported actual clock.
  - This accurately resolves midnight rollovers, departures delayed into subsequent days, and negative delays (early departures).
  - Departure timestamps for JFK and LGA are converted to UTC using airport local time and daylight-saving rules.
- **Consistency checks:**
  - For completed flights, verify that `ActualElapsedTime == AirTime + TaxiIn + TaxiOut`. A full scan of June–October 2025 found no discrepancies (the June 2024 inspection found 12); any future discrepancies are flagged for review.
- **Weather preprocessing:**
  - LCDv2 records mix routine hourly (`FM-15`), special, daily, and monthly summaries.
  - Routine hourly reports are extracted and filtered to unique station-hour keys.
  - Token handling: parses non-numeric tokens, distinguishes trace precipitation (retaining a trace flag) from zero precipitation, and handles variable wind directions.
  - Timestamps: `DATE` is converted to UTC with a fixed +5 hour offset (Local Standard Time), not with daylight-saving rules. Duplicated or missing labels around daylight-saving changes are flagged.
- **Enrichment:**
  - Flight records for JFK and LGA are left-joined to hourly weather by airport station mapping and scheduled departure hour in UTC.
  - Joining on scheduled departure hour provides cancelled flights with an objective planned-time weather reference.
  - Unmatched weather observations remain visible in coverage reports; the join cardinality preserves total flight records without multiplication.
- **Verification and quality reporting:**
  - Complete accounting: every input row is accounted for as retained, quarantined, or deduplicated.
  - Overlapping validation flags are tracked without inflating error counts.
  - Synthetic test fixtures cover edge cases: midnight (`2400`), early departures, multi-day delays, cancellations, diversion nulls, conflicting keys, and weather token parsing.

### Spark and Scaling Considerations

- Transform data using the DataFrame API and Spark SQL, preferring native built-in functions over Python UDFs for performance and catalyst optimization.
- Prune unnecessary columns early to reduce memory pressure and I/O.
- Partition Silver tables by year and month to facilitate incremental processing and historical backfills.
- Benchmark and document partition counts, join strategies (e.g., broadcast vs. sort-merge), caching decisions, execution plans, and shuffle metrics.

### Gold: Daily Airport Summaries

*Planned design; not yet implemented.*

Gold produces business-ready daily aggregations for JFK and LGA:

- **Grain:** One row per scheduled flight date and origin airport.
- **Metrics:**
  - Total scheduled operations.
  - Cancellation count and cancellation rate (with explicit, documented denominators).
  - Diversion count.
  - Departure-delay metrics (mean, median, 90th percentile) calculated over explicitly reported eligible flights.
  - Weather summary metrics and coverage indicators.
- **Unbiased weather aggregation:** Weather metrics are aggregated to daily values prior to joining with daily flight summaries, preventing daily weather statistics from being weighted by flight volume.
- **Delivery:** Stored as Parquet and CSV files. A lightweight exploratory notebook evaluates empirical associations between weather conditions and delay patterns.
- **Readiness manifest:** Gold updates produce a readiness manifest verifying input checksums and data quality checks before publishing.

---

## Execution and Deployment

The pipeline executes on the project's dedicated Spark VM environment:

- **Automation (implemented):** Bronze ingestion runs via manually dispatched GitHub Actions on a dedicated self-hosted runner; the monthly schedule is paused during Silver development. Manual dispatches also support backfilling historical ranges.
- **Auditability (implemented for Bronze):** Bronze run manifests and the source catalog are persisted with every run. Silver and Gold quality audit tables are planned.
- **Continuous Integration (planned):** Pull-request unit tests on GitHub-hosted runners with synthetic fixtures. The self-hosted runner is deliberately not exposed to pull requests.
- **Environments (planned):** Development (`dev`) and production (`prod`) outputs are written to separate directory structures.

---

## Environment and VM Setup

### Directory Conventions

```text
/home/ubuntu/project_07/          Repository code, configuration, tests, and documentation
/home/ubuntu/project-data/        External data storage (excluded from Git)
├── bronze/                      Raw BTS archives and NOAA station downloads
├── silver/                      Cleaned Parquet tables, quarantine, and audit metrics
└── gold/                        Daily summary Parquet/CSV files and readiness manifests
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

---

## References and Licences

### Source Data References
1. **BTS Field Definitions:** [BTS TranStats On-Time Performance Fields](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ)
2. **BTS Time Reporting Directive:** [BTS Number 31 Technical Directive: Time Reporting Instructions](https://www.bts.gov/topics/airlines-and-airports/number-31-technical-directive-time-reporting-effective-jan-1-2019)
3. **NOAA Weather Documentation:** [NOAA Local Climatological Data (LCD) v2 Documentation](https://www.ncei.noaa.gov/oa/local-climatological-data/v2/doc/lcdv2_DOCUMENTATION.pdf)

### Project Licence
The project code is licensed under [GNU General Public License v3.0](LICENSE). Source datasets remain subject to their respective government open-data terms and conditions.
