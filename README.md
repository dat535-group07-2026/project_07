# BTS Flight Operations and Airport Weather

DAT535 course project (Group 07) by Alexander Kvalvaag and Christopher Liebich Kolle.

We are building a PySpark batch pipeline that combines Bureau of Transportation Statistics (BTS) flight operations with National Oceanic and Atmospheric Administration (NOAA) airport weather data. The pipeline outputs daily departure-delay and cancellation summaries for John F. Kennedy International Airport (JFK) and LaGuardia Airport (LGA), accompanied by data-quality and weather-coverage metrics.

The primary focus is on data engineering within a Medallion architecture: robust ingestion, explicit domain modelling, anomaly handling, consistency verification, scalable Spark transformations, and reproducible automated runs.

---

## Current Status

The project is currently in the definition and specification stage (as of October 2026):

- **Project definition:** Scope, datasets, schema modelling, and Medallion pipeline architecture defined.
- **Initial data inspection:** Completed on historical reference inputs:
  - Full June 2024 BTS file: 611,132 rows and 109 named columns (~276.8 MB uncompressed).
  - Two full-year 2024 LCDv2 weather station files for JFK and LGA (~18.6 MB uncompressed).
  - Total inspected input: ~295.4 MB.
  - Inspected records identified 5,465 next-day departures, 84 midnight departures reported as `2400`, and 12 elapsed-time arithmetic discrepancies.
- **Target evaluation window:** June–August 2025 selected for pipeline development and evaluation (subject to official source availability and coverage).
- **Next steps:** Ingest 2025 raw data into Bronze, generate baseline manifests, and implement Silver parsing and validation routines.

---

## Scope and Boundaries

### In Scope
- **Flight cleaning & validation:** Full national monthly BTS Reporting Carrier On-Time Performance files.
- **Weather enrichment & analysis:** Station-level observations for JFK and LGA.
- **Temporal coverage:** June–August 2025 for development and evaluation.
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
- **Airport reference mapping:** A static reference table maps JFK and LGA airport codes to their corresponding NOAA LCDv2 station identifiers and local time conventions (Eastern Time / UTC offsets).

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
- A **manifest** is recorded for every retrieval containing source URLs, retrieval timestamps, file sizes, SHA-256 checksums, dataset versions, and run configurations.
- Archive members are inventoried during extraction, keeping unpacked working copies separated.
- **Failure policy:** Missing source files, corrupt downloads, or unexpected schemas trigger immediate job failures rather than silently dropping records or degrading coverage.
- **Idempotence & Updates:** Monthly GitHub Actions check for newly published data months. Runs without new or modified source data exit without rebuilding existing outputs. Backfills and manual runs process historical periods on demand.

### Silver: Cleaning, Model, and Verification

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
  - For completed flights, verify that `ActualElapsedTime == AirTime + TaxiIn + TaxiOut`. (In the June 2024 inspection, 12 records had discrepancies; these are flagged for review).
- **Weather preprocessing:**
  - LCDv2 records mix routine hourly (`FM-15`), special, daily, and monthly summaries.
  - Routine hourly reports are extracted and filtered to unique station-hour keys.
  - Token handling: parses non-numeric tokens, distinguishes trace precipitation (retaining a trace flag) from zero precipitation, and handles variable wind directions.
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

- **Continuous Integration (CI):** Pull requests run unit tests, schema assertions, and synthetic fixture validations.
- **Environments:** Development (`dev`) and production (`prod`) outputs are written to separate directory structures.
- **Automation:** Monthly automated runs ingest newly released data via GitHub Actions; manual workflow dispatches support backfilling historical ranges.
- **Auditability:** Execution logs, manifests, and quality audit tables are persisted with every pipeline run.

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

Activate the Spark environment on the VM:

```bash
source /home/ubuntu/spark-env/bin/activate
python -m pip install -r requirements.txt
```

---

## References and Licences

### Source Data References
1. **BTS Field Definitions:** [BTS TranStats On-Time Performance Fields](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ)
2. **BTS Time Reporting Directive:** [BTS Number 31 Technical Directive: Time Reporting Instructions](https://www.bts.gov/topics/airlines-and-airports/number-31-technical-directive-time-reporting-effective-jan-1-2019)
3. **NOAA Weather Documentation:** [NOAA Local Climatological Data (LCD) v2 Documentation](https://www.ncei.noaa.gov/oa/local-climatological-data/v2/doc/lcdv2_DOCUMENTATION.pdf)

### Project Licence
The project code is licensed under [GNU General Public License v3.0](LICENSE). Source datasets remain subject to their respective government open-data terms and conditions.
