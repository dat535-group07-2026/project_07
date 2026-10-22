# Silver contract: proposal

Status: proposal for Alexander and Christopher to agree on. Nothing here is
implemented yet.

This document fixes the interfaces that both Silver cleaners depend on, so
that the flight and weather work can proceed in parallel. Each item has a
recommended default. Accept it, change it, or mark it as deferred. Record the
outcome in the item's **Agreed** line. Once both authors have signed off, move
the decisions into `docs/decisions.md`.

Background: [decisions.md](decisions.md) (runtime, evaluation window, roles,
time conventions), [bronze-ingestion.md](bronze-ingestion.md) (catalog), and the
official project guide (Silver is the primary focus; dev/prod environments, PR
checks and a scheduled Bronze-to-Gold run are required deliverables).

## 1. Package layout and test command

**Default.** Use one plain package at the repository root, with no install step:

```text
project07/
    __init__.py
    config.py          # environment and path resolution        (Christopher)
    spark.py           # SparkSession factory                    (Christopher)
    silver/
        __init__.py
        common.py      # catalog reader, provenance, writer, audit (Christopher)
        flights.py     # flight cleaning                         (Alexander)
        weather.py     # weather cleaning                        (Christopher)
        enrichment.py  # later, joint
        run.py         # command-line entry point                (Christopher)
tests/
    spark_case.py      # shared SparkSession for tests
    test_silver_common.py, test_silver_flights.py, test_silver_weather.py
```

- Keep `unittest`, which the Bronze tests already use, and run everything with
  `python -m unittest discover -s tests`.
- Cleaning functions take DataFrames and return DataFrames. Reading and writing
  live only in `common.py` and `run.py`, so tests never touch real files.

**Agreed:**

## 2. Environments and configuration

**Default.**

- Every command takes `--env dev|prod`. Paths come from `project07/config.py`,
  never from hard-coded strings.
- The data root defaults to `/home/ubuntu/project-data` and can be overridden
  with `--data-root`.

| Environment | Silver root | Gold root | Who writes |
| --- | --- | --- | --- |
| `dev` | `silver/dev/<user>/` | `gold/dev/<user>/` | Developers, from their own clones |
| `prod` | `silver/prod/` | `gold/prod/` | Only the GitHub Actions production workflow, from `main` |

- `<user>` defaults to `$USER` and can be set with `--dev-user`, so that two
  people on the VM never collide.
- Both environments read the same Bronze catalog. Bronze is never written by
  Silver.

**Agreed:**

## 3. Catalog reader

**Default.** `common.py` provides:

```python
def select_inputs(bronze_root: Path, asset_ids: list[str]) -> list[SelectedAsset]:
    """Snapshot the selected (last) version of each asset at run start."""

@dataclass(frozen=True)
class SelectedAsset:
    asset_id: str            # "bts:2025-06", "noaa:USW00094789:2025"
    kind: str                # "bts" or "noaa"
    version_sha256: str      # archive hash for BTS, CSV hash for NOAA
    csvs: list[SelectedCsv]  # BTS: version["csvs"]; NOAA: the version itself

@dataclass(frozen=True)
class SelectedCsv:
    path: Path               # absolute, resolved under bronze_root
    sha256: str
    size_bytes: int
```

- Fail if `schema_version != 1`, if an asset is missing, or if a file's size
  or SHA-256 does not match the catalog. Hashing about 1.4 GB takes seconds,
  which is small next to the Spark job.
- Never glob Bronze. Never write the catalog.
- The selection, including hashes, is written into the run manifest (item 8).

**Agreed:**

## 4. Reading raw CSVs

**Default.**

- Read each selected CSV separately, then union the results.
  - Use an explicit all-string schema, `header=true`, `mode=PERMISSIVE` and a
    `_corrupt_record` column.
  - For BTS, use the 109 named columns plus `_trailing_empty`. This was tested
    on October 2025: 605,844 rows, 0 corrupt.
- Add the provenance columns as literals per file (item 5). This is simpler and
  more deterministic than mapping `input_file_name()` back to hashes.
- Cache, or select other columns as well, before querying `_corrupt_record`
  on its own. Spark rejects queries that reference only that column.

**Agreed:**

## 5. Common columns

**Default.** Every Silver table carries:

| Column | Type | Meaning |
| --- | --- | --- |
| `source_asset_id` | string | Catalog asset |
| `source_file` | string | Path relative to `bronze/` |
| `source_sha256` | string | Hash of that CSV |
| `silver_run_id` | string | `YYYYMMDDTHHMMSSZ-<8 hex>`, as in Bronze |
| `rule_version` | string | Version of the rule set, increased by hand when rules change |
| `record_hash` | string | `sha2` over all original fields, with nulls encoded as a fixed marker |
| `quality_flags` | array<string> | Warning codes. Empty when none apply |

Quarantine tables also carry `rejection_reasons` (array<string>, never empty)
and `original` (a struct of the original string fields).

- Column names are `snake_case`. Units go in the name, for example
  `temperature_c` and `dep_delay_min`.
- Keep identifiers as strings.

**Agreed:**

## 6. Rule codes and severities

**Default.**

- Codes look like `<DOMAIN>_<CATEGORY>_<DETAIL>`. `F_` is for flights and `W_`
  for weather, for example `F_KEY_MISSING`, `F_CLOCK_DELAY_MISMATCH`,
  `W_TOKEN_UNPARSEABLE` and `W_DST_LABEL_CONFLICT`.
- Each module keeps a registry, `RULES = {code: (severity, description)}`, and
  `docs/` lists the codes in one table.
- There are three severities:
  - `fatal`: the row is quarantined.
  - `warning`: the row is retained and the code goes into `quality_flags`.
  - `scope`: the row is valid but outside the model. It is counted, not
    quarantined.

**Agreed:**

## 7. Row accounting

**Default.** Use the same order for both datasets, matching the report:

```text
raw = duplicate_extras + unique
unique = scope_excluded + retained + quarantined
```

1. Count every raw row, including malformed ones.
2. Remove exact duplicates, defined over all original fields including
   `_corrupt_record`. One row is kept per group; `duplicate_extras` counts the
   rest. This happens before any columns are pruned.
3. Classify each unique row into exactly one outcome. Malformed rows are
   quarantined with `*_MALFORMED_RECORD`.

- The run fails, and is not marked ready, if either equation does not hold.
- Per-rule counts are reported separately and may overlap.
- Reducing weather observations to hours is a separate step with its own
  counts. It does not count as record loss.

**Agreed:**

## 8. Outputs, reruns and readiness

**Default.** Each run writes immutable output:

```text
<silver_root>/<table>/runs/<run_id>/year=YYYY/month=MM/part-*.parquet
<silver_root>/<table>/runs/<run_id>/_READY.json   # written last
<silver_root>/<table>/current.json                # pointer to the latest ready run
```

- Tables: `flights`, `flights_quarantine`, `weather_observations`,
  `weather_quarantine`, `weather_hourly`, `audit`.
- `_READY.json` records:
  - the run ID and git commit
  - the rule version and environment
  - the selected inputs and their hashes
  - the outcome counts
  - the check results
- `current.json` is replaced atomically (write a temporary file, then rename)
  only after all checks pass.
- Downstream steps (enrichment and Gold) read only through `current.json`.
- A rerun never appends, and a failed run never moves the pointer. Old runs are
  deleted by hand when space is needed.

**Agreed:**

## 9. Partitioning

**Default.**

- Partition flights by the `year` and `month` of the scheduled `FlightDate`,
  even when the actual departure falls in the next month.
- Partition weather observations by the year and month of the source `DATE`.
- Partition hourly weather by the year and month of its UTC hour.
- Do not add repartitioning, caching or broadcasting until a measurement
  justifies it. Check the output file counts after the first full run.

**Agreed:**

## 10. Spark session

**Default.** `project07/spark.py` provides `get_spark(app_name, env)`:

- `spark.sql.session.timeZone=UTC`
- `spark.sql.ansi.enabled=false`. Parsers check each token with a regex before
  casting, so a blank value and an invalid value stay distinct.
- `local[*]` on the VM. In tests, `local[2]` with the UI disabled.
- Runs need Java 8 (`JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64`), and
  `PYSPARK_DRIVER_PYTHON` must be unset. `run.py` fails early with a clear
  message if either is wrong.

**Agreed:**

## 11. Time conventions

**Default.** As in decision D5:

- Flight clocks stay local. Only JFK and LGA scheduled departures are converted
  to UTC, using `America/New_York`.
- Weather `DATE` is converted as `DATE + INTERVAL 5 HOURS`. Avoid
  `Etc/GMT+5`: it means UTC-5, which is easy to misread.
- The raw `DATE` string is kept alongside the converted value.

**Agreed:**

## 12. Tests and CI

**Default.**

- Every rule has small inline fixtures with known answers, covering at least a
  valid case, an invalid case and a boundary case. There is also one
  reconciliation test per dataset.
- A new workflow, `ci.yml`, runs on `pull_request` and `push` to `main`.
  - It uses a GitHub-hosted `ubuntu-latest` runner with Python 3.11 and Java 8
    (Temurin), matching the VM.
  - It installs `requirements.txt` and runs the full unittest suite.
  - It never runs on the self-hosted runner.
- Once CI is green, require a passing CI check and one approving review before
  merging to `main`.

**Agreed:**

## 13. Collaboration

**Default.**

- Branches: `feature/silver-common`, `feature/silver-flights`,
  `feature/silver-weather`.
- Small PRs, each reviewed by the other owner.
- Never commit data, Parquet output or credentials.
- Each person keeps a separate clone and dev output root (item 2). Never switch
  branches in the shared `/home/ubuntu/project_07` checkout.
- Record significant AI-generated artifacts as you go, for the report's AI
  disclosure.

**Agreed:**

## First pull requests after agreement

1. `feature/silver-common` (Christopher): `config.py`, `spark.py`,
   `select_inputs`, the raw reader, the provenance columns, a writer with
   `_READY.json`, tests, and `ci.yml`.
2. `feature/silver-flights` (Alexander): a thin slice on one month. Typed key
   fields, operational-state flags, exact duplicates, the accounting
   equations, fixtures. It codes against the signatures above, using a stub
   until item 1 merges.
3. `feature/silver-weather` (Christopher): report-type and token profile for
   both stations, typed observations, and timestamp conversion with the DST
   anomaly flags.

## Open questions

- Should the hourly weather table select one FM-15 report per hour, using an
  ordering still to be defined, or aggregate several?
- Should prod run on a schedule more often than monthly before the demo, to
  accumulate evidence? This is decided later, with the end-to-end workflow.
