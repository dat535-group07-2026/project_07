# Bronze ingestion

Run these commands from `/home/ubuntu/project_07` on the Spark VM.
The script uses Python 3.11 or later and the standard library. Spark does not
need to be running.

## Existing inputs

Verify and register the June to September 2025 downloads already on the VM:

```sh
python3 scripts/ingest_bronze.py manual --start 2025-06 --end 2025-09 --offline
```

The first run imports the original manifest from
`/home/ubuntu/project-data/bronze/bts-noaa-2026-10-19/manifest.json`.
It does not move or copy the source files. Later runs read `bronze/catalog.json`.
Each rerun checks file sizes, SHA-256 hashes, CSV headers and BTS ZIP integrity.

## Download a month range

```sh
python3 scripts/ingest_bronze.py manual --start 2025-06 --end 2025-09
```

Month ranges are inclusive. Each month downloads the national BTS reporting
carrier archive, not just flights from JFK and LGA. For every year in the range,
the script also retrieves the annual NOAA LCD v2 files for JFK (`USW00094789`)
and LGA (`USW00014732`). Existing verified inputs are reused.

Add `--refresh` to check the sources for revisions. Changed content is kept as
a new version, while previous versions remain available. Without `--refresh`,
an existing file is verified locally rather than downloaded again.

## Check for new flight months

Preview the months that would be checked without network requests or writes:

```sh
python3 scripts/ingest_bronze.py new --dry-run
```

Run the check:

```sh
python3 scripts/ingest_bronze.py new
```

By default this starts after the latest ingested BTS month and ends at the
previous calendar month. It also retries months previously reported as missing.
Use `--since YYYY-MM` and `--through YYYY-MM` to bound a check, especially for
the first run on a new data directory.

BTS HTTP 404 responses are recorded as unavailable releases, not successful
downloads. Network failures, permissions errors and invalid downloads fail the
run instead of being treated as missing months. When new flights are retrieved,
the matching annual weather files are refreshed too.

The script itself does not schedule runs. The GitHub Actions workflow described
below calls `new` monthly. The first check after a long gap can download several
months, so preview it before running manually.

## GitHub Actions

The `Bronze ingestion` workflow runs on the VM's `spark-vm-project07` runner,
labelled `project-07`. It runs on the first day of each month at 06:17 UTC and
checks for new BTS months. Missing releases are retried in later runs. This is
a batch schedule, not real-time ingestion, and it does not yet run Silver or Gold.

Under GitHub Actions, select **Bronze ingestion**, then **Run workflow**:

- `verify` checks the existing month range without network access (the default).
- `manual` downloads any missing inputs for the specified month range.
- `new` checks months after the latest ingested month, plus pending releases.

The start and end month inputs apply only to `verify` and `manual`. Each job
runs the tests before ingestion and uploads a small report artifact containing
that run's manifests and the current catalog. Raw datasets stay on the VM and
are not uploaded to GitHub. Reports are retained for 14 days.

The runner is installed separately in `/home/ubuntu/actions-runner-project07`
and runs as a service under `ubuntu`. It does not replace the lab runner.
The workflow checks code out into its own runner workspace and uses the
external `/home/ubuntu/project-data` directory. It does not modify the working
checkout in `/home/ubuntu/project_07`.

There is deliberately no pull-request trigger on this public repository.
Workflow code runs with the VM user's access, so only trusted code should be
merged or manually dispatched. Actions use fixed commit references and the
workflow token has read-only repository access. The runner still requires the
VM and its network connection to remain available.

## Storage and manifests

The default data directory is `/home/ubuntu/project-data`, beside the repository.
Override it with `--data-root /absolute/path` after the subcommand.

- `bronze/catalog.json`: known source versions, file paths, checksums and pending months.
- `bronze/runs/<run-id>.json`: inputs used, timestamps and outcome of each run.
- `bronze/sources/`: newly downloaded source files, stored by content hash.
- `bronze/extracted/bts/`: extracted CSVs, grouped by month and archive hash.
- `bronze/bts-noaa-2026-10-19/`: existing initial downloads, reused in place.

Downloads are staged before validation. The catalog is updated only after all
required inputs for the run pass their checks. Failed runs keep the previous
catalog and record the error in a run manifest. A retry can reuse verified files
left by a failed batch. A local lock prevents overlapping ingestion runs.

The script does not filter flights, remove rows, fix nulls or interpret weather
values. Those are Silver tasks. Header checks verify the input format, not the
correctness of every record. A changed header stops ingestion for review.

## Tests and troubleshooting

```sh
python3 -m unittest discover -s tests -v
```

The tests use small generated files and mocked HTTP responses, so they do not
download the full datasets. They cover reruns, changed inputs, failed batches,
corruption, schema changes, missing releases, retries and archive paths.

The VM previously had DNS failures for `transtats.bts.gov`. If that persists,
online BTS downloads and new-release checks will fail. Offline verification of
the existing downloads still works. Do not treat a DNS error as evidence that
a month has not been published.

Sources:

- [BTS reporting-carrier data](https://www.transtats.bts.gov/ONTIME/)
- [NOAA LCD v2 downloads](https://www.ncei.noaa.gov/oa/local-climatological-data/v2/access/)
