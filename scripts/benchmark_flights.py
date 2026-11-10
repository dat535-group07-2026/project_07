"""Read-only flight benchmark: three fresh Python/JVM runs per configuration.

python -m scripts.benchmark_flights [--month 2025-06] [--shuffle-partitions 8]
No published output, raw cache or cleaning-rule changes. Spark scratch files stay in /tmp.
"""

import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

from project07 import spark as spark_mod
from project07.silver import common, flights

STEPS = ('select_inputs', 'read_flights', 'clean_and_count', 'audit')


def measure(args):
    from pyspark import StorageLevel

    spark = spark_mod.get_spark(app_name='project07-flight-benchmark')
    if args.shuffle_partitions is not None:
        spark.conf.set('spark.sql.shuffle.partitions', args.shuffle_partitions)
    classified = None
    try:
        timing = {}
        start = time.perf_counter()
        assets = common.select_inputs(args.data_root / 'bronze', [f'bts:{args.month}'])
        timing['select_inputs'] = time.perf_counter() - start
        start = time.perf_counter()
        raw = flights.read_flights(spark, assets, args.data_root / 'bronze', 'benchmark')
        timing['read_flights'] = time.perf_counter() - start
        start = time.perf_counter()
        tables = flights.clean_flights(raw)
        # Persist before count so Spark materializes every rule, not just a row count.
        classified = tables.classified.persist(StorageLevel.DISK_ONLY)
        classified_rows = classified.count()
        timing['clean_and_count'] = time.perf_counter() - start
        start = time.perf_counter()
        counts = tables.audit.first().asDict()
        timing['audit'] = time.perf_counter() - start
        if args.explain:
            classified.explain(mode='formatted')
        print('RESULT ' + json.dumps({
            'month': args.month, 'shuffle_partitions': spark.conf.get('spark.sql.shuffle.partitions'),
            'spark_version': spark.version, 'classified_rows': classified_rows,
            'counts': counts, 'seconds': timing,
        }, sort_keys=True))
    finally:
        if classified is not None:
            classified.unpersist()
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month', default='2025-06')
    parser.add_argument('--data-root', type=Path, default=Path('/home/ubuntu/project-data'))
    parser.add_argument('--shuffle-partitions', type=int)
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--explain', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        measure(args)
        return
    results = []
    for run in range(3):
        command = [sys.executable, '-m', 'scripts.benchmark_flights', '--worker',
                   '--month', args.month, '--data-root', str(args.data_root)]
        if args.shuffle_partitions is not None:
            command += ['--shuffle-partitions', str(args.shuffle_partitions)]
        if run == 0:
            command += ['--explain']
        process = subprocess.run(command, text=True, capture_output=True)
        if process.returncode:
            print(process.stdout)
            print(process.stderr, file=sys.stderr)
            raise SystemExit(process.returncode)
        print(process.stdout, end='', flush=True)
        result = json.loads(next(line[7:] for line in process.stdout.splitlines() if line.startswith('RESULT ')))
        results.append(result)
        if (result['classified_rows'], result['counts']) != (results[0]['classified_rows'], results[0]['counts']):
            raise ValueError('Classified rows or audit counts changed between runs')
    print('Step | Min seconds | Median seconds')
    for step in (*STEPS, 'total'):
        values = [sum(r['seconds'].values()) if step == 'total' else r['seconds'][step] for r in results]
        print(f'{step} | {min(values):.3f} | {statistics.median(values):.3f}')


if __name__ == '__main__':
    main()
