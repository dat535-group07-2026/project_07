import io
import json
import tempfile
import unittest
import urllib.error
import zipfile
from pathlib import Path
from unittest.mock import patch

from scripts import ingest_bronze as bronze


BTS_HEADER = ','.join(sorted(bronze.REQUIRED['bts']))
NOAA_HEADER = ','.join(sorted(bronze.REQUIRED['noaa']))


def archive_bytes(members=None):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        for name, content in (members or {'flights.csv': BTS_HEADER + '\n'}).items():
            archive.writestr(name, content)
    return buffer.getvalue()


class FakeClient:
    def __init__(self, files):
        self.files = files
        self.downloads = []
        self.probes = []

    def available(self, url):
        self.probes.append(url)
        value = self.files.get(url)
        if isinstance(value, Exception):
            raise value
        return value is not None

    def download(self, url, target):
        self.downloads.append(url)
        content = self.files[url]
        if isinstance(content, Exception):
            raise content
        target.write_bytes(content)
        return {'retrieved_at_utc': '2026-10-19T00:00:00+00:00'}


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'project-data'
        self.bronze = self.root / 'bronze'
        self.files = {bronze.flight_source('2025-06')['url']: archive_bytes()}
        for station in bronze.STATIONS.values():
            self.files[bronze.weather_source(2025, station)['url']] = (NOAA_HEADER + '\n').encode()
        self.client = FakeClient(self.files)

    def args(self, mode='manual', *extra):
        options = [mode, '--data-root', str(self.root)]
        if mode == 'manual':
            options += ['--start', '2025-06', '--end', '2025-06']
        return bronze.parser().parse_args(options + list(extra))

    def ingest(self, *extra):
        return bronze.run(self.args('manual', *extra), self.client)

    def reports(self):
        return [json.loads(path.read_text()) for path in (self.bronze / 'runs').glob('*.json')]

    def test_month_range_crosses_year(self):
        self.assertEqual(bronze.month_range('2025-12', '2026-02'), ['2025-12', '2026-01', '2026-02'])
        with self.assertRaises(ValueError):
            bronze.month_range('2026-01', '2025-12')

    def test_invalid_month(self):
        for value in ('2025-13', '2025-1', '1986-01', 'abc'):
            with self.assertRaises(Exception):
                bronze.month(value)

    def test_manual_download_and_idempotent_offline_rerun(self):
        first = self.ingest()
        self.assertTrue(first['new_or_changed_sources'])
        self.assertEqual(len(first['sources']), 3)
        count = len(self.client.downloads)
        second = self.ingest('--offline')
        self.assertFalse(second['new_or_changed_sources'])
        self.assertEqual(len(self.client.downloads), count)
        self.assertEqual(len(list((self.bronze / 'extracted').rglob('*.csv'))), 1)
        self.assertEqual(len(self.reports()), 2)

    def test_reads_all_csv_members_and_keeps_raw_bytes(self):
        payload = archive_bytes({'a.csv': BTS_HEADER + '\n', 'nested/b.csv': BTS_HEADER + '\n'})
        self.client.files[bronze.flight_source('2025-06')['url']] = payload
        result = self.ingest()
        flight = result['sources'][0]
        self.assertEqual(len(flight['csvs']), 2)
        self.assertEqual((self.bronze / flight['path']).read_bytes(), payload)

    def test_corrupt_download_is_not_published(self):
        self.client.files[bronze.flight_source('2025-06')['url']] = b'not a zip'
        with self.assertRaises(zipfile.BadZipFile):
            self.ingest()
        self.assertFalse((self.bronze / 'catalog.json').exists())
        self.assertEqual(self.reports()[0]['status'], 'failed')
        self.assertEqual(list(self.bronze.rglob('.download-*')), [])

    def test_cached_corruption_fails_without_overwriting(self):
        result = self.ingest()
        path = self.bronze / result['sources'][0]['path']
        path.write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            self.ingest('--offline')
        self.assertEqual(path.read_bytes(), b'corrupt')
        self.assertEqual(len(self.client.downloads), 3)

    def test_missing_offline_input_fails(self):
        with self.assertRaisesRegex(ValueError, 'offline'):
            self.ingest('--offline')
        self.assertEqual(self.client.downloads, [])

    def test_failed_refresh_preserves_catalog(self):
        self.ingest()
        previous = (self.bronze / 'catalog.json').read_bytes()
        self.client.files[bronze.flight_source('2025-06')['url']] = RuntimeError('network unavailable')
        with self.assertRaises(RuntimeError):
            self.ingest('--refresh')
        self.assertEqual((self.bronze / 'catalog.json').read_bytes(), previous)

    def test_changed_weather_retains_both_versions(self):
        first = self.ingest()
        source = bronze.weather_source(2025, bronze.STATIONS['JFK'])
        self.client.files[source['url']] = (NOAA_HEADER + '\n' + 'new observation\n').encode()
        self.ingest('--refresh')
        catalog = bronze.load_catalog(self.bronze)
        self.assertEqual(len(catalog['assets'][source['id']]), 2)
        self.assertTrue((self.bronze / first['sources'][1]['path']).exists())

    def test_unexpected_header_fails(self):
        self.client.files[bronze.flight_source('2025-06')['url']] = archive_bytes({'a.csv': 'wrong,columns\n'})
        with self.assertRaisesRegex(ValueError, 'header'):
            self.ingest()

    def test_schema_drift_blocks_refresh(self):
        self.ingest()
        self.client.files[bronze.flight_source('2025-06')['url']] = archive_bytes({'a.csv': BTS_HEADER + ',new_column\n'})
        with self.assertRaisesRegex(ValueError, 'schema changed'):
            self.ingest('--refresh')

    def test_archive_traversal_and_colliding_names_rejected(self):
        for members in ({'../escape.csv': BTS_HEADER + '\n'},
                        {'a/data.csv': BTS_HEADER + '\n', 'b/data.csv': BTS_HEADER + '\n'}):
            self.client.files[bronze.flight_source('2025-06')['url']] = archive_bytes(members)
            with self.assertRaises(ValueError):
                self.ingest()

    def test_failed_batch_can_be_retried(self):
        weather = bronze.weather_source(2025, bronze.STATIONS['JFK'])
        self.client.files[weather['url']] = RuntimeError('temporary outage')
        with self.assertRaises(RuntimeError):
            self.ingest()
        self.client.files[weather['url']] = (NOAA_HEADER + '\n').encode()
        self.assertEqual(self.ingest()['status'], 'complete')

    def test_legacy_manifest_import_reuses_sources_in_place(self):
        self.ingest()
        original = bronze.load_catalog(self.bronze)
        legacy = self.bronze / 'initial-download'
        legacy.mkdir()
        records = []
        for versions in original['assets'].values():
            version = versions[-1]
            path = self.bronze / version['path']
            raw = legacy / version['name']
            raw.write_bytes(path.read_bytes())
            records.append({'path': raw.name, 'size_bytes': raw.stat().st_size, 'sha256': bronze.sha256(raw),
                            'columns': version['columns'], 'extracted_csvs': []})
        bronze.write_json(legacy / 'manifest.json', {'status': 'complete', 'files': records})
        (self.bronze / 'catalog.json').unlink()
        self.client.downloads.clear()
        result = self.ingest('--offline')
        self.assertEqual(self.client.downloads, [])
        self.assertTrue(all(item['path'].startswith('initial-download/') for item in result['sources']))

    def test_new_mode_404_is_not_a_failure(self):
        result = bronze.run(self.args('new', '--since', '2025-07', '--through', '2025-07'), self.client)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['unavailable_months'], ['2025-07'])
        self.assertEqual(result['sources'], [])

    def test_new_mode_network_error_is_not_missing_release(self):
        url = bronze.flight_source('2025-07')['url']
        self.client.files[url] = urllib.error.URLError('DNS unavailable')
        with self.assertRaises(urllib.error.URLError):
            bronze.run(self.args('new', '--since', '2025-07', '--through', '2025-07'), self.client)
        self.assertEqual(self.reports()[0]['status'], 'failed')

    def test_new_mode_retries_when_no_month_has_been_released(self):
        bronze.run(self.args('new', '--since', '2025-07', '--through', '2025-07'), self.client)
        self.client.files[bronze.flight_source('2025-07')['url']] = archive_bytes()
        result = bronze.run(self.args('new', '--through', '2025-07'), self.client)
        self.assertEqual(result['requested_months'], ['2025-07'])
        self.assertEqual(len(result['sources']), 3)

    def test_new_month_refreshes_matching_annual_weather(self):
        self.ingest()
        self.client.downloads.clear()
        flight = bronze.flight_source('2025-07')
        self.client.files[flight['url']] = archive_bytes()
        result = bronze.run(self.args('new', '--through', '2025-07'), self.client)
        self.assertEqual(result['requested_months'], ['2025-07'])
        self.assertEqual(len(self.client.downloads), 3)

    def test_http_404_means_unavailable_but_403_fails(self):
        for code in (404, 403):
            error = urllib.error.HTTPError('https://example.invalid', code, 'test', {}, None)
            with patch('urllib.request.urlopen', side_effect=error):
                if code == 404:
                    self.assertFalse(bronze.HTTPClient(attempts=1).available('https://example.invalid'))
                else:
                    with self.assertRaises(urllib.error.HTTPError):
                        bronze.HTTPClient(attempts=1).available('https://example.invalid')

    def test_http_server_error_is_retried(self):
        error = urllib.error.HTTPError('https://example.invalid', 503, 'test', {}, None)
        response = io.BytesIO()
        with patch('urllib.request.urlopen', side_effect=[error, response]) as request:
            with patch('time.sleep'):
                self.assertTrue(bronze.HTTPClient(attempts=2).available('https://example.invalid'))
        self.assertEqual(request.call_count, 2)

    def test_new_mode_retries_previous_gaps(self):
        self.client.files[bronze.flight_source('2025-08')['url']] = archive_bytes()
        bronze.run(self.args('new', '--since', '2025-07', '--through', '2025-08'), self.client)
        self.client.files[bronze.flight_source('2025-07')['url']] = archive_bytes()
        result = bronze.run(self.args('new', '--through', '2025-08'), self.client)
        self.assertEqual(result['requested_months'], ['2025-07'])
        self.assertEqual(bronze.load_catalog(self.bronze)['pending_months'], [])

    def test_dry_run_has_no_network_or_writes(self):
        result = bronze.run(self.args('new', '--since', '2025-06', '--through', '2025-07', '--dry-run'), self.client)
        self.assertEqual(result['candidate_months'], ['2025-06', '2025-07'])
        self.assertFalse(self.root.exists())
        self.assertEqual(self.client.probes + self.client.downloads, [])

    def test_no_overlapping_runs(self):
        with bronze.ingestion_lock(self.bronze):
            with self.assertRaisesRegex(RuntimeError, 'active'):
                self.ingest()

    def test_manifest_paths_cannot_escape_bronze(self):
        for value in ('../secret', '/etc/passwd'):
            with self.assertRaises(ValueError):
                bronze.safe_path(self.bronze, value)

    def test_transport_rejects_truncated_download(self):
        response = io.BytesIO(b'abc')
        response.headers = {'Content-Length': '10'}
        response.geturl = lambda: 'https://example.invalid/source'
        target = Path(self.temp.name) / 'download.part'
        with patch('urllib.request.urlopen', return_value=response):
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                bronze.HTTPClient(attempts=1).download('https://example.invalid/source', target)
        self.assertFalse(target.exists())


if __name__ == '__main__':
    unittest.main()
