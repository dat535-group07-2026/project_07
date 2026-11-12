"""Check the visibility SQL boundaries without starting Spark."""

import sqlite3
import unittest

from scripts.gold_figures import VISIBILITY_BAND_SQL


class VisibilityBandTests(unittest.TestCase):
    def test_visibility_bands_and_missing_values(self):
        cases = [(0, 1, '<1.6 km'), (1.599, 1, '<1.6 km'), (1.6, 1, '1.6–5 km'),
                 (4.999, 1, '1.6–5 km'), (5, 1, '5–10 km'), (9.999, 1, '5–10 km'),
                 (10, 1, '>=10 km'), (16, 1, '>=10 km'),
                 (None, 1, 'Unknown visibility'), (None, None, 'No weather')]
        with sqlite3.connect(':memory:') as db:
            for visibility, observation, expected in cases:
                with self.subTest(visibility=visibility, observation=observation):
                    actual = db.execute(f'SELECT {VISIBILITY_BAND_SQL} FROM '
                                        '(SELECT ? visibility_km, ? observation_utc)',
                                        (visibility, observation)).fetchone()[0]
                    self.assertEqual(actual, expected)
