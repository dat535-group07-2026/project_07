"""Shared SparkSession unittest base class for Silver and Gold tests."""

from __future__ import annotations

import unittest

from project07.spark import get_spark


class SparkTestCase(unittest.TestCase):
    """Base test case providing a deterministic local[2] SparkSession."""

    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.spark = get_spark(app_name=f"project07-tests-{cls.__name__}", env="test")
        cls.addClassCleanup(cls._stop_spark)

    @classmethod
    def _stop_spark(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
            cls.spark = None

