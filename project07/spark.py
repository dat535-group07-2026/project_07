"""SparkSession factory and runtime environment checks."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

VM_JAVA8_HOME = Path("/usr/lib/jvm/java-8-openjdk-amd64")


def check_spark_env(require_vm_java_path: bool = False) -> None:
    """Fail early with a clear message if Java 8 or PySpark driver env vars are misconfigured."""
    if os.environ.get("PYSPARK_DRIVER_PYTHON") or os.environ.get("PYSPARK_DRIVER_PYTHON_OPTS"):
        raise ValueError("Unset PYSPARK_DRIVER_PYTHON and PYSPARK_DRIVER_PYTHON_OPTS")

    java_home_raw = os.environ.get("JAVA_HOME", "").strip()
    if not java_home_raw:
        raise ValueError(f"Set JAVA_HOME={VM_JAVA8_HOME}")

    java_home = Path(java_home_raw)
    if require_vm_java_path or VM_JAVA8_HOME.exists():
        if not java_home.exists() or java_home.resolve() != VM_JAVA8_HOME.resolve():
            raise ValueError(f"Set JAVA_HOME={VM_JAVA8_HOME}")

    java_bin = java_home / "bin" / "java"
    if not java_bin.is_file():
        raise ValueError(f"JAVA_HOME does not contain bin/java: {java_home}")

    proc = subprocess.run(
        [str(java_bin), "-version"],
        capture_output=True,
        text=True,
        check=False,
    )
    version_output = (proc.stderr or "") + (proc.stdout or "")
    if proc.returncode != 0 or 'version "1.8.' not in version_output:
        raise ValueError(f"Spark requires Java 8 (1.8.x); got: {version_output.splitlines()[:1]}")


def get_spark(
    app_name: str = "project07-silver",
    env: str = "dev",
    *,
    driver_memory: str = "3g",
) -> SparkSession:
    """Create or retrieve a configured SparkSession with UTC time zone and ANSI disabled.

    Uses local[2] with small shuffle partitions when env == 'test', and local[*]
    with driver memory configured for VM batch runs otherwise.
    """
    from pyspark.sql import SparkSession

    if env != "test":
        check_spark_env()

    master = "local[2]" if env == "test" else "local[*]"
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
    )
    if env == "test":
        builder = builder.config("spark.sql.shuffle.partitions", "2")
    else:
        # June benchmark: 8 shuffle partitions cut the measured median from 46.9s to 43.0s on the 4-core VM.
        builder = builder.config("spark.sql.shuffle.partitions", "8")
        builder = builder.config("spark.driver.memory", driver_memory)

    spark = builder.getOrCreate()
    # Enforce session settings even if an existing session was reused in the same process.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.sparkContext.setLogLevel("WARN")
    return spark

