"""Environment and path resolution for Silver and Gold pipelines."""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DATA_ROOT = Path("/home/ubuntu/project-data")
VALID_ENVS = ("dev", "prod")
SILVER_TABLES = (
    "flights",
    "flights_quarantine",
    "weather_observations",
    "weather_quarantine",
    "weather_hourly",
    "audit",
)


def _check_no_symlinks(base: Path, subpaths: tuple[Path, ...] = ()) -> None:
    """Refuse symlinks anywhere along output paths so writes cannot be redirected."""
    for target in (base, *subpaths):
        # Compare lexical normalization against symlink-resolving Path.resolve().
        lexical = Path(os.path.abspath(target))
        if target.resolve() != lexical:
            raise ValueError(f"Output path goes through a symlink: {target}")


@dataclass(frozen=True)
class PipelineConfig:
    env: str
    data_root: Path
    dev_user: str | None = None

    def __post_init__(self) -> None:
        if self.env not in VALID_ENVS:
            raise ValueError(f"Unsupported environment {self.env!r}; expected one of {VALID_ENVS}")
        resolved_root = Path(os.path.abspath(self.data_root))
        if Path(self.data_root).resolve() != resolved_root:
            raise ValueError(f"Data root goes through a symlink: {self.data_root}")
        object.__setattr__(self, "data_root", resolved_root)
        if self.env == "dev":
            if not self.dev_user:
                raise ValueError("--dev-user is required when --env dev is selected")
            if self.dev_user == "ubuntu":
                raise ValueError("Do not use shared VM account 'ubuntu' as --dev-user")
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", self.dev_user):
                raise ValueError(f"Invalid dev_user {self.dev_user!r}; provide a safe identifier")
        else:
            if self.dev_user is not None:
                raise ValueError("Do not specify --dev-user when --env prod is selected")

    @property
    def bronze_root(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_root(self) -> Path:
        if self.env == "dev":
            assert self.dev_user is not None
            out = self.data_root / "silver" / "dev" / self.dev_user
        else:
            out = self.data_root / "silver" / "prod"
        subpaths = tuple(
            p
            for table in SILVER_TABLES
            for p in (out / table / "current", out / table / "runs")
        )
        _check_no_symlinks(out, subpaths)
        return out

    @property
    def gold_root(self) -> Path:
        if self.env == "dev":
            assert self.dev_user is not None
            out = self.data_root / "gold" / "dev" / self.dev_user
        else:
            out = self.data_root / "gold" / "prod"
        _check_no_symlinks(out)
        return out


def resolve_config(
    env: str = "dev",
    data_root: Path | str = DEFAULT_DATA_ROOT,
    dev_user: str | None = None,
) -> PipelineConfig:
    """Build and validate a PipelineConfig for dev or prod."""
    return PipelineConfig(env=env, data_root=Path(data_root), dev_user=dev_user)


def add_config_args(parser: argparse.ArgumentParser) -> None:
    """Attach standard --env, --data-root and --dev-user arguments to a CLI parser."""
    parser.add_argument(
        "--env",
        required=True,
        choices=VALID_ENVS,
        help="Target environment (dev writes under silver/dev/<user>/, prod under silver/prod/).",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="External data root containing bronze/, silver/, and gold/.",
    )
    parser.add_argument(
        "--dev-user",
        default=None,
        help="Developer namespace required when --env dev is used (e.g. ckolle or alkval).",
    )

