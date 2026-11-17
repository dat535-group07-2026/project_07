"""Airport reference data: time zone and NOAA weather station per airport.

The table lives in airports.csv next to this file. Lines starting with '#' are
comments that explain the columns and where the values come from.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

AIRPORTS_CSV = Path(__file__).with_name("airports.csv")
COLUMNS = ["airport", "city", "time_zone", "noaa_station"]
# NOAA LCD v2 station IDs: country code, W, then the 8-digit WBAN number,
# for example USW00094789 (JFK) or RQW00011641 (San Juan, Puerto Rico).
STATION_PATTERN = re.compile(r"^[A-Z]{2}W[0-9]{8}$")


@dataclass(frozen=True)
class Airport:
    code: str
    city: str
    time_zone: str
    noaa_station: str | None


def load_airports(path: Path = AIRPORTS_CSV) -> dict[str, Airport]:
    """Read and check the reference table. Any problem raises ValueError."""
    with Path(path).open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(line for line in stream if not line.startswith("#")))
    if not rows or rows[0] != COLUMNS:
        raise ValueError(f"{path}: header must be {','.join(COLUMNS)}")

    airports: dict[str, Airport] = {}
    stations: set[str] = set()
    for number, row in enumerate(rows[1:], start=2):
        if len(row) != len(COLUMNS):
            raise ValueError(f"{path} row {number}: expected {len(COLUMNS)} fields, got {len(row)}")
        code, city, time_zone, station = (value.strip() for value in row)
        if not re.fullmatch(r"[A-Z]{3}", code):
            raise ValueError(f"{path} row {number}: invalid airport code {code!r}")
        if code in airports:
            raise ValueError(f"{path} row {number}: airport {code} is listed twice")
        try:
            ZoneInfo(time_zone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(f"{path} row {number}: unknown time zone {time_zone!r}") from error
        if station:
            if not STATION_PATTERN.fullmatch(station):
                raise ValueError(f"{path} row {number}: invalid NOAA station {station!r}")
            if station in stations:
                raise ValueError(f"{path} row {number}: NOAA station {station} is listed twice")
            stations.add(station)
        airports[code] = Airport(code, city, time_zone, station or None)
    return airports


def weather_stations(path: Path = AIRPORTS_CSV) -> dict[str, str]:
    """NOAA station ID -> airport code, for the airports that have weather."""
    return {a.noaa_station: a.code for a in load_airports(path).values() if a.noaa_station}


def standard_offset_hours(time_zone: str) -> int:
    """UTC offset of the zone's standard (winter) time, for example -5 for New York.

    NOAA LCD timestamps are in each station's local standard time all year, so
    UTC = local standard time - this offset. Measured on 15 January, when no US
    zone uses daylight saving time.
    """
    offset = datetime(2025, 1, 15, 12, tzinfo=ZoneInfo(time_zone)).utcoffset()
    hours, rest = divmod(offset.total_seconds(), 3600)
    if rest:
        raise ValueError(f"{time_zone}: standard offset is not a whole number of hours")
    return int(hours)
