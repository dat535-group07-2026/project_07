"""Check the airport time zones in project07/reference/airports.csv against BTS data.

BTS gives the scheduled departure clock in the origin's local time, the scheduled
arrival clock in the destination's local time, and the scheduled elapsed minutes.
For a flight between JFK/LGA (time zone known) and another airport X, those three
values give X's UTC offset on that day:

    local time = UTC + offset, and departure UTC + elapsed = arrival UTC, so
    offset(origin) = departure clock - arrival clock + elapsed + offset(destination)

worked out modulo one day. The script compares that offset with the one the
reference time zone gives on the flight date, and fails if an airport disagrees
on more than 5% of its flights. It only reads published Silver flights.

Note: in summer America/Phoenix and America/Los_Angeles have the same offset
(UTC-7), so summer months alone cannot tell them apart. Include winter months.

Example (dev):
    python -m scripts.check_airport_time_zones --env dev --dev-user alkval \
        --months 2025-06 2025-07 2025-08 2025-09 2025-10
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

ANCHORS = ("JFK", "LGA")
MIN_MATCH_SHARE = 0.95

# Minutes after midnight of an HHMM clock; 2400 is the end of the day.
_MINUTES = "(CASE WHEN {c} = '2400' THEN 1440 ELSE int(substr({c}, 1, 2)) * 60 + int(substr({c}, 3, 2)) END)"
# UTC offset in minutes of a time zone at noon UTC on a date (DST-aware).
_OFFSET = ("(unix_timestamp(from_utc_timestamp(timestamp(concat({d}, ' 12:00:00')), {z}))"
           " - unix_timestamp(timestamp(concat({d}, ' 12:00:00')))) DIV 60")

IMPLIED_OFFSET_SQL = f"""
WITH f AS (
  SELECT fl.origin, fl.dest, fl.flight_date,
         {_MINUTES.format(c='fl.scheduled_dep_hhmm')} AS dep_min,
         {_MINUTES.format(c='fl.scheduled_arr_hhmm')} AS arr_min,
         fl.scheduled_elapsed_min AS elapsed,
         o.time_zone AS origin_zone, d.time_zone AS dest_zone
  FROM flights fl
  JOIN zones o ON fl.origin = o.airport
  JOIN zones d ON fl.dest = d.airport
  WHERE fl.scheduled_dep_hhmm IS NOT NULL AND fl.scheduled_arr_hhmm IS NOT NULL
    AND fl.scheduled_elapsed_min IS NOT NULL
)
SELECT origin AS airport, origin_zone AS time_zone, flight_date,
       pmod(dep_min - arr_min + elapsed + {_OFFSET.format(d='flight_date', z='dest_zone')} + 720, 1440) - 720
           AS implied_offset_min,
       {_OFFSET.format(d='flight_date', z='origin_zone')} AS expected_offset_min
FROM f WHERE dest IN ({", ".join(repr(a) for a in ANCHORS)})
UNION ALL
SELECT dest AS airport, dest_zone AS time_zone, flight_date,
       pmod(arr_min - dep_min - elapsed + {_OFFSET.format(d='flight_date', z='origin_zone')} + 720, 1440) - 720
           AS implied_offset_min,
       {_OFFSET.format(d='flight_date', z='dest_zone')} AS expected_offset_min
FROM f WHERE origin IN ({", ".join(repr(a) for a in ANCHORS)})
"""

SUMMARY_SQL = """
SELECT airport, time_zone,
       COUNT(*) AS flights,
       SUM(CASE WHEN implied_offset_min = expected_offset_min THEN 1 ELSE 0 END) AS matching,
       mode(implied_offset_min) AS most_common_implied_offset_min
FROM implied GROUP BY airport, time_zone ORDER BY airport
"""


def implied_offsets(flights, airports):
    """One row per flight to or from JFK/LGA: implied vs expected offset of the other airport."""
    spark = flights.sparkSession
    zones = spark.createDataFrame([(a.code, a.time_zone) for a in airports.values()], "airport string, time_zone string")
    flights.createOrReplaceTempView("flights")
    zones.createOrReplaceTempView("zones")
    return spark.sql(IMPLIED_OFFSET_SQL)


def summarise(implied) -> list[dict]:
    """Per airport: flights checked, how many agree, and the most common implied offset."""
    implied.createOrReplaceTempView("implied")
    rows = []
    for row in implied.sparkSession.sql(SUMMARY_SQL).collect():
        share = row["matching"] / row["flights"]
        rows.append({**row.asDict(), "match_share": round(share, 4), "ok": share >= MIN_MATCH_SHARE})
    return rows


def main(argv: Sequence[str] | None = None) -> int:
    from project07 import config as cfg_mod
    from project07 import reference
    from project07 import spark as spark_mod
    from project07.silver import common

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    cfg_mod.add_config_args(parser)
    parser.add_argument("--months", nargs="+", required=True, help="Published Silver flight months, YYYY-MM")
    args = parser.parse_args(argv)

    cfg = cfg_mod.resolve_config(env=args.env, data_root=args.data_root, dev_user=args.dev_user)
    spark_mod.check_spark_env()
    spark = spark_mod.get_spark(app_name="project07-check-time-zones", env=cfg.env)
    try:
        flights = None
        for month in args.months:
            frame = common.read_ready_table(spark, cfg.silver_root, "flights", pointer_key=month)
            frame = frame.select("origin", "dest", "flight_date", "scheduled_dep_hhmm",
                                 "scheduled_arr_hhmm", "scheduled_elapsed_min")
            flights = frame if flights is None else flights.unionByName(frame)
        airports = reference.load_airports()
        summary = summarise(implied_offsets(flights, airports))
        anchored = flights.filter(flights.origin.isin(*ANCHORS) | flights.dest.isin(*ANCHORS))
        other = anchored.selectExpr(f"CASE WHEN origin IN {ANCHORS} THEN dest ELSE origin END AS airport")
        missing = sorted(r["airport"] for r in other.distinct().collect() if r["airport"] not in airports)
    finally:
        spark.stop()

    for row in summary:
        print(json.dumps(row, default=str))
    failed = [row["airport"] for row in summary if not row["ok"]]
    print(json.dumps({"months": args.months, "airports_checked": len(summary),
                      "failed": failed, "airports_not_in_table": missing}))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
