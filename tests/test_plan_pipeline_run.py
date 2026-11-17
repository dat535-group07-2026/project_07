"""The planner: which Silver and Gold runs are out of date."""

import json
import tempfile
import unittest
from pathlib import Path

from project07.silver import chain, common, flights, weather
from scripts import plan_pipeline_run as plan

STATIONS = sorted(weather.STATION_AIRPORTS)


class PlannerTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        root = Path(folder.name)
        self.bronze, self.silver, self.gold = root / "bronze", root / "silver", root / "gold"
        self.bronze.mkdir()
        self.catalog = {"schema_version": 1, "assets": {}}
        for month in ("2024-12", "2025-01", "2025-02"):
            self.add_bts(month, "a" * 64)
        for station in STATIONS:
            self.add_noaa("2025", station, "b" * 64)
        self.runs = 0

    # --- helpers -----------------------------------------------------------
    def add_bts(self, month, sha):
        self.catalog["assets"][f"bts:{month}"] = [{"kind": "bts", "period": month, "sha256": sha}]
        (self.bronze / "catalog.json").write_text(json.dumps(self.catalog))

    def add_noaa(self, year, station, sha):
        self.catalog["assets"][f"noaa:{station}:{year}"] = [
            {"kind": "noaa", "period": year, "station": station, "sha256": sha}]
        (self.bronze / "catalog.json").write_text(json.dumps(self.catalog))

    def publish(self, root, table, key, report):
        self.runs += 1
        run_id = f"run-{self.runs}"
        common.publish_ready(root, table, root / table / "runs" / run_id / "_READY.json",
                             {"run_id": run_id, **report}, pointer_key=key)
        return run_id

    def publish_flights(self, month, sha="a" * 64, rule=flights.RULE_VERSION):
        return self.publish(self.silver, "flights", month, {
            "rule_version": rule, "inputs": [{"asset_id": f"bts:{month}", "version_sha256": sha}]})

    def publish_weather(self, year="2025", stations=STATIONS):
        return self.publish(self.silver, "weather_hourly", year, {
            "rule_version": weather.RULE_VERSION, "stations": list(stations),
            "inputs": [{"asset_id": f"noaa:{s}:{year}", "version_sha256": "b" * 64} for s in stations]})

    def pointers(self, table, keys):
        return {k: json.loads(common.current_pointer(self.silver, table, k).read_text()) for k in keys}

    def publish_departures(self, month, context):
        flight_runs = {m: self.pointers("flights", [m])[m]["run_id"] for m in context}
        return self.publish(self.silver, "departures_enriched", month, {
            "rule_version": chain.RULE_VERSION,
            "inputs": {"flights": flight_runs,
                       "weather_hourly": self.pointers("weather_hourly", common.published_keys(self.silver, "weather_hourly"))}})

    def publish_gold(self, months):
        inputs = {"weather_hourly": self.pointers("weather_hourly", common.published_keys(self.silver, "weather_hourly"))}
        for month in months:
            for table in ("flights", "departures_enriched"):
                inputs[f"{table} {month}"] = self.pointers(table, [month])[month]
        self.runs += 1
        run_id = f"g{self.runs}"
        for table in plan.GOLD_TABLES:
            common.publish_ready(self.gold, table, self.gold / table / "runs" / run_id / "_READY.json",
                                 {"run_id": run_id, "months": months, "inputs": inputs})

    def run_plan(self, layer):
        return plan.plan(layer, self.bronze, self.silver, self.gold, "2025-01")

    def publish_everything(self):
        for month in ("2025-01", "2025-02"):
            self.publish_flights(month)
        self.publish_weather()
        self.publish_departures("2025-01", ["2025-01"])
        self.publish_departures("2025-02", ["2025-01", "2025-02"])
        self.publish_gold(["2025-01", "2025-02"])

    # --- tests -------------------------------------------------------------
    def test_nothing_published_means_everything_runs_from_since(self):
        self.assertEqual(self.run_plan("flights"), ["2025-01", "2025-02"])
        self.assertEqual(self.run_plan("weather"), ["2025"])
        self.assertEqual(self.run_plan("gold"), ["yes"])

    def test_up_to_date_pipeline_runs_nothing(self):
        self.publish_everything()
        for layer in plan.LAYERS:
            with self.subTest(layer=layer):
                self.assertEqual(self.run_plan(layer), [])

    def test_changed_bronze_month_reruns_flights_then_departures_then_gold(self):
        self.publish_everything()
        self.add_bts("2025-01", "c" * 64)
        self.assertEqual(self.run_plan("flights"), ["2025-01"])
        self.publish_flights("2025-01", sha="c" * 64)
        # January changed, so January and February (which reads January too) are stale.
        self.assertEqual(self.run_plan("departures"), ["2025-01", "2025-02"])
        self.assertEqual(self.run_plan("gold"), ["yes"])

    def test_new_month_is_picked_up(self):
        self.publish_everything()
        self.add_bts("2025-03", "a" * 64)
        self.assertEqual(self.run_plan("flights"), ["2025-03"])
        self.publish_flights("2025-03")
        self.assertEqual(self.run_plan("departures"), ["2025-03"])

    def test_older_rules_are_rerun(self):
        self.publish_everything()
        self.publish_flights("2025-02", rule="flights-0.1")
        self.assertEqual(self.run_plan("flights"), ["2025-02"])

    def test_changed_weather_file_or_station_list_reruns_the_year(self):
        self.publish_everything()
        self.add_noaa("2025", STATIONS[0], "d" * 64)
        self.assertEqual(self.run_plan("weather"), ["2025"])
        self.add_noaa("2025", STATIONS[0], "b" * 64)
        self.publish_weather(stations=STATIONS[1:])
        self.assertEqual(self.run_plan("weather"), ["2025"])

    def test_gold_run_that_stopped_half_way_is_rerun(self):
        self.publish_everything()
        common.publish_ready(self.gold, "airport_hour", self.gold / "airport_hour" / "runs" / "old" / "_READY.json",
                             {"run_id": "old"})
        self.assertEqual(self.run_plan("gold"), ["yes"])

    def test_republished_weather_makes_departures_and_gold_stale(self):
        self.publish_everything()
        self.publish_weather()
        self.assertEqual(self.run_plan("departures"), ["2025-01", "2025-02"])
        self.assertEqual(self.run_plan("gold"), ["yes"])


if __name__ == "__main__":
    unittest.main()
