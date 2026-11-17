"""Hand-made departures with hand-calculated airport_hour and departure_chain rows."""

from datetime import date, datetime

from project07.gold.chain import departure_chain
from project07.gold.hourly import airport_hour
from tests.spark_case import SparkTestCase

SCHEMA = (
    "key string, origin string, flight_date date, weather_hour_utc timestamp, cancelled boolean, "
    "dep_delay_min int, observation_utc timestamp, temperature_c double, precipitation_mm double, "
    "precipitation_trace boolean, wind_speed_mps double, wind_gust_mps double, visibility_km double, "
    "thunderstorm boolean, link_status string, inbound_arr_delay_min int, scheduled_ground_min int, "
    "origin_observation_utc timestamp, origin_precipitation_mm double, origin_visibility_km double, "
    "origin_thunderstorm boolean, late_aircraft_delay_min int, weather_delay_min int"
)
D = date(2025, 6, 1)
H14 = datetime(2025, 6, 1, 14)  # 10:00 in New York
OBS = datetime(2025, 6, 1, 14, 51)
H15 = datetime(2025, 6, 1, 15)


def row(key, origin, hour, cancelled, delay, observed, visibility, status, inbound=None, ground=None,
        origin_observed=None, origin_rain=None, origin_visibility=None, origin_storm=None,
        late_aircraft=None, weather=None):
    return (key, origin, D, hour, cancelled, delay, observed, 20.0 if observed else None, 0.0 if observed else None,
            False if observed else None, 3.0 if observed else None, None, visibility,
            False if observed else None, status, inbound, ground, origin_observed, origin_rain, origin_visibility,
            origin_storm, late_aircraft, weather)


ROWS = [
    # JFK, 14:00 UTC, poor visibility (3 km): bad local weather.
    row("a", "JFK", H14, False, 30, OBS, 3.0, "linked", inbound=20, ground=50,
        origin_observed=OBS, origin_rain=0.0, origin_visibility=16.0, origin_storm=False,
        late_aircraft=15, weather=0),
    row("b", "JFK", H14, False, 0, OBS, 3.0, "linked", inbound=5, ground=70),
    row("c", "JFK", H14, True, None, OBS, 3.0, "cancelled"),
    # LGA, 15:00 UTC, no weather report.
    row("d", "LGA", H15, False, 100, None, None, "linked", inbound=95, ground=40,
        origin_observed=OBS, origin_rain=2.5, origin_visibility=16.0, origin_storm=False,
        late_aircraft=90, weather=10),
    row("e", "LGA", H15, False, 10, None, None, "no_inbound_found"),
]


class GoldTablesTests(SparkTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.departures = cls.spark.createDataFrame(ROWS, SCHEMA)

    def test_airport_hour(self):
        rows = {r.origin: r for r in airport_hour(self.departures).collect()}
        jfk = rows["JFK"]
        self.assertEqual((jfk.local_date, jfk.local_hour), (D, 10))
        self.assertEqual((jfk.flights, jfk.cancelled_flights), (3, 1))
        self.assertEqual(jfk.avg_dep_delay_min, 15.0)
        self.assertEqual(jfk.delayed_15_rate, 0.5)
        self.assertTrue(jfk.has_weather)
        self.assertEqual((jfk.visibility_km, jfk.temperature_c, jfk.thunderstorm), (3.0, 20.0, False))
        lga = rows["LGA"]
        self.assertEqual((lga.flights, lga.avg_dep_delay_min, lga.delayed_15_rate), (2, 55.0, 0.5))
        self.assertFalse(lga.has_weather)
        self.assertIsNone(lga.visibility_km)

    def test_departure_chain_groups_linked_departures(self):
        rows = {(r.origin, r.inbound_delay_band, r.local_weather, r.origin_weather): r
                for r in departure_chain(self.departures).collect()}
        self.assertEqual(set(rows), {
            ("JFK", "15-44", "bad", "good"),
            ("JFK", "under 15", "bad", "unknown"),
            ("LGA", "90+", "unknown", "bad"),
        })
        a = rows[("JFK", "15-44", "bad", "good")]
        self.assertEqual((a.departures, a.delayed_15, a.total_dep_delay_min), (1, 1, 30))
        self.assertEqual((a.total_inbound_arr_delay_min, a.total_scheduled_ground_min), (20, 50))
        self.assertEqual((a.bts_late_aircraft_delay_min, a.bts_weather_delay_min), (15, 0))
        d = rows[("LGA", "90+", "unknown", "bad")]
        self.assertEqual((d.departures, d.delayed_15, d.bts_late_aircraft_delay_min), (1, 1, 90))

    def test_only_linked_departures_are_counted(self):
        total = sum(r.departures for r in departure_chain(self.departures).collect())
        self.assertEqual(total, 3)
