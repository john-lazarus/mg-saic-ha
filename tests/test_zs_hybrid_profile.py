"""MG ZS Hybrid+ (2025, series 'ZS32 EU') profile (#441).

@Jarinox1's tests, 18 Oct 2026, on the default profile:

    iSmart app HIGH     remoteClimateStatus 2, engine started (heat)
    iSmart app LOW      remoteClimateStatus 2
    HA Cool / Heat      control_climate echoed as status 3, A/C light on,
                        but the fan never blew
    windows             only the driver window reported (app too)
    fuel tank           41 L

The same as the MG3 Hybrid+ (ZP22, #258), whose hybrid system it shares.
"""

import sys
import unittest

import test_setup_and_config_flow  # noqa: F401 - loads the stubbed mg_saic package

CONST = sys.modules["mg_saic.const"]
COORD_CLS = sys.modules["mg_saic.coordinator"].SAICMGDataUpdateCoordinator
CLIMATE_SETTINGS = (
    "climate_status_cool",
    "climate_status_fan_only",
    "cool_uses_start_ac",
    "has_front_defrost",
    "has_front_passenger_window",
    "has_rear_windows",
    "supports_target_soc",
    "min_temp",
    "max_temp",
    "temp_offset",
)


def _profiled(series):
    c = COORD_CLS.__new__(COORD_CLS)
    c.vehicle_series = series
    c.battery_capacity_override = None
    COORD_CLS._apply_vehicle_profile(c)
    return c


class ZsHybridProfileTests(unittest.TestCase):
    def test_the_car_gets_its_own_profile(self):
        matches = [key for key in CONST.VEHICLE_PROFILES if key in "ZS32 EU"]
        self.assertEqual(matches, ["ZS32"])

    def test_simple_ac_like_the_mg3_hybrid(self):
        zs = CONST.VEHICLE_PROFILES["ZS32"]
        mg3 = CONST.VEHICLE_PROFILES["ZP22"]
        for key in CLIMATE_SETTINGS:
            self.assertEqual(zs.get(key), mg3.get(key), key)

    def test_the_coordinator_picks_it_up(self):
        c = _profiled("ZS32 EU")
        self.assertTrue(c.cool_uses_start_ac)
        self.assertEqual(c.climate_status_cool, {2})
        self.assertEqual(c.climate_status_fan_only, set())
        self.assertFalse(c.has_front_defrost)
        self.assertFalse(c.has_front_passenger_window)
        self.assertFalse(c.has_rear_windows)
        self.assertFalse(c.supports_target_soc)

    def test_fuel_tank(self):
        self.assertEqual(CONST.VEHICLE_PROFILES["ZS32"]["fuel_tank_litres"], 41.0)

    def test_the_mg3_hybrid_is_unchanged(self):
        c = _profiled("ZP22 EU")
        self.assertTrue(c.cool_uses_start_ac)
        self.assertEqual(CONST.VEHICLE_PROFILES["ZP22"]["fuel_tank_litres"], 36.0)


if __name__ == "__main__":
    unittest.main()
