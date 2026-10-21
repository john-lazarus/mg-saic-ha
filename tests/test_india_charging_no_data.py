"""A missing India charge cache must not trigger another poll or timeout."""

import asyncio
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock, patch

with patch.dict("sys.modules", {
    "aiohttp": MagicMock(), "mg_ismart_india_client": MagicMock(),
}):
    import test_charging_data_freshness as freshness

COORD_MOD = freshness.COORD_MOD


class IndiaChargingNoDataTests(unittest.TestCase):
    def test_missing_cache_is_read_once_even_without_startup_poll_budget(self):
        status = NS(statusTime=1790011000, basicVehicleStatus=NS())
        c = freshness.UpdateCycleRecordingTests()._coord(
            status=lambda: status, charging=lambda: None, vehicle_type="BEV"
        )
        # Keep the real retry implementation as well as the real update cycle.
        del c._fetch_with_retries
        c.client = NS(
            _india_backend=True,
            get_vehicle_info=AsyncMock(return_value=[NS(vin=c.vin)]),
            get_vehicle_status=AsyncMock(return_value=status),
            get_charging_info=AsyncMock(return_value=None),
        )
        c.is_initial_setup = True
        c._note_refresh_step = MagicMock()
        c._show_saved_vehicle_data = MagicMock()
        with (
            patch.object(COORD_MOD, "_startup_wait", side_effect=[10, 0]),
            patch.object(COORD_MOD.asyncio, "sleep", new_callable=AsyncMock) as sleep,
            self.assertLogs(COORD_MOD.LOGGER, "DEBUG") as logs,
        ):
            data = asyncio.run(c._run_update_cycle_inner())

        c.client.get_charging_info.assert_awaited_once_with(c.vin)
        c.client.get_vehicle_status.assert_awaited_once_with(c.vin)
        sleep.assert_not_awaited()
        self.assertIs(data["status"], status)
        self.assertIsNone(data["charging"])
        self.assertEqual(c.charging_data_freshness, "no_data")
        self.assertEqual(c.charging_freshness.last_error, "No cached charging frame")
        self.assertIsNone(c.charging_freshness.last_success)
        self.assertEqual(c.charging_freshness.consecutive_failures, 1)
        self.assertTrue(c._charging_outcome_recorded)
        self.assertFalse(any(r.levelno >= 30 for r in logs.records))
        self.assertNotRegex(
            " ".join(r.getMessage().lower() for r in logs.records),
            r"timed out|startup time used up|retry",
        )

    def test_source_clock_survives_replay_and_recovery_with_freshness_logs(self):
        source_times = (freshness.T0, freshness.T0, freshness.T0 + timedelta(minutes=3))
        receipt_times = tuple(freshness.T0 + timedelta(minutes=n) for n in (1, 2, 4))
        frames = [
            NS(
                india=True,
                chrgMgmtData=NS(bmsPackSOCDsp=620, bmsChrgSts=0),
                rvsChargeStatus=NS(india=True, measurement_time=stamp.timestamp()),
            )
            for stamp in source_times
        ]
        status = NS(statusTime=freshness.T0.timestamp(), basicVehicleStatus=NS())
        c = freshness.UpdateCycleRecordingTests()._coord(
            status=lambda: status, charging=lambda: None, vehicle_type="BEV"
        )
        del c._fetch_with_retries
        c.client = NS(
            _india_backend=True,
            get_vehicle_info=AsyncMock(return_value=[NS(vin=c.vin)]),
            get_vehicle_status=AsyncMock(return_value=status),
            get_charging_info=AsyncMock(side_effect=frames),
        )
        c.counter_reset_guard = COORD_MOD.SinceChargeCounterGuard()
        c.last_charging_data = None

        with (
            patch.object(COORD_MOD, "datetime", wraps=datetime) as clock,
            patch.object(COORD_MOD.asyncio, "sleep", new_callable=AsyncMock) as sleep,
            self.assertLogs(COORD_MOD.LOGGER, "INFO") as logs,
        ):
            for index, (measured, received, state, failures) in enumerate(zip(
                source_times, receipt_times, ("live", "stale", "live"), (0, 1, 0)
            )):
                with self.subTest(cycle=index):
                    clock.now.return_value = received
                    data = asyncio.run(c._run_update_cycle_inner())
                    self.assertIs(data["charging"], frames[index])
                    self.assertEqual(c.charging_data_freshness, state)
                    self.assertEqual(c.charging_freshness.last_success, measured)
                    self.assertEqual(c.charging_freshness.last_attempt, received)
                    self.assertEqual(c.charging_freshness.consecutive_failures, failures)
                    self.assertTrue(c._charging_outcome_recorded)
                    self.assertEqual(c.client.get_charging_info.await_count, index + 1)
                    if failures:
                        self.assertEqual(
                            c.charging_freshness.last_error,
                            "No advancing India charge measurement time",
                        )
                    else:
                        self.assertIsNone(c.charging_freshness.last_error)

        sleep.assert_not_awaited()
        transitions = [
            r.getMessage() for r in logs.records
            if r.getMessage().startswith("Charging data for VIN")
        ]
        self.assertEqual(len(transitions), 2)
        self.assertIn("not available (No advancing India charge measurement time)", transitions[0])
        self.assertIn("back after", transitions[1])
        self.assertFalse(any(r.levelno >= 30 for r in logs.records))
