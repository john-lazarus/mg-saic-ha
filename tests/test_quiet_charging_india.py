"""India has no cached-status backend: quiet charging stays unavailable."""
import asyncio
import sys
import unittest
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock, patch

import test_quiet_charging_charger as maintained

CF = maintained.CF
COORD = maintained.COORD_MOD
INDIA = sys.modules["mg_saic.backends.india"].IndiaBackend


class IndiaQuietChargingTests(unittest.TestCase):
    def _india(self):
        c = maintained._coordinator("on", entity="input_boolean.synthetic_charging")
        c.client = INDIA("0000000000", "synthetic", vin="SYNTHETIC")
        self.assertFalse(callable(getattr(c.client, "get_cached_vehicle_status", None)))
        return c

    def test_no_charger_state_or_subscription_without_cache(self):
        c = self._india()
        self.assertIsNone(c._charger_charging())
        old, stop, track = MagicMock(), MagicMock(), MagicMock()
        c._charger_unsub, c._charger_stop_unsub = old, stop
        with patch.object(COORD, "async_track_state_change_event", track):
            c._watch_charger()
        old.assert_called_once()
        stop.assert_called_once()
        track.assert_not_called()
        self.assertIsNone(c._charger_unsub)
        self.assertIsNone(c._charger_stop_unsub)

    def test_charger_events_and_finish_timer_use_normal_live_poll(self):
        c = self._india()
        later = MagicMock()
        with patch.object(COORD, "async_call_later", later):
            asyncio.run(c._handle_charger_change(maintained._event("off", "on")))
            asyncio.run(c._handle_charger_change(maintained._event("on", "off")))
            asyncio.run(c._handle_charger_stopped())
        later.assert_not_called()
        c.async_request_refresh.assert_not_awaited()
        c._quiet_finish_check_at = maintained.NOW
        asyncio.run(c._async_update_data())
        c._run_update_cycle.assert_awaited_once()
        c._after_quiet_live_poll.assert_not_called()

    def _flow(self, saved=None):
        flow = CF.SAICMGOptionsFlowHandler.__new__(CF.SAICMGOptionsFlowHandler)
        flow.config_entry = NS(options=saved or {}, data={"region": "India"})
        flow._validate_abrp = AsyncMock(return_value={})
        flow.async_create_entry = lambda title, data: {"data": data}
        flow.async_show_form = lambda **kw: kw
        return flow

    def test_options_hide_both_quiet_charging_and_charger_picker(self):
        form = asyncio.run(self._flow().async_step_init())
        keys = {getattr(k, "schema", k) for k in form["data_schema"].schema}
        self.assertNotIn("quiet_hours", keys)
        self.assertNotIn("quiet_charging_charger_entity", keys)

    def test_save_removes_unsupported_new_and_persisted_options(self):
        saved = {"quiet_hours_start": "23:00", "quiet_hours_end": "06:00",
                 "quiet_hours_live_polling": False,
                 "quiet_hours_live_polling_at": "2026-10-12T00:00:00+00:00",
                 "holiday_mode": True}
        entered = {"quiet_hours": True, "quiet_charging_charger_entity": "input_boolean.synthetic_charging",
                   "update_interval": 45}
        data = asyncio.run(self._flow(saved).async_step_init(entered))["data"]
        self.assertFalse(any(key.startswith("quiet_") for key in data))
        self.assertTrue(data["holiday_mode"])
        self.assertEqual(data["update_interval"], 45)
