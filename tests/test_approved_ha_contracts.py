"""Focused offline vertical contracts; collect in a separate HA harness process."""
import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock, patch

import test_charging_data_freshness as freshness
import test_setup_and_config_flow as harness
from test_backends import FakeIndiaClient, india
from test_climate_presets_and_local_control import CLIMATE, _Base
from test_india_soc import IndiaBEVStateOfChargeTests, SENSOR

SETUP = harness.SETUP
sys.modules['mg_saic']._raise_india_login_failure = SETUP._raise_india_login_failure
COORD = sys.modules['mg_saic.coordinator']
C = COORD.SAICMGDataUpdateCoordinator
ABRP = harness._load('mg_saic.abrp', harness.PKG_DIR / 'abrp.py')


def entry(key, account='synthetic@example.invalid'):
    return NS(entry_id=key, options={}, data={
        'username': account, 'password': 'synthetic', 'region': 'India',
        'vin': 'SYNTHETIC-' + key, 'vehicle_type': 'BEV',
    })


class TestAccountLifecycle(unittest.IsolatedAsyncioTestCase):
    async def test_terminal_india_failure_keeps_setup_and_fetch_classification(self):
        from mg_ismart_india_client.client import MgIndiaLoginRejected
        for credential_failure in (True, False):
            with self.subTest(credential_failure=credential_failure):
                rejection = MgIndiaLoginRejected('synthetic-private-login-response')
                if credential_failure:
                    rejection.return_code = 401
                expected = (harness._ConfigEntryAuthFailed if credential_failure
                            else SETUP.ConfigEntryError)
                wrapped = RuntimeError('synthetic-wrapper')
                wrapped.__cause__ = rejection
                c = C.__new__(C)
                fetch = AsyncMock(side_effect=wrapped)
                with self.assertRaises(expected) as caught:
                    await c._fetch_with_retries(fetch, lambda _data: False, 'synthetic')
                fetch.assert_awaited_once()
                self.assertIsNone(caught.exception.__cause__)
                self.assertTrue(caught.exception.__suppress_context__)
                self.assertNotIn('synthetic-private-login-response', str(caught.exception))
                e = entry('TERMINAL')
                hass = NS(data={}, config_entries=NS(async_entries=lambda _domain: [e]))
                local = NS(login=AsyncMock(side_effect=wrapped), close=AsyncMock(),
                           set_api_lock=MagicMock())
                with patch.object(SETUP, 'create_backend', return_value=local):
                    with self.assertRaises(expected):
                        await SETUP.async_setup_entry(hass, e)
                local.login.assert_awaited_once()
                local.close.assert_awaited_once()
                self.assertFalse(hass.data[SETUP.DOMAIN]['setups_in_progress'])

    async def test_local_login_cancel_and_failed_setup_close_once(self):
        for mode in ('cancel', 'login-failure', 'vehicle-list-failure'):
            with self.subTest(mode=mode):
                e = entry('A')
                hass = NS(data={}, config_entries=NS(async_entries=lambda _domain: [e]))
                started, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
                lock_received = []
                async def login():
                    if mode == 'cancel':
                        started.set()
                        await asyncio.Event().wait()
                    elif mode == 'login-failure':
                        raise RuntimeError('synthetic-private-response')
                async def close():
                    if mode == 'cancel':
                        closing.set()
                        await release.wait()
                local = NS(login=AsyncMock(side_effect=login),
                           close=AsyncMock(side_effect=close),
                           set_api_lock=lock_received.append,
                           get_vehicle_info=AsyncMock(return_value=[]))
                with patch.object(SETUP, 'create_backend', return_value=local):
                    task = asyncio.create_task(SETUP.async_setup_entry(hass, e))
                    if mode == 'cancel':
                        await started.wait()
                        task.cancel('first-synthetic-cancel')
                        await closing.wait()
                        task.cancel('second-synthetic-cancel')
                        release.set()
                        with self.assertRaises(asyncio.CancelledError) as caught:
                            await task
                        self.assertEqual(caught.exception.args, ('first-synthetic-cancel',))
                    else:
                        with self.assertRaises(harness._ConfigEntryNotReady):
                            await task
                domain = hass.data[SETUP.DOMAIN]
                self.assertFalse(domain['setups_in_progress'])
                self.assertFalse(domain['account_clients'])
                self.assertNotIn(e.entry_id, domain)
                self.assertEqual(local.close.await_count, 1)
                self.assertIs(lock_received[0], domain['account_locks'][SETUP._account_key(e)])
                self.assertFalse(lock_received[0].locked())

    async def test_last_owner_cleanup_without_poller_preserves_other_account(self):
        a, b, other = entry('A'), entry('B'), entry('OTHER', 'other@example.invalid')
        key, other_key = SETUP._account_key(a), SETUP._account_key(other)
        shared, other_client = NS(close=AsyncMock()), NS(close=AsyncMock())
        api_lock, login_lock = asyncio.Lock(), asyncio.Lock()
        domain = {
            'A': shared, 'B': shared, 'OTHER': other_client,
            'account_clients': {key: shared, other_key: other_client},
            'account_locks': {key: api_lock, other_key: asyncio.Lock()},
            'account_login_locks': {key: login_lock, other_key: asyncio.Lock()},
            'clients_by_vin': {a.data['vin']: shared, b.data['vin']: shared},
            'coordinators_by_vin': {}, 'setups_in_progress': set(),
        }
        hass = NS(data={SETUP.DOMAIN: domain}, config_entries=NS(
            async_entries=lambda _domain: [a, b, other],
            async_unload_platforms=AsyncMock(return_value=True)))
        self.assertTrue(await SETUP.async_unload_entry(hass, a))
        shared.close.assert_not_awaited()
        self.assertIs(domain['account_clients'][key], shared)
        self.assertTrue(await SETUP.async_unload_entry(hass, b))
        shared.close.assert_awaited_once()
        other_client.close.assert_not_awaited()
        self.assertIs(domain['account_clients'][other_key], other_client)
        self.assertIs(domain['account_locks'][key], api_lock)
        self.assertIs(domain['account_login_locks'][key], login_lock)
        self.assertFalse(domain['clients_by_vin'])

    async def test_two_vin_poll_command_serialization_and_refresh_outside_lock(self):
        backend = india.IndiaBackend('0000000000', 'synthetic', vin='VIN1')
        fake = FakeIndiaClient()
        backend._client = fake
        backend._electric_vins = {'VIN1', 'VIN2'}
        lock = asyncio.Lock()
        backend.set_api_lock(lock)
        entered, release = asyncio.Event(), asyncio.Event()
        trace = []
        original_status = fake.status
        async def status(include_charge=False):
            trace.append(('poll-start', fake.vin))
            entered.set()
            await release.wait()
            trace.append(('poll-end', fake.vin))
            return await original_status(include_charge)
        async def vehicles():
            return [NS(vin=vin, raw={}) for vin in ('VIN1', 'VIN2')]
        async def climate(on):
            trace.append(('command', fake.vin, on))
        fake.status, fake.vehicles, fake.control_climate = status, vehicles, climate
        c = freshness.UpdateCycleRecordingTests()._coord(status=lambda: None, charging=lambda: None)
        c.vin = 'VIN1'
        c.config_entry.data['vin'] = 'VIN1'
        c.client, c._api_lock = backend, lock
        async def fetch(call, _generic, _name):
            return await call()
        c._fetch_with_retries = fetch
        poll = asyncio.create_task(c._run_update_cycle())
        await entered.wait()
        command = asyncio.create_task(backend.start_ac('VIN2'))
        await asyncio.sleep(0)
        self.assertEqual(trace, [('poll-start', 'VIN1')])
        self.assertEqual(fake.vin, 'VIN1')
        release.set()
        data = await poll
        await command
        self.assertEqual(trace, [('poll-start', 'VIN1'), ('poll-end', 'VIN1'), ('command', 'VIN2', True)])
        self.assertEqual(data['info'][0].vin, 'VIN1')
        self.assertEqual(data['charging'].statusTime, 1_799_999_900)
        self.assertFalse(backend._charge_status_by_vin)
        entity = _Base()._entity(scheme='mode_select')
        entity._scheme, entity._client, entity._vin = 'on_off', backend, 'VIN2'
        async def refresh(*args):
            self.assertFalse(lock.locked())
            async with lock:
                trace.append(('refresh', fake.vin))
        entity.coordinator.schedule_action_refresh = AsyncMock(side_effect=refresh)
        await entity.async_set_hvac_mode(CLIMATE.HVACMode.OFF)
        self.assertEqual(trace[-2:], [('command', 'VIN2', False), ('refresh', 'VIN2')])


class TestDataContracts(unittest.TestCase):
    def test_elapsed_seconds_zero_none_and_remaining_minutes(self):
        backend = india.IndiaBackend('0000000000', 'synthetic', vin='VIN1')
        fake = FakeIndiaClient()
        for value in (90, 0, None):
            with self.subTest(elapsed=value):
                fake.charge.charge_time_elapsed_s = value
                backend._charge_status_by_vin['VIN1'] = fake.charge
                frame = asyncio.run(backend.get_charging_info('VIN1'))
                self.assertEqual(frame.rvsChargeStatus.chargingDuration, value)
                self.assertEqual(frame.chrgMgmtData.chrgngRmnngTime, 186)
                self.assertEqual(frame.statusTime, 1_799_999_900)
                if value == 90:
                    self.assertEqual(frame.rvsChargeStatus.chargingDuration / 60, 1.5)
        mapped = backend._map_status(NS(status_time=None, raw={}, fuel_level=None))
        self.assertIsNone(mapped.statusTime)
        self.assertIsNone(mapped.basicVehicleStatus.remoteClimateStatus)

    def test_subtype_source_preference_sensor_computation_and_abrp(self):
        c = C.__new__(C)
        c.client = NS(region_name='India')
        c.vehicle_type = 'BEV'
        c.data = {'status': NS(statusTime=300)}
        c._last_status_time = 1000  # High-water must not replace supplied frame time.
        charge = NS(statusTime=200, chrgMgmtData=NS(bmsPackSOCDsp=625),
                    rvsChargeStatus=NS(fuelRangeElec=852))
        status = NS(statusTime=250, basicVehicleStatus=NS(extendedData1=70, fuelRangeElec=900))
        self.assertTrue(c.prefer_basic_measurements(charge, status))
        self.assertFalse(c.prefer_basic_measurements(charge, NS(statusTime=150)))
        self.assertFalse(c.prefer_basic_measurements(NS(statusTime=None), status))
        abrp = ABRP.AbrpApi(object(), 'synthetic-api', 'synthetic-user')
        sensor_harness = IndiaBEVStateOfChargeTests()
        for soc, range_raw, expected_soc, expected_range in (
            (70, 900, 70.0, 90.0), (None, 900, 62.5, 90.0),
            (101, -128, 62.5, 85.2), (0, None, 0.0, 85.2),
        ):
            with self.subTest(soc=soc, range=range_raw):
                status.basicVehicleStatus = NS(extendedData1=soc, fuelRangeElec=range_raw)
                preferred = c.prefer_basic_measurements(charge, status)
                computed = c._extract_soc_pct(status.basicVehicleStatus, charge, prefer_basic=preferred)
                self.assertEqual(computed, expected_soc)
                self.assertEqual(COORD.electric_range_km(status.basicVehicleStatus, charge, factor=0.1, prefer_basic=preferred), expected_range)
                payload = abrp._build_payload(status, charge, prefer_basic=preferred, allow_basic_soc=True)
                self.assertEqual(payload['soc'], expected_soc)
                self.assertEqual(payload['est_battery_range'], expected_range)
                entities = sensor_harness._setup_entities(india.IndiaBackend('0000000000', 'synthetic', vin='VIN1'), 'BEV', status, charge)
                sensor = next(e for e in entities if isinstance(e, SENSOR.SAICMGSOCSensor))
                sensor.coordinator.prefer_basic_measurements = lambda _charge: preferred
                self.assertEqual(sensor.native_value, expected_soc)
        self.assertIsNone(c._extract_soc_pct(NS(extendedData1=80), None, allow_basic_soc=False))
        self.assertEqual(c._extract_soc_pct(NS(extendedData1=80), charge, allow_basic_soc=False), 62.5)
        self.assertIsNone(c._extract_fuel_pct(NS(fuelLevelPrc=80), allow_fuel=False))
        self.assertEqual(c._extract_fuel_pct(NS(fuelLevelPrc=0)), 0.0)
        global_payload = abrp._build_payload(NS(statusTime=1, basicVehicleStatus=NS(extendedData1=90, fuelRangeElec=1000)), charge)
        self.assertEqual(global_payload['soc'], 62.5)
        self.assertEqual(global_payload['est_battery_range'], 100.0)
        payload = abrp._build_payload(status, NS(chrgMgmtData=NS(bmsPackSOCDsp=1001)), prefer_basic=False, allow_basic_soc=False)
        self.assertNotIn('soc', payload)

    def test_india_charge_zero_replaces_retained_nonzero(self):
        entities = IndiaBEVStateOfChargeTests()._setup_entities(
            india.IndiaBackend('0000000000', 'synthetic', vin='VIN1'), 'BEV',
            NS(basicVehicleStatus=NS(extendedData1=80)),
            NS(chrgMgmtData=NS(bmsPackSOCDsp=625)),
        )
        sensor = next(e for e in entities if isinstance(e, SENSOR.SAICMGSOCSensor))
        self.assertEqual(sensor.native_value, 62.5)
        sensor.coordinator.data['charging'].chrgMgmtData.bmsPackSOCDsp = 0
        self.assertEqual(sensor.native_value, 0.0)
        sensor.coordinator.data['charging'] = None
        sensor.coordinator.data['status'] = None
        self.assertEqual(sensor.native_value, 0.0)

    def test_independent_charge_source_age_progression_and_missing_time(self):
        now = datetime.now(timezone.utc)
        epoch = int(now.timestamp())
        charge = NS(statusTime=epoch-600, chrgMgmtData=NS())
        c = freshness.UpdateCycleRecordingTests()._coord(
            status=lambda: NS(statusTime=epoch),
            charging=lambda: charge,
        )
        c.client.region_name = 'India'
        c._is_status_timestamp_valid = C._is_status_timestamp_valid.__get__(c)
        asyncio.run(c._run_update_cycle())
        attrs = c.charging_freshness.attributes(now)
        self.assertEqual(attrs['data_age_minutes'], 10)
        self.assertEqual(c.charging_freshness.state, 'live')
        old = c.charging_freshness.last_success
        self.assertEqual(old.timestamp(), epoch - 600)
        asyncio.run(c._run_update_cycle())
        self.assertEqual(c.charging_freshness.last_success, old)
        self.assertEqual(c.charging_freshness.state, 'stale')
        charge.statusTime = epoch - 300
        asyncio.run(c._run_update_cycle())
        self.assertEqual(c.charging_freshness.last_success.timestamp(), epoch - 300)
        self.assertEqual(c.charging_freshness.state, 'live')
        snapshot = c._charge_snapshot(
            NS(mileage=10000), NS(statusTime=epoch-300,
                                 chrgMgmtData=NS(bmsPackSOCDsp=625),
                                 rvsChargeStatus=NS(packEnergyKwh=31.75)),
        )
        self.assertEqual(datetime.fromisoformat(snapshot.ts).timestamp(), epoch - 300)
        self.assertEqual(snapshot.soc_pct, 62.5)
        self.assertEqual(snapshot.pack_energy_kwh, 31.75)
        c2 = freshness.UpdateCycleRecordingTests()._coord(
            status=lambda: NS(statusTime=epoch), charging=lambda: NS(statusTime=None, chrgMgmtData=NS()))
        c2.client.region_name = 'India'
        c2._is_status_timestamp_valid = C._is_status_timestamp_valid.__get__(c2)
        asyncio.run(c2._run_update_cycle())
        self.assertIsNone(c2.charging_freshness.last_success)
        self.assertEqual(c2.charging_freshness.state, 'no_data')

    def test_india_quiet_charging_unsupported_global_cache_unchanged(self):
        c = C.__new__(C)
        c.client = india.IndiaBackend('0000000000', 'synthetic', vin='VIN1')
        c.quiet_hours, c.live_polling = True, False
        self.assertFalse(callable(getattr(c.client, 'get_cached_vehicle_status', None)))
        self.assertFalse(c.quiet_active)
        c.client = NS(get_cached_vehicle_status=AsyncMock())
        self.assertTrue(callable(c.client.get_cached_vehicle_status))
        self.assertTrue(c.quiet_active)


class TestPublicClimate(_Base):
    def test_india_boolean_controls_and_safe_failed_unsupported_commands(self):
        async def run():
            fixture = self._entity()
            fixture._client.region_name = 'India'
            entity = CLIMATE.SAICMGClimateEntity(
                fixture.coordinator, fixture._client, NS(entry_id='synthetic'),
                fixture._vin_info, 'VIN1',
            )
            self.assertEqual(entity._attr_hvac_modes, ['off', 'heat_cool'])
            await entity.async_set_hvac_mode(CLIMATE.HVACMode.HEAT_COOL)
            entity._client.start_ac.assert_awaited_once_with('VIN1')
            await entity.async_set_hvac_mode(CLIMATE.HVACMode.OFF)
            entity._client.stop_ac.assert_awaited_once_with('VIN1')
            entity._client.start_climate.assert_not_awaited()
            for call in (
                lambda: entity.async_set_hvac_mode(CLIMATE.HVACMode.FAN_ONLY),
                lambda: entity.async_set_temperature(temperature=25),
                lambda: entity.async_set_fan_mode('High'),
            ):
                with self.assertRaises(CLIMATE.HomeAssistantError):
                    await call()
            entity._client.start_ac.side_effect = RuntimeError('synthetic-private-token')
            with self.assertRaises(CLIMATE.HomeAssistantError) as caught:
                await entity.async_set_hvac_mode(CLIMATE.HVACMode.HEAT_COOL)
            self.assertNotIn('synthetic-private-token', str(caught.exception))
            self.assertIsNone(caught.exception.__cause__)
            self.assertTrue(caught.exception.__suppress_context__)
            entity.coordinator.record_command_error.assert_called()
            for error, notification in (
                (CLIMATE.CommandsLimitReachedException(), 'notify_command_limit_reached'),
                (CLIMATE.VehicleNotLockedException(), 'notify_vehicle_not_locked'),
            ):
                entity._client.start_ac.side_effect = error
                with self.assertRaises(CLIMATE.HomeAssistantError):
                    await entity.async_set_hvac_mode(CLIMATE.HVACMode.HEAT_COOL)
                getattr(entity.coordinator, notification).assert_awaited_once()
        asyncio.run(run())
