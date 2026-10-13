"""Actual global validation helper; run with the isolated config-flow loader."""
import asyncio
from dataclasses import dataclass

import pytest

import test_setup_and_config_flow as harness


@dataclass
class Configuration:
    username: str
    password: str
    base_uri: str
    region: str
    tenant_id: str
    phone_country_code: str | None
    username_is_email: bool


@dataclass
class Vehicle:
    vin: str


@dataclass
class VehicleList:
    vinList: list[Vehicle]


@pytest.mark.parametrize(('outcome', 'cleanup'), [
    ('success', 'complete'),
    ('login_failure', 'complete'),
    ('cancellation', 'complete'),
    ('no_vehicles', 'complete'),
    ('success', 'failure'),
    ('login_failure', 'failure'),
    ('cancellation', 'failure'),
    ('cancellation', 'cancelled'),
])
def test_temporary_global_client_lifecycle(monkeypatch, outcome, cleanup):
    async def trace():
        login_started, close_started, release = (asyncio.Event() for _ in range(3))
        failure = RuntimeError('synthetic login failure')
        original_cancel = None
        close_count, transport_closed, listed = 0, False, False

        class Api:
            def __init__(self, configuration: Configuration):
                assert isinstance(configuration, Configuration)
                assert configuration.username_is_email is True

            async def login(self) -> None:
                nonlocal original_cancel
                login_started.set()
                if outcome == 'login_failure':
                    raise failure
                if outcome == 'cancellation':
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError as error:
                        original_cancel = error
                        raise

            async def vehicle_list(self) -> VehicleList:
                nonlocal listed
                listed = True
                return VehicleList([] if outcome == 'no_vehicles' else [Vehicle('SYNTHETIC-VIN')])

            async def close(self) -> None:
                nonlocal close_count, transport_closed
                close_count += 1
                close_started.set()
                await release.wait()
                transport_closed = True
                if cleanup == 'failure':
                    raise RuntimeError('synthetic cleanup failure')
                if cleanup == 'cancelled':
                    raise asyncio.CancelledError('synthetic cleanup cancellation')

        class Hass:
            async def async_add_executor_job(self, constructor, configuration: Configuration) -> Api:
                return constructor(configuration)

        monkeypatch.setattr(harness.CF, 'SaicApi', Api)
        monkeypatch.setattr(harness.CF, 'SaicApiConfiguration', Configuration)
        flow = harness.CF.SAICMGConfigFlow()
        flow.hass = Hass()
        flow.region = next(iter(harness.CF.REGION_BASE_URIS))
        flow.username, flow.password = 'synthetic@example.invalid', 'synthetic-password'
        task = asyncio.create_task(flow.fetch_vehicle_data(True))
        closing = asyncio.create_task(close_started.wait())
        try:
            await login_started.wait()
            if outcome == 'cancellation':
                task.cancel('original validation cancellation')
            await asyncio.wait({task, closing}, timeout=1, return_when=asyncio.FIRST_COMPLETED)
            assert close_started.is_set(), 'temporary client was never closed'
            assert not task.done(), 'helper returned before transport cleanup completed'
            if outcome == 'cancellation':
                for _ in range(2):
                    task.cancel('later cleanup cancellation')
                    await asyncio.sleep(0)
                    assert not task.done(), 'repeated cancellation interrupted cleanup'
            release.set()
            if outcome == 'cancellation':
                with pytest.raises(asyncio.CancelledError) as caught:
                    await task
                assert caught.value is original_cancel
            elif outcome == 'login_failure':
                with pytest.raises(RuntimeError) as caught:
                    await task
                assert caught.value is failure
            elif outcome == 'no_vehicles':
                with pytest.raises(harness.CF.NoVehiclesFoundError, match='Vehicle list API returned no vehicles'):
                    await task
            else:
                assert await task is None
            assert close_count == 1 and transport_closed
            assert listed is (outcome in ('success', 'no_vehicles'))
            assert flow.vehicles == (['SYNTHETIC-VIN'] if outcome == 'success' else [])
        finally:
            release.set()
            closing.cancel()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, closing, return_exceptions=True)

    asyncio.run(trace())
