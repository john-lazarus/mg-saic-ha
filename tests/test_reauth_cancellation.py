"""Focused account recovery tracer; run separately from other HA stub modules."""
import asyncio
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

import test_setup_and_config_flow as harness

CF = harness.CF
SETUP = harness.SETUP
# The maintained harness loads __init__ under an alias; relative production
# imports must still resolve the real helper on the package.
sys.modules['mg_saic']._account_key = SETUP._account_key


class TestReauthRecovery(unittest.IsolatedAsyncioTestCase):
    async def trace(self, *, committed, cancel_at=None, ordinary_at=None,
                    original_cancel=False):
        entries = [SimpleNamespace(entry_id=key, data={
            'username': 'synthetic@example.invalid', 'region': 'India',
            'password': 'old-synthetic', 'vin': 'SYNTHETIC-' + key,
        }) for key in ('A', 'B', 'C')]
        other = SimpleNamespace(entry_id='OTHER', data={
            'username': 'other@example.invalid', 'region': 'India',
            'password': 'other-synthetic', 'vin': 'SYNTHETIC-OTHER',
        })
        account = SETUP._account_key(entries[0])
        shared = SimpleNamespace(close=AsyncMock())
        domain = {key: shared for key in ('A', 'B', 'C')}
        domain['account_clients'] = {account: shared}
        hass = SimpleNamespace(data={CF.DOMAIN: domain})
        attempts, updates, unloads = [], [], []
        entered, release = asyncio.Event(), asyncio.Event()
        initial = asyncio.CancelledError('original-synthetic-cancel')
        ordinary = RuntimeError('ordinary-synthetic-reload-failure')

        async def unload(key):
            unloads.append(key)
            if key == 'C' and not committed:
                if original_cancel:
                    raise initial
                return False
            domain.pop(key)
            return True

        def update(entry, *, data):
            self.assertFalse(domain['account_clients'])
            updates.append(entry.entry_id)
            entry.data = data

        async def reload(key):
            self.assertFalse(domain['setups_in_progress'])
            # All credentials have changed together before the first await.
            self.assertEqual([e.data['password'] for e in entries],
                             ['new-synthetic' if committed else 'old-synthetic'] * 3)
            attempts.append(key)
            if key == cancel_at:
                entered.set()
                await release.wait()
            if key == ordinary_at:
                raise ordinary
            return True

        hass.config_entries = SimpleNamespace(
            async_entries=lambda _domain: entries + [other],
            async_unload=unload, async_update_entry=update, async_reload=reload,
        )
        flow = CF.SAICMGConfigFlow()
        flow.hass, flow._existing_entry = hass, entries[0]
        flow.region, flow.username = 'India', entries[0].data['username']
        flow.fetch_vehicle_data_india = AsyncMock()
        task = asyncio.create_task(flow.async_step_reauth_confirm(
            {'password': ' new-synthetic '}))
        if cancel_at:
            await entered.wait()
            task.cancel('recovery-synthetic-cancel')
            # Allow the outer task to observe cancellation while reload awaits.
            await asyncio.sleep(0)
            release.set()
        if cancel_at or original_cancel:
            with self.assertRaises(asyncio.CancelledError) as caught:
                await task
            if original_cancel:
                self.assertIs(caught.exception, initial)
        elif committed and ordinary_at:
            with self.assertRaises(RuntimeError) as caught:
                await task
            self.assertIs(caught.exception, ordinary)
        else:
            result = await task
            self.assertEqual(result, {'type': 'abort', 'reason': 'reauth_successful'}
                             if committed else {
                                 'type': 'form', 'step_id': 'reauth_confirm',
                                 'data_schema': {'password': CF.PASSWORD_SELECTOR},
                                 'errors': {'base': 'auth'},
                             })
        self.assertEqual(attempts, ['A', 'B', 'C'] if committed else ['A', 'B'])
        self.assertEqual(unloads, ['A', 'B', 'C'])
        self.assertEqual(updates, ['A', 'B', 'C'] if committed else [])
        self.assertFalse(domain['setups_in_progress'])
        self.assertEqual(shared.close.await_count, int(committed))
        self.assertEqual(other.data['password'], 'other-synthetic')
        if not committed:
            self.assertIs(domain['account_clients'][account], shared)
        return attempts

    async def test_unload_refusal_then_recovery_cancellation(self):
        await self.trace(committed=False, cancel_at='A')

    async def test_committed_failure_then_later_cancellation(self):
        await self.trace(committed=True, ordinary_at='A', cancel_at='B')

    async def test_ordinary_reload_failure_still_propagates(self):
        await self.trace(committed=True, ordinary_at='A')

    async def test_original_cancellation_survives_recovery_failures(self):
        await self.trace(committed=False, original_cancel=True,
                         ordinary_at='A', cancel_at='B')

    async def test_success_updates_every_sibling_once(self):
        await self.trace(committed=True)

    async def test_uncommitted_ordinary_failure_keeps_auth_form(self):
        await self.trace(committed=False, ordinary_at='A')
