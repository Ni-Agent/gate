#!/usr/bin/env python3
"""Supplemental wire-independent contracts; native_acceptance.py is primary.

Real PluginContext/registry, mock transport client. These are not server, native
admission or provider acceptance and do not preserve retired task policy helpers.
"""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

REPO = Path(__file__).resolve().parents[1]
SOURCE = Path(sys.executable).parents[2] / 'workspace'
sys.path[:0] = [str(REPO), str(SOURCE)]
for key in list(os.environ):
    if key.endswith(('_TOKEN', '_API_KEY', '_SECRET', '_PASSWORD', '_CREDENTIALS')) or key.startswith(('HERMES_SESSION_', 'HERMES_GATEWAY_', 'GATEWAY_', 'OPENAI_', 'ANTHROPIC_', 'OPENROUTER_')):
        os.environ.pop(key, None)
HOME = tempfile.TemporaryDirectory(prefix='gate-native-units-', dir=os.environ['TMPDIR'])
os.environ.update(HOME=HOME.name, HERMES_HOME=HOME.name)
(Path(HOME.name) / 'config.yaml').write_text('display:\n  busy_input_mode: queue\n')
from client.gate_client import GateError
from gateway.config import PlatformConfig
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from native_gate.adapter import GateAdapter
from native_gate.policy import require_queue_policy
from native_gate.routes import task_route
from native_gate.tools import register_tools
from tools.registry import registry


class NativeContracts(unittest.TestCase):
    def setUp(self):
        self.manager = PluginManager(scope_key=HOME.name)
        self.ctx = PluginContext(PluginManifest(name='generic-contract', key='generic-contract', source='user'), self.manager)
        self.client = Mock()
        self.client.base_url = 'https://fixture.invalid'
        self.client.token = 'synthetic-client-token'
        self.ctx.register_platform(name='gate', label='Gate unit fixture',
            adapter_factory=lambda config: None, check_fn=lambda: True, trusted_inbound=True)
        register_tools(self.ctx, lambda: self.client)
        self.entry = registry.get_entry('gate_call', scope=HOME.name)
        self.assertIsNotNone(self.entry)

    def tearDown(self):
        self.manager.unload()

    def call(self, operation, **arguments):
        return json.loads(self.entry.handler({'operation': operation, 'arguments': arguments}))

    def test_unbound_generic_write_returns_committed_result_without_readback(self):
        from gateway.session_context import bound_session_key
        self.assertFalse(bound_session_key())
        committed = {'id': 'gate:tk:other', 'token': 'committed-token', 'status': 'done', 'modified_by': 'hermes:default'}
        self.client.rpc.return_value = committed
        result = self.call('update_task', id='gate:tk:other', expected_token='old', status='done')
        self.assertEqual(result, {'status': 200, 'data': committed})
        self.client.rpc.assert_called_once_with('update_task', id='gate:tk:other', expected_token='old', status='done')
        self.client.authenticate.assert_not_called()

    def test_server_error_status_payload_preserved_without_retry(self):
        for status in (400, 401, 412, 428, 0):
            with self.subTest(status=status):
                self.client.reset_mock()
                payload = {'error': 'synthetic-server-error', 'opaque': ['unchanged']}
                self.client.rpc.side_effect = GateError(status, payload)
                self.assertEqual(self.call('update_task', id='malformed source token'), {'status': status, 'data': payload})
                self.client.rpc.assert_called_once_with('update_task', id='malformed source token')

    def test_error_payload_scrubs_exact_bearer_recursively_preserving_json_types(self):
        for token in ('synthetic-client-token', 'quote"slash\\token', '200'):
            for status in (200, 401, 0):
                with self.subTest(token=token, status=status):
                    self.client.token = token
                    payload = {'error': 'Authorization: Bearer ' + token,
                        'nested': [{token: ['prefix-' + token + '-suffix', {'echo': token}]}],
                        'opaque': [200, 200.5, True, False, None, '雪', {'status': 200}]}
                    expected = {'error': 'Authorization: Bearer <redacted>',
                        'nested': [{'<redacted>': ['prefix-<redacted>-suffix', {'echo': '<redacted>'}]}],
                        'opaque': [200, 200.5, True, False, None, '雪', {'status': 200}]}
                    original = json.dumps(payload)
                    self.client.reset_mock()
                    self.client.rpc.side_effect = GateError(status, payload)
                    self.assertEqual(self.call('read_resource', id='gate:tk:any'), {'status': status, 'data': expected})
                    self.assertEqual(json.dumps(payload), original, 'server payload mutated in place')
                    self.client.rpc.assert_called_once_with('read_resource', id='gate:tk:any')

    def test_adapter_repr_before_connect(self):
        adapter = GateAdapter(PlatformConfig(enabled=True), lambda: self.client, Mock())
        self.assertIn('identity=None', repr(adapter))

    def test_public_reconnect_pins_identity_preserves_seen_and_starts_no_listener_on_mismatch(self):
        async def check():
            identity = 'hermes:default'
            authenticated_as = identity
            def authenticate(expected=None):
                if expected is not None and expected != authenticated_as:
                    raise GateError(403, {'error': 'identity mismatch'})
                return authenticated_as
            self.client.authenticate.side_effect = authenticate
            spawn = Mock(side_effect=lambda coro, **kw: asyncio.create_task(coro, **kw))
            adapter = GateAdapter(PlatformConfig(enabled=True), lambda: self.client, spawn)
            async def listen():
                await asyncio.Event().wait()
            with patch.object(adapter, '_listen', side_effect=listen):
                try:
                    await adapter.connect()
                    adapter.seen.add('gate:tk:held')
                    await adapter.disconnect()
                    authenticated_as = 'hermes:prometheus'
                    with self.assertRaises(GateError):
                        await adapter.connect(is_reconnect=True)
                    self.assertEqual(adapter.identity, identity)
                    self.assertEqual(adapter.seen, {'gate:tk:held'})
                    self.assertEqual(spawn.call_count, 1)
                    self.assertIsNone(adapter._transport_task)
                    self.assertEqual(self.client.authenticate.call_args.args, (identity,))
                finally:
                    await adapter.disconnect()
        asyncio.run(check())

    def test_terminal_task_offline_reassignment_stops_original_lane_on_snapshot(self):
        async def check(status):
            adapter = GateAdapter(PlatformConfig(enabled=True), lambda: self.client, Mock())
            adapter.identity = 'hermes:default'
            events = []
            async def handle(event):
                events.append(event)
            adapter.handle_message = handle
            task = {'id': 'gate:tk:held', 'assignee': adapter.identity, 'status': 'ready',
                'execution_revision': 1, 'execution_updated_at': '2026-11-24T00:00:00Z'}
            await adapter._dispatch(task)
            original_route = events[0].source.chat_id
            task = dict(task, status=status)
            await adapter._dispatch(task)
            self.assertEqual(len(events), 1, 'terminal state must not start another turn')
            reassigned = dict(task, assignee='hermes:prometheus', execution_revision=2)
            self.client.rpc.side_effect = lambda op, **kw: {'items': [], 'has_more': False} if op == 'list_tasks' else reassigned
            await adapter.reconcile_assigned()
            self.assertEqual(len(events), 2, 'missing stop for terminal task reassigned offline')
            self.assertEqual(events[-1].source.chat_id, original_route)
            self.assertEqual(events[-1].get_command(), 'stop')
            self.assertTrue(events[-1].allow_gateway_control)
            self.assertNotIn(task['id'], adapter.seen)
        for status in ('done', 'blocked'):
            with self.subTest(status=status):
                asyncio.run(check(status))

    def test_generic_read_does_not_auto_fetch_guidance(self):
        task = {'id': 'gate:tk:any', 'body': 'optional gate:tx:guidance', 'verification': ['gate:tx:guidance']}
        self.client.rpc.return_value = task
        self.assertEqual(self.call('read_resource', id=task['id'])['data'], task)
        self.client.rpc.assert_called_once_with('read_resource', id=task['id'])

    def test_binary_calls_forward_streaming_file_arguments(self):
        self.client.upload_binary.return_value = {'id': 'gate:bn:file', 'token': 'hash'}
        self.client.download_binary_file.return_value = {'bytes': 12}
        self.assertEqual(self.call('upload_binary', resource_id='gate:bn:file', source_path='/fixture/source', mime='application/octet-stream')['status'], 200)
        self.client.upload_binary.assert_called_once_with(resource_id='gate:bn:file', source_path='/fixture/source', mime='application/octet-stream')
        self.assertEqual(self.call('download_binary', resource_id='gate:bn:file', path='/fixture/target', revision=2)['data'], {'bytes': 12})
        self.client.download_binary_file.assert_called_once_with(resource_id='gate:bn:file', path='/fixture/target', revision=2)
        self.client.rpc.assert_not_called()

    def test_queue_is_explicit_readonly_prerequisite(self):
        for mode, legacy, env, allowed in [('interrupt', '', {}, False), ('steer', '', {}, False), ('queue', '', {}, True), ('queue', 'interrupt', {}, False), ('queue', '', {'HERMES_GATEWAY_BUSY_INPUT_MODE': 'steer'}, False)]:
            with self.subTest(mode=mode, legacy=legacy, env=env), patch('hermes_cli.config.load_config_readonly', return_value={'display': {'busy_input_mode': mode, 'busy_text_mode': legacy}}), patch('gateway.platforms._shared.get_scoped_secret', side_effect=lambda key: env.get(key)):
                if allowed:
                    require_queue_policy()
                else:
                    with self.assertRaisesRegex(ValueError, 'requires host'):
                        require_queue_policy()

    def test_nonqueue_connect_refuses_before_authentication_and_listener(self):
        async def check():
            spawn = Mock()
            adapter = GateAdapter(PlatformConfig(enabled=True), lambda: self.client, spawn)
            with patch('hermes_cli.config.load_config_readonly', return_value={'display': {'busy_input_mode': 'interrupt'}}):
                with self.assertRaisesRegex(ValueError, 'requires host'):
                    await adapter.connect()
            self.client.authenticate.assert_not_called()
            spawn.assert_not_called()
        asyncio.run(check())

    def test_native_lane_scope_separates_origin_exact_identity_and_task(self):
        routes = {task_route(origin, identity, task) for origin in ('https://one.invalid', 'https://two.invalid') for identity in ('hermes:default', 'hermes:prometheus') for task in ('gate:tk:one', 'gate:tk:two')}
        self.assertEqual(len(routes), 8)
        self.assertTrue(all(route.startswith('gate-v2-') for route in routes))


if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        HOME.cleanup()
