#!/usr/bin/env python3
"""Isolated actual-native crash/unknown/operator-resolution proof.

Uses installed Hermes, real AIAgent/tool dispatch and native SQLite, but a fixture
HTTPX provider and fsynced private file effect (NOT the Gate server). No model
network, active profile, external Gate write, or dependency installation. The only
listener is this run's disposable, same-user native Unix control socket. Operator
RPC is sent through the actual native ASGI WebSocket gate, in memory.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
from types import SimpleNamespace
from unittest.mock import patch

SOURCE = Path(sys.executable).parents[2] / 'workspace'
REPO = Path(__file__).resolve().parents[1]
SCRATCH = Path(os.environ['TMPDIR'])
EVIDENCE = SCRATCH / 'gate-native-unknown-evidence'
REAL_CONNECT = socket.socket.connect
REAL_BIND = socket.socket.bind
NETWORK = []
SUPPRESSED = []
SUPPRESSED_BINDS = []
REQUESTS = []
TOOL_CALLS = []
POST_EFFECT = threading.Event()
HOME: Path
RUN: Path
PHASE = None


def save(name, value):
    path = RUN / name
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n')
    temporary.replace(path)


def effect():
    raw = (HOME / 'effect.jsonl').read_bytes()
    return {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
            'records': [json.loads(line) for line in raw.splitlines()]}


def configure_child():
    # Do this before importing any installed Hermes module. No credential file is copied.
    for name in list(os.environ):
        if (name.endswith(('_TOKEN', '_API_KEY', '_SECRET', '_PASSWORD', '_CREDENTIALS'))
                or name.startswith(('HERMES_SESSION_', 'GATEWAY_', 'OPENAI_', 'ANTHROPIC_', 'OPENROUTER_'))):
            os.environ.pop(name, None)
    os.environ.update(HERMES_HOME=str(HOME), HOME=str(HOME), XDG_CACHE_HOME=str(HOME / '.cache'),
                      TMPDIR=str(HOME), OPENAI_API_KEY='disposable-fixture-not-a-secret',
                      OPENAI_BASE_URL='https://native-unknown-fixture.invalid/v1',
                      PYTHONDONTWRITEBYTECODE='1')
    sys.path.insert(0, str(SOURCE))


def guarded_connect(sock, address):
    if sock.family == socket.AF_UNIX and str(address) == str(HOME / 'gateway.sock'):
        return REAL_CONNECT(sock, address)
    NETWORK.append(repr(address))
    raise AssertionError('Network outside disposable native control socket forbidden')


def guarded_bind(sock, address):
    if sock.family == socket.AF_UNIX and str(address) == str(HOME / 'gateway.sock'):
        return REAL_BIND(sock, address)
    # urllib3 probes IPv6 support at import; no socket is actually bound.
    if sock.family == socket.AF_INET6 and address == ('::1', 0):
        SUPPRESSED_BINDS.append(address)
        raise OSError('IPv6 capability bind suppressed by isolated proof')
    NETWORK.append('bind ' + repr(address))
    raise AssertionError('Only the disposable native Unix control listener is allowed')


def verify_isolation():
    assert not NETWORK, NETWORK
    assert all(isinstance(argv, list) and (argv[:2] == ['uname', '-p'] or argv[0] == 'python3'
               or any('hermes_constants_scratch_detach' in str(a) for a in argv))
               for argv in SUPPRESSED), SUPPRESSED


def suppressed_subprocess(*args, **kwargs):
    SUPPRESSED.append(args[0] if args else kwargs.get('args'))
    raise RuntimeError('Core metadata/pruner subprocess suppressed by proof')


def write_effect(args, **kwargs):
    from gateway.session_context import bound_session_key
    assert PHASE == 'crash', 'Continuation must not reissue the effect'
    record = {'operation_id': 'known-effect-one', 'session_key': bound_session_key()}
    # Deliberately NOT idempotent: another execution would append a second record.
    with (HOME / 'effect.jsonl').open('ab', buffering=0) as stream:
        stream.write((json.dumps(record, sort_keys=True) + '\n').encode())
        os.fsync(stream.fileno())
    TOOL_CALLS.append({'tool': 'native_probe_persist', **record})
    return json.dumps({'persisted': True, 'operation_id': record['operation_id']})


def inspect_effect(args, **kwargs):
    assert PHASE == 'restart'
    observed = effect()
    assert len(observed['records']) == 1
    TOOL_CALLS.append({'tool': 'native_probe_inspect', 'effect': observed})
    return json.dumps(observed)


def fixture_http(self, request, **kwargs):
    import httpx
    assert request.url.host == 'native-unknown-fixture.invalid', str(request.url)
    if request.method != 'POST' or not request.url.path.endswith('/chat/completions'):
        return httpx.Response(404, json={'error': 'no fixture catalog'}, request=request)
    body = json.loads(request.content)
    messages = body['messages']
    latest = next(m for m in reversed(messages) if m['role'] == 'user')['content']
    if isinstance(latest, list):
        latest = '\n'.join(p.get('text', '') for p in latest)
    assert 'NATIVE_UNKNOWN_PROBE' in latest, latest
    REQUESTS.append({'phase': PHASE, 'last_role': messages[-1]['role'], 'user_text': latest,
                     'tool_names': [t['function']['name'] for t in body.get('tools', [])]})
    if PHASE == 'crash' and messages[-1]['role'] == 'tool':
        # Actual AIAgent has consumed the real tool result; it cannot settle this turn.
        POST_EFFECT.set()
        threading.Event().wait(120)
        raise AssertionError('Owner was not killed at the post-effect barrier')
    if messages[-1]['role'] != 'tool':
        name = 'native_probe_persist' if PHASE == 'crash' else 'native_probe_inspect'
        assert name in REQUESTS[-1]['tool_names']
        message = {'role': 'assistant', 'content': None, 'tool_calls': [{
            'id': 'call-' + PHASE, 'type': 'function',
            'function': {'name': name, 'arguments': '{}'}}]}
        finish = 'tool_calls'
    else:
        message = {'role': 'assistant', 'content': 'Inspected the existing effect; no mutation repeated.'}
        finish = 'stop'
    completion = {'id': 'fixture-' + PHASE, 'object': 'chat.completion', 'created': 1,
                  'model': 'native-unknown-fixture', 'choices': [{'index': 0, 'message': message, 'finish_reason': finish}],
                  'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}}
    if body.get('stream'):
        delta = dict(message)
        if 'tool_calls' in delta:
            delta['tool_calls'] = [dict(t, index=i) for i, t in enumerate(delta['tool_calls'])]
        chunk = {'id': completion['id'], 'object': 'chat.completion.chunk', 'created': 1,
                 'model': completion['model'], 'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}
        tail = dict(chunk, choices=[{'index': 0, 'delta': {}, 'finish_reason': finish}], usage=completion['usage'])
        text = 'data: ' + json.dumps(chunk) + '\n\ndata: ' + json.dumps(tail) + '\n\ndata: [DONE]\n\n'
        return httpx.Response(200, content=text.encode(), headers={'content-type': 'text/event-stream'}, request=request)
    return httpx.Response(200, json=completion, request=request)


async def until(check, seconds=40):
    async with asyncio.timeout(seconds):
        while not check():
            await asyncio.sleep(.02)


async def native_operator(runner, authority, row):
    """Acquire a ticket over actual peer-authenticated Unix socket; never forge an actor."""
    from gateway.control_socket import GatewayControlServer
    from gateway.runtime_bootstrap import TicketStore
    from gateway.run_api import GatewayRuntimeAPI
    from gateway.session_contract import CANONICAL_GATEWAY_PROTOCOL
    from hermes_cli.gateway_client import _session_ticket, private_control_query

    runner.session_ticket_store = TicketStore(authority.instance_id, frozenset({authority.profile_id}))
    runner.session_runtime_descriptor = {'state': 'ready'}
    control = GatewayControlServer(home=HOME)
    control.ticket_store = runner.session_ticket_store
    assert await control.start(), 'Disposable native control socket did not start'
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    task = None

    async def fallback(scope, receive, send):
        await send({'type': 'websocket.close', 'code': 4403})

    async def rpc(method, params, rid):
        await incoming.put({'type': 'websocket.receive', 'text': json.dumps({
            'jsonrpc': '2.0', 'id': rid, 'method': method, 'params': params})})
        async with asyncio.timeout(20):
            while True:
                frame = await outgoing.get()
                assert frame['type'] != 'websocket.close', frame
                if frame['type'] == 'websocket.send':
                    reply = json.loads(frame['text'])
                    if reply.get('id') == rid:
                        return reply

    try:
        # Prove request data alone is not bootstrap authorization.
        raw = json.dumps({'protocol': 1, 'id': 1, 'verb': 'session-ticket', 'params': {
            'profile_id': authority.profile_id, 'instance_id': 'wrong-instance', 'purpose': 'interactive'}}).encode() + b'\n'
        denied = json.loads(await asyncio.to_thread(private_control_query, HOME, raw))
        assert denied['ok'] is False and 'stale runtime instance' in denied['error'], denied
        # Same supported client acquisition used by connect_gateway; the server derives UID
        # from SO_PEERCRED and mints a short-lived one-use ticket. No subject override.
        endpoint = SimpleNamespace(profile_id=authority.profile_id, instance_id=authority.instance_id, control_home=str(HOME))
        ticket = await asyncio.to_thread(_session_ticket, HOME, endpoint)
        web_app = SimpleNamespace(state=SimpleNamespace(bound_host='127.0.0.1', bound_port=47619, auth_required=False))
        app = GatewayRuntimeAPI(fallback, runner, web_app)
        scope = {'type': 'websocket', 'asgi': {'version': '3.0'}, 'scheme': 'ws',
                 'path': '/api/ws', 'raw_path': b'/api/ws', 'query_string': b'',
                 'root_path': '', 'client': ('127.0.0.1', 43210), 'server': ('127.0.0.1', 47619),
                 'headers': [(b'host', b'127.0.0.1:47619'), (b'sec-websocket-protocol',
                     (CANONICAL_GATEWAY_PROTOCOL + ',hermes-gateway-ticket.' + ticket).encode())],
                 'subprotocols': [CANONICAL_GATEWAY_PROTOCOL, 'hermes-gateway-ticket.' + ticket]}
        await incoming.put({'type': 'websocket.connect'})
        task = asyncio.create_task(app(scope, incoming.get, outgoing.put))
        async with asyncio.timeout(20):
            accepted = await outgoing.get()
        assert accepted['type'] == 'websocket.accept', accepted
        params = {'session_id': row['session_id'], 'admission_id': row['admission_id'],
                  'execution_generation': row['generation']}
        stale = await rpc('prompt.resolve_unknown', dict(params, execution_generation=row['generation'] + 1), 11)
        assert stale.get('error', {}).get('data', {}).get('reason') == 'stale_generation', stale
        receipt = await rpc('prompt.receipt', {'session_id': row['session_id'], 'admission_id': row['admission_id']}, 12)
        assert receipt['result']['status'] == 'unknown', receipt
        resolved = await rpc('prompt.resolve_unknown', params, 13)
        assert resolved['result']['status'] == 'terminal' and resolved['result']['outcome'] == 'interrupted', resolved
        # Read back exact target through the same authenticated native connection.
        readback = await rpc('prompt.receipt', {'session_id': row['session_id'], 'admission_id': row['admission_id']}, 14)
        assert readback['result']['status'] == 'terminal' and readback['result']['outcome'] == 'interrupted', readback
        return {'path': 'GatewayControlServer SO_PEERCRED -> gateway_client._session_ticket -> GatewayRuntimeAPI -> tui_gateway.ws -> AuthorityConnection.dispatch',
                'unix_socket': str(HOME / 'gateway.sock'), 'websocket_transport': 'in-memory ASGI queues, no TCP listener',
                'stale_instance_bootstrap_refused': denied, 'stale_generation_refused': stale,
                'unknown_receipt': receipt, 'resolution': resolved, 'resolution_readback': readback}
    finally:
        if task:
            await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
            await asyncio.wait_for(task, 15)
        await control.stop()


async def child_main():
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import BasePlatformAdapter, SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
    from hermes_state_runtime import list_session_admissions
    from gateway.session_unattended_unknown import is_unattended
    from run_agent import AIAgent
    from datetime import datetime

    class Adapter(BasePlatformAdapter):
        interactive_resume = False
        def __init__(self, config):
            super().__init__(config, Platform('native_unknown_probe'))
        async def connect(self, *, is_reconnect=False):
            raise AssertionError('No service connection in this proof')
        async def disconnect(self):
            pass
        async def send(self, chat_id, content, reply_to=None, metadata=None):
            return SendResult(success=True, message_id='fixture-delivery')
        async def send_typing(self, chat_id, metadata=None):
            pass
        async def get_chat_info(self, chat_id):
            return {'id': chat_id, 'name': chat_id, 'type': 'service'}

    manager = PluginManager(scope_key=str(HOME))
    ctx = PluginContext(PluginManifest(name='native-unknown-probe', key='native-unknown-probe', source='user'), manager)
    ctx.register_platform(name='native_unknown_probe', label='Isolated native unknown proof',
                          adapter_factory=Adapter, check_fn=lambda: True, trusted_inbound=True, display_tier='minimal')
    for name, handler in [('native_probe_persist', write_effect), ('native_probe_inspect', inspect_effect)]:
        ctx.register_tool(name=name, toolset='native_unknown_fixture', schema={
            'name': name, 'description': 'Disposable private proof effect ' + name,
            'parameters': {'type': 'object', 'properties': {}, 'additionalProperties': False}},
            handler=handler, check_fn=lambda: True)
    runner = GatewayRunner(GatewayConfig(max_concurrent_sessions=1))
    runner._gateway_loop = asyncio.get_running_loop()
    runner._running = True
    authority = await initialize_session_authority(runner, profile_id=str(HOME), instance_id='native-unknown-' + PHASE)
    adapter = runner._create_adapter(Platform('native_unknown_probe'), PlatformConfig(enabled=True))
    runner.adapters[adapter.platform] = adapter
    runner._wire_adapter_handlers(adapter)
    source = adapter.build_source(chat_id='isolated-owner-crash-lane', chat_type='service', user_id=None)

    def event(phase):
        return MessageEvent(text=('NATIVE_UNKNOWN_PROBE ' + phase + '\n' + (
            'Use native_probe_persist exactly once, then await the next model response.' if phase == 'crash' else
            'Operator inspected the persisted effect. Deliberate continuation: use ONLY native_probe_inspect to read it. Do not repeat the mutation.')),
            source=adapter.build_source(chat_id=source.chat_id, chat_type='service', user_id=None),
            message_id='native-unknown-' + phase, timestamp=datetime(2026, 1, 1),
            internal=False, allow_gateway_control=False, reply_expected=True)

    def rows():
        return list_session_admissions(authority.db, session_id=sid, pending_only=False)

    def compact(values):
        return [dict(session_id=row['target_session_id'], **{k: row.get(k) for k in (
            'admission_id', 'request_id', 'status', 'outcome', 'generation', 'owner_epoch')}) for row in values]

    try:
        if PHASE == 'crash':
            await adapter.handle_message(event('crash'))
            await until(POST_EFFECT.is_set)
            entry = runner.session_store.get_or_create_session(source)
            sid = entry.session_id
            values = rows()
            assert len(values) == 1 and values[0]['status'] == 'started', values
            assert is_unattended(values[0]) is False
            agent = runner._resident_agent_for(entry.session_key)
            assert isinstance(agent, AIAgent), type(agent)
            verify_isolation()
            save('crash-checkpoint.json', {'session_id': sid, 'session_key': entry.session_key,
                'epoch': authority.epoch, 'agent_class': type(agent).__module__ + '.' + type(agent).__name__,
                'admissions': compact(values), 'effect': effect(), 'model_requests': REQUESTS,
                'tool_calls': TOOL_CALLS, 'is_unattended': False, 'unexpected_network': NETWORK,
                'suppressed_core_subprocesses': SUPPRESSED,
                'suppressed_ipv6_capability_binds': SUPPRESSED_BINDS,
                'barrier': 'tool returned and actual AIAgent issued post-effect provider request; owner still started'})
            await asyncio.Event().wait()  # parent SIGKILLs this owner; no graceful settlement
        else:
            checkpoint = json.loads((RUN / 'crash-checkpoint.json').read_text())
            sid = checkpoint['session_id']
            original = rows()[0]
            assert original['status'] == 'unknown', original
            assert is_unattended(original) is False
            assert effect() == checkpoint['effect']
            recovery = await authority.recover_native_sessions([(sid, source, adapter)])
            assert recovery == {sid: 'unknown_execution'}, recovery
            # Explicit test/operator-chosen new input, never an automatic replay of old work.
            await adapter.handle_message(event('continue'))
            await until(lambda: len(rows()) == 2)
            await asyncio.sleep(.3)
            paused = rows()
            assert [r['status'] for r in paused] == ['unknown', 'queued'], paused
            assert not REQUESTS and not TOOL_CALLS
            save('paused-checkpoint.json', {'epoch': authority.epoch, 'recovery': recovery,
                'admissions': compact(paused), 'effect': effect(), 'provider_requests': len(REQUESTS)})
            operator = await native_operator(runner, authority, dict(original, session_id=sid))
            await until(lambda: all(r['status'] == 'terminal' for r in rows()))
            await until(lambda: not adapter._active_sessions)
            final = rows()
            assert [r['outcome'] for r in final] == ['interrupted', 'completed'], final
            entry = runner.session_store.get_or_create_session(source)
            assert entry.session_id == sid and entry.session_key == checkpoint['session_key']
            assert isinstance(runner._resident_agent_for(entry.session_key), AIAgent)
            assert effect() == checkpoint['effect']
            assert [r['tool'] for r in TOOL_CALLS] == ['native_probe_inspect'], TOOL_CALLS
            assert len(REQUESTS) == 2, REQUESTS
            verify_isolation()
            save('restart-result.json', {'passed': True, 'session_id': sid, 'session_key': entry.session_key,
                'epoch': authority.epoch, 'same_isolated_home': str(HOME), 'native_pause': compact(paused),
                'operator': operator, 'final_admissions': compact(final), 'effect': effect(),
                'model_requests': REQUESTS, 'tool_calls': TOOL_CALLS, 'unexpected_network': NETWORK,
                'suppressed_core_subprocesses': SUPPRESSED,
                'suppressed_ipv6_capability_binds': SUPPRESSED_BINDS,
                'limitations': ['Private file effect, not real Gate server mutation.',
                    'HTTPX fixture model transport, not a real provider call.',
                    'ASGI WebSocket transport fixture; actual Unix peer authentication and actual native gate/dispatcher.',
                    'No generic exactly-once guarantee: operator inspected effect and continuation explicitly reads only.']})
    finally:
        runner._running = False
        manager.unload()
        await runner._session_db.close()


def orchestrate():
    global RUN, HOME
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    RUN = EVIDENCE / ('run-' + uuid.uuid4().hex[:12])
    RUN.mkdir()
    HOME = Path(tempfile.mkdtemp(prefix='nup-', dir=SCRATCH))
    HOME.chmod(0o700)
    assert len(str(HOME / 'gateway.sock').encode()) <= 100  # never fallback to /tmp
    (HOME / 'config.yaml').write_text('''model:
  default: native-unknown-fixture
  provider: custom
  base_url: https://native-unknown-fixture.invalid/v1
  context_length: 65536
  api_mode: chat_completions
auxiliary:
  title_generation:
    enabled: false
memory:
  memory_enabled: false
  user_profile_enabled: false
compression:
  enabled: false
display:
  busy_input_mode: queue
  tool_progress: off
  streaming: false
tools:
  tool_search:
    enabled: off
platform_toolsets:
  native_unknown_probe:
    - native_unknown_fixture
plugins:
  entries: {}
terminal:
  cwd: ''' + str(HOME) + '\n')
    save('run.json', {'home': str(HOME), 'installed_source': str(SOURCE), 'python': sys.executable})
    hashes = {}
    for name in ['run_agent.py', 'gateway/session_authority.py', 'gateway/session_controls.py',
                 'gateway/control_socket.py', 'gateway/runtime_bootstrap.py', 'gateway/run_api.py',
                 'tui_gateway/ws.py', 'gateway/session_unattended_unknown.py']:
        hashes[name] = hashlib.sha256((SOURCE / name).read_bytes()).hexdigest()
    save('source-index.json', hashes)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
    child = None
    try:
        with (RUN / 'crash.log').open('w') as log:
            child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--child', 'crash', '--home', str(HOME), '--run', str(RUN)], stdout=log, stderr=subprocess.STDOUT, env=env)
            deadline = time.monotonic() + 75
            while not (RUN / 'crash-checkpoint.json').exists():
                if child.poll() is not None:
                    raise RuntimeError('Crash owner exited early: see ' + str(RUN / 'crash.log'))
                if time.monotonic() >= deadline:
                    raise TimeoutError('No native post-effect checkpoint')
                time.sleep(.05)
            before = json.loads((RUN / 'crash-checkpoint.json').read_text())
            assert len(before['effect']['records']) == 1
            child.send_signal(signal.SIGKILL)
            code = child.wait(timeout=15)
            assert code == -signal.SIGKILL, code
            save('owner-kill.json', {'owned_pid': child.pid, 'signal': 'SIGKILL', 'returncode': code, 'reaped': True})
        child = None
        with (RUN / 'restart.log').open('w') as log:
            completed = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--child', 'restart', '--home', str(HOME), '--run', str(RUN)], stdout=log, stderr=subprocess.STDOUT, env=env, timeout=100)
            assert completed.returncode == 0, 'Restart proof failed: see ' + str(RUN / 'restart.log')
        result = json.loads((RUN / 'restart-result.json').read_text())
        with sqlite3.connect('file:' + str(HOME / 'state.db') + '?mode=ro', uri=True) as db:
            integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
            db.row_factory = sqlite3.Row
            exact = [dict(row) for row in db.execute('SELECT target_session_id, admission_id, request_id, status, outcome, generation, owner_epoch FROM session_admissions WHERE target_session_id=? ORDER BY seq', (result['session_id'],))]
            assert [(r['status'], r['outcome']) for r in exact] == [('terminal', 'interrupted'), ('terminal', 'completed')]
            assert [r['admission_id'] for r in exact] == [r['admission_id'] for r in result['final_admissions']]
            with sqlite3.connect(str(RUN / 'state.db')) as snapshot:
                db.backup(snapshot)
        (RUN / 'effect.jsonl').write_bytes((HOME / 'effect.jsonl').read_bytes())
        save('sqlite-readback.json', exact)
        assert integrity == 'ok'
        assert not (HOME / 'gateway.sock').exists(), 'Owned socket was not cleaned up'
        result.update(sqlite_integrity=integrity, owner_sigkill_reaped=True, disposable_socket_removed=True,
                      evidence_dir=str(RUN), isolated_home=str(HOME))
        save('result.json', result)
        print(json.dumps({'passed': True, 'evidence_dir': str(RUN), 'isolated_home': str(HOME),
                          'session_id': result['session_id'], 'final_admissions': result['final_admissions'],
                          'effect_records': len(result['effect']['records']), 'sqlite_integrity': integrity}, indent=2))
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.wait(timeout=15)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--child', choices=['crash', 'restart'])
    parser.add_argument('--home', type=Path)
    parser.add_argument('--run', type=Path)
    args = parser.parse_args()
    if args.child:
        HOME, RUN, PHASE = args.home.resolve(), args.run.resolve(), args.child
        assert HOME.parent == SCRATCH and HOME.name.startswith('nup-')
        assert RUN.parent == EVIDENCE
        configure_child()
        import httpx
        try:
            with patch.object(httpx.Client, 'send', fixture_http), patch.object(socket.socket, 'connect', guarded_connect), patch.object(socket.socket, 'bind', guarded_bind), patch.object(subprocess, 'Popen', suppressed_subprocess):
                asyncio.run(child_main())
        except BaseException:
            traceback.print_exc()
            sys.exit(1)
    else:
        orchestrate()
