#!/usr/bin/env python3
"""Synthetic-only acceptance: real Go HTTP/auth/SSE and installed native Hermes.

Only provider HTTP responses are deterministic fixtures. Admission, FIFO,
TurnRunner, AIAgent, tool dispatch, Gate responses and sockets are real. No --real
mode, provider credentials, deployed Gate or production profile access.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import copy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
SOURCE = Path(sys.executable).parents[2] / 'workspace'
PRODUCTION = Path('/home/hermes/.hermes')
OUTPUT = sys.stdout
IDENTITY = 'hermes:default'
OTHER = 'hermes:prometheus'
TOKEN = 'synthetic-native-test-token'
REQUESTS = []
LOCK = threading.Lock()
ENTERED = {k: threading.Event() for k in ('a', 'b')}
RELEASE = {k: threading.Event() for k in ('a', 'b')}
FIRST = set()
INFLIGHT = PEAK = 0
REAL_SEND = None
ORIGINS = set()
NETWORK_DENIED = []


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def audit_guard(event, args):
    if event == 'socket.connect':
        sock, address = args
        if sock.family in (socket.AF_INET, socket.AF_INET6) and f'http://{address[0]}:{address[1]}' in ORIGINS:
            return
        NETWORK_DENIED.append(repr(address))
        raise PermissionError('network outside owned loopback Gate servers forbidden')
    if event != 'open':
        return
    path, mode, flags = args
    if not isinstance(path, (str, bytes, os.PathLike)):
        return
    p = Path(os.fsdecode(path)).resolve()
    if p == PRODUCTION or PRODUCTION in p.parents:
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            # The disposable profile is explicitly below TMPDIR, never another profile.
            if RUN == p or RUN in p.parents:
                return
            raise PermissionError('production writes forbidden by acceptance host')
        if RUN == p or RUN in p.parents or SOURCE == p or SOURCE in p.parents:
            return
        if any(x in p.parts for x in ('tools', 'installs', 'cache')) and p.suffix in ('.py', '.pyc', '.so', '.yaml', '.json', '.md'):
            if 'profiles' not in p.parts and p.name not in ('auth.json', '.env', 'config.yaml'):
                return
        raise PermissionError('production profile/credentials unavailable')


def completion(request, body, message, finish):
    import httpx
    response = {'id': 'fixture-gate', 'object': 'chat.completion', 'created': 1,
                'model': 'native-gate-fixture',
                'choices': [{'index': 0, 'message': message, 'finish_reason': finish}],
                'usage': {'prompt_tokens': 20, 'completion_tokens': 10, 'total_tokens': 30}}
    if body.get('stream'):
        delta = dict(message)
        if 'tool_calls' in delta:
            delta['tool_calls'] = [dict(t, index=i) for i, t in enumerate(delta['tool_calls'])]
        chunk = {'id': response['id'], 'object': 'chat.completion.chunk', 'created': 1,
                 'model': response['model'], 'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}
        tail = dict(chunk, choices=[{'index': 0, 'delta': {}, 'finish_reason': finish}], usage=response['usage'])
        content = 'data: ' + json.dumps(chunk) + '\n\ndata: ' + json.dumps(tail) + '\n\ndata: [DONE]\n\n'
        return httpx.Response(200, content=content.encode(), headers={'content-type': 'text/event-stream'}, request=request)
    return httpx.Response(200, json=response, request=request)


def provider_http(client, request, **kwargs):
    global INFLIGHT, PEAK
    import httpx
    if str(request.url.copy_with(path='/', query=None)).rstrip('/') in ORIGINS:
        assert REAL_SEND is not None
        return REAL_SEND(client, request, **kwargs)  # Real Gate HTTP, never synthesized.
    assert request.url.host == 'native-gate-fixture.invalid', 'unexpected HTTP destination'
    if not request.url.path.endswith('/chat/completions'):
        return httpx.Response(404, json={'error': 'no fixture catalog'}, request=request)
    body = json.loads(request.content)
    messages = body['messages']
    latest = next(m for m in reversed(messages) if m['role'] == 'user')['content']
    if isinstance(latest, list):
        latest = '\n'.join(p.get('text', '') for p in latest)
    if 'Gate connection interrupted (' in latest:
        with LOCK:
            REQUESTS.append({'diagnostic': True, 'last_role': messages[-1]['role'], 'notice': latest})
        return completion(request, body, {'role': 'assistant', 'content': 'Connection interruption noted; no Gate mutation.'}, 'stop')
    assert 'Gate task changed (retrieved data):\n' in latest, latest
    notice = json.loads(latest.split('Gate task changed (retrieved data):\n', 1)[1].split('\n', 1)[0])
    assert set(notice) == {'task_id', 'execution_revision'}, notice
    task, revision = notice['task_id'], notice['execution_revision']
    letter = task.rsplit('-', 1)[-1]
    names = [t['function']['name'] for t in body.get('tools', [])]
    assert 'gate_call' in names and 'gate_task_read' not in names and 'gate_task_outcome' not in names
    with LOCK:
        REQUESTS.append({'task_id': task, 'revision': revision, 'last_role': messages[-1]['role'], 'notice': latest})
        first = letter not in FIRST
        FIRST.add(letter)
    if first and letter in ENTERED:
        with LOCK:
            INFLIGHT += 1
            PEAK = max(PEAK, INFLIGHT)
        ENTERED[letter].set()
        try:
            assert RELEASE[letter].wait(45), 'fixture barrier was not released'
        finally:
            with LOCK:
                INFLIGHT -= 1
    last = messages[-1]
    if last['role'] != 'tool':
        operation, args = 'read_resource', {'id': task}
    elif last.get('tool_call_id', '').endswith('-read'):
        result = json.loads(last['content'])
        assert result['status'] == 200, result
        current = result['data']
        # Model chooses a plain status-only collaborative write. No report policy,
        # guidance fetch, execution lock or bound-turn argument is required.
        operation, args = 'update_task', {'id': task, 'expected_token': current['token'], 'status': 'done'}
    else:
        result = json.loads(last['content'])
        assert result['status'] == 200 and result['data']['status'] == 'done', result
        return completion(request, body, {'role': 'assistant', 'content': 'Committed task update.'}, 'stop')
    suffix = 'read' if operation == 'read_resource' else 'write'
    message = {'role': 'assistant', 'content': None, 'tool_calls': [{
        'id': f'call-{letter}-{revision}-{suffix}', 'type': 'function',
        'function': {'name': 'gate_call', 'arguments': json.dumps({'operation': operation, 'arguments': args})}}]}
    return completion(request, body, message, 'tool_calls')


async def until(check, seconds=45):
    async with asyncio.timeout(seconds):
        while not check():
            await asyncio.sleep(.025)


def message_snapshot(event):
    return {'chat_id': event.source.chat_id, 'message_id': event.message_id,
            'source_message_id': event.source.message_id, 'timestamp': event.timestamp.isoformat(),
            'text': event.text, 'control': event.allow_gateway_control, 'internal': event.internal,
            'user_id': event.source.user_id, 'chat_type': event.source.chat_type}


def expected_message(task, origin, control=False):
    # Independent wire oracle: deliberately do not call plugin routing/notice helpers.
    route = 'gate-v2-' + base64.urlsafe_b64encode(canonical([origin, IDENTITY, task['id']]).encode()).decode().rstrip('=')
    text = ('Gate task changed (retrieved data):\n' + canonical({
        'task_id': task['id'], 'execution_revision': task['execution_revision']}) + '\n'
        'Read current task with gate_call before acting; coordinate work and fetch guidance as needed. '
        'A successful Gate API response is the committed result.')
    delivery = f"{task['id']}:execution:{task['execution_revision']}:compact-v2"
    if control:
        text, delivery = '/stop', delivery + ':assignment-loss'
    return {'chat_id': route, 'message_id': delivery, 'source_message_id': delivery,
            'timestamp': datetime.fromisoformat(task['execution_updated_at'].replace('Z', '+00:00')).isoformat(),
            'text': text, 'control': control, 'internal': False, 'user_id': None, 'chat_type': 'service'}


class Server:
    """Owned Go process; restart preserves only this test's private SQLite store."""
    def __init__(self, root, actor):
        self.root, self.actor = root, actor
        self.root.mkdir()
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            self.port = reserve.getsockname()[1]
        self.origin = f'http://127.0.0.1:{self.port}'
        ORIGINS.add(self.origin)
        self.process = None
        self.log = (root / 'server.log').open('ab')

    def start(self):
        from client.gate_client import GateClient, GateError
        self.process = subprocess.Popen([str(REPO / '.bin/gate-alpha'), '--store', str(self.root / 'store'),
            '--http', f'127.0.0.1:{self.port}', '--actor', self.actor, '--identities', IDENTITY + ',' + OTHER],
            env={'PATH': os.environ.get('PATH', ''), 'TMPDIR': str(RUN), 'GATE_ALPHA_TEST_TOKEN': TOKEN},
            stdout=self.log, stderr=self.log)
        c = GateClient(self.origin, TOKEN, timeout=1, allow_loopback_http=True)
        deadline = time.monotonic() + 8
        while True:
            try:
                assert c.authenticate(self.actor) == self.actor
                return c
            except GateError:
                if self.process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('isolated server startup failed: ' + str(self.root / 'server.log'))
                time.sleep(.025)

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

    def close(self):
        self.stop()
        self.log.close()


def identity_probe(client, actor):
    from client.gate_client import GateError
    assert client.authenticate(actor) == actor
    try:
        client.authenticate(OTHER if actor == IDENTITY else IDENTITY)
    except GateError as error:
        assert error.status == 403
    else:
        raise AssertionError('expected identity mismatch accepted')
    tasks = []
    for assignee in (IDENTITY, OTHER):
        task = client.rpc('create_task', operation_id='identity-' + assignee.replace(':', '-'),
                          assignee=assignee, body='identity attribution', status='triage')
        assert task['assignee'] == assignee and task['creator'] == actor and task['modified_by'] == actor
        task = client.rpc('update_task', id=task['id'], expected_token=task['token'], body='collaborative editor')
        assert task['assignee'] == assignee and task['modified_by'] == actor
        exact = client.rpc('read_resource', id=task['id'])
        assert exact == task
        assert any(t['id'] == task['id'] for t in client.rpc('list_tasks', assignee=assignee)['items'])
        tasks.append({'id': task['id'], 'creator': task['creator'], 'assignee': task['assignee'], 'modified_by': task['modified_by']})
    return {'authenticated_identity': actor, 'tasks': tasks}


async def exercise(run, home, server, client, identities):
    import httpx
    from client.gate_client import GateClient
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platform_registry import platform_registry
    from gateway.run import GatewayRunner
    from gateway.session import SessionSource
    from gateway.session_authority import initialize_session_authority
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
    from hermes_state_runtime import list_session_admissions
    from model_tools import handle_function_call
    from run_agent import AIAgent
    import native_gate

    manager = PluginManager(scope_key=str(home))
    ctx = PluginContext(PluginManifest(name='gate-alpha-native', key='gate-alpha-native', source='user'), manager)
    spawned, observed, rpcs, streams, wire, invalidations = [], [], [], [], [], []
    spawn = ctx.spawn_task
    def observed_spawn(coro, *, name=None):
        task = spawn(coro, name=name)
        spawned.append(task)
        return task
    ctx.spawn_task = observed_spawn
    previous = platform_registry.snapshot_registration('gate', scope=str(home))
    native_gate.register(ctx)
    ctx.register_platform(name='gate_untrusted', label='Untrusted negative control',
                          adapter_factory=lambda config: None, check_fn=lambda: True, trusted_inbound=False)
    runner = GatewayRunner(GatewayConfig(max_concurrent_sessions=3))
    runner._gateway_loop, runner._running = asyncio.get_running_loop(), True
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='native-minimal-acceptance')
    assert not runner._is_user_authorized(SessionSource(platform=Platform('gate_untrusted'), chat_id='untrusted', chat_type='service', user_id=None))
    adapter = runner._create_adapter(Platform('gate'), PlatformConfig(enabled=True))
    from native_gate.adapter import GateAdapter
    assert isinstance(adapter, GateAdapter)
    runner.adapters[adapter.platform] = adapter
    runner._wire_adapter_handlers(adapter)
    handle_message, rpc_impl = adapter.handle_message, GateClient.rpc
    stream_impl, transport_impl = httpx.AsyncClient.stream, httpx.HTTPTransport.handle_request
    async def observed_message(event):
        row = {'event': copy.deepcopy(event), 'snapshot': message_snapshot(event), 'handed_off': False}
        observed.append(row)
        await handle_message(event)
        row['handed_off'] = True
    adapter.handle_message = observed_message
    invalidate_impl = adapter.invalidate
    async def observed_invalidate(event):
        await invalidate_impl(event)
        invalidations.append(dict(event))
    adapter.invalidate = observed_invalidate
    def session_key():
        from gateway.session_context import bound_session_key
        return bound_session_key()
    def observed_rpc(c, operation, **args):
        row = {'operation': operation, 'args': dict(args), 'session_key': session_key()}
        result = rpc_impl(c, operation, **args)
        with LOCK:
            rpcs.append(dict(row, result=copy.deepcopy(result)))
        return result
    def observed_wire(transport, request):
        with LOCK:
            wire.append({'method': request.method, 'path': request.url.path, 'session_key': session_key()})
        return transport_impl(transport, request)
    @contextlib.asynccontextmanager
    async def observed_stream(c, method, url, **kwargs):
        assert url == server.origin + '/events'
        row: dict = {'active': False, 'closed': False, 'cancelled': False}
        streams.append(row)
        response = None
        try:
            async with stream_impl(c, method, url, **kwargs) as response:
                row.update(active=True, status=response.status_code)
                try:
                    yield response
                except asyncio.CancelledError:
                    row['cancelled'] = True
                    raise
        finally:
            row.update(active=False, closed=response is not None and response.is_closed)
    def snapshots():
        return [r for r in rpcs if r['operation'] == 'list_tasks']
    def messages(mid):
        return [r for r in observed if r['snapshot']['message_id'] == mid and r['handed_off']]
    def mid(task, revision=None):
        return f"{task['id']}:execution:{revision or task['execution_revision']}:compact-v2"
    def settled():
        return not adapter._active_sessions and all(l.task is None or l.task.done() for l in authority.sessions.values())
    async def rpc(op, **args):
        return await asyncio.to_thread(client.rpc, op, **args)
    async def generic(op, **args):
        return json.loads(await asyncio.to_thread(handle_function_call, 'gate_call', {'operation': op, 'arguments': args}))
    entries, tasks = {}, {}
    unloaded = False
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(GateClient, 'rpc', observed_rpc))
        stack.enter_context(patch.object(httpx.HTTPTransport, 'handle_request', observed_wire))
        stack.enter_context(patch.object(httpx.AsyncClient, 'stream', observed_stream))
        try:
            # Ordinary installed dispatcher, no bound service session/turn/task env.
            # This also writes a task assigned to the OTHER identity: no ownership lock.
            assert not session_key()
            before = len(wire)
            ordinary = await generic('create_task', operation_id='ordinary-generic', assignee=OTHER, body='plain unbound write', status='triage')
            assert ordinary['status'] == 200 and set(ordinary) == {'status', 'data'}, ordinary
            assert [r['path'] for r in wire[before:]] == ['/rpc/create_task'], 'hidden authenticate/readback'
            old = ordinary['data']
            before = len(wire)
            changed = await generic('update_task', id=old['id'], expected_token=old['token'], body='no report format or bound-turn guard')
            assert changed['status'] == 200 and changed['data']['modified_by'] == IDENTITY
            assert [r['path'] for r in wire[before:]] == ['/rpc/update_task'], 'hidden tool-wrapper readback'
            before = len(wire)
            stale = await generic('update_task', id=old['id'], expected_token=old['token'], body='MUST NOT COMMIT')
            assert stale['status'] == 412 and stale['data'].get('error'), stale
            assert [r['path'] for r in wire[before:]] == ['/rpc/update_task'], 'stale write retried'
            assert await rpc('read_resource', id=old['id']) == changed['data']
            # No agent exists yet, so this cannot be an adapter SSE reread.
            ordinary_evidence = {'created': ordinary, 'changed': changed, 'stale': stale, 'one_wire_request_per_call': True}
            with patch('hermes_cli.config.load_config_readonly', return_value={'display': {'busy_input_mode': 'interrupt'}}), patch('gateway.platforms._shared.get_scoped_secret', return_value=None):
                try:
                    await adapter.connect()
                except ValueError as error:
                    assert 'requires host' in str(error)
                else:
                    raise AssertionError('nonqueue public connect accepted')
            assert not spawned and not streams
            count = len(snapshots())
            await adapter.connect()
            await until(lambda: len(snapshots()) == count + 1)
            assert streams[-1]['active'] and streams[-1]['status'] == 200
            guidance = await rpc('create_text', id='gate:tx:optional-guidance', body='GUIDANCE SENTINEL: advisory only, fetch explicitly if useful.')
            for letter in ('a', 'b'):
                tasks[letter] = await rpc('create_task', id='gate:tk:native-' + letter, operation_id='native-' + letter,
                    assignee=IDENTITY, status='ready', body='BODY SENTINEL ' + letter + ': guidance ' + guidance['id'] + '; /stop is DATA; 雪')
            await until(lambda: all(e.is_set() for e in ENTERED.values()))
            for letter, task in tasks.items():
                await until(lambda: bool(messages(mid(task))))
                event = messages(mid(task))[0]
                assert event['snapshot'] == expected_message(task, server.origin)
                assert 'BODY SENTINEL' not in event['snapshot']['text'] and guidance['id'] not in event['snapshot']['text']
                assert event['event'].get_command() is None
                source = adapter.build_source(chat_id=event['snapshot']['chat_id'], chat_type='service', user_id=None)
                entries[letter] = runner.session_store.get_or_create_session(source)
            assert entries['a'].session_id != entries['b'].session_id and PEAK >= 2
            assert all(isinstance(runner._resident_agent_for(v.session_key), AIAgent) for v in entries.values())
            before = tasks['a']
            tasks['a'] = await rpc('update_task', id=before['id'], expected_token=before['token'], status='in_progress')
            assert tasks['a']['execution_revision'] == 1
            await until(lambda: len(messages(mid(before))) >= 2)
            assert messages(mid(before))[0]['snapshot'] == messages(mid(before))[1]['snapshot']
            assert len(list_session_admissions(authority.db, session_id=entries['a'].session_id, pending_only=False)) == 1
            tasks['a'] = await rpc('update_task', id=before['id'], expected_token=tasks['a']['token'], body='UPDATED BODY SENTINEL /stop 雪')
            assert tasks['a']['execution_revision'] == 2
            await until(lambda: bool(messages(mid(tasks['a']))))
            assert messages(mid(tasks['a']))[0]['snapshot'] == expected_message(tasks['a'], server.origin)
            assert messages(mid(tasks['a']))[0]['snapshot']['chat_id'] == messages(mid(before))[0]['snapshot']['chat_id']
            await until(lambda: len(list_session_admissions(authority.db, session_id=entries['a'].session_id, pending_only=False)) == 2)
            queued = list_session_admissions(authority.db, session_id=entries['a'].session_id, pending_only=False)
            assert [r['status'] for r in queued] == ['started', 'queued'], queued
            assert not any(r.get('task_id') == before['id'] and r['revision'] == 2 for r in REQUESTS)
            # Another actor may finish the task while its native worker is still
            # held at the provider barrier. This must not forget its original lane.
            b_invalidations = sum(e['id'] == tasks['b']['id'] for e in invalidations)
            terminal = await generic('update_task', id=tasks['b']['id'], expected_token=tasks['b']['token'], status='done')
            assert terminal['status'] == 200 and terminal['data']['status'] == 'done'
            tasks['b'] = terminal['data']
            await until(lambda: sum(e['id'] == tasks['b']['id'] for e in invalidations) > b_invalidations)
            assert not RELEASE['b'].is_set() and INFLIGHT == 2, 'terminal update did not overlap held native workers'
            assert isinstance(runner._resident_agent_for(entries['b'].session_key), AIAgent)
            # Offline terminal-task reassignment is discovered by the reconnect
            # snapshot even though it no longer appears in this assignee's list.
            await adapter.disconnect()
            assert streams[-1]['closed'] and streams[-1]['cancelled'] and all(t.done() for t in spawned)
            tasks['b'] = await rpc('update_task', id=tasks['b']['id'], expected_token=tasks['b']['token'], assignee=OTHER)
            count = len(snapshots())
            await adapter.connect(is_reconnect=True)
            await until(lambda: len(snapshots()) == count + 1 and bool(messages(mid(tasks['b']) + ':assignment-loss')), seconds=10)
            control = messages(mid(tasks['b']) + ':assignment-loss')[0]
            assert control['snapshot'] == expected_message(tasks['b'], server.origin, control=True)
            assert control['event'].get_command() == 'stop'
            for e in RELEASE.values():
                e.set()
            await until(settled)
            final = {k: await rpc('read_resource', id=t['id']) for k, t in tasks.items()}
            assert final['a']['status'] == 'done'
            assert final['b']['status'] == 'done' and final['b']['assignee'] == OTHER
            rows = {k: list_session_admissions(authority.db, session_id=e.session_id, pending_only=False) for k, e in entries.items()}
            assert [r['outcome'] for r in rows['a']] == ['completed', 'completed'], rows['a']
            assert rows['b'][0]['outcome'] == 'interrupted', rows['b']
            # Both native turns fetched CURRENT state, including the queued body.
            a_calls = [r for r in rpcs if r['session_key'] == entries['a'].session_key]
            assert [r['operation'] for r in a_calls] == ['read_resource', 'update_task', 'read_resource', 'update_task'], a_calls
            assert all(r['result']['body'] == 'UPDATED BODY SENTINEL /stop 雪' for r in a_calls if r['operation'] == 'read_resource')
            assert all(r['args'].get('id') != guidance['id'] for r in rpcs if r['operation'] == 'read_resource'), 'plugin auto-fetched optional guidance'
            frozen = messages(mid(tasks['a']))[0]['event']
            model_count, admission_count = len(REQUESTS), len(rows['a'])
            await adapter.handle_message(copy.deepcopy(frozen))
            await until(settled)
            await asyncio.sleep(.2)
            assert len(REQUESTS) == model_count
            assert len(list_session_admissions(authority.db, session_id=entries['a'].session_id, pending_only=False)) == admission_count
            count, connections = len(snapshots()), len(streams)
            await adapter.disconnect()
            await adapter.connect(is_reconnect=True)
            await until(lambda: len(snapshots()) == count + 1 and len(streams) == connections + 1)
            await asyncio.sleep(.35)
            assert len(REQUESTS) == model_count and len(snapshots()) == count + 1, 'feedback work or periodic task list'
            # Actual socket outage and real restart; retry transport isn't mocked.
            snapshots_before_outage = len(snapshots())
            auth_before_outage = sum(r['path'] == '/whoami' for r in wire)
            server.stop()
            await until(lambda: any(r.get('diagnostic') for r in REQUESTS))
            await asyncio.sleep(1.1)  # Covers multiple .25/.5 second retries, not a periodic poll.
            diagnostics = [r for r in REQUESTS if r.get('diagnostic')]
            assert len(diagnostics) == 1, diagnostics
            assert sum(r['path'] == '/whoami' for r in wire) >= auth_before_outage + 2, 'outage retry not exercised'
            assert len(snapshots()) == snapshots_before_outage
            await until(settled)
            diagnostic_ingress = [r for r in observed if r['snapshot']['chat_id'].startswith('gate-outage-')]
            assert len(diagnostic_ingress) == 1 and diagnostic_ingress[0]['handed_off']
            notice = diagnostic_ingress[0]['snapshot']
            assert not notice['control'] and not notice['internal'] and notice['user_id'] is None
            assert 'BODY SENTINEL' not in notice['text'] and guidance['id'] not in notice['text']
            source = adapter.build_source(chat_id=notice['chat_id'], chat_type='service', user_id=None)
            diagnostic_entry = runner.session_store.get_or_create_session(source)
            assert diagnostic_entry.session_id not in {e.session_id for e in entries.values()}
            diagnostic_rows = list_session_admissions(authority.db, session_id=diagnostic_entry.session_id, pending_only=False)
            assert len(diagnostic_rows) == 1 and diagnostic_rows[0]['outcome'] == 'completed'
            assert isinstance(runner._resident_agent_for(diagnostic_entry.session_key), AIAgent)
            assert not any(r['session_key'] == diagnostic_entry.session_key for r in rpcs), 'diagnostic fixture wrote/read Gate'
            # No monkeypatch of Popen: this restarts the real isolated Go server.
            count = len(snapshots())
            await asyncio.to_thread(server.start)
            await until(lambda: streams[-1]['active'] and len(snapshots()) >= count + 1)
            await asyncio.sleep(.4)
            assert len(snapshots()) == count + 1 and len([r for r in REQUESTS if r.get('diagnostic')]) == 1
            assert len([r for r in REQUESTS if not r.get('diagnostic')]) == model_count
            # Actual PluginContext-owned cancellation while subscribed, not merely disconnect.
            assert streams[-1]['active'] and any(not t.done() for t in spawned)
            manager.unload()
            unloaded = True
            await until(lambda: all(t.done() for t in spawned) and all(not r['active'] for r in streams), 10)
            assert all(t.cancelled() for t in spawned), 'PluginContext-owned listener was not cancelled'
            assert streams[-1]['closed'] and streams[-1]['cancelled']
            assert platform_registry.snapshot_registration('gate', scope=str(home)) == previous
            counts = len(rpcs), len(streams), len(REQUESTS)
            await asyncio.sleep(.3)
            assert counts == (len(rpcs), len(streams), len(REQUESTS)), 'post-unload feedback'
            assert not NETWORK_DENIED, NETWORK_DENIED
            result = {'passed': True, 'mode': 'synthetic-provider-only', 'identities': identities,
                'gate': 'actual isolated Go HTTP/auth/SSE/CAS/committed responses',
                'native': 'installed GatewayRunner/authority/FIFO/TurnRunner/AIAgent + installed tool dispatcher',
                'peak_provider_calls': PEAK, 'ordinary_generic': ordinary_evidence,
                'terminal_while_worker_held': terminal,
                'sessions': {k: {'id': e.session_id, 'key': e.session_key} for k, e in entries.items()},
                'admissions': rows, 'provider_requests': REQUESTS, 'observed_messages': [r['snapshot'] for r in observed],
                'completed_rpc': rpcs, 'wire_requests': wire, 'final_tasks': final,
                'streams': streams, 'assigned_snapshots': len(snapshots()),
                'diagnostic_sessions_per_outage': len(diagnostics),
                'diagnostic_session': {'id': diagnostic_entry.session_id, 'key': diagnostic_entry.session_key, 'admissions': diagnostic_rows},
                'owned_tasks_cancelled': all(t.cancelled() for t in spawned), 'denied_network': NETWORK_DENIED,
                'installed_source_sha256': {name: hashlib.sha256((SOURCE / name).read_bytes()).hexdigest() for name in (
                    'run_agent.py', 'gateway/run.py', 'gateway/session_authority.py', 'hermes_cli/plugins.py', 'model_tools.py')},
                'unloaded_while_subscribed': True, 'no_post_unload_feedback': True,
                'limits': ['Only provider responses mocked; no live provider acceptance.',
                           'Assignment-loss proves native stop/interruption, not external side-effect rollback.',
                           'Ordinary unbound tool dispatch tested; no delegated child session asserted.',
                           'Crash/operator authority acceptance is a separate supplemental probe.']}
            (run / 'result.json').write_text(json.dumps(result, indent=2, default=str) + '\n')
            print(json.dumps({'passed': True, 'evidence': str(run), 'peak_provider_calls': PEAK,
                              'native_sessions': result['sessions'], 'diagnostics': len(diagnostics),
                              'unloaded_while_subscribed': True}, indent=2), file=OUTPUT)
        finally:
            for e in RELEASE.values():
                e.set()
            if not unloaded:
                manager.unload()
            await adapter.disconnect()
            runner._running = False
            await runner._session_db.close()


def main():
    global RUN, REAL_SEND
    # Reject all options, in particular the removed unsafe credential smoke mode.
    import argparse
    argparse.ArgumentParser(description=__doc__).parse_args()
    RUN = Path(tempfile.mkdtemp(prefix='gate-native-acceptance-', dir=os.environ['TMPDIR']))
    home = RUN / 'home'
    home.mkdir()
    sys.dont_write_bytecode = True
    for key in list(os.environ):
        if key.endswith(('_TOKEN', '_API_KEY', '_SECRET', '_PASSWORD', '_CREDENTIALS')) or key.startswith(('HERMES_SESSION_', 'HERMES_GATEWAY_', 'GATEWAY_', 'OPENAI_', 'ANTHROPIC_', 'OPENROUTER_')):
            os.environ.pop(key, None)
    os.environ.update(HOME=str(home), HERMES_HOME=str(home), CODEX_HOME=str(home / 'codex'),
        TMPDIR=str(RUN), XDG_CACHE_HOME=str(home / '.cache'), PYTHONDONTWRITEBYTECODE='1',
        OPENAI_API_KEY='synthetic-fixture', OPENAI_BASE_URL='https://native-gate-fixture.invalid/v1')
    sys.path[:0] = [str(SOURCE), str(REPO)]
    sys.addaudithook(audit_guard)
    servers = []
    try:
        import httpx
        import yaml
        from client.gate_client import GateClient, GateError
        identities = []
        for actor in (OTHER, IDENTITY):
            server = Server(RUN / actor.replace(':', '-'), actor)
            servers.append(server)
            client = server.start()
            identities.append(identity_probe(client, actor))
            if actor == OTHER:
                server.close()
        server = servers[-1]
        for token in ('', 'synthetic-invalid-token'):
            # Missing and invalid credentials both reach the actual server.
            with httpx.Client(trust_env=False) as raw:
                headers = {'Authorization': 'Bearer ' + token} if token else {}
                response = raw.get(server.origin + '/whoami', headers=headers)
                assert response.status_code == 401
        os.environ.update(GATE_URL=server.origin, GATE_TOKEN=TOKEN)
        config = {'model': {'default': 'native-gate-fixture', 'provider': 'custom',
            'base_url': 'https://native-gate-fixture.invalid/v1', 'context_length': 65536, 'api_mode': 'chat_completions'},
            'auxiliary': {'title_generation': {'enabled': False}},
            'memory': {'memory_enabled': False, 'user_profile_enabled': False}, 'compression': {'enabled': False},
            'terminal': {'cwd': str(RUN)}, 'agent': {'max_turns': 5},
            'display': {'busy_input_mode': 'queue', 'tool_progress': 'off', 'streaming': False},
            'tools': {'tool_search': {'enabled': False}}, 'platform_toolsets': {'gate': ['gate_native']},
            'plugins': {'entries': {'gate-alpha-native': {'settings': {'url': server.origin,
                'identity': IDENTITY, 'allow_loopback_http': True}}}}}
        (home / 'config.yaml').write_text(yaml.safe_dump(config))
        REAL_SEND = httpx.Client.send
        # Keep installed imports' metadata subprocesses off real profiles. The only
        # allowed child while agents run is our exact local Go restart command.
        real_popen = subprocess.Popen
        def safe_popen(argv, *args, **kwargs):
            if isinstance(argv, list) and argv and argv[0] == str(REPO / '.bin/gate-alpha'):
                return real_popen(argv, *args, **kwargs)
            raise RuntimeError('native metadata subprocess disabled in isolated host')
        with patch.object(httpx.Client, 'send', provider_http), patch.object(subprocess, 'Popen', safe_popen):
            asyncio.run(asyncio.wait_for(exercise(RUN, home, server, client, identities), timeout=150))
    except BaseException as error:
        for event in RELEASE.values():
            event.set()
        (RUN / 'failure.json').write_text(json.dumps({'type': type(error).__name__, 'error': str(error), 'provider_requests': REQUESTS}, indent=2) + '\n')
        traceback.print_exc(file=OUTPUT)
        print('Evidence: ' + str(RUN), file=OUTPUT)
        raise SystemExit(1)
    finally:
        for server in servers:
            server.close()
        (RUN / 'cleanup.json').write_text(json.dumps({'servers_reaped': all(s.process.poll() is not None for s in servers)}) + '\n')


if __name__ == '__main__':
    main()
