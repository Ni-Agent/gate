"""Compact Gate notifications; Hermes owns admission, queuing and execution."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Callable
import uuid
import httpx
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent
from client.gate_client import GateClient
from .policy import require_queue_policy
from .routes import canonical, task_route

log = logging.getLogger('hermes.gate.native')


@dataclass(eq=False)
class GateAdapter(BasePlatformAdapter):
    config: PlatformConfig
    client_factory: Callable[[], GateClient]
    spawn_task: Callable
    expected_identity: str | None = None
    client: GateClient = field(init=False, repr=False)
    identity: str | None = field(default=None, init=False)
    seen: set[str] = field(default_factory=set, init=False, repr=False)
    _transport_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _lifecycle: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _outage: bool = field(default=False, init=False)
    interactive_resume = False

    def __post_init__(self):
        super().__init__(self.config, Platform('gate'))
        self.client = self.client_factory()

    async def connect(self, *, is_reconnect=False):
        async with self._lifecycle:
            if self._transport_task and not self._transport_task.done():
                return True
            require_queue_policy()
            self.identity = await asyncio.to_thread(self.client.authenticate, self.identity or self.expected_identity)
            self._transport_task = self.spawn_task(self._listen(), name='Gate SSE transport')
            self._mark_connected()
            return True

    async def disconnect(self):
        async with self._lifecycle:
            if self._transport_task:
                self._transport_task.cancel()
                try:
                    await self._transport_task
                except asyncio.CancelledError:
                    pass
                self._transport_task = None
            self._mark_disconnected()

    async def _submit(self, task, *, control=False):
        assert self.identity is not None  # Authenticated before listener handoff.
        task_id, revision = task['id'], task['execution_revision']
        text = ('Gate task changed (retrieved data):\n' + canonical({'task_id': task_id, 'execution_revision': revision}) +
                '\nRead current task with gate_call before acting; coordinate work and fetch guidance as needed. '
                'A successful Gate API response is the committed result.')
        delivery = f'{task_id}:execution:{revision}:compact-v2'
        if control:
            text, delivery = '/stop', delivery + ':assignment-loss'
        source = self.build_source(chat_id=task_route(self.client.base_url, self.identity, task_id),
                                   chat_type='service', user_id=None, message_id=delivery)
        await self.handle_message(MessageEvent(text=text, source=source, message_id=delivery,
            timestamp=datetime.fromisoformat(task['execution_updated_at'].replace('Z', '+00:00')),
            internal=False, allow_gateway_control=control))

    async def _dispatch(self, task, *, previous_assignee=None):
        task_id = task['id']
        if task['assignee'] != self.identity:
            if task_id in self.seen or previous_assignee == self.identity:
                await self._submit(task, control=True)
            self.seen.discard(task_id)
        elif task['status'] in ('ready', 'in_progress'):
            await self._submit(task)
            self.seen.add(task_id)
        # A same-assignee terminal task may still have an active native worker.
        # Retain its observed lane until assignment loss has been handed off.

    async def invalidate(self, event):
        task = await asyncio.to_thread(self.client.rpc, 'read_resource', id=event['id'])
        await self._dispatch(task, previous_assignee=event.get('previous_assignee'))

    async def reconcile_assigned(self):
        # One snapshot after subscription, not periodic task polling.
        after, current = '', set()
        while True:
            page = await asyncio.to_thread(self.client.rpc, 'list_tasks', assignee=self.identity, after=after)
            for task in page['items']:
                current.add(task['id'])
                await self._dispatch(task)
            if not page['has_more']:
                break
            after = page['next_cursor']
        for task_id in self.seen - current:
            await self.invalidate({'id': task_id})

    async def _connection(self):
        await asyncio.to_thread(self.client.authenticate, self.identity)
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=httpx.Timeout(45, connect=5)) as transport:
            async with transport.stream('GET', self.client.base_url + '/events',
                                        headers={'Authorization': 'Bearer ' + self.client.token}) as response:
                response.raise_for_status()
                if response.headers.get('content-type', '').split(';')[0] != 'text/event-stream':
                    raise ValueError('Gate SSE protocol refused')
                event, data = '', []
                async for line in response.aiter_lines():
                    if not line:
                        if event == 'ready':
                            await self.reconcile_assigned()
                            self._outage = False
                        elif event == 'task.updated':
                            await self.invalidate(json.loads('\n'.join(data)))
                        event, data = '', []
                    elif line.startswith('event:'):
                        event = line[6:].strip()
                    elif line.startswith('data:'):
                        data.append(line[5:].lstrip())
                raise ConnectionError('Gate SSE closed')

    async def _listen(self):
        delay = .25
        while True:
            try:
                await self._connection()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning('Gate connection/handoff failed: %s', type(error).__name__)
                if not self._outage:
                    self._outage = True
                    try:
                        await self._diagnose(type(error).__name__)
                    except Exception as diagnostic_error:
                        log.warning('Gate diagnostic handoff failed: %s', type(diagnostic_error).__name__)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

    async def _diagnose(self, error_type):
        delivery = 'gate-outage-' + uuid.uuid4().hex
        source = self.build_source(chat_id=delivery, chat_type='service', user_id=None, message_id=delivery)
        await self.handle_message(MessageEvent(
            text=f'Gate connection interrupted ({error_type}). Diagnose connectivity if useful; do not repeat uncertain task effects.',
            source=source, message_id=delivery, timestamp=datetime.now(timezone.utc),
            internal=False, allow_gateway_control=False))

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        # Pre-agent admission failures and a Gate outage cannot be reported by an agent to Gate.
        from agent.redact import redact_sensitive_text
        safe = redact_sensitive_text(content.replace(self.client.token, '<redacted>'), force=True, redact_url_credentials=True)
        log.warning('Gate native notice [%s]: %s', chat_id, safe)
        return SendResult(success=True, message_id='native-presentation')

    async def get_chat_info(self, chat_id):
        return {'name': 'Gate task', 'type': 'service', 'id': chat_id}
