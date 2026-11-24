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
from .routes import task_event_route

log = logging.getLogger('hermes.gate.native')


@dataclass(eq=False)
class GateAdapter(BasePlatformAdapter):
    config: PlatformConfig
    client_factory: Callable[[], GateClient]
    spawn_task: Callable
    expected_identity: str | None = None
    client: GateClient = field(init=False, repr=False)
    identity: str | None = field(default=None, init=False)
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

    async def _dispatch(self, task, *, previous_assignee=None):
        assert self.identity is not None  # Authenticated before listener handoff.
        if task['assignee'] == self.identity:
            if task['status'] not in ('ready', 'in_progress'):
                return
        elif previous_assignee != self.identity:
            return
        task_id = task['id']
        delivery = task_event_route(self.client.base_url, self.identity, task_id, task['execution_revision'])
        await self._submit(f'Update your task {task_id}', delivery,
                           datetime.fromisoformat(task['execution_updated_at'].replace('Z', '+00:00')))

    async def _submit(self, text, delivery, timestamp):
        source = self.build_source(chat_id=delivery, chat_type='service', user_id=None, message_id=delivery)
        await self.handle_message(MessageEvent(text=text, source=source, message_id=delivery,
            timestamp=timestamp, internal=False, allow_gateway_control=False))

    async def reconcile_assigned(self):
        # One snapshot after subscription, not periodic task polling.
        after = ''
        while True:
            page = await asyncio.to_thread(self.client.rpc, 'list_tasks', assignee=self.identity, after=after)
            for task in page['items']:
                await self._dispatch(task)
            if not page['has_more']:
                break
            after = page['next_cursor']

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
                            notice = json.loads('\n'.join(data))
                            task = await asyncio.to_thread(self.client.rpc, 'read_resource', id=notice['id'])
                            await self._dispatch(task, previous_assignee=notice.get('previous_assignee'))
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
        await self._submit(f'Gate connection interrupted ({error_type}). Diagnose if useful; do not repeat uncertain effects.',
                           delivery, datetime.now(timezone.utc))

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        # Pre-agent admission failures and a Gate outage cannot be reported by an agent to Gate.
        from agent.redact import redact_sensitive_text
        safe = redact_sensitive_text(content.replace(self.client.token, '<redacted>'), force=True, redact_url_credentials=True)
        log.warning('Gate native notice [%s]: %s', chat_id, safe)
        return SendResult(success=True, message_id='native-presentation')

    async def get_chat_info(self, chat_id):
        return {'name': 'Gate task', 'type': 'service', 'id': chat_id}
