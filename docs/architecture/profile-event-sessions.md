# Fresh profile event sessions (prototype)

Status: implementation spike, not run or deployed. Supersedes the fixed task route only on this branch. No Gate schema change or Hermes core patch.

## First message vs. message to an existing session

- **Gate task notification:** the authenticated Gate connector uses `BasePlatformAdapter.handle_message(MessageEvent(...))` to submit `Update your task <task-id>` to the receiving Hermes profile. The adapter uses a synthetic `chat_id` derived from Gate origin, recipient identity, task ID and execution revision. A different revision gets a fresh route. Reconnect and repeated hints for the same revision use the same chat ID, message ID, text and timestamp so Hermes can deduplicate. Gate sends invalidations, not an ordered event log; fast edits can collapse onto the latest revision.
- **App/peer message:** reuse Hermes's authenticated A2A endpoint for the receiving profile, without a `contextId` for a fresh conversation. The app maps `hermes:prometheus` to the endpoint or the configured agent path and owns the room, membership and message correlation. The A2A result goes back to the app, not to Gate's task adapter. No app rooms or Gate message storage are implemented in this prototype.
- **Peer session message:** `send_to_session(session_key, text)` calls the live plugin context's `inject_message(text, role='peer', session_key=...)`. It neither looks up tasks nor switches profiles. It reports `scheduled`, not delivered or answered. Use only in a profile that intentionally lets its agents send text to other sessions in that profile. Hermes still owns target authorisation and admission.

Example app request body for `POST /` on a dedicated Prometheus [A2A endpoint](https://hermes-agent.nousresearch.com/docs/user-guide/messaging/a2a#inbound-being-callable), server-to-server with its configured bearer credential (not the Gate token):

```json
{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"project-message-42","role":"ROLE_USER","parts":[{"text":"Message from grok-bot:mai: Should projects X and Y share the Z library?"}]}}}
```

The app authenticates that sender before attaching the label. The Hermes A2A auth identity is the app's peer identity unless the originating peer authenticates directly. Do not assume that resending this first request to a local/default endpoint deduplicates it; retry behavior is endpoint-specific.

## Session references in Gate

Example `chat_id`: `gate-v3-a1b2…`. Example gateway `session_key`: `agent:prometheus:gate:service:gate-v3-a1b2…` (multiplexed gateway; standalone can use `agent:main`). A key routes to the current conversation for that chat. `session_id` is the stored conversation/transcript ID, not an individual turn. Several turns can share an ID. A reset on a messaging route can open a new transcript under the same key.

1. Read the task with `gate_call("read_resource", {"id": ...})`; check current assignment/state before effects.
2. Use `list_sessions_for_task` to find registered references. `current_session_address()` produces `{session_key, session_id}` for the running conversation. If taking up the task, serialize that result as the string `add_session` in the existing `update_task` conditional request. This is a convention on top of the opaque existing Gate reference field, not a new RPC. Legacy refs may be plain IDs and may not be addressable by key.
3. If prior context is all that is needed, call `session_search(session_id=...)`; that reads saved messages, not an in-flight model prompt or the current state of a tool call. If prior work should hear about the change, send text by `session_key`. Gate refs are hints, not proof of liveness, ownership or permission.

Enable injection explicitly in the target profile's Hermes configuration, using the actual plugin registration ID (shown here for `gate-alpha-native`):

```yaml
plugins:
  entries:
    gate-alpha-native:
      allow_gateway_injection: true
```

## What changed operationally

Removed the remembered task set, the fixed per-task chat and automatic `/stop`. On live reassignment, the prior assignee gets a fresh update when the SSE hint names it; the new assignee receives its own update if the task is actionable. Reconnect snapshots only recover currently assigned/actionable tasks, not notifications for tasks transferred away while disconnected. A session's already-running tools are not cancelled on reassignment. Fresh sessions can run in parallel; task registration and CAS do not make external effects exclusive.

Kept profile-scoped authentication/account pin, reconnect snapshot, one short diagnostic conversation per outage, and Hermes native admission/queue rules. No new queue, session registry, profile router or synchronous request/reply mechanism was added. Plugin replies to Gate service events still use diagnostic logging; regular app replies use the app's A2A request.

First live check later: verify that `inject_message` called from a fresh Gate turn reaches another Gate service route and starts its next turn. A positive scheduling boolean alone is not evidence; route/provenance rejection can happen asynchronously. The previous baseline's acceptance harness encodes fixed task routes and automatic `/stop`; those expectations are not acceptance criteria for this spike. No tests or live provider calls ran for this implementation.
