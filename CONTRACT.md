# Gate v0.0.2-alpha.1 isolated implementation contract

RECOVERY NOTE: The original component contract below is preserved as historical
partial-work context, not current authority. The latest 11 BINDING DECISIONS in
task b83ab60980aca99730d57969 supersede conflicting paragraphs (notably current
association hash checks, durable object publication before metadata transaction,
grace-period GC and online backup, parent-to-child verification discovery,
required immutable final selection, default tombstone omission, activity actor
and matched time, prefix matching, and installed-client serial acceptance).
The manual recovery instruction permits bounded disposable localhost E2E tests
with synthetic auth and guaranteed cleanup, and scoped local commits; it forbids
all shared Gate writes/run claims by this subordinate. See REPORT.md for the
tested final behavior and limitations. No remote design/history was edited.

Task b83ab60980aca99730d57969, run 02b5bcebc1bb421387a99a9896314752. TEST ONLY. This tree did not exist at initial inspection. No production installation, deployment, gateway restart, credential changes, public push or new listener is authorized. Tests must use in-process handlers or stdio, not listen on TCP/Unix sockets. No further tasks. Do not access production credentials/config. No Gate writes from delegated workers. Parent handles reporting and commits.

Authority: design /kb/projects/gate/ideas/vnext-consolidated-design-2026-11-22 plus decisions F01-F16 and M1-M5 in task b65cf0506062d8ba80c68ead. Task explicitly authorizes isolated implementation despite historical review-only labels. Exactly tx/bn/tk/tg. SQLite metadata only; bytes on disk. Do not copy old task validation, execution/lease engine, body search or historical notes.

## Component interface (fixed for parallel authors)

Module `gate-alpha`, Go 1.26.10. Package `gate-alpha/internal/core` owns storage. Package `gate-alpha/internal/transport` owns HTTP/MCP. `client/` owns a Python transport-only client. No shared implementation files between authors.

Core exports:

    type Error struct { Status int; Message string; Resource map[string]any }
    func (e *Error) Error() string
    func New(dir string, identities []string) (*Store, error)
    func (s *Store) Close() error
    func (s *Store) Call(ctx context.Context, actor, operation string, args map[string]any) (map[string]any, error)
    func (s *Store) ReadBytes(ctx context.Context, actor, id string, revision int) ([]byte, map[string]any, error)
    type Event struct { Type string `json:"type"`; ID string `json:"id"`; Assignee string `json:"assignee"`; PreviousAssignee string `json:"previous_assignee,omitempty"` }
    func (s *Store) Subscribe(identity string) (<-chan Event, func())

All inputs from JSON numbers may be float64. Errors use Status 400/401/403/404/409/410/412/413/428/500 as appropriate. 410 Resource contains compact tombstone metadata and token. Return maps use snake_case. Empty lists serialize as [] not null. Each map resource includes id,type,created_at,updated_at,token; text/binary also revision,content_hash,mime,sealed,deleted_at (null when live). Text/task reads include body; binary reads metadata only. Tag reads include key,value,description_id and description_state=none|available|unavailable|too_large; available description includes id,revision,token,body. Too large returns explicit read_resource continuation ID, no silent truncation. Task reads creator,assignee,modified_by,status,priority,verification and latest_session|null; never full session history.

Store.Call operations and args:

- read_resource(id); read_revision(id,revision). Current read rejects pinned IDs; historical read uses separate positive integer revision. Unknown 404, deleted 410 without fallback.
- create_text(id,body,tags?); update_text(id,body,expected_revision,expected_hash). UTF-8 text; cap 1MiB.
- create_binary(id,content []byte,mime,tags?); update_binary(id,content []byte,mime,expected_revision,expected_hash). content is INTERNAL ONLY, raw bytes from HTTP, never public JSON/MCP base64. Cap 8MiB. ReadBytes authenticates registered actor, resolves metadata, 410 rules and returns bytes. Binary metadata read may include download route supplied by transport, not bytes.
- seal_revision(id,expected_revision,expected_hash): atomically seal current and create one mutable next copy inheriting active tags at seal; no tags argument. Reject tombstone; same old revision/hash retry 412 even identical byte hashes. Current update changes no revision; historical update impossible. Hash SHA256 over raw bytes; explicit expected_revision protects implicit-latest retargeting. Metadata token is SHA256 canonical state for tag/task/tombstone. Missing preconditions 428, stale 412.
- delete_resource(id,revision?,expected_revision,expected_hash): content only. Tasks/tags explicitly reject 405, no cascade. Tombstone retains MIME/token; current update restores same number using expected_hash=tombstone token. Historical delete allowed with explicit revision and its expected hash. No historical restoration. Tombstones included as compact marked entries in lists; no fallback. Seal on tombstone 409.
- list_revisions_for_resource(id,after?,limit?); list_tags_for_resource(id,revision?,as_of?,after?,limit?); list_resources_for_tag(tag_id,all_tags? [],types? [],after?,limit?); list_tags(key?,tagged_with?,match?,after?,limit?); list_tasks(identity?,assignee?,creator?,modified_by?,status?,after?,limit?); list_activity(from,to,after?,limit?); list_sessions_for_task(task_id,after?,limit?). Lists return {items:[],next_cursor:null|string,has_more:bool}. Default limit 50, maximum 200. after uses last ID (revisions/sessions may document their deterministic cursor). Direct-only tag intersection, highest revision chosen BEFORE matching tags. Compact SQL metadata selection, never load bodies to filter. list_tasks identity matches any creator/assignee/modified_by; other filters AND. Activity half-open RFC3339 UTC instants, all committed resource changes incl historical tagging and all task edits; retain occurrence rows so yesterday edit survives today edit, but no old body history.
- create_tag(id,key,value,description_id?); update_tag(id,expected_token,key?,value?,description_id? null allowed). Descriptions nullable and must reference existing tx; on later unavailable/deleted description tag read stays 200. Description output cap 64KiB with explicit continuation. Current labels on historical temporal membership.
- tag_resource(resource_id,tag_id,revision?,expected_revision?); untag_resource(same): independent content-free operations, historical revisions allowed. Versioned implicit-current target requires expected_revision to prevent retarget; explicit revision targets exact version. Existing attach/missing detach are no-op. Preserve repeated attach/detach intervals and one active edge. Tags-on-tags/cycles allowed. Creation optional tags atomically validated/attached.
- list_verification_candidates(seed_tags []): deterministic recursive traversal. Follow tags directly classifying each seed (outward towards classifications/ancestors), visited-set cycle-safe. Gather live tx resources directly classified by visited tags AND a tag with key=verification,value=instructions. Return compact references sorted by ID; no automatic execution. This convention is a reviewable OPEN choice.
- create_task(operation_id,assignee,body,status?,priority?,tags?,verification []): caller supplies constrained gate:tk ID optionally, otherwise deterministic suffix from actor-scoped operation ID digest. Immutable creator=actor; required registered assignee; modified_by=actor. Exactly triage|ready|in_progress|blocked|done (default triage). priority integer (default 0). Mandatory nonempty operation_id <=128 chars, durable actor-scoped fingerprint prevents conflicting reuse (409), identical retry returns same task current representation without duplicate. Final verification list explicit (default empty), each {id,revision?}: no revision=live; supplied revision must be sealed/live bytes. Store final set in SQLite; future tag/default edits never rewrite it. Selected instructions advisory; unavailable/conflicts must be reported by executor, Gate never executes checks.
- update_task(id,expected_token,assignee?,body?,status?,priority?,add_session?,verification?): all supplied fields/session/editor/activity transactionally atomic; reject creator mutation and all unknown fields. Session opaque <=1024 chars, duplicate same-task 409 rolls back EVERY other change. Same ref across tasks allowed. Session list deterministic newest-first, tie by row seq; read exposes only latest_session={ref,registered_at}|null. No task revisions, claims, leases, runs, receipts for arbitrary mutations, execution state or polling. Append-session is part of update only.

## Storage and reliability

Use modernc.org/sqlite v1.34.5 already cached. Narrow direct SQL tables/constraints and WAL. Register supplied identities in metadata at initial New; validate all actor/assignee identifiers (human:slug or agent:slug). DB private; store cannot follow untrusted resource paths. Blob SHA256 paths internal only. Row-first means INSERT/UPDATE metadata inside UNCOMMITTED transaction first, then stage/write/fsync immutable bytes, publish/fsync directory, then COMMIT live pointer/metadata/activity/session. Rollback or injected interruption never exposes missing bytes. Keep old committed state intact until commit. Reopen verifies live pointers/digests and fails closed on missing/corrupt bytes. Orphans may remain pending conservative GC, but no hidden recoverable history API. If implementing GC, coordinate writers/readers via store lock and exclusive process/store ownership; do not promise secure erasure. Single writable Store process per directory enforced with flock; multiple concurrent reads should not be globally serialized. Document backup as stopped-store consistent copy unless tested online protocol. Add fault-injection tests for row/blob/commit boundaries and shared-byte deletion/reclamation. No automatic background GC/polling/listener.

Task notifications are best-effort invalidations AFTER commit to previous and current assignee, including body/status/session. Core Subscribe channel capacity bounded; overflow CLOSE subscription (no silent loss); cancellation safe under race detector. Transport SSE emits ready only AFTER subscription installed, then harness reconciles assigned state, consumes queued events. Reconnect reconciles; no durable replay, task polling or execution in plugin. No upstream Hermes F08 workaround; document installed connector concurrency limitation honestly.

## HTTP/MCP adapter

Export transport.New(store *core.Store, tokens map[string]string) http.Handler; map token=>registered identity. Tokens supplied in process at test invocation, not committed secrets. No OAuth replacement/production migration promised. Server binary optional cmd/gate-alpha supports stdio MCP by default; no socket listener needed or started. HTTP handler is embeddable for later isolated deployment, not live deployment.

Routes authenticated Bearer; actor comes ONLY from token mapping, reject untrusted acting identity headers:
POST /rpc/{operation} JSON args (binary create/update rejected here; use stream route)
GET /binary?id=gate:bn:slug&revision=N optional, octet-stream download, metadata headers, attachment/nosniff, no raw digest object access
POST /binary raw application/octet-stream create; PUT /binary update. Headers X-Gate-ID, X-Gate-Mime, X-Gate-Revision and If-Match (raw byte hash quoted accepted), optional X-Gate-Tags JSON array bounded to 4KiB on create only. Validate transport MIME and valid media type metadata; content sniff mismatch policy documented. No filenames trusted as paths. Bounded read and no state effects on oversize/malformed requests.
GET /events SSE for authenticated assignee, ready then task.updated; closed channel terminates stream forces reconciliation. Use request context. No new listener is started in tests.
POST /mcp Streamable HTTP SDK handler or compatible protocol; typed named tools match Call operations, schemas reject unknown fields. Binary create/update MCP tools return instructions for authenticated octet-stream (not fake transfer). SDK stdio usable with real Go MCP client (in-memory IO pipes) and/or Python MCP client through built subprocess. No custom scheduling workaround.

Transport tests use httptest.NewRecorder/Handler.ServeHTTP, SDK in-memory transports or net.Pipe (NOT httptest.NewServer). Verify auth/forged identity, binary byte exact create/update/seal/historical/delete/410/restore/limits, task ready+assignment SSE invalidations and reconcilable list, real MCP client current/historical flows, errors/schema. No external credentials needed.

## Ownership

core author: internal/core/** only and docs/core.md; may add own tests. No go.mod changes (request parent if needed).
transport author: internal/transport/**, cmd/gate-alpha/**, docs/transport.md; may add own tests. Go dependencies fixed SDK v1.8.0 already cached; no go.mod changes.
client author: client/**, docs/client.md; a dependency-free Python thin transport client with injected authenticated opener and notification invalidation callback, no durable state/execution/retry lifecycle. Optional native-plugin shim isolated under client only; no installation. Test using in-process HTTP bridge or fake opener+real Go command stdio; never start listener. No dependency/config/live source edits.
Parent: module files, integration tests, report, evidence, commits, Gate writes.
