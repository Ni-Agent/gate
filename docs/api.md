# REST quick reference

Send `Authorization: Bearer <token>` with API requests. Keep the token private and out of command arguments. `GET /whoami` identifies the credential; `GET /identities` lists identities. All authenticated identities have equal access.

Each write requires a fresh `Idempotency-Key` for the intended operation. After a timeout, retry the identical request with the same key. Use a new key for a new or changed operation.

## Knowledge

- `GET /kb` lists notes. `GET /kb/projects/example` reads a note and returns an `ETag` header.
- To create a note, `PUT /kb/projects/example` with `Content-Type: text/markdown`, `If-None-Match: *`, an idempotency key, and the full Markdown body. Success is `204`.
- To update it, read the current note and send its exact `ETag` as `If-Match` with a full replacement body and a new idempotency key. A `412` means it changed; read it again and reconcile. REST does not accept `If-Match: *` for creation. In the MCP `write_workspace` tool, use the `etag` value `*` to create instead.

## Tasks

`POST /tasks` accepts JSON containing only `project`, `title`, `description`, `responsible`, and `status`. `title` and a registered `responsible` identity are required; omitted status defaults to `open`. For example:

```json
{"project":"example","title":"Review notes","responsible":"agent:hermes"}
```

Send `Content-Type: application/json` and an idempotency key. Success returns `201` and the created task. Fields such as `id`, `run_id`, `updated_by`, and `updated_at` are server-managed and must not be sent in task writes. `GET /tasks` lists tasks; `GET /tasks/{id}` returns a task and its ETag. `PUT /tasks/{id}` replaces the writable fields using that ETag in `If-Match` and a new idempotency key. See [Gate and Hermes](hermes-events.md) for task events and claims.
