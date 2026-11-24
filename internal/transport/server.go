// Package transport exposes authenticated HTTP and MCP, without task execution.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gate-alpha/internal/core"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const Version = "v0.0.2-alpha.1-test"

// Options are operational resource constraints, not content model limits.
// Zero disables the cap. Binary uploads/downloads are streamed regardless.
type Options struct {
	MaxJSONBytes   int64
	MaxBinaryBytes int64
}

type operation struct {
	fields, required string
	description      string
}

var operations = map[string]operation{
	"read_resource":                {"id", "id", "Read current resource only; historical content uses read_revision."},
	"read_revision":                {"id revision", "id revision", "Read an exact numbered text/binary revision, including Gone metadata."},
	"create_text":                  {"id body tags", "id body", "Create mutable revision one with optional explicit tags."},
	"update_text":                  {"id body expected_revision expected_hash", "id body expected_revision expected_hash", "Edit current bytes in place, without creating a revision; conditional restore uses tombstone token."},
	"create_binary":                {"id mime tags", "id mime", "Prepare authenticated POST /binary octet-stream instructions. This tool does NOT upload bytes or create a resource."},
	"update_binary":                {"id mime expected_revision expected_hash", "id mime expected_revision expected_hash", "Prepare authenticated PUT /binary instructions. This tool does NOT upload bytes or mutate a resource."},
	"seal_revision":                {"id expected_revision expected_hash", "id expected_revision expected_hash", "Seal current and create next mutable copy, inheriting preceding tags. No replacement tag set."},
	"delete_resource":              {"id revision expected_revision expected_hash", "id expected_revision expected_hash", "Tombstone one content revision. Tasks/tags reject deletion; no cascade."},
	"create_tag":                   {"id key value description_id", "id key value", "Create a stable tag with optional text description."},
	"update_tag":                   {"id key value description_id expected_token", "id expected_token", "Conditional tag metadata edit; null description clears it."},
	"tag_resource":                 {"resource_id tag_id revision expected_revision expected_hash", "resource_id tag_id", "Attach a direct tag; current content requires expected_revision and expected_hash. Historical selectors do not change bytes."},
	"untag_resource":               {"resource_id tag_id revision expected_revision expected_hash", "resource_id tag_id", "Detach direct tag; current content requires expected_revision and expected_hash. Retain temporal membership."},
	"create_task":                  {"id operation_id assignee body status priority tags verification", "operation_id assignee body", "Idempotent task create with required registered assignee, immutable creator, advisory verification references. Gate does not enforce execution of selected checks."},
	"update_task":                  {"id expected_token assignee body status priority add_session", "id expected_token", "Atomic task metadata/body/session update. Creator and final verification selection immutable; duplicate same-task session conflicts."},
	"list_revisions_for_resource":  {"id after limit include_deleted", "id", "Compact revision index; ascending number cursor. Tombstones omitted unless include_deleted."},
	"list_tags_for_resource":       {"id revision as_of after limit", "id", "Direct tags with current labels; optional exact revision and as_of membership time. Revision defaults to current NOW, not historical head resolution."},
	"list_resources_for_tag":       {"tag_id all_tags types after limit include_deleted", "tag_id", "Direct members only, latest-before-tag matching; all_tags intersection; tombstones omitted unless include_deleted."},
	"list_tags":                    {"id key value tagged_with match after limit", "", "Compact exact tag/project discovery and case-insensitive ID/key/value prefix matching, never body search."},
	"list_tasks":                   {"identity assignee creator modified_by status after limit", "", "Compact task query; identity matches any creator/assignee/editor, other filters intersect."},
	"list_notification_history":    {"identity task_id from to after limit", "", "Post-commit ~24h notification diagnostics. Not audit, replay or delivery receipt; absence is not proof of no mutation."},
	"list_activity":                {"from to after limit", "from to", "Retained occurrence discovery over half-open RFC3339 interval; compact current logical state."},
	"list_sessions_for_task":       {"task_id after limit", "task_id", "Append-only registrations newest-first, diagnostic not liveness."},
	"list_verification_candidates": {"seed_tags", "seed_tags", "Project-tag seed; snapshot breadth-first parent-to-child tag traversal ordered by tag ID with cycle deduplication. Text candidates classified verification; selected sealed pins/live references are advisory guidance, not mandatory checks."},
}

func Schema(name string) map[string]any {
	spec := operations[name]
	properties := map[string]any{}
	for _, field := range strings.Fields(spec.fields) {
		typ := "string"
		switch field {
		case "include_deleted":
			typ = "boolean"
		case "revision", "expected_revision", "priority", "limit":
			typ = "integer"
		case "tags", "all_tags", "types", "seed_tags", "verification":
			typ = "array"
		}
		p := map[string]any{"type": typ}
		if typ == "array" {
			p["items"] = map[string]any{"type": "string"}
			if field == "verification" {
				p["items"] = map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "string"}, "revision": map[string]any{"type": "integer", "minimum": 1}}, "required": []string{"id"}, "additionalProperties": false}
			}
		}
		if field == "description_id" {
			p["type"] = []string{"string", "null"}
		}
		if field == "revision" || field == "expected_revision" {
			p["minimum"] = 1
		}
		if field == "limit" {
			p["minimum"] = 1
			p["maximum"] = 200
		}
		properties[field] = p
	}
	required := strings.Fields(spec.required)
	if required == nil {
		required = []string{}
	}
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}
func validate(name string, args map[string]any) error {
	spec, ok := operations[name]
	if !ok {
		return &core.Error{Status: 404, Message: "unknown operation"}
	}
	allowed := map[string]bool{}
	for _, k := range strings.Fields(spec.fields) {
		allowed[k] = true
	}
	for _, k := range strings.Fields(spec.required) {
		if _, ok := args[k]; !ok {
			return &core.Error{Status: 400, Message: "missing argument: " + k}
		}
	}
	for k, v := range args {
		if !allowed[k] {
			return &core.Error{Status: 400, Message: "unknown argument: " + k}
		}
		switch k {
		case "include_deleted":
			if _, ok := v.(bool); !ok {
				return &core.Error{Status: 400, Message: "boolean required: " + k}
			}
		case "revision", "expected_revision", "limit", "priority":
			n, ok := v.(float64)
			if !ok || n != float64(int(n)) {
				return &core.Error{Status: 400, Message: "integer required: " + k}
			}
			if (k == "revision" || k == "expected_revision") && n < 1 {
				return &core.Error{Status: 400, Message: "positive revision required"}
			}
			if k == "limit" && (n < 1 || n > 200) {
				return &core.Error{Status: 400, Message: "limit must be 1..200"}
			}
		case "tags", "all_tags", "types", "seed_tags":
			vv, ok := v.([]any)
			if !ok {
				return &core.Error{Status: 400, Message: "array required: " + k}
			}
			if len(vv) > 200 {
				return &core.Error{Status: 400, Message: "too many array items"}
			}
			for _, x := range vv {
				if _, ok := x.(string); !ok {
					return &core.Error{Status: 400, Message: "string array required: " + k}
				}
			}
		case "verification":
			vv, ok := v.([]any)
			if !ok {
				return &core.Error{Status: 400, Message: "reference array required"}
			}
			for _, x := range vv {
				if _, ok := x.(map[string]any); !ok {
					return &core.Error{Status: 400, Message: "reference object required"}
				}
			}
		case "description_id":
			if v != nil {
				if _, ok := v.(string); !ok {
					return &core.Error{Status: 400, Message: "description_id must be string or null"}
				}
			}
		default:
			if _, ok := v.(string); !ok {
				return &core.Error{Status: 400, Message: "string required: " + k}
			}
		}
	}
	return nil
}
func errorMap(err error) (int, map[string]any) {
	var e *core.Error
	if errors.As(err, &e) {
		m := map[string]any{"status": e.Status, "error": e.Message}
		if e.Resource != nil {
			m["resource"] = e.Resource
		}
		return e.Status, m
	}
	return 500, map[string]any{"status": 500, "error": "internal store failure"}
}
func decorate(m map[string]any) map[string]any {
	if m != nil && m["type"] == "bn" {
		m["download_path"] = "/binary?id=" + url.QueryEscape(m["id"].(string)) + "&revision=" + fmt.Sprint(m["revision"])
	}
	return m
}
func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, err error) {
	status, m := errorMap(err)
	if status == 401 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="gate-test"`)
	}
	jsonResponse(w, status, m)
}
func bad(status int, msg string) error { return &core.Error{Status: status, Message: msg} }
func decode(body io.Reader) (map[string]any, error) {
	raw, e := readJSONWire(body)
	if e != nil {
		return nil, e
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	var a map[string]any
	if e := d.Decode(&a); e != nil || a == nil {
		var max *http.MaxBytesError
		if errors.As(e, &max) {
			return nil, e
		}
		return nil, bad(400, "JSON object required")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, bad(400, "one JSON object required")
	}
	return a, nil
}

// New returns an embeddable handler, not a listener. Tokens are runtime inputs;
// all configured identities share this alpha's trusted-workspace authorization.
func New(store *core.Store, tokens map[string]string) http.Handler {
	return NewWithOptions(store, tokens, Options{})
}
func NewWithOptions(store *core.Store, tokens map[string]string, options Options) http.Handler {
	copied := map[string]string{}
	servers := map[string]*mcp.Server{}
	for token, id := range tokens {
		if token == "" {
			continue
		}
		copied[token] = id
		if servers[id] == nil {
			servers[id] = NewMCP(store, id)
		}
	}
	sdkLimit := options.MaxJSONBytes
	if sdkLimit <= 0 {
		sdkLimit = int64(^uint64(0) >> 1)
	} // zero would restore SDK default
	mcpHTTP := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server { return servers[r.Context().Value(identityKey{}).(string)] }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true, MaxRequestBodyBytes: sdkLimit})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		if r.Header.Get("Origin") != "" {
			writeError(w, bad(403, "browser-origin access not enabled"))
			return
		}
		for _, k := range []string{"X-Gate-Actor", "X-Gate-Identity", "X-Acting-Identity"} {
			if r.Header.Get(k) != "" {
				writeError(w, bad(400, "acting identity cannot be supplied by client"))
				return
			}
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, bad(401, "authentication required"))
			return
		}
		actor, ok := copied[strings.TrimPrefix(auth, "Bearer ")]
		if !ok {
			writeError(w, bad(401, "invalid authentication"))
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), identityKey{}, actor))
		switch {
		case r.URL.Path == "/whoami":
			if r.Method != http.MethodGet {
				writeError(w, bad(405, "GET required"))
				return
			}
			jsonResponse(w, 200, map[string]any{"identity": actor, "protocol": "gate-alpha-native-1"})
		case r.URL.Path == "/mcp":
			// Guard BEFORE SDK decoding/schema processing can normalize Unicode.
			// GET/SSE and output framing remain entirely SDK-owned.
			if r.Method == http.MethodPost {
				raw, e := readJSONWireLimit(r.Body, options.MaxJSONBytes)
				if e != nil {
					status, _ := errorMap(e)
					code := -32700
					if status == 413 {
						code = -32600
					}
					jsonResponse(w, status, map[string]any{"jsonrpc": "2.0", "id": nil, "error": map[string]any{"code": code, "message": e.Error()}})
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(raw))
			}
			mcpHTTP.ServeHTTP(w, r)
		case r.URL.Path == "/events":
			events(store, actor, w, r)
		case r.URL.Path == "/binary":
			binary(store, actor, w, r, options.MaxBinaryBytes)
		case strings.HasPrefix(r.URL.Path, "/rpc/"):
			if r.Method != http.MethodPost {
				writeError(w, bad(405, "POST required"))
				return
			}
			if typ, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); typ != "application/json" {
				writeError(w, bad(415, "application/json required"))
				return
			}
			op := strings.TrimPrefix(r.URL.Path, "/rpc/")
			if op == "create_binary" || op == "update_binary" {
				writeError(w, bad(415, "use authenticated octet-stream /binary route"))
				return
			}
			var limited io.Reader = r.Body
			if options.MaxJSONBytes > 0 {
				limited = http.MaxBytesReader(w, r.Body, options.MaxJSONBytes)
			}
			a, e := decode(limited)
			if e != nil {
				var max *http.MaxBytesError
				if errors.As(e, &max) {
					e = bad(413, "request too large")
				}
				writeError(w, e)
				return
			}
			if e = validate(op, a); e != nil {
				writeError(w, e)
				return
			}
			m, e := store.Call(r.Context(), actor, op, a)
			if e != nil {
				writeError(w, e)
				return
			}
			if token, ok := m["token"].(string); ok {
				w.Header().Set("ETag", strconv.Quote(token))
			}
			jsonResponse(w, 200, decorate(m))
		default:
			writeError(w, bad(404, "unknown route"))
		}
	})
}

type identityKey struct{}

func parseRevision(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	n, e := strconv.Atoi(raw)
	if e != nil || n < 1 || strconv.Itoa(n) != raw {
		return 0, bad(400, "canonical positive revision required")
	}
	return n, nil
}
func binary(store *core.Store, actor string, w http.ResponseWriter, r *http.Request, limit int64) {
	if r.Method == http.MethodGet {
		for k, v := range r.URL.Query() {
			if (k != "id" && k != "revision") || len(v) != 1 {
				writeError(w, bad(400, "unknown or repeated binary query field"))
				return
			}
		}
		rev, e := parseRevision(r.URL.Query().Get("revision"))
		if e != nil {
			writeError(w, e)
			return
		}
		b, m, e := store.OpenBinary(r.Context(), actor, r.URL.Query().Get("id"), rev)
		if e != nil {
			writeError(w, e)
			return
		}
		defer b.Close()
		st, e := b.Stat()
		if e != nil {
			writeError(w, e)
			return
		}
		if expected := strings.Trim(r.Header.Get("If-Match"), `"`); expected != "" && expected != m["token"] {
			writeError(w, bad(412, "binary content changed"))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Gate-Mime", m["mime"].(string))
		w.Header().Set("X-Gate-ID", m["id"].(string))
		w.Header().Set("X-Gate-Revision", fmt.Sprint(m["revision"]))
		w.Header().Set("ETag", strconv.Quote(m["token"].(string)))
		w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
		w.Header().Set("Content-Disposition", `attachment; filename="gate-binary"`)
		w.WriteHeader(200)
		if _, e := io.Copy(w, b); e != nil {
			panic(http.ErrAbortHandler)
		}
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeError(w, bad(405, "GET, POST or PUT required"))
		return
	}
	if typ, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); typ != "application/octet-stream" {
		writeError(w, bad(415, "application/octet-stream required"))
		return
	}
	id := r.Header.Get("X-Gate-ID")
	media := r.Header.Get("X-Gate-Mime")
	typ, e := binaryMetadata(id, media, nil)
	if e != nil {
		writeError(w, e)
		return
	}
	if limit > 0 && r.ContentLength > limit {
		writeError(w, bad(413, "binary exceeds configured limit"))
		return
	}
	var source io.Reader = r.Body
	if limit > 0 {
		source = http.MaxBytesReader(w, r.Body, limit)
	}
	// Sniff at most 512 bytes, then stream that prefix and the remaining reader.
	prefix := make([]byte, 512)
	n, e := io.ReadFull(source, prefix)
	if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
		writeError(w, uploadError(e))
		return
	}
	prefix = prefix[:n]
	detected := strings.Split(http.DetectContentType(prefix), ";")[0]
	switch typ {
	case "image/png", "image/jpeg", "image/gif", "application/pdf":
		if typ != detected {
			writeError(w, bad(400, "declared MIME does not match bytes"))
			return
		}
	}
	reader := &uploadReader{reader: io.MultiReader(bytes.NewReader(prefix), source), expected: r.ContentLength}
	a := map[string]any{"id": id, "mime": media, "content": reader}
	op := "create_binary"
	if r.Method == http.MethodPut {
		op = "update_binary"
		rev, e := parseRevision(r.Header.Get("X-Gate-Revision"))
		if e != nil {
			writeError(w, e)
			return
		}
		token := strings.Trim(r.Header.Get("If-Match"), `"`)
		if rev == 0 || token == "" {
			writeError(w, bad(428, "X-Gate-Revision and If-Match required"))
			return
		}
		if r.Header.Get("X-Gate-Tags") != "" {
			writeError(w, bad(400, "update cannot replace tags"))
			return
		}
		a["expected_revision"] = rev
		a["expected_hash"] = token
	} else {
		if r.Header.Get("X-Gate-Revision") != "" || r.Header.Get("If-Match") != "" {
			writeError(w, bad(400, "create cannot carry update preconditions"))
			return
		}
		if raw := r.Header.Get("X-Gate-Tags"); raw != "" {
			if len(raw) > 4096 {
				writeError(w, bad(400, "tag header too long"))
				return
			}
			var tags []string
			if json.Unmarshal([]byte(raw), &tags) != nil {
				writeError(w, bad(400, "X-Gate-Tags must be JSON string array"))
				return
			}
			if _, e := binaryMetadata(id, media, tags); e != nil {
				writeError(w, e)
				return
			}
			a["tags"] = tags
		}
	}
	m, e := store.Call(r.Context(), actor, op, a)
	if e != nil {
		writeError(w, e)
		return
	}
	w.Header().Set("ETag", strconv.Quote(m["token"].(string)))
	jsonResponse(w, 200, decorate(m))
}
func events(store *core.Store, actor string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, bad(405, "GET required"))
		return
	}
	ch, cancel := store.Subscribe(actor)
	defer cancel()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, bad(500, "streaming unavailable"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	// Subscription installed before ready. Caller snapshots assigned tasks AFTER ready,
	// then consumes buffered invalidations; overflow closes this stream for reset.
	if _, e := io.WriteString(w, "event: ready\ndata: {\"reconcile_assigned\":true}\n\n"); e != nil {
		return
	}
	flusher.Flush()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(e)
			if _, err := fmt.Fprintf(w, "event: task.updated\ndata: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, e := io.WriteString(w, ": transport-health\n\n"); e != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// NewMCP creates a real SDK server bound to one already authenticated identity.
// The stdio command's local process boundary supplies this identity; HTTP selects
// it only from the validated bearer mapping, never from model arguments.
func NewMCP(store *core.Store, actor string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "gate-alpha-test", Version: Version}, nil)
	for name, spec := range operations {
		server.AddTool(&mcp.Tool{Name: name, Description: spec.description, InputSchema: Schema(name)}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			a, e := decode(bytes.NewReader(req.Params.Arguments))
			if e == nil {
				e = validate(name, a)
			}
			var m map[string]any
			if e == nil {
				if name == "create_binary" || name == "update_binary" {
					m, e = prepareBinary(name, a)
				} else {
					m, e = store.Call(ctx, actor, name, a)
				}
			}
			result := &mcp.CallToolResult{}
			if e != nil {
				_, m = errorMap(e)
				result.IsError = true
			}
			m = decorate(m)
			b, _ := json.Marshal(m)
			result.Content = []mcp.Content{&mcp.TextContent{Text: string(b)}}
			result.StructuredContent = m
			return result, nil
		})
	}
	return server
}
