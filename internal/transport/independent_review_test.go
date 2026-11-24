package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Regression expectations, deliberately red on the reviewed snapshot.
func TestIndependentRejectInvalidUTF8Wire(t *testing.T) {
	_, h := fixture(t)
	raw := append([]byte(`{"id":"gate:tx:invalid-utf8","body":"`), 0xff)
	raw = append(raw, []byte(`"}`)...)
	w := request(t, h, "POST", "/rpc/create_text", raw, map[string]string{"Content-Type": "application/json"})
	if w.Code != 400 {
		t.Errorf("malformed UTF-8 accepted: status=%d body=%s", w.Code, w.Body.String())
	}
	code, got := rpc(t, h, "read_resource", map[string]any{"id": "gate:tx:invalid-utf8"})
	if code != 404 {
		t.Errorf("malformed request published resource: status=%d body=%q", code, got["body"])
	}
}

func TestIndependentBinaryMCPRejectsMalformedID(t *testing.T) {
	_, h := fixture(t)
	session := connectMCP(t, h, nil, nil)
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "create_binary", Arguments: map[string]any{"id": "gate:bn:../escape", "mime": "application/octet-stream"}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(result.StructuredContent)
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !result.IsError || got["status"] != float64(400) {
		t.Errorf("malformed ID accepted by MCP preparation tool: isError=%v result=%v", result.IsError, got)
	}
}

func TestIndependentUnicodeLabelPrefixCaseFold(t *testing.T) {
	_, h := fixture(t)
	okRPC(t, h, "create_tag", map[string]any{"id": "gate:tg:unicode-label", "key": "project", "value": "École"})
	code, got := rpc(t, h, "list_tags", map[string]any{"match": "éC"})
	if code != 200 {
		t.Fatal(code, got)
	}
	if len(got["items"].([]any)) != 1 {
		t.Errorf("case-insensitive Unicode label prefix failed: %v", got)
	}
}

func TestIndependentMCPAcceptsLegalEscapedFormerMaxText(t *testing.T) {
	_, h := fixture(t)
	body := strings.Repeat("\x00", (1 << 20))
	// encoding/json emits six wire bytes for each NUL; raw body remains valid 1MiB UTF-8.
	args := map[string]any{"id": "gate:tx:max-escaped-rpc", "body": body}
	code, _ := rpc(t, h, "create_text", args)
	if code != 200 {
		t.Fatalf("control RPC rejected in-cap body: %d", code)
	}
	args["id"] = "gate:tx:max-escaped-mcp"
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": "create_text", "arguments": args}})
	r := httptest.NewRequest("POST", "https://test.invalid/mcp", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer fixture-token-hermes")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	r.Header.Set("MCP-Protocol-Version", "2025-06-18")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("valid %d-byte text, %d-byte JSON rejected by MCP only: status=%d response=%.180s", len(body), len(raw), w.Code, w.Body.String())
	}
}
