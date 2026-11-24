package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func rawMCP(t *testing.T, h http.Handler, operation string, args []byte) *httptest.ResponseRecorder {
	t.Helper()
	raw := append([]byte(`{"jsonrpc":"2.0","id":17,"method":"tools/call","params":{"name":`+fmt.Sprintf("%q", operation)+`,"arguments":`), args...)
	raw = append(raw, []byte(`}}`)...)
	return request(t, h, "POST", "/mcp", raw, map[string]string{"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-06-18"})
}
func TestReviewStrictWireUnicodeCreateAndUpdate(t *testing.T) {
	for _, route := range []string{"rpc", "mcp"} {
		t.Run(route, func(t *testing.T) {
			_, h := fixture(t)
			send := func(op string, raw []byte) *httptest.ResponseRecorder {
				if route == "mcp" {
					return rawMCP(t, h, op, raw)
				}
				return request(t, h, "POST", "/rpc/"+op, raw, map[string]string{"Content-Type": "application/json"})
			}
			invalid := [][]byte{{0xff}, []byte(`\ud800`), []byte(`\udc00`), []byte(`\ud800x`), []byte(`\ud800\u0041`), []byte(`\ud800\ud800`)}
			for i, value := range invalid {
				id := fmt.Sprintf("gate:tx:invalid-%d", i)
				raw := append([]byte(`{"id":`+fmt.Sprintf("%q", id)+`,"body":"`), value...)
				raw = append(raw, []byte(`"}`)...)
				w := send("create_text", raw)
				if w.Code != 400 {
					t.Errorf("invalid create %q accepted: %d", value, w.Code)
				}
				code, _ := rpc(t, h, "read_resource", map[string]any{"id": id})
				if code != 404 {
					t.Errorf("invalid create committed: %d", code)
				}
				m := okRPC(t, h, "create_text", map[string]any{"id": fmt.Sprintf("gate:tx:update-%d", i), "body": "preserve"})
				raw = []byte(`{"id":` + fmt.Sprintf("%q", m["id"]) + `,"expected_revision":1,"expected_hash":` + fmt.Sprintf("%q", m["token"]) + `,"body":"`)
				raw = append(raw, value...)
				raw = append(raw, []byte(`"}`)...)
				w = send("update_text", raw)
				if w.Code != 400 {
					t.Errorf("invalid update %q accepted: %d", value, w.Code)
				}
				current := okRPC(t, h, "read_resource", map[string]any{"id": m["id"]})
				if current["body"] != "preserve" || current["token"] != m["token"] {
					t.Error("invalid update changed state", current)
				}
				if route == "mcp" {
					var envelope map[string]any
					if json.Unmarshal(w.Body.Bytes(), &envelope) != nil || envelope["jsonrpc"] != "2.0" || envelope["error"] == nil {
						t.Error("MCP rejection lost protocol envelope", w.Body.String())
					}
				}
			}
			for i, value := range []string{`\ud83d\ude00`, "�", `\ufffd`, `literal \\ud800`} {
				id := fmt.Sprintf("gate:tx:valid-%d", i)
				w := send("create_text", []byte(`{"id":`+fmt.Sprintf("%q", id)+`,"body":"`+value+`"}`))
				if w.Code != 200 {
					t.Fatal("valid Unicode rejected", w.Code, w.Body.String())
				}
				got := okRPC(t, h, "read_resource", map[string]any{"id": id})
				want := []string{"😀", "�", "�", `literal \ud800`}[i]
				if got["body"] != want {
					t.Errorf("valid bytes changed: got %q want %q", got["body"], want)
				}
			}
		})
	}
}

func TestReviewUnicodePrefixLiteralAndPaging(t *testing.T) {
	_, h := fixture(t)
	for i, label := range []string{"unrelated", "École", "éCLAIR", "Économie", "100%_\\[Alpha]", "Σχολείο", "Kelvin"} {
		okRPC(t, h, "create_tag", map[string]any{"id": fmt.Sprintf("gate:tg:label-%d", i), "key": "project", "value": label})
	}
	after := ""
	seen := []string{}
	for {
		out := okRPC(t, h, "list_tags", map[string]any{"match": "éC", "after": after, "limit": 1})
		for _, item := range out["items"].([]any) {
			seen = append(seen, item.(map[string]any)["id"].(string))
		}
		if out["has_more"] != true {
			break
		}
		after = out["next_cursor"].(string)
	}
	if strings.Join(seen, ",") != "gate:tg:label-1,gate:tg:label-2,gate:tg:label-3" {
		t.Fatal("Unicode paging filtered after LIMIT or missed prefix", seen)
	}
	for _, match := range []string{"100%_\\[a", "σΧ", "kel", "GATE:TG:LABEL-6"} {
		out := okRPC(t, h, "list_tags", map[string]any{"match": match})
		if len(out["items"].([]any)) != 1 {
			t.Error(match, out)
		}
	}
	out := okRPC(t, h, "list_tags", map[string]any{"match": "100_"})
	if len(out["items"].([]any)) != 0 {
		t.Error("wildcard semantics leaked", out)
	}
}

func TestReviewJSONLimitsParity(t *testing.T) {
	for _, route := range []string{"rpc", "mcp"} {
		t.Run(route, func(t *testing.T) {
			s, h := fixture(t)
			send := func(handler http.Handler, op string, raw []byte) *httptest.ResponseRecorder {
				if route == "mcp" {
					return rawMCP(t, handler, op, raw)
				}
				return request(t, handler, "POST", "/rpc/"+op, raw, map[string]string{"Content-Type": "application/json"})
			}
			body := strings.Repeat("\x00", (1<<20)+123)
			raw, _ := json.Marshal(map[string]any{"id": "gate:tx:large", "body": body})
			w := send(h, "create_text", raw)
			if w.Code != 200 || bytes.Contains(w.Body.Bytes(), []byte(`"isError":true`)) {
				t.Fatal(w.Code)
			}
			got := okRPC(t, h, "read_resource", map[string]any{"id": "gate:tx:large"})
			if got["body"] != body {
				t.Fatal("large bytes differ")
			}
			raw, _ = json.Marshal(map[string]any{"id": got["id"], "body": body + "x", "expected_revision": got["revision"], "expected_hash": got["token"]})
			w = send(h, "update_text", raw)
			if w.Code != 200 || bytes.Contains(w.Body.Bytes(), []byte(`"isError":true`)) {
				t.Fatal(w.Code)
			}
			bounded := NewWithOptions(s, map[string]string{"fixture-token-hermes": "agent:hermes"}, Options{MaxJSONBytes: 1024})
			raw, _ = json.Marshal(map[string]any{"id": "gate:tx:limited", "body": strings.Repeat("x", 2048)})
			if w = send(bounded, "create_text", raw); w.Code != 413 {
				t.Fatal("configured bound missing", w.Code)
			}
			if code, _ := rpc(t, h, "read_resource", map[string]any{"id": "gate:tx:limited"}); code != 404 {
				t.Fatal("rejected request published")
			}
		})
	}
}

func TestReviewBinaryPreparationMetadata(t *testing.T) {
	_, h := fixture(t)
	session := connectMCP(t, h, nil, nil)
	for _, op := range []string{"create_binary", "update_binary"} {
		t.Run(op, func(t *testing.T) {
			base := map[string]any{"id": "gate:bn:valid", "mime": "application/octet-stream"}
			if op == "update_binary" {
				base["expected_revision"] = 1
				base["expected_hash"] = strings.Repeat("a", 64)
			}
			invalid := []map[string]any{{"id": "gate:bn:../escape"}, {"id": "gate:tx:wrong"}, {"mime": "bad mime"}, {"mime": "text/plain\r\nX-Bad: value"}, {"mime": "text/plain; x=" + strings.Repeat("a", 256)}}
			if op == "create_binary" {
				invalid = append(invalid, map[string]any{"tags": []string{"gate:tg:../bad"}}, map[string]any{"tags": []string{"gate:tx:wrong"}}, map[string]any{"tags": []string{"gate:tg:" + strings.Repeat("a", 110)}})
				tags := []string{}
				for i := 0; i < 64; i++ {
					tags = append(tags, fmt.Sprintf("gate:tg:%03d-", i)+strings.Repeat("a", 95))
				}
				invalid = append(invalid, map[string]any{"tags": tags})
			} else {
				invalid = append(invalid, map[string]any{"expected_hash": "bad\nheader"}, map[string]any{"tags": []string{"gate:tg:unsupported-update-tag"}})
			}
			for _, delta := range invalid {
				args := map[string]any{}
				for k, v := range base {
					args[k] = v
				}
				for k, v := range delta {
					args[k] = v
				}
				got := tool(t, session, op, args, true)
				if got["status"] != float64(400) {
					t.Fatal(delta, got)
				}
			}
			got := tool(t, session, op, base, false)
			if got["transfer_required"] != true || got["resource_created"] != false {
				t.Fatal(got)
			}
			code, _ := rpc(t, h, "read_resource", map[string]any{"id": base["id"]})
			if code != 404 {
				t.Fatal("prepare uploaded bytes", code)
			}
		})
	}
}
