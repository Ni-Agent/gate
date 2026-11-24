package transport

import (
	"encoding/hex"
	"encoding/json"
	"mime"
	"strings"

	"gate-alpha/internal/core"
)

func printableHeader(s string) bool {
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

// Shared by raw HTTP upload and MCP transfer preparation. This validates only
// metadata syntax/bounds, not bytes, referenced-tag existence or upload success.
func binaryMetadata(id, media string, tags []string) (string, error) {
	if e := core.ValidateResourceID(id, "bn"); e != nil {
		return "", e
	}
	if len(media) > 256 || !printableHeader(media) {
		return "", bad(400, "invalid MIME metadata header")
	}
	typ, _, e := mime.ParseMediaType(media)
	if e != nil || !strings.Contains(typ, "/") {
		return "", bad(400, "valid X-Gate-Mime required")
	}
	if len(tags) > 64 {
		return "", bad(400, "too many tags")
	}
	for _, id := range tags {
		if e := core.ValidateResourceID(id, "tg"); e != nil {
			return "", e
		}
	}
	encoded, _ := json.Marshal(tags)
	if len(encoded) > 4096 {
		return "", bad(400, "tag header too long")
	}
	return typ, nil
}
func prepareBinary(name string, a map[string]any) (map[string]any, error) {
	tags := []string{}
	if raw, ok := a["tags"].([]any); ok {
		for _, v := range raw {
			tags = append(tags, v.(string))
		}
	}
	if _, e := binaryMetadata(a["id"].(string), a["mime"].(string), tags); e != nil {
		return nil, e
	}
	headers := map[string]any{"Content-Type": "application/octet-stream", "X-Gate-ID": a["id"], "X-Gate-Mime": a["mime"]}
	method := "POST"
	if name == "update_binary" {
		digest := a["expected_hash"].(string)
		if _, e := hex.DecodeString(digest); e != nil || len(digest) != 64 {
			return nil, bad(400, "expected_hash must be a SHA-256 validator")
		}
		method = "PUT"
		headers["X-Gate-Revision"] = a["expected_revision"]
		headers["If-Match"] = digest
	} else if _, ok := a["tags"]; ok {
		raw, _ := json.Marshal(tags)
		headers["X-Gate-Tags"] = string(raw)
	}
	return map[string]any{"transfer_required": true, "resource_created": false, "method": method, "path": "/binary", "headers": headers, "authorization": "Supply configured Bearer credential via HTTP client, never model-visible tool arguments.", "size_policy": "streamed; no product content-size cap"}, nil
}
