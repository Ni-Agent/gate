package gate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"net/http"
	"testing"
)

func TestEqualAccessTokens(t *testing.T) {
	s := openTest(t)
	tokens := []MachineToken{}
	for _, id := range []string{"agent:chatgpt", "agent:grok-bot", "agent:cursor", "agent:hermes"} {
		want(t, s.Write("human:owner", "POST", "/identities", jsonText(Identity{ID: id, Name: id}), "", id), 201)
		h := sha256.Sum256([]byte("test-only-" + id))
		tokens = append(tokens, MachineToken{Identity: id, SHA256: hex.EncodeToString(h[:])})
	}
	v, e := Verifier(s, tokens, func(context.Context, string, *http.Request) (*auth.TokenInfo, error) {
		return nil, auth.ErrInvalidToken
	})
	if e != nil {
		t.Fatal(e)
	}
	for _, token := range tokens {
		info, e := v(context.Background(), "test-only-"+token.Identity, nil)
		if e != nil || info.UserID != token.Identity {
			t.Fatal(info, e)
		}
		want(t, s.Write(info.UserID, "PUT", "/kb/projects/shared", token.Identity, "*", token.Identity), map[bool]int{true: 204, false: 412}[token.Identity == "agent:chatgpt"])
	}
	if _, e = v(context.Background(), "wrong", nil); e == nil {
		t.Fatal("invalid token accepted")
	}
	if _, e = Verifier(s, []MachineToken{{Identity: "missing", SHA256: tokens[0].SHA256}}, nil); e == nil {
		t.Fatal("unknown identity accepted")
	}
}

func TestOAuthIdentityAttribution(t *testing.T) {
	s := openTest(t)
	want(t, s.Write("human:owner", "POST", "/identities", jsonText(Identity{ID: "agent:chatgpt", Name: "ChatGPT"}), "", "register-chatgpt"), 201)
	v, e := Verifier(s, nil, func(context.Context, string, *http.Request) (*auth.TokenInfo, error) {
		return &auth.TokenInfo{UserID: "agent:chatgpt"}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	info, e := v(context.Background(), "oauth-token", nil)
	if e != nil || info.UserID != "agent:chatgpt" {
		t.Fatal("OAuth identity was overwritten", info, e)
	}
}
