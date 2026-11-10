package mcptest

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"workflow-api/internal/content"
)

type fixture struct {
	server *httptest.Server
	client *http.Client
	login  *LocalAuth
	cred   Credentials
	config Config
	root   string
}

func setup(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "voice.md"), []byte("# Voice test\nMarker: untouched\n"), 0600)
	catalog := filepath.Join(t.TempDir(), "catalog.json")
	os.WriteFile(catalog, []byte(`{"documents":[{"id":"voice","title":"Voice test","path":"voice.md"}],"resources":[]}`), 0600)
	store, err := content.Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	c := Config{PublicURL: "https://workspace.example.invalid"}
	cred := NewCredentials("https://chatgpt.com/connector/oauth/test-callback-123")
	login, err := NewLocalAuth(c, cred)
	if err != nil {
		t.Fatal(err)
	}
	resource, err := NewHandler(c, store.Handler(), login.Verify)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, p := range []string{"/authorize", "/token", "/.well-known/oauth-authorization-server"} {
		mux.Handle(p, login.Handler())
	}
	mux.Handle("/", resource)
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	client := srv.Client()
	client.Jar, _ = cookiejar.New(nil)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &fixture{server: srv, client: client, login: login, cred: cred, config: c, root: root}
}
func (f *fixture) request(t *testing.T, method, path, body, token string, headers map[string]string) (int, http.Header, string) {
	t.Helper()
	r, _ := http.NewRequest(method, f.server.URL+path, strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := f.client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(b)
}
func (f *fixture) begin(t *testing.T, scope string) (string, string) {
	t.Helper()
	verifier := randomSecret()
	digest := sha256.Sum256([]byte(verifier))
	q := url.Values{"client_id": {f.cred.ClientID}, "redirect_uri": {f.cred.CallbackURL}, "resource": {f.config.PublicURL + "/mcp"}, "response_type": {"code"}, "scope": {scope}, "state": {"0123456789random-state"}, "code_challenge_method": {"S256"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(digest[:])}}
	code, h, body := f.request(t, "GET", "/authorize?"+q.Encode(), "", "", nil)
	if code != 200 {
		t.Fatalf("start login: %d %s", code, body)
	}
	if h.Get("Referrer-Policy") != "same-origin" {
		t.Fatal("login form must preserve Origin on native browser POSTs", h)
	}
	wantPolicy := "default-src 'none'; form-action 'self' " + f.cred.CallbackURL + "; frame-ancestors 'none'; base-uri 'none'"
	if h.Get("Content-Security-Policy") != wantPolicy {
		t.Fatal("login form must allow only self and the exact registered callback", h)
	}
	response := &http.Response{Header: h}
	var flow string
	for _, c := range response.Cookies() {
		if c.Name == "__Host-workflow-login" {
			flow = c.Value
		}
	}
	if flow == "" {
		t.Fatal("missing secure login cookie")
	}
	return flow, verifier
}
func (f *fixture) loginCode(t *testing.T, scope string) (string, string) {
	t.Helper()
	flow, verifier := f.begin(t, scope)
	code, h, body := f.request(t, "POST", "/authorize", url.Values{"flow": {flow}, "password": {f.cred.LoginPassword}}.Encode(), "", map[string]string{"Content-Type": "application/x-www-form-urlencoded", "Origin": f.config.PublicURL})
	if code != 302 && code != 303 {
		t.Fatalf("login: %d %s", code, body)
	}
	location, e := url.Parse(h.Get("Location"))
	if e != nil {
		t.Fatal(e)
	}
	if location.Scheme+"://"+location.Host+location.Path != f.cred.CallbackURL {
		t.Fatal("wrong callback")
	}
	if location.Query().Get("iss") != f.config.PublicURL {
		t.Fatal("issuer missing from callback")
	}
	if location.Query().Get("state") != "0123456789random-state" {
		t.Fatal("state lost")
	}
	return location.Query().Get("code"), verifier
}
func (f *fixture) exchange(t *testing.T, code, verifier, secret, resource string) (int, string) {
	t.Helper()
	params := url.Values{"grant_type": {"authorization_code"}, "client_id": {f.cred.ClientID}, "client_secret": {secret}, "redirect_uri": {f.cred.CallbackURL}, "resource": {resource}, "code": {code}, "code_verifier": {verifier}}
	status, _, body := f.request(t, "POST", "/token", params.Encode(), "", map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	return status, body
}
func (f *fixture) token(t *testing.T, scope string) string {
	t.Helper()
	code, verifier := f.loginCode(t, scope)
	status, body := f.exchange(t, code, verifier, f.cred.ClientSecret, f.config.PublicURL+"/mcp")
	if status != 200 {
		t.Fatalf("token: %d %s", status, body)
	}
	var data struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal([]byte(body), &data); err != nil {
		t.Fatal(err)
	}
	if data.AccessToken == "" {
		t.Fatal("missing access token")
	}
	return data.AccessToken
}

func TestOAuthSecurity(t *testing.T) {
	t.Run("PKCE and code replay", func(t *testing.T) {
		f := setup(t)
		code, verifier := f.loginCode(t, ReadScope+" "+WriteScope)
		status, body := f.exchange(t, code, verifier, f.cred.ClientSecret, f.config.PublicURL+"/mcp")
		if status != 200 {
			t.Fatalf("%d %s", status, body)
		}
		status, _ = f.exchange(t, code, verifier, f.cred.ClientSecret, f.config.PublicURL+"/mcp")
		if status == 200 {
			t.Fatal("replayed authorization code")
		}
	})
	t.Run("wrong PKCE", func(t *testing.T) {
		f := setup(t)
		code, _ := f.loginCode(t, ReadScope)
		status, _ := f.exchange(t, code, randomSecret(), f.cred.ClientSecret, f.config.PublicURL+"/mcp")
		if status == 200 {
			t.Fatal("accepted wrong PKCE")
		}
	})
	t.Run("wrong client secret", func(t *testing.T) {
		f := setup(t)
		code, v := f.loginCode(t, ReadScope)
		status, _ := f.exchange(t, code, v, randomSecret(), f.config.PublicURL+"/mcp")
		if status == 200 {
			t.Fatal("accepted wrong client")
		}
	})
	t.Run("wrong resource", func(t *testing.T) {
		f := setup(t)
		code, v := f.loginCode(t, ReadScope)
		status, _ := f.exchange(t, code, v, f.cred.ClientSecret, "https://elsewhere.invalid/mcp")
		if status != 400 {
			t.Fatal(status)
		}
	})
	for _, origin := range []string{"https://attacker.invalid", "null", ""} {
		t.Run("CSRF/"+origin, func(t *testing.T) {
			f := setup(t)
			flow, _ := f.begin(t, ReadScope)
			status, _, _ := f.request(t, "POST", "/authorize", url.Values{"flow": {flow}, "password": {f.cred.LoginPassword}}.Encode(), "", map[string]string{"Content-Type": "application/x-www-form-urlencoded", "Origin": origin})
			if status != 403 {
				t.Fatal(status)
			}
		})
	}
	t.Run("wrong login", func(t *testing.T) {
		f := setup(t)
		flow, _ := f.begin(t, ReadScope)
		status, _, _ := f.request(t, "POST", "/authorize", url.Values{"flow": {flow}, "password": {"wrong"}}.Encode(), "", map[string]string{"Content-Type": "application/x-www-form-urlencoded", "Origin": f.config.PublicURL})
		if status != 401 {
			t.Fatal(status)
		}
	})
	t.Run("redirect and grant boundaries", func(t *testing.T) {
		f := setup(t)
		for _, p := range []string{"/authorize?redirect_uri=https://attacker.invalid", "/authorize?response_type=token", "/authorize?code_challenge_method=plain"} {
			status, h, _ := f.request(t, "GET", p, "", "", nil)
			if status != 400 || h.Get("Location") != "" {
				t.Fatal(status, h)
			}
		}
	})
}

type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c := r.Clone(r.Context())
	c.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(c)
}
func (f *fixture) connect(t *testing.T, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	httpClient := &http.Client{Transport: bearerTransport{f.client.Transport, token}, Timeout: 10 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: f.server.URL + "/mcp", HTTPClient: httpClient}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}
func tool(t *testing.T, s *mcp.ClientSession, name string, args any) (*mcp.CallToolResult, result) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := s.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	var out result
	if response.StructuredContent != nil {
		b, _ := json.Marshal(response.StructuredContent)
		if err = json.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
	}
	return response, out
}

func TestMCPAndIndependentReadback(t *testing.T) {
	f := setup(t)
	token := f.token(t, ReadScope+" "+WriteScope)
	session := f.connect(t, token)
	list, err := session.ListTools(context.Background(), nil)
	if err != nil || len(list.Tools) != 3 {
		t.Fatal("tool discovery", err)
	}
	_, before := tool(t, session, "read_note", noteArgs{ID: "voice"})
	if before.Status != 200 || before.ETag == "" {
		t.Fatal(before)
	}
	_, write := tool(t, session, "replace_note", replaceArgs{ID: "voice", Content: "# Voice test\nMarker: blue lantern 47\n", ETag: before.ETag})
	if write.Status != 204 {
		t.Fatal(write)
	}
	status, _, body := f.request(t, "GET", "/knowledge/voice", "", token, nil)
	if status != 200 || !strings.Contains(body, "blue lantern 47") {
		t.Fatal("cross-client persistence", status, body)
	}
	_, stale := tool(t, session, "replace_note", replaceArgs{ID: "voice", Content: "lost update", ETag: before.ETag})
	if stale.Status != 412 {
		t.Fatal(stale)
	}
}

func TestAuthorizationBoundaries(t *testing.T) {
	f := setup(t)
	for _, path := range []string{"/mcp", "/knowledge", "/knowledge/voice"} {
		code, h, _ := f.request(t, "GET", path, "", "", nil)
		if code != 401 || !strings.Contains(h.Get("WWW-Authenticate"), "oauth-protected-resource") {
			t.Fatal(path, code, h)
		}
	}
	for _, path := range []string{"/documents/any", "/tasks", "/projects", "/content"} {
		code, _, _ := f.request(t, "GET", path, "", "", nil)
		if code != 404 {
			t.Fatal("legacy route exposed", path, code)
		}
	}
	code, _, _ := f.request(t, "GET", "/knowledge/voice", "", "bad-token", nil)
	if code != 401 {
		t.Fatal(code)
	}
	token := f.token(t, ReadScope)
	session := f.connect(t, token)
	response, _ := tool(t, session, "replace_note", replaceArgs{ID: "voice", Content: "unauthorized", ETag: "*"})
	if !response.IsError {
		t.Fatal("read-only token could write")
	}
	code, _, _ = f.request(t, "PUT", "/knowledge/voice", "unauthorized", token, map[string]string{"Content-Type": "text/markdown", "If-Match": "*"})
	if code != 403 {
		t.Fatal(code)
	}
	code, _, _ = f.request(t, "GET", "/knowledge/voice", "", token, map[string]string{"Origin": "https://attacker.invalid"})
	if code != 403 {
		t.Fatal(code)
	}
	data, _ := os.ReadFile(filepath.Join(f.root, "voice.md"))
	if strings.Contains(string(data), "unauthorized") {
		t.Fatal("rejected write changed bytes")
	}
}

func TestExpiredTokenRejected(t *testing.T) {
	c := Config{PublicURL: "https://workspace.example.invalid"}
	h, e := NewHandler(c, http.NotFoundHandler(), func(context.Context, string, *http.Request) (*auth.TokenInfo, error) {
		return &auth.TokenInfo{UserID: "test-owner", Scopes: []string{ReadScope}, Expiration: time.Now().Add(-time.Minute)}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	r := httptest.NewRequest("GET", "http://127.0.0.1/knowledge", nil)
	r.Header.Set("Authorization", "Bearer expired")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}
