package mcptest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ory/fosite"
	"golang.org/x/time/rate"
)

func grokMetadata() *GrokClientMetadata {
	return &GrokClientMetadata{ClientID: GrokClientID, ClientName: "Grok", RedirectURIs: []string{"https://grok.com/connectors-oauth-exchange-code/", "https://console.x.ai/connectors-oauth-exchange-code/"}, GrantTypes: []string{"authorization_code", "refresh_token"}, ResponseTypes: []string{"code"}, TokenEndpointAuthMethod: "none"}
}

func enableGrok(t *testing.T, f *fixture) Credentials {
	t.Helper()
	owner := f.cred
	owner.Grok = grokMetadata()
	reopenWith(t, f, owner)
	return owner
}

func reopenWith(t *testing.T, f *fixture, owner Credentials) {
	t.Helper()
	path := f.login.tokens.path
	f.login.Close()
	var err error
	f.login, err = NewPersistentAuth(f.config, owner, path)
	if err != nil {
		t.Fatal(err)
	}
	f.login.limiter.SetLimit(rate.Inf)
	f.server.Config.Handler = f.login.Handler()
}

func selectGrok(f *fixture, redirect string) {
	f.cred.ClientID = GrokClientID
	f.cred.ClientSecret = ""
	f.cred.CallbackURL = redirect
}

func publicToken(t *testing.T, f *fixture, params url.Values) (int, map[string]any) {
	t.Helper()
	status, _, body := f.request(t, "POST", "/token", params.Encode(), "", map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	var result map[string]any
	// Boundary errors can be plain text; successful responses must be JSON.
	if err := json.Unmarshal([]byte(body), &result); status == 200 && err != nil {
		t.Fatal(err)
	}
	return status, result
}

func publicExchange(t *testing.T, f *fixture, code, verifier string) (int, map[string]any) {
	return publicToken(t, f, url.Values{"client_id": {GrokClientID}, "grant_type": {"authorization_code"}, "redirect_uri": {f.cred.CallbackURL}, "resource": {f.config.PublicURL + "/mcp"}, "code": {code}, "code_verifier": {verifier}})
}
func publicRefresh(t *testing.T, f *fixture, token string) (int, map[string]any) {
	return publicToken(t, f, url.Values{"client_id": {GrokClientID}, "grant_type": {"refresh_token"}, "refresh_token": {token}})
}

func TestGrokMetadataTrustBoundaries(t *testing.T) {
	for _, mutate := range []func(*GrokClientMetadata){
		func(g *GrokClientMetadata) { g.ClientID = "https://attacker.invalid/client.json" },
		func(g *GrokClientMetadata) { g.ClientName = "" },
		func(g *GrokClientMetadata) { g.TokenEndpointAuthMethod = "client_secret_post" },
		func(g *GrokClientMetadata) { g.RedirectURIs = nil },
		func(g *GrokClientMetadata) {
			g.RedirectURIs = []string{"https://grok.com/connectors-oauth-exchange-code"}
		},
		func(g *GrokClientMetadata) {
			g.RedirectURIs = []string{"https://grok.com.attacker.invalid/connectors-oauth-exchange-code/"}
		},
		func(g *GrokClientMetadata) {
			g.RedirectURIs = []string{"https://grok.com/connectors-oauth-exchange-code/?next=evil"}
		},
		func(g *GrokClientMetadata) { g.RedirectURIs = append(g.RedirectURIs[:1], g.RedirectURIs[0]) },
		func(g *GrokClientMetadata) { g.GrantTypes = []string{"client_credentials", "refresh_token"} },
		func(g *GrokClientMetadata) { g.ResponseTypes = []string{"token"} },
	} {
		g := grokMetadata()
		mutate(g)
		if g.Validate() == nil {
			t.Fatalf("untrusted metadata accepted: %+v", g)
		}
	}
	if err := grokMetadata().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestGrokDiscoveryIsOptIn(t *testing.T) {
	f := persistentFixture(t)
	for _, enabled := range []bool{false, true} {
		if enabled {
			enableGrok(t, f)
		}
		status, _, body := f.request(t, "GET", "/.well-known/oauth-authorization-server", "", "", nil)
		var meta struct {
			CIMD    bool     `json:"client_id_metadata_document_supported"`
			Methods []string `json:"token_endpoint_auth_methods_supported"`
		}
		if status != 200 || json.Unmarshal([]byte(body), &meta) != nil {
			t.Fatal(status, body)
		}
		if meta.CIMD != enabled || strings.Contains(strings.Join(meta.Methods, ","), "none") != enabled {
			t.Fatal("incorrect discovery", body)
		}
	}
}

func TestGrokLifecyclePreservesChatGPTSessions(t *testing.T) {
	f := persistentFixture(t)
	chatAccess, chatRefresh := issue(t, f)
	owner := enableGrok(t, f)
	if _, err := f.login.Verify(context.Background(), chatAccess, nil); err != nil {
		t.Fatal("enabling Grok invalidated ChatGPT", err)
	}
	selectGrok(f, grokMetadata().RedirectURIs[0])
	code, verifier := f.loginCode(t, ReadScope+" "+WriteScope)
	status, result := publicExchange(t, f, code, verifier)
	if status != 200 {
		t.Fatal("public exchange", status, result)
	}
	access, refreshToken := result["access_token"].(string), result["refresh_token"].(string)
	reopenWith(t, f, owner)
	info, err := f.login.Verify(context.Background(), access, nil)
	if err != nil || info.UserID != GrokIdentity {
		t.Fatal("Grok identity after restart", info, err)
	}
	// Exercise renewal after access expiry, without a public-client secret.
	for sig, raw := range f.login.tokens.state.Access {
		request, _ := f.login.tokens.unpack(raw)
		if request.GetClient().GetID() == GrokClientID {
			request.GetSession().SetExpiresAt(fosite.AccessToken, time.Now().Add(-time.Minute))
			f.login.tokens.state.Access[sig], _ = json.Marshal(request)
		}
	}
	if _, err := f.login.Verify(context.Background(), access, nil); err == nil {
		t.Fatal("expired access accepted")
	}
	status, result = publicRefresh(t, f, refreshToken)
	if status != 200 {
		t.Fatal("public refresh", status, result)
	}
	nextAccess, nextRefresh := result["access_token"].(string), result["refresh_token"].(string)
	if nextRefresh == refreshToken {
		t.Fatal("refresh did not rotate")
	}
	reopenWith(t, f, owner)
	if info, err := f.login.Verify(context.Background(), nextAccess, nil); err != nil || info.UserID != GrokIdentity {
		t.Fatal("rotated session", info, err)
	}
	if status, _ := publicRefresh(t, f, refreshToken); status == 200 {
		t.Fatal("refresh replay accepted")
	}
	reopenWith(t, f, owner)
	if _, err := f.login.Verify(context.Background(), nextAccess, nil); err == nil {
		t.Fatal("replayed grant survived restart")
	}
	if status, _ := publicRefresh(t, f, nextRefresh); status == 200 {
		t.Fatal("revoked refresh accepted")
	}
	if _, err := f.login.Verify(context.Background(), chatAccess, nil); err != nil {
		t.Fatal("Grok replay revoked ChatGPT", err)
	}
	f.cred = owner
	if status, _ := refresh(t, f, chatRefresh, owner.ClientSecret, ""); status != 200 {
		t.Fatal("ChatGPT refresh broken", status)
	}
}

func TestGrokCallbackAndClientIsolation(t *testing.T) {
	f := persistentFixture(t)
	owner := enableGrok(t, f)
	for _, callback := range grokMetadata().RedirectURIs {
		selectGrok(f, callback)
		code, verifier := f.loginCode(t, ReadScope)
		status, result := publicExchange(t, f, code, verifier)
		if status != 200 {
			t.Fatal(callback, status, result)
		}
	}
	selectGrok(f, grokMetadata().RedirectURIs[0])
	for _, mutation := range []func(url.Values){
		func(q url.Values) { q.Set("redirect_uri", "https://grok.com/connectors-oauth-exchange-code/evil") },
		func(q url.Values) { q.Set("redirect_uri", owner.CallbackURL) },
		func(q url.Values) { q.Set("client_id", "https://attacker.invalid/client.json") },
		func(q url.Values) { q.Add("client_id", GrokClientID) },
		func(q url.Values) { q.Set("resource", "https://attacker.invalid/mcp") },
		func(q url.Values) { q.Del("resource") },
		func(q url.Values) { q.Set("code_challenge_method", "plain") },
		func(q url.Values) { q.Del("code_challenge") },
		func(q url.Values) { q.Set("scope", "admin") },
	} {
		// Begin a valid flow to get a full request, then mutate a copy.
		flow, _ := f.begin(t, ReadScope)
		q, _ := url.ParseQuery(f.login.pending[flow].query)
		mutation(q)
		status, h, _ := f.request(t, "GET", "/authorize?"+q.Encode(), "", "", nil)
		if status != 400 || h.Get("Location") != "" {
			t.Fatal("invalid request accepted", q, status)
		}
	}
	code, verifier := f.loginCode(t, ReadScope)
	for _, params := range []url.Values{
		{"client_id": {GrokClientID}, "grant_type": {"authorization_code"}, "redirect_uri": {f.cred.CallbackURL}, "resource": {f.config.PublicURL + "/mcp"}, "code": {code}, "code_verifier": {randomSecret()}},
		{"client_id": {GrokClientID}, "grant_type": {"authorization_code"}, "redirect_uri": {grokMetadata().RedirectURIs[1]}, "resource": {f.config.PublicURL + "/mcp"}, "code": {code}, "code_verifier": {verifier}},
		{"client_id": {owner.ClientID}, "client_secret": {owner.ClientSecret}, "grant_type": {"authorization_code"}, "redirect_uri": {owner.CallbackURL}, "resource": {f.config.PublicURL + "/mcp"}, "code": {code}, "code_verifier": {verifier}},
	} {
		if status, _ := publicToken(t, f, params); status == 200 {
			t.Fatal("code binding bypass", params)
		}
	}
	// A valid fresh code tests secret rejection without prior PKCE failures.
	code, verifier = f.loginCode(t, ReadScope)
	status, result := publicExchange(t, f, code, verifier)
	if status != 200 {
		t.Fatal(status, result)
	}
	token := result["refresh_token"].(string)
	for _, params := range []url.Values{
		{"client_id": {GrokClientID}, "client_secret": {"anything"}, "grant_type": {"refresh_token"}, "refresh_token": {token}},
		{"client_id": {owner.ClientID}, "client_secret": {owner.ClientSecret}, "grant_type": {"refresh_token"}, "refresh_token": {token}},
		{"client_id": {GrokClientID}, "grant_type": {"refresh_token"}, "resource": {"https://attacker.invalid/mcp"}, "refresh_token": {token}},
	} {
		if status, _ := publicToken(t, f, params); status == 200 {
			t.Fatal("refresh binding bypass")
		}
	}
	if status, _ := publicRefresh(t, f, token); status != 200 {
		t.Fatal("invalid client attempts consumed grant", status)
	}
	f.cred = owner
	code, verifier = f.loginCode(t, ReadScope)
	if status, _ := f.exchange(t, code, verifier, "", f.config.PublicURL+"/mcp"); status == 200 {
		t.Fatal("ChatGPT became public")
	}
}

func TestGrokLoginShowsClientAndPreservesCSRF(t *testing.T) {
	f := persistentFixture(t)
	enableGrok(t, f)
	selectGrok(f, grokMetadata().RedirectURIs[0])
	flow, _ := f.begin(t, ReadScope)
	pending := f.login.pending[flow]
	status, _, body := f.request(t, "GET", "/authorize?"+pending.query, "", "", nil)
	if status != 200 || !strings.Contains(body, "Connect Grok to Gate") {
		t.Fatal("incorrect consent", status, body)
	}
	// The second GET changes the secure cookie, so the old flow cannot be submitted.
	status, _, _ = f.request(t, "POST", "/authorize", url.Values{"flow": {flow}, "password": {f.cred.LoginPassword}}.Encode(), "", map[string]string{"Origin": f.config.PublicURL, "Content-Type": "application/x-www-form-urlencoded"})
	if status != http.StatusForbidden {
		t.Fatal("cookie/flow mismatch accepted", status)
	}
}

func TestGrokCodeReplayRevokesOnlyItsGrant(t *testing.T) {
	f := persistentFixture(t)
	chatAccess, _ := issue(t, f)
	enableGrok(t, f)
	selectGrok(f, grokMetadata().RedirectURIs[0])
	code, verifier := f.loginCode(t, ReadScope)
	status, result := publicExchange(t, f, code, verifier)
	if status != 200 {
		t.Fatal(status, result)
	}
	access := result["access_token"].(string)
	if _, err := f.login.Verify(context.Background(), access, nil); err != nil {
		t.Fatal(err)
	}
	if status, _ := publicExchange(t, f, code, verifier); status == 200 {
		t.Fatal("code replay accepted")
	}
	if _, err := f.login.Verify(context.Background(), access, nil); err == nil {
		t.Fatal("code replay did not revoke grant")
	}
	if _, err := f.login.Verify(context.Background(), chatAccess, nil); err != nil {
		t.Fatal("code replay affected ChatGPT", err)
	}
}

func TestChatGPTBasicAuthWithGrokEnabled(t *testing.T) {
	f := persistentFixture(t)
	enableGrok(t, f)
	code, verifier := f.loginCode(t, ReadScope)
	params := url.Values{"grant_type": {"authorization_code"}, "redirect_uri": {f.cred.CallbackURL}, "resource": {f.config.PublicURL + "/mcp"}, "code": {code}, "code_verifier": {verifier}}
	req, _ := http.NewRequest("POST", f.server.URL+"/token", strings.NewReader(params.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(url.QueryEscape(f.cred.ClientID), url.QueryEscape(f.cred.ClientSecret))
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal("ChatGPT Basic authentication changed", resp.StatusCode)
	}
}
