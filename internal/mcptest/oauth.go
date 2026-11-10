package mcptest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"html/template"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/ory/fosite"
	"github.com/ory/fosite/compose"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/time/rate"
)

// Credentials retain the existing ChatGPT registration and shared owner login.
// Grok is an optional, independently attributed public PKCE client.
type Credentials struct {
	ClientID      string              `json:"client_id"`
	ClientSecret  string              `json:"client_secret"`
	CallbackURL   string              `json:"callback_url"`
	Identity      string              `json:"identity,omitempty"`
	LoginPassword string              `json:"login_password"`
	Grok          *GrokClientMetadata `json:"grok,omitempty"`
}
type pendingLogin struct {
	query   string
	expires time.Time
}
type registeredClient struct {
	client   *fosite.DefaultClient
	name     string
	identity string
}
type LocalAuth struct {
	clients     map[string]registeredClient
	config      Config
	credentials Credentials
	provider    fosite.OAuth2Provider
	mu          sync.Mutex
	pending     map[string]pendingLogin
	limiter     *rate.Limiter
	opMu        sync.Mutex
	tokens      *tokenStore
	failed      bool
}

func randomSecret() string {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func NewCredentials(callback string) Credentials {
	return Credentials{ClientID: "workflow-chatgpt-test", ClientSecret: randomSecret(), CallbackURL: callback, LoginPassword: randomSecret()}
}

func NewLocalAuth(c Config, cred Credentials) (*LocalAuth, error) {
	return NewPersistentAuth(c, cred, "")
}

// NewPersistentAuth retains OAuth signing state and token sessions outside the KB.
func NewPersistentAuth(c Config, cred Credentials, statePath string) (*LocalAuth, error) {
	if cred.Identity == "" {
		cred.Identity = "test-owner"
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	callback, e := url.Parse(cred.CallbackURL)
	if e != nil || callback.Scheme != "https" || callback.Host != "chatgpt.com" || callback.User != nil || callback.RawQuery != "" || callback.Fragment != "" || (callback.Path != "/connector_platform_oauth_redirect" && (!strings.HasPrefix(callback.Path, "/connector/oauth/") || len(strings.TrimPrefix(callback.Path, "/connector/oauth/")) < 8)) {
		return nil, errors.New("copy the exact HTTPS OAuth callback URI from ChatGPT")
	}
	if cred.ClientID == "" || len(cred.ClientSecret) < 32 || len(cred.LoginPassword) < 32 {
		return nil, errors.New("test credentials require an ID and at least 32-character random secrets")
	}
	if cred.Grok != nil {
		if e := cred.Grok.Validate(); e != nil {
			return nil, e
		}
		if cred.ClientID == GrokClientID {
			return nil, errors.New("duplicate OAuth client ID")
		}
	}
	hashed, e := bcrypt.GenerateFromPassword([]byte(cred.ClientSecret), bcrypt.DefaultCost)
	if e != nil {
		return nil, e
	}
	memory, e := newTokenStore(statePath)
	if e != nil {
		return nil, e
	}
	memory.Clients[cred.ClientID] = &fosite.DefaultClient{ID: cred.ClientID, Secret: hashed, RedirectURIs: []string{cred.CallbackURL}, GrantTypes: []string{"authorization_code", "refresh_token"}, ResponseTypes: []string{"code"}, Scopes: []string{ReadScope, WriteScope, "offline_access"}, Audience: []string{c.PublicURL + "/mcp"}}
	clients := map[string]registeredClient{cred.ClientID: {client: memory.Clients[cred.ClientID].(*fosite.DefaultClient), name: "ChatGPT", identity: cred.Identity}}
	if cred.Grok != nil {
		g := cred.Grok
		client := &fosite.DefaultClient{ID: g.ClientID, Public: true, RedirectURIs: slices.Clone(g.RedirectURIs), GrantTypes: slices.Clone(g.GrantTypes), ResponseTypes: slices.Clone(g.ResponseTypes), Scopes: []string{ReadScope, WriteScope, "offline_access"}, Audience: []string{c.PublicURL + "/mcp"}}
		memory.Clients[g.ClientID] = client
		clients[g.ClientID] = registeredClient{client: client, name: "Grok", identity: GrokIdentity}
	}
	settings := &fosite.Config{GlobalSecret: []byte(memory.state.Secret), AccessTokenLifespan: time.Hour, RefreshTokenLifespan: 90 * 24 * time.Hour, RefreshTokenScopes: []string{}, AuthorizeCodeLifespan: 2 * time.Minute, EnforcePKCE: true, EnforcePKCEForPublicClients: true, EnablePKCEPlainChallengeMethod: false, ScopeStrategy: fosite.ExactScopeStrategy, MinParameterEntropy: 8, DisableRefreshTokenValidation: true}
	provider := compose.Compose(settings, memory, compose.NewOAuth2HMACStrategy(settings), compose.OAuth2AuthorizeExplicitFactory, compose.OAuth2PKCEFactory, compose.OAuth2RefreshTokenGrantFactory, compose.OAuth2TokenIntrospectionFactory)
	return &LocalAuth{clients: clients, config: c, credentials: cred, provider: provider, pending: make(map[string]pendingLogin), tokens: memory, limiter: rate.NewLimiter(rate.Every(time.Second), 10)}, nil
}

func (a *LocalAuth) Verify(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	if a.failed {
		return nil, auth.ErrInvalidToken
	}
	if len(token) > 8192 {
		return nil, auth.ErrInvalidToken
	}
	session := &fosite.DefaultSession{}
	_, request, err := a.provider.IntrospectToken(ctx, token, fosite.AccessToken, session, ReadScope)
	if err != nil {
		return nil, auth.ErrInvalidToken
	}
	client, ok := a.clients[request.GetClient().GetID()]
	if !ok || !request.GetGrantedAudience().Has(a.config.PublicURL+"/mcp") || request.GetSession().GetSubject() != client.identity {
		return nil, auth.ErrInvalidToken
	}
	return &auth.TokenInfo{UserID: request.GetSession().GetSubject(), Scopes: request.GetGrantedScopes(), Expiration: request.GetSession().GetExpiresAt(fosite.AccessToken)}, nil
}

func (a *LocalAuth) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/oauth-authorization-server", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		methods := []string{"client_secret_basic", "client_secret_post"}
		if a.credentials.Grok != nil {
			methods = append(methods, "none")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"client_id_metadata_document_supported": a.credentials.Grok != nil, "issuer": a.config.PublicURL, "authorization_response_iss_parameter_supported": true, "authorization_endpoint": a.config.PublicURL + "/authorize", "token_endpoint": a.config.PublicURL + "/token", "response_types_supported": []string{"code"}, "grant_types_supported": []string{"authorization_code", "refresh_token"}, "token_endpoint_auth_methods_supported": methods, "code_challenge_methods_supported": []string{"S256"}, "scopes_supported": []string{ReadScope, WriteScope, "offline_access"}})
	})
	mux.HandleFunc("GET /authorize", a.startLogin)
	mux.HandleFunc("POST /authorize", a.finishLogin)
	mux.HandleFunc("POST /token", a.token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.opMu.Lock()
		defer a.opMu.Unlock()
		if a.failed {
			http.Error(w, "OAuth persistence unavailable; operator intervention required", 503)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		// The login handlers add only the validated flow's exact callback.
		setCallbackPolicy(w, "")
		if !a.limiter.Allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limited", 429)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		// Buffer responses until token rotations/revocations are durably accepted.
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, r)
		if r.Method == "POST" {
			if err := a.tokens.save(); err != nil {
				a.failed = true
				http.Error(w, "OAuth persistence failed; reconnect after operator recovery", 503)
				return
			}
		}
		if r.URL.Path == "/token" {
			log.Printf("oauth_token client=%q grant=%q status=%d", tokenClientID(r), r.PostForm.Get("grant_type"), recorder.Code)
		}
		for key, values := range recorder.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	})
}

func (a *LocalAuth) validateRequest(r *http.Request) error {
	q := r.URL.Query()
	for _, values := range q {
		if len(values) != 1 {
			return errors.New("duplicate parameters")
		}
	}
	client, ok := a.clients[q.Get("client_id")]
	if !ok || !slices.Contains(client.client.RedirectURIs, q.Get("redirect_uri")) || q.Get("resource") != a.config.PublicURL+"/mcp" || q.Get("response_type") != "code" || q.Get("response_mode") != "" && q.Get("response_mode") != "query" || q.Get("code_challenge_method") != "S256" {
		return errors.New("invalid client, redirect, resource or flow")
	}
	challenge, e := base64.RawURLEncoding.DecodeString(q.Get("code_challenge"))
	if e != nil || len(challenge) != 32 {
		return errors.New("invalid S256 challenge")
	}
	if len(q.Get("state")) < 8 || len(q.Get("state")) > 2048 {
		return errors.New("invalid state")
	}
	return nil
}

var consent = template.Must(template.New("consent").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Gate login</title><h1>Connect {{.Client}} to Gate</h1><p>This connection can read and update your Gate knowledge base and tasks.</p><p>Requested permissions: {{.Scopes}}</p><form method="post" action="/authorize"><input type="hidden" name="flow" value="{{.Flow}}"><label>Gate login password <input type="password" name="password" required autocomplete="current-password"></label><button type="submit">Sign in and allow</button></form></html>`))

func (a *LocalAuth) startLogin(w http.ResponseWriter, r *http.Request) {
	if err := a.validateRequest(r); err != nil {
		http.Error(w, "invalid authorization request", 400)
		return
	}
	setCallbackPolicy(w, r.URL.Query().Get("redirect_uri"))
	ar, err := a.provider.NewAuthorizeRequest(r.Context(), r)
	if err != nil {
		http.Error(w, "invalid authorization request", 400)
		return
	}
	if !ar.GetRequestedScopes().Has(ReadScope) {
		http.Error(w, "notes:read scope required", 400)
		return
	}
	a.mu.Lock()
	now := time.Now()
	for id, p := range a.pending {
		if now.After(p.expires) {
			delete(a.pending, id)
		}
	}
	if len(a.pending) >= 128 {
		a.mu.Unlock()
		http.Error(w, "too many pending logins", 429)
		return
	}
	flow := randomSecret()
	a.pending[flow] = pendingLogin{query: r.URL.RawQuery, expires: now.Add(5 * time.Minute)}
	a.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "__Host-workflow-login", Value: flow, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 300})
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Native form POSTs under no-referrer send Origin: null. Preserve the
	// same-origin proof for our CSRF check without leaking OAuth query values
	// to the external callback. Other auth responses retain no-referrer.
	w.Header().Set("Referrer-Policy", "same-origin")
	_ = consent.Execute(w, map[string]string{"Client": a.clients[ar.GetClient().GetID()].name, "Flow": flow, "Scopes": strings.Join(ar.GetRequestedScopes(), ", ")})
}

func (a *LocalAuth) finishLogin(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != a.config.PublicURL {
		http.Error(w, "origin rejected", 403)
		return
	}
	if r.URL.RawQuery != "" || r.ParseForm() != nil {
		http.Error(w, "invalid form", 400)
		return
	}
	for _, values := range r.PostForm {
		if len(values) != 1 {
			http.Error(w, "duplicate parameters", 400)
			return
		}
	}
	cookie, err := r.Cookie("__Host-workflow-login")
	flow := r.PostForm.Get("flow")
	if err != nil || flow == "" || cookie.Value != flow {
		http.Error(w, "invalid login session", 403)
		return
	}
	a.mu.Lock()
	pending, ok := a.pending[flow]
	delete(a.pending, flow)
	a.mu.Unlock()
	if !ok || time.Now().After(pending.expires) {
		http.Error(w, "login expired; reconnect from your connector", 400)
		return
	}
	got, want := sha256.Sum256([]byte(r.PostForm.Get("password"))), sha256.Sum256([]byte(a.credentials.LoginPassword))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		http.Error(w, "login failed; reconnect from your connector", 401)
		return
	}
	original, _ := http.NewRequestWithContext(r.Context(), "GET", a.config.PublicURL+"/authorize?"+pending.query, nil)
	if err := a.validateRequest(original); err != nil {
		http.Error(w, "invalid authorization request", 400)
		return
	}
	setCallbackPolicy(w, original.URL.Query().Get("redirect_uri"))
	ar, err := a.provider.NewAuthorizeRequest(r.Context(), original)
	if err != nil {
		http.Error(w, "invalid authorization request", 400)
		return
	}
	for _, scope := range ar.GetRequestedScopes() {
		ar.GrantScope(scope)
	}
	ar.GrantAudience(a.config.PublicURL + "/mcp")
	response, err := a.provider.NewAuthorizeResponse(r.Context(), ar, &fosite.DefaultSession{Subject: a.clients[ar.GetClient().GetID()].identity})
	if err != nil {
		http.Error(w, "authorization failed", 400)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: "__Host-workflow-login", Value: "", Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: -1})
	response.GetParameters().Set("iss", a.config.PublicURL)
	a.provider.WriteAuthorizeResponse(r.Context(), w, ar, response)
}

func (a *LocalAuth) token(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" || r.ParseForm() != nil {
		http.Error(w, "invalid token request", 400)
		return
	}
	for _, values := range r.PostForm {
		if len(values) != 1 {
			http.Error(w, "duplicate parameters", 400)
			return
		}
	}
	client, ok := a.clients[tokenClientID(r)]
	if !ok {
		http.Error(w, "invalid client", 401)
		return
	}
	// Public clients authenticate the code with PKCE, never a shared secret.
	if client.client.Public && (r.Header.Get("Authorization") != "" || r.PostForm.Has("client_secret") || r.PostForm.Has("client_assertion") || r.PostForm.Has("client_assertion_type")) {
		http.Error(w, "invalid public client authentication", 400)
		return
	}
	grant := r.PostForm.Get("grant_type")
	resource := r.PostForm.Get("resource")
	redirect := r.PostForm.Get("redirect_uri")
	if (grant == "authorization_code" && (resource != a.config.PublicURL+"/mcp" || !slices.Contains(client.client.RedirectURIs, redirect))) || (grant == "refresh_token" && ((resource != "" && resource != a.config.PublicURL+"/mcp") || (redirect != "" && !slices.Contains(client.client.RedirectURIs, redirect)))) || (grant != "authorization_code" && grant != "refresh_token") {
		http.Error(w, "invalid token resource, grant or redirect", 400)
		return
	}
	access, err := a.provider.NewAccessRequest(r.Context(), r, &fosite.DefaultSession{})
	if err != nil {
		a.provider.WriteAccessError(r.Context(), w, access, err)
		return
	}
	response, err := a.provider.NewAccessResponse(r.Context(), access)
	if err != nil {
		a.provider.WriteAccessError(r.Context(), w, access, err)
		return
	}
	if !access.GetGrantedAudience().Has(a.config.PublicURL + "/mcp") {
		http.Error(w, "invalid token audience", 400)
		return
	}
	a.provider.WriteAccessResponse(r.Context(), w, access, response)
}

// tokenClientID is used for boundary validation and sanitized attribution logs;
// Fosite still authenticates the confidential client and binds tokens to clients.
func tokenClientID(r *http.Request) string {
	if id, _, ok := r.BasicAuth(); ok {
		decoded, err := url.QueryUnescape(id)
		if err != nil {
			return ""
		}
		if formID := r.PostForm.Get("client_id"); formID != "" && formID != decoded {
			return ""
		}
		return decoded
	}
	return r.PostForm.Get("client_id")
}

func setCallbackPolicy(w http.ResponseWriter, callback string) {
	action := "'self'"
	if callback != "" {
		action += " " + callback
	}
	w.Header().Set("Content-Security-Policy", "default-src 'none'; form-action "+action+"; frame-ancestors 'none'; base-uri 'none'")
}
