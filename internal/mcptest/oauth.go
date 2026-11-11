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
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		// The login handlers add only the validated flow's exact callback.
		setCallbackPolicy(w, "")
		if a.failed {
			if r.URL.Path == "/authorize" {
				a.loginError(w, 503, "Gate sign-in is unavailable", "Ask the Gate operator to restore the service, then start the connection again.")
			} else {
				http.Error(w, "OAuth persistence unavailable; operator intervention required", 503)
			}
			return
		}
		if !a.limiter.Allow() {
			w.Header().Set("Retry-After", "1")
			if r.URL.Path == "/authorize" {
				a.loginError(w, 429, "Too many connection requests", "Wait a moment, then start the connection again from your connector.")
			} else {
				http.Error(w, "rate limited", 429)
			}
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		// Buffer responses until token rotations/revocations are durably accepted.
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, r)
		if r.Method == "POST" {
			if err := a.tokens.save(); err != nil {
				a.failed = true
				if r.URL.Path == "/authorize" {
					a.loginError(w, 503, "Gate sign-in is unavailable", "Ask the Gate operator to restore the service, then start the connection again.")
				} else {
					http.Error(w, "OAuth persistence failed; reconnect after operator recovery", 503)
				}
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

const consentCSS = `:root{color-scheme:light dark;font-family:system-ui,-apple-system,sans-serif;background:#f2f4f7;color:#172232}*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;padding:28px 18px}.shell{width:min(100%,490px)}.brand{font-weight:750;letter-spacing:.02em;margin:0 0 20px;color:#355469}.mark{display:inline-grid;place-items:center;width:32px;height:32px;margin-right:9px;border-radius:10px;background:#153f47;color:#fff;font-size:15px}.card{background:#fff;border:1px solid #dce4e7;border-radius:20px;padding:clamp(24px,6vw,38px);box-shadow:0 16px 48px #20353a12}.eyebrow{font-size:.75rem;text-transform:uppercase;letter-spacing:.1em;font-weight:750;color:#36766e;margin:0 0 12px}h1{font-size:clamp(1.65rem,5vw,2rem);line-height:1.16;letter-spacing:-.035em;margin:0 0 14px}p{line-height:1.55;color:#4d5e69}.intro{margin:0 0 24px}.notice{background:#edf6f3;border:1px solid #cfe6dc;border-radius:12px;padding:14px 16px;margin:0 0 24px;color:#244c42;font-size:.94rem}.notice strong{display:block;margin-bottom:4px}label{display:block;font-weight:700;margin:0 0 8px}input{width:100%;font:inherit;padding:13px 14px;border:1px solid #aabac0;border-radius:10px;background:#fff;color:#172232}input:focus-visible,button:focus-visible{outline:3px solid #54ab9b;outline-offset:3px}.hint{font-size:.86rem;margin:8px 0 20px}button{width:100%;font:inherit;font-weight:700;color:white;background:#155f56;border:0;border-radius:10px;padding:14px;cursor:pointer}button:hover{background:#104c45}.origin{font-size:.83rem;overflow-wrap:anywhere;margin:22px 0 0;color:#637780}.origin strong{color:inherit}.error{background:#fff2ed;border-color:#f1cbbd;color:#773f2f}.footer{font-size:.83rem;text-align:center;margin:18px 8px;color:#6a7b84}@media(prefers-color-scheme:dark){:root{background:#101a1d;color:#e8f1ef}.brand{color:#bad3ce}.card{background:#19292c;border-color:#32494b;box-shadow:none}p,.origin,.footer{color:#aec1be}.eyebrow{color:#81cab8}.notice{background:#1d3833;border-color:#365f52;color:#cce9da}.error{background:#422a25;border-color:#73473d;color:#f5d2c7}input{background:#102023;color:#edf6f4;border-color:#607b77}button{background:#68c4ae;color:#102520}button:hover{background:#8bd8c5}}`

var consent = template.Must(template.New("consent").Parse(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{{if .Error}}Connection interrupted{{else}}Connect {{.Client}}{{end}} · Gate</title><style>{{.Style}}</style></head><body><main class="shell"><div class="brand"><span class="mark" aria-hidden="true">G</span>Gate</div><section class="card" aria-labelledby="title">{{if .Error}}<p class="eyebrow">Connection interrupted</p><h1 id="title">{{.ErrorTitle}}</h1><div class="notice error" role="alert">{{.Error}}</div><p>Return to your connector and start the connection again. If you need the Gate login password, ask the Gate operator.</p>{{else}}<p class="eyebrow">Connect your workspace</p><h1 id="title">Connect {{.Client}} to Gate</h1><p class="intro">Allow {{.Client}} to use the knowledge and tasks in your Gate workspace.</p><div class="notice"><strong>Review access</strong>This connection can {{if .CanWrite}}read and update{{else}}read{{end}} shared knowledge and tasks. This access applies to all knowledge and tasks in this workspace.</div>{{if .FormError}}<div class="notice error" role="alert">{{.FormError}}</div>{{end}}<form method="post" action="/authorize"><input type="hidden" name="flow" value="{{.Flow}}"><label for="password">Gate login password</label><input id="password" type="password" name="password" required autocomplete="current-password" aria-describedby="password-hint"><p class="hint" id="password-hint">Use the password provided by your Gate operator. This is separate from your {{.Client}} password.</p><button type="submit">Sign in and allow access</button></form><p class="hint">To cancel, close this window and return to {{.Client}}.</p>{{end}}<p class="origin">Connecting to <strong>{{.Origin}}</strong></p></section><p class="footer">Gate · Shared knowledge and tasks</p></main></body></html>`))

func (a *LocalAuth) loginPage(w http.ResponseWriter, status int, values map[string]any) {
	values["Origin"] = a.config.PublicURL
	values["Style"] = template.CSS(consentCSS)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = consent.Execute(w, values)
}
func (a *LocalAuth) loginError(w http.ResponseWriter, status int, title, message string) {
	a.loginPage(w, status, map[string]any{"ErrorTitle": title, "Error": message})
}

func (a *LocalAuth) startLogin(w http.ResponseWriter, r *http.Request) {
	if err := a.validateRequest(r); err != nil {
		a.loginError(w, 400, "This connection request is invalid", "Return to your connector and start a new connection.")
		return
	}
	setCallbackPolicy(w, r.URL.Query().Get("redirect_uri"))
	ar, err := a.provider.NewAuthorizeRequest(r.Context(), r)
	if err != nil {
		a.loginError(w, 400, "This connection request is invalid", "Return to your connector and start a new connection.")
		return
	}
	if !ar.GetRequestedScopes().Has(ReadScope) {
		a.loginError(w, 400, "Access could not be requested", "The connector did not request the required access.")
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
		a.loginError(w, 429, "Too many connection requests", "Wait a moment before trying again.")
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
	a.loginPage(w, http.StatusOK, map[string]any{"Client": a.clients[ar.GetClient().GetID()].name, "Flow": flow, "CanWrite": ar.GetRequestedScopes().Has(WriteScope)})
}

func (a *LocalAuth) finishLogin(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != a.config.PublicURL {
		a.loginError(w, 403, "Your connection could not continue", "The sign-in request could not be verified. Start the connection again from your connector.")
		return
	}
	if r.URL.RawQuery != "" || r.ParseForm() != nil {
		a.loginError(w, 400, "Your connection could not continue", "The sign-in request was invalid. Start the connection again from your connector.")
		return
	}
	for _, values := range r.PostForm {
		if len(values) != 1 {
			a.loginError(w, 400, "Your connection could not continue", "The sign-in request was invalid. Start the connection again from your connector.")
			return
		}
	}
	cookie, err := r.Cookie("__Host-workflow-login")
	flow := r.PostForm.Get("flow")
	if err != nil || flow == "" || cookie.Value != flow {
		a.loginError(w, 403, "Your connection could not continue", "The login session is missing or no longer valid.")
		return
	}
	a.mu.Lock()
	pending, ok := a.pending[flow]
	a.mu.Unlock()
	if !ok || time.Now().After(pending.expires) {
		a.loginError(w, 400, "Your connection expired", "For your security, connection requests expire after a few minutes.")
		return
	}
	original, _ := http.NewRequestWithContext(r.Context(), "GET", a.config.PublicURL+"/authorize?"+pending.query, nil)
	if err := a.validateRequest(original); err != nil {
		a.loginError(w, 400, "This connection request is invalid", "Return to your connector and start a new connection.")
		return
	}
	setCallbackPolicy(w, original.URL.Query().Get("redirect_uri"))
	ar, err := a.provider.NewAuthorizeRequest(r.Context(), original)
	if err != nil {
		a.loginError(w, 400, "This connection request is invalid", "Return to your connector and start a new connection.")
		return
	}
	got, want := sha256.Sum256([]byte(r.PostForm.Get("password"))), sha256.Sum256([]byte(a.credentials.LoginPassword))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		w.Header().Set("Referrer-Policy", "same-origin")
		a.loginPage(w, 401, map[string]any{"Client": a.clients[ar.GetClient().GetID()].name, "Flow": flow, "CanWrite": ar.GetRequestedScopes().Has(WriteScope), "FormError": "That password did not match. Check your Gate login password and try again."})
		return
	}
	a.mu.Lock()
	delete(a.pending, flow)
	a.mu.Unlock()
	for _, scope := range ar.GetRequestedScopes() {
		ar.GrantScope(scope)
	}
	ar.GrantAudience(a.config.PublicURL + "/mcp")
	response, err := a.provider.NewAuthorizeResponse(r.Context(), ar, &fosite.DefaultSession{Subject: a.clients[ar.GetClient().GetID()].identity})
	if err != nil {
		a.loginError(w, 400, "Your connection could not be completed", "Start the connection again from your connector. If this continues, contact the Gate operator.")
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
	digest := sha256.Sum256([]byte(consentCSS))
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'sha256-"+base64.StdEncoding.EncodeToString(digest[:])+"'; form-action "+action+"; frame-ancestors 'none'; base-uri 'none'")
}
