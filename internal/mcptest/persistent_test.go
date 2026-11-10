package mcptest

import (
	"context"
	"encoding/json"
	"github.com/ory/fosite"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func persistentFixture(t *testing.T) *fixture {
	f := setup(t)
	path := filepath.Join(t.TempDir(), "auth", "sessions.json")
	f.login.Close()
	var e error
	f.login, e = NewPersistentAuth(f.config, f.cred, path)
	if e != nil {
		t.Fatal(e)
	} // HTTP routes in setup capture the old auth, so replace the server handler.
	f.server.Config.Handler = f.login.Handler()
	t.Cleanup(func() { f.login.Close() })
	return f
}
func restartAuth(t *testing.T, f *fixture) {
	t.Helper()
	path := f.login.tokens.path
	f.login.Close()
	var e error
	f.login, e = NewPersistentAuth(f.config, f.cred, path)
	if e != nil {
		t.Fatal(e)
	}
	f.server.Config.Handler = f.login.Handler()
}
func issue(t *testing.T, f *fixture) (string, string) {
	t.Helper()
	code, v := f.loginCode(t, ReadScope+" "+WriteScope)
	status, body := f.exchange(t, code, v, f.cred.ClientSecret, f.config.PublicURL+"/mcp")
	if status != 200 {
		t.Fatal(status, body)
	}
	var out map[string]any
	json.Unmarshal([]byte(body), &out)
	if out["expires_in"].(float64) > 3600 {
		t.Fatal("access token lifetime unexpectedly extended")
	}
	a, _ := out["access_token"].(string)
	r, _ := out["refresh_token"].(string)
	if a == "" || r == "" {
		t.Fatal("missing access or refresh token")
	}
	return a, r
}
func refresh(t *testing.T, f *fixture, token, secret, resource string) (int, map[string]any) {
	t.Helper()
	form := url.Values{"grant_type": {"refresh_token"}, "client_id": {f.cred.ClientID}, "client_secret": {secret}, "refresh_token": {token}}
	if resource != "" {
		form.Set("resource", resource)
	}
	status, _, body := f.request(t, "POST", "/token", form.Encode(), "", map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	var out map[string]any
	json.Unmarshal([]byte(body), &out)
	return status, out
}
func TestPersistentRefreshLifecycle(t *testing.T) {
	f := persistentFixture(t)
	a, r := issue(t, f)
	if _, e := f.login.Verify(context.Background(), a, nil); e != nil {
		t.Fatal(e)
	}
	disk, e := os.ReadFile(f.login.tokens.path)
	if e != nil {
		t.Fatal(e)
	}
	for _, secret := range []string{a, r, f.cred.ClientSecret, f.cred.LoginPassword} {
		if strings.Contains(string(disk), secret) {
			t.Fatal("raw credential persisted in session state")
		}
	}
	if info, _ := os.Stat(f.login.tokens.path); info.Mode().Perm() != 0600 {
		t.Fatal("state not private")
	}
	restartAuth(t, f)
	if _, e := f.login.Verify(context.Background(), a, nil); e != nil {
		t.Fatal("restart invalidated token", e)
	}
	// Expire only the access credential; renewal must work without another login.
	for sig, b := range f.login.tokens.state.Access {
		req, _ := f.login.tokens.unpack(b)
		req.GetSession().SetExpiresAt(fosite.AccessToken, time.Now().Add(-time.Minute))
		raw, _ := json.Marshal(req)
		f.login.tokens.state.Access[sig] = raw
	}
	f.login.tokens.save()
	if _, e := f.login.Verify(context.Background(), a, nil); e == nil {
		t.Fatal("expired access accepted")
	}
	code, out := refresh(t, f, r, f.cred.ClientSecret, "")
	if code != 200 {
		t.Fatal(code, out)
	}
	a2 := out["access_token"].(string)
	r2 := out["refresh_token"].(string)
	if r == r2 {
		t.Fatal("refresh token not rotated")
	}
	restartAuth(t, f)
	if _, e := f.login.Verify(context.Background(), a2, nil); e != nil {
		t.Fatal(e)
	}
	// Replay the old credential: revoke the entire grant, including the latest pair.
	if code, _ := refresh(t, f, r, f.cred.ClientSecret, ""); code == 200 {
		t.Fatal("old refresh replay accepted")
	}
	restartAuth(t, f)
	if _, e := f.login.Verify(context.Background(), a2, nil); e == nil {
		t.Fatal("replayed grant still valid after restart")
	}
	if code, _ := refresh(t, f, r2, f.cred.ClientSecret, ""); code == 200 {
		t.Fatal("revoked refresh accepted")
	}
}
func TestRefreshBoundaries(t *testing.T) {
	f := persistentFixture(t)
	_, r := issue(t, f)
	if code, _ := refresh(t, f, r, "incorrect", ""); code == 200 {
		t.Fatal("wrong secret accepted")
	}
	if code, _ := refresh(t, f, r, f.cred.ClientSecret, "https://evil.invalid/mcp"); code == 200 {
		t.Fatal("wrong resource accepted")
	}
	if code, _ := refresh(t, f, r, f.cred.ClientSecret, ""); code != 200 {
		t.Fatal("boundary failures consumed grant")
	}
	_, r = issue(t, f)
	for sig, v := range f.login.tokens.state.Refresh {
		req, _ := f.login.tokens.unpack(v.Request)
		req.GetSession().SetExpiresAt(fosite.RefreshToken, time.Now().Add(-time.Hour))
		v.Request, _ = json.Marshal(req)
		f.login.tokens.state.Refresh[sig] = v
	}
	if code, _ := refresh(t, f, r, f.cred.ClientSecret, ""); code == 200 {
		t.Fatal("expired refresh accepted")
	}
}
func TestPersistenceFailureAndRevocation(t *testing.T) {
	f := persistentFixture(t)
	a, _ := issue(t, f)
	if e := f.login.RevokeAll(); e != nil {
		t.Fatal(e)
	}
	restartAuth(t, f)
	if _, e := f.login.Verify(context.Background(), a, nil); e == nil {
		t.Fatal("revocation lost on restart")
	}
	_, r := issue(t, f)
	originalPath := f.login.tokens.path
	f.login.tokens.path = filepath.Join(t.TempDir(), "missing", "state.json")
	code, _ := refresh(t, f, r, f.cred.ClientSecret, "")
	if code != 503 {
		t.Fatal("persistence failure released credential", code)
	}
	f.login.tokens.path = originalPath
	if _, e := f.login.Verify(context.Background(), a, nil); e == nil {
		t.Fatal("failed provider remained available")
	}
}
func TestStateLockAndCorruption(t *testing.T) {
	f := persistentFixture(t)
	if a, e := NewPersistentAuth(f.config, f.cred, f.login.tokens.path); e == nil {
		a.Close()
		t.Fatal("second state owner allowed")
	}
	path := f.login.tokens.path
	f.login.Close()
	os.WriteFile(path, []byte("broken"), 0600)
	if a, e := NewPersistentAuth(f.config, f.cred, path); e == nil {
		a.Close()
		t.Fatal("corrupt state silently reset")
	}
}
