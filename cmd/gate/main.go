package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
	"workflow-api/internal/content"
	"workflow-api/internal/gate"
	"workflow-api/internal/mcptest"
)

func privateJSON(path string, v any) error {
	info, e := os.Stat(path)
	if e != nil {
		return e
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%s must be a private regular file", path)
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func main() {
	addr := flag.String("addr", "127.0.0.1:18084", "loopback listen address")
	repo := flag.String("repo", "", "service-owned bare Git repository")
	public := flag.String("public-url", "", "public HTTPS origin")
	credentials := flag.String("credentials", "", "private OAuth credentials JSON")
	tokens := flag.String("tokens", "", "private machine token hash JSON; optional")
	mint := flag.String("mint-token", "", "registered identity to issue a token for; writes token once to stdout and exits")
	authState := flag.String("oauth-state", "", "private durable OAuth state JSON outside KB")
	revokeOAuth := flag.Bool("revoke-oauth", false, "revoke all OAuth sessions and exit; stop the service first")
	version := flag.Bool("version", false, "show version")
	flag.Parse()
	if *version {
		fmt.Println(gate.Version)
		return
	}
	if *repo == "" {
		log.Fatal("-repo required; never point it at the live vault")
	}
	store, e := gate.Open(*repo)
	if e != nil {
		log.Fatal(e)
	}
	defer store.Close()
	if *mint != "" {
		if *tokens == "" || !store.HasIdentity(*mint) {
			log.Fatal("registered identity and -tokens required")
		}
		list := []gate.MachineToken{}
		if _, e := os.Stat(*tokens); e == nil {
			if e = privateJSON(*tokens, &list); e != nil {
				log.Fatal(e)
			}
		} else if !os.IsNotExist(e) {
			log.Fatal(e)
		}
		b := make([]byte, 32)
		if _, e = rand.Read(b); e != nil {
			log.Fatal(e)
		}
		token := base64.RawURLEncoding.EncodeToString(b)
		hash := sha256.Sum256([]byte(token))
		list = append(list, gate.MachineToken{Identity: *mint, SHA256: hex.EncodeToString(hash[:])})
		out, _ := json.MarshalIndent(list, "", "  ")
		f, e := os.OpenFile(*tokens+".new", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			log.Fatal(e)
		}
		if _, e = f.Write(out); e != nil {
			log.Fatal(e)
		}
		if e = f.Sync(); e != nil {
			log.Fatal(e)
		}
		if e = f.Close(); e != nil {
			log.Fatal(e)
		}
		if e = os.Rename(*tokens+".new", *tokens); e != nil {
			log.Fatal(e)
		}
		fmt.Println(token)
		return
	}
	if e = content.ValidateLocalBind(*addr); e != nil {
		log.Fatal(e)
	}
	var cred mcptest.Credentials
	if e = privateJSON(*credentials, &cred); e != nil {
		log.Fatal(e)
	}
	if *authState == "" {
		log.Fatal("-oauth-state required; do not use transient OAuth sessions")
	}
	if cred.Identity == "" {
		cred.Identity = "agent:chatgpt"
	}
	if !store.HasIdentity(cred.Identity) {
		log.Fatal("OAuth identity must be registered in Gate")
	}
	login, e := mcptest.NewPersistentAuth(mcptest.Config{PublicURL: *public}, cred, *authState)
	if e != nil {
		log.Fatal(e)
	}
	defer login.Close()
	if *revokeOAuth {
		if e := login.RevokeAll(); e != nil {
			log.Fatal(e)
		}
		fmt.Println("Revoked all OAuth sessions")
		return
	}
	list := []gate.MachineToken{}
	if *tokens != "" {
		if e = privateJSON(*tokens, &list); e != nil {
			log.Fatal(e)
		}
	}
	verify, e := gate.Verifier(store, list, login.Verify)
	if e != nil {
		log.Fatal(e)
	}
	handler, e := gate.Handler(store, *public, verify)
	if e != nil {
		log.Fatal(e)
	}
	mux := http.NewServeMux()
	for _, p := range []string{"/authorize", "/token", "/.well-known/oauth-authorization-server"} {
		mux.Handle(p, login.Handler())
	}
	mux.Handle("/", handler)
	handlerForServer := http.Handler(mux)
	if origin, _ := url.Parse(*public); origin.Scheme == "http" {
		handlerForServer = requireHost(origin.Host, handlerForServer)
	}
	srv := &http.Server{Addr: *addr, Handler: handlerForServer, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(c)
	}()
	log.Printf("Gate %s listening %s", gate.Version, *addr)
	if e = srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		log.Fatal(e)
	}
}

// Local HTTP must not accept DNS-rebound requests with an attacker-controlled host.
func requireHost(host string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != host {
			http.Error(w, "invalid host", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
