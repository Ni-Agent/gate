// gate-init creates a new standalone instance; no running service or cloud API is needed.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"workflow-api/internal/gate"
	"workflow-api/internal/mcptest"
)

func secret() string {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		log.Fatal(e)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
func save(path string, v any) {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		log.Fatal(e)
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		log.Fatal(e)
	}
	if _, e = f.Write(b); e != nil {
		log.Fatal(e)
	}
	if e = f.Sync(); e != nil {
		log.Fatal(e)
	}
	if e = f.Close(); e != nil {
		log.Fatal(e)
	}
}
func main() {
	config := flag.String("config", "/etc/gate", "new private configuration directory")
	data := flag.String("data", "/var/lib/gate", "new data directory")
	url := flag.String("public-url", "", "public HTTPS origin")
	owner := flag.String("owner", "human:owner", "owner identity")
	flag.Parse()
	if e := (mcptest.Config{PublicURL: *url}).Validate(); e != nil {
		log.Fatal(e)
	}
	for _, p := range []string{*config, *data} {
		entries, e := os.ReadDir(p)
		if e == nil && len(entries) != 0 {
			log.Fatal("initialization requires new empty directories: ", p)
		}
		if e != nil && !os.IsNotExist(e) {
			log.Fatal(e)
		}
		if e = os.MkdirAll(p, 0700); e != nil {
			log.Fatal(e)
		}
	}
	s, e := gate.OpenWithOwner(filepath.Join(*data, "workspace.git"), *owner)
	if e != nil {
		log.Fatal(e)
	}
	defer s.Close()
	r := s.Write(*owner, "POST", "/identities", `{"id":"agent:chatgpt","name":"ChatGPT"}`, "", "bootstrap-chatgpt")
	if r.Status != 201 {
		log.Fatal(r.Body)
	}
	token := secret()
	hash := sha256.Sum256([]byte(token))
	save(filepath.Join(*config, "tokens.json"), []gate.MachineToken{{Identity: *owner, SHA256: hex.EncodeToString(hash[:]), ID: "operator"}})
	f, e := os.OpenFile(filepath.Join(*config, "operator.token"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		log.Fatal(e)
	}
	_, e = f.WriteString(token + "\n")
	if e != nil {
		log.Fatal(e)
	}
	if e = f.Close(); e != nil {
		log.Fatal(e)
	}
	cred := mcptest.NewCredentials("https://chatgpt.com/connector_platform_oauth_redirect")
	cred.ClientID = "gate-chatgpt"
	cred.Identity = "agent:chatgpt"
	save(filepath.Join(*config, "credentials.json"), cred)
	json.NewEncoder(os.Stdout).Encode(map[string]string{"url": *url, "config": *config, "data": *data, "owner": *owner, "operator_token_file": filepath.Join(*config, "operator.token"), "oauth_credentials_file": filepath.Join(*config, "credentials.json")})
	fmt.Fprintln(os.Stderr, "Initialized private instance. Run Gate as an unprivileged service user; no secrets printed.")
}
