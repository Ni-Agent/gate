// gate-admin is an OS-admin operation, not an HTTP admin API. Only token hashes
// reach the server; it requires write access to the private token file.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"workflow-api/internal/gate"
)

func fatal(e error) {
	if e != nil {
		log.Fatal(e)
	}
}
func main() {
	action := flag.String("action", "create-agent", "create-agent, list-tokens, revoke-token")
	public := flag.String("url", "", "Gate HTTPS origin")
	tokenFile := flag.String("operator-token-file", "/etc/gate/operator.token", "private operator credential")
	hashFile := flag.String("tokens", "/etc/gate/tokens.json", "private token hash file")
	identity := flag.String("identity", "", "agent identity to provision")
	out := flag.String("out", "", "new private output token file")
	id := flag.String("token-id", "", "token ID to revoke")
	flag.Parse()
	f, e := os.OpenFile(*hashFile+".admin-lock", os.O_CREATE|os.O_RDWR, 0600)
	fatal(e)
	defer f.Close()
	fatal(syscall.Flock(int(f.Fd()), syscall.LOCK_EX))
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	info, e := os.Lstat(*hashFile)
	fatal(e)
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		log.Fatal("token file must be private and regular")
	}
	raw, e := os.ReadFile(*hashFile)
	fatal(e)
	var tokens []gate.MachineToken
	fatal(json.Unmarshal(raw, &tokens))
	for n := range tokens {
		if len(tokens[n].SHA256) != 64 {
			log.Fatal("invalid token hash file")
		}
		if tokens[n].ID == "" {
			tokens[n].ID = tokens[n].SHA256[:12]
		}
	}
	if *action == "list-tokens" {
		for _, t := range tokens {
			fmt.Printf("%s\t%s\t%s\n", t.ID, t.Identity, t.CreatedAt)
		}
		return
	}
	if *action == "create-agent" {
		u, e := url.Parse(*public)
		fatal(e)
		if (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || net.ParseIP(u.Hostname()).IsLoopback()))) || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			log.Fatal("--url must be an HTTPS origin")
		}
		if *identity == "" || *out == "" {
			log.Fatal("--identity and --out required")
		}
		if _, e = os.Lstat(*out); !os.IsNotExist(e) {
			log.Fatal("output already exists or is inaccessible")
		}
		info, e := os.Lstat(*tokenFile)
		fatal(e)
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			log.Fatal("operator credential must be private")
		}
		operator, e := os.ReadFile(*tokenFile)
		fatal(e)
		body, _ := json.Marshal(gate.Identity{ID: *identity, Name: *identity})
		req, e := http.NewRequest("POST", *public+"/identities", strings.NewReader(string(body)))
		fatal(e)
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(operator)))
		req.Header.Set("Content-Type", "application/json")
		b := make([]byte, 32)
		_, e = rand.Read(b)
		fatal(e)
		key := hex.EncodeToString(b)
		req.Header.Set("Idempotency-Key", "admin-"+key)
		client := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		res, e := client.Do(req)
		fatal(e)
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 201 && res.StatusCode != 409 {
			log.Fatal("identity registration failed, status ", res.StatusCode)
		}
		// A 409 can only mean an already registered ID for this fresh operation key.
		_, e = rand.Read(b)
		fatal(e)
		token := base64.RawURLEncoding.EncodeToString(b)
		h := sha256.Sum256([]byte(token))
		newID := hex.EncodeToString(h[:])[:12]
		tokens = append(tokens, gate.MachineToken{Identity: *identity, SHA256: hex.EncodeToString(h[:]), ID: newID, CreatedAt: time.Now().UTC().Format(time.RFC3339)})
		output, e := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		fatal(e)
		_, e = output.WriteString(token + "\n")
		fatal(e)
		fatal(output.Sync())
		fatal(output.Close())
		fmt.Printf("Gate URL: %s\nIdentity: %s\nToken ID: %s\nToken file: %s\n", *public, *identity, newID, *out)
	} else if *action == "revoke-token" {
		found := false
		kept := tokens[:0]
		for _, t := range tokens {
			if t.ID == *id {
				found = true
			} else {
				kept = append(kept, t)
			}
		}
		if !found {
			log.Fatal("unknown token ID")
		}
		tokens = kept
	} else {
		log.Fatal("unsupported action")
	}
	data, e := json.MarshalIndent(tokens, "", "  ")
	fatal(e)
	tmp, e := os.CreateTemp(filepath.Dir(*hashFile), ".gate-tokens-")
	fatal(e)
	defer os.Remove(tmp.Name())
	_, e = tmp.Write(data)
	fatal(e)
	fatal(tmp.Sync())
	fatal(tmp.Close())
	fatal(os.Rename(tmp.Name(), *hashFile))
	dir, e := os.Open(filepath.Dir(*hashFile))
	fatal(e)
	fatal(dir.Sync())
	dir.Close()
	fmt.Println("Token hashes updated. Restart Gate to load credentials and close revoked streams.")
}
