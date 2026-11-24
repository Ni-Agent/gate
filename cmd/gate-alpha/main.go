// gate-alpha defaults to stdio MCP. Optional loopback HTTP is for separately
// authorized manual review; it is never started by this task or its tests.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"gate-alpha/internal/core"
	"gate-alpha/internal/transport"
)

func main() {
	dir := flag.String("store", "", "required: private isolated store directory (never use production storage)")
	actor := flag.String("actor", "agent:hermes", "local authenticated acting identity; stdio process access is the trust boundary")
	identities := flag.String("identities", "agent:hermes,agent:mai,human:d0f", "comma-separated trusted workspace identities")
	version := flag.Bool("version", false, "print test build version")
	address := flag.String("http", "", "optional explicit loopback IP:port; requires GATE_ALPHA_TEST_TOKEN; not production TLS/OAuth")
	maxJSON := flag.Int64("max-json-bytes", 0, "optional operational wire limit; 0 disables")
	maxBinary := flag.Int64("max-binary-bytes", 0, "optional operational upload limit; 0 disables")
	flag.Parse()
	if *maxJSON < 0 || *maxBinary < 0 || uint64(*maxJSON) > uint64(^uint(0)>>1)-2 {
		fmt.Fprintln(os.Stderr, "nonnegative representable operational limits required")
		os.Exit(2)
	}
	if *version {
		fmt.Println(transport.Version)
		return
	}
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "--store is required; test-only build defaults to stdio without listener")
		os.Exit(2)
	}
	if *address != "" {
		if e := validLoopback(*address); e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(2)
		}
		if len(os.Getenv("GATE_ALPHA_TEST_TOKEN")) < 16 {
			fmt.Fprintln(os.Stderr, "HTTP review mode requires runtime GATE_ALPHA_TEST_TOKEN (at least 16 bytes)")
			os.Exit(2)
		}
	}
	ids := strings.Split(*identities, ",")
	found := false
	for _, id := range ids {
		if id == *actor {
			found = true
		}
	}
	if !found {
		fmt.Fprintln(os.Stderr, "actor must be in --identities")
		os.Exit(2)
	}
	store, e := core.New(*dir, ids)
	if e != nil {
		fmt.Fprintln(os.Stderr, "cannot open isolated store:", e)
		os.Exit(1)
	}
	defer store.Close()
	if *address != "" {
		server := &http.Server{Addr: *address, Handler: transport.NewWithOptions(store, map[string]string{os.Getenv("GATE_ALPHA_TEST_TOKEN"): *actor}, transport.Options{MaxJSONBytes: *maxJSON, MaxBinaryBytes: *maxBinary}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
		fmt.Fprintln(os.Stderr, "test-only loopback HTTP review mode; no production compatibility is claimed")
		if e = server.ListenAndServe(); e != nil {
			fmt.Fprintln(os.Stderr, "HTTP review server stopped:", e)
			os.Exit(1)
		}
		return
	}
	e = transport.NewMCP(store, *actor).Run(context.Background(), transport.NewStrictIOTransportWithLimit(os.Stdin, os.Stdout, int(*maxJSON)))
	if e != nil {
		fmt.Fprintln(os.Stderr, "MCP transport failed:", e)
		os.Exit(1)
	}
}
func validLoopback(address string) error {
	host, port, e := net.SplitHostPort(address)
	if e != nil {
		return fmt.Errorf("explicit loopback IP:port required")
	}
	ip, e := netip.ParseAddr(host)
	if e != nil || !ip.IsLoopback() {
		return fmt.Errorf("HTTP review binding must be a numeric loopback IP")
	}
	n, e := strconv.Atoi(port)
	if e != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
		return fmt.Errorf("canonical port in 1..65535 required")
	}
	return nil
}
