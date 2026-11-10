#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
testroot=$(mktemp -d)
pid=
cleanup() { if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi; rm -rf "$testroot"; }
trap cleanup EXIT HUP INT TERM
for cmd in gate gate-init gate-admin; do go build -buildvcs=false -o "$testroot/$cmd" "./cmd/$cmd"; done
cp scripts/gate-local "$testroot/gate-local"
export GATE_HOME="$testroot/instance"
"$testroot/gate-local" setup
start() {
 "$testroot/gate-local" run >"$testroot/log" 2>&1 & pid=$!
 for n in 1 2 3 4 5 6 7 8 9 10; do
  if curl -fsS http://127.0.0.1:18084/healthz >/dev/null 2>&1; then return; fi
  kill -0 "$pid"; sleep 1
 done
 return 1
}
stop() { kill "$pid"; wait "$pid" || :; pid=; }
start
"$testroot/gate-local" verify
code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18084/whoami)
[ "$code" = 401 ]
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: evil.invalid' http://127.0.0.1:18084/healthz)
[ "$code" = 403 ]
# Header file keeps the test token out of the process argument list.
printf 'Authorization: Bearer %s\n' "$(cat "$GATE_HOME/config/hermes.token")" > "$testroot/header"
chmod 600 "$testroot/header"
curl -fsS -H @"$testroot/header" http://127.0.0.1:18084/whoami | grep -q 'agent:hermes'
curl -fsS -o /dev/null -X PUT -H @"$testroot/header" -H 'Content-Type: text/markdown' -H 'If-None-Match: *' -H 'Idempotency-Key: install-test' --data '# Fresh instance' http://127.0.0.1:18084/kb/projects/test
stop
start
curl -fsS -H @"$testroot/header" http://127.0.0.1:18084/kb/projects/test | grep -q 'Fresh instance'
stop
# A new instance must fail closed until the Grok identity is registered.
cp examples/grok-client.json "$testroot/grok.json"
chmod 600 "$testroot/grok.json"
grok_start() {
 "$testroot/gate" -addr 127.0.0.1:18084 -repo "$GATE_HOME/data/workspace.git" -public-url http://127.0.0.1:18084 -credentials "$GATE_HOME/config/credentials.json" -tokens "$GATE_HOME/config/tokens.json" -oauth-state "$GATE_HOME/data/auth/sessions.json" -grok-client-metadata "$testroot/grok.json"
}
if grok_start >"$testroot/grok-log" 2>&1; then echo "Grok unexpectedly started without its identity" >&2; exit 1; fi
grep -q 'Grok OAuth identity must be registered' "$testroot/grok-log"
start
printf 'Authorization: Bearer %s\n' "$(cat "$GATE_HOME/config/operator.token")" > "$testroot/header"
curl -fsS -o /dev/null -H @"$testroot/header" -H 'Content-Type: application/json' -H 'Idempotency-Key: grok-test' --data '{"id":"agent:grok-bot","name":"Grok"}' http://127.0.0.1:18084/identities
stop
grok_start >"$testroot/grok-log" 2>&1 & pid=$!
for n in 1 2 3 4 5 6 7 8 9 10; do
 if curl -fsS http://127.0.0.1:18084/.well-known/oauth-authorization-server | grep -q '"client_id_metadata_document_supported":true'; then break; fi
 kill -0 "$pid"; [ "$n" != 10 ]; sleep 1
done
stop
printf '%s\n' 'Fresh setup, authentication, write, restart, and Grok enablement passed.' 
