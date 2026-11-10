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
printf '%s\n' 'Fresh setup, authentication, write and restart passed.'
