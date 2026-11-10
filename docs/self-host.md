# New instance setup

This is the standalone Gate server. The Hermes plugin is a separate client and
connects to this HTTPS origin. Requires Linux, Git, systemd, Caddy, a public DNS
record and inbound HTTPS (plus HTTP if using Caddy's HTTP certificate challenge).

Use the binaries from a matching release archive, or build `gate`, `gate-admin` and `gate-init` from source. From the directory containing the binaries:

```sh
sudo install -D -m 0755 gate /usr/local/libexec/gate
sudo install -m 0755 gate-admin gate-init /usr/local/bin/
sudo useradd --system --home-dir /var/lib/gate --shell /usr/sbin/nologin gate
sudo gate-init -public-url https://gate.example.org
sudo chown -R gate:gate /var/lib/gate
```

Use new empty config/data directories. `gate-init` refuses an existing instance;
do not use it for upgrades. It initializes a service-owned bare Git repository,
a local owner, a ChatGPT identity, private OAuth configuration and a private
operator machine token. Secret values are written to mode-0600 files, not printed.
It starts with an empty workspace.

Edit `deploy/gate.example.service` and `deploy/Caddyfile.example`, replacing
`gate.example.org` with your own HTTPS hostname. Install the unit as
`/etc/systemd/system/gate.service`, and put the Caddy site block into your existing
Caddy configuration. Preserve other services/site blocks. Then:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now gate
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
curl --fail https://gate.example.org/healthz
```

Keep Gate bound to loopback. Caddy terminates TLS on 443 and proxies only the listed
routes to `127.0.0.1:18084`. The example systemd unit uses `LoadCredential` so the
unprivileged service can consume root-owned credential files. Gate's data and
OAuth session state remain private and writable only by the service user.

Create the first Hermes credential:

```sh
sudo gate-admin -url https://gate.example.org \
  -identity agent:hermes -out /etc/gate/hermes.token
sudo systemctl restart gate
```

Give the user the URL and token through a private channel. The native Hermes
installer prompts for them; no browser OAuth login is needed for this machine
credential. Never put token values in a KB page, release file or command argument.
See `hermes-events.md` and the plugin README for listener/replay and token rotation.

For ChatGPT use the server's `/mcp` URL and the privately generated OAuth client ID
and secret in `/etc/gate/credentials.json`. On its authorize page the human enters
the generated login password from that same private file. This is distinct from
the agent's machine token. Existing OAuth callback configuration targets ChatGPT;
see [Grok OAuth](features/grok-oauth.md) for optional Grok access.

All data identities have equal access. Administration of machine credentials is
an OS-admin operation over SSH/sudo; there is no public dashboard or online token
mint endpoint. Root access already controls this host and its authentication files.

Upgrades preserve `/var/lib/gate/workspace.git`, `/var/lib/gate/auth` and
`/etc/gate`. Preserve existing systemd drop-ins and all service flags. Replace the executable, restart, and verify readiness; never rerun
initialization. Keep the previous executable for rollback. 
