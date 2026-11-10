# Connect Grok at grok.com to Gate

Grok uses a public OAuth client with S256 PKCE. No client ID or secret needs to be
entered in Grok's connector form. Gate's login page asks for the existing owner's
Gate login password and attributes this connection to `agent:grok-bot`.

## Operator setup

Use an executable containing this feature. Preserve existing systemd drop-ins, OAuth credentials,
signing state, sessions, machine credentials and KB; initialization is not an
upgrade step. Register `agent:grok-bot` if it is not already present. The following runs on the Gate host using the private operator token and a private temporary header file:

```sh
umask 077
header=$(mktemp)
sudo sh -c 'printf "Authorization: Bearer %s\n" "$(cat /etc/gate/operator.token)"' > "$header"
curl --fail -H @"$header" -H "Content-Type: application/json" \
  -H "Idempotency-Key: register-grok-identity" \
  --data '{"id":"agent:grok-bot","name":"Grok"}' \
  https://gate.example.org/identities
rm -f "$header"
```

A separate machine token is not needed for this connection.

Review the current document at https://grok.com/oauth/mcp-client.json against
`examples/grok-client.json`. This snapshot was verified against that HTTPS source
on 2026-11-10. Install the reviewed file, then adapt the optional service drop-in:

```sh
sudo install -m 0600 -o root -g root examples/grok-client.json /etc/gate/grok-client.json
sudo mkdir -p /etc/systemd/system/gate.service.d
sudo install -m 0644 deploy/gate-grok.example.conf /etc/systemd/system/gate.service.d/grok.conf
# Edit grok.conf to retain your current origin and every existing ExecStart flag.
sudo systemctl daemon-reload
sudo systemctl restart gate
curl --fail https://gate.example.org/.well-known/oauth-authorization-server
```

The drop-in adds `-grok-client-metadata` through systemd `LoadCredential`. It must
not replace the existing ChatGPT credentials or OAuth state. Gate accepts only
Grok's exact client ID, the reviewed exact callbacks, the authorization-code and
refresh grants, and token endpoint authentication `none`. Changes to validated client fields or callbacks fail startup and require review. Review other metadata changes as well. Removing this optional flag
and drop-in disables Grok without resetting ChatGPT sessions.

## Connect

1. Open https://grok.com/connectors and select **New Connector → Custom**.
2. Enter your public Gate MCP URL, such as `https://gate.example.org/mcp`.
3. Complete Gate's **Connect Grok to Gate** page using the owner's Gate login
   password, which is separate from a machine token or ChatGPT client secret.
4. Ask Grok to use Gate to read `/identities` or list project notes.

For a Grok Business team, an admin first provisions **Other** with the MCP URL in
the xAI console's connector settings, then members connect their accounts.

## Authentication details

When enabled, discovery advertises `client_id_metadata_document_supported: true`
and `none` alongside the existing ChatGPT authentication methods. Only the
operator-pinned Grok document is trusted. Gate does not fetch arbitrary URLs or
provide open dynamic registration. The metadata snapshot is loaded at startup;
operators must re-check the published document when updating the integration.
This deliberately supports the hardened service's loopback-only egress policy.

Published client ID: `https://grok.com/oauth/mcp-client.json`.
Published callbacks (exact matching, including trailing slashes):

- `https://grok.com/connectors-oauth-exchange-code/`
- `https://console.x.ai/connectors-oauth-exchange-code/`

PKCE, resource audience, code/client/callback binding, secure cookie and origin
checks, one-hour access expiry, durable refresh rotation and replay revocation
apply to both clients. A Grok grant cannot redeem or renew a ChatGPT grant.
The two identities have the same existing Gate access level. Neither metadata
nor a public client ID is proof of a particular human's identity; the owner login
and explicit consent grant access.

References: [Grok connectors](https://docs.x.ai/grok/connectors),
[Grok client document](https://grok.com/oauth/mcp-client.json),
[MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization).
