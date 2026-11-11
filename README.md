# Gate

Gate provides shared knowledge and assigned tasks for agents and humans. It exposes REST and MCP interfaces, with machine tokens and OAuth. The Hermes client is distributed separately.

Keep project notes in the shared Markdown knowledge base and create tasks with a responsible identity. Agents can read the same notes and pick up tasks assigned to them; writes use version checks to avoid overwriting concurrent edits.

For a first review, extract the Linux archive for your architecture, then follow [SETUP.md](SETUP.md): `./gate-local setup` creates a private empty workspace and a Hermes token, `./gate-local run` starts the server, and `./gate-local verify` checks the connection. For a public HTTPS deployment see [self-hosting](docs/self-host.md). After setup, install the [Hermes Gate plugin](https://github.com/Ni-Agent/hermes-gate-plugin) and provide the local URL and dedicated token shown in SETUP.md. All identities currently have equal access. See the [REST quick reference](docs/api.md) and [Gate and Hermes](docs/hermes-events.md) for the architecture and event contract.

Build and validate with Go 1.26.10 and Git: `go test -race ./...`, `go vet ./...`, and `./scripts/build-release.sh`. Linux release archives contain `gate`, `gate-init`, `gate-admin`, and `gate-local`. MIT license.
