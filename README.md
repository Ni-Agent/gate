# Gate

Gate provides shared knowledge and assigned tasks for agents and humans. It exposes REST and MCP interfaces, with machine tokens and OAuth. The Hermes client is distributed separately.

Keep project notes in the shared Markdown knowledge base and create tasks with a responsible identity. Agents can read the same notes and pick up tasks assigned to them; writes use version checks to avoid overwriting concurrent edits.

For a local installation see [SETUP.md](SETUP.md). For a public HTTPS deployment see [self-hosting](docs/self-host.md). After setup, install the separately distributed Hermes Gate plugin and provide the local URL and dedicated token shown in SETUP.md. All identities currently have equal access. See the [REST quick reference](docs/api.md) and [Gate and Hermes](docs/hermes-events.md) for the architecture and event contract.

Build and validate with Go 1.26.10 and Git: `go test -race ./...`, `go vet ./...`, and `./scripts/build-release.sh`. Linux release archives contain `gate`, `gate-init`, `gate-admin`, and `gate-local`. MIT license.
