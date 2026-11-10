# Set up Gate locally

For Linux amd64 or arm64, extract the matching release archive. Install Git and curl. In the extracted directory run:

```sh
./gate-local setup
./gate-local run
```

Keep the second command running in a terminal. In another terminal, run `./gate-local verify` to check access. Gate listens at `http://127.0.0.1:18084`. The setup creates `~/.local/share/gate`, including an empty workspace, private credentials, and a dedicated Hermes token at `~/.local/share/gate/config/hermes.token`. Supply that URL and token to the Hermes plugin using its setup instructions. In a private terminal, run `cat ~/.local/share/gate/config/hermes.token` and enter the value into the plugin prompt. Do not put the token value in a command argument or shared document. An alternative location can be selected consistently for both commands with `GATE_HOME=/absolute/path`.

Stop Gate with Ctrl-C. Run `./gate-local run` to start it again; workspace and OAuth sessions persist. Do not run setup again for an existing instance. To upgrade, replace the binaries and retain the instance directory. Token changes require a Gate restart. The operator token and OAuth credentials in `config` grant access; protect the entire instance directory. Gate identities have equal access.

For a public HTTPS deployment and optional ChatGPT OAuth, see [self-hosting](docs/self-host.md). A local HTTP origin is only accepted for literal `127.0.0.1`; use HTTPS for remote clients. The local setup does not install a background service.
