# Local setup

Clone this repository and run commands from its root. No shared workspace checkout is required.

1. Use the Go version declared by [go.mod](../../go.mod), including its `toolchain` directive; generic methods require the declared Go 1.27 toolchain.
2. Run `go mod download`.
3. Start a Docker-compatible daemon accessible to testcontainers and verify `docker info`. Integration tests provision PostgreSQL 16 containers themselves.
4. For the playground, provide a local dashboard token through your environment and run `go run ./cmd/pgkit-playground`. Open `http://localhost:8080` with any username and that token as the Basic Auth password. The playground starts its own disposable PostgreSQL container.

## Embedded admin UI

Go-only development uses the committed embedded assets and needs no frontend build. UI work uses the pnpm version in [ui/embedded/package.json](../../ui/embedded/package.json), the root [workspace](../../pnpm-workspace.yaml) and lockfile, and a Node release compatible with the installed Vite version.

```sh
pnpm install --frozen-lockfile
pnpm --dir ui/embedded check
pnpm --dir ui/embedded build
```

The static adapter writes `ui/embedded/build`; Go embeds `adminui/dist`. After an intentional asset change, replace the embedded directory with that build and review the generated diff:

```sh
rm -rf adminui/dist
cp -R ui/embedded/build adminui/dist
```

Run the admin API tests and playground afterward. Keep frontend source and its embedded output in the same PR.

## Agent clients

Codex, OpenCode and Grok Build read the local `AGENTS.md` directly. Claude Code loads it through the committed [.claude/settings.json](../../.claude/settings.json) SessionStart hook, which resolves the Git root and prints that guide using only Git and the shell. Start from this repository or a nested directory.

Approve repository trust through the client when required, and start a new session after changing hooks or installed skills so the client reloads them. Trust and login are developer-controlled. Keep personal overrides in ignored `.claude/settings.local.json`; no shared-workspace checkout or external skill installer is required.
