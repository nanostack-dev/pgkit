# Local setup

Clone this repository and run commands from its root. No shared workspace checkout is required.

1. Use the Go version declared by [go.mod](../../go.mod), including its `toolchain` directive; generic methods require the declared Go 1.27 toolchain.
2. Run `go mod download`.
3. Start a Docker-compatible daemon accessible to testcontainers and verify `docker info`. Integration tests provision PostgreSQL 16 containers themselves.
4. For the playground, run `go run ./cmd/pgkit-playground` and open `http://127.0.0.1:18081` with any username and the password `change-me` (or the `PGKIT_DASHBOARD_TOKEN` you set). It starts its own disposable PostgreSQL container and seeds orders that succeed, fail, wait for approval and get cancelled. `PGKIT_PLAYGROUND_ADDR` changes the listen address, for example a Tailscale IP to review the UI from a phone.

## Embedded admin UI

Go-only development uses the committed embedded assets and needs no frontend build. UI work uses the pnpm version in [ui/embedded/package.json](../../ui/embedded/package.json), the root [workspace](../../pnpm-workspace.yaml) and lockfile, and a Node release compatible with the installed Vite version.

```sh
pnpm install --frozen-lockfile
pnpm --dir ui/embedded check
pnpm --dir ui/embedded lint
pnpm --dir ui/embedded build
```

`check` type-checks the app and its Vite config; `build` runs `check` before `vite build`. The app is React with Vite; see the [admin UI](../technical/admin-ui.md) for its structure.

For live development, start the playground on the port the dev server proxies to, then run Vite:

```sh
PGKIT_PLAYGROUND_ADDR=127.0.0.1:18083 go run ./cmd/pgkit-playground
pnpm --dir ui/embedded dev
```

Vite serves `http://127.0.0.1:4173` and forwards `/api` to `PGKIT_ADMIN_API` (default `http://127.0.0.1:18083`) with Basic Auth from `PGKIT_DASHBOARD_TOKEN` (default `change-me`). Without a backend, add `?data=demo`, `?data=worst` or `?data=empty` to the URL, or use the toggle at the bottom of the page, to render fixtures; the worst case holds long names, huge JSON, hundreds of checkpoints and a deep run tree. The toggle and fixtures exist only in development builds.

Vite writes `ui/embedded/build`; Go embeds `adminui/dist`. After an intentional asset change, replace the embedded directory with that build and review the generated diff:

```sh
rm -rf adminui/dist
cp -R ui/embedded/build adminui/dist
```

Run the admin API tests and playground afterward. Keep frontend source and its embedded output in the same PR.

## Agent clients

Codex, OpenCode and Grok Build read the local `AGENTS.md` directly. Claude Code loads it through the committed [.claude/settings.json](../../.claude/settings.json) SessionStart hook, which resolves the Git root and prints that guide using only Git and the shell. Start from this repository or a nested directory.

Approve repository trust through the client when required, and start a new session after changing hooks or installed skills so the client reloads them. Trust and login are developer-controlled. Keep personal overrides in ignored `.claude/settings.local.json`; no shared-workspace checkout or external skill installer is required.
