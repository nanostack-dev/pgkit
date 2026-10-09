# Admin UI

`adminui` serves an embedded React single-page app and the JSON API it reads, both behind one handler. Consumers mount `adminui.New(queueClient, adminui.Options{...}).Handler()` (or the `fx/adminui` module) and choose its exposure, token and whether mutations are enabled.

## Screens

| Route | Purpose |
| --- | --- |
| `/` | Health: due/processing/failed/done jobs, run counts by status, throughput for the last hour or 24 hours, runs waiting and what they wait for, recent failures and activity. |
| `/queues` | Per-queue breakdown, job explorer (`queue`, `status`, `search`, `offset` query parameters) and a job sheet opened with `?job=<id>`: payload, attempts and claims, error, linked workflow run, replay and delete. Enqueue lives here. |
| `/workflows` | Per-workflow breakdown and run explorer. Child runs are hidden unless `children=1`. |
| `/workflows/{id}` | Run detail: status, retry or cancel, ancestors, checkpoint timeline (kind, status, attempts, durations, wake and timeout countdowns, signal waits, child runs, errors, output), input, output, run tree and delivered signals. Refreshes while the run or one of its children is unfinished. |
| `/locks` | PostgreSQL advisory locks in the current database with the holding or waiting session. |

Every list and detail polls with TanStack Query (5 s lists, 1.5 s unfinished runs, 3 s locks) and pauses while the tab is hidden. Countdowns use the server clock: responses that carry `now` (database time) set the offset between the browser and PostgreSQL.

## JSON API

All routes are under `/api/dashboard/` and require the token. The response types are the Go structs in [adminui](../../adminui) and their TypeScript mirror in [ui/embedded/src/api/types.ts](../../ui/embedded/src/api/types.ts); keep both in step. Times are UTC RFC 3339 with milliseconds; 64-bit lock keys are strings.

| Method and path | Returns |
| --- | --- |
| `GET config` | `mutations_enabled`, `workflows_enabled`, `page_size`. |
| `GET overview?range=1h\|24h` | Queue summary, due jobs, throughput buckets, recent and failed jobs; workflow counts, throughput, waiting, failed and recent runs (`workflow` is null without a workflow client). |
| `GET queue/queues` | Per-queue counts, due jobs, oldest due time, last activity. |
| `GET queue/jobs?queue=&status=&search=&limit=&offset=` | A page of jobs with payload previews. |
| `GET queue/jobs/{id}` | One job (jobs carry `attempts`, `max_attempts` and `claims`, every claim including snoozes and replays) with its payload (at most 1 MiB, `payload_encoding` json/text/base64, `payload_truncated`) and the workflow `run_id` for `pgworkflow:` jobs. |
| `GET workflow/workflows` | Per-workflow versions and counts by status. |
| `GET workflow/runs?workflow=&status=&parent_run_id=&top_level=true&search=&limit=&offset=` | Run summaries without input/output, with `child_count`, `step_count` and `current_step` (the latest checkpoint that has not succeeded). |
| `GET workflow/runs/{id}` | The run with input/output, its queue `job_id`, checkpoints, up to 100 child summaries and `child_total`, root-first `ancestors` and the 50 newest signals. |
| `GET workflow/runs/{id}/tree` | The run and its descendants depth-first, at most 8 levels and 300 nodes (`truncated`). |
| `GET locks` | Advisory locks joined with `pg_stat_activity`, without query text. |
| `GET snapshot`, `queue/summary`, `queue/locks` | Earlier read endpoints, kept for compatibility. |
| `POST queue/jobs`, `POST queue/jobs/{id}/replay`, `DELETE queue/jobs/{id}`, `POST workflow/runs/{id}/retry`, `POST workflow/runs/{id}/cancel` | Mutations, registered only when mutations are enabled. |

The aggregate read models query `pgqueue_jobs` and `pgworkflow_*` directly with read-only SQL; their tests run against real PostgreSQL, so a schema change in `queue` or `workflow` fails them. Throughput reads `done_at`/`updated_at` and the queue breakdown groups the whole table; neither has a supporting index, so on very large job tables these queries scan. Purging finished jobs and runs keeps them bounded.

## Security invariants

- Basic Auth with any non-empty username and the token as password, compared in constant time.
- Mutations require `X-Requested-With: pgkit-admin-ui` or an `Origin` whose host equals the request host exactly; `EnableMutations=false` (or `PGKIT_DASHBOARD_ENABLE_API=false`) removes the routes, and the UI hides the actions and explains why.
- 5xx responses carry only the status text; 4xx responses carry fixed messages such as `job not found` or `payload is required`, never driver errors.
- Every response sets `X-Content-Type-Options: nosniff`. Hashed assets under `/_app/` are cached as immutable; the HTML is `no-cache`. Text assets of 1 KiB or more are gzip-compressed once and served when the client accepts gzip.

## Frontend

Source is [ui/embedded](../../ui/embedded): Vite, React, React Router, TanStack Query, Base UI primitives (dialogs, drawer, select, menu, popover, tooltip), Sonner toasts, cmdk for the command menu and Tailwind CSS. The build writes `ui/embedded/build`, which is copied to the embedded [adminui/dist](../../adminui/dist) (see [setup](../development/setup.md#embedded-admin-ui)). The decision and its size cost are recorded in [ADR 0002](../adr/0002-react-admin-ui.md).

- Phone-first: bottom tab bar and card rows below 768 px, sidebar above; rows switch to columns with container queries; touch targets are at least 40–44 px; safe-area insets are respected.
- Keyboard: `⌘K`/`Ctrl+K` command menu (pages, run IDs, job numbers, searches, theme), `g` then `o`/`q`/`w`/`l` to navigate, `/` to search, `j`/`k` between rows, `?` for help. Keyboard-opened surfaces do not animate.
- Motion is limited to state and feedback: press scale, popover and dialog fades from their origin, a swipeable sheet, short collapsible panels. Everything degrades to opacity under `prefers-reduced-motion`.
- Light, dark and system themes; the choice is stored in `localStorage` and applied before the first paint.
- Long values: queue names truncate in the middle, IDs show their random suffix with a copy button, keys clamp to two lines, JSON blocks scroll inside their card, timelines over 90 checkpoints collapse their middle, and very large JSON skips syntax highlighting.

In `pnpm dev`, a development-only toggle switches the data source between the live API and demo, worst-case and empty fixtures ([src/dev](../../ui/embedded/src/dev)); production builds exclude it.

## Known limitations

- The bundle is about 790 KB minified (about 250 KB gzip), roughly four times the earlier Svelte build, and it is embedded in every binary that imports `adminui`.
- Polling, not server push: a change appears within one polling interval.
- The UI must be served at the root of its origin; assets use absolute `/_app/` paths.
