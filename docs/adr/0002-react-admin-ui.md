# 0002: React admin UI

Status: accepted (2026-10-08)

## Context

The embedded admin UI was a SvelteKit static build with Skeleton components. Code-first workflows (ADR 0001) made the admin surface larger: checkpoint timelines, signal and child-run waits, run trees, retry and cancel. Its owner reviews it mostly from a phone, and the Svelte tables required desktop widths. The owner asked for a rebuilt full admin panel in React.

## Decision

Rebuild `ui/embedded` as a Vite + React + TypeScript app: React Router for routes, TanStack Query for polling server state, Base UI for accessible primitives (dialog, alert dialog, drawer, select, menu, popover, tooltip, switch, toggle group, collapsible), Sonner for toasts, cmdk for the command menu and Tailwind CSS v4 with design tokens for light and dark themes. Keep the embedding contract unchanged: the build is copied to `adminui/dist`, assets stay under `/_app/`, and Go serves the SPA routes behind token auth.

Add the read endpoints the screens need to `adminui` (overview, queue breakdown, job detail, workflow stats, run summaries, run tree, locks, config) as read-only SQL over the pgkit tables, without changing `queue` or `workflow`.

## Alternatives

- Keep SvelteKit and redesign it: the smallest bundle (about 65 KB gzip), but not the requested stack.
- React with a heavier component kit or charting library: faster to assemble, but larger and harder to restyle; a hand-written bar chart and unstyled primitives cover the need.
- Server-rendered HTML (as the older `queue` dashboard does): no client bundle, but no live timeline, command menu or swipeable sheets without rebuilding them by hand.

## Consequences

- The embedded bundle grows to about 790 KB minified (about 250 KB gzip). `adminui` now gzips text assets once in memory and caches hashed assets as immutable to keep transfers small.
- Frontend changes need Node and pnpm (see [setup](../development/setup.md#embedded-admin-ui)); Go-only work still uses the committed `adminui/dist`.
- The admin read models depend on the `pgqueue_jobs` and `pgworkflow_*` schemas; their PostgreSQL tests fail when those schemas change.
