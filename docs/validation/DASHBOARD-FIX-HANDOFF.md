# agenity dashboard — "archaic /app" resolved (handoff / completion record)

## Problem
`/app` (the route the agenity product and openova `bp-agenity` deploy) served an
archaic single-column dashboard. The new **"workspaces v0.9.4"** redesign
(`web/src/components/ws/Dashboardws.svelte`) existed only at the `/v0.9.4` preview
route and was **never promoted to `/app`** — so every build/deploy served the old one.

## Fix — agenity repo
- **PR #752 — MERGED — merge SHA `5620ad668197f146dc529c4f18f932371249cb72`** (2026-06-24).
  - `web/src/pages/app.astro` now mounts `../components/ws/Dashboardws.svelte`.
  - `web/src/components/Dashboard.svelte` (old single-column dashboard) **deleted/retired**.

## Deploy — openova (bp-agenity), already executed by the openova agent
- `openova/.github/workflows/agenity-build.yaml`: `AGENITY_REF = 5620ad6`.
- `openova/products/agenity/chart/Chart.yaml`: `appVersion: "0.9.7"` — per openova's own
  chart notes, **the first image whose `/app` mounts `Dashboardws`**.
- Image: `ghcr.io/openova-io/bp-agenity:0.9.7` serves the new dashboard at `/app`.

## Proof
`docs/validation/app-new-dashboard-loggedin.png` — `/app` built from `5620ad6`, run under
a real daemon, logged in: the workspaces v0.9.4 multi-pane UI (Work/Terminal/Talk/Board;
Sessions/Terminal/Details). Byte-identical to what `bp-agenity:0.9.7` ships.

## Status: DONE at every layer (source → build → /app).
Only residual: re-verify against the **live** `agenity.demo.omani.homes/app/` once that
demo env is back up — it is currently torn down / unreachable (separate cluster,
no access from this host).

---
## Live-URL reachability — BLOCKED (infra, not config) — 2026-06-30T22:18:46Z
`https://agenity.demo.omani.homes/app/` is unreachable, verified two independent ways:
- `curl` → HTTP **000** (connection failed, ~4s)
- Playwright browser navigation → **60s timeout** on `domcontentloaded` (browser uses a different resolver than the host — rules out local DNS as the cause)

The demo env is torn down on cluster `212.72.24.33`, which is **not reachable from this host** (no kubeconfig for it; verified across every available context). A live-URL screenshot **cannot be produced** until that env is restored — this is an infra blocker, not a deployment-config question.

**Equivalent proof already on record:** `app-new-dashboard-loggedin.png` — the byte-identical `/app` built from `5620ad6`, run under a real daemon + logged in (exactly what `bp-agenity:0.9.7` serves).

**Next infra step** (demo-cluster owner): restore the demo env / (re)deploy `bp-agenity:0.9.7`, then re-verify `agenity.demo.omani.homes/app/` shows the workspaces dashboard.
