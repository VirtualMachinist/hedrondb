# HedronDB

Local-first knowledge OS for AI agents.

## Phase 0

Phase 0 is the `hedron-core` crate. It proves the distinctive model:

- **Desired State** — declarative spec vs observed status (Kubernetes-like).
- **Causal Event Log** — append-only rows with `caused_by`, `reconciles`, `supersedes` on the event first.
- **Two query paths** — `current_state` (Warm: what is true now) and `causal_chain` (Cool: supersession / causal history). They are separate APIs and must not be mixed.

The first recon loop is **Option B (docs / EOD count)**: a Desired State lists named briefs that should exist for a date; observe reports `Pending` when some are missing; reconcile bumps `state_version` + `content_hash` and appends a causal event.

Storage is one SQLite file (`rusqlite` only) with four tables: `nodes`, `edges`, `desired_states`, `events`. Vaults are isolated named containers. An agent token is required for mutating and querying calls; tokens live in process memory and are rotated at the library gate. Hivemind / shared is deny-by-default until a grant row exists.

Store files must be mode **0600**. Do not put tokens or other secrets in YAML / frontmatter.

## Phase 0 is not

A general database, a trading desk, a web service, or a Python / HQL product.

Out of scope: `hedron-py`, `hedron-cli`, tokio, pyo3, HQL, HTTP, any TCP listener, Redis, embeddings, PDF ingest, hybrid search, automatic tier movers, Forgejo/git, nix-darwin, k3s, ingesting an existing Atrium / lattice store, Client / Quiz / Lab / trading node types.

H.TEC is a `path` string on an Agent node, not a filesystem tree. Tier is a column (`hot` / `warm` / `cool` / `cold`), not a cache process.

## Run tests

Tests are the public surface.

```bash
cargo test
```

Requires a Rust toolchain. The crate is sync-only; `cargo test` should not open sockets.
