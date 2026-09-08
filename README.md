<p align="center">
  <a href="https://github.com/VirtualMachinist/hedrondb">
    <img src="assets/hedrondb-logo.jpeg" alt="HedronDB" width="180">
  </a>
</p>

<h1 align="center">HedronDB</h1>

<p align="center">
  <strong>Local-first intent store for agents.</strong><br>
  Desired state, a causal event log, and a read-only query language — one SQLite file.
</p>

<p align="center">
  <a href="https://github.com/VirtualMachinist/hedrondb/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/VirtualMachinist/hedrondb/ci.yml?branch=main&style=flat&colorA=1A1A1A&colorB=2A7A84&label=ci" alt="CI"></a>
  <a href="https://github.com/VirtualMachinist/hedrondb"><img src="https://img.shields.io/badge/HedronDB-v0.1.0-C9A227?style=flat&colorA=1A1A1A" alt="HedronDB v0.1.0"></a>
  <a href="https://rustup.rs"><img src="https://img.shields.io/badge/Rust-1.83-F46623?style=flat&colorA=1A1A1A&logo=rust&logoColor=white" alt="Rust 1.83"></a>
  <a href="https://hedronite.com"><img src="https://img.shields.io/badge/Hedronite-hedronite.com-C9A227?style=flat&colorA=1A1A1A" alt="Hedronite"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#what-it-is">What it is</a> ·
  <a href="#hql-for-humans-and-agents">HQL</a> ·
  <a href="#hedron-db-and-facet">HedronDB and Facet</a> ·
  <a href="#how-it-works">How it works</a> ·
  <a href="#status">Status</a> ·
  <a href="#contributing">Contributing</a>
</p>

<p align="center">
  Built by <a href="https://hedronite.com">Hedronite</a>'s <a href="https://github.com/VirtualMachinist">VirtualMachinist</a>.
  <em>The HedronDB mark is the teal crystal with a gold cap.</em>
</p>

---

HedronDB is a local-first knowledge store for software agents. It records **what should be true** (desired state) and **what happened** (an append-only causal log), then lets you query both without a hosted control plane.

One binary, `hedron`. One file, SQLite. Mutations go through an in-process store with vault isolation and short-lived tokens. Reads can go through **HQL**, a small pipe language that never writes.

It is for people who want intent and reconciliation next to the agent, not in a remote database. [Facet](https://github.com/VirtualMachinist/facet) is the API client and run history. HedronDB is the intent store. They work together; they do not share a database.

**0.1.0** · `hedron` CLI · HQL v0 · rusqlite

## Quick start

**1. Build.**

```bash
cargo build --release --bins
export PATH="$PWD/target/release:$PATH"
```

**2. Import a markdown tree.**

```bash
hedron import --src ./notes --db ./intent.db --vault my-vault --agent my-agent
```

**3. Query it.**

```bash
hedron hql --db ./intent.db --format json 'vault my-vault | search "HedronDB" | limit 20'
```

`hedron hql` opens the file read-only. Quote the pipeline so the shell is not the parser.

## What it is

| | HedronDB |
|---|---|
| Store | One SQLite file (`nodes`, `edges`, `desired_states`, `events`) |
| Isolation | Named vaults |
| Writes | `Store` API with in-process tokens (never printed, never in YAML) |
| Reads | `hedron hql` — read-only pipes |
| Twin | `python/hql` must return the same JSON for the same pipeline |

**Warm vs cool.** `state` is the latest desired-state spec/status (what is true now). `history` is the causal chain of that state (what superseded what). They are separate APIs and must not be mixed.

## What you get

- **`hedron import`** — walk a markdown tree into the store (wikilinks become `mentions`; unresolved targets are allowed).
- **`hedron hql`** — operators `vault`, `agent`, `state`, `history`, `search`, `traverse`, `filter`, `select`, `limit`. Unknown operators (including `causal`) are rejected.
- **`hedron-import`** — thin alias of `hedron import`. Prefer `hedron import`.
- **Rust/Python result twin** — `cargo test --test hql_twin` compares `hedron hql --format json` with `python3 -m hql`.

## HQL, for humans and agents

HQL is a small pipe language, not SQL. One string, two audiences:

| | Human | Agent |
|---|---|---|
| Command | `hedron hql --db FILE 'vault x \| state'` | the same, plus `--format json` |
| Output | table or TSV in a terminal | deterministic JSON |
| Writes | none — HQL never mutates | none — HQL never mutates |
| Proof | you can read the table | Rust CLI and `python/hql` must agree on the JSON |

A person explores: search a vault, follow mentions, look at **what is true now** (`state`) or **how it got that way** (`history`). An agent does the same pipeline as a tool call and parses JSON. If the two ever disagree, that is a bug (`cargo test --test hql_twin`).

Mutations are a different door: the `Store` API (tokens, reconcile). HQL cannot be used to “just update the row.” That split is the product: agents can look freely and cannot accidentally rewrite intent through a query.

Operators: `vault`, `agent`, `state`, `history`, `search`, `traverse`, `filter`, `select`, `limit`. Unknown names are rejected. Details: [docs/HQL.md](docs/HQL.md).

## HedronDB and Facet

Two local products, two questions.

| | [Facet](https://github.com/VirtualMachinist/facet) | HedronDB |
|---|---|---|
| Question | What did we call, and what came back? | What did we mean to be true, and how did that change? |
| Canonical files | OpenCollection YAML (Git) | One SQLite intent file |
| History | **Lattice** — runs, bodies, sessions (SQLite or Rust Turso) | **Causal log** — desired state vs observed, supersession |
| Query | `facet history`, SQL over runs | `hedron hql` pipes |
| Binary | `facet` | `hedron` |

They meet in a session, not in a table. An operator (or an agent) uses Facet to run an authenticated request and keep the evidence. The same session can declare or reconcile intent in HedronDB, then read it back with HQL. Lineage is explicit IDs (actor, session, run, vault, event) — Facet’s Lattice is never HedronDB’s registry, and HedronDB is never the API collection.

Typical loop:

1. **Facet** runs the request; Lattice records the call.
2. **HedronDB** records the intent (desired vs observed) and reconciles.
3. **HQL** reads state or history — human as a table, agent as JSON.

Neither product embeds the other. Wire them with a small CLI or MCP tool, scoped to a file and vault.

## How it works

One file, two query paths, no network.

- Store files should be mode `0600`.
- Tokens live in process memory and rotate at the library gate. `hedron hql` does not take a token.
- The crate is synchronous. No tokio, HTTP, or extra TCP ports.
- Schema drift is reported, not migrated.

<details>
<summary><strong>Repository map</strong></summary>

```
src/                  # hedron-core + hedron / hedron-import binaries
tests/                # phase0, cli, hql, import, hql_twin
python/hql/           # stdlib sqlite3 result-twin (read-only)
assets/               # product mark
docs/HQL.md           # query language
.github/workflows/    # ci.yml, nightly.yml
```

</details>

## Status

Phase 0 is the working tree at **0.1.0**; track `main`.

- **Store / vaults / desired state / causal log:** ready.
- **HQL v0 + Python twin:** ready.
- **Import:** ready.
- **Not in this repo:** HTTP service, SQL as the query language, embeddings, a UI, or a cluster control plane.

CI (`ubuntu-latest`, rustc 1.83) runs `cargo test` and a release build of `hedron`.

## Contributing

Issues and pull requests are welcome.

```bash
cargo test
cargo test --test hql_twin
cd python/hql && python3 -m unittest discover -s tests -v
cargo build --release && test -x target/release/hedron
```

## Credits

HedronDB is built and maintained by [Hedronite](https://hedronite.com).
