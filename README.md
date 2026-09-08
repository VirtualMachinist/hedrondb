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
  <a href="#what-you-get">What you get</a> ·
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

It is for people who want intent and reconciliation next to the agent, not in a remote database. Facet's Lattice (run history) is a different store; HedronDB does not replace it.

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

HQL details: [docs/HQL.md](docs/HQL.md).

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
