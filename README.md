# HedronDB

Local-first knowledge OS for AI agents.

Working remotes: **GitHub [Hedronite/hedrondb](https://github.com/Hedronite/hedrondb)** (working) and **GitHedron Hedronite/hedrondb** (mesh SoT).

## Product CLI (`hedron`)

The product surface is one binary: **`hedron`**. It folds already-shipped Phase 0 capabilities; it is not a new product.

```bash
cargo build --bins
hedron import --src DIR --db FILE --vault NAME --agent NAME
hedron hql --db FILE [--format tsv|table|json] 'vault … | …'
```

- **`hedron import`** — markdown tree loader (same behavior as the original `hedron-import` flags).
- **`hedron hql`** — HQL v0 read-only pipes on `hedron-core` / rusqlite. Opens the store read-only. Never writes. Quote the pipeline so the shell is not the parser.

**`hedron-import` is kept as a thin alias** of `hedron import` (same flags, same output). Prefer `hedron import`.

**`python/hql` stays in-tree as a result-twin.** The same pipeline string against the same db must produce the same rows (fields and values). `cargo test --test hql_twin` is the documented comparison: it runs `hedron hql --format json` and `python3 -m hql --format json` on the fixture pipes and checks `json.loads` equality.

```bash
cargo test
cd python/hql && python3 -m unittest discover -s tests -v
```

## Phase 0 kernel

Phase 0 lives in the `hedron-core` crate. Tests are the surface. It proves:

- **Desired State** — declarative spec vs observed status (Kubernetes-like).
- **Causal Event Log** — append-only rows with `caused_by`, `reconciles`, `supersedes` on the event first.
- **Two query paths** — `current_state` (Warm: what is true now) and `causal_chain` (Cool: supersession / causal history). They are separate APIs and must not be mixed.

The first recon loop is **Option B (docs / EOD count)**. Storage is one SQLite file (`rusqlite` only) with four tables: `nodes`, `edges`, `desired_states`, `events`. Vaults are isolated named containers. An agent token is required for mutating and querying calls; tokens live in process memory and are rotated at the library gate. `hedron hql` is a separate read-only path: it does not take a token and does not read the event log.

Store files must be mode **0600**. Do not put tokens or other secrets in YAML / frontmatter. H.TEC is a `path` string on an Agent node, not a filesystem tree.

The crate is sync-only. No tokio, pyo3, HTTP, or new TCP.

## Query surface

`hedron hql` is the product HQL. `python/hql` is the stdlib `sqlite3` twin (`file:...?mode=ro`, never writes). Schema drift is reported, not migrated.

```bash
hedron hql --db FILE [--format tsv|table|json] 'vault atrium-fixture | search "HedronDB"'
cd python/hql
python3 -m hql --db FILE [--format tsv|table|json] PIPELINE
```

Fluent (Rust or Python): `Query::open(db).vault(...).search(...).filter(...).select(...).limit(...).run()`.

Operators: `vault`, `agent`, `state`, `search`, `traverse --edge/--hops`, `filter`, `select`, `limit`. `causal` is rejected. Default table columns hide `spec`/`status` unless selected. `state` attaches the latest `desired_states` row per `vault_id` onto Agent or Vault rows only. `agent` matches `extra.name` / `extra.title` / path basename (exact, else substring) and stays in the current vault slice.

Three pipes that already ran on citadel (2026-08-25), plus the Warm session pipe:

```text
vault atrium-fixture | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20
vault atrium-fixture | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw
vault atrium-fixture | filter path ^= "mail_room/" && path !^= "mail_room/Uri/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path
vault htec-leo | agent leo | state | select path, extra.name, extra.title, state_version, status
```

Notes without `extra.domain` stay without it. A domain filter drops those rows; it does not infer or backfill.

## Markdown fixture loader

`hedron import` (and the `hedron-import` alias) walks a markdown tree into a HedronDB store using `hedron-core` (wikilink `mentions`, unresolved `to_raw` allowed). It does not hardcode citadel paths. Tokens stay in-process and are never printed.

```bash
hedron import --src DIR --db FILE --vault NAME --agent NAME [--htec-path PATH] [--exclude-prefix mail_room/Uri/]
```

Frontmatter is the first `---` YAML fence; a deprecated `<!-- hal:authoritative:yaml -->` comment above it is ignored. HAL is the YAML keys (`name`, `title`, `domain`, …), not that html wrapper. GROK.md / HAL `supersedes` stays on the node `extra`. It is not written as `Event.supersedes`. Do not infer `extra.domain` or `extra.name` from the path.

## Not in this repo

HedronOS / NixOS / Kosha / tuwunel. Installing H.TEC on a Grok Bot. Hot feeds, Kelly, Client type. Atrium / lattice.db convert. HTTP or Redis. SQL as a query language. History / write / recon subcommands.
