# HedronDB

Local-first knowledge OS for AI agents.

Working remotes: **GitHub [Hedronite/hedrondb](https://github.com/Hedronite/hedrondb)** (working) and **GitHedron Hedronite/hedrondb** (mesh SoT).

## Phase 0 kernel

Phase 0 lives in the `hedron-core` crate. Tests are the surface. It proves:

- **Desired State** — declarative spec vs observed status (Kubernetes-like).
- **Causal Event Log** — append-only rows with `caused_by`, `reconciles`, `supersedes` on the event first.
- **Two query paths** — `current_state` (Warm: what is true now) and `causal_chain` (Cool: supersession / causal history). They are separate APIs and must not be mixed.

The first recon loop is **Option B (docs / EOD count)**. Storage is one SQLite file (`rusqlite` only) with four tables: `nodes`, `edges`, `desired_states`, `events`. Vaults are isolated named containers. An agent token is required for mutating and querying calls; tokens live in process memory and are rotated at the library gate.

Store files must be mode **0600**. Do not put tokens or other secrets in YAML / frontmatter. H.TEC is a `path` string on an Agent node, not a filesystem tree.

```bash
cargo test
```

The crate is sync-only. No tokio, pyo3, HTTP, or new TCP.

## Query surface (`python/hql`)

`python/hql` is HQL v0 — a stdlib `sqlite3` read-only pipe language. No pyo3. It opens the store as `file:...?mode=ro` and never writes. Schema drift is reported, not migrated.

```bash
cd python/hql
python3 -m hql --db FILE [--format tsv|table|json] PIPELINE
./bin/hql --db FILE PIPELINE
python3 -m unittest discover -s tests -v
```

Fluent: `Query.open(db).vault(...).search(...).filter(...).select(...).limit(...).run()`.

Three pipes that already ran on citadel (2026-08-25):

```text
vault atrium-fixture | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20
vault atrium-fixture | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw
vault atrium-fixture | filter path ^= "mail_room/" && path !^= "mail_room/Uri/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path
```

Notes without `extra.domain` stay without it. A domain filter drops those rows; it does not infer or backfill.

## Markdown fixture loader (`hedron-import`)

`hedron-import` walks a markdown tree into a HedronDB store using `hedron-core` (wikilink `mentions`, unresolved `to_raw` allowed). It does not hardcode citadel paths. Tokens stay in-process and are never printed.

```bash
hedron-import --src DIR --db FILE --vault NAME --agent NAME [--htec-path PATH] [--exclude-prefix mail_room/Uri/]
```

GROK.md / HAL `supersedes` in frontmatter stays on the node `extra`. It is not written as `Event.supersedes`.

## Not in this repo

HedronOS / NixOS / Kosha / tuwunel. Installing H.TEC on a Grok Bot. Hot feeds, Kelly, Client type. Atrium / lattice.db convert. HTTP or Redis.
