# HQL v0

Read-only pipe queries over a HedronDB store. stdlib `sqlite3` plus PyYAML —
no pyo3, no writes, no migrations.

This package is a **result-twin** of the product CLI `hedron hql`. The same
pipeline string against the same db must produce the same rows (fields and
values). See the crate README and `cargo test --test hql_twin`.

Open the store as `file:...?mode=ro`. `schema_mismatches()` reports drift
against the crate-root `schema.sql` (the one `CREATE TABLE` source, read
at runtime). It does not migrate.

`nodes.extra` is parsed with PyYAML using a YAML 1.2 core-schema loader
(no `yes`/`no` booleans, no timestamps) so scalars match the Rust twin.

## CLI

```bash
python3 -m hql --db FILE [--format tsv|table|json] PIPELINE
./bin/hql --db FILE PIPELINE
```

## Fluent

```python
from hql import Query

rows = (
    Query.open("store.db")
    .vault("demo-vault")
    .search("HedronDB")
    .filter('extra.domain == "foundry"')
    .select("path", "extra.name")
    .limit(20)
    .run()
)

intents = Query.open("store.db").vault("prod").state().run()   # every named state

session = (
    Query.open("store.db")
    .vault("prod")
    .agent("deploy")
    .state()                       # the state named after the agent; .state("deploy") is explicit
    .select("path", "extra.name", "extra.title", "state_version", "status")
    .run()
)

chain = (
    Query.open("store.db")
    .vault("prod")
    .agent("deploy")
    .history()
    .run()
)
```

## Operators

| Stage | Shape |
| --- | --- |
| `vault <name>` | Nodes whose `vault_id` matches a Vault with `path` or `extra.name` |
| `agent <name>` | Keep Agent rows whose `extra.name`, `extra.title`, or `path` matches (exact on name/title/path basename wins; otherwise substring). Stays inside the current vault slice |
| `state [NAME]` | Warm. After `vault`: every named `desired_states` row in that vault. After `agent`: the one whose `name` matches the agent (exact on `extra.name` / `extra.title` / path basename, else substring). `NAME` selects explicitly and also works with no Agent/Vault row in the slice. Exposes `name`, `spec`, `status`, `state_version`, `reconciled_by`, `importance`, `id`. Does not read `events` |
| `history [NAME]` | Cool: the events of the same selection (`events.reconciles = id`, ordered `ts, id`). Event metadata only (`id`, `ts`, `actor`, `type`, `caused_by`, `reconciles`, `supersedes`). No spec/status/name/data |
| `search <text>` | Substring over `path` + `extra` + edge `to_raw` / `properties` |
| `traverse --edge <type> --hops N` | Walk rows (`from` + edge + optional `to`). Dangling `to_id` stays |
| `filter <expr>` | `== != ^= !^=` with `and` / `&&`. `null` literal |
| `select <fields>` | `path`, `to_id`, `to_raw`, `from_id`, `extra.X` (YAML; nested values render as `[a, b]` / `{k: v}`), `from.path`, `to.path`, plus Warm `state` fields and Cool `history` fields |
| `limit N` | First N rows |

Missing `extra` keys are `None`, not errors. Notes without `extra.domain` stay
without it — a domain filter simply drops those rows.

H.TEC agent files often set `extra.title` and omit `extra.name`. `agent` accepts
either. `state` is Warm only (`current_state`). `history` is Cool only
(`causal_chain`). `causal` is rejected.

## Example pipes

```text
vault demo-vault | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20
vault demo-vault | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw
vault demo-vault | filter path ^= "inbox/" && path !^= "inbox/private/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path
vault prod | state
vault prod | state eod-2026-08-25 | select name, state_version, status
vault prod | agent deploy | state | select path, extra.name, extra.title, state_version, status
vault prod | agent deploy | history
```

## Tests

```bash
python3 -m pip install --user pyyaml
python3 -m unittest discover -s tests -v
```

Run from this directory (`python/hql`).
