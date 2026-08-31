# HQL v0

Read-only pipe queries over a HedronDB store. stdlib `sqlite3` only — no pyo3,
no writes, no migrations.

This package is a **result-twin** of the product CLI `hedron hql`. The same
pipeline string against the same db must produce the same rows (fields and
values). See the crate README and `cargo test --test hql_twin`.

Open the store as `file:...?mode=ro`. `schema_mismatches()` reports drift
against the `hedron-core` `CREATE TABLE` for `nodes`, `edges`,
`desired_states`, and `events`. It does not migrate.

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
    .vault("atrium-fixture")
    .search("HedronDB")
    .filter('extra.domain == "foundry"')
    .select("path", "extra.name")
    .limit(20)
    .run()
)

session = (
    Query.open("store.db")
    .vault("htec-leo")
    .agent("leo")
    .state()
    .select("path", "extra.name", "extra.title", "state_version", "status")
    .run()
)

chain = (
    Query.open("store.db")
    .vault("htec-elio")
    .agent("elio")
    .history()
    .run()
)
```

## Operators

| Stage | Shape |
| --- | --- |
| `vault <name>` | Nodes whose `vault_id` matches a Vault with `path` or `extra.name` |
| `agent <name>` | Keep Agent rows whose `extra.name`, `extra.title`, or `path` matches (exact on name/title/path basename wins; otherwise substring). Stays inside the current vault slice |
| `state` | Warm: latest `desired_states` row for each Agent/Vault `vault_id` (highest `state_version`). Exposes `spec`, `status`, `state_version`, `reconciled_by`, `importance`, `id`. Does not read `events` |
| `history` | Cool: `causal_chain` for that latest Desired State. Event metadata only (`id`, `ts`, `actor`, `type`, `caused_by`, `reconciles`, `supersedes`). No spec/status/data |
| `search <text>` | Substring over `path` + `extra` + edge `to_raw` / `properties` |
| `traverse --edge <type> --hops N` | Walk rows (`from` + edge + optional `to`). Dangling `to_id` stays |
| `filter <expr>` | `== != ^= !^=` with `and` / `&&`. `null` literal |
| `select <fields>` | `path`, `to_id`, `to_raw`, `from_id`, `extra.X`, `from.path`, `to.path`, plus Warm `state` fields and Cool `history` fields |
| `limit N` | First N rows |

Missing `extra` keys are `None`, not errors. Notes without `extra.domain` stay
without it — a domain filter simply drops those rows.

H.TEC agent files often set `extra.title` and omit `extra.name`. `agent` accepts
either. `state` is Warm only (`current_state`). `history` is Cool only
(`causal_chain`). `causal` is rejected.

## Example pipes (citadel 2026-08-25)

```text
vault atrium-fixture | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20
vault atrium-fixture | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw
vault atrium-fixture | filter path ^= "mail_room/" && path !^= "mail_room/Uri/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path
vault htec-leo | agent leo | state | select path, extra.name, extra.title, state_version, status
vault htec-elio | agent elio | history
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Run from this directory (`python/hql`).
