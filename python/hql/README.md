# HQL v0

Read-only pipe queries over a HedronDB store. stdlib `sqlite3` only — no pyo3,
no writes, no migrations.

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
```

## Operators

| Stage | Shape |
| --- | --- |
| `vault <name>` | Nodes whose `vault_id` matches a Vault with `path` or `extra.name` |
| `search <text>` | Substring over `path` + `extra` + edge `to_raw` / `properties` |
| `traverse --edge <type> --hops N` | Walk rows (`from` + edge + optional `to`). Dangling `to_id` stays |
| `filter <expr>` | `== != ^= !^=` with `and` / `&&`. `null` literal |
| `select <fields>` | `path`, `to_id`, `to_raw`, `from_id`, `extra.X`, `from.path`, `to.path` |
| `limit N` | First N rows |

Missing `extra` keys are `None`, not errors. Notes without `extra.domain` stay
without it — a domain filter simply drops those rows.

## Example pipes (citadel 2026-08-25)

```text
vault atrium-fixture | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20
vault atrium-fixture | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw
vault atrium-fixture | filter path ^= "mail_room/" && path !^= "mail_room/Uri/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Run from this directory (`python/hql`).
