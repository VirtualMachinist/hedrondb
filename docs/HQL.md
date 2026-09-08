# HQL v0

Read-only pipes over a HedronDB store. The product CLI is `hedron hql`. `python/hql` is a stdlib `sqlite3` twin (`file:...?mode=ro`, never writes). The same pipeline against the same database must produce the same rows.

```bash
hedron hql --db FILE [--format tsv|table|json] 'vault my-vault | search "HedronDB"'
cd python/hql
python3 -m hql --db FILE [--format tsv|table|json] PIPELINE
```

Fluent (Rust or Python): `Query::open(db).vault(...).agent(...).state()` or `.history()`, then `search` / `filter` / `select` / `limit` / `run()`.

## Operators

`vault`, `agent`, `state`, `history`, `search`, `traverse --edge/--hops`, `filter`, `select`, `limit`.

`causal` is rejected (no alias). Default table columns hide `spec` / `status` unless selected.

- **`state`** attaches the latest `desired_states` row per `vault_id` onto Agent or Vault rows only (Warm).
- **`history`** replaces those rows with `causal_chain` event metadata for that latest desired state (Cool: no spec/status/data).
- **`agent`** matches `extra.name` / `extra.title` / path basename (exact, else substring) and stays in the current vault slice.

Quote the pipeline so the shell is not the parser.

## Examples

```text
vault atrium-fixture | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20
vault atrium-fixture | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw
vault htec-leo | agent leo | state | select path, extra.name, extra.title, state_version, status
vault htec-elio | agent elio | history
```

Notes without `extra.domain` stay without it. A domain filter drops those rows; it does not infer or backfill.

## Import

```bash
hedron import --src DIR --db FILE --vault NAME --agent NAME [--htec-path PATH] [--exclude-prefix mail_room/Uri/]
```

Frontmatter is the first `---` YAML fence. Do not put tokens in YAML. Do not infer `extra.domain` or `extra.name` from the path.

## Tests

```bash
cargo test
cargo test --test hql_twin
cd python/hql && python3 -m unittest discover -s tests -v
```
