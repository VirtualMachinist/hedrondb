# HQL v0

Read-only pipes over a HedronDB store. Humans and agents use the **same** pipeline string. Humans usually want a table; agents want `--format json`. HQL never writes — mutations go through the Store API.

The product CLI is `hedron hql`. `python/hql` is an independent `sqlite3` + PyYAML twin (`file:...?mode=ro`, never writes). The same pipeline against the same database must produce the same rows.

```bash
hedron hql --db FILE [--format tsv|table|json] 'vault my-vault | search "HedronDB"'
cd python/hql
python3 -m hql --db FILE [--format tsv|table|json] PIPELINE
```

Fluent (Rust or Python): `Query::open(db).vault(...).agent(...).state()` or `.history()` (Rust: `.state_named("deploy")`; Python: `.state("deploy")`), then `search` / `filter` / `select` / `limit` / `run()`.

## Operators

`vault`, `agent`, `state [NAME]`, `history [NAME]`, `search`, `traverse --edge/--hops`, `filter`, `select`, `limit`.

`causal` is rejected (no alias). Default table columns hide `spec` / `status` unless selected.

A vault holds many **named** desired states (`desired_states.name`, unique per vault). `state` and `history` select by name:

| Pipe | Rows |
|---|---|
| `vault prod \| state` | one `State` row per named desired state in `prod` (subject: the vault node) |
| `vault prod \| state deploy` | the desired state named `deploy` |
| `vault prod \| agent deploy \| state` | the desired state whose name matches the agent (`extra.name` / `extra.title` / path basename, exact then substring) — `deploy` |
| `vault prod \| agent ops \| state deploy` | `deploy`, with agent `ops` as the subject |
| `vault prod \| filter path ^= "notes/" \| state deploy` | `deploy`, no subject node (`path` is null) |
| `… \| history [NAME]` | the events of the same selection: `events.reconciles = ds.id`, ordered `ts, id` |

- **`state`** is Warm: `id`, `name`, `state_version`, `reconciled_by`, `importance`, `spec`, `status` from `desired_states`, plus the subject node's `path` / `extra.*`. It never reads `events`.
- **`history`** is Cool: `id`, `ts`, `actor`, `type`, `caused_by`, `reconciles`, `supersedes`. It never carries `spec` / `status` / `name` / event `data`.
- **`agent`** matches `extra.name` / `extra.title` / path basename (exact, else substring) and stays in the current vault slice.
- **`extra.k`** reads `nodes.extra` as YAML. Missing keys are null; nested values render in flow style (`[a, b]`, `{k: v}`).

Quote the pipeline so the shell is not the parser.

## Examples

```text
vault demo-vault | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20
vault demo-vault | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw
vault prod | state
vault prod | state eod-2026-08-25 | select name, state_version, status
vault prod | agent deploy | state | select path, extra.name, extra.title, state_version, status
vault prod | agent deploy | history
vault prod | filter path ^= "notes/" | history deploy | select id, supersedes
```

Notes without `extra.domain` stay without it. A domain filter drops those rows; it does not infer or backfill.

## Import

```bash
hedron import --src DIR --db FILE --vault NAME --agent NAME [--htec-path PATH] [--exclude-prefix inbox/private/]
```

Default `--exclude-prefix` is `inbox/private/`. When `--briefs-date DATE` is set, the import writes one desired state named `eod-DATE` (`kind: docs_eod`) and reconciles it; stems under `inbox/` are preferred if that tree exists. The whole import is one transaction.

Frontmatter is the first `---` YAML fence. Do not put tokens in YAML. Do not infer `extra.domain` or `extra.name` from the path.

## Tests

```bash
cargo test
cargo test --test hql_twin
cd python/hql && python3 -m unittest discover -s tests -v   # needs pyyaml
```
