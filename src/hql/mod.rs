//! HQL v0 read-only pipes. Opens the store read-only; never writes.

mod expr;
mod row;
mod store;

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::Path;

pub use expr::{parse_filter, Comparison, FilterExpr};
pub use row::{
    default_history_fields, default_output_fields, parse_extra_map, DesiredStateView,
    HistoryEventView, Row, Value,
};
pub use store::RoStore;

use crate::error::{Error, Result};
use crate::hql::store::EdgeRec;

pub const HQL_HELP: &str = "\
Run a read-only HQL v0 pipeline.

Usage:
  hedron hql --db FILE [--format tsv|table|json] PIPELINE

Opens the store read-only (never writes). Quote the pipeline so the
shell is not the parser.

Options:
  --db FILE              HedronDB sqlite file
  --format tsv|table|json
                         Output format (default tsv)
  -h, --help             Print help

Operators: vault, agent, state, history, search, traverse, filter, select, limit.
`causal` is rejected. `state` is Warm (latest spec/status). `history` is Cool
(causal_chain only). Default columns hide spec/status unless selected.

Python `python/hql` is a result-twin of this command.
";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    Tsv,
    Table,
    Json,
}

impl OutputFormat {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "tsv" => Ok(Self::Tsv),
            "table" => Ok(Self::Table),
            "json" => Ok(Self::Json),
            other => Err(Error::Invalid(format!(
                "unknown format {other:?} (expected tsv, table, or json)"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Vault(String),
    Agent(String),
    State,
    History,
    Search(String),
    Traverse { edge: String, hops: i64 },
    Filter(String),
    Select(Vec<String>),
    Limit(i64),
}

pub struct Query {
    store: RoStore,
    ops: Vec<Op>,
}

impl Query {
    pub fn open(db: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            store: RoStore::open(db)?,
            ops: Vec::new(),
        })
    }

    pub fn vault(mut self, name: impl Into<String>) -> Self {
        self.ops.push(Op::Vault(name.into()));
        self
    }

    pub fn agent(mut self, name: impl Into<String>) -> Self {
        self.ops.push(Op::Agent(name.into()));
        self
    }

    pub fn state(mut self) -> Self {
        self.ops.push(Op::State);
        self
    }

    pub fn history(mut self) -> Self {
        self.ops.push(Op::History);
        self
    }

    pub fn search(mut self, text: impl Into<String>) -> Self {
        self.ops.push(Op::Search(text.into()));
        self
    }

    pub fn traverse(mut self, edge: impl Into<String>, hops: i64) -> Self {
        self.ops.push(Op::Traverse {
            edge: edge.into(),
            hops,
        });
        self
    }

    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.ops.push(Op::Filter(expr.into()));
        self
    }

    pub fn select<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut parsed = Vec::new();
        for field in fields {
            parsed.extend(split_fields(field.as_ref()));
        }
        self.ops.push(Op::Select(parsed));
        self
    }

    pub fn limit(mut self, n: i64) -> Self {
        self.ops.push(Op::Limit(n));
        self
    }

    pub fn pipe(mut self, pipeline: &str) -> Result<Self> {
        for stage in split_pipeline(pipeline) {
            self.apply_stage(&stage)?;
        }
        Ok(self)
    }

    pub fn run(self) -> Result<Vec<Row>> {
        let nodes = self.store.nodes()?;
        let edges = self.store.edges()?;
        let mut by_id: HashMap<String, Row> = HashMap::new();
        for row in &nodes {
            if let Some(id) = &row.node_id {
                by_id.insert(id.clone(), row.clone());
            }
        }
        let mut outgoing: HashMap<String, Vec<EdgeRec>> = HashMap::new();
        for edge in edges {
            outgoing.entry(edge.from_id.clone()).or_default().push(edge);
        }

        let mut rows = nodes;
        for op in &self.ops {
            match op {
                Op::Vault(name) => {
                    let vault_ids: HashSet<String> =
                        self.store.vault_ids_named(name)?.into_iter().collect();
                    rows.retain(|row| {
                        row.vault_id
                            .as_ref()
                            .map(|id| vault_ids.contains(id))
                            .unwrap_or(false)
                    });
                }
                Op::Agent(name) => {
                    rows = filter_agents(rows, name);
                }
                Op::State => {
                    rows = apply_state(rows, &self.store.latest_desired_states()?);
                }
                Op::History => {
                    rows = apply_history(rows, &self.store)?;
                }
                Op::Search(needle) => {
                    rows.retain(|row| search_hit(row, needle, &outgoing, &by_id));
                }
                Op::Traverse { edge, hops } => {
                    rows = traverse(rows, edge, *hops, &outgoing, &by_id)?;
                }
                Op::Filter(expr) => {
                    let expr = parse_filter(expr)?;
                    rows.retain(|row| expr.eval(row));
                }
                Op::Select(fields) => {
                    rows = rows.into_iter().map(|row| row.project(fields)).collect();
                }
                Op::Limit(n) => apply_limit(&mut rows, *n),
            }
        }
        Ok(rows)
    }

    fn apply_stage(&mut self, stage: &str) -> Result<()> {
        let stage = stage.trim();
        if stage.is_empty() {
            return Ok(());
        }
        let (name, rest) = match stage.split_once(' ') {
            Some((name, rest)) => (name, rest.trim()),
            None => (stage, ""),
        };
        match name {
            "vault" => {
                self.ops.push(Op::Vault(unquote_arg(rest)));
            }
            "agent" => {
                if rest.is_empty() {
                    return Err(Error::Invalid("agent requires a name".into()));
                }
                self.ops.push(Op::Agent(unquote_arg(rest)));
            }
            "state" => {
                if !rest.is_empty() {
                    return Err(Error::Invalid("state takes no arguments".into()));
                }
                self.ops.push(Op::State);
            }
            "history" => {
                if !rest.is_empty() {
                    return Err(Error::Invalid("history takes no arguments".into()));
                }
                self.ops.push(Op::History);
            }
            "search" => {
                self.ops.push(Op::Search(unquote_arg(rest)));
            }
            "traverse" => {
                let (edge, hops) = parse_traverse(rest)?;
                self.ops.push(Op::Traverse { edge, hops });
            }
            "filter" => {
                self.ops.push(Op::Filter(rest.to_string()));
            }
            "select" => {
                self.ops.push(Op::Select(split_fields(rest)));
            }
            "limit" => {
                let n = rest
                    .parse::<i64>()
                    .map_err(|_| Error::Invalid(format!("limit needs an integer, got {rest:?}")))?;
                self.ops.push(Op::Limit(n));
            }
            other => return Err(Error::Invalid(format!("unknown operator {other:?}"))),
        }
        Ok(())
    }
}

pub fn run_pipeline(db: impl AsRef<Path>, pipeline: &str) -> Result<Vec<Row>> {
    Query::open(db)?.pipe(pipeline)?.run()
}

/// Quote-aware `|` split. The shell is not the parser.
pub fn split_pipeline(pipeline: &str) -> Vec<String> {
    let mut stages = Vec::new();
    let mut buf = String::new();
    let chars: Vec<char> = pipeline.chars().collect();
    let mut i = 0;
    let mut quote: Option<char> = None;
    while i < chars.len() {
        let ch = chars[i];
        if quote.is_some() {
            buf.push(ch);
            if Some(ch) == quote {
                quote = None;
            } else if ch == '\\' && i + 1 < chars.len() {
                buf.push(chars[i + 1]);
                i += 1;
            }
            i += 1;
            continue;
        }
        if ch == '"' || ch == '\'' {
            quote = Some(ch);
            buf.push(ch);
            i += 1;
            continue;
        }
        if ch == '|' {
            stages.push(buf.trim().to_string());
            buf.clear();
            i += 1;
            continue;
        }
        buf.push(ch);
        i += 1;
    }
    let tail = buf.trim();
    if !tail.is_empty() {
        stages.push(tail.to_string());
    }
    stages
}

fn search_hit(
    row: &Row,
    needle: &str,
    outgoing: &HashMap<String, Vec<EdgeRec>>,
    by_id: &HashMap<String, Row>,
) -> bool {
    if row.searchable_text().contains(needle) {
        return true;
    }
    let source_id = row
        .node_id
        .as_deref()
        .or(row.from_id.as_deref())
        .unwrap_or("");
    if source_id.is_empty() {
        return false;
    }
    for edge in outgoing.get(source_id).map(Vec::as_slice).unwrap_or(&[]) {
        let blob = format!(
            "{}\n{}",
            edge.to_raw.as_deref().unwrap_or(""),
            edge.properties
        );
        if blob.contains(needle) {
            return true;
        }
        if let Some(to_id) = &edge.to_id {
            if let Some(dest) = by_id.get(to_id) {
                if dest.searchable_text().contains(needle) {
                    return true;
                }
            }
        }
    }
    false
}

fn traverse(
    rows: Vec<Row>,
    edge_type: &str,
    hops: i64,
    outgoing: &HashMap<String, Vec<EdgeRec>>,
    by_id: &HashMap<String, Row>,
) -> Result<Vec<Row>> {
    if hops < 1 {
        return Err(Error::Invalid("traverse --hops must be >= 1".into()));
    }
    let mut frontier: Vec<Row> = Vec::new();
    for row in rows {
        if row.node_id.is_some() {
            frontier.push(row);
        } else if let Some(to_id) = &row.to_id {
            if let Some(dest) = by_id.get(to_id) {
                frontier.push(dest.clone());
            }
        }
    }
    let mut emitted = Vec::new();
    let mut seen_edges: HashSet<String> = HashSet::new();
    for _ in 0..hops {
        let mut nxt = Vec::new();
        for src in &frontier {
            let Some(src_id) = &src.node_id else {
                continue;
            };
            for edge in outgoing.get(src_id).map(Vec::as_slice).unwrap_or(&[]) {
                if edge.edge_type != edge_type {
                    continue;
                }
                if !seen_edges.insert(edge.id.clone()) {
                    continue;
                }
                let dest = edge.to_id.as_ref().and_then(|id| by_id.get(id));
                let walk = Row {
                    path: src.path.clone(),
                    extra: src.extra.clone(),
                    extra_map: src.extra_map.clone(),
                    node_id: src.node_id.clone(),
                    vault_id: src.vault_id.clone(),
                    node_type: src.node_type.clone(),
                    from_id: Some(edge.from_id.clone()),
                    from_path: src.path.clone(),
                    to_id: edge.to_id.clone(),
                    to_raw: edge.to_raw.clone(),
                    to_path: dest.and_then(|d| d.path.clone()),
                    edge_type: Some(edge.edge_type.clone()),
                    properties: edge.properties.clone(),
                    ..Row::default()
                };
                emitted.push(walk);
                if let Some(dest) = dest {
                    nxt.push(dest.clone());
                }
            }
        }
        frontier = nxt;
    }
    Ok(emitted)
}

fn filter_agents(rows: Vec<Row>, name: &str) -> Vec<Row> {
    let agents: Vec<Row> = rows
        .into_iter()
        .filter(|row| row.node_type.as_deref() == Some("Agent"))
        .collect();
    let exact: Vec<Row> = agents
        .iter()
        .filter(|row| agent_exact(row, name))
        .cloned()
        .collect();
    if !exact.is_empty() {
        return exact;
    }
    agents
        .into_iter()
        .filter(|row| agent_substring(row, name))
        .collect()
}

fn agent_exact(row: &Row, name: &str) -> bool {
    let extra_name = row.extra_map.get("name").and_then(|v| v.as_deref());
    let extra_title = row.extra_map.get("title").and_then(|v| v.as_deref());
    if extra_name == Some(name) || extra_title == Some(name) {
        return true;
    }
    let path = row.path.as_deref().unwrap_or("");
    if path == name {
        return true;
    }
    path_basename(path) == name
}

fn agent_substring(row: &Row, name: &str) -> bool {
    let extra_name = row
        .extra_map
        .get("name")
        .and_then(|v| v.as_deref())
        .unwrap_or("");
    let extra_title = row
        .extra_map
        .get("title")
        .and_then(|v| v.as_deref())
        .unwrap_or("");
    let path = row.path.as_deref().unwrap_or("");
    extra_name.contains(name) || extra_title.contains(name) || path.contains(name)
}

fn path_basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn apply_state(rows: Vec<Row>, latest: &HashMap<String, DesiredStateView>) -> Vec<Row> {
    let mut emitted = Vec::new();
    for row in rows {
        if !matches!(row.node_type.as_deref(), Some("Agent") | Some("Vault")) {
            continue;
        }
        let Some(vault_id) = row.vault_id.as_deref() else {
            continue;
        };
        let Some(ds) = latest.get(vault_id) else {
            continue;
        };
        emitted.push(row.with_state(ds));
    }
    emitted
}

fn apply_history(rows: Vec<Row>, store: &RoStore) -> Result<Vec<Row>> {
    let latest = store.latest_desired_states()?;
    let mut emitted = Vec::new();
    for row in rows {
        if !matches!(row.node_type.as_deref(), Some("Agent") | Some("Vault")) {
            continue;
        }
        let Some(vault_id) = row.vault_id.as_deref() else {
            continue;
        };
        let Some(ds) = latest.get(vault_id) else {
            continue;
        };
        for event in store.causal_chain(&ds.id)? {
            emitted.push(Row::from_history(&event));
        }
    }
    Ok(emitted)
}

fn apply_limit(rows: &mut Vec<Row>, n: i64) {
    if n >= 0 {
        rows.truncate((n as usize).min(rows.len()));
    } else {
        let drop = n.unsigned_abs() as usize;
        let keep = rows.len().saturating_sub(drop);
        rows.truncate(keep);
    }
}

fn parse_traverse(rest: &str) -> Result<(String, i64)> {
    let tokens = tokenize_args(rest);
    let mut edge = None;
    let mut hops = 1i64;
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i].as_str();
        if tok == "--edge" && i + 1 < tokens.len() {
            edge = Some(unquote_arg(&tokens[i + 1]));
            i += 2;
            continue;
        }
        if tok == "--hops" && i + 1 < tokens.len() {
            hops = tokens[i + 1]
                .parse::<i64>()
                .map_err(|_| Error::Invalid(format!("invalid --hops {}", tokens[i + 1])))?;
            i += 2;
            continue;
        }
        return Err(Error::Invalid(format!("unknown traverse argument {tok:?}")));
    }
    let edge = edge.ok_or_else(|| Error::Invalid("traverse requires --edge".into()))?;
    Ok((edge, hops))
}

fn tokenize_args(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut buf = String::new();
    let mut quote: Option<char> = None;
    for ch in text.chars() {
        if let Some(q) = quote {
            if ch == q {
                quote = None;
            } else {
                buf.push(ch);
            }
            continue;
        }
        if ch == '"' || ch == '\'' {
            quote = Some(ch);
            continue;
        }
        if ch.is_whitespace() {
            if !buf.is_empty() {
                tokens.push(std::mem::take(&mut buf));
            }
            continue;
        }
        buf.push(ch);
    }
    if !buf.is_empty() {
        tokens.push(buf);
    }
    tokens
}

fn unquote_arg(text: &str) -> String {
    let text = text.trim();
    if text.len() >= 2 {
        let bytes = text.as_bytes();
        let first = bytes[0];
        if (first == b'\'' || first == b'"') && bytes[text.len() - 1] == first {
            return text[1..text.len() - 1].to_string();
        }
    }
    text.to_string()
}

fn split_fields(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

pub fn fields_of(rows: &[Row]) -> Vec<String> {
    if rows.is_empty() {
        return Vec::new();
    }
    if let Some(selected) = rows[0].selected_fields() {
        return selected;
    }
    default_output_fields()
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

pub fn write_output<W: Write>(mut stream: W, format: OutputFormat, rows: &[Row]) -> io::Result<()> {
    let fields = fields_of(rows);
    match format {
        OutputFormat::Json => write_json(&mut stream, &fields, rows),
        OutputFormat::Table => write_table(&mut stream, &fields, rows),
        OutputFormat::Tsv => write_tsv(&mut stream, &fields, rows),
    }
}

fn write_tsv<W: Write>(stream: &mut W, fields: &[String], rows: &[Row]) -> io::Result<()> {
    if fields.is_empty() {
        return Ok(());
    }
    writeln!(stream, "{}", fields.join("\t"))?;
    for row in rows {
        let line: Vec<String> = fields.iter().map(|f| row.get(f).to_display()).collect();
        writeln!(stream, "{}", line.join("\t"))?;
    }
    Ok(())
}

fn write_table<W: Write>(stream: &mut W, fields: &[String], rows: &[Row]) -> io::Result<()> {
    if fields.is_empty() {
        return Ok(());
    }
    let mut widths: Vec<usize> = fields.iter().map(|f| f.len()).collect();
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| fields.iter().map(|f| row.get(f).to_display()).collect())
        .collect();
    for line in &cells {
        for (i, value) in line.iter().enumerate() {
            widths[i] = widths[i].max(value.len());
        }
    }
    let header: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, field)| pad_right(field, widths[i]))
        .collect();
    writeln!(stream, "{}", header.join("  "))?;
    for line in cells {
        let padded: Vec<String> = line
            .iter()
            .enumerate()
            .map(|(i, value)| pad_right(value, widths[i]))
            .collect();
        writeln!(stream, "{}", padded.join("  "))?;
    }
    Ok(())
}

fn pad_right(value: &str, width: usize) -> String {
    if value.len() >= width {
        value.to_string()
    } else {
        format!("{value}{}", " ".repeat(width - value.len()))
    }
}

fn write_json<W: Write>(stream: &mut W, fields: &[String], rows: &[Row]) -> io::Result<()> {
    stream.write_all(b"[")?;
    for (ri, row) in rows.iter().enumerate() {
        if ri > 0 {
            stream.write_all(b", ")?;
        }
        stream.write_all(b"{")?;
        for (fi, field) in fields.iter().enumerate() {
            if fi > 0 {
                stream.write_all(b", ")?;
            }
            write_json_string(stream, field)?;
            stream.write_all(b": ")?;
            write_json_value(stream, &row.get(field))?;
        }
        stream.write_all(b"}")?;
    }
    stream.write_all(b"]\n")?;
    Ok(())
}

fn write_json_value<W: Write>(stream: &mut W, value: &Value) -> io::Result<()> {
    match value {
        Value::Null => stream.write_all(b"null"),
        Value::Str(s) => write_json_string(stream, s),
        Value::Int(n) => write!(stream, "{n}"),
        Value::Float(f) => stream.write_all(row::format_float(*f).as_bytes()),
    }
}

fn write_json_string<W: Write>(stream: &mut W, s: &str) -> io::Result<()> {
    stream.write_all(b"\"")?;
    for ch in s.chars() {
        match ch {
            '"' => stream.write_all(b"\\\"")?,
            '\\' => stream.write_all(b"\\\\")?,
            '\u{08}' => stream.write_all(b"\\b")?,
            '\u{0c}' => stream.write_all(b"\\f")?,
            '\n' => stream.write_all(b"\\n")?,
            '\r' => stream.write_all(b"\\r")?,
            '\t' => stream.write_all(b"\\t")?,
            c if (c as u32) < 0x20 => write!(stream, "\\u{:04x}", c as u32)?,
            c if (c as u32) > 0x7f => {
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    write!(stream, "\\u{:04x}", unit)?;
                }
            }
            c => {
                let mut buf = [0u8; 4];
                stream.write_all(c.encode_utf8(&mut buf).as_bytes())?;
            }
        }
    }
    stream.write_all(b"\"")
}

struct HqlArgs {
    db: std::path::PathBuf,
    format: OutputFormat,
    pipeline: String,
}

fn parse_hql_args(raw: Vec<String>) -> Result<HqlArgs> {
    let mut db = None;
    let mut format = OutputFormat::Tsv;
    let mut pipeline = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--db" => {
                db = Some(std::path::PathBuf::from(need_value(&raw, i, "--db")?));
                i += 2;
            }
            "--format" => {
                format = OutputFormat::parse(need_value(&raw, i, "--format")?)?;
                i += 2;
            }
            other if other.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown argument {other}")));
            }
            other => {
                pipeline.push(other.to_string());
                i += 1;
            }
        }
    }
    let db = db.ok_or_else(|| Error::Invalid("--db FILE is required".into()))?;
    if pipeline.is_empty() {
        return Err(Error::Invalid("PIPELINE is required".into()));
    }
    Ok(HqlArgs {
        db,
        format,
        pipeline: pipeline.join(" "),
    })
}

fn need_value<'a>(raw: &'a [String], i: usize, flag: &str) -> Result<&'a str> {
    raw.get(i + 1)
        .map(String::as_str)
        .ok_or_else(|| Error::Invalid(format!("{flag} needs a value")))
}

/// Parse flags, run the pipeline, write formatted rows to stdout.
pub fn run_cli(raw: Vec<String>) -> std::result::Result<(), String> {
    if raw.iter().any(|arg| arg == "--help" || arg == "-h") {
        print!("{HQL_HELP}");
        return Ok(());
    }
    let args = parse_hql_args(raw).map_err(|err| err.to_string())?;
    let rows = Query::open(&args.db)
        .map_err(|err| err.to_string())?
        .pipe(&args.pipeline)
        .map_err(|err| err.to_string())?
        .run()
        .map_err(|err| err.to_string())?;
    write_output(io::stdout(), args.format, &rows).map_err(|err| err.to_string())?;
    Ok(())
}
