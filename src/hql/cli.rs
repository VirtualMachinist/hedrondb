//! `hedron hql` surface: flag parsing and tsv / table / json writers.

use std::io::{self, Write};
use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::hql::row::{format_float, Row, Value};
use crate::hql::Query;

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

Operators: vault, agent, state [NAME], history [NAME], search, traverse,
filter, select, limit. `causal` is rejected. `state` is Warm: every named
desired state in the vault, or the one an agent / NAME selects. `history`
is Cool: the causal events of the same selection. Default columns hide
spec/status unless selected.

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

/// Output columns: the first row's selection, else its kind's defaults.
pub fn fields_of(rows: &[Row]) -> Vec<String> {
    let Some(first) = rows.first() else {
        return Vec::new();
    };
    if let Some(selected) = first.selected_fields() {
        return selected;
    }
    first
        .default_fields()
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
        Value::Float(f) => stream.write_all(format_float(*f).as_bytes()),
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
    db: PathBuf,
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
                db = Some(PathBuf::from(need_value(&raw, i, "--db")?));
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
