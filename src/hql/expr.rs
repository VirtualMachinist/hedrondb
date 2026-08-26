//! HQL v0 filter expressions: == != ^= !^= with and / &&. null literal.

use crate::error::{Error, Result};
use crate::hql::row::{Row, Value};

const OPS: &[&str] = &["!^=", "==", "!=", "^="];

#[derive(Clone, Debug)]
pub enum FilterExpr {
    Comparison(Comparison),
    And(Vec<Comparison>),
}

#[derive(Clone, Debug)]
pub struct Comparison {
    pub field: String,
    pub op: String,
    pub value: Option<String>,
}

impl FilterExpr {
    pub fn eval(&self, row: &Row) -> bool {
        match self {
            FilterExpr::Comparison(c) => c.eval(row),
            FilterExpr::And(parts) => parts.iter().all(|part| part.eval(row)),
        }
    }
}

impl Comparison {
    pub fn eval(&self, row: &Row) -> bool {
        compare(&row.get(&self.field), &self.op, self.value.as_deref())
    }
}

pub fn parse_filter(expr: &str) -> Result<FilterExpr> {
    let mut tokens = tokenize(expr)?;
    if tokens.is_empty() {
        return Err(Error::Invalid("empty filter".into()));
    }
    parse_and(&mut tokens)
}

fn parse_and(tokens: &mut Vec<String>) -> Result<FilterExpr> {
    let mut parts = vec![parse_comparison(tokens)?];
    while !tokens.is_empty() {
        let join = tokens.remove(0);
        if join != "and" && join != "&&" {
            return Err(Error::Invalid(format!(
                "expected 'and' or '&&', got {join:?}"
            )));
        }
        parts.push(parse_comparison(tokens)?);
    }
    if parts.len() == 1 {
        Ok(FilterExpr::Comparison(parts.remove(0)))
    } else {
        Ok(FilterExpr::And(parts))
    }
}

fn parse_comparison(tokens: &mut Vec<String>) -> Result<Comparison> {
    if tokens.len() < 3 {
        return Err(Error::Invalid("expected field OP value".into()));
    }
    let field = tokens.remove(0);
    let op = tokens.remove(0);
    if !OPS.contains(&op.as_str()) {
        return Err(Error::Invalid(format!("unknown operator {op:?}")));
    }
    let raw = tokens.remove(0);
    let value = if raw == "null" { None } else { Some(raw) };
    Ok(Comparison { field, op, value })
}

fn tokenize(expr: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = expr.chars().collect();
    let n = chars.len();
    let mut i = 0;
    while i < n {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        if starts_with_at(expr, i, "&&") {
            tokens.push("&&".into());
            i += 2;
            continue;
        }
        if let Some(op) = OPS.iter().copied().find(|op| starts_with_at(expr, i, op)) {
            tokens.push(op.to_string());
            i += op.chars().count();
            continue;
        }
        if ch == '"' || ch == '\'' {
            let quote = ch;
            i += 1;
            let mut buf = String::new();
            while i < n && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < n {
                    buf.push(chars[i + 1]);
                    i += 2;
                    continue;
                }
                buf.push(chars[i]);
                i += 1;
            }
            if i >= n {
                return Err(Error::Invalid("unterminated string".into()));
            }
            i += 1;
            tokens.push(buf);
            continue;
        }
        let start = i;
        while i < n && !chars[i].is_whitespace() && !starts_op_at(expr, i) {
            i += 1;
        }
        let word: String = chars[start..i].iter().collect();
        if !word.is_empty() {
            tokens.push(word);
        }
    }
    Ok(tokens)
}

fn starts_with_at(expr: &str, char_index: usize, needle: &str) -> bool {
    expr.chars()
        .skip(char_index)
        .collect::<String>()
        .starts_with(needle)
}

fn starts_op_at(expr: &str, char_index: usize) -> bool {
    if starts_with_at(expr, char_index, "&&") {
        return true;
    }
    OPS.iter().any(|op| starts_with_at(expr, char_index, op))
}

fn compare(left: &Value, op: &str, right: Option<&str>) -> bool {
    match op {
        "==" => match right {
            None => left.is_null() || matches!(left, Value::Str(s) if s.is_empty()),
            Some(right) => !left.is_null() && left.py_str() == right,
        },
        "!=" => match right {
            None => !left.is_null() && !matches!(left, Value::Str(s) if s.is_empty()),
            Some(right) => left.is_null() || left.py_str() != right,
        },
        "^=" => match right {
            None => false,
            Some(right) => !left.is_null() && left.py_str().starts_with(right),
        },
        "!^=" => match right {
            None => false,
            Some(right) => left.is_null() || !left.py_str().starts_with(right),
        },
        _ => false,
    }
}
