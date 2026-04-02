use abash_core::SandboxError;
use serde_json::{Map, Number, Value};

#[derive(Clone)]
pub(crate) struct Program {
    pub(crate) filter: Filter,
}

#[derive(Clone)]
pub(crate) enum Filter {
    Identity,
    Path(PathExpr),
    Literal(Value),
    Pipe(Box<Filter>, Box<Filter>),
    Comma(Vec<Filter>),
    Array(Box<Filter>),
    Object(Vec<ObjectEntry>),
    Call(FunctionCall),
    Binary {
        left: Box<Filter>,
        op: BinaryOp,
        right: Box<Filter>,
    },
}

#[derive(Clone)]
pub(crate) struct PathExpr {
    pub(crate) ops: Vec<PathOp>,
}

#[derive(Clone)]
pub(crate) enum PathOp {
    Key(String),
    Index(isize),
    Slice(Option<isize>, Option<isize>),
    Iterate,
}

#[derive(Clone)]
pub(crate) struct ObjectEntry {
    pub(crate) key: String,
    pub(crate) value: Filter,
}

#[derive(Clone)]
pub(crate) enum FunctionCall {
    Length,
    Type,
    Keys,
    Has(Box<Filter>),
    Select(Box<Filter>),
    Map(Box<Filter>),
}

#[derive(Clone, Copy)]
pub(crate) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Alt,
}

pub(crate) fn parse_program(source: &str) -> Result<Program, SandboxError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "jq filter must not be empty".to_string(),
        ));
    }
    let mut parser = Parser::new(source);
    let filter = parser.parse_filter()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(SandboxError::InvalidRequest(format!(
            "jq filter token is not supported near: {}",
            &parser.source[parser.index..]
        )));
    }
    Ok(Program { filter })
}

pub(crate) fn run_program(program: &Program, inputs: &[Value]) -> Vec<Value> {
    let mut outputs = Vec::new();
    for input in inputs {
        outputs.extend(eval_filter(&program.filter, input));
    }
    outputs
}

fn eval_filter(filter: &Filter, input: &Value) -> Vec<Value> {
    match filter {
        Filter::Identity => vec![input.clone()],
        Filter::Path(path) => apply_path(input, &path.ops),
        Filter::Literal(value) => vec![value.clone()],
        Filter::Pipe(left, right) => {
            let mut outputs = Vec::new();
            for value in eval_filter(left, input) {
                outputs.extend(eval_filter(right, &value));
            }
            outputs
        }
        Filter::Comma(filters) => {
            let mut outputs = Vec::new();
            for filter in filters {
                outputs.extend(eval_filter(filter, input));
            }
            outputs
        }
        Filter::Array(inner) => vec![Value::Array(eval_filter(inner, input))],
        Filter::Object(entries) => {
            let mut object = Map::new();
            for entry in entries {
                let value = eval_filter(&entry.value, input)
                    .into_iter()
                    .next()
                    .unwrap_or(Value::Null);
                object.insert(entry.key.clone(), value);
            }
            vec![Value::Object(object)]
        }
        Filter::Call(call) => eval_call(call, input),
        Filter::Binary { left, op, right } => {
            let left = eval_filter(left, input)
                .into_iter()
                .next()
                .unwrap_or(Value::Null);
            let right = eval_filter(right, input)
                .into_iter()
                .next()
                .unwrap_or(Value::Null);
            vec![eval_binary(*op, left, right)]
        }
    }
}

fn eval_call(call: &FunctionCall, input: &Value) -> Vec<Value> {
    match call {
        FunctionCall::Length => vec![length_value(input)],
        FunctionCall::Type => vec![Value::String(type_name(input).to_string())],
        FunctionCall::Keys => vec![keys_value(input)],
        FunctionCall::Has(arg) => {
            let needle = eval_filter(arg, input)
                .into_iter()
                .next()
                .unwrap_or(Value::Null);
            vec![Value::Bool(has_value(input, &needle))]
        }
        FunctionCall::Select(predicate) => {
            let keep = eval_filter(predicate, input)
                .into_iter()
                .next()
                .is_some_and(|value| is_truthy(&value));
            if keep {
                vec![input.clone()]
            } else {
                Vec::new()
            }
        }
        FunctionCall::Map(inner) => match input {
            Value::Array(items) => {
                let mapped = items
                    .iter()
                    .map(|item| eval_filter(inner, item))
                    .map(|outputs| outputs.into_iter().next().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                vec![Value::Array(mapped)]
            }
            _ => vec![Value::Null],
        },
    }
}

fn eval_binary(op: BinaryOp, left: Value, right: Value) -> Value {
    match op {
        BinaryOp::Eq => Value::Bool(left == right),
        BinaryOp::Ne => Value::Bool(left != right),
        BinaryOp::Lt => compare_values(&left, &right, |a, b| a < b),
        BinaryOp::Le => compare_values(&left, &right, |a, b| a <= b),
        BinaryOp::Gt => compare_values(&left, &right, |a, b| a > b),
        BinaryOp::Ge => compare_values(&left, &right, |a, b| a >= b),
        BinaryOp::Alt => {
            if matches!(left, Value::Null) || matches!(left, Value::Bool(false)) {
                right
            } else {
                left
            }
        }
        BinaryOp::Add => add_values(left, right),
        BinaryOp::Sub => numeric_binary(left, right, |a, b| a - b),
        BinaryOp::Mul => numeric_binary(left, right, |a, b| a * b),
        BinaryOp::Div => numeric_binary(left, right, |a, b| a / b),
        BinaryOp::Mod => numeric_binary(left, right, |a, b| a % b),
    }
}

fn compare_values(left: &Value, right: &Value, op: impl Fn(f64, f64) -> bool) -> Value {
    Value::Bool(match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => op(a, b),
        _ => false,
    })
}

fn add_values(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Null, other) | (other, Value::Null) => other,
        (Value::Number(a), Value::Number(b)) => {
            number_value(a.as_f64().unwrap() + b.as_f64().unwrap())
        }
        (Value::String(a), Value::String(b)) => Value::String(format!("{a}{b}")),
        (Value::Array(mut a), Value::Array(b)) => {
            a.extend(b);
            Value::Array(a)
        }
        (Value::Object(mut a), Value::Object(b)) => {
            for (key, value) in b {
                a.insert(key, value);
            }
            Value::Object(a)
        }
        _ => Value::Null,
    }
}

fn numeric_binary(left: Value, right: Value, op: impl Fn(f64, f64) -> f64) -> Value {
    match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => number_value(op(a, b)),
        _ => Value::Null,
    }
}

fn number_value(value: f64) -> Value {
    Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn length_value(input: &Value) -> Value {
    match input {
        Value::Array(items) => Value::Number(Number::from(items.len())),
        Value::Object(map) => Value::Number(Number::from(map.len())),
        Value::String(text) => Value::Number(Number::from(text.chars().count() as u64)),
        Value::Number(number) => number_value(number.as_f64().unwrap_or_default().abs()),
        Value::Null => Value::Number(Number::from(0)),
        _ => Value::Null,
    }
}

fn type_name(input: &Value) -> &'static str {
    match input {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn keys_value(input: &Value) -> Value {
    match input {
        Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            Value::Array(keys.into_iter().map(Value::String).collect())
        }
        Value::Array(items) => Value::Array(
            (0..items.len())
                .map(|index| Value::Number(Number::from(index)))
                .collect(),
        ),
        _ => Value::Null,
    }
}

fn has_value(input: &Value, needle: &Value) -> bool {
    match (input, needle) {
        (Value::Object(map), Value::String(key)) => map.contains_key(key),
        (Value::Array(items), Value::Number(index)) => index
            .as_u64()
            .is_some_and(|index| (index as usize) < items.len()),
        _ => false,
    }
}

fn is_truthy(value: &Value) -> bool {
    !matches!(value, Value::Null | Value::Bool(false))
}

fn apply_path(value: &Value, ops: &[PathOp]) -> Vec<Value> {
    let mut current = vec![value.clone()];
    for op in ops {
        let mut next = Vec::new();
        for value in current {
            next.extend(apply_op(&value, op));
        }
        current = next;
    }
    current
}

fn apply_op(value: &Value, op: &PathOp) -> Vec<Value> {
    match op {
        PathOp::Key(key) => vec![match value {
            Value::Object(map) => map.get(key).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        }],
        PathOp::Index(index) => vec![index_value(value, *index)],
        PathOp::Slice(start, finish) => vec![slice_value(value, *start, *finish)],
        PathOp::Iterate => match value {
            Value::Array(items) => items.clone(),
            Value::Object(map) => map.values().cloned().collect(),
            _ => Vec::new(),
        },
    }
}

fn index_value(value: &Value, index: isize) -> Value {
    match value {
        Value::Array(items) => normalized_index(items.len(), index)
            .and_then(|resolved| items.get(resolved).cloned())
            .unwrap_or(Value::Null),
        Value::String(text) => normalized_index(text.chars().count(), index)
            .and_then(|resolved| text.chars().nth(resolved))
            .map(|ch| Value::String(ch.to_string()))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn slice_value(value: &Value, start: Option<isize>, finish: Option<isize>) -> Value {
    match value {
        Value::Array(items) => {
            let (start, finish) = normalized_bounds(items.len(), start, finish);
            Value::Array(items[start..finish].to_vec())
        }
        Value::String(text) => {
            let chars = text.chars().collect::<Vec<_>>();
            let (start, finish) = normalized_bounds(chars.len(), start, finish);
            Value::String(chars[start..finish].iter().collect())
        }
        _ => Value::Null,
    }
}

fn normalized_index(length: usize, index: isize) -> Option<usize> {
    if length == 0 {
        return None;
    }
    let length = length as isize;
    let resolved = if index < 0 { length + index } else { index };
    if resolved < 0 || resolved >= length {
        None
    } else {
        Some(resolved as usize)
    }
}

fn normalized_bounds(length: usize, start: Option<isize>, finish: Option<isize>) -> (usize, usize) {
    let length = length as isize;
    let start = normalize_bound(start.unwrap_or(0), length);
    let finish = normalize_bound(finish.unwrap_or(length), length);
    if finish < start {
        (start as usize, start as usize)
    } else {
        (start as usize, finish as usize)
    }
}

fn normalize_bound(bound: isize, length: isize) -> isize {
    let resolved = if bound < 0 { length + bound } else { bound };
    resolved.clamp(0, length)
}

struct Parser<'a> {
    source: &'a str,
    index: usize,
}

impl<'a> Parser<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, index: 0 }
    }

    fn parse_filter(&mut self) -> Result<Filter, SandboxError> {
        self.parse_pipe()
    }

    fn parse_pipe(&mut self) -> Result<Filter, SandboxError> {
        let mut filter = self.parse_comma_inner()?;
        loop {
            self.skip_ws();
            if !self.consume_char('|') {
                break;
            }
            filter = Filter::Pipe(Box::new(filter), Box::new(self.parse_comma_inner()?));
        }
        Ok(filter)
    }

    fn parse_pipe_no_comma(&mut self) -> Result<Filter, SandboxError> {
        let mut filter = self.parse_binary(0)?;
        loop {
            self.skip_ws();
            if !self.consume_char('|') {
                break;
            }
            filter = Filter::Pipe(Box::new(filter), Box::new(self.parse_binary(0)?));
        }
        Ok(filter)
    }

    fn parse_comma_inner(&mut self) -> Result<Filter, SandboxError> {
        let mut filters = vec![self.parse_binary(0)?];
        loop {
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
            filters.push(self.parse_binary(0)?);
        }
        if filters.len() == 1 {
            Ok(filters.remove(0))
        } else {
            Ok(Filter::Comma(filters))
        }
    }

    fn parse_binary(&mut self, min_prec: u8) -> Result<Filter, SandboxError> {
        let mut left = self.parse_primary()?;
        loop {
            self.skip_ws();
            let Some((op, prec, width)) = self.peek_binary_op() else {
                break;
            };
            if prec < min_prec {
                break;
            }
            self.index += width;
            let right = self.parse_binary(prec + 1)?;
            left = Filter::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Filter, SandboxError> {
        self.skip_ws();
        match self.peek_char() {
            Some('(') => {
                self.expect_char('(')?;
                let filter = self.parse_filter()?;
                self.skip_ws();
                self.expect_char(')')?;
                Ok(filter)
            }
            Some('.') => self.parse_dot_filter(),
            Some('[') => self.parse_array(),
            Some('{') => self.parse_object(),
            Some('"') => Ok(Filter::Literal(Value::String(self.parse_string()?))),
            Some(ch) if ch.is_ascii_digit() || ch == '-' => self.parse_number(),
            Some(_) => self.parse_identifier_filter(),
            None => Err(SandboxError::InvalidRequest(
                "jq filter ended unexpectedly".to_string(),
            )),
        }
    }

    fn parse_dot_filter(&mut self) -> Result<Filter, SandboxError> {
        self.expect_char('.')?;
        let mut ops = Vec::new();
        while let Some(ch) = self.peek_char() {
            match ch {
                '.' => {
                    self.index += 1;
                }
                '[' => {
                    self.index += 1;
                    self.skip_ws();
                    if self.consume_char(']') {
                        ops.push(PathOp::Iterate);
                        continue;
                    }
                    let content = self.take_until(']')?;
                    self.expect_char(']')?;
                    let content = content.trim();
                    if let Some((start, finish)) = parse_slice(content)? {
                        ops.push(PathOp::Slice(start, finish));
                    } else if let Some(key) = parse_bracket_key(content)? {
                        ops.push(PathOp::Key(key));
                    } else if let Ok(index) = content.parse::<isize>() {
                        ops.push(PathOp::Index(index));
                    } else {
                        return Err(SandboxError::InvalidRequest(format!(
                            "jq bracket expression is not supported: [{content}]"
                        )));
                    }
                }
                ch if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' => {
                    ops.push(PathOp::Key(self.parse_identifier()?));
                }
                _ => break,
            }
        }
        if ops.is_empty() {
            Ok(Filter::Identity)
        } else {
            Ok(Filter::Path(PathExpr { ops }))
        }
    }

    fn parse_array(&mut self) -> Result<Filter, SandboxError> {
        self.expect_char('[')?;
        self.skip_ws();
        if self.consume_char(']') {
            return Ok(Filter::Literal(Value::Array(Vec::new())));
        }
        let inner = self.parse_filter()?;
        self.skip_ws();
        self.expect_char(']')?;
        Ok(Filter::Array(Box::new(inner)))
    }

    fn parse_object(&mut self) -> Result<Filter, SandboxError> {
        self.expect_char('{')?;
        let mut entries = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            let key = if self.peek_char() == Some('"') {
                self.parse_string()?
            } else {
                self.parse_identifier()?
            };
            self.skip_ws();
            let value = if self.consume_char(':') {
                self.parse_pipe_no_comma()?
            } else {
                Filter::Path(PathExpr {
                    ops: vec![PathOp::Key(key.clone())],
                })
            };
            entries.push(ObjectEntry { key, value });
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            self.expect_char(',')?;
        }
        Ok(Filter::Object(entries))
    }

    fn parse_number(&mut self) -> Result<Filter, SandboxError> {
        let start = self.index;
        if self.peek_char() == Some('-') {
            self.index += 1;
        }
        while self.peek_char().is_some_and(|ch| ch.is_ascii_digit()) {
            self.index += 1;
        }
        if self.peek_char() == Some('.') {
            self.index += 1;
            while self.peek_char().is_some_and(|ch| ch.is_ascii_digit()) {
                self.index += 1;
            }
        }
        let value = self.source[start..self.index].parse::<f64>().map_err(|_| {
            SandboxError::InvalidRequest(format!(
                "jq numeric literal is not valid: {}",
                &self.source[start..self.index]
            ))
        })?;
        Ok(Filter::Literal(number_value(value)))
    }

    fn parse_identifier_filter(&mut self) -> Result<Filter, SandboxError> {
        let name = self.parse_identifier()?;
        self.skip_ws();
        if self.consume_char('(') {
            let argument = if self.consume_char(')') {
                None
            } else {
                let filter = self.parse_filter()?;
                self.skip_ws();
                self.expect_char(')')?;
                Some(filter)
            };
            return Ok(Filter::Call(match name.as_str() {
                "has" => FunctionCall::Has(Box::new(argument.ok_or_else(|| {
                    SandboxError::InvalidRequest("jq has() requires one argument".to_string())
                })?)),
                "select" => FunctionCall::Select(Box::new(argument.ok_or_else(|| {
                    SandboxError::InvalidRequest("jq select() requires one argument".to_string())
                })?)),
                "map" => FunctionCall::Map(Box::new(argument.ok_or_else(|| {
                    SandboxError::InvalidRequest("jq map() requires one argument".to_string())
                })?)),
                other => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "jq function is not supported: {other}"
                    )))
                }
            }));
        }
        Ok(match name.as_str() {
            "null" => Filter::Literal(Value::Null),
            "true" => Filter::Literal(Value::Bool(true)),
            "false" => Filter::Literal(Value::Bool(false)),
            "length" => Filter::Call(FunctionCall::Length),
            "type" => Filter::Call(FunctionCall::Type),
            "keys" => Filter::Call(FunctionCall::Keys),
            other => {
                return Err(SandboxError::InvalidRequest(format!(
                    "jq filter segment is not supported: {other}"
                )))
            }
        })
    }

    fn parse_identifier(&mut self) -> Result<String, SandboxError> {
        let start = self.index;
        while self
            .peek_char()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            self.index += 1;
        }
        if start == self.index {
            return Err(SandboxError::InvalidRequest(format!(
                "jq filter token is not supported near: {}",
                &self.source[self.index..]
            )));
        }
        Ok(self.source[start..self.index].to_string())
    }

    fn parse_string(&mut self) -> Result<String, SandboxError> {
        self.expect_char('"')?;
        let mut rendered = String::new();
        while let Some(ch) = self.peek_char() {
            self.index += ch.len_utf8();
            match ch {
                '"' => return Ok(rendered),
                '\\' => {
                    let escaped = self.peek_char().ok_or_else(|| {
                        SandboxError::InvalidRequest(
                            "jq filter has an unterminated string literal".to_string(),
                        )
                    })?;
                    self.index += escaped.len_utf8();
                    rendered.push(match escaped {
                        '"' => '"',
                        '\\' => '\\',
                        '/' => '/',
                        'b' => '\u{0008}',
                        'f' => '\u{000c}',
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        other => other,
                    });
                }
                other => rendered.push(other),
            }
        }
        Err(SandboxError::InvalidRequest(
            "jq filter has an unterminated string literal".to_string(),
        ))
    }

    fn peek_binary_op(&self) -> Option<(BinaryOp, u8, usize)> {
        let rest = &self.source[self.index..];
        for (token, op, prec) in [
            ("//", BinaryOp::Alt, 1),
            ("==", BinaryOp::Eq, 2),
            ("!=", BinaryOp::Ne, 2),
            ("<=", BinaryOp::Le, 2),
            (">=", BinaryOp::Ge, 2),
            ("<", BinaryOp::Lt, 2),
            (">", BinaryOp::Gt, 2),
            ("+", BinaryOp::Add, 3),
            ("-", BinaryOp::Sub, 3),
            ("*", BinaryOp::Mul, 4),
            ("/", BinaryOp::Div, 4),
            ("%", BinaryOp::Mod, 4),
        ] {
            if rest.starts_with(token) {
                return Some((op, prec, token.len()));
            }
        }
        None
    }

    fn take_until(&mut self, target: char) -> Result<String, SandboxError> {
        let start = self.index;
        let mut quote = None::<char>;
        while let Some(ch) = self.peek_char() {
            if let Some(active) = quote {
                self.index += ch.len_utf8();
                if ch == active {
                    quote = None;
                }
                continue;
            }
            if ch == '"' {
                quote = Some(ch);
                self.index += 1;
                continue;
            }
            if ch == target {
                return Ok(self.source[start..self.index].to_string());
            }
            self.index += ch.len_utf8();
        }
        Err(SandboxError::InvalidRequest(
            "jq bracket expression is unterminated".to_string(),
        ))
    }

    fn skip_ws(&mut self) {
        while self.peek_char().is_some_and(char::is_whitespace) {
            self.index += self.peek_char().unwrap().len_utf8();
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), SandboxError> {
        if self.consume_char(expected) {
            Ok(())
        } else {
            Err(SandboxError::InvalidRequest(format!(
                "jq filter token is not supported near: {}",
                &self.source[self.index..]
            )))
        }
    }

    fn consume_char(&mut self, expected: char) -> bool {
        if self.peek_char() == Some(expected) {
            self.index += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.source[self.index..].chars().next()
    }

    fn is_eof(&self) -> bool {
        self.index >= self.source.len()
    }
}

fn parse_slice(source: &str) -> Result<Option<(Option<isize>, Option<isize>)>, SandboxError> {
    let Some((left, right)) = source.split_once(':') else {
        return Ok(None);
    };
    Ok(Some((
        parse_optional_index(left.trim())?,
        parse_optional_index(right.trim())?,
    )))
}

fn parse_optional_index(source: &str) -> Result<Option<isize>, SandboxError> {
    if source.is_empty() {
        return Ok(None);
    }
    source
        .parse::<isize>()
        .map(Some)
        .map_err(|_| SandboxError::InvalidRequest(format!("jq slice bound is not valid: {source}")))
}

fn parse_bracket_key(source: &str) -> Result<Option<String>, SandboxError> {
    if source.len() < 2 {
        return Ok(None);
    }
    let quote = source.chars().next().unwrap_or_default();
    if (quote != '"' && quote != '\'') || !source.ends_with(quote) {
        return Ok(None);
    }
    Ok(Some(source[1..source.len() - 1].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_array_object_and_binary_filters() {
        let program = parse_program("{name, count: (.items | length), ok: (.n > 2)}").unwrap();
        let Filter::Object(entries) = program.filter else {
            panic!("expected object");
        };
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn evaluates_select_map_and_construction() {
        let program = parse_program("[.[] | select(. > 2)] | map(. * 10)").unwrap();
        let input = json!([1, 2, 3, 4]);
        let output = run_program(&program, &[input]);
        assert_eq!(output, vec![json!([30.0, 40.0])]);
    }

    #[test]
    fn evaluates_keys_has_and_type() {
        let keys = parse_program("keys").unwrap();
        let has = parse_program("has(\"a\")").unwrap();
        let ty = parse_program("type").unwrap();
        let input = json!({"b": 1, "a": 2});
        assert_eq!(
            run_program(&keys, std::slice::from_ref(&input)),
            vec![json!(["a", "b"])]
        );
        assert_eq!(
            run_program(&has, std::slice::from_ref(&input)),
            vec![json!(true)]
        );
        assert_eq!(run_program(&ty, &[input]), vec![json!("object")]);
    }

    #[test]
    fn supports_bracket_string_key_access() {
        let program = parse_program(r#".root.user["+@id"]"#).unwrap();
        let input = json!({"root": {"user": {"+@id": "7"}}});
        assert_eq!(run_program(&program, &[input]), vec![json!("7")]);
    }
}
