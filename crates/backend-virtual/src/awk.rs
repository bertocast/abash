use std::collections::BTreeMap;

use abash_core::SandboxError;

pub(crate) fn execute<F>(
    args: &[String],
    stdin: Vec<u8>,
    mut read_file: F,
) -> Result<Vec<u8>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    let spec = parse_invocation(args)?;
    let program = parse_program(&spec.program)?;
    let inputs = read_inputs(&spec.paths, stdin, &mut read_file)?;
    let lines = run_program(&program, &spec.vars, spec.delimiter, &inputs)?;
    Ok(render_output(&lines))
}

struct Invocation {
    delimiter: Option<char>,
    vars: BTreeMap<String, String>,
    program: String,
    paths: Vec<String>,
}

struct AwkProgram {
    rules: Vec<AwkRule>,
}

struct AwkRule {
    kind: RuleKind,
    actions: Vec<AwkStmt>,
}

enum RuleKind {
    Begin,
    End,
    Main(Option<AwkExpr>),
}

enum AwkStmt {
    Print(Vec<AwkExpr>),
    Assign {
        name: String,
        op: AssignOp,
        expr: AwkExpr,
    },
}

enum AssignOp {
    Set,
    Add,
    Sub,
    Mul,
    Div,
}

enum AwkExpr {
    EntireLine,
    Field(usize),
    Counter(AwkCounter),
    Variable(String),
    StringLiteral(String),
    NumberLiteral(f64),
    UnaryMinus(Box<AwkExpr>),
    Binary(Box<AwkExpr>, BinaryOp, Box<AwkExpr>),
}

enum AwkCounter {
    Nf,
    Nr,
    Fnr,
}

enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    And,
    Or,
    Contains,
}

struct AwkInput {
    filename: String,
    text: String,
}

struct AwkRecord<'a> {
    line: &'a str,
    fields: Vec<&'a str>,
    nr: usize,
    fnr: usize,
    filename: &'a str,
}

#[derive(Clone, Debug)]
enum AwkValue {
    Number(f64),
    String(String),
}

impl AwkValue {
    fn to_number(&self) -> f64 {
        match self {
            Self::Number(value) => *value,
            Self::String(value) => value.trim().parse::<f64>().unwrap_or(0.0),
        }
    }

    fn to_string_value(&self) -> String {
        match self {
            Self::Number(value) => format_number(*value),
            Self::String(value) => value.clone(),
        }
    }

    fn truthy(&self) -> bool {
        match self {
            Self::Number(value) => *value != 0.0,
            Self::String(value) => {
                if value.is_empty() {
                    false
                } else {
                    value
                        .trim()
                        .parse::<f64>()
                        .map(|number| number != 0.0)
                        .unwrap_or(true)
                }
            }
        }
    }
}

#[derive(Default)]
struct RuntimeState {
    vars: BTreeMap<String, AwkValue>,
    output: Vec<String>,
}

fn parse_invocation(args: &[String]) -> Result<Invocation, SandboxError> {
    let mut delimiter = None;
    let mut vars = BTreeMap::new();
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        if flag == "-F" {
            let Some(value) = args.get(index + 1) else {
                return Err(SandboxError::InvalidRequest(
                    "awk -F requires a single-character delimiter".to_string(),
                ));
            };
            delimiter = Some(parse_delimiter(value)?);
            index += 2;
            continue;
        }
        if let Some(value) = flag.strip_prefix("-F") {
            if value.is_empty() {
                return Err(SandboxError::InvalidRequest(
                    "awk -F requires a single-character delimiter".to_string(),
                ));
            }
            delimiter = Some(parse_delimiter(value)?);
            index += 1;
            continue;
        }
        if flag == "-v" {
            let Some(value) = args.get(index + 1) else {
                return Err(SandboxError::InvalidRequest(
                    "awk -v requires NAME=VALUE".to_string(),
                ));
            };
            parse_var_assignment(value, &mut vars)?;
            index += 2;
            continue;
        }
        if flag.starts_with('-') {
            return Err(SandboxError::InvalidRequest(format!(
                "awk flag is not supported: {flag}"
            )));
        }
        break;
    }

    let Some(program) = args.get(index) else {
        return Err(SandboxError::InvalidRequest(
            "awk requires a program string".to_string(),
        ));
    };

    Ok(Invocation {
        delimiter,
        vars,
        program: program.clone(),
        paths: args[index + 1..].to_vec(),
    })
}

fn parse_var_assignment(
    source: &str,
    vars: &mut BTreeMap<String, String>,
) -> Result<(), SandboxError> {
    let Some((name, value)) = source.split_once('=') else {
        return Err(SandboxError::InvalidRequest(
            "awk -v requires NAME=VALUE".to_string(),
        ));
    };
    if !is_identifier(name) {
        return Err(SandboxError::InvalidRequest(format!(
            "awk variable name is invalid: {name}"
        )));
    }
    vars.insert(name.to_string(), value.to_string());
    Ok(())
}

fn parse_delimiter(value: &str) -> Result<char, SandboxError> {
    let mut chars = value.chars();
    let Some(delimiter) = chars.next() else {
        return Err(SandboxError::InvalidRequest(
            "awk delimiter must be a single character".to_string(),
        ));
    };
    if chars.next().is_some() {
        return Err(SandboxError::InvalidRequest(
            "awk delimiter must be a single character".to_string(),
        ));
    }
    Ok(delimiter)
}

fn read_inputs<F>(
    paths: &[String],
    stdin: Vec<u8>,
    read_file: &mut F,
) -> Result<Vec<AwkInput>, SandboxError>
where
    F: FnMut(&str) -> Result<Vec<u8>, SandboxError>,
{
    if paths.is_empty() {
        return Ok(vec![AwkInput {
            filename: String::new(),
            text: bytes_to_text(stdin, "awk currently requires UTF-8 text input")?,
        }]);
    }

    let mut inputs = Vec::new();
    for path in paths {
        inputs.push(AwkInput {
            filename: path.clone(),
            text: bytes_to_text(read_file(path)?, "awk currently requires UTF-8 text input")?,
        });
    }
    Ok(inputs)
}

fn bytes_to_text(contents: Vec<u8>, message: &str) -> Result<String, SandboxError> {
    String::from_utf8(contents).map_err(|_| SandboxError::InvalidRequest(message.to_string()))
}

fn parse_program(source: &str) -> Result<AwkProgram, SandboxError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "awk program must not be empty".to_string(),
        ));
    }

    let mut rules = Vec::new();
    let mut offset = 0usize;
    while offset < source.len() {
        offset += skip_whitespace(&source[offset..]);
        if offset >= source.len() {
            break;
        }

        let open = find_char_outside_quotes(&source[offset..], '{').ok_or_else(|| {
            SandboxError::InvalidRequest("awk action block is missing an opening brace".to_string())
        })? + offset;
        let close = find_matching_brace(source, open)?;
        let pattern_text = source[offset..open].trim();
        let action_text = source[open + 1..close].trim();
        let kind = parse_rule_kind(pattern_text)?;
        let actions = parse_action_block(action_text)?;
        rules.push(AwkRule { kind, actions });
        offset = close + 1;
    }

    if rules.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "awk program must contain at least one rule".to_string(),
        ));
    }

    Ok(AwkProgram { rules })
}

fn skip_whitespace(source: &str) -> usize {
    source
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index))
        .unwrap_or(source.len())
}

fn parse_rule_kind(source: &str) -> Result<RuleKind, SandboxError> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Ok(RuleKind::Main(None));
    }
    match trimmed {
        "BEGIN" => Ok(RuleKind::Begin),
        "END" => Ok(RuleKind::End),
        _ => Ok(RuleKind::Main(Some(parse_expr(trimmed)?))),
    }
}

fn parse_action_block(source: &str) -> Result<Vec<AwkStmt>, SandboxError> {
    let mut actions = Vec::new();
    for part in split_outside_quotes(source, ';')? {
        let stmt = part.trim();
        if stmt.is_empty() {
            continue;
        }
        actions.push(parse_stmt(stmt)?);
    }
    if actions.is_empty() {
        actions.push(AwkStmt::Print(vec![AwkExpr::EntireLine]));
    }
    Ok(actions)
}

fn parse_stmt(source: &str) -> Result<AwkStmt, SandboxError> {
    let source = source.trim();
    if let Some(remainder) = source.strip_prefix("print") {
        return Ok(AwkStmt::Print(parse_print_exprs(remainder.trim())?));
    }

    for (needle, op) in [
        ("+=", AssignOp::Add),
        ("-=", AssignOp::Sub),
        ("*=", AssignOp::Mul),
        ("/=", AssignOp::Div),
        ("=", AssignOp::Set),
    ] {
        if let Some(index) = find_operator_outside_quotes(source, needle) {
            let name = source[..index].trim();
            if !is_identifier(name) {
                return Err(SandboxError::InvalidRequest(format!(
                    "awk assignment target is not supported: {name}"
                )));
            }
            return Ok(AwkStmt::Assign {
                name: name.to_string(),
                op,
                expr: parse_expr(source[index + needle.len()..].trim())?,
            });
        }
    }

    Err(SandboxError::InvalidRequest(format!(
        "awk statement is not supported: {source}"
    )))
}

fn parse_print_exprs(source: &str) -> Result<Vec<AwkExpr>, SandboxError> {
    if source.is_empty() {
        return Ok(vec![AwkExpr::EntireLine]);
    }
    split_outside_quotes(source, ',')?
        .into_iter()
        .map(parse_expr)
        .collect()
}

fn parse_expr(source: &str) -> Result<AwkExpr, SandboxError> {
    let mut parser = ExprParser::new(source);
    let expr = parser.parse_expr()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(SandboxError::InvalidRequest(format!(
            "awk expression is not supported: {}",
            source.trim()
        )));
    }
    Ok(expr)
}

struct ExprParser<'a> {
    source: &'a str,
    index: usize,
}

impl<'a> ExprParser<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, index: 0 }
    }

    fn parse_expr(&mut self) -> Result<AwkExpr, SandboxError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<AwkExpr, SandboxError> {
        let mut expr = self.parse_and()?;
        loop {
            self.skip_ws();
            if self.consume("||") {
                let right = self.parse_and()?;
                expr = AwkExpr::Binary(Box::new(expr), BinaryOp::Or, Box::new(right));
            } else {
                return Ok(expr);
            }
        }
    }

    fn parse_and(&mut self) -> Result<AwkExpr, SandboxError> {
        let mut expr = self.parse_compare()?;
        loop {
            self.skip_ws();
            if self.consume("&&") {
                let right = self.parse_compare()?;
                expr = AwkExpr::Binary(Box::new(expr), BinaryOp::And, Box::new(right));
            } else {
                return Ok(expr);
            }
        }
    }

    fn parse_compare(&mut self) -> Result<AwkExpr, SandboxError> {
        let mut expr = self.parse_add()?;
        loop {
            self.skip_ws();
            let op = if self.consume("==") {
                Some(BinaryOp::Eq)
            } else if self.consume("!=") {
                Some(BinaryOp::Ne)
            } else if self.consume(">=") {
                Some(BinaryOp::Ge)
            } else if self.consume("<=") {
                Some(BinaryOp::Le)
            } else if self.consume(">") {
                Some(BinaryOp::Gt)
            } else if self.consume("<") {
                Some(BinaryOp::Lt)
            } else if self.consume("~") {
                Some(BinaryOp::Contains)
            } else {
                None
            };
            let Some(op) = op else {
                return Ok(expr);
            };
            let right = self.parse_add()?;
            expr = AwkExpr::Binary(Box::new(expr), op, Box::new(right));
        }
    }

    fn parse_add(&mut self) -> Result<AwkExpr, SandboxError> {
        let mut expr = self.parse_mul()?;
        loop {
            self.skip_ws();
            if self.consume("+") {
                let right = self.parse_mul()?;
                expr = AwkExpr::Binary(Box::new(expr), BinaryOp::Add, Box::new(right));
            } else if self.consume("-") {
                let right = self.parse_mul()?;
                expr = AwkExpr::Binary(Box::new(expr), BinaryOp::Sub, Box::new(right));
            } else {
                return Ok(expr);
            }
        }
    }

    fn parse_mul(&mut self) -> Result<AwkExpr, SandboxError> {
        let mut expr = self.parse_unary()?;
        loop {
            self.skip_ws();
            if self.consume("*") {
                let right = self.parse_unary()?;
                expr = AwkExpr::Binary(Box::new(expr), BinaryOp::Mul, Box::new(right));
            } else if self.consume("/") {
                let right = self.parse_unary()?;
                expr = AwkExpr::Binary(Box::new(expr), BinaryOp::Div, Box::new(right));
            } else {
                return Ok(expr);
            }
        }
    }

    fn parse_unary(&mut self) -> Result<AwkExpr, SandboxError> {
        self.skip_ws();
        if self.consume("-") {
            return Ok(AwkExpr::UnaryMinus(Box::new(self.parse_unary()?)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<AwkExpr, SandboxError> {
        self.skip_ws();
        if self.consume("(") {
            let expr = self.parse_expr()?;
            self.skip_ws();
            if !self.consume(")") {
                return Err(SandboxError::InvalidRequest(
                    "awk expression is missing a closing ')'".to_string(),
                ));
            }
            return Ok(expr);
        }
        if let Some(text) = self.parse_string()? {
            return Ok(AwkExpr::StringLiteral(text));
        }
        if self.consume("$0") {
            return Ok(AwkExpr::EntireLine);
        }
        if self.peek_char() == Some('$') {
            self.index += 1;
            let number = self.read_while(|ch| ch.is_ascii_digit());
            if number.is_empty() {
                return Err(SandboxError::InvalidRequest(
                    "awk field references must be $0 or positive integers".to_string(),
                ));
            }
            let index = number.parse::<usize>().map_err(|_| {
                SandboxError::InvalidRequest(
                    "awk field references must be $0 or positive integers".to_string(),
                )
            })?;
            return if index == 0 {
                Ok(AwkExpr::EntireLine)
            } else {
                Ok(AwkExpr::Field(index))
            };
        }
        if let Some(number) = self.parse_number()? {
            return Ok(AwkExpr::NumberLiteral(number));
        }
        let ident = self.read_identifier();
        if ident.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "awk expression must not be empty".to_string(),
            ));
        }
        Ok(match ident.as_str() {
            "NF" => AwkExpr::Counter(AwkCounter::Nf),
            "NR" => AwkExpr::Counter(AwkCounter::Nr),
            "FNR" => AwkExpr::Counter(AwkCounter::Fnr),
            "FILENAME" => AwkExpr::Variable("FILENAME".to_string()),
            _ => AwkExpr::Variable(ident),
        })
    }

    fn parse_string(&mut self) -> Result<Option<String>, SandboxError> {
        let Some(quote) = self.peek_char() else {
            return Ok(None);
        };
        if quote != '"' && quote != '\'' {
            return Ok(None);
        }
        self.index += quote.len_utf8();
        let start = self.index;
        while let Some(ch) = self.peek_char() {
            if ch == quote {
                let value = self.source[start..self.index].to_string();
                self.index += ch.len_utf8();
                return Ok(Some(value));
            }
            self.index += ch.len_utf8();
        }
        Err(SandboxError::InvalidRequest(
            "awk string literal is unterminated".to_string(),
        ))
    }

    fn parse_number(&mut self) -> Result<Option<f64>, SandboxError> {
        let start = self.index;
        let mut saw_digit = false;
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() {
                saw_digit = true;
                self.index += ch.len_utf8();
            } else if ch == '.' {
                self.index += ch.len_utf8();
            } else {
                break;
            }
        }
        if !saw_digit {
            self.index = start;
            return Ok(None);
        }
        self.source[start..self.index]
            .parse::<f64>()
            .map(Some)
            .map_err(|_| SandboxError::InvalidRequest("awk number literal is invalid".to_string()))
    }

    fn read_identifier(&mut self) -> String {
        self.read_while(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    }

    fn read_while(&mut self, predicate: impl Fn(char) -> bool) -> String {
        let start = self.index;
        while let Some(ch) = self.peek_char() {
            if predicate(ch) {
                self.index += ch.len_utf8();
            } else {
                break;
            }
        }
        self.source[start..self.index].to_string()
    }

    fn consume(&mut self, needle: &str) -> bool {
        if self.source[self.index..].starts_with(needle) {
            self.index += needle.len();
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.index += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.source[self.index..].chars().next()
    }

    fn is_eof(&self) -> bool {
        self.index >= self.source.len()
    }
}

fn find_char_outside_quotes(source: &str, target: char) -> Option<usize> {
    let mut active_quote = None::<char>;
    for (index, ch) in source.char_indices() {
        match active_quote {
            Some(quote) if ch == quote => active_quote = None,
            None if ch == '\'' || ch == '"' => active_quote = Some(ch),
            None if ch == target => return Some(index),
            _ => {}
        }
    }
    None
}

fn find_matching_brace(source: &str, open_index: usize) -> Result<usize, SandboxError> {
    let mut active_quote = None::<char>;
    let mut depth = 0usize;
    for (index, ch) in source[open_index..].char_indices() {
        let absolute = open_index + index;
        match active_quote {
            Some(quote) if ch == quote => active_quote = None,
            None if ch == '\'' || ch == '"' => active_quote = Some(ch),
            None if ch == '{' => depth += 1,
            None if ch == '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok(absolute);
                }
            }
            _ => {}
        }
    }
    Err(SandboxError::InvalidRequest(
        "awk action block is missing a closing brace".to_string(),
    ))
}

fn find_operator_outside_quotes(source: &str, operator: &str) -> Option<usize> {
    let mut active_quote = None::<char>;
    let mut index = 0usize;
    while index < source.len() {
        let ch = source[index..].chars().next()?;
        match active_quote {
            Some(quote) if ch == quote => {
                active_quote = None;
                index += ch.len_utf8();
            }
            None if ch == '\'' || ch == '"' => {
                active_quote = Some(ch);
                index += ch.len_utf8();
            }
            None if source[index..].starts_with(operator) => return Some(index),
            _ => index += ch.len_utf8(),
        }
    }
    None
}

fn split_outside_quotes(source: &str, delimiter: char) -> Result<Vec<&str>, SandboxError> {
    let mut parts = Vec::new();
    let mut active_quote = None::<char>;
    let mut start = 0usize;

    for (index, ch) in source.char_indices() {
        match active_quote {
            Some(quote) if ch == quote => active_quote = None,
            None if ch == '\'' || ch == '"' => active_quote = Some(ch),
            None if ch == delimiter => {
                parts.push(source[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    if active_quote.is_some() {
        return Err(SandboxError::InvalidRequest(
            "awk string literal is unterminated".to_string(),
        ));
    }

    parts.push(source[start..].trim());
    Ok(parts)
}

fn is_identifier(source: &str) -> bool {
    let mut chars = source.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn run_program(
    program: &AwkProgram,
    initial_vars: &BTreeMap<String, String>,
    delimiter: Option<char>,
    inputs: &[AwkInput],
) -> Result<Vec<String>, SandboxError> {
    let mut state = RuntimeState::default();
    for (name, value) in initial_vars {
        state.vars.insert(name.clone(), parsed_scalar(value));
    }

    execute_rules(program, RuleKindMatcher::Begin, None, &mut state)?;

    let mut nr = 0usize;
    for input in inputs {
        let mut fnr = 0usize;
        for line in input.text.lines() {
            nr += 1;
            fnr += 1;
            let record = AwkRecord {
                line,
                fields: split_fields(line, delimiter),
                nr,
                fnr,
                filename: &input.filename,
            };
            execute_rules(program, RuleKindMatcher::Main, Some(&record), &mut state)?;
        }
    }

    let final_record = inputs.last().map(|input| AwkRecord {
        line: "",
        fields: Vec::new(),
        nr: inputs
            .iter()
            .map(|input| input.text.lines().count())
            .sum::<usize>(),
        fnr: inputs
            .last()
            .map(|input| input.text.lines().count())
            .unwrap_or(0),
        filename: &input.filename,
    });
    execute_rules(
        program,
        RuleKindMatcher::End,
        final_record.as_ref(),
        &mut state,
    )?;
    Ok(state.output)
}

enum RuleKindMatcher {
    Begin,
    Main,
    End,
}

fn execute_rules(
    program: &AwkProgram,
    target: RuleKindMatcher,
    record: Option<&AwkRecord<'_>>,
    state: &mut RuntimeState,
) -> Result<(), SandboxError> {
    for rule in &program.rules {
        let matches = match (&target, &rule.kind) {
            (RuleKindMatcher::Begin, RuleKind::Begin) => true,
            (RuleKindMatcher::End, RuleKind::End) => true,
            (RuleKindMatcher::Main, RuleKind::Main(None)) => true,
            (RuleKindMatcher::Main, RuleKind::Main(Some(expr))) => {
                eval_expr(expr, record, state)?.truthy()
            }
            _ => false,
        };
        if matches {
            for stmt in &rule.actions {
                execute_stmt(stmt, record, state)?;
            }
        }
    }
    Ok(())
}

fn split_fields(line: &str, delimiter: Option<char>) -> Vec<&str> {
    match delimiter {
        Some(delimiter) => line.split(delimiter).collect(),
        None => line.split_whitespace().collect(),
    }
}

fn execute_stmt(
    stmt: &AwkStmt,
    record: Option<&AwkRecord<'_>>,
    state: &mut RuntimeState,
) -> Result<(), SandboxError> {
    match stmt {
        AwkStmt::Print(exprs) => {
            let mut rendered = Vec::new();
            for expr in exprs {
                rendered.push(eval_expr(expr, record, state)?.to_string_value());
            }
            state.output.push(rendered.join(" "));
        }
        AwkStmt::Assign { name, op, expr } => {
            let right = eval_expr(expr, record, state)?;
            let next = match op {
                AssignOp::Set => right,
                AssignOp::Add => AwkValue::Number(
                    state.vars.get(name).map(AwkValue::to_number).unwrap_or(0.0)
                        + right.to_number(),
                ),
                AssignOp::Sub => AwkValue::Number(
                    state.vars.get(name).map(AwkValue::to_number).unwrap_or(0.0)
                        - right.to_number(),
                ),
                AssignOp::Mul => AwkValue::Number(
                    state.vars.get(name).map(AwkValue::to_number).unwrap_or(0.0)
                        * right.to_number(),
                ),
                AssignOp::Div => AwkValue::Number(
                    state.vars.get(name).map(AwkValue::to_number).unwrap_or(0.0)
                        / right.to_number(),
                ),
            };
            state.vars.insert(name.clone(), next);
        }
    }
    Ok(())
}

fn eval_expr(
    expr: &AwkExpr,
    record: Option<&AwkRecord<'_>>,
    state: &RuntimeState,
) -> Result<AwkValue, SandboxError> {
    Ok(match expr {
        AwkExpr::EntireLine => AwkValue::String(
            record
                .map(|value| value.line)
                .unwrap_or_default()
                .to_string(),
        ),
        AwkExpr::Field(index) => AwkValue::String(
            record
                .and_then(|value| value.fields.get(index - 1).copied())
                .unwrap_or_default()
                .to_string(),
        ),
        AwkExpr::Counter(AwkCounter::Nf) => {
            AwkValue::Number(record.map(|value| value.fields.len() as f64).unwrap_or(0.0))
        }
        AwkExpr::Counter(AwkCounter::Nr) => {
            AwkValue::Number(record.map(|value| value.nr as f64).unwrap_or(0.0))
        }
        AwkExpr::Counter(AwkCounter::Fnr) => {
            AwkValue::Number(record.map(|value| value.fnr as f64).unwrap_or(0.0))
        }
        AwkExpr::Variable(name) => {
            if name == "FILENAME" {
                AwkValue::String(
                    record
                        .map(|value| value.filename)
                        .unwrap_or_default()
                        .to_string(),
                )
            } else {
                state
                    .vars
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| AwkValue::String(String::new()))
            }
        }
        AwkExpr::StringLiteral(value) => AwkValue::String(value.clone()),
        AwkExpr::NumberLiteral(value) => AwkValue::Number(*value),
        AwkExpr::UnaryMinus(value) => {
            AwkValue::Number(-eval_expr(value, record, state)?.to_number())
        }
        AwkExpr::Binary(left, op, right) => {
            let left = eval_expr(left, record, state)?;
            let right = eval_expr(right, record, state)?;
            match op {
                BinaryOp::Add => AwkValue::Number(left.to_number() + right.to_number()),
                BinaryOp::Sub => AwkValue::Number(left.to_number() - right.to_number()),
                BinaryOp::Mul => AwkValue::Number(left.to_number() * right.to_number()),
                BinaryOp::Div => AwkValue::Number(left.to_number() / right.to_number()),
                BinaryOp::Eq => {
                    AwkValue::Number(
                        compare_values(&left, &right, |a, b| a == b, |a, b| a == b) as i32 as f64
                    )
                }
                BinaryOp::Ne => {
                    AwkValue::Number(
                        compare_values(&left, &right, |a, b| a != b, |a, b| a != b) as i32 as f64
                    )
                }
                BinaryOp::Gt => {
                    AwkValue::Number(
                        compare_values(&left, &right, |a, b| a > b, |a, b| a > b) as i32 as f64
                    )
                }
                BinaryOp::Ge => {
                    AwkValue::Number(
                        compare_values(&left, &right, |a, b| a >= b, |a, b| a >= b) as i32 as f64
                    )
                }
                BinaryOp::Lt => {
                    AwkValue::Number(
                        compare_values(&left, &right, |a, b| a < b, |a, b| a < b) as i32 as f64
                    )
                }
                BinaryOp::Le => {
                    AwkValue::Number(
                        compare_values(&left, &right, |a, b| a <= b, |a, b| a <= b) as i32 as f64
                    )
                }
                BinaryOp::And => AwkValue::Number((left.truthy() && right.truthy()) as i32 as f64),
                BinaryOp::Or => AwkValue::Number((left.truthy() || right.truthy()) as i32 as f64),
                BinaryOp::Contains => AwkValue::Number(
                    left.to_string_value().contains(&right.to_string_value()) as i32 as f64,
                ),
            }
        }
    })
}

fn compare_values(
    left: &AwkValue,
    right: &AwkValue,
    numeric: impl Fn(f64, f64) -> bool,
    stringy: impl Fn(&str, &str) -> bool,
) -> bool {
    match (left, right) {
        (AwkValue::Number(a), AwkValue::Number(b)) => numeric(*a, *b),
        _ => {
            let left_text = left.to_string_value();
            let right_text = right.to_string_value();
            if let (Ok(a), Ok(b)) = (left_text.parse::<f64>(), right_text.parse::<f64>()) {
                numeric(a, b)
            } else {
                stringy(&left_text, &right_text)
            }
        }
    }
}

fn parsed_scalar(value: &str) -> AwkValue {
    value
        .parse::<f64>()
        .map(AwkValue::Number)
        .unwrap_or_else(|_| AwkValue::String(value.to_string()))
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn render_output(lines: &[String]) -> Vec<u8> {
    if lines.is_empty() {
        Vec::new()
    } else {
        format!("{}\n", lines.join("\n")).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rules_with_begin_and_end() {
        let program = parse_program(
            r#"BEGIN { total = 1 } $2 == "core" { total += 1; print $1 } END { print total }"#,
        )
        .unwrap();
        assert_eq!(program.rules.len(), 3);
        assert!(matches!(program.rules[0].kind, RuleKind::Begin));
        assert!(matches!(program.rules[2].kind, RuleKind::End));
    }

    #[test]
    fn executes_records_with_stateful_accumulators() {
        let program = parse_program(
            r#"BEGIN { total = 0 } $2 == "core" { total += $3 } END { print total }"#,
        )
        .unwrap();
        let inputs = vec![AwkInput {
            filename: "/workspace/data.csv".to_string(),
            text: "bert,core,2\nana,product,9\ncami,core,3\n".to_string(),
        }];

        let output = run_program(&program, &BTreeMap::new(), Some(','), &inputs).unwrap();
        assert_eq!(output, vec!["5".to_string()]);
    }

    #[test]
    fn supports_v_assignments_and_filename() {
        let program = parse_program(r#"BEGIN { print greeting } { print FILENAME, $1 }"#).unwrap();
        let mut vars = BTreeMap::new();
        vars.insert("greeting".to_string(), "hello".to_string());
        let inputs = vec![AwkInput {
            filename: "/workspace/data.txt".to_string(),
            text: "bert core\n".to_string(),
        }];

        let output = run_program(&program, &vars, None, &inputs).unwrap();
        assert_eq!(
            output,
            vec!["hello".to_string(), "/workspace/data.txt bert".to_string()]
        );
    }
}
