use std::collections::BTreeMap;

use abash_core::SandboxError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChainOp {
    Seq,
    AndIf,
    OrIf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RedirectSpec {
    Input(ScriptWord),
    StdoutTruncate(ScriptWord),
    StdoutAppend(ScriptWord),
    StderrTruncate(ScriptWord),
    StderrAppend(ScriptWord),
    StderrToStdout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SimpleCommand {
    pub assignments: Vec<(String, ScriptWord)>,
    pub argv: Vec<ScriptWord>,
    pub redirects: Vec<RedirectSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Pipeline {
    pub commands: Vec<SimpleCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScriptStep {
    pub op: Option<ChainOp>,
    pub kind: StepKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StepKind {
    Pipeline(Pipeline),
    If(IfBlock),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IfBlock {
    pub condition: Pipeline,
    pub then_steps: Vec<ScriptStep>,
    pub else_steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScriptWord {
    parts: Vec<WordPart>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WordPart {
    text: String,
    expandable: bool,
    keyword_eligible: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    Word(ScriptWord),
    Pipe,
    AndIf,
    OrIf,
    Semicolon,
    RedirectIn,
    RedirectOut,
    RedirectAppend,
    RedirectErrOut,
    RedirectErrAppend,
    RedirectErrToStdout,
}

pub(crate) fn parse_script(source: &str) -> Result<Vec<ScriptStep>, SandboxError> {
    let tokens = tokenize(source)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

impl ScriptWord {
    fn new(parts: Vec<WordPart>) -> Self {
        Self { parts }
    }

    #[cfg(test)]
    fn literal(&self) -> String {
        let mut output = String::new();
        for part in &self.parts {
            output.push_str(&part.text);
        }
        output
    }

    pub(crate) fn expand(
        &self,
        env: &BTreeMap<String, String>,
        positional_args: &[String],
    ) -> Result<String, SandboxError> {
        let mut output = String::new();
        for part in &self.parts {
            if part.expandable {
                output.push_str(&expand_part(&part.text, env, positional_args)?);
            } else {
                output.push_str(&part.text);
            }
        }
        Ok(output)
    }

    pub(crate) fn expands_to_positional_args(&self) -> bool {
        self.parts.len() == 1 && self.parts[0].expandable && self.parts[0].text == "$@"
    }

    fn is_keyword(&self, keyword: &str) -> bool {
        self.parts.iter().all(|part| part.keyword_eligible) && self.literal_value() == keyword
    }

    fn literal_value(&self) -> String {
        let mut output = String::new();
        for part in &self.parts {
            output.push_str(&part.text);
        }
        output
    }

    fn split_assignment(&self) -> Option<(String, ScriptWord)> {
        let mut name = String::new();

        for (part_index, part) in self.parts.iter().enumerate() {
            for (char_index, ch) in part.text.char_indices() {
                if ch == '=' {
                    if !is_valid_assignment_name(&name) {
                        return None;
                    }
                    let mut value_parts = Vec::new();
                    let suffix = &part.text[char_index + ch.len_utf8()..];
                    if !suffix.is_empty() {
                        value_parts.push(WordPart {
                            text: suffix.to_string(),
                            expandable: part.expandable,
                            keyword_eligible: part.keyword_eligible,
                        });
                    }
                    for next_part in self.parts.iter().skip(part_index + 1) {
                        value_parts.push(next_part.clone());
                    }
                    return Some((name, ScriptWord::new(value_parts)));
                }
                name.push(ch);
            }
        }

        None
    }
}

fn tokenize(source: &str) -> Result<Vec<Token>, SandboxError> {
    let mut tokens = Vec::new();
    let mut chars = source.chars().peekable();
    let mut builder = WordBuilder::default();

    while let Some(ch) = chars.next() {
        match ch {
            ' ' | '\t' | '\r' => builder.flush(&mut tokens),
            '\n' => {
                builder.flush(&mut tokens);
                tokens.push(Token::Semicolon);
            }
            '2' if builder.is_empty() => {
                if chars.next_if_eq(&'>').is_some() {
                    if chars.next_if_eq(&'&').is_some() {
                        if chars.next_if_eq(&'1').is_some() {
                            tokens.push(Token::RedirectErrToStdout);
                        } else {
                            return Err(SandboxError::InvalidRequest(
                                "unsupported file-descriptor redirect in script".to_string(),
                            ));
                        }
                    } else if chars.next_if_eq(&'>').is_some() {
                        tokens.push(Token::RedirectErrAppend);
                    } else {
                        tokens.push(Token::RedirectErrOut);
                    }
                } else {
                    builder.push_char(ch, true, true);
                }
            }
            '\'' => {
                let mut closed = false;
                while let Some(next) = chars.next() {
                    if next == '\'' {
                        closed = true;
                        break;
                    }
                    builder.push_char(next, false, false);
                }
                if !closed {
                    return Err(SandboxError::InvalidRequest(
                        "unterminated single-quoted string in script".to_string(),
                    ));
                }
            }
            '"' => {
                let mut closed = false;
                while let Some(next) = chars.next() {
                    match next {
                        '"' => {
                            closed = true;
                            break;
                        }
                        '\\' => {
                            let escaped = chars.next().ok_or_else(|| {
                                SandboxError::InvalidRequest(
                                    "unterminated escape in double-quoted string".to_string(),
                                )
                            })?;
                            builder.push_char(escaped, true, false);
                        }
                        _ => builder.push_char(next, true, false),
                    }
                }
                if !closed {
                    return Err(SandboxError::InvalidRequest(
                        "unterminated double-quoted string in script".to_string(),
                    ));
                }
            }
            '\\' => {
                let escaped = chars.next().ok_or_else(|| {
                    SandboxError::InvalidRequest(
                        "unterminated escape sequence in script".to_string(),
                    )
                })?;
                builder.push_char(escaped, true, false);
            }
            '#' if builder.is_empty() => {
                builder.flush(&mut tokens);
                for next in chars.by_ref() {
                    if next == '\n' {
                        tokens.push(Token::Semicolon);
                        break;
                    }
                }
            }
            '|' => {
                builder.flush(&mut tokens);
                if chars.next_if_eq(&'|').is_some() {
                    tokens.push(Token::OrIf);
                } else {
                    tokens.push(Token::Pipe);
                }
            }
            '&' => {
                builder.flush(&mut tokens);
                if chars.next_if_eq(&'&').is_some() {
                    tokens.push(Token::AndIf);
                } else {
                    return Err(SandboxError::InvalidRequest(
                        "unsupported script operator: &".to_string(),
                    ));
                }
            }
            ';' => {
                builder.flush(&mut tokens);
                tokens.push(Token::Semicolon);
            }
            '<' => {
                builder.flush(&mut tokens);
                tokens.push(Token::RedirectIn);
            }
            '>' => {
                builder.flush(&mut tokens);
                if chars.next_if_eq(&'>').is_some() {
                    tokens.push(Token::RedirectAppend);
                } else {
                    tokens.push(Token::RedirectOut);
                }
            }
            _ => builder.push_char(ch, true, true),
        }
    }

    builder.flush(&mut tokens);
    Ok(tokens)
}

#[derive(Default)]
struct WordBuilder {
    parts: Vec<WordPart>,
}

impl WordBuilder {
    fn push_char(&mut self, ch: char, expandable: bool, keyword_eligible: bool) {
        if let Some(last) = self.parts.last_mut() {
            if last.expandable == expandable && last.keyword_eligible == keyword_eligible {
                last.text.push(ch);
                return;
            }
        }
        self.parts.push(WordPart {
            text: ch.to_string(),
            expandable,
            keyword_eligible,
        });
    }

    fn flush(&mut self, tokens: &mut Vec<Token>) {
        if self.parts.is_empty() {
            return;
        }
        tokens.push(Token::Word(ScriptWord::new(std::mem::take(
            &mut self.parts,
        ))));
    }

    fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    fn parse(&mut self) -> Result<Vec<ScriptStep>, SandboxError> {
        self.parse_steps_until(&[])
    }

    fn parse_steps_until(
        &mut self,
        stop_keywords: &[&str],
    ) -> Result<Vec<ScriptStep>, SandboxError> {
        let mut steps = Vec::new();

        while self.skip_semicolons() {
            if self.peek_keyword(stop_keywords) {
                break;
            }
            let op = if steps.is_empty() {
                None
            } else {
                Some(ChainOp::Seq)
            };
            steps.push(ScriptStep {
                op,
                kind: self.parse_step_kind()?,
            });

            loop {
                if self.peek_keyword(stop_keywords) {
                    return Ok(steps);
                }
                match self.peek() {
                    Some(Token::Semicolon) => {
                        self.index += 1;
                        while matches!(self.peek(), Some(Token::Semicolon)) {
                            self.index += 1;
                        }
                        if self.peek_keyword(stop_keywords) {
                            return Ok(steps);
                        }
                        break;
                    }
                    Some(Token::AndIf) => {
                        self.index += 1;
                        steps.push(ScriptStep {
                            op: Some(ChainOp::AndIf),
                            kind: self.parse_step_kind()?,
                        });
                    }
                    Some(Token::OrIf) => {
                        self.index += 1;
                        steps.push(ScriptStep {
                            op: Some(ChainOp::OrIf),
                            kind: self.parse_step_kind()?,
                        });
                    }
                    Some(Token::Pipe) => {
                        return Err(SandboxError::InvalidRequest(
                            "unexpected pipe in script".to_string(),
                        ));
                    }
                    None => return Ok(steps),
                    _ => break,
                }
            }
        }

        Ok(steps)
    }

    fn parse_step_kind(&mut self) -> Result<StepKind, SandboxError> {
        if self.consume_keyword("if") {
            return Ok(StepKind::If(self.parse_if_block()?));
        }
        Ok(StepKind::Pipeline(self.parse_pipeline()?))
    }

    fn parse_if_block(&mut self) -> Result<IfBlock, SandboxError> {
        let condition = self.parse_pipeline()?;
        self.expect_clause_separator("then")?;
        self.expect_keyword("then")?;
        let then_steps = self.parse_steps_until(&["else", "fi"])?;
        if then_steps.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "if blocks require at least one command in the then branch".to_string(),
            ));
        }

        let else_steps = if self.consume_keyword("else") {
            self.parse_steps_until(&["fi"])?
        } else {
            Vec::new()
        };
        self.expect_keyword("fi")?;

        Ok(IfBlock {
            condition,
            then_steps,
            else_steps,
        })
    }

    fn parse_pipeline(&mut self) -> Result<Pipeline, SandboxError> {
        let mut commands = vec![self.parse_command()?];
        while matches!(self.peek(), Some(Token::Pipe)) {
            self.index += 1;
            commands.push(self.parse_command()?);
        }
        Ok(Pipeline { commands })
    }

    fn parse_command(&mut self) -> Result<SimpleCommand, SandboxError> {
        let mut assignments = Vec::new();
        let mut argv = Vec::new();
        let mut redirects = Vec::new();

        loop {
            match self.peek() {
                Some(Token::Word(_)) => {
                    let Token::Word(word) = self.next().expect("word token") else {
                        unreachable!();
                    };
                    if argv.is_empty() {
                        if let Some((name, value)) = word.split_assignment() {
                            assignments.push((name, value));
                            continue;
                        }
                    }
                    argv.push(word);
                }
                Some(Token::RedirectIn) => {
                    self.index += 1;
                    redirects.push(RedirectSpec::Input(self.expect_word("input redirection")?));
                }
                Some(Token::RedirectOut) => {
                    self.index += 1;
                    redirects.push(RedirectSpec::StdoutTruncate(
                        self.expect_word("output redirection")?,
                    ));
                }
                Some(Token::RedirectAppend) => {
                    self.index += 1;
                    redirects.push(RedirectSpec::StdoutAppend(
                        self.expect_word("append redirection")?,
                    ));
                }
                Some(Token::RedirectErrOut) => {
                    self.index += 1;
                    redirects.push(RedirectSpec::StderrTruncate(
                        self.expect_word("stderr redirection")?,
                    ));
                }
                Some(Token::RedirectErrAppend) => {
                    self.index += 1;
                    redirects.push(RedirectSpec::StderrAppend(
                        self.expect_word("stderr append redirection")?,
                    ));
                }
                Some(Token::RedirectErrToStdout) => {
                    self.index += 1;
                    redirects.push(RedirectSpec::StderrToStdout);
                }
                _ => break,
            }
        }

        if argv.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "script command must include at least one command word".to_string(),
            ));
        }

        Ok(SimpleCommand {
            assignments,
            argv,
            redirects,
        })
    }

    fn expect_word(&mut self, context: &str) -> Result<ScriptWord, SandboxError> {
        let Some(Token::Word(word)) = self.next() else {
            return Err(SandboxError::InvalidRequest(format!(
                "{context} requires a target path"
            )));
        };
        Ok(word)
    }

    fn skip_semicolons(&mut self) -> bool {
        while matches!(self.peek(), Some(Token::Semicolon)) {
            self.index += 1;
        }
        self.peek().is_some()
    }

    fn expect_clause_separator(&mut self, keyword: &str) -> Result<(), SandboxError> {
        if !matches!(self.peek(), Some(Token::Semicolon)) {
            return Err(SandboxError::InvalidRequest(format!(
                "expected command separator before {keyword}"
            )));
        }
        while matches!(self.peek(), Some(Token::Semicolon)) {
            self.index += 1;
        }
        Ok(())
    }

    fn peek_keyword(&self, keywords: &[&str]) -> bool {
        let Some(Token::Word(word)) = self.peek() else {
            return false;
        };
        keywords.iter().any(|keyword| word.is_keyword(keyword))
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        if !self.peek_keyword(&[keyword]) {
            return false;
        }
        self.index += 1;
        true
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), SandboxError> {
        if self.consume_keyword(keyword) {
            return Ok(());
        }
        Err(SandboxError::InvalidRequest(format!(
            "expected {keyword} in script"
        )))
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.index).cloned();
        if token.is_some() {
            self.index += 1;
        }
        token
    }
}

fn is_valid_assignment_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn expand_part(
    part: &str,
    env: &BTreeMap<String, String>,
    positional_args: &[String],
) -> Result<String, SandboxError> {
    let mut output = String::new();
    let chars = part.chars().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        if chars[index] != '$' {
            output.push(chars[index]);
            index += 1;
            continue;
        }

        if index + 1 >= chars.len() {
            output.push('$');
            index += 1;
            continue;
        }

        if chars[index + 1] == '{' {
            let mut expression = String::new();
            index += 2;
            while index < chars.len() && chars[index] != '}' {
                expression.push(chars[index]);
                index += 1;
            }
            if index >= chars.len() || chars[index] != '}' {
                return Err(SandboxError::InvalidRequest(
                    "unterminated ${...} variable expansion in script".to_string(),
                ));
            }

            let (name, default_value) = match expression.split_once(":-") {
                Some((name, default)) => (name, Some(default)),
                None => (expression.as_str(), None),
            };
            if !is_valid_assignment_name(name) {
                return Err(SandboxError::InvalidRequest(format!(
                    "invalid variable name in expansion: {name}"
                )));
            }

            match env.get(name) {
                Some(value) if !value.is_empty() => output.push_str(value),
                _ => {
                    if let Some(default) = default_value {
                        output.push_str(&expand_part(default, env, positional_args)?);
                    }
                }
            }
            index += 1;
            continue;
        }

        if chars[index + 1] == '#' {
            output.push_str(&positional_args.len().to_string());
            index += 2;
            continue;
        }

        if chars[index + 1] == '@' {
            output.push_str(&positional_args.join(" "));
            index += 2;
            continue;
        }

        if chars[index + 1].is_ascii_digit() {
            let mut cursor = index + 1;
            let mut digits = String::new();
            while cursor < chars.len() && chars[cursor].is_ascii_digit() {
                digits.push(chars[cursor]);
                cursor += 1;
            }
            let position = digits.parse::<usize>().map_err(|_| {
                SandboxError::InvalidRequest(
                    "invalid positional parameter expansion in script".to_string(),
                )
            })?;
            if position > 0 {
                output.push_str(
                    positional_args
                        .get(position - 1)
                        .map(String::as_str)
                        .unwrap_or(""),
                );
            }
            index = cursor;
            continue;
        }

        let mut name = String::new();
        let mut cursor = index + 1;
        while cursor < chars.len()
            && (chars[cursor] == '_' || chars[cursor].is_ascii_alphanumeric())
        {
            name.push(chars[cursor]);
            cursor += 1;
        }
        if name.is_empty() || !is_valid_assignment_name(&name) {
            output.push('$');
            index += 1;
            continue;
        }
        output.push_str(env.get(&name).map(String::as_str).unwrap_or(""));
        index = cursor;
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pipelines_redirections_and_chaining() {
        let parsed =
            parse_script("cat < in.txt | cat > out.txt && echo ok; false || echo done").unwrap();
        assert_eq!(parsed.len(), 4);
        let StepKind::Pipeline(pipeline) = &parsed[0].kind else {
            panic!("expected pipeline");
        };
        assert_eq!(pipeline.commands.len(), 2);
        assert_eq!(
            match &pipeline.commands[0].redirects[0] {
                RedirectSpec::Input(value) => value.literal(),
                other => panic!("expected input redirect, got {other:?}"),
            },
            "in.txt"
        );
        assert_eq!(parsed[1].op, Some(ChainOp::AndIf));
        assert_eq!(parsed[2].op, Some(ChainOp::Seq));
        assert_eq!(parsed[3].op, Some(ChainOp::OrIf));
    }

    #[test]
    fn parses_assignment_prefixes() {
        let parsed = parse_script("FOO=hello BAR=\"$FOO world\" printenv FOO").unwrap();
        let StepKind::Pipeline(pipeline) = &parsed[0].kind else {
            panic!("expected pipeline");
        };
        let command = &pipeline.commands[0];
        assert_eq!(command.assignments.len(), 2);
        assert_eq!(command.assignments[0].0, "FOO");
        assert_eq!(command.assignments[0].1.literal(), "hello");
        assert_eq!(command.assignments[1].0, "BAR");
        assert_eq!(command.argv[0].literal(), "printenv");
    }

    #[test]
    fn parses_stderr_redirect_tokens_in_order() {
        let parsed = parse_script("echo hi 2>err.log >out.log 2>&1").unwrap();
        let StepKind::Pipeline(pipeline) = &parsed[0].kind else {
            panic!("expected pipeline");
        };
        let redirects = &pipeline.commands[0].redirects;
        assert!(matches!(redirects[0], RedirectSpec::StderrTruncate(_)));
        assert!(matches!(redirects[1], RedirectSpec::StdoutTruncate(_)));
        assert!(matches!(redirects[2], RedirectSpec::StderrToStdout));
    }

    #[test]
    fn newlines_and_comments_split_commands() {
        let parsed = parse_script("echo one\n# note\necho two").unwrap();
        assert_eq!(parsed.len(), 2);
        let StepKind::Pipeline(first) = &parsed[0].kind else {
            panic!("expected pipeline");
        };
        let StepKind::Pipeline(second) = &parsed[1].kind else {
            panic!("expected pipeline");
        };
        assert_eq!(first.commands[0].argv[0].literal(), "echo");
        assert_eq!(first.commands[0].argv[1].literal(), "one");
        assert_eq!(second.commands[0].argv[0].literal(), "echo");
        assert_eq!(second.commands[0].argv[1].literal(), "two");
    }

    #[test]
    fn expansion_respects_single_quotes() {
        let word = parse_script("echo '$NAME' \"$NAME\" $NAME").unwrap()[0]
            .kind
            .pipeline()
            .commands[0]
            .argv
            .clone()
            .into_iter()
            .skip(1)
            .collect::<Vec<_>>();
        let env = BTreeMap::from([(String::from("NAME"), String::from("demo"))]);
        let values = word
            .iter()
            .map(|part| part.expand(&env, &[]).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values, vec!["$NAME", "demo", "demo"]);
    }

    #[test]
    fn expansion_supports_default_values() {
        let env = BTreeMap::from([
            (String::from("SET"), String::from("value")),
            (String::from("EMPTY"), String::new()),
            (String::from("FALLBACK"), String::from("fallback")),
        ]);

        assert_eq!(
            expand_part("${SET:-default}", &env, &[]).unwrap(),
            "value".to_string()
        );
        assert_eq!(
            expand_part("${EMPTY:-default}", &env, &[]).unwrap(),
            "default".to_string()
        );
        assert_eq!(
            expand_part("${MISSING:-$FALLBACK}", &env, &[]).unwrap(),
            "fallback".to_string()
        );
    }

    #[test]
    fn expansion_supports_positional_parameters() {
        let positional = vec!["first".to_string(), "second".to_string()];
        let env = BTreeMap::new();

        assert_eq!(expand_part("$1", &env, &positional).unwrap(), "first");
        assert_eq!(expand_part("$2", &env, &positional).unwrap(), "second");
        assert_eq!(expand_part("$3", &env, &positional).unwrap(), "");
        assert_eq!(expand_part("$#", &env, &positional).unwrap(), "2");
        assert_eq!(
            expand_part("$@", &env, &positional).unwrap(),
            "first second"
        );
    }

    #[test]
    fn rejects_unterminated_quotes() {
        let error = parse_script("echo \"oops").unwrap_err();
        assert_eq!(error.kind(), abash_core::ErrorKind::InvalidRequest);
    }

    #[test]
    fn parses_if_then_else_blocks() {
        let parsed = parse_script("if true; then echo yes; else echo no; fi").unwrap();
        let StepKind::If(block) = &parsed[0].kind else {
            panic!("expected if block");
        };
        assert_eq!(block.condition.commands.len(), 1);
        assert_eq!(block.then_steps.len(), 1);
        assert_eq!(block.else_steps.len(), 1);
    }

    #[test]
    fn parses_nested_if_blocks() {
        let parsed =
            parse_script("if true; then if false; then echo no; else echo yes; fi; fi").unwrap();
        let StepKind::If(block) = &parsed[0].kind else {
            panic!("expected if block");
        };
        let StepKind::If(_) = &block.then_steps[0].kind else {
            panic!("expected nested if block");
        };
    }
}

impl StepKind {
    #[cfg(test)]
    fn pipeline(&self) -> &Pipeline {
        match self {
            Self::Pipeline(pipeline) => pipeline,
            Self::If(_) => panic!("expected pipeline"),
        }
    }
}
