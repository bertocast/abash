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
    Case(CaseBlock),
    Subshell(SubshellBlock),
    While(WhileBlock),
    Until(WhileBlock),
    For(ForBlock),
    FunctionDef(FunctionDef),
    Return(ReturnStep),
    Break(ControlStep),
    Continue(ControlStep),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IfBlock {
    pub condition: Pipeline,
    pub then_steps: Vec<ScriptStep>,
    pub else_steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CaseBlock {
    pub subject: ScriptWord,
    pub arms: Vec<CaseArm>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CaseArm {
    pub patterns: Vec<ScriptWord>,
    pub steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubshellBlock {
    pub body_steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WhileBlock {
    pub condition: Pipeline,
    pub body_steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ForBlock {
    pub name: String,
    pub items: Vec<ScriptWord>,
    pub body_steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FunctionDef {
    pub name: String,
    pub body_steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReturnStep {
    pub status: Option<ScriptWord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControlStep {
    pub levels: Option<ScriptWord>,
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
    DoubleSemicolon,
    LParen,
    RParen,
    LBrace,
    RBrace,
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
        if builder.has_open_command_substitution() {
            builder.push_char(ch, true, true);
            continue;
        }

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
                if chars.next_if_eq(&';').is_some() {
                    tokens.push(Token::DoubleSemicolon);
                } else {
                    tokens.push(Token::Semicolon);
                }
            }
            '(' if builder.is_empty() => {
                builder.flush(&mut tokens);
                tokens.push(Token::LParen);
            }
            ')' => {
                if builder.has_open_command_substitution() {
                    builder.push_char(ch, true, true);
                } else {
                    builder.flush(&mut tokens);
                    tokens.push(Token::RParen);
                }
            }
            '{' if builder.is_empty() => {
                builder.flush(&mut tokens);
                tokens.push(Token::LBrace);
            }
            '}' if builder.is_empty() => {
                builder.flush(&mut tokens);
                tokens.push(Token::RBrace);
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

    fn has_open_command_substitution(&self) -> bool {
        let text = self
            .parts
            .iter()
            .map(|part| part.text.as_str())
            .collect::<String>();
        let chars = text.chars().collect::<Vec<_>>();
        let mut index = 0usize;
        let mut depth = 0usize;
        while index < chars.len() {
            if chars[index] == '$' && chars.get(index + 1) == Some(&'(') {
                depth += 1;
                index += 2;
                continue;
            }
            if chars[index] == ')' && depth > 0 {
                depth -= 1;
            }
            index += 1;
        }
        depth > 0
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
        if self.peek_function_definition() {
            return Ok(StepKind::FunctionDef(self.parse_function_def()?));
        }
        if self.consume_keyword("if") {
            return Ok(StepKind::If(self.parse_if_block()?));
        }
        if self.consume_keyword("case") {
            return Ok(StepKind::Case(self.parse_case_block()?));
        }
        if matches!(self.peek(), Some(Token::LParen)) {
            self.index += 1;
            return Ok(StepKind::Subshell(self.parse_subshell_block()?));
        }
        if self.consume_keyword("while") {
            return Ok(StepKind::While(self.parse_while_block()?));
        }
        if self.consume_keyword("until") {
            return Ok(StepKind::Until(self.parse_while_block()?));
        }
        if self.consume_keyword("for") {
            return Ok(StepKind::For(self.parse_for_block()?));
        }
        if self.consume_keyword("return") {
            return Ok(StepKind::Return(self.parse_return_step()?));
        }
        if self.consume_keyword("break") {
            return Ok(StepKind::Break(self.parse_control_step("break")?));
        }
        if self.consume_keyword("continue") {
            return Ok(StepKind::Continue(self.parse_control_step("continue")?));
        }
        Ok(StepKind::Pipeline(self.parse_pipeline()?))
    }

    fn parse_if_block(&mut self) -> Result<IfBlock, SandboxError> {
        let condition = self.parse_pipeline()?;
        self.expect_clause_separator("then")?;
        self.expect_keyword("then")?;
        let then_steps = self.parse_steps_until(&["elif", "else", "fi"])?;
        if then_steps.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "if blocks require at least one command in the then branch".to_string(),
            ));
        }

        let else_steps = if self.consume_keyword("elif") {
            vec![ScriptStep {
                op: None,
                kind: StepKind::If(self.parse_if_block()?),
            }]
        } else if self.consume_keyword("else") {
            let steps = self.parse_steps_until(&["fi"])?;
            self.expect_keyword("fi")?;
            steps
        } else {
            self.expect_keyword("fi")?;
            Vec::new()
        };

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

    fn parse_case_block(&mut self) -> Result<CaseBlock, SandboxError> {
        let subject = self.expect_word("case subject")?;
        self.expect_keyword("in")?;
        let mut arms = Vec::new();

        loop {
            self.skip_semicolons();
            if self.consume_keyword("esac") {
                break;
            }
            if matches!(self.peek(), Some(Token::LParen)) {
                self.index += 1;
            }

            let (first_pattern, mut closed) =
                split_case_pattern_word(self.expect_word("case pattern")?);
            let mut patterns = vec![first_pattern];
            while !closed && matches!(self.peek(), Some(Token::Pipe)) {
                self.index += 1;
                let (pattern, pattern_closed) =
                    split_case_pattern_word(self.expect_word("case pattern")?);
                patterns.push(pattern);
                closed = pattern_closed;
            }

            if !closed && !matches!(self.next(), Some(Token::RParen)) {
                return Err(SandboxError::InvalidRequest(
                    "expected ) after case pattern".to_string(),
                ));
            }

            let (steps, terminated) = self.parse_case_arm_steps()?;
            arms.push(CaseArm { patterns, steps });
            if !terminated {
                self.expect_keyword("esac")?;
                break;
            }
        }

        if arms.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "case blocks require at least one pattern arm".to_string(),
            ));
        }

        Ok(CaseBlock { subject, arms })
    }

    fn parse_while_block(&mut self) -> Result<WhileBlock, SandboxError> {
        let condition = self.parse_pipeline()?;
        self.expect_clause_separator("do")?;
        self.expect_keyword("do")?;
        let body_steps = self.parse_steps_until(&["done"])?;
        if body_steps.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "while blocks require at least one command in the body".to_string(),
            ));
        }
        self.expect_keyword("done")?;

        Ok(WhileBlock {
            condition,
            body_steps,
        })
    }

    fn parse_subshell_block(&mut self) -> Result<SubshellBlock, SandboxError> {
        let body_steps = self.parse_steps_until_paren()?;
        if body_steps.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "subshells require at least one command".to_string(),
            ));
        }
        self.expect_rparen()?;
        Ok(SubshellBlock { body_steps })
    }

    fn parse_for_block(&mut self) -> Result<ForBlock, SandboxError> {
        let Some(Token::Word(name_word)) = self.next() else {
            return Err(SandboxError::InvalidRequest(
                "for loops require a loop variable name".to_string(),
            ));
        };
        let name = name_word.literal_value();
        if !is_valid_assignment_name(&name) {
            return Err(SandboxError::InvalidRequest(format!(
                "invalid for-loop variable name: {name}"
            )));
        }

        let mut items = Vec::new();
        if self.consume_keyword("in") {
            loop {
                match self.peek() {
                    Some(Token::Semicolon) => break,
                    Some(Token::Word(_)) => items.push(self.expect_word("for loop item")?),
                    Some(_) => {
                        return Err(SandboxError::InvalidRequest(
                            "invalid token in for loop item list".to_string(),
                        ));
                    }
                    None => {
                        return Err(SandboxError::InvalidRequest(
                            "unterminated for loop".to_string(),
                        ));
                    }
                }
            }
        }

        self.expect_clause_separator("do")?;
        self.expect_keyword("do")?;
        let body_steps = self.parse_steps_until(&["done"])?;
        if body_steps.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "for loops require at least one command in the body".to_string(),
            ));
        }
        self.expect_keyword("done")?;

        Ok(ForBlock {
            name,
            items,
            body_steps,
        })
    }

    fn parse_function_def(&mut self) -> Result<FunctionDef, SandboxError> {
        let Some(Token::Word(name_word)) = self.next() else {
            unreachable!();
        };
        let raw_name = name_word.literal_value();
        let name = if let Some(stripped) = raw_name.strip_suffix("()") {
            stripped.to_string()
        } else if let Some(stripped) = raw_name.strip_suffix('(') {
            self.expect_rparen()?;
            stripped.to_string()
        } else {
            return Err(SandboxError::InvalidRequest(
                "invalid function declaration".to_string(),
            ));
        };
        if !is_valid_assignment_name(&name) {
            return Err(SandboxError::InvalidRequest(format!(
                "invalid function name: {name}"
            )));
        }
        self.expect_lbrace()?;
        let body_steps = self.parse_steps_until_brace()?;
        if body_steps.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "function bodies require at least one command".to_string(),
            ));
        }
        self.expect_rbrace()?;
        Ok(FunctionDef { name, body_steps })
    }

    fn parse_return_step(&mut self) -> Result<ReturnStep, SandboxError> {
        Ok(ReturnStep {
            status: self.parse_optional_control_word("return", "status")?,
        })
    }

    fn parse_control_step(&mut self, keyword: &str) -> Result<ControlStep, SandboxError> {
        Ok(ControlStep {
            levels: self.parse_optional_control_word(keyword, "count")?,
        })
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

        if argv.is_empty() && assignments.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "script command must include at least one command word".to_string(),
            ));
        }

        if argv.is_empty() && !redirects.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "assignment-only script commands do not support redirects".to_string(),
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

    fn parse_steps_until_brace(&mut self) -> Result<Vec<ScriptStep>, SandboxError> {
        let mut steps = Vec::new();

        while self.skip_semicolons() {
            if matches!(self.peek(), Some(Token::RBrace)) {
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
                if matches!(self.peek(), Some(Token::RBrace)) {
                    return Ok(steps);
                }
                match self.peek() {
                    Some(Token::Semicolon) => {
                        self.index += 1;
                        while matches!(self.peek(), Some(Token::Semicolon)) {
                            self.index += 1;
                        }
                        if matches!(self.peek(), Some(Token::RBrace)) {
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
                            "unexpected pipe in function body".to_string(),
                        ));
                    }
                    Some(Token::RBrace) | None => return Ok(steps),
                    _ => break,
                }
            }
        }

        Ok(steps)
    }

    fn parse_steps_until_paren(&mut self) -> Result<Vec<ScriptStep>, SandboxError> {
        let mut steps = Vec::new();

        while self.skip_semicolons() {
            if matches!(self.peek(), Some(Token::RParen)) {
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
                if matches!(self.peek(), Some(Token::RParen)) {
                    return Ok(steps);
                }
                match self.peek() {
                    Some(Token::Semicolon) => {
                        self.index += 1;
                        while matches!(self.peek(), Some(Token::Semicolon)) {
                            self.index += 1;
                        }
                        if matches!(self.peek(), Some(Token::RParen)) {
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
                            "unexpected pipe in subshell body".to_string(),
                        ));
                    }
                    Some(Token::RParen) | None => return Ok(steps),
                    _ => break,
                }
            }
        }

        Ok(steps)
    }

    fn parse_case_arm_steps(&mut self) -> Result<(Vec<ScriptStep>, bool), SandboxError> {
        let mut steps = Vec::new();

        while self.skip_semicolons() {
            if matches!(self.peek(), Some(Token::DoubleSemicolon)) {
                self.index += 1;
                return Ok((steps, true));
            }
            if self.peek_keyword(&["esac"]) {
                return Ok((steps, false));
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
                if matches!(self.peek(), Some(Token::DoubleSemicolon)) {
                    self.index += 1;
                    return Ok((steps, true));
                }
                if self.peek_keyword(&["esac"]) {
                    return Ok((steps, false));
                }
                match self.peek() {
                    Some(Token::Semicolon) => {
                        self.index += 1;
                        while matches!(self.peek(), Some(Token::Semicolon)) {
                            self.index += 1;
                        }
                        if matches!(self.peek(), Some(Token::DoubleSemicolon)) {
                            self.index += 1;
                            return Ok((steps, true));
                        }
                        if self.peek_keyword(&["esac"]) {
                            return Ok((steps, false));
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
                            "unexpected pipe in case arm".to_string(),
                        ));
                    }
                    Some(Token::DoubleSemicolon) => {
                        self.index += 1;
                        return Ok((steps, true));
                    }
                    Some(Token::RBrace) | None => return Ok((steps, false)),
                    _ => break,
                }
            }
        }

        Ok((steps, false))
    }

    fn parse_optional_control_word(
        &mut self,
        keyword: &str,
        noun: &str,
    ) -> Result<Option<ScriptWord>, SandboxError> {
        let value = if matches!(self.peek(), Some(Token::Word(_))) {
            Some(self.expect_word(keyword)?)
        } else {
            None
        };

        if matches!(self.peek(), Some(Token::Word(_))) {
            return Err(SandboxError::InvalidRequest(format!(
                "{keyword} accepts at most one {noun}"
            )));
        }

        Ok(value)
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

    fn expect_lbrace(&mut self) -> Result<(), SandboxError> {
        if matches!(self.next(), Some(Token::LBrace)) {
            return Ok(());
        }
        Err(SandboxError::InvalidRequest(
            "expected { in function definition".to_string(),
        ))
    }

    fn expect_rbrace(&mut self) -> Result<(), SandboxError> {
        if matches!(self.next(), Some(Token::RBrace)) {
            return Ok(());
        }
        Err(SandboxError::InvalidRequest(
            "expected } at the end of a function body".to_string(),
        ))
    }

    fn expect_rparen(&mut self) -> Result<(), SandboxError> {
        if matches!(self.next(), Some(Token::RParen)) {
            return Ok(());
        }
        Err(SandboxError::InvalidRequest(
            "expected ) at the end of a subshell body".to_string(),
        ))
    }

    fn peek_function_definition(&self) -> bool {
        match (
            self.peek(),
            self.tokens.get(self.index + 1),
            self.tokens.get(self.index + 2),
        ) {
            (Some(Token::Word(word)), Some(Token::LBrace), _)
                if word.literal_value().ends_with("()") =>
            {
                true
            }
            (Some(Token::Word(word)), Some(Token::RParen), Some(Token::LBrace))
                if word.literal_value().ends_with('(') =>
            {
                true
            }
            _ => false,
        }
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

pub(crate) fn is_valid_assignment_name(value: &str) -> bool {
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
            if let Ok(position) = name.parse::<usize>() {
                if position > 0 {
                    match positional_args
                        .get(position - 1)
                        .filter(|value| !value.is_empty())
                    {
                        Some(value) => output.push_str(value),
                        None => {
                            if let Some(default) = default_value {
                                output.push_str(&expand_part(default, env, positional_args)?);
                            }
                        }
                    }
                }
                index += 1;
                continue;
            }

            if !is_valid_assignment_name(name) {
                return Err(SandboxError::InvalidRequest(format!(
                    "invalid variable name in expansion: {name}"
                )));
            }

            match env.get(name).filter(|value| !value.is_empty()) {
                Some(value) => output.push_str(value),
                None => {
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

fn split_case_pattern_word(mut word: ScriptWord) -> (ScriptWord, bool) {
    let Some(last_part) = word.parts.last_mut() else {
        return (word, false);
    };
    if !last_part.text.ends_with(')') {
        return (word, false);
    }

    last_part.text.pop();
    if last_part.text.is_empty() && word.parts.len() > 1 {
        word.parts.pop();
    }
    (word, true)
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
        let positional = vec!["first".to_string()];
        assert_eq!(
            expand_part("${1:-default}", &env, &positional).unwrap(),
            "first".to_string()
        );
        assert_eq!(
            expand_part("${2:-default}", &env, &positional).unwrap(),
            "default".to_string()
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

    #[test]
    fn parses_if_elif_else_blocks() {
        let parsed =
            parse_script("if false; then echo no; elif true; then echo yes; else echo later; fi")
                .unwrap();
        let StepKind::If(block) = &parsed[0].kind else {
            panic!("expected if block");
        };
        let StepKind::If(elif_block) = &block.else_steps[0].kind else {
            panic!("expected elif block");
        };
        assert_eq!(elif_block.then_steps.len(), 1);
        assert_eq!(elif_block.else_steps.len(), 1);
    }

    #[test]
    fn parses_while_do_done_blocks() {
        let parsed = parse_script("while true; do echo tick; done").unwrap();
        let StepKind::While(block) = &parsed[0].kind else {
            panic!("expected while block");
        };
        assert_eq!(block.condition.commands.len(), 1);
        assert_eq!(block.body_steps.len(), 1);
    }

    #[test]
    fn parses_until_for_and_function_blocks() {
        let parsed = parse_script(
            "until true; do echo wait; done; for item in a b; do echo $item; done; greet() { echo hi; }",
        )
        .unwrap();
        let StepKind::Until(until_block) = &parsed[0].kind else {
            panic!("expected until block");
        };
        assert_eq!(until_block.body_steps.len(), 1);
        let StepKind::For(for_block) = &parsed[1].kind else {
            panic!("expected for block");
        };
        assert_eq!(for_block.name, "item");
        assert_eq!(for_block.items.len(), 2);
        let StepKind::FunctionDef(function) = &parsed[2].kind else {
            panic!("expected function definition");
        };
        assert_eq!(function.name, "greet");
        assert_eq!(function.body_steps.len(), 1);
    }

    #[test]
    fn parses_subshell_blocks() {
        let parsed = parse_script("(echo hi; echo there) && echo done").unwrap();
        let StepKind::Subshell(block) = &parsed[0].kind else {
            panic!("expected subshell block");
        };
        assert_eq!(block.body_steps.len(), 2);
    }

    #[test]
    fn parses_case_and_control_flow_keywords() {
        let parsed = parse_script(
            "case $name in bert) echo yes ;; a*) echo no ;; *) echo later ;; esac; return 7; break 2; continue",
        )
        .unwrap();

        let StepKind::Case(case_block) = &parsed[0].kind else {
            panic!("expected case block");
        };
        assert_eq!(case_block.arms.len(), 3);
        assert_eq!(case_block.arms[0].patterns[0].literal(), "bert");
        assert_eq!(case_block.arms[1].patterns[0].literal(), "a*");
        assert_eq!(case_block.arms[2].patterns[0].literal(), "*");
        assert_eq!(case_block.arms[0].steps.len(), 1);
        assert_eq!(case_block.arms[1].steps.len(), 1);
        assert_eq!(case_block.arms[2].steps.len(), 1);

        let StepKind::Return(return_step) = &parsed[1].kind else {
            panic!("expected return step");
        };
        assert_eq!(
            return_step
                .status
                .as_ref()
                .expect("return status")
                .literal(),
            "7"
        );

        let StepKind::Break(control_step) = &parsed[2].kind else {
            panic!("expected break step");
        };
        assert_eq!(
            control_step
                .levels
                .as_ref()
                .expect("break levels")
                .literal(),
            "2"
        );

        let StepKind::Continue(control_step) = &parsed[3].kind else {
            panic!("expected continue step");
        };
        assert!(control_step.levels.is_none());
    }

    #[test]
    fn parses_case_after_assignment_prefix() {
        let parsed = parse_script(
            "name=bert; case $name in bert) echo exact ;; a*) echo prefix ;; *) echo none ;; esac",
        )
        .unwrap();

        assert_eq!(parsed.len(), 2);
        let StepKind::Case(case_block) = &parsed[1].kind else {
            panic!("expected case block");
        };
        assert_eq!(case_block.arms.len(), 3);
    }
}

impl StepKind {
    #[cfg(test)]
    fn pipeline(&self) -> &Pipeline {
        match self {
            Self::Pipeline(pipeline) => pipeline,
            Self::If(_) => panic!("expected pipeline"),
            Self::Case(_) => panic!("expected pipeline"),
            Self::Subshell(_) => panic!("expected pipeline"),
            Self::While(_) => panic!("expected pipeline"),
            Self::Until(_) => panic!("expected pipeline"),
            Self::For(_) => panic!("expected pipeline"),
            Self::FunctionDef(_) => panic!("expected pipeline"),
            Self::Return(_) => panic!("expected pipeline"),
            Self::Break(_) => panic!("expected pipeline"),
            Self::Continue(_) => panic!("expected pipeline"),
        }
    }
}
