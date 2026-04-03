use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

mod awk;
mod base64cmd;
mod chmodcmd;
mod column;
mod comm;
mod cp;
mod curlcmd;
mod date;
mod diffcmd;
mod ducmd;
mod envcmd;
mod exprcmd;
mod find;
mod grepcmd;
mod gzipcmd;
mod hashcmd;
mod hostexec;
mod htmltomarkdown;
mod jq;
mod jq_engine;
mod jsexeccmd;
mod ls;
mod mv;
mod odcmd;
mod pathcmd;
mod printf;
mod python3cmd;
mod rg;
mod rm;
mod rmdir;
mod script;
mod seq;
mod splitcmd;
mod sqlite3cmd;
mod tarcmd;
mod tee;
mod tier2_files;
mod tier2_text;
mod tier3_exec;
mod tier3_shell;
mod which;
mod xancmd;
mod xargs;
mod yq;

use abash_core::{
    create_filesystem, resolve_sandbox_path, ExecutionMode, ExecutionRequest, ExecutionResult,
    ExtensionCommandResult, FilesystemMode, LazyPathEntry, SandboxConfig, SandboxError,
    SandboxExtensions, SandboxFilesystem, SessionBackend, SessionState, TerminationReason,
};
use script::{
    is_valid_assignment_name, parse_script, ChainOp, ForBlock, FunctionDef, IfBlock, Pipeline,
    RedirectSpec, ScriptStep, ScriptWord, SimpleCommand, StepKind, WhileBlock,
};
use ureq::{http, Agent, RequestExt};
use url::Url;

pub fn create_session(config: SandboxConfig) -> Result<Box<dyn SessionBackend>, SandboxError> {
    Ok(Box::new(VirtualSession {
        filesystem: create_filesystem(&config)?,
        default_cwd: config.default_cwd.clone(),
        session_state: config.session_state.clone(),
        current_cwd: config.default_cwd.clone(),
        exported_env: BTreeMap::new(),
        aliases: BTreeMap::new(),
        history: Vec::new(),
        active_extensions: None,
    }))
}

struct VirtualSession {
    filesystem: Box<dyn SandboxFilesystem>,
    default_cwd: String,
    session_state: SessionState,
    current_cwd: String,
    exported_env: BTreeMap<String, String>,
    aliases: BTreeMap<String, Vec<String>>,
    history: Vec<String>,
    active_extensions: Option<Arc<dyn SandboxExtensions>>,
}

#[derive(Default)]
struct LazyPathSnapshot {
    files: BTreeSet<String>,
    dirs: BTreeSet<String>,
}

impl LazyPathSnapshot {
    fn from_entries(entries: Vec<LazyPathEntry>) -> Result<Self, SandboxError> {
        let mut snapshot = Self::default();
        for entry in entries {
            let path = abash_core::normalize_sandbox_path(&entry.path)?;
            if entry.is_dir {
                snapshot.dirs.insert(path.clone());
            } else {
                snapshot.files.insert(path.clone());
            }
            for parent in parent_directory_chain(&path) {
                snapshot.dirs.insert(parent);
            }
        }
        Ok(snapshot)
    }

    fn exists(&self, path: &str) -> bool {
        self.files.contains(path) || self.dirs.contains(path)
    }

    fn is_dir(&self, path: &str) -> bool {
        self.dirs.contains(path)
    }

    fn list_paths(&self) -> Vec<String> {
        self.files
            .iter()
            .chain(self.dirs.iter())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

impl SessionBackend for VirtualSession {
    fn name(&self) -> &'static str {
        "virtual"
    }

    fn run(
        &mut self,
        request: ExecutionRequest,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<Arc<dyn SandboxExtensions>>,
    ) -> Result<ExecutionResult, SandboxError> {
        self.active_extensions = extensions.clone();
        if request.network_enabled && config.network_policy.is_none() {
            return Err(SandboxError::PolicyDenied(
                "network access is disabled unless explicit policy is configured".to_string(),
            ));
        }

        if request.filesystem_mode != config.filesystem_mode {
            return Err(SandboxError::InvalidRequest(
                "execution filesystem mode must match the sandbox filesystem mode".to_string(),
            ));
        }

        let persisted_cwd = match self.session_state {
            SessionState::Persistent => self.current_cwd.clone(),
            SessionState::PerExec => self.default_cwd.clone(),
        };
        let exported_env = match self.session_state {
            SessionState::Persistent => self.exported_env.clone(),
            SessionState::PerExec => BTreeMap::new(),
        };
        let aliases = match self.session_state {
            SessionState::Persistent => self.aliases.clone(),
            SessionState::PerExec => BTreeMap::new(),
        };
        if matches!(self.session_state, SessionState::PerExec) {
            self.history.clear();
        }

        let requested_cwd = if request.cwd.is_empty() {
            persisted_cwd.clone()
        } else {
            request.cwd.clone()
        };
        let cwd = resolve_sandbox_path(&config.default_cwd, &requested_cwd)?;
        let mut runtime = RuntimeState::new(
            cwd,
            persisted_cwd,
            exported_env,
            request.env.clone(),
            request.replace_env,
            aliases,
            request.argv.clone(),
        );

        if cancel_flag.load(Ordering::SeqCst) {
            return Err(SandboxError::Cancellation(
                "execution was cancelled before it started".to_string(),
            ));
        }

        self.push_history(render_history_entry(&request));

        let result = match request.mode {
            ExecutionMode::Argv => self.run_argv(
                &mut runtime,
                request,
                config,
                cancel_flag,
                extensions.as_deref(),
            ),
            ExecutionMode::Script => self.run_script(
                &mut runtime,
                request,
                config,
                cancel_flag,
                extensions.as_deref(),
            ),
        };

        if matches!(self.session_state, SessionState::Persistent) {
            self.current_cwd = runtime.persisted_cwd.clone();
            self.exported_env = runtime.exported_env.clone();
            self.aliases = runtime.aliases.clone();
        }

        self.active_extensions = None;
        result
    }

    fn read_file(&mut self, path: &str) -> Result<Vec<u8>, SandboxError> {
        self.read_path(path)
    }

    fn write_file(
        &mut self,
        path: &str,
        contents: Vec<u8>,
        create_parents: bool,
    ) -> Result<(), SandboxError> {
        self.filesystem.write_file(path, contents, create_parents)
    }

    fn mkdir(&mut self, path: &str, parents: bool) -> Result<(), SandboxError> {
        self.filesystem.mkdir(path, parents)
    }

    fn exists(&mut self, path: &str) -> Result<bool, SandboxError> {
        self.path_exists(path)
    }
}

impl VirtualSession {
    fn lazy_snapshot(&self) -> Result<LazyPathSnapshot, SandboxError> {
        let Some(extensions) = self.active_extensions.as_deref() else {
            return Ok(LazyPathSnapshot::default());
        };
        LazyPathSnapshot::from_entries(extensions.list_lazy_paths()?)
    }

    fn list_paths(&self) -> Result<Vec<String>, SandboxError> {
        let mut paths = self.filesystem.list_paths()?;
        paths.extend(self.lazy_snapshot()?.list_paths());
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    fn path_exists(&self, path: &str) -> Result<bool, SandboxError> {
        match self.filesystem.exists(path) {
            Ok(true) => Ok(true),
            Ok(false) | Err(SandboxError::InvalidRequest(_)) => {
                Ok(self.lazy_snapshot()?.exists(path))
            }
            Err(error) => Err(error),
        }
    }

    fn path_is_dir(&self, path: &str) -> Result<bool, SandboxError> {
        match self.filesystem.is_dir(path) {
            Ok(true) => Ok(true),
            Ok(false) | Err(SandboxError::InvalidRequest(_)) => {
                Ok(self.lazy_snapshot()?.is_dir(path))
            }
            Err(error) => Err(error),
        }
    }

    fn read_path(&self, path: &str) -> Result<Vec<u8>, SandboxError> {
        match self.filesystem.read_file(path) {
            Ok(contents) => Ok(contents),
            Err(SandboxError::InvalidRequest(_)) => {
                let Some(extensions) = self.active_extensions.as_deref() else {
                    return Err(SandboxError::InvalidRequest(format!(
                        "file does not exist: {path}"
                    )));
                };
                match extensions.read_lazy_file(path)? {
                    Some(contents) => Ok(contents),
                    None => Err(SandboxError::InvalidRequest(format!(
                        "file does not exist: {path}"
                    ))),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn run_argv(
        &mut self,
        runtime: &mut RuntimeState,
        request: ExecutionRequest,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
    ) -> Result<ExecutionResult, SandboxError> {
        let argv = resolve_alias_words(&request.argv, &runtime.aliases)?;
        let command = argv.first().cloned().ok_or_else(|| {
            SandboxError::InvalidRequest("argv mode requires a command".to_string())
        })?;
        let cwd = runtime.cwd.clone();
        let env = runtime.env.clone();
        self.execute_command(
            runtime,
            &cwd,
            argv.iter().skip(1).cloned().collect::<Vec<_>>(),
            request.stdin,
            command,
            config,
            cancel_flag,
            extensions,
            request.timeout_ms,
            request.network_enabled,
            env,
            request.metadata,
        )
    }

    fn run_script(
        &mut self,
        runtime: &mut RuntimeState,
        request: ExecutionRequest,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
    ) -> Result<ExecutionResult, SandboxError> {
        let source = request.script.clone().ok_or_else(|| {
            SandboxError::InvalidRequest("script mode requires a script string".to_string())
        })?;
        let steps = parse_script(&source)?;
        let started = Instant::now();
        let mut script_stdin = Some(request.stdin);
        let mut state = ScriptState::default();

        if let Err(mut failed) = self.run_steps(
            runtime,
            &steps,
            config,
            cancel_flag,
            request.timeout_ms,
            started,
            request.network_enabled,
            extensions,
            &request.metadata,
            &mut script_stdin,
            &mut state,
        ) {
            let last_command = failed.metadata.get("command").cloned();
            failed.metadata = decorate_script_metadata(
                failed.metadata,
                &runtime.cwd,
                state.executed_steps,
                last_command,
            );
            return Ok(failed);
        }

        if let Some(mut result) = state.last_result {
            let last_command = result.metadata.get("command").cloned();
            result.stdout = state.stdout;
            result.stderr = state.stderr;
            result.metadata = decorate_script_metadata(
                result.metadata,
                &runtime.cwd,
                state.executed_steps,
                last_command,
            );
            return Ok(result);
        }

        Ok(ExecutionResult::success(
            Vec::new(),
            decorate_script_metadata(self.base_metadata(&runtime.cwd), &runtime.cwd, 0, None),
        ))
    }

    fn run_steps(
        &mut self,
        runtime: &mut RuntimeState,
        steps: &[ScriptStep],
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        timeout_ms: Option<u64>,
        started: Instant,
        network_enabled: bool,
        extensions: Option<&dyn SandboxExtensions>,
        base_metadata: &BTreeMap<String, String>,
        script_stdin: &mut Option<Vec<u8>>,
        state: &mut ScriptState,
    ) -> Result<(), ExecutionResult> {
        for step in steps {
            if !should_run_step(step.op.as_ref(), state.last_result.as_ref()) {
                continue;
            }

            match &step.kind {
                StepKind::Pipeline(pipeline) => {
                    let pipeline_result = self
                        .run_pipeline(
                            runtime,
                            pipeline,
                            config,
                            cancel_flag,
                            timeout_ms,
                            started,
                            network_enabled,
                            extensions,
                            base_metadata,
                            script_stdin,
                        )
                        .map_err(|error| {
                            let mut failed =
                                ExecutionResult::failure(error, self.base_metadata(&runtime.cwd));
                            failed.stdout = state.stdout.clone();
                            failed.stderr = state.stderr.clone();
                            failed
                        })?;
                    state.executed_steps += 1;
                    state.stdout.extend(&pipeline_result.stdout);
                    state.stderr.extend(&pipeline_result.stderr);

                    if pipeline_result.error.is_some() {
                        let mut failed = pipeline_result;
                        failed.stdout = state.stdout.clone();
                        failed.stderr = state.stderr.clone();
                        return Err(failed);
                    }

                    state.last_result = Some(pipeline_result);
                }
                StepKind::If(block) => self.run_if_block(
                    runtime,
                    block,
                    config,
                    cancel_flag,
                    timeout_ms,
                    started,
                    network_enabled,
                    extensions,
                    base_metadata,
                    script_stdin,
                    state,
                )?,
                StepKind::While(block) => self.run_while_block(
                    runtime,
                    block,
                    config,
                    cancel_flag,
                    timeout_ms,
                    started,
                    network_enabled,
                    extensions,
                    base_metadata,
                    script_stdin,
                    state,
                )?,
                StepKind::Until(block) => self.run_until_block(
                    runtime,
                    block,
                    config,
                    cancel_flag,
                    timeout_ms,
                    started,
                    network_enabled,
                    extensions,
                    base_metadata,
                    script_stdin,
                    state,
                )?,
                StepKind::For(block) => self.run_for_block(
                    runtime,
                    block,
                    config,
                    cancel_flag,
                    timeout_ms,
                    started,
                    network_enabled,
                    extensions,
                    base_metadata,
                    script_stdin,
                    state,
                )?,
                StepKind::FunctionDef(definition) => {
                    self.register_function(runtime, definition, base_metadata, state)
                }
            }
        }

        Ok(())
    }

    fn run_if_block(
        &mut self,
        runtime: &mut RuntimeState,
        block: &IfBlock,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        timeout_ms: Option<u64>,
        started: Instant,
        network_enabled: bool,
        extensions: Option<&dyn SandboxExtensions>,
        base_metadata: &BTreeMap<String, String>,
        script_stdin: &mut Option<Vec<u8>>,
        state: &mut ScriptState,
    ) -> Result<(), ExecutionResult> {
        let condition = self
            .run_pipeline(
                runtime,
                &block.condition,
                config,
                cancel_flag,
                timeout_ms,
                started,
                network_enabled,
                extensions,
                base_metadata,
                script_stdin,
            )
            .map_err(|error| {
                let mut failed = ExecutionResult::failure(error, self.base_metadata(&runtime.cwd));
                failed.stdout = state.stdout.clone();
                failed.stderr = state.stderr.clone();
                failed
            })?;
        state.executed_steps += 1;
        state.stdout.extend(&condition.stdout);
        state.stderr.extend(&condition.stderr);

        if condition.error.is_some() {
            let mut failed = condition;
            failed.stdout = state.stdout.clone();
            failed.stderr = state.stderr.clone();
            return Err(failed);
        }

        let branch_steps = if condition.exit_code == 0 {
            &block.then_steps
        } else {
            &block.else_steps
        };

        if branch_steps.is_empty() {
            state.last_result = Some(self.if_no_match_result(&runtime.cwd, base_metadata));
            return Ok(());
        }

        let mut branch_state = ScriptState::default();
        if let Err(mut failed) = self.run_steps(
            runtime,
            branch_steps,
            config,
            cancel_flag,
            timeout_ms,
            started,
            network_enabled,
            extensions,
            base_metadata,
            script_stdin,
            &mut branch_state,
        ) {
            state.merge(branch_state);
            failed.stdout = state.stdout.clone();
            failed.stderr = state.stderr.clone();
            return Err(failed);
        }

        state.merge(branch_state);
        state.last_result = state
            .last_result
            .clone()
            .or_else(|| Some(self.if_no_match_result(&runtime.cwd, base_metadata)));
        Ok(())
    }

    fn run_while_block(
        &mut self,
        runtime: &mut RuntimeState,
        block: &WhileBlock,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        timeout_ms: Option<u64>,
        started: Instant,
        network_enabled: bool,
        extensions: Option<&dyn SandboxExtensions>,
        base_metadata: &BTreeMap<String, String>,
        script_stdin: &mut Option<Vec<u8>>,
        state: &mut ScriptState,
    ) -> Result<(), ExecutionResult> {
        let mut ran_body = false;

        loop {
            let condition = self
                .run_pipeline(
                    runtime,
                    &block.condition,
                    config,
                    cancel_flag,
                    timeout_ms,
                    started,
                    network_enabled,
                    extensions,
                    base_metadata,
                    script_stdin,
                )
                .map_err(|error| {
                    let mut failed =
                        ExecutionResult::failure(error, self.base_metadata(&runtime.cwd));
                    failed.stdout = state.stdout.clone();
                    failed.stderr = state.stderr.clone();
                    failed
                })?;
            state.executed_steps += 1;
            state.stdout.extend(&condition.stdout);
            state.stderr.extend(&condition.stderr);

            if condition.error.is_some() {
                let mut failed = condition;
                failed.stdout = state.stdout.clone();
                failed.stderr = state.stderr.clone();
                return Err(failed);
            }

            if condition.exit_code != 0 {
                if !ran_body {
                    state.last_result = Some(self.if_no_match_result(&runtime.cwd, base_metadata));
                }
                return Ok(());
            }

            ran_body = true;
            let mut body_state = ScriptState::default();
            if let Err(mut failed) = self.run_steps(
                runtime,
                &block.body_steps,
                config,
                cancel_flag,
                timeout_ms,
                started,
                network_enabled,
                extensions,
                base_metadata,
                script_stdin,
                &mut body_state,
            ) {
                state.merge(body_state);
                failed.stdout = state.stdout.clone();
                failed.stderr = state.stderr.clone();
                return Err(failed);
            }

            state.merge(body_state);
        }
    }

    fn run_until_block(
        &mut self,
        runtime: &mut RuntimeState,
        block: &WhileBlock,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        timeout_ms: Option<u64>,
        started: Instant,
        network_enabled: bool,
        extensions: Option<&dyn SandboxExtensions>,
        base_metadata: &BTreeMap<String, String>,
        script_stdin: &mut Option<Vec<u8>>,
        state: &mut ScriptState,
    ) -> Result<(), ExecutionResult> {
        let mut ran_body = false;

        loop {
            let condition = self
                .run_pipeline(
                    runtime,
                    &block.condition,
                    config,
                    cancel_flag,
                    timeout_ms,
                    started,
                    network_enabled,
                    extensions,
                    base_metadata,
                    script_stdin,
                )
                .map_err(|error| {
                    let mut failed =
                        ExecutionResult::failure(error, self.base_metadata(&runtime.cwd));
                    failed.stdout = state.stdout.clone();
                    failed.stderr = state.stderr.clone();
                    failed
                })?;
            state.executed_steps += 1;
            state.stdout.extend(&condition.stdout);
            state.stderr.extend(&condition.stderr);

            if condition.error.is_some() {
                let mut failed = condition;
                failed.stdout = state.stdout.clone();
                failed.stderr = state.stderr.clone();
                return Err(failed);
            }

            if condition.exit_code == 0 {
                if !ran_body {
                    state.last_result = Some(self.if_no_match_result(&runtime.cwd, base_metadata));
                }
                return Ok(());
            }

            ran_body = true;
            let mut body_state = ScriptState::default();
            if let Err(mut failed) = self.run_steps(
                runtime,
                &block.body_steps,
                config,
                cancel_flag,
                timeout_ms,
                started,
                network_enabled,
                extensions,
                base_metadata,
                script_stdin,
                &mut body_state,
            ) {
                state.merge(body_state);
                failed.stdout = state.stdout.clone();
                failed.stderr = state.stderr.clone();
                return Err(failed);
            }

            state.merge(body_state);
        }
    }

    fn run_for_block(
        &mut self,
        runtime: &mut RuntimeState,
        block: &ForBlock,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        timeout_ms: Option<u64>,
        started: Instant,
        network_enabled: bool,
        extensions: Option<&dyn SandboxExtensions>,
        base_metadata: &BTreeMap<String, String>,
        script_stdin: &mut Option<Vec<u8>>,
        state: &mut ScriptState,
    ) -> Result<(), ExecutionResult> {
        let items = if block.items.is_empty() {
            runtime.positional_args.clone()
        } else {
            expand_words(&block.items, &runtime.env, &runtime.positional_args).map_err(|error| {
                let mut failed = ExecutionResult::failure(error, self.base_metadata(&runtime.cwd));
                failed.stdout = state.stdout.clone();
                failed.stderr = state.stderr.clone();
                failed
            })?
        };

        if items.is_empty() {
            state.last_result = Some(self.if_no_match_result(&runtime.cwd, base_metadata));
            return Ok(());
        }

        for item in items {
            runtime.env.insert(block.name.clone(), item);
            let mut body_state = ScriptState::default();
            if let Err(mut failed) = self.run_steps(
                runtime,
                &block.body_steps,
                config,
                cancel_flag,
                timeout_ms,
                started,
                network_enabled,
                extensions,
                base_metadata,
                script_stdin,
                &mut body_state,
            ) {
                state.merge(body_state);
                failed.stdout = state.stdout.clone();
                failed.stderr = state.stderr.clone();
                return Err(failed);
            }
            state.merge(body_state);
        }

        Ok(())
    }

    fn register_function(
        &mut self,
        runtime: &mut RuntimeState,
        definition: &FunctionDef,
        base_metadata: &BTreeMap<String, String>,
        state: &mut ScriptState,
    ) {
        runtime
            .functions
            .insert(definition.name.clone(), definition.body_steps.clone());
        state.last_result = Some(ExecutionResult::success(
            Vec::new(),
            self.command_metadata(&runtime.cwd, base_metadata.clone(), &definition.name),
        ));
    }

    fn run_pipeline(
        &mut self,
        runtime: &mut RuntimeState,
        pipeline: &Pipeline,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        timeout_ms: Option<u64>,
        started: Instant,
        network_enabled: bool,
        extensions: Option<&dyn SandboxExtensions>,
        base_metadata: &BTreeMap<String, String>,
        script_stdin: &mut Option<Vec<u8>>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut piped_stdin = None;
        let mut pipeline_stderr = Vec::new();
        let mut last_result = None;

        for (index, command) in pipeline.commands.iter().enumerate() {
            validate_pipeline_redirections(pipeline, index, command)?;
            let command_env =
                expand_command_env(&command.assignments, &runtime.env, &runtime.positional_args)?;
            let expanded_argv =
                expand_words(&command.argv, &command_env, &runtime.positional_args)?;
            let expanded_argv = resolve_alias_words(&expanded_argv, &runtime.aliases)?;
            let command_name = expanded_argv.first().cloned().ok_or_else(|| {
                SandboxError::InvalidRequest(
                    "script command must expand to at least one command word".to_string(),
                )
            })?;
            let args = self.expand_script_args(&runtime.cwd, &command_name, &expanded_argv[1..])?;

            let stdin = if let Some(path) = input_redirect(command) {
                let resolved = resolve_sandbox_path(
                    &runtime.cwd,
                    &path.expand(&command_env, &runtime.positional_args)?,
                )?;
                self.read_path(&resolved)?
            } else if let Some(input) = piped_stdin.take() {
                input
            } else {
                script_stdin.take().unwrap_or_default()
            };

            let remaining_timeout = remaining_timeout_ms(timeout_ms, started)?;
            let cwd = runtime.cwd.clone();
            let result = match self.execute_command(
                runtime,
                &cwd,
                args,
                stdin,
                command_name.clone(),
                config,
                cancel_flag,
                extensions,
                remaining_timeout,
                network_enabled,
                command_env.clone(),
                base_metadata.clone(),
            ) {
                Ok(result) => result,
                Err(error) => ExecutionResult::failure(
                    error,
                    self.command_metadata(&cwd, base_metadata.clone(), &command_name),
                ),
            };
            let routed = self.apply_redirects(
                &runtime.cwd,
                &command.redirects,
                &result.stdout,
                &result.stderr,
                &command_env,
                &runtime.positional_args,
            )?;
            pipeline_stderr.extend(&routed.stderr);
            let mut result = result;
            result.stdout = routed.stdout.clone();
            result.stderr = routed.stderr.clone();

            if result.error.is_some() {
                let mut failed = result;
                failed.stderr = pipeline_stderr;
                return Ok(failed);
            }

            piped_stdin = Some(routed.stdout);
            last_result = Some(result);
        }

        let Some(mut result) = last_result else {
            return Err(SandboxError::InvalidRequest(
                "pipeline must contain at least one command".to_string(),
            ));
        };
        result.stdout = piped_stdin.unwrap_or_default();
        result.stderr = pipeline_stderr;
        Ok(result)
    }

    fn execute_command(
        &mut self,
        runtime: &mut RuntimeState,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        command: String,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        env: BTreeMap<String, String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        if let Some(extensions) = extensions {
            let custom_request = ExecutionRequest {
                mode: ExecutionMode::Argv,
                argv: std::iter::once(command.clone())
                    .chain(args.clone())
                    .collect(),
                script: None,
                cwd: cwd.to_string(),
                env: env.clone(),
                replace_env: false,
                stdin: stdin.clone(),
                timeout_ms,
                network_enabled,
                filesystem_mode: config.filesystem_mode.clone(),
                metadata: metadata.clone(),
            };
            if let Some(extension_result) = extensions.exec_custom_command(&custom_request)? {
                match extension_result {
                    ExtensionCommandResult::Completed(mut result) => {
                        result
                            .metadata
                            .entry("backend".to_string())
                            .or_insert_with(|| "custom".to_string());
                        result
                            .metadata
                            .entry("command".to_string())
                            .or_insert_with(|| command.clone());
                        result
                            .metadata
                            .entry("cwd".to_string())
                            .or_insert_with(|| cwd.to_string());
                        result
                            .metadata
                            .entry("filesystem_mode".to_string())
                            .or_insert_with(|| config.filesystem_mode.as_str().to_string());
                        return Ok(result);
                    }
                    ExtensionCommandResult::Delegate(mut delegated_request) => {
                        if delegated_request.cwd.is_empty() {
                            delegated_request.cwd = cwd.to_string();
                        }
                        if delegated_request.filesystem_mode != config.filesystem_mode {
                            delegated_request.filesystem_mode = config.filesystem_mode.clone();
                        }
                        if delegated_request.metadata.is_empty() {
                            delegated_request.metadata = metadata.clone();
                        } else {
                            for (key, value) in &metadata {
                                delegated_request
                                    .metadata
                                    .entry(key.clone())
                                    .or_insert_with(|| value.clone());
                            }
                        }
                        return match delegated_request.mode {
                            ExecutionMode::Argv => self.run_argv(
                                runtime,
                                delegated_request,
                                config,
                                cancel_flag,
                                Some(extensions),
                            ),
                            ExecutionMode::Script => self.run_script(
                                runtime,
                                delegated_request,
                                config,
                                cancel_flag,
                                Some(extensions),
                            ),
                        };
                    }
                }
            }
        }

        if runtime.functions.contains_key(&command) {
            return self.run_function_call(
                runtime,
                &command,
                args,
                config,
                cancel_flag,
                extensions,
                timeout_ms,
                network_enabled,
                metadata,
            );
        }
        if command == "local" {
            return self.run_local(runtime, args, metadata);
        }
        if !config.allowlisted_commands.contains(&command) {
            return Err(SandboxError::PolicyDenied(format!(
                "command is not allowlisted: {command}"
            )));
        }

        let metadata = self.command_metadata(cwd, metadata, &command);
        match command.as_str() {
            "echo" => Ok(ExecutionResult::success(
                format!("{}\n", args.join(" ")).into_bytes(),
                metadata,
            )),
            "env" => self.run_env(
                runtime,
                cwd,
                args,
                config,
                cancel_flag,
                extensions,
                timeout_ms,
                network_enabled,
                env,
                metadata,
            ),
            "curl" => self.run_curl(
                cwd,
                args,
                stdin,
                config,
                timeout_ms,
                network_enabled,
                metadata,
            ),
            "which" => self.run_which(args, config, metadata),
            "dirname" => self.run_dirname(args, metadata),
            "basename" => self.run_basename(args, metadata),
            "cd" => self.run_cd(runtime, args, &config.default_cwd, metadata),
            "export" => self.run_export(runtime, args, env, metadata),
            "expr" => self.run_expr(args, metadata),
            "time" => self.run_time(
                runtime,
                cwd,
                args,
                config,
                cancel_flag,
                extensions,
                timeout_ms,
                network_enabled,
                metadata,
            ),
            "timeout" => self.run_timeout(
                runtime,
                cwd,
                args,
                config,
                cancel_flag,
                extensions,
                timeout_ms,
                network_enabled,
                metadata,
            ),
            "whoami" => self.run_whoami(args, &env, metadata),
            "hostname" => self.run_hostname(args, metadata),
            "help" => self.run_help(config, metadata),
            "clear" => self.run_clear(args, metadata),
            "history" => self.run_history(args, metadata),
            "alias" => self.run_alias(runtime, args, metadata),
            "unalias" => self.run_unalias(runtime, args, metadata),
            "bash" | "sh" => self.run_shell_command(
                runtime,
                cwd,
                &command,
                args,
                config,
                cancel_flag,
                extensions,
                timeout_ms,
                network_enabled,
                metadata,
            ),
            "pwd" => Ok(ExecutionResult::success(
                format!("{}\n", runtime.cwd).into_bytes(),
                metadata,
            )),
            "printenv" => Ok(ExecutionResult::success(
                render_env(&env, &args).into_bytes(),
                metadata,
            )),
            "tree" => self.run_tree(cwd, args, metadata),
            "stat" => self.run_stat(cwd, args, metadata),
            "du" => self.run_du(cwd, args, metadata),
            "file" => self.run_file(cwd, args, metadata),
            "readlink" => self.run_readlink(cwd, args, metadata),
            "ln" => self.run_ln(cwd, args, metadata),
            "cat" => self.run_cat(cwd, args, stdin, metadata),
            "grep" => self.run_grep(cwd, args, stdin, metadata),
            "egrep" => self.run_grep_alias(cwd, args, stdin, metadata, "-E"),
            "fgrep" => self.run_grep_alias(cwd, args, stdin, metadata, "-F"),
            "wc" => self.run_wc(cwd, args, stdin, metadata),
            "sort" => self.run_sort(cwd, args, stdin, metadata),
            "uniq" => self.run_uniq(cwd, args, stdin, metadata),
            "head" => self.run_head(cwd, args, stdin, metadata),
            "tail" => self.run_tail(cwd, args, stdin, metadata),
            "cut" => self.run_cut(cwd, args, stdin, metadata),
            "tr" => self.run_tr(cwd, args, stdin, metadata),
            "paste" => self.run_paste(cwd, args, stdin, metadata),
            "sed" => self.run_sed(cwd, args, stdin, metadata),
            "join" => self.run_join(cwd, args, metadata),
            "awk" => self.run_awk(cwd, args, stdin, metadata),
            "jq" => self.run_jq(cwd, args, stdin, metadata),
            "yq" => self.run_yq(cwd, args, stdin, metadata),
            "find" => self.run_find(cwd, args, metadata),
            "ls" => self.run_ls(cwd, args, metadata),
            "rev" => self.run_rev(cwd, args, stdin, metadata),
            "nl" => self.run_nl(cwd, args, stdin, metadata),
            "tac" => self.run_tac(cwd, args, stdin, metadata),
            "strings" => self.run_strings(cwd, args, stdin, metadata),
            "fold" => self.run_fold(cwd, args, stdin, metadata),
            "expand" => self.run_expand(cwd, args, stdin, metadata),
            "unexpand" => self.run_unexpand(cwd, args, stdin, metadata),
            "rm" => self.run_rm(cwd, args, metadata),
            "cp" => self.run_cp(cwd, args, metadata),
            "mv" => self.run_mv(cwd, args, metadata),
            "tee" => self.run_tee(cwd, args, stdin, metadata),
            "printf" => self.run_printf(args, metadata),
            "seq" => self.run_seq(args, metadata),
            "date" => self.run_date(args, metadata),
            "gzip" => self.run_gzip(cwd, args, stdin, metadata),
            "html-to-markdown" => self.run_html_to_markdown(cwd, args, stdin, metadata),
            "gunzip" => self.run_gunzip(cwd, args, stdin, metadata),
            "zcat" => self.run_zcat(cwd, args, stdin, metadata),
            "tar" => self.run_tar(cwd, args, stdin, metadata),
            "sqlite3" => self.run_sqlite3(cwd, args, stdin, metadata),
            "split" => self.run_split(cwd, args, stdin, metadata),
            "od" => self.run_od(cwd, args, stdin, metadata),
            "base64" => self.run_base64(cwd, args, stdin, metadata),
            "md5sum" => self.run_hash(cwd, args, stdin, hashcmd::HashKind::Md5, metadata),
            "sha1sum" => self.run_hash(cwd, args, stdin, hashcmd::HashKind::Sha1, metadata),
            "sha256sum" => self.run_hash(cwd, args, stdin, hashcmd::HashKind::Sha256, metadata),
            "mkdir" => self.run_mkdir(cwd, args, metadata),
            "touch" => self.run_touch(cwd, args, metadata),
            "rmdir" => self.run_rmdir(cwd, args, metadata),
            "comm" => self.run_comm(cwd, args, metadata),
            "diff" => self.run_diff(cwd, args, metadata),
            "column" => self.run_column(cwd, args, stdin, metadata),
            "chmod" => self.run_chmod(cwd, args, metadata),
            "python" => self.run_python3(cwd, args, stdin, &env, timeout_ms, cancel_flag, metadata),
            "python3" => {
                self.run_python3(cwd, args, stdin, &env, timeout_ms, cancel_flag, metadata)
            }
            "js-exec" => {
                self.run_js_exec(cwd, args, stdin, &env, timeout_ms, cancel_flag, metadata)
            }
            "xan" => self.run_xan(cwd, args, stdin, metadata),
            "xargs" => self.run_xargs(
                runtime,
                cwd,
                args,
                stdin,
                config,
                cancel_flag,
                extensions,
                timeout_ms,
                network_enabled,
                env,
                metadata,
            ),
            "rg" => self.run_rg(cwd, args, stdin, metadata),
            "true" => Ok(ExecutionResult::success(Vec::new(), metadata)),
            "false" => Ok(ExecutionResult {
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 1,
                termination_reason: TerminationReason::Exited,
                error: None,
                metadata,
            }),
            "sleep" => run_sleep(args, timeout_ms, cancel_flag, metadata),
            _ => Err(SandboxError::BackendFailure(format!(
                "allowlisted command is not implemented by virtual backend: {command}"
            ))),
        }
    }

    fn command_metadata(
        &self,
        cwd: &str,
        mut metadata: BTreeMap<String, String>,
        command: &str,
    ) -> BTreeMap<String, String> {
        metadata.insert("backend".to_string(), self.name().to_string());
        metadata.insert("command".to_string(), command.to_string());
        metadata.insert("cwd".to_string(), cwd.to_string());
        metadata.insert(
            "filesystem_mode".to_string(),
            self.filesystem.mode().as_str().to_string(),
        );
        if self.filesystem.mode() != FilesystemMode::Memory {
            metadata.insert("workspace_mount".to_string(), "/workspace".to_string());
        }
        metadata
    }

    fn base_metadata(&self, cwd: &str) -> BTreeMap<String, String> {
        self.command_metadata(cwd, BTreeMap::new(), "script")
    }

    fn if_no_match_result(
        &self,
        cwd: &str,
        base_metadata: &BTreeMap<String, String>,
    ) -> ExecutionResult {
        ExecutionResult::success(
            Vec::new(),
            self.command_metadata(cwd, base_metadata.clone(), "if"),
        )
    }

    fn push_history(&mut self, entry: String) {
        if !entry.is_empty() {
            self.history.push(entry);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_nested_script(
        &mut self,
        runtime: &RuntimeState,
        source: String,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let steps = parse_script(&source)?;
        let started = Instant::now();
        let mut script_stdin = Some(Vec::new());
        let mut state = ScriptState::default();
        let mut child = runtime.child();

        if let Err(mut failed) = self.run_steps(
            &mut child,
            &steps,
            config,
            cancel_flag,
            timeout_ms,
            started,
            network_enabled,
            extensions,
            &metadata,
            &mut script_stdin,
            &mut state,
        ) {
            let last_command = failed.metadata.get("command").cloned();
            failed.metadata = decorate_script_metadata(
                failed.metadata,
                &child.cwd,
                state.executed_steps,
                last_command,
            );
            return Ok(failed);
        }

        if let Some(mut result) = state.last_result {
            let last_command = result.metadata.get("command").cloned();
            result.stdout = state.stdout;
            result.stderr = state.stderr;
            result.metadata = decorate_script_metadata(
                result.metadata,
                &child.cwd,
                state.executed_steps,
                last_command,
            );
            return Ok(result);
        }

        Ok(ExecutionResult::success(
            Vec::new(),
            decorate_script_metadata(self.base_metadata(&child.cwd), &child.cwd, 0, None),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn run_function_call(
        &mut self,
        runtime: &mut RuntimeState,
        name: &str,
        args: Vec<String>,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let body_steps = runtime.functions.get(name).cloned().ok_or_else(|| {
            SandboxError::BackendFailure(format!("function is not defined: {name}"))
        })?;
        let started = Instant::now();
        let mut child = runtime.child();
        child.positional_args = args;
        child.function_depth += 1;
        let mut script_stdin = Some(Vec::new());
        let mut state = ScriptState::default();

        if let Err(mut failed) = self.run_steps(
            &mut child,
            &body_steps,
            config,
            cancel_flag,
            timeout_ms,
            started,
            network_enabled,
            extensions,
            &metadata,
            &mut script_stdin,
            &mut state,
        ) {
            runtime.cwd = child.cwd;
            runtime.persisted_cwd = child.persisted_cwd;
            runtime.exported_env = child.exported_env;
            runtime.aliases = child.aliases;
            runtime.functions = child.functions;
            let last_command = failed.metadata.get("command").cloned();
            failed.metadata = decorate_script_metadata(
                failed.metadata,
                &runtime.cwd,
                state.executed_steps,
                last_command,
            );
            return Ok(failed);
        }

        runtime.cwd = child.cwd;
        runtime.persisted_cwd = child.persisted_cwd;
        runtime.exported_env = child.exported_env;
        runtime.aliases = child.aliases;
        runtime.functions = child.functions;

        if let Some(mut result) = state.last_result {
            let last_command = result.metadata.get("command").cloned();
            result.stdout = state.stdout;
            result.stderr = state.stderr;
            result.metadata = decorate_script_metadata(
                result.metadata,
                &runtime.cwd,
                state.executed_steps,
                last_command,
            );
            return Ok(result);
        }

        Ok(ExecutionResult::success(
            Vec::new(),
            decorate_script_metadata(self.base_metadata(&runtime.cwd), &runtime.cwd, 0, None),
        ))
    }

    fn run_local(
        &mut self,
        runtime: &mut RuntimeState,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        if runtime.function_depth == 0 {
            return Err(SandboxError::InvalidRequest(
                "local may only be used inside a function".to_string(),
            ));
        }
        if args.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "local requires at least one variable name".to_string(),
            ));
        }
        for arg in args {
            if let Some((name, value)) = arg.split_once('=') {
                if !is_valid_assignment_name(name) {
                    return Err(SandboxError::InvalidRequest(format!(
                        "invalid local variable name: {name}"
                    )));
                }
                runtime.env.insert(name.to_string(), value.to_string());
                continue;
            }
            if !is_valid_assignment_name(&arg) {
                return Err(SandboxError::InvalidRequest(format!(
                    "invalid local variable name: {arg}"
                )));
            }
            runtime.env.entry(arg).or_default();
        }
        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn apply_redirects(
        &mut self,
        cwd: &str,
        redirects: &[RedirectSpec],
        stdout: &[u8],
        stderr: &[u8],
        env: &BTreeMap<String, String>,
        positional_args: &[String],
    ) -> Result<RoutedOutput, SandboxError> {
        let mut stdout_target = StreamTarget::StdoutCapture;
        let mut stderr_target = StreamTarget::StderrCapture;

        for redirect in redirects {
            match redirect {
                RedirectSpec::Input(_) => {}
                RedirectSpec::StdoutTruncate(path) => {
                    stdout_target = StreamTarget::File(resolve_file_target(
                        cwd,
                        path,
                        env,
                        positional_args,
                        false,
                    )?);
                }
                RedirectSpec::StdoutAppend(path) => {
                    stdout_target = StreamTarget::File(resolve_file_target(
                        cwd,
                        path,
                        env,
                        positional_args,
                        true,
                    )?);
                }
                RedirectSpec::StderrTruncate(path) => {
                    stderr_target = StreamTarget::File(resolve_file_target(
                        cwd,
                        path,
                        env,
                        positional_args,
                        false,
                    )?);
                }
                RedirectSpec::StderrAppend(path) => {
                    stderr_target = StreamTarget::File(resolve_file_target(
                        cwd,
                        path,
                        env,
                        positional_args,
                        true,
                    )?);
                }
                RedirectSpec::StderrToStdout => {
                    stderr_target = stdout_target.clone();
                }
            }
        }

        let mut routed = RoutedOutput::default();
        let mut pending_writes = Vec::new();
        route_stream(&mut routed, &mut pending_writes, &stdout_target, stdout);
        route_stream(&mut routed, &mut pending_writes, &stderr_target, stderr);

        if let (StreamTarget::File(stdout_file), StreamTarget::File(stderr_file)) =
            (&stdout_target, &stderr_target)
        {
            if stdout_file == stderr_file {
                pending_writes = vec![PendingFileWrite {
                    target: stdout_file.clone(),
                    contents: [stdout, stderr].concat(),
                }];
            }
        }

        for write in pending_writes {
            self.write_redirect_file(&write)?;
        }

        Ok(routed)
    }

    fn write_redirect_file(&mut self, write: &PendingFileWrite) -> Result<(), SandboxError> {
        let contents = if write.target.append {
            let mut existing = match self.read_path(&write.target.path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == abash_core::ErrorKind::InvalidRequest => Vec::new(),
                Err(error) => return Err(error),
            };
            existing.extend_from_slice(&write.contents);
            existing
        } else {
            write.contents.clone()
        };
        self.filesystem
            .write_file(&write.target.path, contents, false)
    }

    fn expand_globs(&self, cwd: &str, args: Vec<String>) -> Result<Vec<String>, SandboxError> {
        let mut expanded = Vec::new();
        let candidates = self.list_paths()?;

        for arg in args {
            if !contains_glob_pattern(&arg) {
                expanded.push(arg);
                continue;
            }

            let pattern = resolve_sandbox_path(cwd, &arg)?;
            let mut matches = candidates
                .iter()
                .filter(|candidate| glob_matches_path(&pattern, candidate))
                .map(|candidate| format_glob_match(cwd, &arg, candidate))
                .collect::<Vec<_>>();
            matches.sort();
            matches.dedup();

            if matches.is_empty() {
                expanded.push(arg);
            } else {
                expanded.extend(matches);
            }
        }

        Ok(expanded)
    }

    fn expand_script_args(
        &self,
        cwd: &str,
        command_name: &str,
        args: &[String],
    ) -> Result<Vec<String>, SandboxError> {
        if command_name != "find"
            && command_name != "rg"
            && command_name != "grep"
            && command_name != "egrep"
            && command_name != "fgrep"
            && command_name != "jq"
            && command_name != "yq"
            && command_name != "sqlite3"
            && command_name != "xan"
        {
            return self.expand_globs(cwd, args.to_vec());
        }

        if command_name == "xan" {
            return self.expand_xan_script_args(cwd, args);
        }

        let mut expanded = Vec::new();
        let candidates = self.list_paths()?;
        let mut literal_next = false;
        let mut rg_pattern_consumed = command_name != "rg";
        let mut grep_pattern_consumed =
            command_name != "grep" && command_name != "egrep" && command_name != "fgrep";
        let mut jq_filter_consumed = command_name != "jq";
        let mut yq_filter_consumed = command_name != "yq";
        let mut yq_option_value_expected = false;
        let mut sqlite3_option_value_expected = false;
        let mut sqlite3_database_consumed = command_name != "sqlite3";
        let mut sqlite3_sql_consumed = command_name != "sqlite3";
        let mut grep_flags_done = false;

        for arg in args {
            if literal_next {
                expanded.push(arg.clone());
                literal_next = false;
                continue;
            }

            if yq_option_value_expected {
                expanded.push(arg.clone());
                yq_option_value_expected = false;
                continue;
            }

            if sqlite3_option_value_expected {
                expanded.push(arg.clone());
                sqlite3_option_value_expected = false;
                continue;
            }

            if arg == "-name" {
                expanded.push(arg.clone());
                literal_next = true;
                continue;
            }

            if command_name == "rg" && !rg_pattern_consumed {
                if arg.starts_with('-') {
                    expanded.push(arg.clone());
                    continue;
                }
                expanded.push(arg.clone());
                rg_pattern_consumed = true;
                continue;
            }

            if (command_name == "grep" || command_name == "egrep" || command_name == "fgrep")
                && !grep_pattern_consumed
            {
                if !grep_flags_done && arg == "--" {
                    expanded.push(arg.clone());
                    grep_flags_done = true;
                    continue;
                }
                if !grep_flags_done && arg.starts_with('-') && arg != "-" {
                    expanded.push(arg.clone());
                    continue;
                }
                expanded.push(arg.clone());
                grep_pattern_consumed = true;
                continue;
            }

            if command_name == "jq" && !jq_filter_consumed {
                if arg != "-" && arg.starts_with('-') {
                    expanded.push(arg.clone());
                    continue;
                }
                expanded.push(arg.clone());
                jq_filter_consumed = true;
                continue;
            }

            if command_name == "yq"
                && matches!(
                    arg.as_str(),
                    "-p" | "--input-format" | "-o" | "--output-format"
                )
            {
                expanded.push(arg.clone());
                yq_option_value_expected = true;
                continue;
            }

            if command_name == "yq" && !yq_filter_consumed {
                if arg != "-" && arg.starts_with('-') {
                    expanded.push(arg.clone());
                    continue;
                }
                expanded.push(arg.clone());
                yq_filter_consumed = true;
                continue;
            }

            if command_name == "sqlite3" && matches!(arg.as_str(), "-separator" | "-cmd") {
                expanded.push(arg.clone());
                sqlite3_option_value_expected = true;
                continue;
            }

            if command_name == "sqlite3" && !sqlite3_database_consumed {
                if arg.starts_with('-') {
                    expanded.push(arg.clone());
                    continue;
                }
                expanded.push(arg.clone());
                sqlite3_database_consumed = true;
                continue;
            }

            if command_name == "sqlite3" && sqlite3_database_consumed && !sqlite3_sql_consumed {
                expanded.push(arg.clone());
                sqlite3_sql_consumed = true;
                continue;
            }

            if !contains_glob_pattern(arg) {
                expanded.push(arg.clone());
                continue;
            }

            let pattern = resolve_sandbox_path(cwd, arg)?;
            let mut matches = candidates
                .iter()
                .filter(|candidate| glob_matches_path(&pattern, candidate))
                .map(|candidate| format_glob_match(cwd, arg, candidate))
                .collect::<Vec<_>>();
            matches.sort();
            matches.dedup();

            if matches.is_empty() {
                expanded.push(arg.clone());
            } else {
                expanded.extend(matches);
            }
        }

        Ok(expanded)
    }

    fn expand_xan_script_args(
        &self,
        cwd: &str,
        args: &[String],
    ) -> Result<Vec<String>, SandboxError> {
        let Some(subcommand) = args.first() else {
            return Ok(Vec::new());
        };
        let candidates = self.list_paths()?;
        let mut expanded = vec![subcommand.clone()];
        let mut literal_next = false;
        let mut positional_consumed = 0usize;

        for arg in &args[1..] {
            if literal_next {
                expanded.push(arg.clone());
                literal_next = false;
                continue;
            }

            match subcommand.as_str() {
                "head" | "tail" if matches!(arg.as_str(), "-n" | "-l") => {
                    expanded.push(arg.clone());
                    literal_next = true;
                    continue;
                }
                "slice"
                    if matches!(
                        arg.as_str(),
                        "-s" | "--start" | "-e" | "--end" | "-l" | "--len"
                    ) =>
                {
                    expanded.push(arg.clone());
                    literal_next = true;
                    continue;
                }
                "enum" if arg == "-c" => {
                    expanded.push(arg.clone());
                    literal_next = true;
                    continue;
                }
                "search" if matches!(arg.as_str(), "-s" | "--select") => {
                    expanded.push(arg.clone());
                    literal_next = true;
                    continue;
                }
                "search"
                    if matches!(
                        arg.as_str(),
                        "-v" | "--invert" | "-i" | "--ignore-case" | "-r" | "--regex"
                    ) =>
                {
                    expanded.push(arg.clone());
                    continue;
                }
                "sort" if matches!(arg.as_str(), "-s" | "--select") => {
                    expanded.push(arg.clone());
                    literal_next = true;
                    continue;
                }
                "sort"
                    if matches!(arg.as_str(), "-N" | "--numeric" | "-R" | "-r" | "--reverse") =>
                {
                    expanded.push(arg.clone());
                    continue;
                }
                "filter" if matches!(arg.as_str(), "-l" | "--limit") => {
                    expanded.push(arg.clone());
                    literal_next = true;
                    continue;
                }
                "filter" if matches!(arg.as_str(), "-v" | "--invert") => {
                    expanded.push(arg.clone());
                    continue;
                }
                "rename" if matches!(arg.as_str(), "-s" | "--select") => {
                    expanded.push(arg.clone());
                    literal_next = true;
                    continue;
                }
                _ => {}
            }

            let keep_literal = match subcommand.as_str() {
                "select" => positional_consumed == 0,
                "drop" => positional_consumed == 0,
                "rename" => positional_consumed == 0,
                "search" => positional_consumed == 0,
                "filter" => positional_consumed == 0,
                _ => false,
            };
            if keep_literal {
                expanded.push(arg.clone());
                positional_consumed += 1;
                continue;
            }

            if !contains_glob_pattern(arg) {
                expanded.push(arg.clone());
                positional_consumed += 1;
                continue;
            }

            let pattern = resolve_sandbox_path(cwd, arg)?;
            let mut matches = candidates
                .iter()
                .filter(|candidate| glob_matches_path(&pattern, candidate))
                .map(|candidate| format_glob_match(cwd, arg, candidate))
                .collect::<Vec<_>>();
            matches.sort();
            matches.dedup();

            if matches.is_empty() {
                expanded.push(arg.clone());
            } else {
                expanded.extend(matches);
            }
            positional_consumed += 1;
        }

        Ok(expanded)
    }

    #[allow(clippy::too_many_arguments)]
    fn run_env(
        &mut self,
        runtime: &mut RuntimeState,
        cwd: &str,
        args: Vec<String>,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        env: BTreeMap<String, String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = envcmd::parse_spec(&args)?;
        let mut resolved_env = if spec.clear_env { BTreeMap::new() } else { env };
        resolved_env.extend(spec.assignments);

        let Some(command) = spec.command else {
            return Ok(ExecutionResult::success(
                render_env(&resolved_env, &[]).into_bytes(),
                metadata,
            ));
        };

        let mut child = runtime.child();
        child.env = resolved_env.clone();
        child.exported_env = resolved_env.clone();
        self.execute_command(
            &mut child,
            cwd,
            spec.args,
            Vec::new(),
            command,
            config,
            cancel_flag,
            extensions,
            timeout_ms,
            network_enabled,
            resolved_env,
            metadata,
        )
    }

    fn run_curl(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        config: &SandboxConfig,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        mut metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        if !network_enabled {
            return Err(SandboxError::PolicyDenied(
                "network access is disabled for this execution".to_string(),
            ));
        }
        let policy = config.network_policy.as_ref().ok_or_else(|| {
            SandboxError::PolicyDenied(
                "network access is disabled unless explicit policy is configured".to_string(),
            )
        })?;
        let spec = curlcmd::parse_spec(&args, stdin)?;
        let method = abash_core::normalize_http_method(&spec.method)?;
        policy.allows_method(&method)?;

        let timeout_limit = timeout_ms
            .map(|limit| limit.min(policy.request_timeout_ms))
            .unwrap_or(policy.request_timeout_ms);
        let timeout = Duration::from_millis(timeout_limit);
        let agent: Agent = Agent::config_builder()
            .timeout_global(Some(timeout))
            .timeout_per_call(Some(timeout))
            .timeout_resolve(Some(timeout))
            .max_redirects(0)
            .build()
            .into();

        let mut current_url = Url::parse(&spec.url).map_err(|error| {
            SandboxError::InvalidRequest(format!("curl URL must be valid: {error}"))
        })?;
        let mut redirects_left = if spec.follow_redirects {
            5usize
        } else {
            0usize
        };

        loop {
            let origin = policy.match_url(&current_url)?;
            let addrs = resolve_remote_addrs(&current_url)?;
            policy.ensure_remote_addrs(&addrs)?;

            let mut request = http::Request::builder()
                .method(
                    http::Method::from_bytes(method.as_bytes()).map_err(|error| {
                        SandboxError::InvalidRequest(format!("curl method is invalid: {error}"))
                    })?,
                )
                .uri(current_url.as_str());
            for (name, value) in &origin.injected_headers {
                request = request.header(name, value);
            }

            let body = spec.body.clone().unwrap_or_default();
            let mut response = if body.is_empty() {
                request
                    .body(())
                    .map_err(|error| {
                        SandboxError::InvalidRequest(format!(
                            "curl request could not be built: {error}"
                        ))
                    })?
                    .with_agent(&agent)
                    .configure()
                    .http_status_as_error(false)
                    .run()
                    .map_err(map_ureq_error)?
            } else {
                request
                    .body(body.clone())
                    .map_err(|error| {
                        SandboxError::InvalidRequest(format!(
                            "curl request could not be built: {error}"
                        ))
                    })?
                    .with_agent(&agent)
                    .configure()
                    .http_status_as_error(false)
                    .run()
                    .map_err(map_ureq_error)?
            };

            let status = response.status().as_u16();
            if spec.follow_redirects && is_redirect_status(status) {
                let Some(location) = response.headers().get("location") else {
                    return Err(SandboxError::BackendFailure(
                        "curl redirect response did not include a Location header".to_string(),
                    ));
                };
                if redirects_left == 0 {
                    return Err(SandboxError::BackendFailure(
                        "curl exceeded the redirect limit".to_string(),
                    ));
                }
                let location = location.to_str().map_err(|_| {
                    SandboxError::BackendFailure(
                        "curl redirect location was not valid text".to_string(),
                    )
                })?;
                current_url = current_url.join(location).map_err(|error| {
                    SandboxError::BackendFailure(format!(
                        "curl redirect target was invalid: {error}"
                    ))
                })?;
                redirects_left -= 1;
                continue;
            }

            let headers = response
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_string(),
                        value.to_str().unwrap_or_default().to_string(),
                    )
                })
                .collect::<Vec<_>>();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string);
            let body = if spec.head_only {
                Vec::new()
            } else {
                response
                    .body_mut()
                    .with_config()
                    .limit(policy.max_response_bytes as u64)
                    .read_to_vec()
                    .map_err(map_ureq_error)?
            };
            let rendered = curlcmd::render_response(&spec, status, &headers, &body);

            metadata.insert("http_status".to_string(), status.to_string());
            metadata.insert("http_final_url".to_string(), current_url.to_string());
            metadata.insert("http_method".to_string(), method.clone());
            if let Some(content_type) = content_type {
                metadata.insert("http_content_type".to_string(), content_type);
            }

            if let Some(path) = &spec.output_path {
                let resolved = resolve_sandbox_path(cwd, path)?;
                self.filesystem.write_file(&resolved, rendered, false)?;
                return Ok(ExecutionResult::success(Vec::new(), metadata));
            }

            return Ok(ExecutionResult::success(rendered, metadata));
        }
    }

    fn run_which(
        &mut self,
        args: Vec<String>,
        config: &SandboxConfig,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        if args.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "which requires at least one command name".to_string(),
            ));
        }
        let result = which::execute(&args, &config.allowlisted_commands);
        Ok(ExecutionResult {
            stdout: result.output,
            stderr: Vec::new(),
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_dirname(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(pathcmd::dirname(&args)?, metadata))
    }

    fn run_basename(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(
            pathcmd::basename(&args)?,
            metadata,
        ))
    }

    fn run_cd(
        &mut self,
        runtime: &mut RuntimeState,
        args: Vec<String>,
        default_cwd: &str,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let target = tier3_shell::cd(&runtime.cwd, default_cwd, &args)?;
        let resolved = resolve_sandbox_path(default_cwd, &target)?;
        runtime.cwd = resolved.clone();
        runtime.persisted_cwd = resolved;
        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_export(
        &mut self,
        runtime: &mut RuntimeState,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut command_env = env;
        let mut exported_env = runtime.exported_env.clone();
        let rendered = tier3_shell::export(&args, &mut command_env, &mut exported_env)?;
        for (name, value) in &exported_env {
            runtime.env.insert(name.clone(), value.clone());
        }
        runtime.exported_env = exported_env;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_expr(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(exprcmd::execute(&args)?, metadata))
    }

    #[allow(clippy::too_many_arguments)]
    fn run_time(
        &mut self,
        runtime: &mut RuntimeState,
        cwd: &str,
        args: Vec<String>,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let (command, command_args) = split_nested_command("time", &args)?;
        let started = Instant::now();
        let env = runtime.env.clone();
        let mut result = self.execute_command(
            runtime,
            cwd,
            command_args,
            Vec::new(),
            command,
            config,
            cancel_flag,
            extensions,
            timeout_ms,
            network_enabled,
            env,
            metadata,
        )?;
        result.stderr = tier3_exec::render_time(&result.stderr, started.elapsed());
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn run_timeout(
        &mut self,
        runtime: &mut RuntimeState,
        cwd: &str,
        args: Vec<String>,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let Some(duration) = args.first() else {
            return Err(SandboxError::InvalidRequest(
                "timeout requires a duration and command".to_string(),
            ));
        };
        let limit_ms = tier3_exec::parse_timeout_ms(duration)?;
        let (command, command_args) = split_nested_command("timeout", &args[1..])?;
        let nested_timeout = match timeout_ms {
            Some(existing) => Some(existing.min(limit_ms)),
            None => Some(limit_ms),
        };
        let env = runtime.env.clone();
        self.execute_command(
            runtime,
            cwd,
            command_args,
            Vec::new(),
            command,
            config,
            cancel_flag,
            extensions,
            nested_timeout,
            network_enabled,
            env,
            metadata,
        )
    }

    fn run_whoami(
        &mut self,
        args: Vec<String>,
        env: &BTreeMap<String, String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(
            tier3_shell::whoami(&args, env)?,
            metadata,
        ))
    }

    fn run_hostname(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(
            tier3_shell::hostname(&args)?,
            metadata,
        ))
    }

    fn run_help(
        &mut self,
        config: &SandboxConfig,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(
            tier3_shell::help(&config.allowlisted_commands),
            metadata,
        ))
    }

    fn run_clear(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(
            tier3_shell::clear(&args)?,
            metadata,
        ))
    }

    fn run_history(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        if !args.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "history does not accept arguments".to_string(),
            ));
        }
        Ok(ExecutionResult::success(
            tier3_shell::render_history(&self.history),
            metadata,
        ))
    }

    fn run_alias(
        &mut self,
        runtime: &mut RuntimeState,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(
            tier3_shell::alias(&args, &mut runtime.aliases)?,
            metadata,
        ))
    }

    fn run_unalias(
        &mut self,
        runtime: &mut RuntimeState,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        tier3_shell::unalias(&args, &mut runtime.aliases)?;
        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    #[allow(clippy::too_many_arguments)]
    fn run_shell_command(
        &mut self,
        runtime: &mut RuntimeState,
        cwd: &str,
        command: &str,
        args: Vec<String>,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let program = tier3_exec::parse_shell_program(&args, command)?;
        let source = match program {
            tier3_exec::ShellProgram::Inline(source) => source,
            tier3_exec::ShellProgram::File(path) => {
                let resolved = resolve_sandbox_path(cwd, &path)?;
                String::from_utf8(self.read_path(&resolved)?).map_err(|_| {
                    SandboxError::InvalidRequest(
                        "bash/sh script files currently require UTF-8 text".to_string(),
                    )
                })?
            }
        };
        self.run_nested_script(
            &runtime.child(),
            source,
            config,
            cancel_flag,
            extensions,
            timeout_ms,
            network_enabled,
            metadata,
        )
    }

    fn run_cat(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(
            self.read_command_inputs(cwd, &args, stdin)?,
            metadata,
        ))
    }

    fn run_grep(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let result = grepcmd::execute(
            cwd,
            &args,
            stdin,
            |path| self.read_path(path),
            || self.list_paths(),
            |path| self.path_is_dir(path),
        )?;
        Ok(ExecutionResult {
            stdout: result.output,
            stderr: result.stderr,
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_grep_alias(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
        injected_flag: &str,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut forwarded = vec![injected_flag.to_string()];
        forwarded.extend(args);
        self.run_grep(cwd, forwarded, stdin, metadata)
    }

    fn run_wc(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut count_lines = false;
        let mut count_words = false;
        let mut count_bytes = false;
        let mut index = 0usize;
        while let Some(flag) = args.get(index) {
            match flag.as_str() {
                "-l" => count_lines = true,
                "-w" => count_words = true,
                "-c" => count_bytes = true,
                _ if flag.starts_with('-') => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "wc flag is not supported: {flag}"
                    )));
                }
                _ => break,
            }
            index += 1;
        }
        if !count_lines && !count_words && !count_bytes {
            count_lines = true;
            count_words = true;
            count_bytes = true;
        }

        let contents = self.read_command_inputs(cwd, &args[index..], stdin)?;
        let text = String::from_utf8_lossy(&contents);
        let mut fields = Vec::new();
        if count_lines {
            fields.push(text.lines().count().to_string());
        }
        if count_words {
            fields.push(text.split_whitespace().count().to_string());
        }
        if count_bytes {
            fields.push(contents.len().to_string());
        }

        Ok(ExecutionResult::success(
            format!("{}\n", fields.join(" ")).into_bytes(),
            metadata,
        ))
    }

    fn run_sort(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut reverse = false;
        let mut index = 0usize;
        while let Some(flag) = args.get(index) {
            match flag.as_str() {
                "-r" => reverse = true,
                _ if flag.starts_with('-') => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "sort flag is not supported: {flag}"
                    )));
                }
                _ => break,
            }
            index += 1;
        }

        let contents = self.read_command_inputs(cwd, &args[index..], stdin)?;
        let mut lines = String::from_utf8_lossy(&contents)
            .lines()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        lines.sort();
        if reverse {
            lines.reverse();
        }

        Ok(ExecutionResult::success(
            render_lines(&lines).into_bytes(),
            metadata,
        ))
    }

    fn run_uniq(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut count = false;
        let mut index = 0usize;
        while let Some(flag) = args.get(index) {
            match flag.as_str() {
                "-c" => count = true,
                _ if flag.starts_with('-') => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "uniq flag is not supported: {flag}"
                    )));
                }
                _ => break,
            }
            index += 1;
        }

        let contents = self.read_command_inputs(cwd, &args[index..], stdin)?;
        let lines = String::from_utf8_lossy(&contents)
            .lines()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let mut deduped: Vec<(usize, String)> = Vec::new();
        for line in lines {
            if let Some((seen, previous)) = deduped.last_mut() {
                if *previous == line {
                    if count {
                        *seen += 1;
                    }
                    continue;
                }
            }
            deduped.push((1usize, line));
        }

        let rendered = if count {
            deduped
                .into_iter()
                .map(|(seen, line)| format!("{seen} {line}"))
                .collect::<Vec<_>>()
        } else {
            deduped
                .into_iter()
                .map(|(_, line)| line)
                .collect::<Vec<_>>()
        };

        Ok(ExecutionResult::success(
            render_lines(&rendered).into_bytes(),
            metadata,
        ))
    }

    fn run_head(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let (count, index) = parse_line_count_flag("head", &args)?;
        let contents = self.read_command_inputs(cwd, &args[index..], stdin)?;
        let lines = String::from_utf8_lossy(&contents)
            .lines()
            .take(count)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        Ok(ExecutionResult::success(
            render_lines(&lines).into_bytes(),
            metadata,
        ))
    }

    fn run_tail(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let (count, index) = parse_line_count_flag("tail", &args)?;
        let lines =
            String::from_utf8_lossy(&self.read_command_inputs(cwd, &args[index..], stdin)?)
                .lines()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
        let start = lines.len().saturating_sub(count);
        Ok(ExecutionResult::success(
            render_lines(&lines[start..]).into_bytes(),
            metadata,
        ))
    }

    fn run_cut(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut delimiter = None;
        let mut fields = None;
        let mut index = 0usize;
        while let Some(flag) = args.get(index) {
            match flag.as_str() {
                "-d" => {
                    let Some(value) = args.get(index + 1) else {
                        return Err(SandboxError::InvalidRequest(
                            "cut -d requires a single-character delimiter".to_string(),
                        ));
                    };
                    delimiter = Some(parse_delimiter(value)?);
                    index += 2;
                }
                "-f" => {
                    let Some(value) = args.get(index + 1) else {
                        return Err(SandboxError::InvalidRequest(
                            "cut -f requires a field list".to_string(),
                        ));
                    };
                    fields = Some(parse_cut_fields(value)?);
                    index += 2;
                }
                _ if flag.starts_with('-') => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "cut flag is not supported: {flag}"
                    )));
                }
                _ => break,
            }
        }

        let delimiter = delimiter.ok_or_else(|| {
            SandboxError::InvalidRequest("cut requires -d <delimiter>".to_string())
        })?;
        let fields = fields
            .ok_or_else(|| SandboxError::InvalidRequest("cut requires -f <fields>".to_string()))?;
        let contents = self.read_command_inputs(cwd, &args[index..], stdin)?;
        let rendered = String::from_utf8_lossy(&contents)
            .lines()
            .map(|line| cut_line(line, delimiter, &fields))
            .collect::<Vec<_>>();
        Ok(ExecutionResult::success(
            render_lines(&rendered).into_bytes(),
            metadata,
        ))
    }

    fn run_tr(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut delete = false;
        let mut index = 0usize;
        while let Some(flag) = args.get(index) {
            match flag.as_str() {
                "-d" => {
                    delete = true;
                    index += 1;
                }
                _ if flag.starts_with('-') => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "tr flag is not supported: {flag}"
                    )));
                }
                _ => break,
            }
        }

        let source = args.get(index).ok_or_else(|| {
            SandboxError::InvalidRequest("tr requires a source character set".to_string())
        })?;
        let target = if delete {
            None
        } else {
            Some(args.get(index + 1).ok_or_else(|| {
                SandboxError::InvalidRequest(
                    "tr requires a destination character set when -d is not used".to_string(),
                )
            })?)
        };
        let remaining_index = index + if delete { 1 } else { 2 };
        let contents = self.read_command_inputs(cwd, &args[remaining_index..], stdin)?;

        let source_chars = parse_tr_charset(source)?;
        let rendered = if delete {
            translate_bytes(&contents, &source_chars, None)
        } else {
            let target_chars = parse_tr_charset(target.expect("target checked"))?;
            translate_bytes(&contents, &source_chars, Some(&target_chars))
        }?;

        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_paste(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut delimiter = '\t';
        let mut index = 0usize;
        while let Some(flag) = args.get(index) {
            match flag.as_str() {
                "-d" => {
                    let Some(value) = args.get(index + 1) else {
                        return Err(SandboxError::InvalidRequest(
                            "paste -d requires a single-character delimiter".to_string(),
                        ));
                    };
                    delimiter = parse_delimiter(value)?;
                    index += 2;
                }
                _ if flag.starts_with('-') => {
                    return Err(SandboxError::InvalidRequest(format!(
                        "paste flag is not supported: {flag}"
                    )));
                }
                _ => break,
            }
        }

        let columns = self.read_paste_columns(cwd, &args[index..], stdin)?;
        let row_count = columns.iter().map(Vec::len).max().unwrap_or(0);
        let mut lines = Vec::new();
        for row in 0..row_count {
            let fields = columns
                .iter()
                .map(|column| column.get(row).cloned().unwrap_or_default())
                .collect::<Vec<_>>();
            lines.push(fields.join(&delimiter.to_string()));
        }

        Ok(ExecutionResult::success(
            render_lines(&lines).into_bytes(),
            metadata,
        ))
    }

    fn run_sed(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let Some(script) = args.first() else {
            return Err(SandboxError::InvalidRequest(
                "sed requires a substitution script".to_string(),
            ));
        };
        let command = parse_sed_substitution(script)?;
        let contents = self.read_command_inputs(cwd, &args[1..], stdin)?;
        let input = String::from_utf8(contents).map_err(|_| {
            SandboxError::InvalidRequest("sed currently requires UTF-8 text input".to_string())
        })?;
        let rendered = input
            .lines()
            .map(|line| apply_sed_substitution(line, &command))
            .collect::<Vec<_>>();

        Ok(ExecutionResult::success(
            render_lines(&rendered).into_bytes(),
            metadata,
        ))
    }

    fn run_join(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let join_spec = parse_join_spec(&args)?;
        let left_rows = self.read_join_rows(
            cwd,
            &join_spec.left_path,
            join_spec.left_field,
            join_spec.delimiter,
        )?;
        let right_rows = self.read_join_rows(
            cwd,
            &join_spec.right_path,
            join_spec.right_field,
            join_spec.delimiter,
        )?;
        ensure_join_rows_sorted("join", &left_rows)?;
        ensure_join_rows_sorted("join", &right_rows)?;

        let mut right_by_key: BTreeMap<String, Vec<JoinRow>> = BTreeMap::new();
        for row in right_rows {
            right_by_key.entry(row.key.clone()).or_default().push(row);
        }

        let mut rendered = Vec::new();
        for left in left_rows {
            if let Some(matches) = right_by_key.get(&left.key) {
                for right in matches {
                    rendered.push(left.render(right));
                }
            }
        }

        Ok(ExecutionResult::success(
            render_lines(&rendered).into_bytes(),
            metadata,
        ))
    }

    fn run_awk(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = awk::execute(&args, stdin, |path| {
            let resolved = resolve_sandbox_path(cwd, path)?;
            self.read_path(&resolved)
        })?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_jq(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let result = jq::execute(&args, stdin, |path| {
            let resolved = resolve_sandbox_path(cwd, path)?;
            self.read_path(&resolved)
        })?;
        Ok(ExecutionResult {
            stdout: result.stdout,
            stderr: Vec::new(),
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_xan(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let result = xancmd::execute(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult {
            stdout: result.output,
            stderr: Vec::new(),
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_yq(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let result = yq::execute(&args, stdin, |path| {
            let resolved = resolve_sandbox_path(cwd, path)?;
            self.read_path(&resolved)
        })?;
        for writeback in result.writebacks {
            let resolved = resolve_sandbox_path(cwd, &writeback.path)?;
            self.filesystem
                .write_file(&resolved, writeback.contents, false)?;
        }
        Ok(ExecutionResult {
            stdout: result.stdout,
            stderr: Vec::new(),
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_find(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = find::execute(
            cwd,
            &args,
            || self.list_paths(),
            |path| self.path_is_dir(path),
        )?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_ls(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = ls::execute(
            cwd,
            &args,
            || self.list_paths(),
            |path| self.path_is_dir(path),
        )?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_tree(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_files::tree(
            cwd,
            &args,
            || self.list_paths(),
            |path| self.path_is_dir(path),
        )?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_stat(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let candidates = self.list_paths()?;
        let rendered = tier2_files::stat(
            cwd,
            &args,
            |path| self.read_path(path),
            |path| self.path_is_dir(path),
            |path| self.filesystem.get_mode_bits(path),
            |path| self.filesystem.read_link(path),
            &candidates,
        )?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_du(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let candidates = self.list_paths()?;
        let result = ducmd::execute(
            cwd,
            &args,
            |path| self.path_exists(path),
            |path| self.read_path(path),
            |path| self.path_is_dir(path),
            &candidates,
        )?;
        Ok(ExecutionResult {
            stdout: result.stdout,
            stderr: result.stderr,
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_html_to_markdown(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let result = htmltomarkdown::execute(cwd, &args, &stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult {
            stdout: result.stdout,
            stderr: result.stderr,
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_chmod(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = chmodcmd::parse(cwd, &args)?;
        let mut stdout = String::new();
        let mut stderr = String::new();
        let mut exit_code = 0;
        let mut candidates = self.list_paths()?;
        candidates.sort();

        for target in &spec.targets {
            if !self.path_exists(target)? {
                exit_code = 1;
                stderr.push_str(&format!(
                    "chmod: cannot access '{}': No such file or directory\n",
                    target
                ));
                continue;
            }

            let mut paths = vec![target.clone()];
            if spec.recursive && self.path_is_dir(target)? {
                paths.extend(chmodcmd::descendant_targets(target, &candidates));
            }

            for path in paths {
                let current = self.filesystem.get_mode_bits(&path)?;
                let next = chmodcmd::resolve_mode(&spec.mode, current)?;
                self.filesystem.chmod(&path, next)?;
                if spec.verbose {
                    stdout.push_str(&format!("mode of '{}' changed to {:04o}\n", path, next));
                }
            }
        }

        Ok(ExecutionResult {
            stdout: stdout.into_bytes(),
            stderr: stderr.into_bytes(),
            exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_python3(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        env: &BTreeMap<String, String>,
        timeout_ms: Option<u64>,
        cancel_flag: &AtomicBool,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        python3cmd::execute(
            &mut *self.filesystem,
            cwd,
            env,
            &stdin,
            &args,
            timeout_ms,
            cancel_flag,
            metadata,
        )
    }

    fn run_js_exec(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        env: &BTreeMap<String, String>,
        timeout_ms: Option<u64>,
        cancel_flag: &AtomicBool,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        jsexeccmd::execute(
            &mut *self.filesystem,
            cwd,
            env,
            &stdin,
            &args,
            timeout_ms,
            cancel_flag,
            metadata,
        )
    }

    fn run_file(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_files::file(
            cwd,
            &args,
            |path| self.read_path(path),
            |path| self.path_is_dir(path),
            |path| self.filesystem.read_link(path),
        )?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_readlink(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_files::readlink(cwd, &args, |path| self.filesystem.read_link(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_ln(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = match tier2_files::parse_ln(cwd, &args) {
            Ok(spec) => spec,
            Err(error) => return Ok(ln_error_result(error.to_string(), metadata)),
        };

        if self.path_exists(&spec.link_path)? {
            if spec.force {
                if let Err(error) = self.filesystem.delete_path(&spec.link_path, false) {
                    return Ok(ln_error_result(
                        format!("cannot remove '{}': {error}", spec.link_arg),
                        metadata,
                    ));
                }
            } else {
                return Ok(ln_error_result(
                    format!("failed to create link '{}': File exists", spec.link_arg),
                    metadata,
                ));
            }
        }

        let created = if spec.symbolic {
            self.filesystem
                .create_symlink(&spec.target_path, &spec.link_path)
        } else {
            self.filesystem
                .create_hard_link(&spec.target_path, &spec.link_path)
        };

        if let Err(error) = created {
            return Ok(ln_error_result(error.to_string(), metadata));
        }

        let stdout = if spec.verbose {
            format!("'{}' -> '{}'\n", spec.link_arg, spec.target_arg).into_bytes()
        } else {
            Vec::new()
        };

        Ok(ExecutionResult::success(stdout, metadata))
    }

    fn run_rev(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_text::rev(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_nl(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_text::nl(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_tac(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_text::tac(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_strings(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_text::strings(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_fold(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_text::fold(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_expand(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_text::expand(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_unexpand(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = tier2_text::unexpand(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_rm(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = rm::parse_spec(cwd, &args)?;
        for path in spec.paths {
            if !self.path_exists(&path)? {
                if spec.force {
                    continue;
                }
                return Err(SandboxError::InvalidRequest(format!(
                    "path does not exist: {path}"
                )));
            }

            if self.path_is_dir(&path)? && !spec.recursive {
                return Err(SandboxError::InvalidRequest(format!(
                    "cannot remove directory without -r: {path}"
                )));
            }

            self.filesystem.delete_path(&path, spec.recursive)?;
        }

        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_cp(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = cp::parse_spec(cwd, &args)?;
        let destination_exists = self.path_exists(&spec.destination)?;
        let destination_is_dir = destination_exists && self.path_is_dir(&spec.destination)?;

        if spec.sources.len() > 1 && !destination_is_dir {
            return Err(SandboxError::InvalidRequest(
                "cp with multiple sources requires an existing destination directory".to_string(),
            ));
        }

        let snapshot = self.list_paths()?;
        for source in &spec.sources {
            if !self.path_exists(source)? {
                return Err(SandboxError::InvalidRequest(format!(
                    "path does not exist: {source}"
                )));
            }

            if self.path_is_dir(source)? {
                if !spec.recursive {
                    return Err(SandboxError::InvalidRequest(format!(
                        "cannot copy directory without -r: {source}"
                    )));
                }
                let target_root = if destination_is_dir {
                    cp::join_path(&spec.destination, cp::path_basename(source)?)
                } else {
                    if spec.sources.len() > 1 {
                        return Err(SandboxError::InvalidRequest(
                            "cp with multiple sources requires an existing destination directory"
                                .to_string(),
                        ));
                    }
                    if destination_exists && !destination_is_dir {
                        return Err(SandboxError::InvalidRequest(format!(
                            "cannot overwrite non-directory with directory: {}",
                            spec.destination
                        )));
                    }
                    spec.destination.clone()
                };
                self.filesystem.mkdir(&target_root, false)?;

                for descendant in cp::descendant_paths(source, &snapshot) {
                    let suffix = descendant
                        .strip_prefix(&(source.to_string() + "/"))
                        .expect("descendant prefix checked");
                    let target = cp::join_path(&target_root, suffix);
                    if self.path_is_dir(&descendant)? {
                        self.filesystem.mkdir(&target, true)?;
                    } else {
                        let contents = self.read_path(&descendant)?;
                        self.filesystem.write_file(&target, contents, true)?;
                    }
                }
            } else {
                let target = if destination_is_dir {
                    cp::join_path(&spec.destination, cp::path_basename(source)?)
                } else {
                    spec.destination.clone()
                };
                let contents = self.read_path(source)?;
                self.filesystem.write_file(&target, contents, false)?;
            }
        }

        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_mv(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = mv::parse_spec(cwd, &args)?;
        let destination_exists = self.path_exists(&spec.destination)?;
        let destination_is_dir = destination_exists && self.path_is_dir(&spec.destination)?;

        if spec.sources.len() > 1 && !destination_is_dir {
            return Err(SandboxError::InvalidRequest(
                "mv with multiple sources requires an existing destination directory".to_string(),
            ));
        }

        let snapshot = self.list_paths()?;
        for source in &spec.sources {
            if !self.path_exists(source)? {
                return Err(SandboxError::InvalidRequest(format!(
                    "path does not exist: {source}"
                )));
            }

            let source_is_dir = self.path_is_dir(source)?;
            let target = if destination_is_dir {
                cp::join_path(&spec.destination, cp::path_basename(source)?)
            } else {
                spec.destination.clone()
            };

            if source == &target {
                continue;
            }

            if source_is_dir && mv::path_within_root(&target, source) {
                return Err(SandboxError::InvalidRequest(format!(
                    "cannot move a directory into itself: {source}"
                )));
            }

            if source_is_dir {
                let target_exists = self.path_exists(&target)?;
                if target_exists && !self.path_is_dir(&target)? {
                    return Err(SandboxError::InvalidRequest(format!(
                        "cannot overwrite non-directory with directory: {target}"
                    )));
                }
                if !target_exists {
                    self.filesystem.mkdir(&target, false)?;
                }

                for descendant in cp::descendant_paths(source, &snapshot) {
                    let suffix = descendant
                        .strip_prefix(&(source.to_string() + "/"))
                        .expect("descendant prefix checked");
                    let destination_path = cp::join_path(&target, suffix);
                    if self.path_is_dir(&descendant)? {
                        self.filesystem.mkdir(&destination_path, true)?;
                    } else {
                        let contents = self.read_path(&descendant)?;
                        self.filesystem
                            .write_file(&destination_path, contents, true)?;
                    }
                }
                self.filesystem.delete_path(source, true)?;
            } else {
                if self.path_exists(&target)? && self.path_is_dir(&target)? {
                    return Err(SandboxError::InvalidRequest(format!(
                        "cannot overwrite directory with file: {target}"
                    )));
                }
                let contents = self.read_path(source)?;
                self.filesystem.write_file(&target, contents, false)?;
                self.filesystem.delete_path(source, false)?;
            }
        }

        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_tee(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = tee::parse_spec(cwd, &args)?;
        for path in spec.paths {
            let contents = if spec.append {
                let mut existing = match self.read_path(&path) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == abash_core::ErrorKind::InvalidRequest => {
                        Vec::new()
                    }
                    Err(error) => return Err(error),
                };
                existing.extend_from_slice(&stdin);
                existing
            } else {
                stdin.clone()
            };
            self.filesystem.write_file(&path, contents, false)?;
        }

        Ok(ExecutionResult::success(stdin, metadata))
    }

    fn run_printf(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(printf::execute(&args)?, metadata))
    }

    fn run_seq(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(seq::execute(&args)?, metadata))
    }

    fn run_date(
        &mut self,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        Ok(ExecutionResult::success(date::execute(&args)?, metadata))
    }

    fn run_sqlite3(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        match sqlite3cmd::parse(&args, &stdin)? {
            sqlite3cmd::CommandOutcome::Cli {
                stdout,
                stderr,
                exit_code,
            } => Ok(ExecutionResult {
                stdout,
                stderr,
                exit_code,
                termination_reason: TerminationReason::Exited,
                error: None,
                metadata,
            }),
            sqlite3cmd::CommandOutcome::Script(plan) => {
                let existing_db = if plan.database == ":memory:" {
                    None
                } else {
                    let resolved = resolve_sandbox_path(cwd, &plan.database)?;
                    if self.path_exists(&resolved)? {
                        Some(self.read_path(&resolved)?)
                    } else {
                        None
                    }
                };
                let result = sqlite3cmd::execute(&plan, existing_db)?;
                if let Some(writeback) = result.writeback {
                    let resolved = resolve_sandbox_path(cwd, &plan.database)?;
                    self.filesystem.write_file(&resolved, writeback, true)?;
                }
                Ok(ExecutionResult {
                    stdout: result.stdout,
                    stderr: result.stderr,
                    exit_code: result.exit_code,
                    termination_reason: TerminationReason::Exited,
                    error: None,
                    metadata,
                })
            }
        }
    }

    fn run_gzip(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = gzipcmd::parse(&args)?;
        if spec.paths.is_empty() || spec.paths.iter().any(|path| path == "-") {
            let transformed = if spec.decompress {
                gzipcmd::decompress_bytes(&stdin)?
            } else {
                gzipcmd::compress_bytes(&stdin)?
            };
            return Ok(ExecutionResult::success(transformed, metadata));
        }

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code = 0;

        for path in &spec.paths {
            let resolved = resolve_sandbox_path(cwd, path)?;
            let input = self.read_path(&resolved)?;
            let transformed = if spec.decompress {
                gzipcmd::decompress_bytes(&input)?
            } else {
                gzipcmd::compress_bytes(&input)?
            };

            if spec.stdout {
                stdout.extend_from_slice(&transformed);
                continue;
            }

            let target = if spec.decompress {
                gzipcmd::decompressed_path(path, &spec.suffix)?
            } else {
                gzipcmd::compressed_path(path, &spec.suffix)?
            };
            let resolved_target = resolve_sandbox_path(cwd, &target)?;
            if self.path_exists(&resolved_target)? && !spec.force {
                exit_code = 1;
                stderr.extend_from_slice(
                    format!("gzip: target already exists: {target}\n").as_bytes(),
                );
                continue;
            }

            self.filesystem
                .write_file(&resolved_target, transformed, true)?;
            if !spec.keep {
                self.filesystem.delete_path(&resolved, false)?;
            }
        }

        Ok(ExecutionResult {
            stdout,
            stderr,
            exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_gunzip(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut forwarded = vec!["-d".to_string()];
        forwarded.extend(args);
        self.run_gzip(cwd, forwarded, stdin, metadata)
    }

    fn run_zcat(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let mut forwarded = vec!["-d".to_string(), "-c".to_string()];
        forwarded.extend(args);
        self.run_gzip(cwd, forwarded, stdin, metadata)
    }

    fn run_tar(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = tarcmd::parse(cwd, &args)?;
        let rendered = tarcmd::execute(&mut *self.filesystem, cwd, &spec, &stdin)?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_mkdir(
        &mut self,
        cwd: &str,
        mut args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        if args.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "mkdir requires at least one path".to_string(),
            ));
        }
        let parents = if args.first().is_some_and(|arg| arg == "-p") {
            args.remove(0);
            true
        } else {
            false
        };
        if args.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "mkdir requires at least one path".to_string(),
            ));
        }

        for path in args {
            let resolved = resolve_sandbox_path(cwd, &path)?;
            self.filesystem.mkdir(&resolved, parents)?;
        }
        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_rmdir(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = rmdir::parse_spec(cwd, &args)?;
        for path in spec.paths {
            if !self.path_exists(&path)? {
                return Err(SandboxError::InvalidRequest(format!(
                    "path does not exist: {path}"
                )));
            }
            if !self.path_is_dir(&path)? {
                return Err(SandboxError::InvalidRequest(format!(
                    "rmdir requires a directory path: {path}"
                )));
            }
            if !directory_is_empty(&path, &self.list_paths()?) {
                return Err(SandboxError::InvalidRequest(format!(
                    "directory is not empty: {path}"
                )));
            }
            self.filesystem.delete_path(&path, true)?;

            if spec.parents {
                let mut current = rmdir::parent_path(&path);
                while let Some(parent) = current {
                    if !self.path_exists(&parent)? || !self.path_is_dir(&parent)? {
                        break;
                    }
                    if !directory_is_empty(&parent, &self.list_paths()?) {
                        break;
                    }
                    if self.filesystem.delete_path(&parent, true).is_err() {
                        break;
                    }
                    current = rmdir::parent_path(&parent);
                }
            }
        }
        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_touch(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        if args.is_empty() {
            return Err(SandboxError::InvalidRequest(
                "touch requires at least one path".to_string(),
            ));
        }
        for path in args {
            let resolved = resolve_sandbox_path(cwd, &path)?;
            if !self.path_exists(&resolved)? {
                self.filesystem.write_file(&resolved, Vec::new(), false)?;
            }
        }
        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_comm(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = comm::execute(cwd, &args, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_diff(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let diff = diffcmd::execute(cwd, &args, |path| self.read_path(path))?;
        Ok(ExecutionResult {
            stdout: diff.output,
            stderr: Vec::new(),
            exit_code: if diff.identical { 0 } else { 1 },
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn run_column(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = column::execute(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    #[allow(clippy::too_many_arguments)]
    fn run_xargs(
        &mut self,
        runtime: &mut RuntimeState,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        config: &SandboxConfig,
        cancel_flag: &AtomicBool,
        extensions: Option<&dyn SandboxExtensions>,
        timeout_ms: Option<u64>,
        network_enabled: bool,
        _env: BTreeMap<String, String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = xargs::parse_spec(&args)?;
        let tokens = xargs::tokenize_input(&stdin)?;
        let invocations = xargs::build_invocations(&spec, &tokens);
        let started = Instant::now();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut last_result = None;

        for invocation_args in invocations {
            let mut child = runtime.child();
            let child_env = child.env.clone();
            let mut result = self.execute_command(
                &mut child,
                cwd,
                invocation_args,
                Vec::new(),
                spec.command.clone(),
                config,
                cancel_flag,
                extensions,
                remaining_timeout_ms(timeout_ms, started)?,
                network_enabled,
                child_env,
                metadata.clone(),
            )?;
            stdout.extend(result.stdout.clone());
            stderr.extend(result.stderr.clone());

            if result.error.is_some() || result.exit_code != 0 {
                result.stdout = stdout;
                result.stderr = stderr;
                return Ok(result);
            }

            last_result = Some(result);
        }

        let mut result =
            last_result.unwrap_or_else(|| ExecutionResult::success(Vec::new(), metadata));
        result.stdout = stdout;
        result.stderr = stderr;
        Ok(result)
    }

    fn run_split(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let spec = splitcmd::parse_spec(cwd, &args)?;
        let input = if let Some(path) = spec.input {
            self.read_path(&path)?
        } else {
            stdin
        };
        let text = String::from_utf8(input).map_err(|_| {
            SandboxError::InvalidRequest("split currently requires UTF-8 text input".to_string())
        })?;
        let lines = text.lines().map(ToString::to_string).collect::<Vec<_>>();
        for (index, chunk) in lines.chunks(spec.line_count).enumerate() {
            let path = format!("{}{}", spec.prefix, splitcmd::suffix_for(index)?);
            self.filesystem.write_file(
                &path,
                format!("{}\n", chunk.join("\n")).into_bytes(),
                false,
            )?;
        }
        Ok(ExecutionResult::success(Vec::new(), metadata))
    }

    fn run_od(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = odcmd::execute(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_base64(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = base64cmd::execute(cwd, &args, stdin, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_hash(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        kind: hashcmd::HashKind,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let rendered = hashcmd::execute(cwd, &args, stdin, kind, |path| self.read_path(path))?;
        Ok(ExecutionResult::success(rendered, metadata))
    }

    fn run_rg(
        &mut self,
        cwd: &str,
        args: Vec<String>,
        stdin: Vec<u8>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ExecutionResult, SandboxError> {
        let result = rg::execute(
            cwd,
            &args,
            stdin,
            |path| self.read_path(path),
            || self.list_paths(),
            |path| self.path_is_dir(path),
        )?;
        Ok(ExecutionResult {
            stdout: result.output,
            stderr: Vec::new(),
            exit_code: result.exit_code,
            termination_reason: TerminationReason::Exited,
            error: None,
            metadata,
        })
    }

    fn read_command_inputs(
        &mut self,
        cwd: &str,
        args: &[String],
        stdin: Vec<u8>,
    ) -> Result<Vec<u8>, SandboxError> {
        if args.is_empty() {
            return Ok(stdin);
        }

        let mut output = Vec::new();
        for path in args {
            let resolved = resolve_sandbox_path(cwd, path)?;
            output.extend(self.read_path(&resolved)?);
        }
        Ok(output)
    }

    fn read_paste_columns(
        &mut self,
        cwd: &str,
        args: &[String],
        stdin: Vec<u8>,
    ) -> Result<Vec<Vec<String>>, SandboxError> {
        if args.is_empty() {
            return Ok(vec![String::from_utf8(stdin)
                .map_err(|_| {
                    SandboxError::InvalidRequest(
                        "paste currently requires UTF-8 text input".to_string(),
                    )
                })?
                .lines()
                .map(ToString::to_string)
                .collect::<Vec<_>>()]);
        }

        let mut columns = Vec::new();
        for path in args {
            let resolved = resolve_sandbox_path(cwd, path)?;
            let contents = self.read_path(&resolved)?;
            columns.push(
                String::from_utf8(contents)
                    .map_err(|_| {
                        SandboxError::InvalidRequest(
                            "paste currently requires UTF-8 text input".to_string(),
                        )
                    })?
                    .lines()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            );
        }
        Ok(columns)
    }

    fn read_join_rows(
        &mut self,
        cwd: &str,
        path: &str,
        join_field: usize,
        delimiter: Option<char>,
    ) -> Result<Vec<JoinRow>, SandboxError> {
        let resolved = resolve_sandbox_path(cwd, path)?;
        let contents = self.read_path(&resolved)?;
        let text = String::from_utf8(contents).map_err(|_| {
            SandboxError::InvalidRequest("join currently requires UTF-8 text input".to_string())
        })?;

        let mut rows = Vec::new();
        for line in text.lines() {
            let fields = parse_join_fields(line, delimiter);
            if fields.len() <= join_field {
                return Err(SandboxError::InvalidRequest(format!(
                    "join field {} is missing in input line: {line}",
                    join_field + 1
                )));
            }
            rows.push(JoinRow {
                key: fields[join_field].clone(),
                fields,
                join_field,
                delimiter,
            });
        }
        Ok(rows)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileTarget {
    path: String,
    append: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StreamTarget {
    StdoutCapture,
    StderrCapture,
    File(FileTarget),
}

#[derive(Default)]
struct RoutedOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct PendingFileWrite {
    target: FileTarget,
    contents: Vec<u8>,
}

#[derive(Default)]
struct ScriptState {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    last_result: Option<ExecutionResult>,
    executed_steps: usize,
}

fn parent_directory_chain(path: &str) -> Vec<String> {
    let mut current = path.to_string();
    let mut parents = Vec::new();
    while let Some(index) = current.rfind('/') {
        if index == 0 {
            break;
        }
        current.truncate(index);
        parents.push(current.clone());
    }
    parents
}

#[derive(Clone, Debug)]
struct RuntimeState {
    cwd: String,
    persisted_cwd: String,
    env: BTreeMap<String, String>,
    exported_env: BTreeMap<String, String>,
    aliases: BTreeMap<String, Vec<String>>,
    positional_args: Vec<String>,
    functions: BTreeMap<String, Vec<ScriptStep>>,
    function_depth: usize,
}

impl RuntimeState {
    fn new(
        cwd: String,
        persisted_cwd: String,
        exported_env: BTreeMap<String, String>,
        request_env: BTreeMap<String, String>,
        replace_env: bool,
        aliases: BTreeMap<String, Vec<String>>,
        positional_args: Vec<String>,
    ) -> Self {
        let env = if replace_env {
            request_env
        } else {
            let mut env = exported_env.clone();
            env.extend(request_env);
            env
        };
        Self {
            cwd,
            persisted_cwd,
            env,
            exported_env,
            aliases,
            positional_args,
            functions: BTreeMap::new(),
            function_depth: 0,
        }
    }

    fn child(&self) -> Self {
        self.clone()
    }
}

impl ScriptState {
    fn merge(&mut self, other: ScriptState) {
        self.stdout.extend(other.stdout);
        self.stderr.extend(other.stderr);
        self.executed_steps += other.executed_steps;
        self.last_result = other.last_result;
    }
}

fn should_run_step(op: Option<&ChainOp>, last_result: Option<&ExecutionResult>) -> bool {
    match (op, last_result) {
        (None, _) => true,
        (Some(ChainOp::Seq), _) => true,
        (Some(ChainOp::AndIf), Some(result)) => result.exit_code == 0,
        (Some(ChainOp::OrIf), Some(result)) => result.exit_code != 0,
        (Some(ChainOp::AndIf | ChainOp::OrIf), None) => true,
    }
}

fn ln_error_result(message: String, metadata: BTreeMap<String, String>) -> ExecutionResult {
    ExecutionResult {
        stdout: Vec::new(),
        stderr: format!("ln: {message}\n").into_bytes(),
        exit_code: 1,
        termination_reason: TerminationReason::Exited,
        error: None,
        metadata,
    }
}

fn remaining_timeout_ms(
    timeout_ms: Option<u64>,
    started: Instant,
) -> Result<Option<u64>, SandboxError> {
    let Some(limit_ms) = timeout_ms else {
        return Ok(None);
    };
    let elapsed_ms = started.elapsed().as_millis() as u64;
    if elapsed_ms >= limit_ms {
        return Err(SandboxError::Timeout(
            "execution exceeded the configured timeout".to_string(),
        ));
    }
    Ok(Some(limit_ms - elapsed_ms))
}

fn validate_pipeline_redirections(
    pipeline: &Pipeline,
    index: usize,
    command: &SimpleCommand,
) -> Result<(), SandboxError> {
    if input_redirect(command).is_some() && index > 0 {
        return Err(SandboxError::InvalidRequest(
            "input redirection is supported only for the first command in a pipeline".to_string(),
        ));
    }
    if has_stdout_redirect(command) && index + 1 != pipeline.commands.len() {
        return Err(SandboxError::InvalidRequest(
            "output redirection is supported only for the last command in a pipeline".to_string(),
        ));
    }
    Ok(())
}

fn input_redirect(command: &SimpleCommand) -> Option<&ScriptWord> {
    command
        .redirects
        .iter()
        .rev()
        .find_map(|redirect| match redirect {
            RedirectSpec::Input(path) => Some(path),
            _ => None,
        })
}

fn has_stdout_redirect(command: &SimpleCommand) -> bool {
    command.redirects.iter().any(|redirect| {
        matches!(
            redirect,
            RedirectSpec::StdoutTruncate(_) | RedirectSpec::StdoutAppend(_)
        )
    })
}

fn resolve_file_target(
    cwd: &str,
    path: &ScriptWord,
    env: &BTreeMap<String, String>,
    positional_args: &[String],
    append: bool,
) -> Result<FileTarget, SandboxError> {
    Ok(FileTarget {
        path: resolve_sandbox_path(cwd, &path.expand(env, positional_args)?)?,
        append,
    })
}

fn route_stream(
    routed: &mut RoutedOutput,
    pending_writes: &mut Vec<PendingFileWrite>,
    target: &StreamTarget,
    contents: &[u8],
) {
    if contents.is_empty() {
        return;
    }

    match target {
        StreamTarget::StdoutCapture => routed.stdout.extend_from_slice(contents),
        StreamTarget::StderrCapture => routed.stderr.extend_from_slice(contents),
        StreamTarget::File(file) => {
            if let Some(existing) = pending_writes
                .iter_mut()
                .find(|write| write.target == *file)
            {
                existing.contents.extend_from_slice(contents);
                return;
            }
            pending_writes.push(PendingFileWrite {
                target: file.clone(),
                contents: contents.to_vec(),
            });
        }
    }
}

fn expand_command_env(
    assignments: &[(String, ScriptWord)],
    base_env: &BTreeMap<String, String>,
    positional_args: &[String],
) -> Result<BTreeMap<String, String>, SandboxError> {
    let mut env = base_env.clone();
    for (name, value) in assignments {
        env.insert(name.clone(), value.expand(&env, positional_args)?);
    }
    Ok(env)
}

fn expand_words(
    words: &[ScriptWord],
    env: &BTreeMap<String, String>,
    positional_args: &[String],
) -> Result<Vec<String>, SandboxError> {
    let mut expanded = Vec::new();
    for word in words {
        if word.expands_to_positional_args() {
            expanded.extend(positional_args.iter().cloned());
        } else {
            expanded.push(word.expand(env, positional_args)?);
        }
    }
    Ok(expanded)
}

fn resolve_alias_words(
    words: &[String],
    aliases: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<String>, SandboxError> {
    if words.is_empty() {
        return Ok(Vec::new());
    }

    let mut expanded = words.to_vec();
    for _ in 0..10 {
        let Some(alias) = aliases.get(&expanded[0]) else {
            return Ok(expanded);
        };
        if alias.is_empty() {
            return Ok(expanded);
        }
        let mut next = alias.clone();
        next.extend(expanded.iter().skip(1).cloned());
        expanded = next;
    }

    Err(SandboxError::InvalidRequest(
        "alias expansion exceeded the recursion limit".to_string(),
    ))
}

fn split_nested_command(
    wrapper: &str,
    args: &[String],
) -> Result<(String, Vec<String>), SandboxError> {
    let Some(command) = args.first().cloned() else {
        return Err(SandboxError::InvalidRequest(format!(
            "{wrapper} requires a nested command"
        )));
    };
    Ok((command, args.iter().skip(1).cloned().collect::<Vec<_>>()))
}

fn resolve_remote_addrs(url: &Url) -> Result<Vec<IpAddr>, SandboxError> {
    let host = url
        .host_str()
        .ok_or_else(|| SandboxError::InvalidRequest("curl URL must include a host".to_string()))?;
    let port = url.port_or_known_default().ok_or_else(|| {
        SandboxError::InvalidRequest("curl URL must include an effective port".to_string())
    })?;
    let resolved = format!("{host}:{port}")
        .to_socket_addrs()
        .map_err(|error| {
            SandboxError::BackendFailure(format!("curl could not resolve the remote host: {error}"))
        })?
        .map(|addr| addr.ip())
        .collect::<Vec<_>>();
    if resolved.is_empty() {
        return Err(SandboxError::BackendFailure(
            "curl could not resolve the remote host".to_string(),
        ));
    }
    Ok(resolved)
}

fn map_ureq_error(error: ureq::Error) -> SandboxError {
    match error {
        ureq::Error::Timeout(_) => {
            SandboxError::Timeout("curl request exceeded the configured timeout".to_string())
        }
        ureq::Error::BodyExceedsLimit(_) => SandboxError::BackendFailure(
            "curl response exceeded the configured size limit".to_string(),
        ),
        ureq::Error::HostNotFound => {
            SandboxError::BackendFailure("curl could not resolve the remote host".to_string())
        }
        other => SandboxError::BackendFailure(format!("curl request failed: {other}")),
    }
}

fn is_redirect_status(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

fn contains_glob_pattern(value: &str) -> bool {
    value.contains('*') || value.contains('?') || value.contains('[')
}

fn format_glob_match(cwd: &str, original: &str, candidate: &str) -> String {
    if original.starts_with('/') {
        return candidate.to_string();
    }
    if cwd == "/" {
        return candidate.trim_start_matches('/').to_string();
    }
    candidate
        .strip_prefix(&(cwd.to_string() + "/"))
        .unwrap_or(candidate)
        .to_string()
}

fn glob_matches_path(pattern: &str, candidate: &str) -> bool {
    let pattern_segments = path_segments(pattern);
    let candidate_segments = path_segments(candidate);
    if pattern_segments.len() != candidate_segments.len() {
        return false;
    }
    pattern_segments.iter().zip(candidate_segments.iter()).all(
        |(pattern_segment, candidate_segment)| {
            glob_matches_segment(pattern_segment, candidate_segment)
        },
    )
}

fn path_segments(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn glob_matches_segment(pattern: &str, text: &str) -> bool {
    glob_matches_chars(
        &pattern.chars().collect::<Vec<_>>(),
        0,
        &text.chars().collect::<Vec<_>>(),
        0,
    )
}

fn glob_matches_chars(
    pattern: &[char],
    pattern_index: usize,
    text: &[char],
    text_index: usize,
) -> bool {
    if pattern_index == pattern.len() {
        return text_index == text.len();
    }

    match pattern[pattern_index] {
        '*' => {
            for index in text_index..=text.len() {
                if glob_matches_chars(pattern, pattern_index + 1, text, index) {
                    return true;
                }
            }
            false
        }
        '?' => {
            if text_index == text.len() {
                false
            } else {
                glob_matches_chars(pattern, pattern_index + 1, text, text_index + 1)
            }
        }
        '[' => match parse_bracket_class(pattern, pattern_index) {
            Some((matcher, next_index)) => {
                if text_index == text.len() || !matcher.matches(text[text_index]) {
                    false
                } else {
                    glob_matches_chars(pattern, next_index, text, text_index + 1)
                }
            }
            None => {
                text_index < text.len()
                    && pattern[pattern_index] == text[text_index]
                    && glob_matches_chars(pattern, pattern_index + 1, text, text_index + 1)
            }
        },
        literal => {
            text_index < text.len()
                && literal == text[text_index]
                && glob_matches_chars(pattern, pattern_index + 1, text, text_index + 1)
        }
    }
}

struct BracketMatcher {
    negated: bool,
    ranges: Vec<(char, char)>,
    literals: Vec<char>,
}

impl BracketMatcher {
    fn matches(&self, value: char) -> bool {
        let direct = self.literals.contains(&value)
            || self
                .ranges
                .iter()
                .any(|(start, end)| *start <= value && value <= *end);
        if self.negated {
            !direct
        } else {
            direct
        }
    }
}

fn parse_bracket_class(pattern: &[char], start_index: usize) -> Option<(BracketMatcher, usize)> {
    let mut index = start_index + 1;
    if index >= pattern.len() {
        return None;
    }
    let negated = matches!(pattern.get(index), Some('!' | '^'));
    if negated {
        index += 1;
    }
    let mut literals = Vec::new();
    let mut ranges = Vec::new();
    let mut closed = false;

    while index < pattern.len() {
        if pattern[index] == ']' && (!literals.is_empty() || !ranges.is_empty()) {
            closed = true;
            index += 1;
            break;
        }
        if index + 2 < pattern.len() && pattern[index + 1] == '-' && pattern[index + 2] != ']' {
            ranges.push((pattern[index], pattern[index + 2]));
            index += 3;
            continue;
        }
        literals.push(pattern[index]);
        index += 1;
    }

    if !closed {
        return None;
    }

    Some((
        BracketMatcher {
            negated,
            ranges,
            literals,
        },
        index,
    ))
}

fn decorate_script_metadata(
    mut metadata: BTreeMap<String, String>,
    cwd: &str,
    commands_executed: usize,
    last_command: Option<String>,
) -> BTreeMap<String, String> {
    metadata.insert("mode".to_string(), "script".to_string());
    metadata.insert("cwd".to_string(), cwd.to_string());
    metadata.insert(
        "commands_executed".to_string(),
        commands_executed.to_string(),
    );
    if let Some(command) = last_command {
        metadata.insert("last_command".to_string(), command);
    }
    metadata
}

fn render_history_entry(request: &ExecutionRequest) -> String {
    match request.mode {
        ExecutionMode::Argv => request.argv.join(" "),
        ExecutionMode::Script => request.script.clone().unwrap_or_default(),
    }
}

fn render_env(env: &BTreeMap<String, String>, args: &[String]) -> String {
    if args.is_empty() {
        let mut lines = env
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        lines.sort();
        if lines.is_empty() {
            String::new()
        } else {
            format!("{}\n", lines.join("\n"))
        }
    } else {
        let mut values = Vec::new();
        for key in args {
            if let Some(value) = env.get(key) {
                values.push(value.clone());
            }
        }
        if values.is_empty() {
            String::new()
        } else {
            format!("{}\n", values.join("\n"))
        }
    }
}

fn render_lines(lines: &[String]) -> String {
    if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    }
}

fn directory_is_empty(path: &str, candidates: &[String]) -> bool {
    let prefix = if path == "/" {
        "/".to_string()
    } else {
        format!("{path}/")
    };
    !candidates
        .iter()
        .any(|candidate| candidate.starts_with(&prefix))
}

fn parse_line_count_flag(command: &str, args: &[String]) -> Result<(usize, usize), SandboxError> {
    let Some(flag) = args.first() else {
        return Ok((10, 0));
    };
    if flag == "-n" {
        let Some(value) = args.get(1) else {
            return Err(SandboxError::InvalidRequest(format!(
                "{command} -n requires a positive integer"
            )));
        };
        let count = value.parse::<usize>().map_err(|_| {
            SandboxError::InvalidRequest(format!("{command} -n requires a positive integer"))
        })?;
        return Ok((count, 2));
    }
    if flag.starts_with('-') {
        return Err(SandboxError::InvalidRequest(format!(
            "{command} flag is not supported: {flag}"
        )));
    }
    Ok((10, 0))
}

fn parse_delimiter(value: &str) -> Result<char, SandboxError> {
    let mut chars = value.chars();
    let Some(delimiter) = chars.next() else {
        return Err(SandboxError::InvalidRequest(
            "cut delimiter must be a single character".to_string(),
        ));
    };
    if chars.next().is_some() {
        return Err(SandboxError::InvalidRequest(
            "cut delimiter must be a single character".to_string(),
        ));
    }
    Ok(delimiter)
}

fn parse_cut_fields(value: &str) -> Result<Vec<usize>, SandboxError> {
    let mut fields = Vec::new();
    for part in value.split(',') {
        let field = part.parse::<usize>().map_err(|_| {
            SandboxError::InvalidRequest("cut fields must be comma-separated integers".to_string())
        })?;
        if field == 0 {
            return Err(SandboxError::InvalidRequest(
                "cut fields are 1-based and must be positive".to_string(),
            ));
        }
        fields.push(field);
    }
    if fields.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "cut requires at least one field".to_string(),
        ));
    }
    Ok(fields)
}

fn cut_line(line: &str, delimiter: char, fields: &[usize]) -> String {
    let parts = line.split(delimiter).collect::<Vec<_>>();
    fields
        .iter()
        .filter_map(|field| parts.get(field - 1).copied())
        .collect::<Vec<_>>()
        .join(&delimiter.to_string())
}

fn parse_tr_charset(value: &str) -> Result<Vec<char>, SandboxError> {
    if value.is_empty() {
        return Err(SandboxError::InvalidRequest(
            "tr character sets must not be empty".to_string(),
        ));
    }
    if value.contains('[') || value.contains(']') || value.contains('-') {
        return Err(SandboxError::InvalidRequest(
            "tr ranges and character classes are not supported".to_string(),
        ));
    }
    Ok(value.chars().collect())
}

fn translate_bytes(
    contents: &[u8],
    source: &[char],
    target: Option<&[char]>,
) -> Result<Vec<u8>, SandboxError> {
    let text = String::from_utf8(contents.to_vec()).map_err(|_| {
        SandboxError::InvalidRequest("tr currently requires UTF-8 text input".to_string())
    })?;

    let mut output = String::new();
    match target {
        None => {
            for ch in text.chars() {
                if !source.contains(&ch) {
                    output.push(ch);
                }
            }
        }
        Some(target_chars) => {
            if source.len() != target_chars.len() {
                return Err(SandboxError::InvalidRequest(
                    "tr source and destination sets must be the same length".to_string(),
                ));
            }
            for ch in text.chars() {
                if let Some(index) = source.iter().position(|candidate| *candidate == ch) {
                    output.push(target_chars[index]);
                } else {
                    output.push(ch);
                }
            }
        }
    }

    Ok(output.into_bytes())
}

struct SedSubstitution {
    pattern: String,
    replacement: String,
    global: bool,
}

struct JoinSpec {
    delimiter: Option<char>,
    left_field: usize,
    right_field: usize,
    left_path: String,
    right_path: String,
}

#[derive(Clone)]
struct JoinRow {
    key: String,
    fields: Vec<String>,
    join_field: usize,
    delimiter: Option<char>,
}

impl JoinRow {
    fn render(&self, other: &JoinRow) -> String {
        let mut fields = vec![self.key.clone()];
        fields.extend(
            self.fields
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != self.join_field)
                .map(|(_, field)| field.clone()),
        );
        fields.extend(
            other
                .fields
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != other.join_field)
                .map(|(_, field)| field.clone()),
        );

        match self.delimiter {
            Some(delimiter) => fields.join(&delimiter.to_string()),
            None => fields.join(" "),
        }
    }
}

fn parse_sed_substitution(script: &str) -> Result<SedSubstitution, SandboxError> {
    let mut chars = script.chars();
    if chars.next() != Some('s') {
        return Err(SandboxError::InvalidRequest(
            "sed currently supports only substitution scripts".to_string(),
        ));
    }
    let delimiter = chars.next().ok_or_else(|| {
        SandboxError::InvalidRequest("sed substitution is missing a delimiter".to_string())
    })?;
    let remainder = chars.collect::<String>();
    let parts = remainder.split(delimiter).collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(SandboxError::InvalidRequest(
            "sed substitution must be in the form s/old/new/".to_string(),
        ));
    }
    let flags = parts[2];
    if !flags.is_empty() && flags != "g" {
        return Err(SandboxError::InvalidRequest(
            "sed supports only the optional g flag".to_string(),
        ));
    }
    Ok(SedSubstitution {
        pattern: parts[0].to_string(),
        replacement: parts[1].to_string(),
        global: flags == "g",
    })
}

fn apply_sed_substitution(line: &str, command: &SedSubstitution) -> String {
    if command.pattern.is_empty() {
        return line.to_string();
    }
    if command.global {
        line.replace(&command.pattern, &command.replacement)
    } else {
        line.replacen(&command.pattern, &command.replacement, 1)
    }
}

fn parse_join_spec(args: &[String]) -> Result<JoinSpec, SandboxError> {
    let mut delimiter = None;
    let mut left_field = 1usize;
    let mut right_field = 1usize;
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        match flag.as_str() {
            "-t" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "join -t requires a single-character delimiter".to_string(),
                    ));
                };
                delimiter = Some(parse_join_delimiter(value)?);
                index += 2;
            }
            "-1" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "join -1 requires a positive field number".to_string(),
                    ));
                };
                left_field = parse_join_field_number(value, "-1")?;
                index += 2;
            }
            "-2" => {
                let Some(value) = args.get(index + 1) else {
                    return Err(SandboxError::InvalidRequest(
                        "join -2 requires a positive field number".to_string(),
                    ));
                };
                right_field = parse_join_field_number(value, "-2")?;
                index += 2;
            }
            _ if flag.starts_with('-') => {
                return Err(SandboxError::InvalidRequest(format!(
                    "join flag is not supported: {flag}"
                )));
            }
            _ => break,
        }
    }

    let Some(left_path) = args.get(index) else {
        return Err(SandboxError::InvalidRequest(
            "join requires exactly two input files".to_string(),
        ));
    };
    let Some(right_path) = args.get(index + 1) else {
        return Err(SandboxError::InvalidRequest(
            "join requires exactly two input files".to_string(),
        ));
    };
    if args.get(index + 2).is_some() {
        return Err(SandboxError::InvalidRequest(
            "join requires exactly two input files".to_string(),
        ));
    }

    Ok(JoinSpec {
        delimiter,
        left_field: left_field - 1,
        right_field: right_field - 1,
        left_path: left_path.clone(),
        right_path: right_path.clone(),
    })
}

fn parse_join_delimiter(value: &str) -> Result<char, SandboxError> {
    let mut chars = value.chars();
    let Some(delimiter) = chars.next() else {
        return Err(SandboxError::InvalidRequest(
            "join delimiter must be a single character".to_string(),
        ));
    };
    if chars.next().is_some() {
        return Err(SandboxError::InvalidRequest(
            "join delimiter must be a single character".to_string(),
        ));
    }
    Ok(delimiter)
}

fn parse_join_field_number(value: &str, flag: &str) -> Result<usize, SandboxError> {
    let field = value.parse::<usize>().map_err(|_| {
        SandboxError::InvalidRequest(format!("join {flag} requires a positive field number"))
    })?;
    if field == 0 {
        return Err(SandboxError::InvalidRequest(format!(
            "join {flag} requires a positive field number"
        )));
    }
    Ok(field)
}

fn ensure_join_rows_sorted(command: &str, rows: &[JoinRow]) -> Result<(), SandboxError> {
    let mut previous = None::<&str>;
    for row in rows {
        if let Some(previous_key) = previous {
            if previous_key > row.key.as_str() {
                return Err(SandboxError::InvalidRequest(format!(
                    "{command} currently requires both inputs to be sorted by the join field"
                )));
            }
        }
        previous = Some(&row.key);
    }
    Ok(())
}

fn parse_join_fields(line: &str, delimiter: Option<char>) -> Vec<String> {
    match delimiter {
        Some(delimiter) => line.split(delimiter).map(ToString::to_string).collect(),
        None => line.split_whitespace().map(ToString::to_string).collect(),
    }
}

fn run_sleep(
    args: Vec<String>,
    timeout_ms: Option<u64>,
    cancel_flag: &AtomicBool,
    metadata: BTreeMap<String, String>,
) -> Result<ExecutionResult, SandboxError> {
    let Some(first_arg) = args.first() else {
        return Err(SandboxError::InvalidRequest(
            "sleep expects a single numeric duration".to_string(),
        ));
    };
    let requested_seconds = first_arg
        .parse::<f64>()
        .map_err(|_| SandboxError::InvalidRequest("sleep duration must be numeric".to_string()))?;
    if requested_seconds.is_sign_negative() {
        return Err(SandboxError::InvalidRequest(
            "sleep duration must be non-negative".to_string(),
        ));
    }

    let total = Duration::from_secs_f64(requested_seconds);
    let deadline = timeout_ms.map(Duration::from_millis);
    let started = Instant::now();
    let step = Duration::from_millis(5);

    if let Some(limit) = deadline {
        if total > limit {
            return Err(SandboxError::Timeout(
                "execution exceeded the configured timeout".to_string(),
            ));
        }
    }

    while started.elapsed() < total {
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(SandboxError::Cancellation(
                "execution was cancelled".to_string(),
            ));
        }

        if let Some(limit) = deadline {
            if started.elapsed() > limit {
                return Err(SandboxError::Timeout(
                    "execution exceeded the configured timeout".to_string(),
                ));
            }
        }

        let remaining = total.saturating_sub(started.elapsed());
        thread::sleep(step.min(remaining));
    }

    Ok(ExecutionResult::success(Vec::new(), metadata))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::sync::{atomic::AtomicBool, Arc};

    use abash_core::{ExecutionProfile, SandboxSession, SessionState};
    use tempfile::TempDir;

    use super::*;

    fn memory_config() -> SandboxConfig {
        SandboxConfig {
            profile: ExecutionProfile::Safe,
            filesystem_mode: FilesystemMode::Memory,
            session_state: SessionState::Persistent,
            allowlisted_commands: [
                "echo",
                "env",
                "which",
                "dirname",
                "basename",
                "cd",
                "export",
                "expr",
                "time",
                "timeout",
                "whoami",
                "hostname",
                "help",
                "clear",
                "history",
                "alias",
                "unalias",
                "bash",
                "sh",
                "tree",
                "stat",
                "du",
                "file",
                "readlink",
                "ln",
                "curl",
                "sleep",
                "mkdir",
                "touch",
                "rmdir",
                "cat",
                "grep",
                "wc",
                "sort",
                "uniq",
                "head",
                "tail",
                "cut",
                "tr",
                "paste",
                "sed",
                "join",
                "awk",
                "jq",
                "yq",
                "find",
                "ls",
                "rev",
                "nl",
                "tac",
                "strings",
                "fold",
                "expand",
                "unexpand",
                "rm",
                "cp",
                "mv",
                "tee",
                "printf",
                "seq",
                "date",
                "gzip",
                "html-to-markdown",
                "gunzip",
                "zcat",
                "tar",
                "sqlite3",
                "comm",
                "diff",
                "column",
                "chmod",
                "python",
                "python3",
                "js-exec",
                "xan",
                "xargs",
                "rg",
                "split",
                "od",
                "base64",
                "md5sum",
                "sha1sum",
                "sha256sum",
                "pwd",
            ]
            .into_iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>(),
            default_cwd: "/".to_string(),
            workspace_root: None,
            host_mounts: Vec::new(),
            writable_roots: BTreeSet::new(),
            network_policy: None,
        }
    }

    fn workspace_tempdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn denies_non_allowlisted_commands() {
        let cancel = Arc::new(AtomicBool::new(false));
        let backend = create_session(memory_config()).unwrap();
        let mut session = SandboxSession::new(memory_config(), backend, None, cancel);
        let result = session.run(ExecutionRequest {
            mode: ExecutionMode::Argv,
            argv: vec!["uname".to_string()],
            script: None,
            cwd: "/".to_string(),
            env: BTreeMap::new(),
            replace_env: false,
            stdin: Vec::new(),
            timeout_ms: None,
            network_enabled: false,
            filesystem_mode: FilesystemMode::Memory,
            metadata: BTreeMap::new(),
        });
        assert_eq!(
            result.error.unwrap().kind,
            abash_core::ErrorKind::PolicyDenied
        );
    }

    #[test]
    fn timeout_results_are_typed() {
        let cancel = Arc::new(AtomicBool::new(false));
        let backend = create_session(memory_config()).unwrap();
        let mut session = SandboxSession::new(memory_config(), backend, None, cancel);
        let result = session.run(ExecutionRequest {
            mode: ExecutionMode::Argv,
            argv: vec!["sleep".to_string(), "0.05".to_string()],
            script: None,
            cwd: "/".to_string(),
            env: BTreeMap::new(),
            replace_env: false,
            stdin: Vec::new(),
            timeout_ms: Some(1),
            network_enabled: false,
            filesystem_mode: FilesystemMode::Memory,
            metadata: BTreeMap::new(),
        });
        assert_eq!(result.error.unwrap().kind, abash_core::ErrorKind::Timeout);
    }

    #[test]
    fn cancellation_results_are_typed() {
        let config = memory_config();
        let cancel = Arc::new(AtomicBool::new(true));
        let mut backend = create_session(config.clone()).unwrap();
        let error = backend
            .run(
                ExecutionRequest {
                    mode: ExecutionMode::Argv,
                    argv: vec!["sleep".to_string(), "0.05".to_string()],
                    script: None,
                    cwd: "/".to_string(),
                    env: BTreeMap::new(),
                    replace_env: false,
                    stdin: Vec::new(),
                    timeout_ms: Some(1_000),
                    network_enabled: false,
                    filesystem_mode: FilesystemMode::Memory,
                    metadata: BTreeMap::new(),
                },
                &config,
                cancel.as_ref(),
                None,
            )
            .unwrap_err();
        assert_eq!(error.kind(), abash_core::ErrorKind::Cancellation);
    }

    #[test]
    fn shell_commands_modify_memory_filesystem() {
        let cancel = Arc::new(AtomicBool::new(false));
        let backend = create_session(memory_config()).unwrap();
        let mut session = SandboxSession::new(memory_config(), backend, None, cancel);

        let mkdir = session.run(ExecutionRequest {
            mode: ExecutionMode::Argv,
            argv: vec![
                "mkdir".to_string(),
                "-p".to_string(),
                "workspace/data".to_string(),
            ],
            script: None,
            cwd: "/".to_string(),
            env: BTreeMap::new(),
            replace_env: false,
            stdin: Vec::new(),
            timeout_ms: None,
            network_enabled: false,
            filesystem_mode: FilesystemMode::Memory,
            metadata: BTreeMap::new(),
        });
        assert_eq!(mkdir.exit_code, 0);

        let touch = session.run(ExecutionRequest {
            mode: ExecutionMode::Argv,
            argv: vec!["touch".to_string(), "workspace/data/demo.txt".to_string()],
            script: None,
            cwd: "/".to_string(),
            env: BTreeMap::new(),
            replace_env: false,
            stdin: Vec::new(),
            timeout_ms: None,
            network_enabled: false,
            filesystem_mode: FilesystemMode::Memory,
            metadata: BTreeMap::new(),
        });
        assert_eq!(touch.exit_code, 0);

        let cat = session.run(ExecutionRequest {
            mode: ExecutionMode::Argv,
            argv: vec!["cat".to_string(), "workspace/data/demo.txt".to_string()],
            script: None,
            cwd: "/".to_string(),
            env: BTreeMap::new(),
            replace_env: false,
            stdin: Vec::new(),
            timeout_ms: None,
            network_enabled: false,
            filesystem_mode: FilesystemMode::Memory,
            metadata: BTreeMap::new(),
        });
        assert_eq!(cat.stdout, Vec::<u8>::new());
    }

    #[test]
    fn host_readonly_reflects_host_workspace() {
        let workspace = workspace_tempdir();
        fs::create_dir_all(workspace.path().join("docs")).unwrap();
        fs::write(workspace.path().join("docs/demo.txt"), b"hello").unwrap();

        let config = SandboxConfig {
            profile: ExecutionProfile::Workspace,
            filesystem_mode: FilesystemMode::HostReadonly,
            session_state: SessionState::Persistent,
            allowlisted_commands: ["cat"]
                .into_iter()
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>(),
            default_cwd: "/workspace".to_string(),
            workspace_root: Some(workspace.path().to_path_buf()),
            host_mounts: Vec::new(),
            writable_roots: BTreeSet::new(),
            network_policy: None,
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let backend = create_session(config.clone()).unwrap();
        let mut session = SandboxSession::new(config, backend, None, cancel);
        let result = session.run(ExecutionRequest {
            mode: ExecutionMode::Argv,
            argv: vec!["cat".to_string(), "docs/demo.txt".to_string()],
            script: None,
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
            replace_env: false,
            stdin: Vec::new(),
            timeout_ms: None,
            network_enabled: false,
            filesystem_mode: FilesystemMode::HostReadonly,
            metadata: BTreeMap::new(),
        });
        assert_eq!(result.stdout, b"hello".to_vec());
    }

    #[test]
    fn glob_matcher_supports_star_question_and_brackets() {
        assert!(glob_matches_path(
            "/workspace/glob/*.txt",
            "/workspace/glob/a1.txt"
        ));
        assert!(glob_matches_path(
            "/workspace/glob/a?.txt",
            "/workspace/glob/a1.txt"
        ));
        assert!(glob_matches_path(
            "/workspace/glob/[ab]1.txt",
            "/workspace/glob/b1.txt"
        ));
    }

    #[test]
    fn redirect_router_preserves_2_to_1_ordering() {
        let config = memory_config();
        let mut session = VirtualSession {
            filesystem: create_filesystem(&config).unwrap(),
            default_cwd: config.default_cwd.clone(),
            session_state: SessionState::Persistent,
            current_cwd: config.default_cwd.clone(),
            exported_env: BTreeMap::new(),
            aliases: BTreeMap::new(),
            history: Vec::new(),
            active_extensions: None,
        };

        let merge_into_file = parse_script("echo ok > /workspace/out.txt 2>&1").unwrap();
        let StepKind::Pipeline(merge_pipeline) = &merge_into_file[0].kind else {
            panic!("expected pipeline");
        };
        let merged = session
            .apply_redirects(
                "/",
                &merge_pipeline.commands[0].redirects,
                b"out",
                b"err",
                &BTreeMap::new(),
                &[],
            )
            .unwrap();
        assert!(merged.stdout.is_empty());
        assert!(merged.stderr.is_empty());
        assert_eq!(
            session.filesystem.read_file("/workspace/out.txt").unwrap(),
            b"outerr".to_vec()
        );

        let stderr_to_old_stdout = parse_script("echo ok 2>&1 > /workspace/err-order.txt").unwrap();
        let StepKind::Pipeline(order_pipeline) = &stderr_to_old_stdout[0].kind else {
            panic!("expected pipeline");
        };
        let routed = session
            .apply_redirects(
                "/",
                &order_pipeline.commands[0].redirects,
                b"out",
                b"err",
                &BTreeMap::new(),
                &[],
            )
            .unwrap();
        assert_eq!(routed.stdout, b"err".to_vec());
        assert!(routed.stderr.is_empty());
        assert_eq!(
            session
                .filesystem
                .read_file("/workspace/err-order.txt")
                .unwrap(),
            b"out".to_vec()
        );
    }

    #[test]
    fn join_renders_matching_rows_with_custom_fields() {
        let join_spec = parse_join_spec(&[
            "-t".to_string(),
            ",".to_string(),
            "-1".to_string(),
            "2".to_string(),
            "-2".to_string(),
            "2".to_string(),
            "/workspace/left.csv".to_string(),
            "/workspace/right.csv".to_string(),
        ])
        .unwrap();

        let left = vec![
            JoinRow {
                key: "1".to_string(),
                fields: vec!["bert".to_string(), "1".to_string()],
                join_field: join_spec.left_field,
                delimiter: join_spec.delimiter,
            },
            JoinRow {
                key: "2".to_string(),
                fields: vec!["ana".to_string(), "2".to_string()],
                join_field: join_spec.left_field,
                delimiter: join_spec.delimiter,
            },
        ];
        let right = JoinRow {
            key: "2".to_string(),
            fields: vec!["growth".to_string(), "2".to_string()],
            join_field: join_spec.right_field,
            delimiter: join_spec.delimiter,
        };

        assert_eq!(left[1].render(&right), "2,ana,growth");
    }

    #[test]
    fn join_rejects_unsorted_inputs() {
        let rows = vec![
            JoinRow {
                key: "b".to_string(),
                fields: vec!["b".to_string()],
                join_field: 0,
                delimiter: None,
            },
            JoinRow {
                key: "a".to_string(),
                fields: vec!["a".to_string()],
                join_field: 0,
                delimiter: None,
            },
        ];

        let error = ensure_join_rows_sorted("join", &rows).unwrap_err();
        assert_eq!(error.kind(), abash_core::ErrorKind::InvalidRequest);
    }
}
