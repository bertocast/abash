use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use abash_core::SandboxError;
use rusqlite::ffi;
use rusqlite::{Connection, OpenFlags};
use serde_json::{Map, Value};

pub(crate) enum CommandOutcome {
    Cli {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        exit_code: i32,
    },
    Script(Plan),
}

pub(crate) struct Plan {
    pub(crate) output: OutputMode,
    pub(crate) header: bool,
    pub(crate) separator: String,
    pub(crate) readonly: bool,
    pub(crate) database: String,
    pub(crate) sql: String,
}

#[derive(Clone, Copy)]
pub(crate) enum OutputMode {
    List,
    Csv,
    Json,
}

pub(crate) fn parse(args: &[String], stdin: &[u8]) -> Result<CommandOutcome, SandboxError> {
    let mut output = OutputMode::List;
    let mut header = false;
    let mut separator = "|".to_string();
    let mut readonly = false;
    let mut cmd_sql = None::<String>;
    let mut show_help = false;
    let mut show_version = false;
    let mut index = 0usize;

    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "--help" | "-help" => show_help = true,
            "-version" => show_version = true,
            "-json" => output = OutputMode::Json,
            "-csv" => output = OutputMode::Csv,
            "-header" => header = true,
            "-noheader" => header = false,
            "-readonly" => readonly = true,
            "-separator" => {
                let Some(value) = args.get(index + 1) else {
                    return Ok(CommandOutcome::Cli {
                        stdout: Vec::new(),
                        stderr: b"sqlite3: Error: missing argument to -separator\n".to_vec(),
                        exit_code: 1,
                    });
                };
                separator = value.clone();
                index += 2;
                continue;
            }
            "-cmd" => {
                let Some(value) = args.get(index + 1) else {
                    return Ok(CommandOutcome::Cli {
                        stdout: Vec::new(),
                        stderr: b"sqlite3: Error: missing argument to -cmd\n".to_vec(),
                        exit_code: 1,
                    });
                };
                cmd_sql = Some(value.clone());
                index += 2;
                continue;
            }
            "--" => {
                index += 1;
                break;
            }
            value if value.starts_with('-') => {
                return Ok(CommandOutcome::Cli {
                    stdout: Vec::new(),
                    stderr: format!(
                        "sqlite3: Error: unknown option: {value}\nUse -help for a list of options.\n"
                    )
                    .into_bytes(),
                    exit_code: 1,
                });
            }
            _ => break,
        }
        index += 1;
    }

    if show_help {
        return Ok(CommandOutcome::Cli {
            stdout: help_text().into_bytes(),
            stderr: Vec::new(),
            exit_code: 0,
        });
    }

    if show_version {
        return Ok(CommandOutcome::Cli {
            stdout: format!("{}\n", rusqlite::version()).into_bytes(),
            stderr: Vec::new(),
            exit_code: 0,
        });
    }

    let Some(database) = args.get(index) else {
        return Ok(CommandOutcome::Cli {
            stdout: Vec::new(),
            stderr: b"sqlite3: Error: missing database argument\n".to_vec(),
            exit_code: 1,
        });
    };

    let sql = if let Some(sql) = args.get(index + 1) {
        sql.clone()
    } else if !stdin.is_empty() {
        String::from_utf8(stdin.to_vec()).map_err(|_| {
            SandboxError::InvalidRequest("sqlite3 stdin currently requires UTF-8 text".to_string())
        })?
    } else {
        String::new()
    };

    let sql = if let Some(prefix) = cmd_sql {
        if sql.trim().is_empty() {
            prefix
        } else {
            format!("{prefix}; {sql}")
        }
    } else {
        sql
    };

    Ok(CommandOutcome::Script(Plan {
        output,
        header,
        separator,
        readonly,
        database: database.clone(),
        sql,
    }))
}

pub(crate) fn execute(
    plan: &Plan,
    existing_db: Option<Vec<u8>>,
) -> Result<Execution, SandboxError> {
    let db_kind = if plan.database == ":memory:" {
        DatabaseKind::Memory
    } else {
        DatabaseKind::File(create_temp_db_path()?)
    };
    let connection = open_connection(&db_kind, plan.readonly, existing_db)?;
    let execution = if let Some(command) = parse_meta_command(&plan.sql) {
        run_meta_command(&connection, command)?
    } else {
        let mut statements = Vec::new();
        run_sql(&connection, &plan.sql, &mut statements)?
    };
    let writeback = if plan.database == ":memory:" || plan.readonly {
        None
    } else {
        Some(
            fs::read(db_kind.path().expect("file path")).map_err(|error| {
                SandboxError::BackendFailure(format!("sqlite3 could not persist database: {error}"))
            })?,
        )
    };
    drop(connection);
    db_kind.cleanup();

    match execution {
        SqlExecution::Rows(sets) => Ok(Execution {
            stdout: render_sets(&sets, plan)?,
            stderr: Vec::new(),
            exit_code: 0,
            writeback,
        }),
        SqlExecution::Message(message) => Ok(Execution {
            stdout: format!("Error: {message}\n").into_bytes(),
            stderr: Vec::new(),
            exit_code: 0,
            writeback,
        }),
    }
}

pub(crate) struct Execution {
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) exit_code: i32,
    pub(crate) writeback: Option<Vec<u8>>,
}

enum DatabaseKind {
    Memory,
    File(PathBuf),
}

impl DatabaseKind {
    fn path(&self) -> Option<&Path> {
        match self {
            Self::Memory => None,
            Self::File(path) => Some(path.as_path()),
        }
    }

    fn cleanup(&self) {
        if let Self::File(path) = self {
            let _ = fs::remove_file(path);
        }
    }
}

struct ResultSet {
    columns: Vec<String>,
    rows: Vec<Vec<CellValue>>,
}

enum CellValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

enum SqlExecution {
    Rows(Vec<ResultSet>),
    Message(String),
}

enum MetaCommand<'a> {
    Tables,
    Schema(Option<&'a str>),
}

fn help_text() -> String {
    [
        "sqlite3 [OPTIONS] DATABASE [SQL]",
        "",
        "Options:",
        "  -json",
        "  -csv",
        "  -header",
        "  -noheader",
        "  -separator SEP",
        "  -readonly",
        "  -cmd SQL",
        "  -version",
        "  -help, --help",
    ]
    .join("\n")
        + "\n"
}

fn open_connection(
    db_kind: &DatabaseKind,
    readonly: bool,
    existing_db: Option<Vec<u8>>,
) -> Result<Connection, SandboxError> {
    match db_kind {
        DatabaseKind::Memory => {
            let _ = existing_db;
            Connection::open_in_memory().map_err(to_backend_error)
        }
        DatabaseKind::File(path) => {
            if let Some(bytes) = existing_db {
                fs::write(path, bytes).map_err(|error| {
                    SandboxError::BackendFailure(format!(
                        "sqlite3 could not stage database file: {error}"
                    ))
                })?;
            }
            let flags = if readonly {
                OpenFlags::SQLITE_OPEN_READ_ONLY
            } else {
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
            };
            Connection::open_with_flags(path, flags).map_err(to_backend_error)
        }
    }
}

fn parse_meta_command(sql: &str) -> Option<MetaCommand<'_>> {
    let trimmed = sql.trim();
    if trimmed == ".tables" {
        return Some(MetaCommand::Tables);
    }
    if let Some(rest) = trimmed.strip_prefix(".schema") {
        let name = rest.trim();
        if name.is_empty() {
            return Some(MetaCommand::Schema(None));
        }
        return Some(MetaCommand::Schema(Some(name)));
    }
    None
}

fn run_meta_command(
    connection: &Connection,
    command: MetaCommand<'_>,
) -> Result<SqlExecution, SandboxError> {
    match command {
        MetaCommand::Tables => run_tables_command(connection),
        MetaCommand::Schema(name) => run_schema_command(connection, name),
    }
}

fn create_temp_db_path() -> Result<PathBuf, SandboxError> {
    let mut path = std::env::temp_dir();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SandboxError::BackendFailure(format!("sqlite3 clock error: {error}")))?
        .as_nanos();
    path.push(format!("abash-sqlite-{}-{stamp}.db", std::process::id()));
    Ok(path)
}

fn run_tables_command(connection: &Connection) -> Result<SqlExecution, SandboxError> {
    let mut statement = connection
        .prepare(
            "SELECT name
             FROM sqlite_master
             WHERE type IN ('table', 'view')
               AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .map_err(to_backend_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(to_backend_error)?;

    let mut table_rows = Vec::new();
    for row in rows {
        table_rows.push(vec![CellValue::Text(row.map_err(to_backend_error)?)]);
    }

    Ok(SqlExecution::Rows(vec![ResultSet {
        columns: vec!["name".to_string()],
        rows: table_rows,
    }]))
}

fn run_schema_command(
    connection: &Connection,
    name: Option<&str>,
) -> Result<SqlExecution, SandboxError> {
    let sql = if name.is_some() {
        "SELECT sql
         FROM sqlite_master
         WHERE sql IS NOT NULL
           AND name NOT LIKE 'sqlite_%'
           AND (name = ?1 OR tbl_name = ?1)
         ORDER BY type, name"
    } else {
        "SELECT sql
         FROM sqlite_master
         WHERE sql IS NOT NULL
           AND name NOT LIKE 'sqlite_%'
         ORDER BY type, name"
    };
    let mut statement = connection.prepare(sql).map_err(to_backend_error)?;
    let mapped = if let Some(name) = name {
        statement
            .query_map([name], |row| row.get::<_, String>(0))
            .map_err(to_backend_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_backend_error)?
    } else {
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(to_backend_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_backend_error)?
    };

    Ok(SqlExecution::Rows(vec![ResultSet {
        columns: vec!["sql".to_string()],
        rows: mapped
            .into_iter()
            .map(|value| vec![CellValue::Text(format!("{value};"))])
            .collect(),
    }]))
}

fn run_sql(
    connection: &Connection,
    sql: &str,
    statements: &mut Vec<String>,
) -> Result<SqlExecution, SandboxError> {
    if sql.trim().is_empty() {
        return Ok(SqlExecution::Rows(Vec::new()));
    }

    let mut tail_ptr = sql.as_ptr() as *const i8;
    let sql_end = unsafe { tail_ptr.add(sql.len()) };
    let mut sets = Vec::new();

    while tail_ptr < sql_end {
        let mut statement = std::ptr::null_mut();
        let mut next_tail = std::ptr::null();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                connection.handle(),
                tail_ptr,
                (sql_end as usize - tail_ptr as usize) as i32,
                &mut statement,
                &mut next_tail,
            )
        };
        if rc != ffi::SQLITE_OK {
            return Ok(SqlExecution::Message(sqlite_error(unsafe {
                connection.handle()
            })));
        }
        if statement.is_null() {
            if next_tail.is_null() || next_tail <= tail_ptr {
                break;
            }
            tail_ptr = next_tail;
            continue;
        }

        statements.push(
            unsafe { std::ffi::CStr::from_ptr(ffi::sqlite3_sql(statement)) }
                .to_string_lossy()
                .to_string(),
        );

        let column_count = unsafe { ffi::sqlite3_column_count(statement) };
        let mut rows = Vec::new();
        let mut columns = Vec::new();
        for column in 0..column_count {
            let name = unsafe { ffi::sqlite3_column_name(statement, column) };
            columns.push(
                unsafe { std::ffi::CStr::from_ptr(name) }
                    .to_string_lossy()
                    .to_string(),
            );
        }

        loop {
            let step = unsafe { ffi::sqlite3_step(statement) };
            match step {
                ffi::SQLITE_ROW => {
                    let mut row = Vec::new();
                    for column in 0..column_count {
                        row.push(read_cell(statement, column));
                    }
                    rows.push(row);
                }
                ffi::SQLITE_DONE => break,
                _ => {
                    unsafe { ffi::sqlite3_finalize(statement) };
                    return Ok(SqlExecution::Message(sqlite_error(unsafe {
                        connection.handle()
                    })));
                }
            }
        }

        if column_count > 0 {
            sets.push(ResultSet { columns, rows });
        }

        unsafe { ffi::sqlite3_finalize(statement) };
        if next_tail.is_null() || next_tail <= tail_ptr {
            break;
        }
        tail_ptr = next_tail;
    }

    Ok(SqlExecution::Rows(sets))
}

fn read_cell(statement: *mut ffi::sqlite3_stmt, column: i32) -> CellValue {
    match unsafe { ffi::sqlite3_column_type(statement, column) } {
        ffi::SQLITE_INTEGER => {
            CellValue::Integer(unsafe { ffi::sqlite3_column_int64(statement, column) })
        }
        ffi::SQLITE_FLOAT => {
            CellValue::Real(unsafe { ffi::sqlite3_column_double(statement, column) })
        }
        ffi::SQLITE_TEXT => {
            let ptr = unsafe { ffi::sqlite3_column_text(statement, column) };
            let len = unsafe { ffi::sqlite3_column_bytes(statement, column) };
            let bytes = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
            CellValue::Text(String::from_utf8_lossy(bytes).to_string())
        }
        ffi::SQLITE_BLOB => {
            let ptr = unsafe { ffi::sqlite3_column_blob(statement, column) as *const u8 };
            let len = unsafe { ffi::sqlite3_column_bytes(statement, column) };
            let bytes = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
            CellValue::Blob(bytes.to_vec())
        }
        _ => CellValue::Null,
    }
}

fn render_sets(sets: &[ResultSet], plan: &Plan) -> Result<Vec<u8>, SandboxError> {
    match plan.output {
        OutputMode::Json => render_json_sets(sets),
        OutputMode::Csv => Ok(render_delimited_sets(sets, ",", plan.header).into_bytes()),
        OutputMode::List => {
            Ok(render_delimited_sets(sets, &plan.separator, plan.header).into_bytes())
        }
    }
}

fn render_json_sets(sets: &[ResultSet]) -> Result<Vec<u8>, SandboxError> {
    let mut rendered = String::new();
    for set in sets {
        let mut rows = Vec::new();
        for row in &set.rows {
            let mut object = Map::new();
            for (column, cell) in set.columns.iter().zip(row) {
                object.insert(column.clone(), cell_to_json(cell));
            }
            rows.push(Value::Object(object));
        }
        rendered.push_str(&serde_json::to_string(&rows).map_err(|error| {
            SandboxError::BackendFailure(format!("sqlite3 could not render JSON output: {error}"))
        })?);
        rendered.push('\n');
    }
    Ok(rendered.into_bytes())
}

fn render_delimited_sets(sets: &[ResultSet], separator: &str, header: bool) -> String {
    let mut rendered = String::new();
    for set in sets {
        if header {
            rendered.push_str(&set.columns.join(separator));
            rendered.push('\n');
        }
        for row in &set.rows {
            rendered.push_str(
                &row.iter()
                    .map(|cell| format_cell(cell, separator == ","))
                    .collect::<Vec<_>>()
                    .join(separator),
            );
            rendered.push('\n');
        }
    }
    rendered
}

fn format_cell(cell: &CellValue, csv: bool) -> String {
    match cell {
        CellValue::Null => String::new(),
        CellValue::Integer(value) => value.to_string(),
        CellValue::Real(value) => value.to_string(),
        CellValue::Text(value) => {
            if csv && (value.contains(',') || value.contains('"') || value.contains('\n')) {
                format!("\"{}\"", value.replace('"', "\"\""))
            } else {
                value.clone()
            }
        }
        CellValue::Blob(bytes) => {
            let hex = bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            format!("x'{hex}'")
        }
    }
}

fn cell_to_json(cell: &CellValue) -> Value {
    match cell {
        CellValue::Null => Value::Null,
        CellValue::Integer(value) => Value::from(*value),
        CellValue::Real(value) => Value::from(*value),
        CellValue::Text(value) => Value::from(value.clone()),
        CellValue::Blob(bytes) => Value::from(
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        ),
    }
}

fn sqlite_error(handle: *mut ffi::sqlite3) -> String {
    let message = unsafe { ffi::sqlite3_errmsg(handle) };
    unsafe { std::ffi::CStr::from_ptr(message) }
        .to_string_lossy()
        .to_string()
}

fn to_backend_error(error: rusqlite::Error) -> SandboxError {
    SandboxError::BackendFailure(format!("sqlite3 failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_cli_flags() {
        let outcome = parse(
            &[
                "-json".to_string(),
                "-header".to_string(),
                ":memory:".to_string(),
                "SELECT 1".to_string(),
            ],
            &[],
        )
        .unwrap();
        let CommandOutcome::Script(plan) = outcome else {
            panic!("expected script plan");
        };
        assert!(matches!(plan.output, OutputMode::Json));
        assert!(plan.header);
    }
}
