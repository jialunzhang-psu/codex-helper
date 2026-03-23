use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use walkdir::WalkDir;

const UNTITLED_SESSION_NAME: &str = "(untitled)";

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let store = Store::new(cli.codex_home)?;

    match cli.command {
        Command::AddResume(args) => store.add_resume(&args),
        Command::Clean(args) => store.clean(&args),
        Command::CleanNotInResume(args) => store.clean_not_in_resume(&args),
        Command::List(args) => store.list_sessions(&args),
    }
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Manage local Codex sessions stored under ~/.codex"
)]
struct Cli {
    #[arg(long, value_name = "PATH", global = true)]
    codex_home: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    AddResume(AddResumeArgs),
    Clean(CleanArgs),
    #[command(name = "clean-not-in-resume", visible_alias = "clean-unindexed")]
    CleanNotInResume(CleanNotInResumeArgs),
    List(ListArgs),
}

#[derive(Args, Debug)]
struct AddResumeArgs {
    session_id: String,
    #[arg(long)]
    name: Option<String>,
}

#[derive(Args, Debug)]
struct CleanArgs {
    #[arg(long, conflicts_with_all = ["name", "unnamed"])]
    id: Option<String>,
    #[arg(long, conflicts_with_all = ["id", "unnamed"])]
    name: Option<String>,
    #[arg(long, conflicts_with_all = ["id", "name"])]
    unnamed: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Args, Debug)]
struct CleanNotInResumeArgs {
    #[arg(long)]
    dry_run: bool,
}

#[derive(Args, Debug)]
struct ListArgs {
    #[arg(long, conflicts_with = "named")]
    unnamed: bool,
    #[arg(long, conflicts_with = "unnamed")]
    named: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListFilter {
    All,
    Named,
    Unnamed,
}

struct Store {
    _codex_home: PathBuf,
    sessions_root: PathBuf,
    session_index_path: PathBuf,
    history_path: PathBuf,
    state_db_path: Option<PathBuf>,
}

impl Store {
    fn new(codex_home: Option<PathBuf>) -> Result<Self> {
        let root = match codex_home {
            Some(path) => path,
            None => {
                let home =
                    env::var("HOME").context("HOME is not set; pass --codex-home explicitly")?;
                PathBuf::from(home).join(".codex")
            }
        };
        let state_db_path = find_latest_state_db(&root)?;
        Ok(Self {
            sessions_root: root.join("sessions"),
            session_index_path: root.join("session_index.jsonl"),
            history_path: root.join("history.jsonl"),
            _codex_home: root,
            state_db_path,
        })
    }

    fn list_sessions(&self, args: &ListArgs) -> Result<()> {
        let filter = list_filter_from_args(args);
        let sessions: Vec<SessionInfo> = self
            .load_sessions()?
            .into_iter()
            .filter(|session| session_matches_list_filter(filter, session))
            .collect();
        if filter == ListFilter::Unnamed && sessions.is_empty() {
            println!("no unnamed sessions found");
            return Ok(());
        }
        if filter == ListFilter::Named && sessions.is_empty() {
            println!("no named sessions found");
            return Ok(());
        }
        println!("IN_RESUME\tSESSION_ID\tNAME\tCWD");
        for session in sessions {
            let in_resume = if session.in_resume { "yes" } else { "no" };
            println!(
                "{in_resume}\t{}\t{}\t{}",
                session.id,
                session.display_name,
                session.cwd.display()
            );
        }
        Ok(())
    }

    fn add_resume(&self, args: &AddResumeArgs) -> Result<()> {
        self.ensure_state_db_available("update the resume list")?;
        let sessions = self.load_sessions()?;
        let session = sessions
            .into_iter()
            .find(|session| session.id == args.session_id)
            .ok_or_else(|| {
                anyhow!(
                    "session {} not found under {}",
                    args.session_id,
                    self.sessions_root.display()
                )
            })?;

        let thread_name = args
            .name
            .clone()
            .or_else(|| session.indexed_name.clone())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| session.display_name.clone());
        let updated_at = session
            .timestamp
            .clone()
            .unwrap_or_else(current_rfc3339_timestamp);
        let new_entry = serde_json::json!({
            "id": session.id.clone(),
            "thread_name": thread_name.clone(),
            "updated_at": updated_at,
        });
        let mut lines = self.read_lines(&self.session_index_path)?;
        lines.retain(|line| match parse_index_entry(line) {
            Some(entry) => entry.id != session.id,
            None => true,
        });
        lines.push(new_entry.to_string());
        self.write_lines(&self.session_index_path, &lines)?;
        self.promote_thread_in_state_db(&session, &thread_name)?;

        println!(
            "added_to_resume {}\t{}\t{}",
            session.id,
            thread_name,
            session.cwd.display()
        );
        Ok(())
    }

    fn clean(&self, args: &CleanArgs) -> Result<()> {
        let sessions = self.load_sessions()?;
        let target_ids = if let Some(id) = &args.id {
            let session = sessions
                .iter()
                .find(|session| session.id == *id)
                .ok_or_else(|| anyhow!("session {} not found", id))?;
            vec![session.id.clone()]
        } else if let Some(name) = &args.name {
            self.resolve_name_matches(&sessions, name)?
                .into_iter()
                .map(|session| session.id.clone())
                .collect()
        } else if args.unnamed {
            sessions
                .iter()
                .filter(|session| session_is_unnamed(session))
                .map(|session| session.id.clone())
                .collect()
        } else {
            bail!("pass either --id, --name, or --unnamed");
        };

        if args.unnamed && target_ids.is_empty() {
            println!("no unnamed sessions found");
            return Ok(());
        }

        self.clean_ids(&target_ids, args.dry_run)
    }

    fn clean_not_in_resume(&self, args: &CleanNotInResumeArgs) -> Result<()> {
        self.ensure_state_db_available("read the resume list")?;
        let sessions = self.load_sessions()?;
        let target_ids: Vec<String> = sessions
            .into_iter()
            .filter(|session| !session.in_resume)
            .map(|session| session.id)
            .collect();

        if target_ids.is_empty() {
            println!("no sessions outside the resume list found");
            return Ok(());
        }

        self.clean_ids(&target_ids, args.dry_run)
    }

    fn clean_ids(&self, target_ids: &[String], dry_run: bool) -> Result<()> {
        let target_set: HashSet<&str> = target_ids.iter().map(String::as_str).collect();
        let sessions = self.load_sessions()?;
        let victims: Vec<SessionInfo> = sessions
            .into_iter()
            .filter(|session| target_set.contains(session.id.as_str()))
            .collect();

        if victims.is_empty() {
            println!("nothing to remove");
            return Ok(());
        }

        for session in &victims {
            println!(
                "{} {}\t{}\t{}",
                if dry_run { "would_remove" } else { "remove" },
                session.id,
                session.display_name,
                session.cwd.display()
            );
        }

        if dry_run {
            return Ok(());
        }

        for session in &victims {
            for path in &session.paths {
                if path.exists() {
                    fs::remove_file(path)
                        .with_context(|| format!("failed to remove {}", path.display()))?;
                    prune_empty_parents(path, &self.sessions_root)?;
                }
            }
        }

        self.rewrite_filtered_jsonl(&self.session_index_path, |line| {
            parse_index_entry(line)
                .map(|entry| !target_set.contains(entry.id.as_str()))
                .unwrap_or(true)
        })?;
        self.rewrite_filtered_jsonl(&self.history_path, |line| {
            parse_history_session_id(line)
                .map(|session_id| !target_set.contains(session_id.as_str()))
                .unwrap_or(true)
        })?;
        self.delete_threads_from_state_db(target_ids)?;

        Ok(())
    }

    fn load_sessions(&self) -> Result<Vec<SessionInfo>> {
        let index_entries = self.load_index_entries()?;
        let history_sessions = self.load_history_sessions()?;
        let db_threads = self.load_state_threads()?;
        let mut sessions_by_id: HashMap<String, SessionInfo> = HashMap::new();

        for entry in WalkDir::new(&self.sessions_root)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
        {
            if entry.path().extension() != Some(OsStr::new("jsonl")) {
                continue;
            }
            match self.load_session_file(entry.path()) {
                Ok(session) => {
                    sessions_by_id
                        .entry(session.id.clone())
                        .and_modify(|existing| merge_sessions(existing, &session))
                        .or_insert(session);
                }
                Err(error) => {
                    eprintln!(
                        "warning: failed to parse {}: {error:#}",
                        entry.path().display()
                    );
                }
            }
        }

        for (id, db_thread) in &db_threads {
            let session = sessions_by_id
                .entry(id.clone())
                .or_insert_with(|| SessionInfo::placeholder(id));
            apply_state_thread(session, db_thread);
        }

        for (id, entry) in &index_entries {
            let session = sessions_by_id
                .entry(id.clone())
                .or_insert_with(|| SessionInfo::placeholder(id));
            apply_index_entry(session, entry);
        }

        for (id, entry) in &history_sessions {
            let session = sessions_by_id
                .entry(id.clone())
                .or_insert_with(|| SessionInfo::placeholder(id));
            apply_history_entry(session, entry);
        }

        let mut sessions: Vec<SessionInfo> = sessions_by_id.into_values().collect();
        for session in &mut sessions {
            refresh_display_name(session);
        }
        sessions.sort_by_key(|session| {
            (
                Reverse(session.timestamp.clone().unwrap_or_default()),
                session.id.clone(),
            )
        });
        Ok(sessions)
    }

    fn load_session_file(&self, path: &Path) -> Result<SessionInfo> {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut session_id = None;
        let mut cwd = None;
        let mut timestamp = None;
        let mut source = None;
        let mut model_provider = None;
        let mut cli_version = None;
        let mut sandbox_policy = None;
        let mut approval_mode = None;
        let mut first_user_message = None;
        let mut derived_name = None;

        for line_result in reader.lines() {
            let line = line_result?;
            let value: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(_) => continue,
            };

            match value.get("type").and_then(Value::as_str) {
                Some("session_meta") => {
                    let payload = value.get("payload").ok_or_else(|| {
                        anyhow!("session_meta missing payload in {}", path.display())
                    })?;
                    session_id = payload.get("id").and_then(Value::as_str).map(str::to_owned);
                    cwd = payload
                        .get("cwd")
                        .and_then(Value::as_str)
                        .map(PathBuf::from);
                    timestamp = payload
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or_else(|| {
                            value
                                .get("timestamp")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        });
                    source = payload
                        .get("source")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    model_provider = payload
                        .get("model_provider")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    cli_version = payload
                        .get("cli_version")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                Some("turn_context") => {
                    if let Some(payload) = value.get("payload") {
                        approval_mode = approval_mode.or_else(|| {
                            payload
                                .get("approval_policy")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        });
                        sandbox_policy = sandbox_policy.or_else(|| {
                            payload
                                .get("sandbox_policy")
                                .map(|policy| policy.to_string())
                        });
                    }
                }
                Some("response_item") => {
                    if first_user_message.is_none() {
                        first_user_message = extract_user_message_line(&value);
                    }
                    if derived_name.is_none() {
                        derived_name = extract_title_candidate(&value);
                    }
                }
                _ => {}
            }
        }

        let id =
            session_id.ok_or_else(|| anyhow!("session_meta.id missing in {}", path.display()))?;
        let cwd = cwd.ok_or_else(|| anyhow!("session_meta.cwd missing in {}", path.display()))?;
        let display_name = derived_name
            .clone()
            .unwrap_or_else(|| UNTITLED_SESSION_NAME.to_string());

        Ok(SessionInfo {
            id,
            paths: vec![path.to_path_buf()],
            primary_rollout_path: Some(path.to_path_buf()),
            cwd,
            timestamp,
            in_resume: false,
            indexed_name: None,
            state_title: None,
            derived_name,
            display_name,
            source,
            model_provider,
            cli_version,
            sandbox_policy,
            approval_mode,
            first_user_message,
        })
    }

    fn load_index_entries(&self) -> Result<HashMap<String, IndexEntry>> {
        let mut map = HashMap::new();
        for line in self.read_lines(&self.session_index_path)? {
            if let Some(entry) = parse_index_entry(&line) {
                map.insert(entry.id.clone(), entry);
            }
        }
        Ok(map)
    }

    fn load_history_sessions(&self) -> Result<HashMap<String, HistorySessionInfo>> {
        let mut map = HashMap::new();
        for line in self.read_lines(&self.history_path)? {
            let Some(entry) = parse_history_entry(&line) else {
                continue;
            };
            let session = map
                .entry(entry.session_id)
                .or_insert_with(HistorySessionInfo::default);
            let text = entry.text.trim();
            if session.first_user_message.is_none() && !text.is_empty() {
                session.first_user_message = Some(text.to_string());
                session.derived_name = Some(truncate_chars(first_line(text), 80));
            }
            session.timestamp = Some(match &session.timestamp {
                Some(existing) if existing >= &entry.timestamp => existing.clone(),
                _ => entry.timestamp,
            });
        }
        Ok(map)
    }

    fn load_state_threads(&self) -> Result<HashMap<String, StateThreadInfo>> {
        let Some(connection) = self.open_state_db()? else {
            return Ok(HashMap::new());
        };

        let mut stmt = connection.prepare(
            "SELECT id, title, source, cwd, first_user_message, archived, rollout_path, updated_at
             FROM threads
             WHERE id <> ''",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(StateThreadInfo {
                id: row.get(0)?,
                title: row.get(1)?,
                source: row.get(2)?,
                cwd: PathBuf::from(row.get::<_, String>(3)?),
                first_user_message: row.get(4)?,
                archived: row.get::<_, i64>(5)? != 0,
                rollout_path: row.get::<_, Option<String>>(6)?.map(PathBuf::from),
                updated_at: row.get::<_, i64>(7).ok().and_then(epoch_seconds_to_rfc3339),
            })
        })?;

        let mut map = HashMap::new();
        for row in rows {
            let thread = row?;
            map.insert(thread.id.clone(), thread);
        }
        Ok(map)
    }

    fn promote_thread_in_state_db(&self, session: &SessionInfo, title: &str) -> Result<()> {
        let Some(connection) = self.open_state_db()? else {
            return Ok(());
        };

        let rollout_path = session
            .primary_rollout_path
            .as_ref()
            .map(|path| path.display().to_string());
        let cwd = if session.cwd == PathBuf::from("(unknown)") {
            String::new()
        } else {
            session.cwd.display().to_string()
        };
        let updated_at = session
            .timestamp
            .as_deref()
            .and_then(parse_timestamp_to_epoch_seconds)
            .unwrap_or_else(current_unix_timestamp);
        let first_user_message = session
            .first_user_message
            .clone()
            .unwrap_or_else(|| title.to_string());
        let cli_version = session.cli_version.clone().unwrap_or_default();
        let model_provider = session
            .model_provider
            .clone()
            .unwrap_or_else(|| "openai".to_string());
        let sandbox_policy = session
            .sandbox_policy
            .clone()
            .unwrap_or_else(|| "{\"type\":\"danger-full-access\"}".to_string());
        let approval_mode = session
            .approval_mode
            .clone()
            .unwrap_or_else(|| "never".to_string());
        let existing_created_at: Option<i64> = connection
            .query_row(
                "SELECT created_at FROM threads WHERE id = ?1",
                params![&session.id],
                |row| row.get(0),
            )
            .optional()?;

        if existing_created_at.is_some() {
            connection.execute(
                "UPDATE threads
                 SET title = ?1,
                     source = 'cli',
                     cwd = CASE WHEN ?2 <> '' THEN ?2 ELSE cwd END,
                     rollout_path = CASE WHEN ?3 <> '' THEN ?3 ELSE rollout_path END,
                     updated_at = ?4,
                     archived = 0,
                     cli_version = CASE WHEN ?5 <> '' THEN ?5 ELSE cli_version END,
                     first_user_message = CASE WHEN ?6 <> '' THEN ?6 ELSE first_user_message END
                 WHERE id = ?7",
                params![
                    title,
                    cwd,
                    rollout_path.clone().unwrap_or_default(),
                    updated_at,
                    cli_version,
                    first_user_message,
                    session.id,
                ],
            )?;
            return Ok(());
        }

        let rollout_path = rollout_path.ok_or_else(|| {
            anyhow!(
                "session {} has no rollout path; cannot insert it into the resume DB",
                session.id
            )
        })?;

        connection.execute(
            "INSERT INTO threads (
                id,
                rollout_path,
                created_at,
                updated_at,
                source,
                model_provider,
                cwd,
                title,
                sandbox_policy,
                approval_mode,
                tokens_used,
                has_user_event,
                archived,
                cli_version,
                first_user_message,
                memory_mode
            ) VALUES (?1, ?2, ?3, ?4, 'cli', ?5, ?6, ?7, ?8, ?9, 0, 0, 0, ?10, ?11, 'enabled')",
            params![
                session.id,
                rollout_path,
                updated_at,
                updated_at,
                model_provider,
                cwd,
                title,
                sandbox_policy,
                approval_mode,
                cli_version,
                first_user_message,
            ],
        )?;
        Ok(())
    }

    fn delete_threads_from_state_db(&self, target_ids: &[String]) -> Result<()> {
        let Some(connection) = self.open_state_db()? else {
            return Ok(());
        };
        let mut delete_thread_stmt = connection.prepare("DELETE FROM threads WHERE id = ?1")?;
        let mut delete_tools_stmt =
            connection.prepare("DELETE FROM thread_dynamic_tools WHERE thread_id = ?1")?;
        for session_id in target_ids {
            delete_thread_stmt.execute(params![session_id])?;
            delete_tools_stmt.execute(params![session_id])?;
        }
        Ok(())
    }

    fn resolve_name_matches<'a>(
        &self,
        sessions: &'a [SessionInfo],
        query: &str,
    ) -> Result<Vec<&'a SessionInfo>> {
        let lower_query = query.to_lowercase();
        let exact: Vec<&SessionInfo> = sessions
            .iter()
            .filter(|session| session.display_name.to_lowercase() == lower_query)
            .collect();
        if exact.len() == 1 {
            return Ok(exact);
        }
        if exact.len() > 1 {
            return Err(ambiguous_name_error(query, &exact));
        }

        let contains: Vec<&SessionInfo> = sessions
            .iter()
            .filter(|session| session.display_name.to_lowercase().contains(&lower_query))
            .collect();
        if contains.len() == 1 {
            return Ok(contains);
        }
        if contains.is_empty() {
            bail!("no session name matches {query:?}");
        }
        Err(ambiguous_name_error(query, &contains))
    }

    fn open_state_db(&self) -> Result<Option<Connection>> {
        let Some(path) = &self.state_db_path else {
            return Ok(None);
        };
        let connection =
            Connection::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(Some(connection))
    }

    fn ensure_state_db_available(&self, action: &str) -> Result<()> {
        if self.state_db_path.is_some() {
            return Ok(());
        }
        bail!(
            "no Codex state DB found under {}; cannot {}",
            self._codex_home.display(),
            action
        )
    }

    fn read_lines(&self, path: &Path) -> Result<Vec<String>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut lines = Vec::new();
        for line in reader.lines() {
            lines.push(line?);
        }
        Ok(lines)
    }

    fn write_lines(&self, path: &Path, lines: &[String]) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let tmp_path = path.with_extension("tmp");
        let mut file = File::create(&tmp_path)
            .with_context(|| format!("failed to create {}", tmp_path.display()))?;
        for line in lines {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        fs::rename(&tmp_path, path)
            .with_context(|| format!("failed to replace {}", path.display()))?;
        Ok(())
    }

    fn rewrite_filtered_jsonl<F>(&self, path: &Path, mut keep: F) -> Result<()>
    where
        F: FnMut(&str) -> bool,
    {
        let lines = self.read_lines(path)?;
        let filtered: Vec<String> = lines.into_iter().filter(|line| keep(line)).collect();
        self.write_lines(path, &filtered)
    }
}

#[derive(Clone, Debug)]
struct SessionInfo {
    id: String,
    paths: Vec<PathBuf>,
    primary_rollout_path: Option<PathBuf>,
    cwd: PathBuf,
    timestamp: Option<String>,
    in_resume: bool,
    indexed_name: Option<String>,
    state_title: Option<String>,
    derived_name: Option<String>,
    display_name: String,
    source: Option<String>,
    model_provider: Option<String>,
    cli_version: Option<String>,
    sandbox_policy: Option<String>,
    approval_mode: Option<String>,
    first_user_message: Option<String>,
}

impl SessionInfo {
    fn placeholder(id: &str) -> Self {
        Self {
            id: id.to_string(),
            paths: Vec::new(),
            primary_rollout_path: None,
            cwd: PathBuf::from("(unknown)"),
            timestamp: None,
            in_resume: false,
            indexed_name: None,
            state_title: None,
            derived_name: None,
            display_name: UNTITLED_SESSION_NAME.to_string(),
            source: None,
            model_provider: None,
            cli_version: None,
            sandbox_policy: None,
            approval_mode: None,
            first_user_message: None,
        }
    }
}

#[derive(Clone, Debug)]
struct StateThreadInfo {
    id: String,
    title: String,
    source: String,
    cwd: PathBuf,
    first_user_message: String,
    archived: bool,
    rollout_path: Option<PathBuf>,
    updated_at: Option<String>,
}

#[derive(Clone, Debug)]
struct IndexEntry {
    id: String,
    thread_name: String,
    updated_at: String,
}

#[derive(Clone, Debug, Default)]
struct HistorySessionInfo {
    first_user_message: Option<String>,
    derived_name: Option<String>,
    timestamp: Option<String>,
}

#[derive(Clone, Debug)]
struct HistoryEntry {
    session_id: String,
    text: String,
    timestamp: String,
}

fn parse_history_session_id(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    value
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn parse_index_entry(line: &str) -> Option<IndexEntry> {
    let value: Value = serde_json::from_str(line).ok()?;
    Some(IndexEntry {
        id: value.get("id")?.as_str()?.to_string(),
        thread_name: value.get("thread_name")?.as_str()?.to_string(),
        updated_at: value.get("updated_at")?.as_str()?.to_string(),
    })
}

fn parse_history_entry(line: &str) -> Option<HistoryEntry> {
    let value: Value = serde_json::from_str(line).ok()?;
    let session_id = value.get("session_id")?.as_str()?.to_string();
    let text = value.get("text")?.as_str()?.to_string();
    let ts = value.get("ts")?.as_i64()?;
    Some(HistoryEntry {
        session_id,
        text,
        timestamp: epoch_seconds_to_rfc3339(ts)?,
    })
}

fn extract_user_message_line(value: &Value) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some("response_item") {
        return None;
    }
    let payload = value.get("payload")?;
    if payload.get("type").and_then(Value::as_str) != Some("message") {
        return None;
    }
    if payload.get("role").and_then(Value::as_str) != Some("user") {
        return None;
    }
    let content = payload.get("content")?.as_array()?;
    for item in content {
        let text = item.get("text").and_then(Value::as_str)?;
        let trimmed = text.trim();
        if trimmed.is_empty()
            || trimmed.starts_with("# AGENTS.md instructions")
            || trimmed.starts_with("<environment_context>")
            || trimmed.starts_with("<INSTRUCTIONS>")
        {
            continue;
        }
        let first_line = trimmed.lines().next()?.trim();
        if !first_line.is_empty() {
            return Some(first_line.to_string());
        }
    }
    None
}

fn extract_title_candidate(value: &Value) -> Option<String> {
    extract_user_message_line(value).map(|line| truncate_chars(&line, 80))
}

fn truncate_chars(text: &str, limit: usize) -> String {
    let truncated: String = text.chars().take(limit).collect();
    if text.chars().count() > limit {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or(text).trim()
}

fn normalize_display_name(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let first_non_empty_line = trimmed
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or(trimmed);
    let collapsed = first_non_empty_line
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if collapsed.is_empty() {
        None
    } else {
        Some(collapsed)
    }
}

fn ambiguous_name_error(query: &str, matches: &[&SessionInfo]) -> anyhow::Error {
    let mut message = format!("name {query:?} matches multiple sessions:");
    for session in matches {
        message.push_str(&format!(
            "\n  {}  {}  {}",
            session.id,
            session.display_name,
            session.cwd.display()
        ));
    }
    anyhow!(message)
}

fn session_is_unnamed(session: &SessionInfo) -> bool {
    let name = session.display_name.trim();
    if name.is_empty() || name == UNTITLED_SESSION_NAME {
        return true;
    }

    let Some(first_user_message) = session
        .first_user_message
        .as_deref()
        .and_then(normalize_display_name)
    else {
        return false;
    };

    name == first_user_message || name == truncate_chars(&first_user_message, 80)
}

fn list_filter_from_args(args: &ListArgs) -> ListFilter {
    if args.unnamed {
        ListFilter::Unnamed
    } else if args.named {
        ListFilter::Named
    } else {
        ListFilter::All
    }
}

fn session_matches_list_filter(filter: ListFilter, session: &SessionInfo) -> bool {
    match filter {
        ListFilter::All => true,
        ListFilter::Named => !session_is_unnamed(session),
        ListFilter::Unnamed => session_is_unnamed(session),
    }
}

fn refresh_display_name(session: &mut SessionInfo) {
    session.display_name = preferred_name(session)
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| UNTITLED_SESSION_NAME.to_string());
}

fn preferred_name(session: &SessionInfo) -> Option<String> {
    session
        .indexed_name
        .as_deref()
        .and_then(normalize_display_name)
        .or_else(|| {
            session
                .state_title
                .as_deref()
                .and_then(normalize_display_name)
        })
        .or_else(|| {
            session
                .derived_name
                .as_deref()
                .and_then(normalize_display_name)
        })
        .or_else(|| {
            session
                .first_user_message
                .as_deref()
                .and_then(normalize_display_name)
                .map(|line| truncate_chars(&line, 80))
        })
}

fn apply_index_entry(session: &mut SessionInfo, entry: &IndexEntry) {
    if !entry.thread_name.trim().is_empty() {
        session.indexed_name = Some(entry.thread_name.clone());
    }
    if session.timestamp.as_deref() < Some(entry.updated_at.as_str()) {
        session.timestamp = Some(entry.updated_at.clone());
    }
}

fn apply_history_entry(session: &mut SessionInfo, entry: &HistorySessionInfo) {
    if session.first_user_message.is_none() {
        session.first_user_message = entry.first_user_message.clone();
    }
    if session.derived_name.is_none() {
        session.derived_name = entry.derived_name.clone();
    }
    if session.timestamp.as_deref() < entry.timestamp.as_deref() {
        session.timestamp = entry.timestamp.clone();
    }
}

fn merge_sessions(existing: &mut SessionInfo, incoming: &SessionInfo) {
    existing.paths.extend(incoming.paths.iter().cloned());
    existing.paths.sort();
    existing.paths.dedup();

    if existing.primary_rollout_path.is_none() && incoming.primary_rollout_path.is_some() {
        existing.primary_rollout_path = incoming.primary_rollout_path.clone();
    }
    if existing.cwd == PathBuf::from("(unknown)") && incoming.cwd != PathBuf::from("(unknown)") {
        existing.cwd = incoming.cwd.clone();
    }
    if existing.first_user_message.is_none() && incoming.first_user_message.is_some() {
        existing.first_user_message = incoming.first_user_message.clone();
    }

    let incoming_newer = incoming.timestamp > existing.timestamp;
    if incoming_newer {
        existing.primary_rollout_path = incoming.primary_rollout_path.clone();
        existing.cwd = incoming.cwd.clone();
        existing.timestamp = incoming.timestamp.clone();
        existing.source = incoming.source.clone();
        existing.model_provider = incoming.model_provider.clone();
        existing.cli_version = incoming.cli_version.clone();
        existing.sandbox_policy = incoming.sandbox_policy.clone();
        existing.approval_mode = incoming.approval_mode.clone();
        existing.first_user_message = incoming.first_user_message.clone();
    }

    if existing.derived_name.is_none() && incoming.derived_name.is_some() {
        existing.derived_name = incoming.derived_name.clone();
    }

    refresh_display_name(existing);
}

fn apply_state_thread(session: &mut SessionInfo, db_thread: &StateThreadInfo) {
    if !db_thread.title.trim().is_empty() {
        session.state_title = Some(db_thread.title.clone());
    }
    session.cwd = db_thread.cwd.clone();
    if session.primary_rollout_path.is_none() {
        session.primary_rollout_path = db_thread.rollout_path.clone();
    }
    if let Some(path) = &db_thread.rollout_path {
        session.paths.push(path.clone());
        session.paths.sort();
        session.paths.dedup();
    }
    if let Some(updated_at) = &db_thread.updated_at {
        if session.timestamp.as_deref() < Some(updated_at.as_str()) {
            session.timestamp = Some(updated_at.clone());
        }
    }
    session.source = Some(db_thread.source.clone());
    if !db_thread.first_user_message.trim().is_empty() {
        session.first_user_message = Some(db_thread.first_user_message.clone());
    }
    session.in_resume = !db_thread.archived;
    refresh_display_name(session);
}

fn prune_empty_parents(path: &Path, stop_at: &Path) -> Result<()> {
    let mut current = path.parent();
    while let Some(dir) = current {
        if dir == stop_at {
            break;
        }
        let mut entries = fs::read_dir(dir)?;
        if entries.next().is_some() {
            break;
        }
        fs::remove_dir(dir)?;
        current = dir.parent();
    }
    Ok(())
}

fn parse_timestamp_to_epoch_seconds(text: &str) -> Option<i64> {
    OffsetDateTime::parse(text, &Rfc3339)
        .ok()
        .map(|timestamp| timestamp.unix_timestamp())
}

fn current_unix_timestamp() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

fn current_rfc3339_timestamp() -> String {
    epoch_seconds_to_rfc3339(current_unix_timestamp())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

fn epoch_seconds_to_rfc3339(seconds: i64) -> Option<String> {
    OffsetDateTime::from_unix_timestamp(seconds)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
}

fn find_latest_state_db(codex_home: &Path) -> Result<Option<PathBuf>> {
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(codex_home)
        .with_context(|| format!("failed to read {}", codex_home.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension() != Some(OsStr::new("sqlite")) {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        let Some(version) = file_name
            .strip_prefix("state_")
            .and_then(|rest| rest.strip_suffix(".sqlite"))
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        match &best {
            Some((best_version, _)) if *best_version >= version => {}
            _ => best = Some((version, path)),
        }
    }
    Ok(best.map(|(_, path)| path))
}

#[cfg(test)]
mod tests {
    use super::{
        ListFilter, SessionInfo, UNTITLED_SESSION_NAME, extract_title_candidate,
        find_latest_state_db, parse_timestamp_to_epoch_seconds, refresh_display_name,
        session_is_unnamed, session_matches_list_filter, truncate_chars,
    };
    use serde_json::json;
    use std::collections::HashSet;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn extracts_user_title_from_response_item() {
        let value = json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{ "text": "帮我找带有SandCell的codex session有哪些" }]
            }
        });
        assert_eq!(
            extract_title_candidate(&value),
            Some("帮我找带有SandCell的codex session有哪些".to_string())
        );
    }

    #[test]
    fn truncates_long_titles() {
        let text = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789long";
        let truncated = truncate_chars(text, 10);
        assert_eq!(truncated, "abcdefghij...");
    }

    #[test]
    fn parses_rfc3339_timestamp() {
        assert_eq!(
            parse_timestamp_to_epoch_seconds("2026-03-19T03:21:22.078Z"),
            Some(1773890482)
        );
    }

    #[test]
    fn picks_latest_state_db_version() {
        let root = PathBuf::from("/tmp/codex-helper-state-db-test");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("state_3.sqlite"), "").unwrap();
        fs::write(root.join("state_5.sqlite"), "").unwrap();
        fs::write(root.join("logs_1.sqlite"), "").unwrap();

        let found = find_latest_state_db(&root).unwrap();
        assert_eq!(found, Some(root.join("state_5.sqlite")));

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn detects_unnamed_sessions() {
        let unnamed = SessionInfo {
            id: "1".to_string(),
            paths: Vec::new(),
            primary_rollout_path: Some(PathBuf::from("/tmp/rollout.jsonl")),
            cwd: PathBuf::from("/tmp"),
            timestamp: None,
            in_resume: false,
            indexed_name: None,
            state_title: None,
            derived_name: None,
            display_name: UNTITLED_SESSION_NAME.to_string(),
            source: None,
            model_provider: None,
            cli_version: None,
            sandbox_policy: None,
            approval_mode: None,
            first_user_message: None,
        };
        assert!(session_is_unnamed(&unnamed));

        let named = SessionInfo {
            display_name: "named".to_string(),
            ..unnamed
        };
        assert!(!session_is_unnamed(&named));
    }

    #[test]
    fn detects_auto_named_sessions() {
        let first_user_message = "this is a long user message that should become the automatic title for a local session";
        let auto_named = SessionInfo {
            id: "1".to_string(),
            paths: Vec::new(),
            primary_rollout_path: Some(PathBuf::from("/tmp/rollout.jsonl")),
            cwd: PathBuf::from("/tmp"),
            timestamp: None,
            in_resume: false,
            indexed_name: None,
            state_title: None,
            derived_name: None,
            display_name: truncate_chars(first_user_message, 80),
            source: None,
            model_provider: None,
            cli_version: None,
            sandbox_policy: None,
            approval_mode: None,
            first_user_message: Some(first_user_message.to_string()),
        };
        assert!(session_is_unnamed(&auto_named));

        let renamed = SessionInfo {
            display_name: "custom title".to_string(),
            ..auto_named
        };
        assert!(!session_is_unnamed(&renamed));
    }

    #[test]
    fn named_and_unnamed_partition_all_sessions_by_id() {
        let sessions = vec![
            SessionInfo {
                id: "untitled".to_string(),
                paths: Vec::new(),
                primary_rollout_path: Some(PathBuf::from("/tmp/untitled.jsonl")),
                cwd: PathBuf::from("/tmp"),
                timestamp: None,
                in_resume: false,
                indexed_name: None,
                state_title: None,
                derived_name: None,
                display_name: UNTITLED_SESSION_NAME.to_string(),
                source: None,
                model_provider: None,
                cli_version: None,
                sandbox_policy: None,
                approval_mode: None,
                first_user_message: None,
            },
            SessionInfo {
                id: "auto".to_string(),
                paths: Vec::new(),
                primary_rollout_path: Some(PathBuf::from("/tmp/auto.jsonl")),
                cwd: PathBuf::from("/tmp"),
                timestamp: None,
                in_resume: true,
                indexed_name: None,
                state_title: None,
                derived_name: None,
                display_name: "auto title".to_string(),
                source: None,
                model_provider: None,
                cli_version: None,
                sandbox_policy: None,
                approval_mode: None,
                first_user_message: Some("auto title".to_string()),
            },
            SessionInfo {
                id: "named".to_string(),
                paths: Vec::new(),
                primary_rollout_path: Some(PathBuf::from("/tmp/named.jsonl")),
                cwd: PathBuf::from("/tmp"),
                timestamp: None,
                in_resume: true,
                indexed_name: None,
                state_title: None,
                derived_name: None,
                display_name: "custom title".to_string(),
                source: None,
                model_provider: None,
                cli_version: None,
                sandbox_policy: None,
                approval_mode: None,
                first_user_message: Some("original prompt".to_string()),
            },
        ];

        let collect_ids = |filter| -> HashSet<String> {
            sessions
                .iter()
                .filter(|session| session_matches_list_filter(filter, session))
                .map(|session| session.id.clone())
                .collect()
        };

        let all_ids = collect_ids(ListFilter::All);
        let named_ids = collect_ids(ListFilter::Named);
        let unnamed_ids = collect_ids(ListFilter::Unnamed);

        assert!(named_ids.is_disjoint(&unnamed_ids));

        let union_ids: HashSet<String> = named_ids.union(&unnamed_ids).cloned().collect();
        assert_eq!(union_ids, all_ids);
    }

    #[test]
    fn display_name_prefers_all_name_sources_in_priority_order() {
        let mut session = SessionInfo {
            id: "1".to_string(),
            paths: Vec::new(),
            primary_rollout_path: None,
            cwd: PathBuf::from("/tmp"),
            timestamp: None,
            in_resume: false,
            indexed_name: Some(" index\tname \nsecond line".to_string()),
            state_title: Some(" state title \nsecond line".to_string()),
            derived_name: Some(" derived\tname ".to_string()),
            display_name: UNTITLED_SESSION_NAME.to_string(),
            source: None,
            model_provider: None,
            cli_version: None,
            sandbox_policy: None,
            approval_mode: None,
            first_user_message: Some(" first user message \nsecond line".to_string()),
        };
        refresh_display_name(&mut session);
        assert_eq!(session.display_name, "index name");

        session.indexed_name = None;
        refresh_display_name(&mut session);
        assert_eq!(session.display_name, "state title");

        session.state_title = None;
        refresh_display_name(&mut session);
        assert_eq!(session.display_name, "derived name");

        session.derived_name = None;
        refresh_display_name(&mut session);
        assert_eq!(session.display_name, "first user message");
    }
}
