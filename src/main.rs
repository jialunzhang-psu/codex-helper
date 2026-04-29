use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
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
        Command::List(args) => store.list_sessions(&args),
        Command::Remove(args) => store.remove_sessions(&args),
        Command::Restore(args) => store.add_resume(&args),
    }
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Inspect, filter, restore, and remove local Codex sessions"
)]
struct Cli {
    #[arg(long, value_name = "PATH", global = true)]
    codex_home: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(visible_alias = "ls")]
    List(ListArgs),
    #[command(visible_alias = "rm", visible_alias = "clean")]
    Remove(RemoveArgs),
    #[command(visible_alias = "resume", visible_alias = "add-resume")]
    Restore(AddResumeArgs),
}

#[derive(Args, Debug)]
struct AddResumeArgs {
    #[arg(value_name = "SESSION_ID")]
    session_id: String,
    #[arg(long, value_name = "TITLE")]
    name: Option<String>,
}

#[derive(Args, Debug)]
struct SessionFilterArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Only include sessions whose workspace matches this file or directory path"
    )]
    path: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct RemoveArgs {
    #[command(flatten)]
    filter: SessionFilterArgs,
    #[arg(long, conflicts_with_all = ["name", "unnamed", "not_in_resume", "pick"])]
    id: Option<String>,
    #[arg(long, value_name = "TEXT", conflicts_with_all = ["id", "unnamed", "not_in_resume", "pick"])]
    name: Option<String>,
    #[arg(long, conflicts_with_all = ["id", "name", "not_in_resume", "pick"])]
    unnamed: bool,
    #[arg(long, conflicts_with_all = ["id", "name", "unnamed", "pick"])]
    not_in_resume: bool,
    #[arg(long, conflicts_with_all = ["id", "name", "unnamed", "not_in_resume"])]
    pick: bool,
    #[arg(
        long,
        short = 'y',
        help = "Delete without interactive confirmation after printing the matched sessions"
    )]
    yes: bool,
}

#[derive(Args, Debug)]
struct ListArgs {
    #[command(flatten)]
    filter: SessionFilterArgs,
    #[arg(long, conflicts_with = "named")]
    unnamed: bool,
    #[arg(long, conflicts_with = "unnamed")]
    named: bool,
    #[arg(long, conflicts_with = "archived")]
    active: bool,
    #[arg(long, conflicts_with = "active")]
    archived: bool,
    #[arg(long, conflicts_with = "not_in_resume")]
    in_resume: bool,
    #[arg(long, conflicts_with = "in_resume")]
    not_in_resume: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListFilter {
    All,
    Named,
    Unnamed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArchiveFilter {
    All,
    ActiveOnly,
    ArchivedOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SessionScope {
    mode: SessionScopeMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SessionScopeMode {
    ProjectRoot(PathBuf),
    ExactDirectory(PathBuf),
}

struct Store {
    _codex_home: PathBuf,
    sessions_root: PathBuf,
    archived_sessions_root: PathBuf,
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
            archived_sessions_root: root.join("archived_sessions"),
            session_index_path: root.join("session_index.jsonl"),
            history_path: root.join("history.jsonl"),
            _codex_home: root,
            state_db_path,
        })
    }

    fn list_sessions(&self, args: &ListArgs) -> Result<()> {
        let scope = session_scope_from_args(&args.filter)?;
        let filter = list_filter_from_args(args);
        let archive_filter = archive_filter_from_args(args.active, args.archived);
        let sessions: Vec<SessionInfo> = self
            .load_sessions()?
            .into_iter()
            .filter(|session| session_matches_scope(scope.as_ref(), session))
            .filter(|session| session_matches_list_filter(filter, session))
            .filter(|session| session_matches_archive_filter(archive_filter, session))
            .filter(|session| !args.in_resume || session.in_resume)
            .filter(|session| !args.not_in_resume || !session.in_resume)
            .collect();
        if sessions.is_empty() {
            println!("no sessions found");
            return Ok(());
        }
        println!("IN_RESUME\tARCHIVED\tSESSION_ID\tNAME\tCWD");
        for session in sessions {
            let in_resume = if session.in_resume { "yes" } else { "no" };
            let archived = if session.archived { "yes" } else { "no" };
            println!(
                "{in_resume}\t{archived}\t{}\t{}\t{}",
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

    fn remove_sessions(&self, args: &RemoveArgs) -> Result<()> {
        let scope = session_scope_from_args(&args.filter)?;
        let sessions: Vec<SessionInfo> = self
            .load_sessions()?
            .into_iter()
            .filter(|session| session_matches_scope(scope.as_ref(), session))
            .collect();
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
        } else if args.not_in_resume {
            sessions
                .iter()
                .filter(|session| !session.in_resume)
                .map(|session| session.id.clone())
                .collect()
        } else if args.pick {
            self.pick_sessions(&sessions)?
        } else {
            bail!("choose one selector: --id, --name, --unnamed, --not-in-resume, or --pick");
        };

        if args.unnamed && target_ids.is_empty() {
            println!("no unnamed sessions found");
            return Ok(());
        }
        if args.not_in_resume && target_ids.is_empty() {
            println!("no sessions outside the resume list found");
            return Ok(());
        }
        if args.pick && target_ids.is_empty() {
            return Ok(());
        }

        self.clean_ids(&target_ids, args.yes)
    }

    fn clean_ids(&self, target_ids: &[String], assume_yes: bool) -> Result<()> {
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
                "remove {}\t{}\t{}\t{}",
                session.id,
                session.display_name,
                if session.archived {
                    "archived"
                } else {
                    "active"
                },
                session.cwd.display()
            );
        }

        if !confirm_removal(&victims, assume_yes)? {
            println!("deletion cancelled");
            return Ok(());
        }

        for session in &victims {
            for path in &session.paths {
                if path.exists() {
                    fs::remove_file(path)
                        .with_context(|| format!("failed to remove {}", path.display()))?;
                    if let Some(stop_at) = self.prune_stop_root(path) {
                        prune_empty_parents(path, stop_at)?;
                    }
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

        for root in [&self.sessions_root, &self.archived_sessions_root] {
            if !root.exists() {
                continue;
            }
            for entry in WalkDir::new(root)
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
        }

        for (id, db_thread) in &db_threads {
            let session = sessions_by_id
                .entry(id.clone())
                .or_insert_with(|| SessionInfo::placeholder(id));
            apply_state_thread(session, db_thread);
        }

        for (id, entry) in &index_entries {
            if let Some(session) = sessions_by_id.get_mut(id) {
                apply_index_entry(session, entry);
            }
        }

        for (id, entry) in &history_sessions {
            if let Some(session) = sessions_by_id.get_mut(id) {
                apply_history_entry(session, entry);
            }
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
        let archived = path
            .ancestors()
            .any(|ancestor| ancestor.file_name() == Some(OsStr::new("archived_sessions")));

        Ok(SessionInfo {
            id,
            paths: vec![path.to_path_buf()],
            primary_rollout_path: Some(path.to_path_buf()),
            cwd,
            timestamp,
            in_resume: false,
            archived,
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
        let mut delete_tools_stmt = if sqlite_table_exists(&connection, "thread_dynamic_tools")? {
            Some(connection.prepare("DELETE FROM thread_dynamic_tools WHERE thread_id = ?1")?)
        } else {
            None
        };
        let mut delete_spawn_edges_stmt = if sqlite_table_exists(&connection, "thread_spawn_edges")?
        {
            Some(connection.prepare(
                "DELETE FROM thread_spawn_edges WHERE child_thread_id = ?1 OR parent_thread_id = ?1",
            )?)
        } else {
            None
        };
        for session_id in target_ids {
            if let Some(stmt) = delete_tools_stmt.as_mut() {
                stmt.execute(params![session_id])?;
            }
            if let Some(stmt) = delete_spawn_edges_stmt.as_mut() {
                stmt.execute(params![session_id])?;
            }
            delete_thread_stmt.execute(params![session_id])?;
        }
        Ok(())
    }

    fn prune_stop_root<'a>(&'a self, path: &Path) -> Option<&'a Path> {
        if path.starts_with(&self.sessions_root) {
            return Some(self.sessions_root.as_path());
        }
        if path.starts_with(&self.archived_sessions_root) {
            return Some(self.archived_sessions_root.as_path());
        }
        None
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

    fn pick_sessions(&self, sessions: &[SessionInfo]) -> Result<Vec<String>> {
        if sessions.is_empty() {
            println!("no sessions found");
            return Ok(Vec::new());
        }
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            bail!("--pick requires an interactive terminal");
        }

        println!("NUM\tIN_RESUME\tARCHIVED\tSESSION_ID\tNAME\tCWD");
        for (index, session) in sessions.iter().enumerate() {
            let in_resume = if session.in_resume { "yes" } else { "no" };
            let archived = if session.archived { "yes" } else { "no" };
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                index + 1,
                in_resume,
                archived,
                session.id,
                session.display_name,
                session.cwd.display()
            );
        }

        let selected_indexes = loop {
            let input = prompt_line(
                "Select sessions to remove by number (for example: 1 3 5-8). Press Enter to cancel: ",
            )?;
            let trimmed = input.trim();
            if trimmed.is_empty() {
                println!("selection cancelled");
                return Ok(Vec::new());
            }

            match parse_pick_selection(trimmed, sessions.len()) {
                Ok(indexes) => break indexes,
                Err(error) => eprintln!("invalid selection: {error:#}"),
            }
        };

        println!("selected sessions:");
        for index in &selected_indexes {
            let session = &sessions[*index - 1];
            println!(
                "{}\t{}\t{}\t{}",
                index,
                session.id,
                session.display_name,
                session.cwd.display()
            );
        }

        Ok(selected_indexes
            .into_iter()
            .map(|index| sessions[index - 1].id.clone())
            .collect())
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
    archived: bool,
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
            archived: false,
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

fn prompt_line(prompt: &str) -> Result<String> {
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input)
}

fn parse_pick_selection(input: &str, max: usize) -> Result<Vec<usize>> {
    if max == 0 {
        return Ok(Vec::new());
    }

    if input.eq_ignore_ascii_case("all") || input == "*" {
        return Ok((1..=max).collect());
    }

    let mut selected = HashSet::new();
    for token in input.split(|ch: char| ch == ',' || ch.is_whitespace()) {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }

        if let Some((start, end)) = token.split_once('-') {
            let start = parse_pick_index(start.trim(), max)?;
            let end = parse_pick_index(end.trim(), max)?;
            let (from, to) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            for index in from..=to {
                selected.insert(index);
            }
            continue;
        }

        selected.insert(parse_pick_index(token, max)?);
    }

    if selected.is_empty() {
        bail!("no valid session numbers were provided");
    }

    let mut indexes: Vec<usize> = selected.into_iter().collect();
    indexes.sort_unstable();
    Ok(indexes)
}

fn parse_pick_index(token: &str, max: usize) -> Result<usize> {
    let index: usize = token
        .parse()
        .with_context(|| format!("invalid session number {token:?}"))?;
    if index == 0 {
        bail!("session numbers start at 1");
    }
    if index > max {
        bail!("session number {index} is out of range 1..={max}");
    }
    Ok(index)
}

fn session_scope_from_args(args: &SessionFilterArgs) -> Result<Option<SessionScope>> {
    args.path.as_deref().map(resolve_session_scope).transpose()
}

fn resolve_session_scope(path: &Path) -> Result<SessionScope> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed to resolve current directory")?
            .join(path)
    };
    let metadata = fs::metadata(&absolute)
        .with_context(|| format!("path does not exist: {}", absolute.display()))?;
    let scope_root = if metadata.is_dir() {
        absolute
    } else {
        absolute
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| anyhow!("file path has no parent: {}", path.display()))?
    };
    let mode = if let Some(workspace_root) = find_workspace_root(&scope_root) {
        let canonical = fs::canonicalize(&workspace_root)
            .with_context(|| format!("failed to resolve {}", workspace_root.display()))?;
        SessionScopeMode::ProjectRoot(canonical)
    } else {
        let canonical = fs::canonicalize(&scope_root)
            .with_context(|| format!("failed to resolve {}", scope_root.display()))?;
        SessionScopeMode::ExactDirectory(canonical)
    };
    Ok(SessionScope { mode })
}

fn session_matches_scope(scope: Option<&SessionScope>, session: &SessionInfo) -> bool {
    let Some(scope) = scope else {
        return true;
    };
    let cwd = canonicalize_for_matching(&session.cwd);
    match &scope.mode {
        SessionScopeMode::ProjectRoot(root) => find_workspace_root(&cwd)
            .map(|session_root| canonicalize_for_matching(&session_root) == *root)
            .unwrap_or(false),
        SessionScopeMode::ExactDirectory(dir) => cwd == *dir,
    }
}

fn canonicalize_for_matching(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn find_workspace_root(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        if has_workspace_marker(ancestor) {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

fn has_workspace_marker(path: &Path) -> bool {
    [
        ".git",
        ".hg",
        ".svn",
        "Cargo.toml",
        "package.json",
        "pyproject.toml",
        "go.mod",
    ]
    .iter()
    .any(|marker| path.join(marker).exists())
}

fn archive_filter_from_args(active_only: bool, archived_only: bool) -> ArchiveFilter {
    if active_only {
        ArchiveFilter::ActiveOnly
    } else if archived_only {
        ArchiveFilter::ArchivedOnly
    } else {
        ArchiveFilter::All
    }
}

fn session_matches_archive_filter(filter: ArchiveFilter, session: &SessionInfo) -> bool {
    match filter {
        ArchiveFilter::All => true,
        ArchiveFilter::ActiveOnly => !session.archived,
        ArchiveFilter::ArchivedOnly => session.archived,
    }
}

fn confirm_removal(victims: &[SessionInfo], assume_yes: bool) -> Result<bool> {
    if victims.is_empty() {
        return Ok(false);
    }
    if assume_yes {
        return Ok(true);
    }
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("removal requires confirmation; re-run with --yes in non-interactive mode");
    }
    println!();
    println!("about to delete {} session(s)", victims.len());
    let confirmation =
        prompt_line("Type DELETE to remove these sessions, or press Enter to cancel: ")?;
    Ok(confirmation.trim() == "DELETE")
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
        .state_title
        .as_deref()
        .and_then(normalize_display_name)
        .or_else(|| {
            session
                .indexed_name
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

fn sqlite_table_exists(connection: &Connection, table_name: &str) -> Result<bool> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
            params![table_name],
            |_| Ok(()),
        )
        .optional()?;
    Ok(exists.is_some())
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
    existing.archived &= incoming.archived;

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
        existing.archived = incoming.archived;
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
    session.archived = db_thread.archived;
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
        ListFilter, SessionInfo, SessionScope, SessionScopeMode, Store, UNTITLED_SESSION_NAME,
        extract_title_candidate, find_latest_state_db, parse_pick_selection,
        parse_timestamp_to_epoch_seconds, refresh_display_name, resolve_session_scope,
        session_is_unnamed, session_matches_list_filter, session_matches_scope, truncate_chars,
    };
    use rusqlite::Connection;
    use serde_json::json;
    use std::collections::HashSet;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!(
            "codex-cleaner-{name}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn write_rollout(root: &PathBuf, relative: &str, session_id: &str, cwd: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let contents = format!(
            "{{\"timestamp\":\"2026-04-28T12:00:00.000Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{session_id}\",\"cwd\":\"{cwd}\",\"timestamp\":\"2026-04-28T12:00:00.000Z\",\"source\":\"cli\",\"model_provider\":\"openai\",\"cli_version\":\"0.125.0\"}}}}\n\
             {{\"timestamp\":\"2026-04-28T12:00:01.000Z\",\"type\":\"response_item\",\"payload\":{{\"type\":\"message\",\"role\":\"user\",\"content\":[{{\"text\":\"hello from {session_id}\"}}]}}}}\n"
        );
        fs::write(path, contents).unwrap();
    }

    fn init_state_db(root: &PathBuf) -> PathBuf {
        let db_path = root.join("state_5.sqlite");
        let connection = Connection::open(&db_path).unwrap();
        connection
            .execute_batch(
                "
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    model_provider TEXT NOT NULL,
                    cwd TEXT NOT NULL,
                    title TEXT NOT NULL,
                    sandbox_policy TEXT NOT NULL,
                    approval_mode TEXT NOT NULL,
                    tokens_used INTEGER NOT NULL DEFAULT 0,
                    has_user_event INTEGER NOT NULL DEFAULT 0,
                    archived INTEGER NOT NULL DEFAULT 0,
                    archived_at INTEGER,
                    git_sha TEXT,
                    git_branch TEXT,
                    git_origin_url TEXT,
                    cli_version TEXT NOT NULL DEFAULT '',
                    first_user_message TEXT NOT NULL DEFAULT '',
                    agent_nickname TEXT,
                    agent_role TEXT,
                    memory_mode TEXT NOT NULL DEFAULT 'enabled',
                    model TEXT,
                    reasoning_effort TEXT,
                    agent_path TEXT,
                    created_at_ms INTEGER,
                    updated_at_ms INTEGER
                );
                CREATE TABLE thread_dynamic_tools (
                    thread_id TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    input_schema TEXT NOT NULL,
                    namespace TEXT,
                    defer_loading INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(thread_id, position)
                );
                CREATE TABLE thread_spawn_edges (
                    parent_thread_id TEXT NOT NULL,
                    child_thread_id TEXT NOT NULL PRIMARY KEY,
                    status TEXT NOT NULL
                );
                ",
            )
            .unwrap();
        db_path
    }

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
        let root = PathBuf::from("/tmp/codex-cleaner-state-db-test");
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
            archived: false,
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
            archived: false,
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
                archived: false,
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
                archived: false,
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
                archived: false,
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
            archived: false,
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
        assert_eq!(session.display_name, "state title");

        session.state_title = None;
        refresh_display_name(&mut session);
        assert_eq!(session.display_name, "index name");

        session.indexed_name = None;
        refresh_display_name(&mut session);
        assert_eq!(session.display_name, "derived name");

        session.derived_name = None;
        refresh_display_name(&mut session);
        assert_eq!(session.display_name, "first user message");
    }

    #[test]
    fn parse_pick_selection_accepts_individual_indexes_and_ranges() {
        let indexes = parse_pick_selection("1 3,5-7", 8).unwrap();
        assert_eq!(indexes, vec![1, 3, 5, 6, 7]);
    }

    #[test]
    fn parse_pick_selection_accepts_all_keyword() {
        let indexes = parse_pick_selection("all", 4).unwrap();
        assert_eq!(indexes, vec![1, 2, 3, 4]);
    }

    #[test]
    fn parse_pick_selection_rejects_out_of_range_values() {
        let error = parse_pick_selection("1 9", 8).unwrap_err().to_string();
        assert!(error.contains("out of range"));
    }

    #[test]
    fn load_sessions_ignores_history_and_index_entries_without_real_sessions() {
        let root = unique_test_dir("load-sessions-real-only");
        fs::create_dir_all(root.join("sessions/2026/04/28")).unwrap();
        write_rollout(
            &root,
            "sessions/2026/04/28/rollout-2026-04-28T12-00-00-real.jsonl",
            "real",
            "/tmp/real",
        );
        fs::write(
            root.join("session_index.jsonl"),
            "{\"id\":\"ghost\",\"thread_name\":\"ghost name\",\"updated_at\":\"2026-04-28T12:00:00Z\"}\n",
        )
        .unwrap();
        fs::write(
            root.join("history.jsonl"),
            "{\"session_id\":\"ghost\",\"ts\":1777387200,\"text\":\"ghost history\"}\n",
        )
        .unwrap();

        let store = Store::new(Some(root.clone())).unwrap();
        let sessions = store.load_sessions().unwrap();
        let ids: Vec<String> = sessions.into_iter().map(|session| session.id).collect();
        assert_eq!(ids, vec!["real".to_string()]);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_sessions_includes_archived_rollouts() {
        let root = unique_test_dir("load-archived-rollouts");
        fs::create_dir_all(root.join("archived_sessions/2026/04/28")).unwrap();
        write_rollout(
            &root,
            "archived_sessions/2026/04/28/rollout-2026-04-28T12-00-00-archived.jsonl",
            "archived",
            "/tmp/archived",
        );

        let store = Store::new(Some(root.clone())).unwrap();
        let sessions = store.load_sessions().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "archived");
        assert!(sessions[0].archived);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_sessions_prefers_state_db_title_over_legacy_index_name() {
        let root = unique_test_dir("title-priority");
        let rollout_path =
            root.join("sessions/2026/04/28/rollout-2026-04-28T12-00-00-thread-1.jsonl");
        write_rollout(
            &root,
            "sessions/2026/04/28/rollout-2026-04-28T12-00-00-thread-1.jsonl",
            "thread-1",
            "/tmp/thread-1",
        );
        init_state_db(&root);
        let connection = Connection::open(root.join("state_5.sqlite")).unwrap();
        connection
            .execute(
                "INSERT INTO threads (
                id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                sandbox_policy, approval_mode, tokens_used, has_user_event, archived, cli_version,
                first_user_message, memory_mode
             ) VALUES (?1, ?2, 1, 2, 'cli', 'openai', '/tmp/thread-1', 'state title',
                       '{}', 'never', 0, 0, 0, '0.125.0', 'first message', 'enabled')",
                rusqlite::params!["thread-1", rollout_path.display().to_string()],
            )
            .unwrap();
        fs::write(
            root.join("session_index.jsonl"),
            "{\"id\":\"thread-1\",\"thread_name\":\"legacy title\",\"updated_at\":\"2026-04-28T12:00:00Z\"}\n",
        ).unwrap();

        let store = Store::new(Some(root.clone())).unwrap();
        let sessions = store.load_sessions().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].display_name, "state title");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn clean_ids_removes_archived_rollouts_and_spawn_edges() {
        let root = unique_test_dir("clean-archived-and-spawn-edges");
        let archived_rollout =
            "archived_sessions/2026/04/28/rollout-2026-04-28T12-00-00-child.jsonl";
        write_rollout(&root, archived_rollout, "child", "/tmp/child");
        init_state_db(&root);
        let db_path = root.join("state_5.sqlite");
        let connection = Connection::open(&db_path).unwrap();
        let rollout_path = root.join(archived_rollout).display().to_string();
        connection
            .execute(
                "INSERT INTO threads (
                id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                sandbox_policy, approval_mode, tokens_used, has_user_event, archived, cli_version,
                first_user_message, memory_mode
             ) VALUES (?1, ?2, 1, 2, 'cli', 'openai', '/tmp/child', 'child title',
                       '{}', 'never', 0, 0, 1, '0.125.0', 'child message', 'enabled')",
                rusqlite::params!["child", rollout_path],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status)
             VALUES ('parent', 'child', 'active')",
                [],
            )
            .unwrap();

        let store = Store::new(Some(root.clone())).unwrap();
        store.clean_ids(&["child".to_string()], true).unwrap();

        assert!(!root.join(archived_rollout).exists());
        let connection = Connection::open(&db_path).unwrap();
        let remaining_threads: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM threads WHERE id = 'child'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining_threads, 0);
        let remaining_edges: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM thread_spawn_edges WHERE child_thread_id = 'child' OR parent_thread_id = 'child'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining_edges, 0);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_session_scope_uses_parent_directory_for_files() {
        let root = unique_test_dir("scope-from-file");
        let repo = root.join("repo");
        fs::create_dir_all(repo.join("src")).unwrap();
        fs::write(repo.join("src/lib.rs"), "fn main() {}\n").unwrap();
        fs::write(repo.join("Cargo.toml"), "[package]\nname = \"demo\"\n").unwrap();

        let scope = resolve_session_scope(&repo.join("src/lib.rs")).unwrap();
        assert_eq!(
            scope,
            SessionScope {
                mode: SessionScopeMode::ProjectRoot(fs::canonicalize(&repo).unwrap())
            }
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn session_scope_matches_repo_and_nested_worktree_paths() {
        let root = unique_test_dir("scope-match");
        let repo = root.join("repo");
        let nested = repo.join("subdir");
        let sibling = root.join("other");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(&sibling).unwrap();
        let tracked_file = nested.join("file.rs");
        fs::write(&tracked_file, "fn main() {}\n").unwrap();
        fs::write(repo.join("Cargo.toml"), "[package]\nname = \"demo\"\n").unwrap();

        let scope = resolve_session_scope(&repo).unwrap();
        let file_scope = resolve_session_scope(&tracked_file).unwrap();
        let repo_session = SessionInfo {
            id: "repo".to_string(),
            paths: Vec::new(),
            primary_rollout_path: None,
            cwd: repo.clone(),
            timestamp: None,
            in_resume: false,
            archived: false,
            indexed_name: None,
            state_title: None,
            derived_name: None,
            display_name: "repo".to_string(),
            source: None,
            model_provider: None,
            cli_version: None,
            sandbox_policy: None,
            approval_mode: None,
            first_user_message: None,
        };
        let nested_session = SessionInfo {
            id: "nested".to_string(),
            cwd: nested.clone(),
            display_name: "nested".to_string(),
            ..repo_session.clone()
        };
        let sibling_session = SessionInfo {
            id: "sibling".to_string(),
            cwd: sibling.clone(),
            display_name: "sibling".to_string(),
            ..repo_session.clone()
        };

        assert!(session_matches_scope(Some(&scope), &nested_session));
        assert!(session_matches_scope(Some(&file_scope), &repo_session));
        assert!(!session_matches_scope(Some(&scope), &sibling_session));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_session_scope_rejects_non_project_roots() {
        let root = unique_test_dir("scope-rejects-wide-dir");
        fs::create_dir_all(&root).unwrap();

        let scope = resolve_session_scope(&root).unwrap();
        assert_eq!(
            scope,
            SessionScope {
                mode: SessionScopeMode::ExactDirectory(fs::canonicalize(&root).unwrap())
            }
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_session_scope_treats_home_like_exact_directory_even_with_dot_codex() {
        let root = unique_test_dir("scope-rejects-home");
        let home = root.join("home");
        let project = home.join("project");
        fs::create_dir_all(home.join(".codex")).unwrap();
        fs::create_dir_all(project.join(".git")).unwrap();

        let original_home = env::var_os("HOME");
        unsafe {
            env::set_var("HOME", &home);
        }

        let home_scope = resolve_session_scope(&home).unwrap();
        assert_eq!(
            home_scope,
            SessionScope {
                mode: SessionScopeMode::ExactDirectory(fs::canonicalize(&home).unwrap())
            }
        );

        let scope = resolve_session_scope(&project).unwrap();
        assert_eq!(
            scope,
            SessionScope {
                mode: SessionScopeMode::ProjectRoot(fs::canonicalize(&project).unwrap())
            }
        );

        match original_home {
            Some(value) => unsafe { env::set_var("HOME", value) },
            None => unsafe { env::remove_var("HOME") },
        }

        fs::remove_dir_all(root).unwrap();
    }
}
