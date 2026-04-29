# codex-cleaner

`codex-cleaner` is a Rust CLI for inspecting, filtering, restoring, and removing local Codex sessions stored under `~/.codex`.

It is useful for:

- listing local sessions and seeing whether they are still in the resume list
- limiting results to the workspace, directory, or file path you care about
- restoring a session back into the resume list
- removing sessions by id, by name, by unnamed status, or by "not in resume"

## Commands

Run everything through Cargo:

```bash
cargo run -- --help
```

Main commands:

- `cargo run -- list`
- `cargo run -- ls --path ~/work/my-repo`
- `cargo run -- list --path ~/work/my-repo/src/lib.rs --named`
- `cargo run -- list --archived`
- `cargo run -- list --not-in-resume`
- `cargo run -- restore <SESSION_ID>`
- `cargo run -- restore <SESSION_ID> --name "Custom title"`
- `cargo run -- remove --pick`
- `cargo run -- remove --path ~/work/my-repo --name "partial or exact name"`
- `cargo run -- remove --unnamed`
- `cargo run -- remove --not-in-resume`
- `cargo run -- remove --id <SESSION_ID> --yes`

All commands also accept:

```bash
--codex-home /path/to/.codex
```

If you want a compiled binary instead of `cargo run`:

```bash
cargo build --release
./target/release/codex-cleaner --help
```

## How Sessions Are Collected

The tool merges session data from local Codex rollout storage and the state DB:

1. `~/.codex/sessions/**/*.jsonl`
   Active rollout files are a primary source of session ids, cwd, timestamps, first user message, model provider, and `cli_version`.
2. `~/.codex/archived_sessions/**/*.jsonl`
   Archived rollout files are scanned the same way so archived threads are included in listing and deletion.
3. The newest `~/.codex/state_*.sqlite`
   This is used for current thread titles, archived state, resume membership, rollout paths, and other thread metadata.
4. `~/.codex/session_index.jsonl`
   This is treated as a legacy sidecar index for explicit thread names, not as the source of truth for which sessions exist.
5. `~/.codex/history.jsonl`
   This is treated as a fallback source for first user messages and derived names for real sessions only.

Sessions are discovered from rollout files and state DB threads. `session_index.jsonl` and `history.jsonl` do not create standalone session rows by themselves.

## How Session Names Are Chosen

The displayed session name is chosen in this order:

1. `title` from the `threads` table in the latest `state_*.sqlite`
2. `thread_name` from `session_index.jsonl`
3. a derived title from the first user message in the session rollout
4. a truncated first user message from `history.jsonl`
5. `(untitled)` if none of the above exists

The tool treats a session as "unnamed" if its name is empty, `(untitled)`, or still matches the auto-generated first user message rather than a user-assigned title.

## Filtering by Path

`--path` accepts either a directory or a file path:

- if you pass a directory, sessions whose `cwd` is that directory or a nested directory are included
- if you pass a file, the file's parent directory is used as the filter root
- if a session was started at the repo root and you pass a file inside that repo, that session still matches

This makes it practical to answer questions like "show me the Codex sessions related to this repository" or "show me the sessions related to this file tree".

## Removal UX

`remove` always prints the matched sessions before deleting anything.

- interactive terminal: you must type `DELETE` to confirm
- non-interactive use: pass `--yes`
- `remove --pick` opens an interactive selector that accepts values like `1 3 5-8`

`list` prints both `IN_RESUME` and `ARCHIVED` so active resume membership and archived rollout state are visible separately.

Legacy aliases still work:

- `ls` for `list`
- `rm` and `clean` for `remove`
- `resume` and `add-resume` for `restore`

## Codex Version

It works with codex-cli 0.116.0.
