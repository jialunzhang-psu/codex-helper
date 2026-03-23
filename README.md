# codex-helper

`codex-helper` is a small Rust CLI for inspecting and cleaning local Codex sessions stored under `~/.codex`.

It is useful for:

- listing local sessions and seeing whether they are still in the resume list
- restoring a session back into the resume list
- removing sessions by id, by name, by unnamed status, or by "not in resume"

## Commands

Run everything through Cargo:

```bash
cargo run -- --help
```

Main commands:

- `cargo run -- list`
- `cargo run -- list --named`
- `cargo run -- list --unnamed`
- `cargo run -- add-resume <SESSION_ID>`
- `cargo run -- add-resume <SESSION_ID> --name "Custom title"`
- `cargo run -- clean --id <SESSION_ID> --dry-run`
- `cargo run -- clean --name "partial or exact name" --dry-run`
- `cargo run -- clean --unnamed --dry-run`
- `cargo run -- clean-not-in-resume --dry-run`

All commands also accept:

```bash
--codex-home /path/to/.codex
```

If you want a compiled binary instead of `cargo run`:

```bash
cargo build --release
./target/release/codex_helper --help
```

## How Sessions Are Collected

The tool merges session data from four local Codex sources:

1. `~/.codex/sessions/**/*.jsonl`
   These rollout files are the primary source of session ids, cwd, timestamps, first user message, model provider, and `cli_version`.
2. `~/.codex/session_index.jsonl`
   This is used for manually indexed resume entries and explicit thread names.
3. `~/.codex/history.jsonl`
   This is used as a fallback source for first user messages and derived names.
4. The newest `~/.codex/state_*.sqlite`
   This is used for current thread titles, archived state, resume membership, rollout paths, and other thread metadata.

Sessions are discovered by walking `~/.codex/sessions` recursively and reading every `.jsonl` file found there.

## How Session Names Are Chosen

The displayed session name is chosen in this order:

1. `thread_name` from `session_index.jsonl`
2. `title` from the `threads` table in the latest `state_*.sqlite`
3. a derived title from the first user message in the session rollout
4. a truncated first user message from `history.jsonl`
5. `(untitled)` if none of the above exists

The tool treats a session as "unnamed" if its name is empty, `(untitled)`, or still matches the auto-generated first user message rather than a user-assigned title.

## Codex Version

It works with codex-cli 0.116.0.
```

