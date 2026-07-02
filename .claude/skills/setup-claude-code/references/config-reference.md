# Claude Code configuration reference

Exact formats and field names for the files the setup skill writes. Read the
section you need; don't load the whole thing. When in doubt, prefer the fields
shown here over inventing new ones — an unknown key is silently ignored at best.

## Table of contents
1. [.claude/settings.json — permissions](#1-claudesettingsjson--permissions)
2. [.claude/settings.json — hooks](#2-claudesettingsjson--hooks)
3. [.claude/agents/*.md — subagents](#3-claudeagentsmd--subagents)
4. [.claude/skills/<name>/SKILL.md — skills](#4-claudeskillsnameskillmd--skills)
5. [.mcp.json — project MCP servers](#5-mcpjson--project-mcp-servers)
6. [File scopes & what to commit](#6-file-scopes--what-to-commit)

---

## 1. .claude/settings.json — permissions

```json
{
  "$schema": "https://json.schemastore.org/claude-code-settings.json",
  "permissions": {
    "allow": [
      "Bash(pnpm test:*)",
      "Bash(pnpm lint)",
      "Bash(pnpm tsc:*)",
      "Bash(pnpm prettier:*)",
      "Bash(git status)",
      "Bash(git diff:*)",
      "Bash(git log:*)"
    ],
    "deny": [
      "Read(./.env)",
      "Read(./.env.local)",
      "Read(./.env.*.local)",
      "Read(./secrets/**)"
    ],
    "ask": []
  }
}
```

### Per-language allow examples

Derive these from the project's real manifest, not from this list verbatim.

```jsonc
// Python (pyproject.toml: pytest, ruff, mypy)
"Bash(pytest:*)", "Bash(ruff check:*)", "Bash(ruff format:*)", "Bash(mypy:*)"

// Go (Makefile wrapping go tooling)
"Bash(make build)", "Bash(make test)", "Bash(make lint)",
"Bash(go build:*)", "Bash(go test:*)", "Bash(go vet:*)", "Bash(gofmt:*)"

// Rust (Cargo.toml)
"Bash(cargo build:*)", "Bash(cargo test:*)", "Bash(cargo clippy:*)", "Bash(cargo fmt:*)"
```

### Permission-pattern syntax

Rules are `Tool(argument-pattern)`; a bare `ToolName` matches every use of it.

| Pattern | Matches |
| --- | --- |
| `Bash(pnpm test:*)` | `pnpm test` and any sub-command/args after it |
| `Bash(git diff:*)` | any `git diff …` invocation |
| `Bash(npm run lint)` | exactly that command, no args |
| `Read(./secrets/**)` | reads anywhere under `secrets/` |
| `Edit(src/**/*.ts)` | edits to TypeScript files under `src/` |
| `WebFetch` | any web fetch (no argument) |

- `*` matches within a path segment; `**` matches across directories.
- `:` after a command means "this command plus anything after it" — prefer it
  over a trailing `*` for Bash command prefixes.
- **`deny` always beats `allow` and `ask`.** Use `deny` for things that must
  never happen regardless of other rules.
- Keep `allow` scoped to commands proven safe. Never add `Bash(*)`,
  `Bash(rm:*)`, `Bash(curl:*)`, or writes outside the repo to a committed file.

### Deriving rules: edge cases that bite

A permission rule only matches the **literal command string** the user runs, so
match how the project is actually invoked, not an idealized form:

- **Bare binary vs `:*`.** Use `Bash(pytest:*)` so flags and paths are covered.
  Use a bare `Bash(pnpm lint)` only for a command that's always run with no args;
  if the user might run `pnpm lint src/`, it won't match — prefer `:*`.
- **`python -m` invocations.** If the team runs `python -m pytest` /
  `python -m mypy` (common, since it doesn't depend on console-script shims being
  on PATH), allowlist `Bash(python -m pytest:*)` — `Bash(pytest:*)` will *not*
  match it. When in doubt, add both.
- **Wrapper vs underlying command.** When a `Makefile`/`justfile` wraps the real
  tools, list the **specific targets** (`Bash(make test)`, `Bash(make lint)`),
  not `Bash(make:*)` — a broad `make:*` is close to a blank check because recipes
  run arbitrary code. If the user also runs the underlying tools directly, add
  those too (`Bash(go test:*)`).
- **Don't blanket-deny `.env` templates.** `Read(./.env.*)` looks tidy but also
  blocks `.env.example` / `.env.sample` / `.env.template` — committed
  placeholders Claude *should* read. Because deny always wins, you can't re-allow
  them. Deny the actual secret files instead: `Read(./.env)`,
  `Read(./.env.local)`, `Read(./.env.*.local)`, `Read(./secrets/**)`.

### Other useful keys (add only with a concrete reason)

- `"env": { "NODE_ENV": "development" }` — environment variables for the session.
- `"model": "claude-sonnet-4-6"` — pin a model for this project.
- Leave these out by default. Empty/absent beats speculative.

---

## 2. .claude/settings.json — hooks

Hooks are deterministic scripts run at lifecycle points. Use them for things that
must happen *every* time (a CLAUDE.md note is only advisory; a hook is enforced).

Auto-format edited files after every Edit/Write:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Edit|Write",
        "hooks": [
          {
            "type": "command",
            "command": "jq -r '.tool_input.file_path' | xargs npx prettier --write",
            "timeout": 30
          }
        ]
      }
    ]
  }
}
```

- Common events: `PreToolUse`, `PostToolUse`, `UserPromptSubmit`,
  `SessionStart`, `Stop`, `Notification`.
- `matcher` is the tool name or a regex like `Edit|Write`; some events take no
  matcher.
- Each entry in `hooks` has `type: "command"` and a shell `command` (optionally
  `timeout` in seconds). The tool-call JSON is piped to the command on stdin.
- A `command` hook that exits with code **2** blocks the action and feeds stderr
  back to Claude; exit **0** lets it proceed.
- The user can also just ask Claude to "write a hook that runs eslint after every
  edit" — Claude Code can author hooks interactively, and `/hooks` browses them.

---

## 3. .claude/agents/*.md — subagents

A subagent runs in its own context with its own tools/model. One file per agent.

```markdown
---
name: security-reviewer
description: Reviews code for security vulnerabilities. Use after auth changes.
tools: Read, Grep, Glob, Bash
model: opus
---

You are a senior security engineer. Review code for:
- Injection vulnerabilities (SQL, XSS, command injection)
- Authentication and authorization flaws
- Secrets or credentials committed to the repo
- Insecure data handling

Provide specific line references and suggested fixes.
```

Frontmatter fields:

| Field | Required | Notes |
| --- | --- | --- |
| `name` | yes | unique identifier |
| `description` | yes | when Claude should delegate to it |
| `tools` | no | allowlist (e.g. `Read, Grep, Glob, Bash`); omit to inherit all |
| `model` | no | `sonnet` \| `opus` \| `haiku` \| `inherit` \| a full model id |

The body is the agent's system prompt. Don't recreate the bundled `/code-review`
skill as an agent — add a custom one only for a specialized, stated need.

---

## 4. .claude/skills/<name>/SKILL.md — skills

Skills load on demand when relevant, so they're where "sometimes relevant"
knowledge or repeatable workflows belong (not CLAUDE.md).

Domain knowledge (model-invoked automatically):

```markdown
---
name: api-conventions
description: REST API design conventions for our services
---

# API Conventions
- Use kebab-case for URL paths
- Use camelCase for JSON properties
- Always paginate list endpoints
- Version APIs in the URL path (/v1/, /v2/)
```

Repeatable workflow (user-invoked with `/fix-issue 1234`):

```markdown
---
name: fix-issue
description: Fix a GitHub issue
disable-model-invocation: true
---

Analyze and fix the GitHub issue: $ARGUMENTS.

1. Use `gh issue view` to get the issue details
2. Search the codebase for relevant files
3. Implement the fix
4. Write and run tests to verify
5. Ensure lint and typecheck pass
6. Commit, push, and open a PR
```

- `name` and `description` are the required frontmatter; `description` is the
  trigger, so make it specific.
- `disable-model-invocation: true` makes a skill **only** user-invocable — use it
  for side-effecting workflows you want to trigger manually.
- `$ARGUMENTS` (all args) and `$1`, `$2` … (positional) are substituted at
  invocation. Files can be referenced with `@path`; `` !`cmd` `` injects command
  output.
- A skill can bundle `scripts/`, `references/`, and `assets/` next to SKILL.md
  and point to them — keep SKILL.md itself lean (progressive disclosure).

---

## 5. .mcp.json — project MCP servers

Project-scoped MCP servers live in `.mcp.json` at the repo root (committed, shared
with the team). Add only servers the project genuinely uses.

```json
{
  "mcpServers": {
    "postgres": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@bytebase/dbhub", "--dsn", "${DATABASE_URL}"]
    },
    "sentry": {
      "type": "http",
      "url": "https://mcp.sentry.dev/mcp",
      "headers": { "Authorization": "Bearer ${SENTRY_TOKEN}" }
    }
  }
}
```

- `type: "stdio"` spawns a local process (`command` + `args`, optional `env`).
- `type: "http"` connects to a remote server (`url`, optional `headers`).
- `${VAR}` expands from the environment — reference secrets this way, never inline
  them. The easiest path is often `claude mcp add …`, which writes this for you.

---

## 6. File scopes & what to commit

| File | Scope | Commit to git? |
| --- | --- | --- |
| `CLAUDE.md` (root or `.claude/`) | team, shared | ✅ yes |
| `CLAUDE.local.md` | personal project notes | ❌ gitignore |
| `.claude/settings.json` | team, shared | ✅ yes |
| `.claude/settings.local.json` | personal / machine-specific | ❌ gitignore |
| `.claude/agents/`, `.claude/skills/` | team, shared | ✅ yes |
| `.mcp.json` | team, shared | ✅ yes |
| `~/.claude/CLAUDE.md`, `~/.claude/settings.json` | your user, all projects | n/a (outside repo) |

A skill or settings file that's meant to apply to **every** repo you work in goes
in `~/.claude/` (user scope), not in a project. The setup skill itself can be
copied to `~/.claude/skills/setup-claude-code/` so it's available everywhere.
