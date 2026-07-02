---
name: setup-claude-code
description: >-
  Set up or upgrade a repository for effective use with Claude Code, following
  Anthropic's official best practices. Detects the project's real toolchain and
  generates a concise CLAUDE.md, a tuned .claude/settings.json permission
  allowlist, correct .gitignore entries, and—only where they clearly fit—hooks,
  a review subagent, and gh/MCP guidance. Use this whenever the user wants to
  "set up Claude Code", "configure this repo for Claude Code", onboard a new
  repo, create or improve a CLAUDE.md, tune permissions or settings.json,
  scaffold the .claude directory, or asks for Claude Code best-practices setup—
  even when they don't name the specific files.
---

# Set up a repository for Claude Code

This skill configures a repo so Claude Code works well in it. The goal is **not**
to generate the maximum amount of configuration. It is to give Claude exactly the
context and guardrails it needs and nothing more.

This matters because every config file you add is loaded into context on future
sessions, and Claude's performance degrades as context fills. The official
guidance is blunt about it: a bloated CLAUDE.md causes Claude to *ignore* your
actual instructions because the important rules get lost in the noise. So the
bar for everything you write here is: **"Would removing this cause Claude to make
a mistake?" If not, cut it.**

Work through the steps below in order. Detect first, then write — everything you
generate should be grounded in what the repo actually contains, not in
boilerplate. For exact file formats and field names, read
`references/config-reference.md` rather than guessing.

## 0. Orient

- Confirm you're at the repo root (look for `.git`). If the user invoked this
  from a subdirectory, ask whether they want the repo root or that subdirectory.
- Check what already exists: `CLAUDE.md`, `.claude/`, `.mcp.json`, `.gitignore`,
  and whether the repo has a GitHub remote (`git remote -v`).

If config already exists, you are **upgrading**, not starting fresh. Two rules
govern every edit in the steps below:

- **Preserve real intent, cut the noise.** Keep the commands, conventions, and
  gotchas that earn their place; drop the boilerplate. If pruning line-by-line
  would remove most of the file, it's fine to rewrite it cleanly from your step-1
  detection — just carry over any genuine commands and project-specific rules the
  old file had.
- **Fact-check existing content against the actual repo.** Hand-written or stale
  config often makes claims that are no longer true — commands that don't exist,
  file-by-file lists that don't match what's on disk. Verify a claim before
  keeping it; a wrong instruction is worse than a missing one.

## 1. Detect the toolchain

The single most valuable thing you produce comes from knowing the repo's real
commands. Read the manifest(s) and extract the actual build / test / lint /
typecheck / format / run commands — do not assume `npm test` if the repo uses
`pnpm`, `make`, or `cargo`.

Look for, in roughly this priority:

- **JS/TS**: `package.json` → `scripts`, and the lockfile (`package-lock.json`,
  `pnpm-lock.yaml`, `yarn.lock`, `bun.lockb`) to pick the right runner.
- **Python**: `pyproject.toml`, `setup.cfg`, `tox.ini`, `noxfile.py`, `Makefile`,
  `requirements*.txt`. Note `ruff`, `pytest`, `mypy`, `poetry`/`uv`/`hatch`.
- **Rust**: `Cargo.toml` (`cargo build/test/clippy/fmt`).
- **Go**: `go.mod` (`go build/test/vet`, `golangci-lint`).
- **Other**: `Makefile`, `justfile`, `Taskfile.yml`, `composer.json`, `Gemfile`,
  `build.gradle`, `pom.xml`, `mix.exs`, etc.

Produce a short internal list like: `test = pnpm test`, `lint = pnpm lint`,
`typecheck = pnpm tsc --noEmit`, `format = pnpm prettier --write`. If a command
category is missing, note it — do not invent one. This list feeds steps 2 and 3.

## 2. CLAUDE.md — concise, every line earns its place

`CLAUDE.md` is read at the start of *every* session, so it is the highest-leverage
and also the easiest file to ruin by overloading. Place it at the **repo root**
(`./CLAUDE.md`) unless the repo already uses `.claude/CLAUDE.md` — both load, and
root is the common convention.

- If **no CLAUDE.md exists**: you have two equivalent options — tell the user to
  run the built-in `/init` (it analyzes the codebase), *or* draft a concise one
  yourself from your step-1 detection. Pick one. Don't do both: if you write the
  file, don't also tell them to run `/init`, since `/init` would overwrite it.
- If **one already exists**: prune and fact-check it per step 0. Move "sometimes
  relevant" domain knowledge into a skill (step 6) rather than leaving it in
  CLAUDE.md.

Use this include/exclude split (from the official best-practices guide):

| ✅ Include | ❌ Exclude |
| --- | --- |
| Bash commands Claude can't guess (the step-1 commands) | Anything Claude can figure out by reading the code |
| Code style rules that **differ** from language defaults | Standard conventions Claude already knows |
| Testing instructions & preferred test runner | Detailed API docs (link to them instead) |
| Repo etiquette (branch naming, PR/commit conventions) | Information that changes frequently |
| Architectural decisions specific to this project | File-by-file descriptions of the codebase |
| Dev-environment quirks (required env vars, setup steps) | Long tutorials or explanations |
| Common gotchas / non-obvious behaviors | Self-evident advice like "write clean code" |

Keep it tight — a focused file of a dozen-ish meaningful lines beats a long one.
A minimal good CLAUDE.md often looks like:

```markdown
# Commands
- Test: `pnpm test` (prefer a single test file over the full suite for speed)
- Lint + typecheck: `pnpm lint && pnpm tsc --noEmit` — run before considering work done
- Dev server: `pnpm dev`

# Conventions
- ES modules only (import/export), never require()
- Co-locate tests as `*.test.ts` next to the source file

# Gotchas
- `DATABASE_URL` must be set; copy `.env.example` to `.env` first
```

You can use `@path` imports (e.g. `See @README.md`) to pull in other files
without duplicating their content. Tell the user this file is checked into git so
the team benefits and it compounds in value — review and prune it when Claude
misbehaves.

## 3. Permissions — `.claude/settings.json`

By default Claude asks before each file write or Bash command, which gets tedious.
Reduce the friction by allowlisting the commands you *know* are safe — derived
from step 1 — without handing over a blank check.

- Build the `permissions.allow` list from the **detected** commands: the test,
  lint, typecheck, format, and build runners, plus safe read-only git
  (`git status`, `git diff`, `git log`). Use the real tool patterns, e.g.
  `Bash(pnpm test:*)`, `Bash(pytest:*)`, `Bash(cargo clippy:*)`, `Bash(make test)`.
- Standard language tooling (`gofmt`, `go vet`, `tsc`) is fine to allow even when
  it isn't a named project script. The "don't invent commands" rule from step 1
  is about not fabricating *project-specific* scripts, not about standard tools.
- Allowlisting a formatter that writes files (`prettier --write`, `ruff format`)
  is intended and fine — it only writes inside the repo. That's different from a
  blanket write grant.
- Leave out commands that block or reach outside the repo. Don't allowlist
  long-running targets (`dev`, `serve`, `run`, `watch`) — they hold the terminal
  and should be launched deliberately, not auto-run — or install/network commands
  (`pnpm install`, `pip install`, `go mod download`). Document those in CLAUDE.md
  instead; they don't meet the "proven-safe, repo-scoped, one-shot" bar.
- Keep a small `deny` list for secret-exposing or destructive actions. **Deny
  always wins over allow** — so don't blanket-deny `Read(./.env.*)`, which also
  blocks committed templates like `.env.example` that Claude legitimately needs.
  Deny the real secret files specifically (see the reference).
- Do **not** broadly allow `Bash(*)`, `Bash(make:*)` (recipes run arbitrary
  code), or anything that writes outside the repo. The point is fewer prompts on
  safe paths, not removing the guardrails.
- `.claude/settings.json` is **committed** (team-shared);
  `.claude/settings.local.json` is **personal and gitignored** — put
  machine-specific or experimental rules there.

See `references/config-reference.md` for per-language permission examples, the
exact pattern syntax, and the wrapper-vs-underlying-command and `python -m` cases.
Don't add `env`, `model`, or `hooks` keys here without a concrete reason — absent
beats speculative.

## 4. .gitignore hygiene

Make sure personal/local Claude files are not committed. Add (if missing):

```gitignore
.claude/settings.local.json
CLAUDE.local.md
```

Leave the shared files tracked: `CLAUDE.md`, `.claude/settings.json`,
`.claude/agents/`, `.claude/skills/`, and `.mcp.json`. If the repo has no
`.gitignore` at all, create one with just these entries (don't generate a giant
language-template gitignore unasked).

## 5. External tools — gh CLI and MCP

- If the repo has a **GitHub remote**, recommend the `gh` CLI: it's the most
  context-efficient way for Claude to open PRs, read issues, and review comments,
  and it avoids the rate limits of unauthenticated API calls. Check whether `gh`
  is installed and authenticated (`gh auth status`); if not, point the user to it.
- Mention **MCP servers** only if there's an obvious fit (the project clearly
  uses a database, Sentry, Figma, Notion, etc.). MCP is configured per-project in
  `.mcp.json` (committed). Don't scaffold MCP servers speculatively — they add
  tools and context cost whether or not they're used. See the reference file for
  `.mcp.json` format if the user wants one.

## 6. Fitted extras — add only where they clearly help

These are powerful but optional. Add one only when the repo gives a real reason
to, and prefer to *offer* rather than silently generate. More config is not
better config.

- **Hooks** (`.claude/settings.json`) — for actions that must happen every time
  with zero exceptions. The classic fit: if the repo has a formatter
  (prettier/ruff/gofmt), a `PostToolUse` hook that formats edited files
  guarantees it deterministically, where a CLAUDE.md note is only advisory. Only
  add this if a formatter actually exists.
- **Review subagent** (`.claude/agents/*.md`) — Claude Code already ships a
  bundled `/code-review` skill, so don't duplicate it. Add a custom subagent only
  for a *specialized* need the project states (e.g. a security reviewer for an
  auth-heavy service, a read-only DB query agent).
- **Skills** (`.claude/skills/<name>/SKILL.md`) — for repeatable multi-step
  workflows (e.g. "cut a release", "fix a GitHub issue") or domain knowledge
  that's only *sometimes* relevant and shouldn't live in CLAUDE.md. Scaffold one
  only if the user describes such a workflow.

Reference formats for all three are in `references/config-reference.md`.

## 7. Verify, then summarize

Close the loop the same way you'd want Claude to close any loop — with a check,
not an assertion. Run **one** documented command (prefer lint or typecheck) and
read the outcome carefully, because the three failure modes mean very different
things:

- **It runs and passes** → the command in CLAUDE.md is correct. Done.
- **It runs but reports findings/failures** (lint warnings, failing tests) → the
  *command* is still correct; that's pre-existing project state, not something to
  fix during setup. Note it and move on.
- **It can't run at all** — `command not found`, missing dependencies, a fresh
  checkout where nothing is installed → this tells you **nothing** about whether
  the command is right. Do **not** "fix" CLAUDE.md in response. Record that
  verification is deferred and tell the user to run it once deps are installed.

Only conclude a documented command is *wrong* when its runner exists but doesn't
recognize it — e.g. `pnpm` is installed but there's no `lint` script. That's a
real detection error worth fixing.

Then give the user a short summary: which files you created or changed, the key
decisions (the permission allowlist, anything you deliberately left out and why),
and recommended next steps (install `gh` if missing, restart the session so the
new `.claude/settings.json` loads, and any extras you flagged as offers in
step 6).

Keep the footprint small and explain the *why* behind each piece so the user can
prune later. A repo where Claude has the right commands, sane permissions, and a
lean CLAUDE.md is fully set up — resist the urge to add more.
