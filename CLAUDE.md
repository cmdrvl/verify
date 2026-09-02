# CLAUDE.md — Claude Code Harness Notes

Read [AGENTS.md](./AGENTS.md) completely before working in this repository. It
is the source of truth for project boundaries, safety rules, Beads and Agent
Mail coordination, file ownership, implementation workflow, and quality gates.
This file contains only Claude Code-specific harness notes.

## Local Settings

Claude Code may create `.claude/settings.local.json` for this checkout. The
entire `.claude/` directory is intentionally ignored by Git, so that file is
machine-local configuration rather than a portable repository contract.

- Do not commit or copy the local settings file into the repository.
- Do not assume another checkout exposes the same tools or command prefixes.
- Inspect the local file when diagnosing a Claude Code permission denial.
- Run repository commands from the Git root; discover it with
  `git rev-parse --show-toplevel` instead of embedding an absolute path.

The current local settings are permission-only. They do not define project
semantics. Their allowlist covers these command families when the local harness
chooses to expose them:

- Beads inspection and lifecycle commands (`br`)
- Cargo build, run, format, check, Clippy, and test commands
- narrow Git staging, commit, and push commands
- direct MCP Agent Mail coordination calls

A harness permission is capability, not authorization. User instructions and
the safety, ownership, destructive-action, and coordination rules in
`AGENTS.md` still govern every invocation. An allowed Git or shell command is
not permission to broaden the requested task.

## Agent Mail Availability

Direct `mcp__mcp-agent-mail__...` tools are optional. When Claude Code exposes
them, follow the registration, exact-file reservation, thread, and release
protocol in `AGENTS.md`. An `ntm` session may be connected to Agent Mail even
when those direct tools are absent. In that case, continue with Beads and narrow
file ownership as documented in `AGENTS.md`; do not block solely on missing MCP
tools.

## Hooks and Harness Overrides

No repository-level Claude Code hooks, model preference, memory policy,
compaction setting, keybinding, or `disallowedTools` list is configured. Use
the harness defaults plus any user-global configuration. Do not infer a hidden
hook or model contract from the local permission allowlist.

Last reviewed: 2026-09-01.
