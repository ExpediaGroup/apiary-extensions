# How to Use AI with apiary-extensions

_A practical guide for contributing to apiary-extensions using AI coding assistants._

## Core Principles

- **Human oversight**: You are accountable for all code you submit. Never commit code you don't understand.
- **Quality standards**: AI-generated code must meet the same bar as human-written code — formatting, tests, copyright headers, and all.
- **Transparency**: Note significant AI assistance in your PR description.

## AI Assistant Configuration

- **`AGENTS.md`** — canonical project guide for AI assistants: module structure, build commands, extension patterns, testing conventions, and guard-rails.
- **`CLAUDE.md`** — thin pointer to `AGENTS.md` for Claude Code.
- **`.claude/rules/`** — additional rule files loaded automatically by Claude Code (Java style, JDK setup, branch naming).

Any assistant that reads `AGENTS.md` at session start will have the context it needs.

## Getting Started with Claude Code

```bash
claude
```

The assistant will read `AGENTS.md` and `.claude/rules/` automatically. For a specific task:

```bash
/plan <describe what you want to do>
```

## Key Rules

1. **Never commit directly to `main`** — always work on a branch (`{type}/<short-name>`)
2. **Run `mvn spotless:apply` before committing** — unformatted code will fail CI
3. **Every new `.java` file needs the Apache 2.0 copyright header** — copy from an existing file
4. **Use `provided` scope for Hive/Hadoop dependencies** — they live on the metastore classpath at runtime
5. **Update `CHANGELOG.md`** for every user-visible change

## Building and Testing

```bash
sdk env install          # activate JDK 11 via sdkman
mvn clean verify         # full build + tests
mvn spotless:apply       # fix formatting
```
