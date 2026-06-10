# Git branch naming

Use this format for new branches:

**`{type}/JIRA-XXXX/<short-name>`**

- `{type}` is the conventional-commit type: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`
- `JIRA-XXXX` is the Jira ticket key. Use `NO-JIRA` for non-ticketed work.
- `<short-name>` is a short, kebab-case description (e.g. `add-search-tool`)

**Examples**:
- `feat/JIRA-7747/add-search-tool`
- `fix/JIRA-8000/trino-connection-timeout`
- `chore/JIRA-7900/bump-deps`
- `chore/NO-JIRA/bump-spring-ai`

When creating or suggesting branch names, always follow this pattern.
