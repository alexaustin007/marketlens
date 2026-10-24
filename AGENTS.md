# Repository Guidelines

## Project Structure & Module Organization

MarketLens is a planned financial data and AI research platform. The repository currently contains `MARKETLENS_END_TO_END_ARCHITECTURE_AND_LEARNING_PLAN (1).md`, the canonical architecture and learning specification, and a local `venv/`. Application code, tests, dependency manifests, and CI are not yet present.

Follow the specification’s target layout as features are implemented:

- `ingestion/`: source adapters and shared HTTP, validation, and Snowflake utilities.
- `orchestration/prefect/`: flows, tasks, and deployments.
- `dbt/`: staging, intermediate, and mart models, snapshots, and tests.
- `snowflake/`: bootstrap SQL, roles, stages, and Cortex configuration.
- `documents/` and `mcp/`: document processing and governed research tools.
- `tests/`: unit, integration, contract, and end-to-end checks.
- `docs/` and `config/`: architecture decisions, runbooks, and source configuration.

Create directories when needed; do not scaffold the entire target tree upfront.

## Build, Test, and Development Commands

- `source venv/bin/activate`: activate the existing local Python environment.
- `git status --short`: inspect pending changes before and after work.

No build, application startup, lint, or test commands are configured yet. Once dbt is implemented and configured, run `dbt build` from `dbt/` to build resources and execute tests. Document actual setup and commands when introducing tooling.

## Coding Style & Naming Conventions

For new Python code, use four-space indentation, `snake_case` functions/modules, and `PascalCase` classes. Prefer explicit types, small functions, and clear adapter/domain boundaries. No formatter or linter is configured yet.

Normalize stored timestamps to UTC with explicit timezone handling. Choose deliberate decimal precision for financial values.

## Testing Guidelines

No testing framework or numerical coverage threshold is established. Use `test_*.py` for new Python tests. Cover payload contracts, retries, duplicate handling, and idempotent ingestion as implemented. The specification requires all critical dbt tests to pass before publishing data.

## Commit & Pull Request Guidelines

There is no commit history to establish conventions. Use concise, imperative commit subjects. Keep pull requests focused; describe the change, architectural impact, validation performed, and relevant issues. Disclose checks that could not run.

## Security & Agent Instructions

Never commit credentials, `.env`, or `venv/`; configure ignore rules before adding these artifacts. Keep `.env.example` limited to variable names without secrets.

This is a hands-on learning project: guide and review by default; modify files only when explicitly requested. Explain consequential commands and design choices. Flag architectural drift, and edit the canonical specification only on explicit request.

When guiding hands-on work, give two or three closely related steps per reply, each with paste-ready commands or SQL and expected results. Use one step when it is large or consequential. Stop at a failure or decision point so the user can share the result before continuing. The user runs the core procedures; provide complete examples in chat so they do not have to write commands or code from scratch.

Keep MarketLens-generated temporary files, sample data, and raw API responses in the repository's `tmp/` directory. Use `tmp/...` paths in instructions instead of the system `/tmp` directory. Keep `tmp/` ignored by Git; do not place credentials there.
