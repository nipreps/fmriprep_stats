# AGENTS instructions

The project is divided in three major components:

- a stats fetcher from Sentry.io under `src/`
- visualization tools (originally developed under `notebooks/` with plotting utilities migrated into `src/viz.py`)
- housekeeping scripts meant for addition into Cron under `scripts/`

# Testing

At this moment, there's no explicit testing built in this repo.

# Documentation

At this moment, there's no explicit documentation in this repo.

# Linting

The repo is linted with `ruff check --fix` and `ruff format` from time to time.

## Codex instructions

- Always plan first
- Think harder in the planning phase
- When proposing tasks, highlight potential critical points that could lead to side effects.

## Commits and PRs

- Commit messages must follow the Conventional Commits specification:
  https://www.conventionalcommits.org/en/v1.0.0/
- PRs titles follow Conventional Commits specifications for the message subject,
  writing the type in upper case and dropping the scope parenthetical (e.g., `FIX: <description-headline>`).
