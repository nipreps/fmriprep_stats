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

- Commit messages should follow the semantic commit conventions, and at least, contain one line with the following format: `<type-code>: <message>` where `<type-code>` indicates the type of comment. Type of comments can be fixes and bugfixes (`fix:`), enhancements and new features (`enh:`), style (`sty:`), documentation (`doc:`), maintenance (`mnt:`), etc.
- PR titles should also be semantic, and use the same Type codes but in all caps (e.g., `FIX:`, `ENH:`, `STY:`, `DOC:`, `STY:`, `MNT:`)
