# Group Scholar Touchpoint Ledger

Command-line ledger for logging scholar touchpoints, reviewing recent outreach, and surfacing upcoming follow-ups. Uses a production Postgres database for persistence and reporting.

## Features
- Log outreach touchpoints with staff, channel, notes, and follow-up dates.
- List recent activity with optional date filters.
- Weekly channel summaries for the current or specified week.
- Upcoming follow-up window reporting.
- Staff activity summaries for recent touchpoints.
- Production-only schema and seed data scripts.

## Commands

```bash
zig build run -- help
zig build run -- init-db
zig build run -- log --scholar "Avery Chen" --channel email --staff "Morgan Lee" --notes "Sent FAFSA reminder" --follow-up 2026-02-14
zig build run -- list --limit 25 --since 2026-01-01
zig build run -- weekly --week-start 2026-02-02
zig build run -- follow-ups --since current_date --days 14
zig build run -- staff-summary --until current_date --days 30 --limit 10
```

## Configuration
Set the production database URL before running commands:

```bash
export GS_TOUCHPOINT_DB_URL="postgres://user:password@host:port/dbname?sslmode=require"
```

The CLI invokes `psql` under the hood. Ensure it is available on your PATH.

## Database
Schema and seed data live in `sql/01_schema.sql` and `sql/02_seed.sql`. Use `init-db` to apply them to production. Do not run against local databases.

## Testing

```bash
zig build test
```

## Tech
- Zig
- Postgres (via `psql`)
