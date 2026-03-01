# Windmill AI Agent Instructions

You are a helpful assistant that can help with Windmill scripts, flows, apps, and resources management.

## Important Notes
- Every new entity MUST be created using the skills listed below.
- Every modification of an entity MUST be done using the skills listed below.
- User MUST be asked where to create the entity. It can be in its user folder, under u/{user_name} folder, or in a new folder, /f/{folder_name}/. folder_name and user_name must be provided by the user.

## Script Writing Guide

You MUST use the `write-script-<language>` skill to write or modify scripts in the language specified by the user. Use bun by default.

## Flow Writing Guide

You MUST use the `write-flow` skill to create or modify flows.

## Raw App Development

You MUST use the `raw-app` skill to create or modify raw apps.
Whenever a new app needs to be created you MUST ask the user to run `wmill app new` in its terminal first.

## Triggers

You MUST use the `triggers` skill to configure HTTP routes, WebSocket, Kafka, NATS, SQS, MQTT, GCP, or Postgres CDC triggers.

## Schedules

You MUST use the `schedules` skill to configure cron schedules.

## Resources

You MUST use the `resources` skill to manage resource types and credentials.

## CLI Reference

You MUST use the `cli-commands` skill to use the CLI.

## Skills

For specific guidance, ALWAYS use the skills listed below.

- `.agent/skills/write-script-python3/SKILL.md` - MUST use when writing Python scripts.
- `.agent/skills/write-script-bun/SKILL.md` - MUST use when writing Bun/TypeScript scripts.
- `.agent/skills/write-script-mysql/SKILL.md` - MUST use when writing MySQL queries.
- `.agent/skills/write-script-powershell/SKILL.md` - MUST use when writing PowerShell scripts.
- `.agent/skills/write-script-snowflake/SKILL.md` - MUST use when writing Snowflake queries.
- `.agent/skills/write-script-go/SKILL.md` - MUST use when writing Go scripts.
- `.agent/skills/write-script-deno/SKILL.md` - MUST use when writing Deno/TypeScript scripts.
- `.agent/skills/write-script-bash/SKILL.md` - MUST use when writing Bash scripts.
- `.agent/skills/write-script-bunnative/SKILL.md` - MUST use when writing Bun Native scripts.
- `.agent/skills/write-script-duckdb/SKILL.md` - MUST use when writing DuckDB queries.
- `.agent/skills/write-script-nativets/SKILL.md` - MUST use when writing Native TypeScript scripts.
- `.agent/skills/write-script-bigquery/SKILL.md` - MUST use when writing BigQuery queries.
- `.agent/skills/write-script-rust/SKILL.md` - MUST use when writing Rust scripts.
- `.agent/skills/write-script-php/SKILL.md` - MUST use when writing PHP scripts.
- `.agent/skills/write-script-mssql/SKILL.md` - MUST use when writing MS SQL Server queries.
- `.agent/skills/write-script-postgresql/SKILL.md` - MUST use when writing PostgreSQL queries.
- `.agent/skills/write-script-graphql/SKILL.md` - MUST use when writing GraphQL queries.
- `.agent/skills/write-script-csharp/SKILL.md` - MUST use when writing C# scripts.
- `.agent/skills/write-script-java/SKILL.md` - MUST use when writing Java scripts.
- `.agent/skills/write-flow/SKILL.md` - MUST use when creating flows.
- `.agent/skills/raw-app/SKILL.md` - MUST use when creating raw apps.
- `.agent/skills/triggers/SKILL.md` - MUST use when configuring triggers.
- `.agent/skills/schedules/SKILL.md` - MUST use when configuring schedules.
- `.agent/skills/resources/SKILL.md` - MUST use when managing resources.
- `.agent/skills/cli-commands/SKILL.md` - MUST use when using the CLI.
- `.agent/skills/yatws/SKILL.md` - MUST use when using the Rust IBKR API or yatws.

