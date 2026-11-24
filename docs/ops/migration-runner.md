# Migration runner

`kiber-analyst-migrate` compares the checked-in SQL migration catalog with
`schema_migrations`, validates the SHA-256 manifest, and applies pending
migrations in order. It is an operator tool; workers and brokers do not run
migrations automatically.

## Baseline and ledger rules

`0001_init` is the externally provisioned bootstrap baseline. There is no
`0001_init.sql`; the runner requires the baseline row to exist and never creates
it. The catalog must be a continuous sequence from `0001_init` through the
last SQL migration. The database ledger must be a continuous prefix of that
sequence, with no unknown versions, duplicates, or gaps.

Every SQL migration must register its own version inside its SQL transaction.
The runner never inserts, repairs, or deletes rows in `schema_migrations`.
After each migration executes, the runner verifies that the SQL registered
exactly one matching version before committing that migration's transaction.

`migrations/checksums.json` pins each SQL file's SHA-256. Before planning or
applying, the runner checks that the file set matches the manifest, each file
matches its pinned checksum, and each file self-registers the matching version.
This protects the current checkout and its migration plan. The database schema
does not store a per-file checksum; historical applied-file checksums are
therefore verified against the reviewed manifest in the checkout, not against
a checksum column in PostgreSQL.

## Run

Use a maintenance window and a PostgreSQL role with the privileges required by
the pending SQL. In particular, migration `0004_mcp_reader` creates or alters
a role and revokes/grants database privileges. Back up the target database and
verify the backup before applying schema changes.

Pass the connection string only in the process environment. The runner accepts
the fixed Kiber-analyst database name `knowledge`; it does not accept a DSN as a
command-line argument, so credentials do not appear in process arguments.

```bash
export KIBER_ANALYST_MIGRATION_DSN='postgresql://<role>:<password>@127.0.0.1:5432/knowledge'

# Validate catalog, checksum, ledger and pending plan without executing SQL.
kiber-analyst-migrate --dry-run

# Apply all pending migrations, one transaction per SQL file.
kiber-analyst-migrate

# Apply only through a migration version; a target behind the database is refused.
kiber-analyst-migrate --target 0006_external_discussions
```

The command uses a PostgreSQL session advisory lock. If another runner holds
the lock, it exits with `MIGRATION_LOCK_BUSY`; rerun after the other operator
finishes. A failed SQL migration rolls back its transaction, while earlier
successfully committed migrations remain recorded as a valid prefix.

Output is JSON and contains versions, plan status, and safe error codes. It does
not include the DSN, SQL text, or raw PostgreSQL error messages. A SQLSTATE may
be reported to help diagnose a failed transaction.

## Disposable PostgreSQL CI

The `migration-postgres` CI job starts a fresh PostgreSQL 17 + pgvector service
for each run. Its integration test creates a temporary `knowledge` database
only on a loopback test server, verifies dry-run and `--target`, applies the
SQL files, checks the generated FTS column and ledger, verifies advisory-lock
exclusion, and confirms a failing migration rolls back both DDL and its ledger
entry. It refuses an existing test database or pre-existing test roles and
drops only resources it created.

Never set `KIBER_ANALYST_MIGRATION_TEST_ADMIN_DSN` to production, a tunnel, or
any shared database. The integration test is destructive by design and must
run only against its disposable PostgreSQL service.
