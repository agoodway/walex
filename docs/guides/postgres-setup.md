# Postgres Setup

This guide covers what to set up in the database. For the Elixir side, see
[Configuration](configuration.md).

## Logical replication

Configure PostgreSQL for [logical
replication](https://www.crunchydata.com/blog/data-to-go-postgres-logical-replication):

```sql
ALTER SYSTEM SET wal_level = 'logical';
```

Docker Compose:

```yaml
command: [ "postgres", "-c", "wal_level=logical" ]
```

Changing `wal_level` requires a restart of the PostgreSQL server.

## Publication

After the restart, [create a
publication](https://www.postgresql.org/docs/current/sql-createpublication.html)
for the tables you want to receive changes for.

All tables:

```sql
CREATE PUBLICATION events FOR ALL TABLES;
```

Specific tables:

```sql
CREATE PUBLICATION events FOR TABLE "user", todo;
```

Filtered by [row
conditions](https://www.postgresql.fastware.com/blog/introducing-publication-row-filters)
(PostgreSQL 15+ only):

```sql
CREATE PUBLICATION user_event FOR TABLE "user" WHERE (active IS TRUE);
```

`user` is a reserved word in Postgres, so the examples quote it.

The `publication:` config option takes one name or a list of names. Each must
be a non-empty string of at most 63 bytes with no backslash and no ASCII
control character (such as newline or tab). Mixed case and hyphens work:
WalEx quotes the name. A name that breaks the rule raises an `ArgumentError`
at startup and in `WalEx.Config.replace_config/3`.

WalEx checks that every configured publication exists when it connects, and
fails if one does not.

## Replica identity

WalEx supports all settings of [REPLICA
IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY).
Use `FULL` if you can, because the old data is then sent alongside the new
data, which makes tracking differences
[easier](https://xata.io/blog/replica-identity-full-performance). Set it for
each table:

```sql
ALTER TABLE "user" REPLICA IDENTITY FULL;
ALTER TABLE todo REPLICA IDENTITY FULL;
```

Be mindful of [replication
gotchas](https://pgdash.io/blog/postgres-replication-gotchas.html).

## Limiting retained WAL

A replication slot makes Postgres keep WAL until the slot's consumer
acknowledges it, so a stalled consumer fills the disk of the Postgres server.
(A logical slot also holds back vacuuming of system catalogs, but not of your
tables.) Alert on slot lag (the retained WAL in `pg_replication_slots`, or
`OldestReplicationSlotLag` on AWS RDS) and set
[`max_slot_wal_keep_size`](https://www.postgresql.org/docs/current/runtime-config-replication.html)
(PostgreSQL 13+) so a stuck slot cannot fill the disk. When the limit is
exceeded, Postgres invalidates the slot and the unconsumed changes are lost.
See [Delivery Guarantees](delivery-guarantees.md#a-missing-or-lost-slot).

## AWS RDS

Amazon RDS Postgres supports logical replication. See [how to change RDS
PostgreSQL
configurations](https://dev.to/vumdao/how-to-change-rds-postgresql-configurations-2kmk).
When you create a new database, set a parameter group with:

```text
rds.logical_replication = 1
max_replication_slots = 5
max_slot_wal_keep_size = 2048
```

## Hosted Postgres notes

- The replication connection must be a direct connection. It does not work
  through PgBouncer in transaction mode, Neon's `-pooler` hostname, Supavisor
  or RDS Proxy.
- Neon may remove inactive slots (check its current limits), and a branch
  restore deletes them. If WalEx is stopped that long, the slot is gone.
- `wal_sender_timeout` defaults differ by provider. Check it before relying on
  the [handler time
  budget](delivery-guarantees.md#slow-handlers-and-wal_sender_timeout).
- PostgreSQL 18's `idle_replication_slot_timeout` invalidates slots that stay
  inactive for longer than it. A WalEx outage longer than that loses the slot.
- Avoid `idle_session_timeout` on the replication role (see [failure
  backoff](delivery-guarantees.md#handler-failures-and-backoff)).
- On PostgreSQL 17+ you can create the slot yourself with `failover => true`
  so it follows a failover (a slot lost in a failover without it is a
  [missing slot](delivery-guarantees.md#a-missing-or-lost-slot)). WalEx uses
  an existing slot as is.
