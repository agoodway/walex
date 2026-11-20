# WalEx

[![Hex.pm](https://img.shields.io/hexpm/v/walex.svg)](https://hex.pm/packages/walex)
[![Documentation](https://img.shields.io/badge/documentation-hexdocs-blue.svg)](https://hexdocs.pm/walex)

> Simple and reliable Postgres [Change Data Capture
> (CDC)](https://en.wikipedia.org/wiki/Change_data_capture) in Elixir.

![Walex mascot](mascot.jpeg)

WalEx listens to the write-ahead log of a PostgreSQL database through logical
replication and calls your Elixir functions when rows are inserted, updated or
deleted. You declare handlers with a small DSL.
[Why capture changes?](https://bbhoss.io/posts/announcing-cainophile/#why-capture-changes)

## Credit

This library borrows liberally from
[realtime](https://github.com/supabase/realtime) from Supabase, which in turn
draws heavily on [cainophile](https://github.com/cainophile/cainophile).
The delivery-guarantee work (exact acknowledgements, backoff, slot handling)
borrows ideas from [Sequin](https://github.com/sequinstream/sequin).

## Installation

If [available in Hex](https://hex.pm/docs/publish), add `walex` to your
dependencies in `mix.exs`, then run `mix deps.get`:

```elixir
def deps do
  [
    {:walex, "~> 4.9.0"}
  ]
end
```

## Postgres Setup

Enable logical replication (needs a restart), create a publication, and set the
replica identity of each table:

```sql
ALTER SYSTEM SET wal_level = 'logical';
CREATE PUBLICATION events FOR ALL TABLES;
ALTER TABLE todo REPLICA IDENTITY FULL;
```

Specific tables, row filters, AWS RDS and hosted Postgres are covered in
[Postgres Setup](docs/guides/postgres-setup.md).

## Quick Start

1. Configure WalEx:

   ```elixir
   # config.exs
   config :my_app, WalEx,
     hostname: "localhost",
     username: "postgres",
     password: "postgres",
     port: "5432",
     database: "postgres",
     publication: "events",
     subscriptions: ["user", "todo"],
     name: MyApp
   ```

   Pass `url: "postgres://user:password@host:port/database"` instead of the
   separate connection options if you prefer.

2. Start it under your supervisor:

   ```elixir
   children = [
     {WalEx.Supervisor, Application.get_env(:my_app, WalEx)}
   ]

   Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
   ```

3. Handle events. For the subscription `"user"` in an app named `MyApp`, WalEx
   looks for the module `MyApp.Events.User`:

   ```elixir
   defmodule MyApp.Events.User do
     use WalEx.Event, name: MyApp

     on_insert(:user, fn users ->
       IO.inspect(on_insert: users)
     end)

     on_update(:user, fn users ->
       IO.inspect(on_update: users)
     end)
   end
   ```

   Each handler receives a list of `%WalEx.Event{}` structs.

## Main options

| Option | Default | Meaning |
| --- | --- | --- |
| `name` | required | Instance name. Also used to find event modules. |
| `url` | none | Database URL, instead of `hostname`, `username`, `password`, `port` and `database`. |
| `publication` | required | Publication name, or a list of names. |
| `subscriptions` | required | Tables to handle, or `[:all_tables]`. |
| `durable_slot` | `false` | Keep the replication slot across disconnects. Otherwise it is temporary. |
| `ack_after_processing` | `false` | Acknowledge WAL only after handlers finish (at-least-once). Needs `durable_slot: true` and PostgreSQL 13+. |

All options, including TLS, slot names and back-pressure, are in
[Configuration](docs/guides/configuration.md).

## Documentation

- [Postgres Setup](docs/guides/postgres-setup.md)
- [Configuration](docs/guides/configuration.md)
- [Events](docs/guides/events.md): the event struct, DSL, filters, functions, `on_connect`
- [Delivery Guarantees](docs/guides/delivery-guarantees.md): `ack_after_processing`, idempotency, failures, slot loss
- [Telemetry](docs/guides/telemetry.md)
- [HexDocs](https://hexdocs.pm/walex): API reference

## Alternatives

If you are not an Elixir user, or don't need to subscribe to WAL events
directly in Elixir, look at [Sequin](https://github.com/sequinstream/sequin).
It streams Postgres changes to destinations such as Kafka and S3.

## Development

You need a local Postgres instance.

```bash
MIX_ENV=test mix walex.setup
MIX_ENV=test mix test
```

Set `PGUSER`, `PGPASSWORD`, `PGHOST` or `PGPORT` to use a customized Postgres
server. They take precedence in tests and in `mix walex.setup`.

## License

MIT. See [LICENSE](https://github.com/cpursley/walex/blob/master/LICENSE).
