# Configuration

WalEx is configured with a keyword list passed in the `{WalEx.Supervisor, config}`
child spec. Each instance is identified by its `:name`, so you can run several
in one application. A minimal config and supervisor setup is in the
[Quick Start](../../README.md#quick-start).

## Quick reference

| Option | Default | Read when |
| --- | --- | --- |
| [`name`](#name-and-event-modules) | required | startup |
| [`url`](#connection) | none | startup |
| [`hostname`, `username`, `password`, `port`, `database`](#connection) | required without `url` | the replication server starts |
| [`ssl`, `ssl_opts`, `socket_options`](#tls) | `false`, `[verify: :verify_none]`, `[]` | the replication server starts |
| [`publication`](#publication-and-subscriptions) | required | the replication server starts |
| [`subscriptions`](#publication-and-subscriptions) | required | each transaction |
| [`modules`](#name-and-event-modules) | `[]` | each transaction |
| [`slot_name`](#replication-slot) | `<name>_walex` | the replication server starts |
| [`durable_slot`](#replication-slot) | `false` | the replication server starts |
| [`ack_after_processing`](#ack_after_processing) | `false` | startup only |
| [`standby_status_interval`](#standby_status_interval) | none, `10_000` with `ack_after_processing` | startup only |
| [`message_middleware`](#message_middleware) | `&WalEx.Replication.Publisher.process_message_async/2` | the replication server starts |

A missing required option raises a `RuntimeError` listing the missing options.
The check is for presence only: an invalid publication or slot name raises an
`ArgumentError`.

## Connection

Give the connection settings as separate options, or as one URL:

```elixir
config :my_app, WalEx,
  url: "postgres://username:password@hostname:port/database",
  publication: "events",
  subscriptions: ["user", "todo"],
  name: MyApp
```

The URL must have a host and a path that is a database name. Separate options
take precedence over values parsed from the URL.

`socket_options` is passed to the underlying Postgrex connection (for example
`[:inet6]`).

## TLS

With `ssl: true` and no `ssl_opts`, WalEx connects with `verify: :verify_none`
(the server certificate is not checked) and logs a warning at startup. To
verify the certificate, pass `ssl_opts`:

```elixir
config :my_app, WalEx,
  ssl: true,
  ssl_opts: [verify: :verify_peer, cacertfile: "/path/to/ca.pem", server_name_indication: ~c"db.example.com"],
  # ...
  name: MyApp
```

## Publication and subscriptions

`publication` is one publication name or a list of names. The rules for a
valid name are in [Postgres Setup](postgres-setup.md#publication).

`subscriptions` lists the tables WalEx handles. To subscribe to every table in
the publication, pass the special atom `:all_tables`:

```elixir
config :my_app, WalEx,
  url: "postgres://username:password@hostname:port/database",
  publication: "events",
  subscriptions: [:all_tables],
  name: MyApp
```

## Name and event modules

If your app is named `MyApp` and you have a subscription `"user"`, WalEx
assumes a module `MyApp.Events.User` that uses `WalEx.Event` (see
[Events](events.md)). Names are converted from `snake_case`, so `todo_item`
maps to `MyApp.Events.TodoItem`.

To use other modules, list them in `modules`. A module only receives tables
listed in `subscriptions`:

```elixir
config :my_app, WalEx,
  # ...
  modules: [MyApp.CustomModule.User, MyApp.OtherCustomModule.ToDo],
  name: MyApp
```

Event modules are checked at startup:

- Without `ack_after_processing`, a module in `modules` that cannot be loaded
  is dropped with a warning.
- With `ack_after_processing`, it makes startup fail with an `ArgumentError`.
  So does a configuration in which no event module resolves at all (for
  example a misspelled subscription whose `MyApp.Events.<Table>` module does
  not exist), because every transaction would otherwise be acknowledged
  without a handler. Modules derived from `subscriptions` are optional as long
  as at least one module resolves.

## Replication slot

By default WalEx creates a temporary replication slot. If the connection
between WalEx and Postgres is interrupted (crash, disconnect), Postgres drops
the slot. This is safe for the Postgres server, because there is no risk of
filling its disk during downtime. The downside is that events are likely lost
while WalEx is down.

For durable replication, use a durable slot:

```elixir
config :my_app, WalEx,
  # ...
  durable_slot: true,
  slot_name: :my_app_replication_slot
```

- `slot_name` defaults to `<name>_walex`, for example `myapp_walex` for
  `name: MyApp`. WalEx lowercases the name and keeps only the part after the
  last `.`, so `MyApp.Repo` becomes `repo`. It replaces each run of characters
  other than `a-z` and `0-9` with `_`, and trims leading and trailing `_`. The
  result must be non-empty, or startup raises an `ArgumentError`.
- `durable_slot` must be exactly `true` or `false`. `nil` (for example an unset
  environment variable) counts as `false`. Any other value raises an
  `ArgumentError`.
- Only one process can be connected to a durable slot at a time. If the slot is
  in use, WalEx logs it and checks again every 5 seconds until the slot is
  free. A standby WalEx instance can use this to wait for an active one to go
  away.
- A durable slot retains WAL while WalEx is not consuming. See [Limiting
  retained WAL](postgres-setup.md#limiting-retained-wal).

For stronger delivery guarantees on a durable slot, see
[`ack_after_processing`](#ack_after_processing) and
[Delivery Guarantees](delivery-guarantees.md).

## ack_after_processing

Type: `boolean`. Default: `false`.

Holds back the acknowledgement of a transaction until its event handlers have
finished. Delivery then becomes at-least-once. The behavior, and the
guarantees it does and does not give, are described in [Delivery
Guarantees](delivery-guarantees.md).

```elixir
config :my_app, WalEx,
  # ...
  durable_slot: true,
  slot_name: :my_app_slot,
  ack_after_processing: true
```

Requirements:

- PostgreSQL 13 or later (WalEx reads `pg_replication_slots.wal_status`). On
  an older server the replication server raises when it first checks the slot.
- `durable_slot: true`. On a temporary slot, startup raises an
  `ArgumentError`, because a temporary slot does not retain WAL across
  reconnects and the guarantee would be meaningless.
- The default `message_middleware`. A custom one raises an `ArgumentError`,
  except `&WalEx.Replication.Publisher.process_message_sync/2`, which is the
  default in this mode. Acknowledgements rely on WalEx handing each message to
  `WalEx.Replication.Publisher` synchronously, which WalEx cannot verify for a
  custom middleware.
- At least one resolvable event module (see [Name and event
  modules](#name-and-event-modules)).

The `durable_slot`, middleware and module requirements are checked at startup
and again each time the replication server starts.

The option must be exactly `true` or `false`. `nil` counts as `false`. Any
other value raises an `ArgumentError`.

`ack_after_processing` is read once at startup and cannot be changed with
`replace_config/3`. The locks that apply to runtime changes while it is on are
listed under [Changing configuration at runtime](#changing-configuration-at-runtime).

## standby_status_interval

Type: positive integer, milliseconds. Default: none, or `10_000` (10 s) with
`ack_after_processing: true`. Any other value raises at startup. An explicit
value always wins.

Postgres asks for a status update only when `wal_sender_timeout` is greater
than `0`. With it disabled, a client that only answers requests never reports
a position and the slot pins WAL indefinitely. Set an interval and WalEx
reports on that schedule instead:

```elixir
config :my_app, WalEx,
  # ...
  standby_status_interval: 30_000
```

- Without `ack_after_processing`, each update reports the latest position
  WalEx has received: the server's last reported WAL position, or the end of
  the last COMMIT received, whichever is later.
- With `ack_after_processing`, updates report the last fully processed commit
  while a transaction is open, and the server's latest position when none is.
  Dispatch is synchronous in that mode, so a position seen while idle has been
  fully processed. The replication connection is blocked while handlers run,
  so a scheduled update goes out only once the running transaction has
  finished.
- Updates start with the first message of each connection and stop when the
  connection drops.

## message_middleware

Type: `(message, app_name -> :ok | {:error, term})`. Default:
`&WalEx.Replication.Publisher.process_message_async/2`, or
`&WalEx.Replication.Publisher.process_message_sync/2` with
`ack_after_processing: true`.

WalEx receives messages from Postgres in `WalEx.Replication.Server` and passes
them to `WalEx.Replication.Publisher`, which joins them into transactions and
processes them. `message_middleware` defines how the two communicate. You can
use it to implement back-pressure, if you expect Postgres to overwhelm WalEx
and cause out-of-memory errors. Use it only if you cannot add memory:

```elixir
config :my_app, WalEx,
  # ...
  message_middleware: fn message, app_name -> ... end
```

A typical design stores messages on disk in the middleware, and a supervised,
strictly ordered consumer calls
`WalEx.Replication.Publisher.process_message_async(message, app_name)` or
`process_message_sync(message, app_name)` for each. Any back-pressure
implementation must guarantee:

- exact message ordering
- exactly-once delivery
- an isolated back-pressure system per running WalEx (for example one queue
  per instance)

## Changing configuration at runtime

`WalEx.Config` lets you change the configuration of a running instance:

```elixir
WalEx.Config.add_config(MyApp, :subscriptions, ["order"])
WalEx.Config.add_config(MyApp, :modules, [MyApp.Events.Order])
WalEx.Config.remove_config(MyApp, :subscriptions, "todo")
WalEx.Config.replace_config(MyApp, :password, "new_password")
```

- `add_config/3` appends one value or a list to `:modules` or
  `:subscriptions`. Duplicates are removed.
- `remove_config/3` removes a value from `:modules` or `:subscriptions`.
- `replace_config/3` replaces one of the connection settings (`:database`,
  `:hostname`, `:password`, `:port`, `:username`), `:publication`,
  `:slot_name`, `:durable_slot` or `:message_middleware`. The new value goes
  through the same rules as at startup (for example a new `:slot_name` is
  sanitized, and a new `:publication` is validated).
- `get_configs/1` returns the stored configuration as a keyword list.
  `get_configs/2` takes a key, or a list of keys. WalEx's internal runtime
  bookkeeping (connection count, failure streak) is kept out of the result.

A changed value is used from when the setting is next read (the "Read when"
column above). Replacing a setting does not reconnect; settings read when the
replication server starts apply after it restarts, not after a reconnect alone.

Event modules are resolved from `subscriptions` only at startup, so a module
for a subscription added at runtime must be added to `:modules` too, as above.
A module added later that cannot be loaded is not skipped silently: calling it
raises, which counts as a handler failure like any other.

While `ack_after_processing` is on:

- `replace_config/3` raises an `ArgumentError` and leaves the config unchanged
  if you set `:durable_slot` to anything other than `true`, or
  `:message_middleware` to anything other than the default synchronous
  middleware.
- `replace_config/3` rejects replacing `:slot_name`, `:publication`,
  `:database`, `:hostname` and `:port`. These point the connection at a
  slot, publication or database, and the stored acknowledgements describe the
  old one.
- `remove_config/3` raises an `ArgumentError` if it would remove the last
  entry of `:modules`, because transactions would then be acknowledged with no
  handler.
- `WalEx.Replication.Server` re-checks the configuration every time it
  starts, and refuses to start rather than replicate without the guarantee.
