# Events

WalEx delivers changes to your code as `%WalEx.Event{}` structs, which you
handle in event modules built with the event DSL.

## The event struct

A handler receives a list of `%WalEx.Event{}` structs. This is an UPDATE event
where the `name` field changed:

```elixir
[
  %WalEx.Event{
    name: :user,
    type: :update,
    source: %WalEx.Event.Source{
      name: "WalEx",
      version: "4.8.0",
      db: "todos",
      schema: "public",
      table: "user",
      columns: %{
        id: "integer",
        name: "varchar",
        created_at: "timestamptz"
      }
    },
    new_record: %{
      id: 1234,
      name: "Chase Pursley",
      created_at: #DateTime<2023-08-18 14:09:05.988369-04:00 -04 Etc/UTC-4>
    },
    # old_record is not shown for updates, to reduce payload size.
    # Old values that changed are under "changes".
    old_record: nil,
    changes: %{
      name: %{
        new_value: "Chase Pursley",
        old_value: "Chase"
      }
    },
    timestamp: ~U[2023-12-18 15:50:08.329504Z],
    lsn: {0, 23_935_672},
    change_index: 0
  }
]
```

| Field | Meaning |
| --- | --- |
| `name` | The table, as an atom. |
| `type` | `:insert`, `:update` or `:delete`. |
| `source` | Where the change came from: WalEx name and version, database, schema, table and column types. |
| `new_record` | The row after an insert or update. `nil` for a delete. |
| `old_record` | The row before a delete (key columns only without `REPLICA IDENTITY FULL`). `nil` for an insert, and for an update. |
| `changes` | For an update, the changed fields with their `new_value` and `old_value`. Otherwise `nil`. |
| `timestamp` | The commit time of the transaction. |
| `lsn` | The transaction's commit position, a `{xlog_file, xlog_offset}` tuple. |
| `change_index` | The change's position within its transaction, counting from `0` in commit order. |

`WalEx.Event.lsn_to_string/1` formats `lsn` the way Postgres prints it, for
example `"16/B374D848"`.

A handler gets all events of one transaction for that table, usually one.

`{lsn, change_index}` is unique per change and stable across redeliveries, so
use it to deduplicate (see [Delivery
Guarantees](delivery-guarantees.md#idempotency)).

Within a transaction, handlers receive the changes newest first. Sort by
`change_index` if you need commit order. Every change takes an index,
including a `TRUNCATE` (which is not cast to a `%WalEx.Event{}`), so the
indexes your handlers see can have gaps.

## Event modules

For an app named `MyApp` and a subscription `"user"`, WalEx assumes a module
`MyApp.Events.User` that uses `WalEx.Event`. Any other module works too if you
add it to the `modules` option (see
[Configuration](configuration.md#name-and-event-modules)).

```elixir
defmodule MyApp.Events.User do
  use WalEx.Event, name: MyApp

  # any user insert event
  on_insert(:user, fn users ->
    IO.inspect(on_insert: users)
  end)

  on_update(:user, fn users ->
    IO.inspect(on_update: users)
  end)

  on_delete(:user, fn users ->
    IO.inspect(on_delete: users)
  end)
end
```

To handle every event of a table, or every subscribed event, use `on_event`.
This module is not named after a subscription, so list it in `modules`:

```elixir
defmodule MyApp.Events.Audit do
  use WalEx.Event, name: MyApp

  # any subscribed event
  on_event(:all, fn events ->
    IO.inspect(events: events)
  end)
end
```

| Macro | Receives |
| --- | --- |
| `on_event(:all, fun)` | Every subscribed event in the transaction. |
| `on_event(table, filters, functions, fun)` | Every event for `table`. |
| `on_insert(table, filters, functions, fun)` | Insert events for `table`. |
| `on_update(table, filters, functions, fun)` | Update events for `table`. |
| `on_delete(table, filters, functions, fun)` | Delete events for `table`. |
| `on_connect(fun)` | Connection info, see [`on_connect`](#on_connect). |

`filters` and `functions` are optional. A handler is only called when at least
one event remains after filtering.

Each macro defines one function in your module (`on_event(:all, ...)` and
`on_event(table, ...)` both define `process_all/1`). A second block of the same
kind in one module is never reached, so use one module per table, or one
module with at most one block of each kind.

## Filters

Filters drop events before your handler sees them:

- `unwatched_records`: ignore events whose record has the given key and value.
  Use it to "unsubscribe" from specific records, for example during a
  migration or data fix, by adding a column like `event_subscribe: false`.
- `unwatched_fields`: ignore an update whose only changes are in these fields,
  for example `updated_at` set by a trigger, or a count.

More filter helpers are in `WalEx.TransactionFilter`.

```elixir
defmodule MyApp.Events.User do
  use WalEx.Event, name: MyApp

  @filters %{
    unwatched_records: %{event_subscribe: false},
    unwatched_fields: ~w(event_id updated_at todos_count)a
  }

  on_insert(:user, @filters, fn users ->
    IO.inspect(on_insert: users)
  end)
end
```

## Functions

You can pass a list of functions to apply to each event, after the filters.
Each function runs as an async `Task` per event. An entry is either an atom, a
function of the current module, or a `{Module, function}` tuple. Each function
takes a single event argument.

```elixir
defmodule MyApp.Events.User do
  use WalEx.Event, name: MyApp

  @filters %{unwatched_records: %{event_subscribe: false}}
  @functions ~w(send_welcome_email add_to_crm clear_cache)a

  on_insert(:user, @filters, @functions, fn users ->
    IO.inspect(on_insert: users)
  end)

  def send_welcome_email(_user), do: :ok
  def add_to_crm(_user), do: :ok
  def clear_cache(_user), do: :ok
end
```

> #### Use with caution {: .warning}
>
> These tasks are not supervised or retried. WalEx does not wait for them, and
> their failures are not retried. They are outside the
> [`ack_after_processing`](delivery-guarantees.md) guarantee: a transaction
> can be acknowledged before they run or after they fail.

## on_connect

Postgres redelivers a slot's unacknowledged transactions whenever the
connection comes back, for example after a failover, restore or major upgrade.
Code that caches anything about the stream should be told. Define
`on_connect` in a module that uses `WalEx.Event`:

```elixir
defmodule MyApp.Events do
  use WalEx.Event, name: MyApp

  on_connect(fn %{app_name: _app, slot_name: _slot, reconnect?: reconnect?} ->
    MyApp.Cache.reset(reconnect?)
  end)
end
```

The block receives `%{app_name: atom, slot_name: String.t(), reconnect?:
boolean}` and runs in `WalEx.Events.EventModules`, the process that runs your
event handlers. It runs on every connection, including the first
(`reconnect?: false`). `reconnect?` stays `true` when WalEx's replication
processes restart after a crash, because Postgres redelivers then too. An
error raised in the block is logged and ignored.

- **With [`ack_after_processing`](delivery-guarantees.md)** it runs
  synchronously during connection setup, once per connection (so also once per
  retry of a failing transaction). The connection waits for every module's
  hook before it checks the publication and starts streaming, so a slow hook
  delays streaming and a hook that never returns blocks the connection. The
  first connection is made while the WalEx supervisor is starting, so the hook
  must not depend on processes your application starts after WalEx. Waiting on
  one would deadlock your application's boot.
- **Without it** the hook is best effort and asynchronous. It is queued as
  soon as the connection opens and runs before any transaction from the new
  connection reaches your handlers. Transactions that the previous connection
  had already queued may still be handled after it, and a slow hook delays
  event processing.

To observe connections from outside an event module, use the [connection
telemetry events](telemetry.md#connection-lifecycle).
