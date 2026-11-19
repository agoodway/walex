# Delivery Guarantees

This guide explains when WalEx tells Postgres that a change has been handled,
and what that means for your handlers.

## Default behavior

By default WalEx acknowledges what it has received, regardless of whether your
event handlers have finished. It acknowledges whenever Postgres asks for a
status update, and on the
[`standby_status_interval`](configuration.md#standby_status_interval) schedule
if you set one. The acknowledged position is the WAL position Postgres last
reported, or the end of the last COMMIT received, whichever is later.

If your process crashes in the middle of a handler, the slot may already have
confirmed that transaction. Postgres then skips it on reconnect, and the event
is not replayed. Handling is effectively at-most-once with respect to handler
crashes. Duplicates are still possible (see [Idempotency](#idempotency)).

## ack_after_processing

For apps on a **durable slot** that need stronger guarantees, WalEx can hold
back the acknowledgement until each transaction has been fully handed off to
your event handlers:

```elixir
config :my_app, WalEx,
  # ...
  durable_slot: true,          # required: temporary slots lose WAL on reconnect anyway
  slot_name: :my_app_slot,
  ack_after_processing: true   # only acknowledge WAL after successful handler dispatch
```

The requirements (PostgreSQL 13+, `durable_slot: true`, the default
middleware, at least one event module) and the runtime locks are in
[Configuration](configuration.md#ack_after_processing). With the option on:

- WAL messages are processed **synchronously**. Each transaction blocks the
  replication connection until `WalEx.Events` has accepted it.
- The acknowledged position only advances to positions that have been
  confirmed, so after a crash and reconnect, Postgres replays any
  unacknowledged transactions from the durable slot.
- Delivery is **at-least-once**, not exactly-once. See
  [Idempotency](#idempotency).
- All handlers of a transaction run in order. If handler A succeeds and
  handler B raises, the remaining handlers are skipped and the transaction is
  redelivered, so A runs again.
- The default `standby_status_interval` is `10_000` (10 s), because the slot
  only advances through status updates. After each handled commit WalEx also
  sends a status update right away, at most once per second.
- Between transactions, WalEx acknowledges the position Postgres reported,
  because everything received so far has been handled. Inside a transaction,
  it acknowledges only the last handled commit. At connect it reports the
  slot's own saved position. Keepalive requests are always answered, with a
  position of `0` if nothing is known, so the connection is not dropped for
  silence while the slot stays where it is.

## Idempotency

After a crash, a disconnect or a handler failure, Postgres redelivers every
transaction the slot has not confirmed, including some that your handlers
already handled. After a database crash or failover it may also replay
transactions that were already acknowledged, because Postgres does not write
the confirmed position to disk on every update. This holds in either mode, so
handlers must be idempotent.

Use `{lsn, change_index}` as the dedupe key. It is unique per change and stable
across redeliveries (see [Events](events.md#the-event-struct)).

If your code caches anything about the stream, reset it in
[`on_connect`](events.md#on_connect).

## What counts as success

A handler is successful unless it raises, throws or exits. Its return value is
ignored, so returning `{:error, _}` counts as success and the transaction is
acknowledged. To get a redelivery, raise.

"Acknowledged" means every handler block for the transaction returned without
raising, throwing or exiting. It does not mean that your downstream I/O
(database write, HTTP call, and so on) succeeded: a handler that swallows an
error still counts as done. For an end-to-end guarantee, write to a durable
queue (Oban, Kafka, SQS or similar) inside the handler, and rely on that
system's retry semantics.

Only the `on_event`, `on_insert`, `on_update` and `on_delete` blocks are
covered. [Functions](events.md#functions) run in unsupervised `Task`s, outside
the guarantee.

## Handler failures and backoff

With `ack_after_processing`, a handler that raises, throws or exits does not
crash anything, so there is no supervisor restart storm. The same holds when a
handler fails through a process it linked to (for example a `Task.async` that
crashes), which kills WalEx's handler process: WalEx reports it as a handler
failure and restarts only that process. Without `ack_after_processing`, a
handler that raises crashes WalEx's event processes, and the result of a
message is ignored.

On a failure, WalEx logs the error, skips the remaining handlers for that
transaction, disconnects from Postgres and reconnects. Postgres then replays
the transaction.

**Backoff.** Starting with the first failure, WalEx waits before it resumes:
1 s, then 2 s, 4 s and so on, up to 60 s.

- The wait is kept across reconnects. A connection dropped while it waits (for
  example by `idle_session_timeout` or a proxy) resumes at the original time
  instead of starting over. Avoid `idle_session_timeout` on the replication
  role.
- Transactions from before the failing one may be replayed and handled again,
  but that is not progress. The wait only resets once replication gets past
  the failing transaction.

**Other processing failures.** The same backoff applies to the other failures
that make WalEx disconnect. They do not emit `handler_failed`:

| Disconnect reason | Cause |
| --- | --- |
| `{:decode_failed, %{kind: _, exception: _}}` | A replication message could not be decoded. |
| `{:publisher_failed, %{kind: _, exception: _}}` | An error inside WalEx while it built the transaction, for example a value it failed to cast. The full error is logged. |
| `{:column_mismatch, relation_id}` | A change whose values do not match its relation's columns. |
| `{:unknown_relation, relation_id}` | A change for a relation WalEx has no definition for. |
| `{:commit_mismatch, %{commit_lsn: _, open_lsn: _}}` | A COMMIT that does not match the open transaction. |
| `{:unexpected_middleware_result, result}` | A custom `message_middleware` returned something other than `:ok` or `{:error, _}`. |

The failure reports never carry the exception message, because it can contain
row data. See [Telemetry](telemetry.md#handler_failed).

## Poison pills

A transaction that **always** fails (a "poison pill") is retried forever, at
most once a minute. A permanently failing handler therefore halts replication
while Postgres keeps retaining WAL, until it is fixed.

Alert on the `[:walex, :replication, :handler_failed]` telemetry event, whose
`consecutive_failures` measurement counts the failures in a row, and on
replication slot lag.

WalEx has no built-in skip, because only your application knows which failures
are permanent. Use a handler-level dead letter: rescue the known permanent
error around one event's work, store that event with its `{lsn, change_index}`
key in a dead-letter table, and return normally only after that write
succeeds. If the write fails, raise, and WalEx retries. The remaining events
and handlers keep running.

```elixir
defmodule MyApp.Events.User do
  use WalEx.Event, name: MyApp

  on_insert(:user, fn users ->
    Enum.each(users, &push/1)
  end)

  defp push(event) do
    Sync.push(event.new_record)
  rescue
    error in Sync.PermanentError -> dead_letter!(event, error)
  end

  # Repo.insert!/2 raises when the write fails, so WalEx retries the
  # transaction. The unique {lsn, change_index} key makes a retry safe.
  defp dead_letter!(event, error) do
    MyApp.Repo.insert!(
      %MyApp.DeadLetter{
        lsn: WalEx.Event.lsn_to_string(event.lsn),
        change_index: event.change_index,
        record: event.new_record,
        error: inspect(error.__struct__)
      },
      on_conflict: :nothing
    )

    :ok
  end
end
```

Rescue only errors you know are permanent. Anything else should still raise,
so the transaction is retried. Store the exception module (as above) rather
than its message, which can contain row data.

## Slow handlers and wal_sender_timeout

While a transaction's handlers run, the replication connection is blocked and
cannot answer Postgres. Postgres pings a client it has not heard from for half
of [`wal_sender_timeout`](https://www.postgresql.org/docs/current/runtime-config-replication.html)
(default 60 s), and disconnects it once `wal_sender_timeout` has passed since
it last heard from it.

WalEx reports its position after handled commits and every
`standby_status_interval`. The handlers of one transaction therefore have
roughly `wal_sender_timeout` minus that interval, about 50 s by default. Stay
well under it. Providers may not use Postgres's 60 s default (RDS and
CloudNativePG, for example), so check it.

A transaction that takes longer is disconnected by Postgres and redelivered.
That is a transport disconnect, not a handler failure: it is **not** backed
off and does not emit `handler_failed`. A transaction whose handlers always
take that long is redelivered immediately, over and over, and its side effects
repeat each time.

For slow handlers, raise the timeout for the replication role. It applies to
that role's new connections, and `ALTER ROLE ... SET wal_sender_timeout`
requires PostgreSQL 12+:

```sql
ALTER ROLE <replication_user> SET wal_sender_timeout = '5min';
```

## WAL retention

Holding back acknowledgements makes Postgres retain WAL from the slot's
confirmed position on. See [Limiting retained
WAL](postgres-setup.md#limiting-retained-wal). When the limit is exceeded,
Postgres invalidates the slot (next section).

## A missing or lost slot

With `ack_after_processing`, if a durable slot that this WalEx instance has
already streamed from disappears, WalEx does not create a new one. A new slot
starts at the current position, so every change since the last acknowledgement
would be skipped. A slot can disappear when:

- it was dropped
- it was lost in a failover without failover slots
- it was lost in a `pg_upgrade` before PostgreSQL 17
- a provider cleaned it up

When this happens, WalEx logs an error, emits
`[:walex, :replication, :slot_missing]` with `reason: :missing`, and checks
again with the backoff above, up to once a minute.

An invalidated slot (`wal_status = 'lost'`, for example after exceeding
`max_slot_wal_keep_size`, or PostgreSQL 18's `idle_replication_slot_timeout`)
is treated the same way, with `reason: :lost`.

To recover, restore the slot, or accept the gap by dropping and recreating it.
For a missing slot, you can also restart WalEx, which then creates it.

This only protects reconnects within a running WalEx. A fresh start cannot
tell a *missing* slot from a first deploy, so it creates the slot. An
invalidated slot is detected on a cold start too.
