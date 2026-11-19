# Telemetry

WalEx emits [`:telemetry`](https://hexdocs.pm/telemetry) events for the
replication connection and for each call into your event handlers.

```elixir
require Logger

:telemetry.attach(
  "walex-handler-failed",
  [:walex, :replication, :handler_failed],
  fn _event, %{consecutive_failures: failures}, %{reason: reason}, _config ->
    Logger.error("WalEx handler failed #{failures} times in a row: #{inspect(reason)}")
  end,
  nil
)
```

## Connection lifecycle

These events carry `app_name` and `slot_name` as metadata and a `system_time`
measurement.

| Event | When |
| --- | --- |
| `[:walex, :replication, :connected]` | Network connection opened, before the publication check and slot acquisition (which may wait while another client holds the slot). Changes are not flowing yet. Metadata also has `reconnect?`. |
| `[:walex, :replication, :streaming]` | On the first replication message of a connection, that is, once the slot is acquired and Postgres is streaming. |
| `[:walex, :replication, :disconnected]` | When the connection drops. |
| `[:walex, :replication, :handler_failed]` | With `ack_after_processing: true`, when WalEx disconnects because a handler failed. See below. |
| `[:walex, :replication, :slot_missing]` | With `ack_after_processing: true`, when WalEx will not stream because the durable slot is missing or invalidated. See below. |

### handler_failed

Additional measurement: `consecutive_failures`, the number of failures in a
row, counting this one.

Additional metadata: `reason`, which is
`{:handler_failed, %{module: _, function: _, kind: _, exception: _}}`:

- `kind` is `:error`, `:throw` or `:exit`.
- `exception` is the exception module for `:error`, otherwise `nil`.
- When the crash was in WalEx's handler process itself, `module` and
  `function` are `WalEx.Events.EventModules` and `:process`, `kind` is `:exit`
  and `exception` is `nil`.

The reason never carries the exception message, because it can contain row
data. The full error, with its stacktrace, is logged once when the handler
fails.

The event does not fire for the other failures that make WalEx disconnect, or
for a disconnect by Postgres because handlers took longer than
`wal_sender_timeout`. See [Delivery
Guarantees](delivery-guarantees.md#handler-failures-and-backoff).

### slot_missing

Additional metadata: `reason`, either `:missing` (the slot this instance had
streamed from no longer exists) or `:lost` (the slot was invalidated,
`wal_status = 'lost'`). See [Delivery
Guarantees](delivery-guarantees.md#a-missing-or-lost-slot).

## Event handler

Each call into an event module's handler (`process_all`, `process_insert`,
`process_update`, `process_delete`) is a [telemetry
span](https://hexdocs.pm/telemetry/telemetry.html#span/3). All three events
carry `app_name`, `module` and `function` as metadata.

| Event | Extra data |
| --- | --- |
| `[:walex, :event_handler, :start]` | |
| `[:walex, :event_handler, :stop]` | The call's `duration`. |
| `[:walex, :event_handler, :exception]` | `kind`, `reason` and `stacktrace`, when the handler raises. |

Without `ack_after_processing`, the error is then re-raised and crashes
WalEx's event processes. With it, the failure is handled as described in
[Delivery Guarantees](delivery-guarantees.md#handler-failures-and-backoff).

The exception event carries the exception and its stacktrace, which can
contain row data. Treat it accordingly.
