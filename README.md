# WalEx

[![Hex.pm](https://img.shields.io/hexpm/v/walex.svg)](https://hex.pm/packages/walex)
[![Documentation](https://img.shields.io/badge/documentation-hexdocs-blue.svg)](https://hexdocs.pm/walex)

Simple and reliable Postgres [Change Data Capture
(CDC)](https://en.wikipedia.org/wiki/Change_data_capture) in Elixir.

![Walex mascot](mascot.jpeg)

WalEx allows you to listen to change events on your Postgres tables then perform callback-like actions with the data via the [Event DSL](#event-module). For example:

- Stream database changes to an external service
- Send a user a welcome email after they create a new account
- Augment an existing Postgres-backed application with business logic
- Send events to third party services (analytics, CRM, webhooks, etc))
- Update index / invalidate cache whenever a record is changed

You can learn more about CDC and what you can do with it here: [Why capture changes?](https://bbhoss.io/posts/announcing-cainophile/#why-capture-changes)

## Credit

This library borrows liberally from
[realtime](https://github.com/supabase/realtime) from Supabase, which in turn
draws heavily on [cainophile](https://github.com/cainophile/cainophile).

## Alternatives

If you don't need to subscribe to WAL events directly in Elixir or are not an Elixir user, take a look at [Sequin](https://github.com/sequinstream/sequin), which allows you to stream Postgres changes to a variety of destinations, including Kafka, S3, and more.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `walex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:walex, "~> 4.7.1"}
  ]
end
```

## PostgreSQL Configuration

### Logical Replication

WalEx only supports PostgreSQL. To get started, you first need to configure
PostgreSQL for [logical replication](https://www.crunchydata.com/blog/data-to-go-postgres-logical-replication):

```sql
ALTER SYSTEM SET wal_level = 'logical';
```

Docker Compose:

```bash
command: [ "postgres", "-c", "wal_level=logical" ]
```

### Publication

When you change the `wal_level` variable, you'll need to restart your
PostgreSQL server. Once you've restarted, go ahead and [create a
publication](https://www.postgresql.org/docs/current/sql-createpublication.html)
for the tables you want to receive changes for:

All tables:

```sql
CREATE PUBLICATION events FOR ALL TABLES;
```

Or just specific tables:

```sql
CREATE PUBLICATION events FOR TABLE user, todo;
```

Filter based on [row conditions](https://www.postgresql.fastware.com/blog/introducing-publication-row-filters) (Postgres v15+ only):

```sql
CREATE PUBLICATION user_event FOR TABLE user WHERE (active IS TRUE);
```

### Replica Identity

WalEx supports all of the settings for [REPLICA
IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY).
Use `FULL` if you can use it, as it will make tracking differences [easier](https://xata.io/blog/replica-identity-full-performance) as
the old data will be sent alongside the new data. You'll need to set this for
each table.

Specific tables:

```sql
ALTER TABLE user REPLICA IDENTITY FULL;
ALTER TABLE todo REPLICA IDENTITY FULL;
```

Also, be mindful of [replication gotchas](https://pgdash.io/blog/postgres-replication-gotchas.html).

### AWS RDS

Amazon (AWS) RDS Postgres allows you to configure logical replication.

- <https://dev.to/vumdao/how-to-change-rds-postgresql-configurations-2kmk>

When creating a new Postgres database on RDS, you'll need to set a Parameter
Group with the following settings:

```text
rds.logical_replication = 1
max_replication_slots = 5
max_slot_wal_keep_size = 2048
```

## Elixir Configuration

### Config

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
  # WalEx assumes your module names match this pattern: MyApp.Events.User, MyApp.Events.ToDo, etc
  # but you can also specify custom modules like so:
  # modules: [MyApp.CustomModule.User, MyApp.OtherCustomModule.ToDo],
  name: MyApp
```

It is also possible to just define the URL configuration for the database

```elixir
# config.exs

config :my_app, WalEx,
  url: "postgres://username:password@hostname:port/database"
  publication: "events",
  subscriptions: ["user", "todo"],
  name: MyApp
```

If you would like to subscribe to all tables, you can pass a special atom to subscriptions.

```elixir
config :my_app, WalEx,
  url: "postgres://username:password@hostname:port/database",
  publication: "events",
  subscriptions: [:all_tables],
  name: MyApp,
```

You can also dynamically update the config at runtime:

```elixir
WalEx.Configs.add_config(MyApp, :subscriptions, ["new_subscriptions_1", "new_subscriptions_2"])
WalEx.Configs.remove_config(MyApp, :subscriptions, "subscriptions")
WalEx.Configs.replace_config(MyApp, :password, "new_password")
```

### Application Supervisor

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {WalEx.Supervisor, Application.get_env(:my_app, WalEx)}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

## Usage

### Event

Returned change data is a List of [%Event{}](lib/walex/event/event.ex) structs with changes. UPDATE event example
where _name_ field was changed):

```elixir
[
  %Walex.Event{
    name: :user,
    type: :update,
    source: %WalEx.Event.Source{
      name: "WalEx",
      version: "3.8.0",
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
    # we don't show old_record for update to reduce payload size
    # however, you can see any old values that changed under "changes"
    old_record: nil,
    changes: %{
      name: %{
        new_value: "Chase Pursley",
        old_value: "Chase"
      }
    },
    timestamp: ~U[2023-12-18 15:50:08.329504Z]
  }
]
```

### Event Module

If your app is named _MyApp_ and you have a subscription called _:user_ (which represents a database table), WalEx assumes you have a module called `MyApp.Events.User` that uses WalEx Event. But you can also define any custom module, just be sure to add it to the _modules_ config.

Note that the result of `events` is a list. This is because WalEx returns a _List_ of  _transactions_ for a particular table when there's a change event. Often times this will just contain one result, but it could be many (for example, if you use database triggers to update a column after an insert).

```elixir
defmodule MyApp.Events.User do
  use WalEx.Event, name: MyApp

  # any subscribed event
  on_event(:all, fn events ->
    IO.inspect(events: events)
  end)

  # any user event
  on_event(:user, fn users ->
    IO.inspect(on_event: users)
    # do something with users data
  end)

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

##### Filters

A common scenario is where you want to _"unsubscribe"_ from specific records (for example, temporarily for a migration or data fix). One way to accomplish this is to have a column with a value like `event_subscribe: false`. Then you can ignore specific events by specifying their key and value to *unwatched_records*.

Another scenario is you might not care when just certain fields change. For example, maybe a database trigger sets updated_at _after_ a record is updated. Or a count changes, or several do that you don't need to react to. In this case, you can ignore the event change by adding them to *unwatched_fields*.

Additional filter helpers available in the
[WalEx.TransactionFilter](lib/walex/transaction_filter.ex) module.

```elixir
defmodule MyApp.Events.User do
  use WalEx.Event, name: MyApp

  @filters %{
    unwatched_records: %{event_subscribe: false},
    unwatched_fields: ~w(event_id updated_at todos_count)a
  }

  on_insert(:user, @filters, fn users ->
    IO.inspect(on_insert: users)
    # resulting users data is filtered
  end)
end
```

##### Functions

You can also provide a list of functions (as atoms) to be applied to each Event (after optional filters are applied). Each function is run as an async Task on each event. The functions must be defined in the current module and take a single _event_ argument. Use with caution!

```elixir
defmodule MyApp.Events.User do
  use WalEx.Event, name: MyApp

  @filters %{unwatched_records: %{event_subscribe: false}}
  @functions ~w(send_welcome_email add_to_crm clear_cache)a

  on_insert(:user, @filters, @functions, fn users ->
    IO.inspect(on_insert: users)
    # resulting users data is first filtered then functions are applied
  end)

  def send_welcome_email(user) do
    # logic for sending welcome email to new user
  end

  def add_to_crm(user) do
  # logic for adding user to crm system
  end

  def clear_cache(user) do
  # logic for clearing user cache
  end
end
```

### Advanced usages

##### Durable slot

By default WalEx will create a temporary replication slot in Postgres.

This means that if the connection between WalEx and Postgres gets interrupted (crash / disconnection / etc.),
the replication slot will get dropped by Postgres.
This makes using WalEx safer as there is not risk of filling up the disk of the Postgres writer instance in
case of downtime.

The downside being that this event-loss is more than likely.
If this is a no-go, WalEx also supports durable replication.

```elixir
# config.exs

config :my_app, WalEx,
  # ...
  durable_slot: true,
  slot_name: my_app_replication_slot
```

Only a single process can be connected to a durable slot at once,
in case the slot is already used `WalEx.Supervisor` will fail to start with a `RuntimeError`.

Be warned that there are many additional potential gotchas (a detailed guide is planned).

##### Event middleware / Back-pressure

WalEx receives events from Postgres in `WalEx.Replication.Server` and then `cast` those to `WalEx.Replication.Publisher`.
It's then `WalEx.Replication.Publisher` that is responsible to join these events together and process them.

In the event where you'd expect Postgres to overwhelm WalEx and potentially cause OOMs,
WalEx provides a config option that should help you implement back-pressure.

As it's a quite advanced use, with many strong requirements,
it's recommended instead to increase the amount of RAM of your instance.

Never the less, if it's not an option or would like to control the consumption rate of events,
WalEx provide the following configuration option:

```
config :my_app, WalEx,
  # ...
  message_middleware: fn message, app_name -> ... end
```

`message_middleware` allows you to define the way `WalEx.Replication.Server` and `WalEx.Replication.Publisher` communicate.

If for instance you'd like to store these events to disk before processing them you would need to:

- provide a `message_middleware` callback. It should serialize messages and store them to disk
- add a supervised strictly-ordered disk consumer. On each event it would call one of:
  - `WalEx.Replication.Publisher.process_message_async(message, app_name)`
  - `WalEx.Replication.Publisher.process_message_sync(message, app_name)`

Any back-pressure implementation needs to guarantee:

- exact message ordering
- exactly-once-delivery
- that each running walex has an isolated back-pressure system (for instance one queue per instance)

## Test

You'll need a local Postgres instance running

```bash
MIX_ENV=test mix walex.setup
MIX_ENV=test mix test
```

### Optional Custom DB Settings

If you have a customized local postgresql server running, you can override the default user, pass etc by setting the following ENV vars. These will then take precedence in tests and `mix walex.setup`.

* PGUSER
* PGPASSWORD
* PGHOST
* PGPORT
