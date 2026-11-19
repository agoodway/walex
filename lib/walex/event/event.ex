defmodule WalEx.Event do
  @moduledoc """
  User-facing change event struct.

  Each replicated row change is cast into a `%WalEx.Event{}` containing the
  table (`name`), kind (`type`: `:insert | :update | :delete`), record(s),
  changed fields (for updates), commit timestamp, LSN, `change_index` (the change's
  position within its transaction), and source metadata.

  Host modules `use WalEx.Event, name: MyApp` to gain the `on_insert/4`,
  `on_update/4`, `on_delete/4`, and `on_event/2` macros from `WalEx.Event.Dsl`.
  """

  alias Postgrex.ReplicationConnection
  alias WalEx.{Changes, Config, Decoder, Event, Event.Source, Helpers}

  @derive Jason.Encoder
  defstruct([
    :name,
    :type,
    :source,
    :new_record,
    :old_record,
    :changes,
    :timestamp,
    :lsn,
    :change_index
  ])

  @type t :: %__MODULE__{
          name: atom(),
          type: :insert | :update | :delete,
          source: Source.t(),
          new_record: map() | nil,
          old_record: map() | nil,
          changes: map() | nil,
          timestamp: DateTime.t(),
          lsn: Changes.lsn(),
          change_index: Changes.change_index() | nil
        }

  import WalEx.TransactionFilter

  @doc """
  Installs the `WalEx.Event.Dsl` macros (`on_insert/4`, `on_update/4`,
  `on_delete/4`, `on_event/2`) and `filter_events/1,4` helpers into the
  calling module, bound to the given `:name` (WalEx app instance).
  """
  defmacro __using__(opts) do
    app_name = Keyword.get(opts, :name)

    quote do
      import WalEx.Event.Dsl

      def filter_events(txn) do
        Event.filter_and_cast(unquote(app_name), txn)
      end

      def filter_events(txn, name, type, filters) do
        Event.filter_and_cast(unquote(app_name), txn, name, type, filters)
      end
    end
  end

  @doc """
  Casts a `WalEx.Changes.{NewRecord,UpdatedRecord,DeletedRecord}` into an
  `Event` struct, attaching source metadata for `app_name`. Returns `nil` for
  unrecognised inputs.
  """
  def cast(change = %Changes.NewRecord{type: "INSERT", record: record}, app_name),
    do: %Event{base_event(change, app_name) | type: :insert, new_record: record}

  def cast(
        change = %Changes.UpdatedRecord{type: "UPDATE", record: record, old_record: old_record},
        app_name
      ) do
    %Event{
      base_event(change, app_name)
      | type: :update,
        new_record: record,
        changes: map_changes(old_record, record)
    }
  end

  def cast(change = %Changes.DeletedRecord{type: "DELETE", old_record: old_record}, app_name),
    do: %Event{base_event(change, app_name) | type: :delete, old_record: old_record}

  def cast(_event, _event_name), do: nil

  # The fields every row change shares.
  defp base_event(
         %{
           schema: schema,
           table: table,
           columns: columns,
           commit_timestamp: timestamp,
           lsn: lsn,
           change_index: change_index
         },
         app_name
       ) do
    %Event{
      # Bounded: table names come from the database schema, one atom per table
      # (public API: %WalEx.Event{name: atom}).
      name: String.to_atom(table),
      source: cast_source(app_name, schema, table, columns),
      timestamp: timestamp,
      lsn: lsn,
      change_index: change_index
    }
  end

  defp cast_source(app_name, schema, table, columns) do
    %Source{
      name: Helpers.get_source_name(),
      version: Helpers.get_source_version(),
      db: Config.get_database(app_name),
      schema: schema,
      table: table,
      columns: map_columns(columns)
    }
  end

  @doc """
  Casts every change in a list (typically `txn.changes`) to an `Event` for
  `app_name`. Changes with no `Event` form (a `TRUNCATE`) are left out, so
  handlers never receive `nil`.
  """
  def cast_events(changes, app_name) do
    for change <- changes, event = cast(change, app_name), do: event
  end

  @doc """
  Filter out events by table and type (optional) from transaction and cast to Event struct
  """
  def filter_and_cast(app_name, txn) do
    txn
    |> filter_subscribed(app_name)
    |> cast_events(app_name)
  end

  # TODO: change order of filter (to before cast!)
  def filter_and_cast(app_name, txn, table, type, %{
        unwatched_records: unwatched_records,
        unwatched_fields: unwatched_fields
      }) do
    txn
    |> filter_changes(table, type, app_name)
    |> cast_events(app_name)
    |> filter_unwatched_records(unwatched_records)
    |> filter_unwatched_fields(unwatched_fields)
  end

  def filter_and_cast(app_name, txn, table, type, %{unwatched_records: unwatched_records}) do
    txn
    |> filter_changes(table, type, app_name)
    |> cast_events(app_name)
    |> filter_unwatched_records(unwatched_records)
  end

  def filter_and_cast(app_name, txn, table, type, %{unwatched_fields: unwatched_fields}) do
    txn
    |> filter_changes(table, type, app_name)
    |> cast_events(app_name)
    |> filter_unwatched_fields(unwatched_fields)
  end

  def filter_and_cast(app_name, txn, table, type, _filters) do
    txn
    |> filter_changes(table, type, app_name)
    |> cast_events(app_name)
  end

  @doc "Formats an LSN tuple the way Postgres prints it, e.g. `\"16/B374D848\"`."
  @spec lsn_to_string(Changes.lsn()) :: String.t()
  def lsn_to_string(lsn) do
    {:ok, string} = lsn |> Decoder.lsn_to_integer() |> ReplicationConnection.encode_lsn()
    string
  end
end
