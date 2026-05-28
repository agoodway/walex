# This file steals liberally from https://github.com/supabase/realtime,
# which in turn draws on https://github.com/cainophile/cainophile

defmodule WalEx.TransactionFilter do
  @moduledoc """
  Filter transactions based on the relation and event type.
  """

  alias WalEx.Changes.{
    DeletedRecord,
    NewRecord,
    Transaction,
    UpdatedRecord
  }

  alias WalEx.Config
  alias WalEx.Decoder.Messages.Relation.Column

  require Logger

  defmodule Filter do
    @moduledoc "Parsed relation filter: schema, table, and an optional condition."
    defstruct [:schema, :table, :condition]

    @type t :: %__MODULE__{
            schema: String.t() | nil,
            table: String.t() | nil,
            condition: term() | nil
          }
  end

  @doc """
  Predicate to check if the filter matches the transaction.

  ## Examples

      iex> txn = %Transaction{changes: [
      ...>   %WalEx.Changes.NewRecord{
      ...>     columns: [
      ...>       %WalEx.Decoder.Messages.Relation.Column{flags: [:key], name: "id", type: "int8", type_modifier: 4294967295},
      ...>       %WalEx.Decoder.Messages.Relation.Column{flags: [], name: "details", type: "text", type_modifier: 4294967295},
      ...>       %WalEx.Decoder.Messages.Relation.Column{flags: [], name: "user_id", type: "int8", type_modifier: 4294967295}
      ...>     ],
      ...>     commit_timestamp: nil,
      ...>     record: %{"details" => "The SCSI system is down, program the haptic microchip so we can back up the SAS circuit!", "id" => "14", "user_id" => "1"},
      ...>     schema: "public",
      ...>     table: "todos",
      ...>     type: "INSERT"
      ...>   }
      ...> ]}
      iex> matches?(%{event: "*", relation: "*"}, txn)
      true
      iex> matches?(%{event: "INSERT", relation: "*"}, txn)
      true
      iex> matches?(%{event: "UPDATE", relation: "*"}, txn)
      false
      iex> matches?(%{event: "INSERT", relation: "public"}, txn)
      true
      iex> matches?(%{event: "INSERT", relation: "myschema"}, txn)
      false
      iex> matches?(%{event: "INSERT", relation: "public:todos"}, txn)
      true
      iex> matches?(%{event: "INSERT", relation: "myschema:users"}, txn)
      false

  """
  def matches?(%{event: event, relation: relation}, %Transaction{changes: changes}) do
    case parse_relation_filter(relation) do
      {:ok, filter} ->
        Enum.any?(changes, fn change -> change_matches(event, filter, change) end)

      {:error, _msg} ->
        false
    end
  end

  # malformed filter or txn. Should not match.
  def matches?(_filter, _txn), do: false

  defp change_matches(event, _filter, %{type: type}) when event != type and event != "*" do
    false
  end

  defp change_matches(_event, filter, change) do
    name_matches?(filter.schema, change.schema) and name_matches?(filter.table, change.table)
  end

  @doc """
  Parse a string representing a relation filter to a `Filter` struct.

  ## Examples

      iex> parse_relation_filter("public:users")
      {:ok, %Filter{schema: "public", table: "users", condition: nil}}

      iex> parse_relation_filter("public")
      {:ok, %Filter{schema: "public", table: nil, condition: nil}}


      iex> parse_relation_filter("")
      {:ok, %Filter{schema: nil, table: nil, condition: nil}}

      iex> parse_relation_filter("public:users:bad")
      {:error, "malformed relation filter"}

  """
  def parse_relation_filter(relation) do
    # We do a very loose validation here.
    # When the relation filter format is well defined we can do
    # proper parsing and validation.
    case String.split(relation, ":") do
      [""] -> {:ok, %Filter{schema: nil, table: nil, condition: nil}}
      ["*"] -> {:ok, %Filter{schema: nil, table: nil, condition: nil}}
      [schema] -> {:ok, %Filter{schema: schema, table: nil, condition: nil}}
      [schema, table] -> {:ok, %Filter{schema: schema, table: table, condition: nil}}
      _ -> {:error, "malformed relation filter"}
    end
  end

  defp name_matches?(nil, _change_name), do: true
  defp name_matches?(filter_name, change_name), do: filter_name == change_name

  @doc "Returns `true` if the transaction includes an `INSERT` on the given relation."
  def insert_event?(relation, txn), do: relation("INSERT", relation, txn)

  @doc "Returns `true` if the transaction includes an `UPDATE` on the given relation."
  def update_event?(relation, txn), do: relation("UPDATE", relation, txn)

  @doc "Returns `true` if the transaction includes a `DELETE` on the given relation."
  def delete_event?(relation, txn), do: relation("DELETE", relation, txn)

  defp relation(event, relation, txn) when is_atom(relation) do
    matches?(%{event: event, relation: "public:" <> to_string(relation)}, txn)
  end

  defp relation(event, relation, txn) when is_binary(relation) do
    if String.contains?(relation, ":") do
      matches?(%{event: event, relation: relation}, txn)
    else
      matches?(%{event: event, relation: "public:" <> relation}, txn)
    end
  end

  @doc """
  Returns a list of subscribed changes
  """
  def filter_subscribed(%Transaction{changes: changes}, app_name) do
    Enum.filter(changes, &subscribes?(&1, app_name))
  end

  @doc """
  Returns a list of changes for the given table name and type (optional)
  """
  def filter_changes(%Transaction{changes: changes}, table, nil, app_name) do
    subscribes_and_has_table(changes, table, app_name)
  end

  def filter_changes(%Transaction{changes: changes}, table, type, app_name) do
    changes
    |> subscribes_and_has_table(table, app_name)
    |> Enum.filter(&record_type?(&1, type))
  end

  defp subscribes_and_has_table(changes, table, app_name) do
    Enum.filter(changes, &subscribes_to_table?(&1, table, app_name))
  end

  @doc "Returns `true` when the change targets `table` and the app subscribes to it."
  def subscribes_to_table?(change, table, app_name) do
    has_table?(change, table) && subscribes?(change, app_name)
  end

  @doc "Returns `true` when the app's subscriptions include the change's table (or `:all_tables`)."
  def subscribes?(%{table: table}, app_name) do
    subscriptions = Config.get_configs(app_name, :subscriptions)

    :all_tables in subscriptions || table in subscriptions
  end

  @doc "Returns `true` when the change's table matches `table_name` (atom or string)."
  def has_table?(%{table: table}, table_name) when is_atom(table), do: table == table_name

  def has_table?(%{table: table}, table_name) when is_binary(table),
    do: String.to_atom(table) == table_name

  def has_table?(_txn, _table_name), do: false

  @doc "Returns `true` when the change record matches the requested type (`:insert | :update | :delete`)."
  def record_type?(%NewRecord{type: "INSERT"}, :insert), do: true
  def record_type?(%UpdatedRecord{type: "UPDATE"}, :update), do: true
  def record_type?(%DeletedRecord{type: "DELETE"}, :delete), do: true
  def record_type?(_txn, _type), do: false

  @doc "Drops events whose changed columns are entirely within `unwatched_changes`."
  def filter_unwatched_fields(events, unwatched_changes) do
    Enum.filter(events, &unwatched_fields?(&1, unwatched_changes))
  end

  @doc "Returns `true` if the event has at least one changed column outside `unwatched_changes`."
  def unwatched_fields?(%{changes: nil}, _unwatched_changes), do: true

  def unwatched_fields?(%{changes: changes}, unwatched_changes) do
    changes
    |> Enum.filter(fn {key, _value} -> key not in unwatched_changes end)
    |> Kernel.!=([])
  end

  def unwatched_fields?(_event, _unwatched_changes), do: true

  @doc "Drops events whose new (or old, for deletes) record fully matches `unwatched_records`."
  def filter_unwatched_records(events, unwatched_records) do
    Enum.filter(events, &watched_record?(&1, unwatched_records))
  end

  @doc "Returns `true` when the event's record is not a full match against `unwatched_records`."
  def watched_record?(%{new_record: nil, old_record: old_record = %{}}, unwatched_records) do
    not contains_unwatched_records?(old_record, unwatched_records)
  end

  def watched_record?(%{new_record: new_record = %{}}, unwatched_records) do
    not contains_unwatched_records?(new_record, unwatched_records)
  end

  def watched_record?(_event, _unwatched_records), do: false

  @doc "Returns `true` when every key/value in `unwatched_records` is present in `record`."
  def contains_unwatched_records?(record = %{}, unwatched_records = %{}) do
    Enum.all?(unwatched_records, fn {key, value} ->
      Map.has_key?(record, key) and Map.get(record, key) == value
    end)
  end

  @doc """
  Diffs two records into a map of `field => %{old_value, new_value}` containing
  only the fields whose value changed.
  """
  def map_changes(old_record, new_record) do
    fields = Map.keys(old_record)

    Enum.reduce(fields, %{}, fn field, acc ->
      old_value = Map.get(old_record, field)
      new_value = Map.get(new_record, field)

      if old_value != new_value do
        Map.put(acc, field, %{old_value: old_value, new_value: new_value})
      else
        acc
      end
    end)
  end

  @doc "Builds a `%{column_name => type}` map from a list of `Relation.Column` structs."
  def map_columns(columns) do
    Enum.reduce(columns, %{}, fn %Column{name: name, type: type}, acc ->
      name = String.to_atom(name)
      Map.put(acc, name, type)
    end)
  end
end
