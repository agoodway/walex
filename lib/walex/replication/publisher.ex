defmodule WalEx.Replication.Publisher do
  @moduledoc """
  GenServer that accumulates decoded `pgoutput` messages into transactions and
  hands the resulting `WalEx.Changes.Transaction` off to `WalEx.Events`.

  Tracks the relation and type tables built up from `Relation` and `Type`
  messages so that subsequent `Insert`/`Update`/`Delete` messages can be
  hydrated into typed change records.
  """
  use GenServer

  alias WalEx.{Casting.Types, Changes, Config, Decoder.Messages, Event, Events, Helpers}
  alias WalEx.Config.Registry, as: WalExRegistry

  require Logger

  defmodule State do
    @moduledoc "Publisher state: open transaction plus accumulated relation and type tables."
    defstruct ack_after_processing: false, relations: %{}, transaction: nil, types: %{}

    @type t :: %__MODULE__{
            ack_after_processing: boolean(),
            relations: %{optional(non_neg_integer()) => struct()},
            transaction: Changes.Transaction.t() | nil,
            types: %{optional(non_neg_integer()) => struct()}
          }
  end

  defstruct [:relations]

  @type t :: %__MODULE__{relations: map() | nil}

  @doc "Starts the publisher GenServer registered under the app name."
  def start_link(opts) do
    name =
      opts
      |> Keyword.get(:app_name)
      |> registry_name

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Casts a decoded replication message into the publisher (fire-and-forget)."
  @spec process_message_async(struct(), atom()) :: :ok
  def process_message_async(message, app_name) do
    name = registry_name(app_name)
    GenServer.cast(name, %{message: message, app_name: app_name})
  end

  @doc """
  Synchronously processes a decoded replication message; used by tests and
  synchronous middleware.

  With `ack_after_processing`, returns `{:error, reason}` when the message
  could not be applied (a dropped COMMIT, a change for an unknown relation or
  whose tuple does not match the relation's columns, a crash while building
  the change, or a failed dispatch), so the caller can avoid acknowledging it.
  Any open transaction is discarded on error.

  Without it, always returns `:ok` and applies the message exactly like
  `process_message_async/2`: a change it cannot apply is skipped with a
  warning and the rest of the transaction is kept.
  """
  @spec process_message_sync(struct(), atom()) :: :ok | {:error, term()}
  def process_message_sync(message, app_name) do
    name = registry_name(app_name)
    GenServer.call(name, %{message: message, app_name: app_name}, :infinity)
  end

  defp registry_name(app_name) do
    WalExRegistry.set_name(:set_gen_server, __MODULE__, app_name)
  end

  @impl true
  def init(opts) do
    Process.flag(:message_queue_data, :off_heap)

    {:ok,
     %State{
       ack_after_processing: opts |> Keyword.get(:app_name) |> Config.ack_after_processing?()
     }}
  end

  @impl true
  def handle_cast(message, state) do
    case process_message(message, state) do
      {:ok, state} -> {:noreply, state}
      # Nobody is waiting for a reply. Every failure was already logged where
      # it was detected: in this module, or by EventModules for a handler.
      {:error, _reason, state} -> {:noreply, state}
    end
  end

  @impl true
  def handle_call(message, _from, state = %State{ack_after_processing: true}) do
    case process_message_safely(message, state) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, %{state | transaction: nil}}
    end
  end

  # Without ack_after_processing nothing acts on the result, so the synchronous
  # path applies the message like the asynchronous one, as it always did: it
  # skips what it cannot apply, keeps the open transaction and replies :ok.
  def handle_call(message, _from, state) do
    {:noreply, state} = handle_cast(message, state)
    {:reply, :ok, state}
  end

  # With ack_after_processing the caller acknowledges only on :ok, so a crash
  # here (a value the casting cannot handle, say) is reported like any other
  # failure: the transaction is redelivered after a backoff instead of the
  # crash taking the replication server down with it and restarting without
  # one. This is the only place that has the stacktrace, so the failure is
  # logged here. Without it, and on the asynchronous path (no caller to tell),
  # a crash propagates as it always did.
  defp process_message_safely(message, state) do
    process_message(message, state)
  catch
    kind, reason ->
      failure =
        Helpers.log_caught_failure(
          "Publisher failed, its transaction will be redelivered:",
          kind,
          reason,
          __STACKTRACE__
        )

      {:error, {:publisher_failed, failure}, state}
  end

  defp process_message(
         %{message: %Messages.Begin{final_lsn: final_lsn, commit_timestamp: commit_timestamp}},
         state = %State{}
       ) do
    {:ok,
     %State{
       state
       | transaction: {
           final_lsn,
           %Changes.Transaction{changes: [], commit_timestamp: commit_timestamp}
         }
     }}
  end

  # A COMMIT for the open transaction: BEGIN announced this same LSN.
  defp process_message(
         %{message: %Messages.Commit{lsn: lsn}, app_name: app_name},
         state = %State{transaction: {lsn, txn}}
       ) do
    case Events.process(txn, app_name) do
      :ok -> {:ok, %State{state | transaction: nil}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp process_message(%{message: %Messages.Type{} = msg}, state) do
    {:ok, %{state | types: Map.put(state.types, msg.id, msg.name)}}
  end

  defp process_message(%{message: %Messages.Relation{} = msg}, state) do
    updated_columns =
      Enum.map(msg.columns, &%{&1 | type: Map.get(state.types, &1.type, &1.type)})

    updated_relations = %{msg | columns: updated_columns}
    {:ok, %{state | relations: Map.put(state.relations, msg.id, updated_relations)}}
  end

  defp process_message(
         %{message: %Messages.Insert{relation_id: relation_id, tuple_data: tuple_data}},
         state = %State{
           transaction: {lsn, %{commit_timestamp: commit_timestamp} = txn},
           relations: relations
         }
       )
       when is_map(relations) do
    case Map.fetch(relations, relation_id) do
      {:ok, %{columns: columns, namespace: namespace, name: name}} when is_list(columns) ->
        data = data_tuple_to_map(columns, tuple_data)

        new_record = %Changes.NewRecord{
          type: "INSERT",
          schema: namespace,
          table: name,
          columns: columns,
          record: data,
          commit_timestamp: commit_timestamp,
          lsn: lsn
        }

        {:ok, state} = add_change(state, lsn, txn, new_record)
        check_columns(state, relation_id, columns, [tuple_data])

      _ ->
        unknown_relation(state, relation_id)
    end
  end

  defp process_message(
         %{
           message: %Messages.Update{
             relation_id: relation_id,
             old_tuple_data: old_tuple_data,
             tuple_data: tuple_data
           }
         },
         state = %State{
           relations: relations,
           transaction: {lsn, %{commit_timestamp: commit_timestamp} = txn}
         }
       )
       when is_map(relations) do
    case Map.fetch(relations, relation_id) do
      {:ok, %{columns: columns, namespace: namespace, name: name}} when is_list(columns) ->
        old_data = data_tuple_to_map(columns, old_tuple_data)
        data = data_tuple_to_map(columns, tuple_data)

        updated_record = %Changes.UpdatedRecord{
          type: "UPDATE",
          schema: namespace,
          table: name,
          columns: columns,
          old_record: old_data,
          record: data,
          commit_timestamp: commit_timestamp,
          lsn: lsn
        }

        {:ok, state} = add_change(state, lsn, txn, updated_record)
        check_columns(state, relation_id, columns, [old_tuple_data, tuple_data])

      _ ->
        unknown_relation(state, relation_id)
    end
  end

  defp process_message(
         %{
           message: %Messages.Delete{
             relation_id: relation_id,
             old_tuple_data: old_tuple_data,
             changed_key_tuple_data: changed_key_tuple_data
           }
         },
         state = %State{
           relations: relations,
           transaction: {lsn, %{commit_timestamp: commit_timestamp} = txn}
         }
       )
       when is_map(relations) do
    case Map.fetch(relations, relation_id) do
      {:ok, %{columns: columns, namespace: namespace, name: name}} when is_list(columns) ->
        tuple_data = old_tuple_data || changed_key_tuple_data
        data = data_tuple_to_map(columns, tuple_data)

        deleted_record = %Changes.DeletedRecord{
          type: "DELETE",
          schema: namespace,
          table: name,
          columns: columns,
          old_record: data,
          commit_timestamp: commit_timestamp,
          lsn: lsn
        }

        {:ok, state} = add_change(state, lsn, txn, deleted_record)
        check_columns(state, relation_id, columns, [tuple_data])

      _ ->
        unknown_relation(state, relation_id)
    end
  end

  defp process_message(
         %{message: %Messages.Truncate{truncated_relations: truncated_relations}},
         state = %State{
           relations: relations,
           transaction: {lsn, %{commit_timestamp: commit_timestamp, changes: changes} = txn}
         }
       )
       when is_list(truncated_relations) and is_list(changes) and is_map(relations) do
    {new_changes, unknown_ids} =
      truncate_changes(truncated_relations, relations, lsn, commit_timestamp, changes)

    state = %State{state | transaction: {lsn, %{txn | changes: new_changes}}}

    case unknown_ids do
      [] -> {:ok, state}
      [relation_id | _] -> unknown_relation(state, relation_id)
    end
  end

  # Postgres sends BEGIN with the LSN its COMMIT will carry, so a mismatch (or
  # a COMMIT with no BEGIN) means the stream is out of step. It is reported as
  # an error so that, with ack_after_processing, the caller does not
  # acknowledge a transaction that was never handed to the handlers.
  defp process_message(%{message: %Messages.Commit{lsn: commit_lsn}}, state = %State{}) do
    open_lsn = open_transaction_lsn(state)

    Logger.warning(
      "Dropping COMMIT at #{Event.lsn_to_string(commit_lsn)}: #{mismatch_reason(open_lsn)}"
    )

    {:error, {:commit_mismatch, %{commit_lsn: commit_lsn, open_lsn: open_lsn}}, state}
  end

  defp process_message(%{message: _message}, state), do: {:ok, state}

  defp open_transaction_lsn(%State{transaction: {lsn, _txn}}), do: lsn
  defp open_transaction_lsn(%State{transaction: nil}), do: nil

  defp mismatch_reason(nil), do: "no transaction is open"
  defp mismatch_reason(lsn), do: "the open transaction is at #{Event.lsn_to_string(lsn)}"

  # Changes are prepended, so the head holds the highest index so far. Reading it
  # avoids a length/1 walk per change and keeps the index in commit order even
  # though the list is handed to the handlers newest-first.
  defp next_index([]), do: 0
  defp next_index([%{change_index: last} | _]), do: last + 1

  # The one place that numbers INSERT/UPDATE/DELETE changes, so a new change
  # type cannot forget the index.
  defp add_change(state = %State{}, lsn, txn, change) do
    change = %{change | change_index: next_index(txn.changes)}
    {:ok, %State{state | transaction: {lsn, %{txn | changes: [change | txn.changes]}}}}
  end

  # Prepends one TRUNCATE change per known relation, and returns the ids of
  # those not in the relation table, in message order. Without
  # ack_after_processing the known ones are kept; with it, the synchronous
  # path discards the transaction.
  defp truncate_changes(relation_ids, relations, lsn, commit_timestamp, changes) do
    {new_changes, unknown_ids} =
      Enum.reduce(relation_ids, {changes, []}, fn relation_id, {acc, unknown_ids} ->
        case Map.fetch(relations, relation_id) do
          {:ok, %{namespace: namespace, name: name}} ->
            truncated = %Changes.TruncatedRelation{
              type: "TRUNCATE",
              schema: namespace,
              table: name,
              commit_timestamp: commit_timestamp,
              lsn: lsn,
              change_index: next_index(acc)
            }

            {[truncated | acc], unknown_ids}

          _ ->
            {acc, [relation_id | unknown_ids]}
        end
      end)

    {new_changes, Enum.reverse(unknown_ids)}
  end

  # Applying the rest of the transaction without this change would produce an
  # incomplete one, so with ack_after_processing the synchronous caller is told.
  # The state is returned without the unknown change: without it, the change is
  # skipped as before.
  defp unknown_relation(state, relation_id) do
    Logger.warning("Change for unknown relation #{relation_id}; incomplete transaction")

    {:error, {:unknown_relation, relation_id}, state}
  end

  # A tuple whose length differs from the relation's column count builds a
  # record with values missing or silently dropped. The change is kept, so
  # without ack_after_processing it is delivered as it always was; with it, the
  # synchronous path discards the transaction rather than acknowledge it. A
  # missing tuple (an UPDATE without its old row) is not a mismatch.
  defp check_columns(state, relation_id, columns, tuples) do
    if Enum.all?(tuples, &columns_match?(columns, &1)) do
      {:ok, state}
    else
      Logger.warning("Change for relation #{relation_id} does not match its columns")

      {:error, {:column_mismatch, relation_id}, state}
    end
  end

  defp columns_match?(columns, tuple_data) when is_tuple(tuple_data),
    do: length(columns) == tuple_size(tuple_data)

  defp columns_match?(_columns, _no_tuple), do: true

  defp data_tuple_to_map(columns, tuple_data) when is_list(columns) and is_tuple(tuple_data) do
    columns
    |> Enum.with_index()
    |> Enum.reduce_while(%{}, fn {column_map, index}, acc ->
      case column_map do
        %Messages.Relation.Column{name: column_name, type: column_type}
        when is_binary(column_name) and is_binary(column_type) ->
          validate_tuple_and_handle_response(tuple_data, index, acc, column_name, column_type)

        _ ->
          {:cont, acc}
      end
    end)
    # Bounded by the columns of the database schema (public API: atom keys).
    |> Map.new(fn {k, v} -> {String.to_atom(k), v} end)
  end

  defp data_tuple_to_map(_columns, _tuple_data), do: %{}

  defp validate_tuple_and_handle_response(tuple_data, index, acc, column_name, column_type) do
    case validate_tuple(tuple_data, index) do
      {:ok, record} ->
        {:cont, Map.put(acc, column_name, Types.cast_record(record, column_type))}

      :error ->
        {:halt, acc}
    end
  end

  defp validate_tuple(tuple_data, index) do
    {:ok, Kernel.elem(tuple_data, index)}
  rescue
    ArgumentError -> :error
  end
end
