# This file steals liberally from https://github.com/chasers/postgrex_replication_demo/blob/main/lib/replication.ex
# which in turn draws on https://hexdocs.pm/postgrex/Postgrex.ReplicationConnection.html#module-logical-replication

defmodule WalEx.Replication.Server do
  @moduledoc """
  This module is responsible for setting up the replication connection
  """
  use Postgrex.ReplicationConnection

  alias WalEx.Config
  alias WalEx.Config.Registry, as: WalExRegistry
  alias WalEx.Decoder
  alias WalEx.Decoder.Messages
  alias WalEx.Events.EventModules
  alias WalEx.Replication.QueryBuilder

  require Logger

  @max_retries 10
  @initial_backoff 1000

  def start_link(opts) do
    app_name = Keyword.get(opts, :app_name)
    opts = set_pgx_replication_conn_opts(app_name)

    Postgrex.ReplicationConnection.start_link(__MODULE__, [app_name: app_name], opts)
  end

  @impl true
  def init(opts) do
    app_name = Keyword.get(opts, :app_name)

    # The config agent outlives this process, so a config that was valid at
    # startup may have been changed since. Refuse to replicate rather than
    # acknowledge transactions whose handlers may not have run.
    case app_name |> Config.get_configs() |> Config.validate_ack_after_processing() do
      :ok ->
        :ok

      {:error, message} ->
        raise ArgumentError, "invalid WalEx config for #{inspect(app_name)}: #{message}"
    end

    [
      slot_name: slot_name,
      publication: publication,
      durable_slot: durable_slot,
      message_middleware: message_middleware,
      ack_after_processing: ack_after_processing,
      standby_status_interval: standby_status_interval
    ] =
      Config.get_configs(app_name, [
        :slot_name,
        :publication,
        :durable_slot,
        :message_middleware,
        :ack_after_processing,
        :standby_status_interval
      ])

    state = %{
      step: :disconnected,
      app_name: app_name,
      slot_name: slot_name,
      publication: publication,
      durable_slot: durable_slot,
      message_middleware: message_middleware,
      ack_after_processing: ack_after_processing,
      standby_status_interval: standby_status_interval,
      streaming?: false,
      last_wal_end: 0,
      in_txn: false,
      standby_timer: nil,
      standby_token: nil,
      slot_confirmed_lsn: nil,
      confirmed_lsn: nil
    }

    Process.flag(:message_queue_data, :off_heap)

    {:ok, state}
  end

  @impl true
  def handle_connect(state) do
    info = %{
      app_name: state.app_name,
      slot_name: state.slot_name,
      reconnect?: Config.record_connection(state.app_name) > 0
    }

    :telemetry.execute([:walex, :replication, :connected], measurements(), info)
    EventModules.connected(state.app_name, info)

    query = QueryBuilder.publication_exists(state)

    {:query, query, %{reset_stream(state) | step: :publication_exists}}
  end

  @impl true
  def handle_disconnect(state) do
    :telemetry.execute([:walex, :replication, :disconnected], measurements(), %{
      app_name: state.app_name,
      slot_name: state.slot_name
    })

    {:noreply, %{reset_stream(state) | step: :disconnected}}
  end

  @impl true
  def handle_result([%Postgrex.Result{num_rows: 1}], state = %{step: :publication_exists}) do
    if state.durable_slot do
      query = QueryBuilder.slot_exists(state)
      {:query, query, %{state | step: :slot_exists}}
    else
      query = QueryBuilder.create_temporary_slot(state)
      {:query, query, %{state | step: :create_slot}}
    end
  end

  @impl true
  def handle_result(results, state = %{step: :publication_exists}) do
    case results do
      [%Postgrex.Result{num_rows: 0}] ->
        raise "Publication doesn't exist. publication: #{inspect(state.publication)}"

      _ ->
        raise "Unexpected result when checking if publication exists. #{inspect(results)}"
    end
  end

  @impl true
  def handle_result([%Postgrex.Result{num_rows: 0}], state = %{step: :slot_exists}) do
    query = QueryBuilder.create_durable_slot(state)
    {:query, query, %{state | step: :create_slot}}
  end

  @impl true
  def handle_result(
        [
          %Postgrex.Result{
            columns: ["active", "confirmed_flush_lsn"],
            rows: [[active, confirmed]]
          }
        ],
        state = %{step: :slot_exists}
      ) do
    state = %{state | slot_confirmed_lsn: parse_lsn(confirmed)}

    case active do
      "f" ->
        Logger.info("Activating inactive replication slot: #{state.slot_name}")
        start_replication_with_retry(state, 0, @initial_backoff)

      "t" ->
        Logger.info(
          "Replication slot #{state.slot_name} is active. Waiting for it to become inactive."
        )

        schedule_slot_check()

        {:noreply, state}
    end
  end

  @impl true
  def handle_result(results, %{step: :slot_exists}) do
    raise "Failed to check if durable slot already exists. #{inspect(results)}"
  end

  @impl true
  def handle_result(results = [%Postgrex.Result{} | _results], state = %{step: :create_slot}) do
    state = %{state | slot_confirmed_lsn: consistent_point(results)}
    start_replication_with_retry(state, 0, @initial_backoff)
  end

  @impl true
  def handle_result(error = %Postgrex.Error{}, %{step: :create_slot}) do
    # if durable slot, can happen if multiple instances try to create the same slot
    raise "Failed to create replication slot, #{inspect(error)}"
  end

  @impl true
  def handle_result(
        %Postgrex.Error{postgres: %{code: :object_in_use}},
        state = %{step: {:start_replication, retry_count, backoff}}
      ) do
    Logger.warning("Replication slot in use, retrying... (attempt #{retry_count + 1})")
    Process.sleep(backoff)
    start_replication_with_retry(state, retry_count + 1, backoff * 2)
  end

  @impl true
  def handle_result(_, state = %{step: {:start_replication, _retry_count, _backoff}}) do
    Logger.info("Successfully started replication slot: #{state.slot_name}")
    {:noreply, %{state | step: :streaming}}
  end

  @impl true
  def handle_data(<<?w, _wal_start::64, _wal_end::64, _clock::64, rest::binary>>, state) do
    state = mark_streaming(state)
    message = Decoder.decode_message(rest)
    state.message_middleware.(message, state.app_name)

    {:noreply, track_transaction(state, message)}
  end

  @impl true
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
    state = mark_streaming(state)
    state = %{state | last_wal_end: max(state.last_wal_end, wal_end)}

    messages =
      case reply do
        1 -> requested_status_update(state)
        0 -> []
      end

    {:noreply, messages, state}
  end

  # Periodic standby status update. Postgres only asks for one when
  # `wal_sender_timeout` is greater than 0 (with 0 it never asks), so without
  # this the slot's confirmed_flush_lsn can stay pinned on a quiet publication
  # while the database retains WAL. Enabled with `standby_status_interval`.
  # Each timer message carries the token it was armed with. Process.cancel_timer/1
  # cannot recall a message that has already been delivered, so a message from a
  # previous connection's timer may still arrive; without the token it would be
  # mistaken for the current one and start a second timer chain.
  @impl true
  def handle_info({:standby_status, token}, state = %{standby_token: token, streaming?: true}) do
    state = schedule_standby_status(%{state | standby_timer: nil, standby_token: nil})
    {:noreply, status_update(state), state}
  end

  def handle_info({:standby_status, token}, state = %{standby_token: token}),
    do: {:noreply, %{state | standby_timer: nil, standby_token: nil}}

  def handle_info({:standby_status, _stale_token}, state), do: {:noreply, state}

  @impl true
  def handle_info(:check_slot_status, state) do
    query = QueryBuilder.slot_exists(state)
    {:query, query, %{state | step: :slot_exists}}
  end

  defp set_pgx_replication_conn_opts(app_name) do
    database_configs_keys = [
      :hostname,
      :username,
      :password,
      :port,
      :database,
      :ssl,
      :ssl_opts,
      :socket_options
    ]

    extra_opts = [auto_reconnect: true]
    database_configs = Config.get_configs(app_name, database_configs_keys)

    replications_name = [
      name: WalExRegistry.set_name(:set_gen_server, __MODULE__, app_name)
    ]

    extra_opts ++ database_configs ++ replications_name
  end

  defp start_replication_with_retry(state, retry_count, backoff)
       when retry_count < @max_retries do
    query = QueryBuilder.start_replication_slot(state)
    {:stream, query, [], %{state | step: {:start_replication, retry_count, backoff}}}
  end

  defp start_replication_with_retry(state, _retry_count, _backoff) do
    Logger.warning(
      "Failed to start replication slot after maximum retries. Scheduling another check."
    )

    schedule_slot_check()

    {:noreply, state}
  end

  # The status update to send, as a (possibly empty) list of replies.
  defp status_update(state) do
    case ack_position(state) do
      nil -> []
      lsn -> [status_message(lsn)]
    end
  end

  # The reply to a keepalive that requested one. Postgres disconnects a client
  # it has not heard from within wal_sender_timeout, so always answer. When no
  # position is safe to report, LSN 0 (InvalidXLogRecPtr) keeps the connection
  # alive without moving the slot.
  defp requested_status_update(state) do
    [status_message(ack_position(state) || 0)]
  end

  defp status_message(lsn), do: <<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>

  # The LSN we can safely report as flushed, or nil if there is nothing to say.
  #
  # Without ack_after_processing, the server's reported position is acked, as
  # before.
  #
  # With ack_after_processing, only positions whose transactions have been fully
  # handed to the event handlers are acked. Dispatch is synchronous in that
  # mode, so once the middleware returns for a COMMIT, that transaction is done,
  # and track_transaction/2 records it as confirmed_lsn. When no transaction is
  # open, the server's position (from a keepalive, which follows all data it
  # covers) has been fully processed, so it is safe to advance to it. That is
  # what lets a quiet publication advance the slot instead of staying pinned at
  # the last commit. Inside a transaction, the last confirmed commit is
  # reported, or, if there is none yet on this connection, the slot's own
  # confirmed_flush_lsn read at connect: Postgres already holds that position,
  # so reporting it is always safe.
  defp ack_position(%{last_wal_end: 0}), do: nil

  defp ack_position(%{ack_after_processing: true, in_txn: false, last_wal_end: wal_end}),
    do: wal_end + 1

  defp ack_position(state = %{ack_after_processing: true}) do
    [state.confirmed_lsn, state.slot_confirmed_lsn]
    |> Enum.reject(&is_nil/1)
    |> Enum.max(fn -> nil end)
  end

  defp ack_position(%{last_wal_end: wal_end}), do: wal_end + 1

  # Postgrex calls handle_result/2 for queries only, never after a successful
  # START_REPLICATION, so the first replication message is the signal that the
  # stream is up. Status updates may only be sent from then on.
  defp mark_streaming(state = %{streaming?: true}), do: state

  defp mark_streaming(state) do
    :telemetry.execute([:walex, :replication, :streaming], measurements(), %{
      app_name: state.app_name,
      slot_name: state.slot_name
    })

    schedule_standby_status(%{state | streaming?: true})
  end

  defp measurements, do: %{system_time: System.system_time()}

  defp reset_stream(state) do
    if state.standby_timer, do: Process.cancel_timer(state.standby_timer)

    %{
      state
      | streaming?: false,
        last_wal_end: 0,
        in_txn: false,
        standby_timer: nil,
        standby_token: nil,
        slot_confirmed_lsn: nil,
        confirmed_lsn: nil
    }
  end

  # Text LSN ("16/B374D848") as returned by Postgres, to the 64-bit wire form.
  # Returns nil for NULL, malformed input, or 0/0.
  defp parse_lsn(lsn) when is_binary(lsn) do
    with [high, low] <- String.split(lsn, "/"),
         {high, ""} <- Integer.parse(high, 16),
         {low, ""} <- Integer.parse(low, 16),
         value when value > 0 <- Bitwise.bsl(high, 32) + low do
      value
    else
      _ -> nil
    end
  end

  defp parse_lsn(_lsn), do: nil

  # CREATE_REPLICATION_SLOT reports the new slot's confirmed_flush_lsn as
  # consistent_point.
  defp consistent_point([%Postgrex.Result{columns: columns, rows: [row | _]} | _])
       when is_list(columns) do
    case Enum.find_index(columns, &(&1 == "consistent_point")) do
      nil -> nil
      index -> row |> Enum.at(index) |> parse_lsn()
    end
  end

  defp consistent_point(_results), do: nil

  defp track_transaction(state, %Messages.Begin{}), do: %{state | in_txn: true}

  # Commit LSN + 1, matching how the server's own position is reported: on
  # restart Postgres skips transactions whose commit record starts before the
  # confirmed position.
  defp track_transaction(
         state = %{ack_after_processing: true},
         %Messages.Commit{lsn: {xlog_file, xlog_offset}}
       ) do
    %{state | in_txn: false, confirmed_lsn: Bitwise.bsl(xlog_file, 32) + xlog_offset + 1}
  end

  defp track_transaction(state, %Messages.Commit{}), do: %{state | in_txn: false}
  defp track_transaction(state, _message), do: state

  defp schedule_standby_status(state = %{standby_timer: nil, standby_status_interval: interval})
       when is_integer(interval) and interval > 0 do
    token = make_ref()
    timer = Process.send_after(self(), {:standby_status, token}, interval)
    %{state | standby_timer: timer, standby_token: token}
  end

  defp schedule_standby_status(state), do: state

  defp schedule_slot_check do
    # Check again after 5 seconds
    Process.send_after(self(), :check_slot_status, 5000)
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end
