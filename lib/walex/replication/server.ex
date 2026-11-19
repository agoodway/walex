# This file steals liberally from https://github.com/chasers/postgrex_replication_demo/blob/main/lib/replication.ex
# which in turn draws on https://hexdocs.pm/postgrex/Postgrex.ReplicationConnection.html#module-logical-replication

defmodule WalEx.Replication.Server do
  @moduledoc """
  Owns the Postgres logical-replication connection: it starts the slot and
  streams decoded WAL messages to the publisher and event handlers.
  """
  use Postgrex.ReplicationConnection

  alias Postgrex.{Error, ReplicationConnection, Result}

  alias WalEx.{
    Config,
    Decoder,
    Decoder.Messages,
    Events.EventModules,
    Helpers,
    Replication.QueryBuilder
  }

  alias WalEx.Config.Registry, as: WalExRegistry

  require Logger

  # Minimum gap between status updates sent right after handled commits, so a
  # busy stream does not send one reply per transaction.
  @commit_ack_throttle_ms 1_000

  # Reconnect backoff after consecutive processing failures: the base doubles
  # per failure, up to the max. Postgrex reconnects immediately on its own.
  @failure_backoff_base_ms 1_000
  @failure_backoff_max_ms 60_000
  # 2^16 times any base is past the max, so a longer streak cannot grow the
  # intermediate number.
  @failure_backoff_max_exponent 16

  # How often a slot held by another consumer is checked again.
  @slot_check_interval_ms 5_000

  # Per-connection state: set at init and again on every connect and disconnect.
  # The failure streak is deliberately not here, it outlives a connection.
  @stream_defaults %{
    streaming?: false,
    last_wal_end: 0,
    in_txn: false,
    txn_lsn: nil,
    standby_timer: nil,
    standby_token: nil,
    resume_timer: nil,
    resume_token: nil,
    slot_check_timer: nil,
    slot_check_token: nil,
    slot_confirmed_lsn: nil,
    confirmed_lsn: nil,
    last_reported_lsn: nil,
    last_status_at: nil,
    slot_checks: 0
  }

  def start_link(opts) do
    app_name = Keyword.get(opts, :app_name)
    opts = set_pgx_replication_conn_opts(app_name)

    ReplicationConnection.start_link(__MODULE__, [app_name: app_name], opts)
  end

  @impl true
  def init(opts) do
    app_name = Keyword.get(opts, :app_name)

    # The config agent outlives this process, so a config that was valid at
    # startup may have been changed since. Refuse to replicate rather than
    # acknowledge transactions whose handlers may not have run.
    case app_name |> Config.get_configs() |> Config.validate_config() do
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

    state =
      Map.merge(@stream_defaults, %{
        step: :disconnected,
        app_name: app_name,
        slot_name: slot_name,
        publication: publication,
        durable_slot: durable_slot,
        message_middleware: message_middleware,
        ack_after_processing: ack_after_processing,
        standby_status_interval: standby_status_interval,
        consecutive_failures: 0,
        failure_lsn: nil,
        # Only tests change the base, to keep the backoff short.
        failure_backoff_base_ms:
          :walex |> Application.get_env(:failure_backoff_base_ms) |> failure_backoff_base_ms()
      })

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

    emit(state, :connected, %{}, info)
    EventModules.connected(state.app_name, info)

    state
    |> reset_stream()
    |> restore_failure_streak()
    |> start_connection_sequence()
  end

  defp restore_failure_streak(state = %{app_name: app_name}) do
    %{
      state
      | consecutive_failures: Config.failure_count(app_name),
        failure_lsn: Config.failure_lsn(app_name)
    }
  end

  # After a processing failure the redelivered transaction will most likely
  # fail again, so the first query waits out a backoff instead of retrying at
  # once. The connection stays open and idle meanwhile. The deadline is kept in
  # the config agent: if the idle connection is dropped during the wait (for
  # instance by idle_session_timeout), the reconnect only waits out the rest,
  # instead of a full delay that such a drop could then keep restarting.
  defp start_connection_sequence(state = %{consecutive_failures: failures}) when failures > 0 do
    now = System.monotonic_time(:millisecond)
    candidate = now + failure_backoff(failures, state.failure_backoff_base_ms)
    delay = max(Config.resume_deadline(state.app_name, candidate) - now, 0)

    resume_after(state, delay, failures)
  end

  defp start_connection_sequence(state), do: query_publication(state)

  # The wait is already over (a reconnect after the deadline): no timer needed.
  defp resume_after(state, 0, _failures), do: query_publication(state)

  defp resume_after(state, delay, failures) do
    Logger.warning(
      "Resuming replication in #{delay}ms, after #{failures} consecutive processing failure(s)"
    )

    {timer, token} = arm_timer(:resume_connect, delay)
    {:noreply, %{state | resume_timer: timer, resume_token: token}}
  end

  defp query_publication(state) do
    query = QueryBuilder.publication_exists(state)
    {:query, query, %{state | step: :publication_exists, resume_timer: nil, resume_token: nil}}
  end

  @doc false
  # Milliseconds to wait before resuming after `failures` consecutive failures.
  @spec failure_backoff(pos_integer(), pos_integer()) :: pos_integer()
  def failure_backoff(failures, base_ms) do
    exponent = min(failures - 1, @failure_backoff_max_exponent)
    min(base_ms * Integer.pow(2, exponent), @failure_backoff_max_ms)
  end

  # A zero base would remove the backoff, and anything but a positive integer
  # would crash Process.send_after/3 on every connect.
  defp failure_backoff_base_ms(nil), do: @failure_backoff_base_ms
  defp failure_backoff_base_ms(base_ms) when is_integer(base_ms) and base_ms > 0, do: base_ms

  defp failure_backoff_base_ms(other) do
    Logger.warning(
      "Ignoring :failure_backoff_base_ms #{inspect(other)}, it must be a positive integer; " <>
        "using #{@failure_backoff_base_ms}"
    )

    @failure_backoff_base_ms
  end

  @impl true
  def handle_disconnect(state) do
    emit(state, :disconnected)

    {:noreply, %{reset_stream(state) | step: :disconnected}}
  end

  @impl true
  def handle_result([%Result{num_rows: 1}], state = %{step: :publication_exists}) do
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
      [%Result{num_rows: 0}] ->
        raise "Publication doesn't exist. publication: #{inspect(state.publication)}"

      _ ->
        raise "Unexpected result when checking if publication exists. #{inspect(results)}"
    end
  end

  @impl true
  def handle_result([%Result{columns: columns, rows: rows}], state = %{step: :slot_exists})
      when is_list(columns) do
    rows |> slot_row(columns) |> handle_slot(state)
  end

  # wal_status, which the ack_after_processing slot query selects, exists from
  # PostgreSQL 13.
  @impl true
  def handle_result(%Error{postgres: %{code: :undefined_column}}, %{step: :slot_exists}) do
    raise "ack_after_processing requires PostgreSQL 13+: pg_replication_slots has no wal_status column"
  end

  @impl true
  def handle_result(results, %{step: :slot_exists}) do
    raise "Failed to check if durable slot already exists. #{inspect(results)}"
  end

  @impl true
  def handle_result(results = [%Result{} | _results], state = %{step: :create_slot}) do
    state = %{state | slot_confirmed_lsn: consistent_point(results)}
    start_replication(state)
  end

  @impl true
  def handle_result(error = %Error{}, %{step: :create_slot}) do
    # if durable slot, can happen if multiple instances try to create the same slot
    raise "Failed to create replication slot, #{inspect(error)}"
  end

  @impl true
  def handle_data(<<?w, _wal_start::64, _wal_end::64, _clock::64, rest::binary>>, state) do
    state = mark_streaming(state)

    case decode(state, rest) do
      {:ok, message} ->
        result = state.message_middleware.(message, state.app_name)
        handle_processed(state, message, result)

      {:error, reason} ->
        disconnect_failed(state, nil, reason)
    end
  end

  @impl true
  def handle_data(<<?k, wal_end::64, _clock::64, reply_requested>>, state) do
    state
    |> mark_streaming()
    |> Map.update!(:last_wal_end, &max(&1, wal_end))
    |> keepalive_reply(reply_requested)
  end

  # A frame that cannot be decoded is a failed message like any other when its
  # position is acknowledged: back off and have Postgres redeliver it. Without
  # ack_after_processing it crashes the server, as it always did.
  defp decode(%{ack_after_processing: true}, frame) do
    {:ok, Decoder.decode_message(frame)}
  catch
    kind, reason ->
      failure =
        Helpers.log_caught_failure(
          "Failed to decode a replication message, it will be redelivered:",
          kind,
          reason,
          __STACKTRACE__
        )

      {:error, {:decode_failed, failure}}
  end

  defp decode(_state, frame), do: {:ok, Decoder.decode_message(frame)}

  defp keepalive_reply(state, 1), do: state |> status_update(:requested) |> noreply()
  defp keepalive_reply(state, 0), do: {:noreply, [], state}

  defp noreply({messages, state}), do: {:noreply, messages, state}

  # Periodic standby status update. Postgres only asks for one when
  # `wal_sender_timeout` is greater than 0 (with 0 it never asks), so without
  # this the slot's confirmed_flush_lsn can stay pinned on a quiet publication
  # while the database retains WAL. Enabled with `standby_status_interval`.
  # Each timer message carries the token it was armed with. Process.cancel_timer/1
  # cannot recall a message that has already been delivered, so a message from a
  # previous connection's timer may still arrive; without the token it would be
  # mistaken for the current one and start a second timer chain.
  @impl true
  def handle_info({:standby_status, token}, state = %{standby_token: token, streaming?: true})
      when is_reference(token) do
    %{state | standby_timer: nil, standby_token: nil}
    |> schedule_standby_status()
    |> status_update()
    |> noreply()
  end

  def handle_info({:standby_status, token}, state = %{standby_token: token})
      when is_reference(token),
      do: {:noreply, %{state | standby_timer: nil, standby_token: nil}}

  def handle_info({:standby_status, _stale_token}, state), do: {:noreply, state}

  # Same token scheme as the standby timer: a resume armed by an earlier
  # connection must not start the sequence on this one.
  def handle_info({:resume_connect, token}, state = %{resume_token: token})
      when is_reference(token),
      do: query_publication(state)

  def handle_info({:resume_connect, _stale_token}, state), do: {:noreply, state}

  # Same token scheme again. A slot check armed by an earlier connection (or a
  # duplicate chain) must not query on this one: it would hit a stream already
  # in progress, or run while the connection is down.
  def handle_info({:check_slot_status, token}, state = %{slot_check_token: token})
      when is_reference(token) do
    state = clear_slot_check(%{state | step: :slot_exists})
    {:query, QueryBuilder.slot_exists(state), state}
  end

  def handle_info({:check_slot_status, _stale_token}, state), do: {:noreply, state}

  def handle_info(message, state) do
    Logger.debug("Ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
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

  # A slot held by another consumer makes START_REPLICATION fail; Postgrex then
  # reconnects, which runs the slot check again (see use_slot/2), so there is no
  # result to handle here.
  defp start_replication(state) do
    query = QueryBuilder.start_replication_slot(state)
    {:stream, query, [], %{state | step: :start_replication}}
  end

  # A failed message must not be tracked, or its commit could be acknowledged.
  # Disconnecting makes Postgres redeliver from the last confirmed position
  # after the automatic reconnect, which handle_connect/1 delays by a backoff.
  # Postgrex logs the disconnect reason, so it is not logged here as well.
  # Without ack_after_processing the result is ignored, as it always was.
  defp handle_processed(state = %{ack_after_processing: true}, message, {:error, reason}),
    do: disconnect_failed(state, message, reason)

  defp handle_processed(state = %{ack_after_processing: true}, message, :ok),
    do: track_and_ack(state, message)

  # Anything but :ok cannot be taken as success when the position is acknowledged.
  defp handle_processed(state = %{ack_after_processing: true}, message, other),
    do: disconnect_failed(state, message, {:unexpected_middleware_result, other})

  defp handle_processed(state, message, _result), do: track_and_ack(state, message)

  # {:disconnect, reason} carries no state, so whatever this callback built (a
  # timer armed by mark_streaming/1, say) is dropped, and handle_disconnect/1
  # sees the state from before it. That is why the failure streak lives in the
  # config agent.
  defp disconnect_failed(state, message, reason) do
    failures = Config.record_failure(state.app_name, failing_lsn(state, message))
    emit_handler_failed(state, reason, failures)
    {:disconnect, reason}
  end

  # The commit LSN of the transaction that failed: the failing message's own
  # when it carries one (BEGIN announces it as final_lsn), otherwise that of
  # the open transaction. nil when no transaction is known.
  defp failing_lsn(_state, %Messages.Begin{final_lsn: lsn}), do: Decoder.lsn_to_integer(lsn)
  defp failing_lsn(_state, %Messages.Commit{lsn: lsn}), do: Decoder.lsn_to_integer(lsn)
  defp failing_lsn(state, _message), do: state.txn_lsn

  defp emit_handler_failed(state, reason = {:handler_failed, _details}, failures) do
    emit(state, :handler_failed, %{consecutive_failures: failures}, %{reason: reason})
  end

  defp emit_handler_failed(_state, _reason, _failures), do: :ok

  defp track_and_ack(state, message) do
    state
    |> reset_failures(message)
    |> track_transaction(message)
    |> commit_ack(message)
  end

  # Postgres redelivers from the last confirmed position, which can be well
  # behind the failing transaction (commits inside the ack throttle window are
  # not confirmed yet), so handling those again is not progress. The streak
  # ends once the failing transaction itself, or a later one, is handled. The
  # streak was read at connect, so the agent is only written when one ends.
  defp reset_failures(state = %{consecutive_failures: failures}, %Messages.Commit{lsn: lsn})
       when failures > 0 do
    if streak_ended?(state.failure_lsn, lsn), do: clear_failures(state), else: state
  end

  defp reset_failures(state, _message), do: state

  # An unknown failing position (nil) is cleared by any handled commit. It has
  # its own clause because in term order `integer >= nil` is always false.
  defp streak_ended?(nil, _commit_lsn), do: true

  defp streak_ended?(failure_lsn, commit_lsn),
    do: Decoder.lsn_to_integer(commit_lsn) >= failure_lsn

  defp clear_failures(state) do
    :ok = Config.reset_failures(state.app_name)
    %{state | consecutive_failures: 0, failure_lsn: nil}
  end

  # A handled commit is acknowledged right away, at most once per
  # @commit_ack_throttle_ms; the periodic and requested updates cover the rest.
  defp commit_ack(state = %{ack_after_processing: true}, %Messages.Commit{}) do
    if commit_ack_due?(state), do: state |> status_update() |> noreply(), else: {:noreply, state}
  end

  defp commit_ack(state, _message), do: {:noreply, state}

  defp commit_ack_due?(%{last_status_at: nil}), do: true

  defp commit_ack_due?(%{last_status_at: at}),
    do: System.monotonic_time(:millisecond) - at >= @commit_ack_throttle_ms

  # The status update to send, as `{replies, state}` with replies a (possibly
  # empty) list. With no position safe to report, nothing is sent, except in
  # reply to a keepalive that requested one (`:requested`): Postgres
  # disconnects a client it has not heard from within wal_sender_timeout, so
  # that is always answered, with LSN 0 (InvalidXLogRecPtr), which keeps the
  # connection alive without moving the slot.
  defp status_update(state, trigger \\ :unprompted),
    do: state |> ack_position() |> status_for(state, trigger)

  defp status_for(nil, state, :unprompted), do: {[], state}
  defp status_for(nil, state, :requested), do: {[status_message(0)], mark_status_sent(state)}
  defp status_for(lsn, state, _trigger), do: report_position(state, lsn)

  # A reported position is a promise to the server, so it never goes back within
  # a connection, even when a later candidate is lower (for instance a
  # keepalive position reported while idle, then a transaction opens and the
  # last confirmed commit is behind it). The slot_confirmed_lsn floor covers
  # reconnects, so the memory is dropped in reset_stream/1.
  defp report_position(state, lsn) do
    lsn = max(lsn, state.last_reported_lsn || 0)
    {[status_message(lsn)], mark_status_sent(%{state | last_reported_lsn: lsn})}
  end

  # Every status update goes through here, so the commit throttle measures time
  # since the last one sent by any path.
  defp mark_status_sent(state), do: %{state | last_status_at: System.monotonic_time(:millisecond)}

  defp status_message(lsn), do: <<?r, lsn::64, lsn::64, lsn::64, current_time()::64, 0>>

  # The LSN we can safely report as flushed, or nil if there is nothing to say.
  #
  # Without ack_after_processing, receipt is what is acknowledged: the higher of
  # the server's reported position (wal_end, which already points at the start
  # of the next record) and the end of the last received COMMIT, floored at the
  # slot's own confirmed_flush_lsn so we never report backwards. The COMMIT
  # counts because a busy stream sends few keepalives, which would otherwise
  # pin the slot at the last one.
  #
  # With ack_after_processing, only positions whose transactions have been fully
  # handed to the event handlers are acked, and the highest such position is
  # reported. Dispatch is synchronous in that mode, so once the middleware
  # returns for a COMMIT, that transaction is done, and track_transaction/2
  # records it as confirmed_lsn. The candidates are:
  #
  #   * confirmed_lsn, the last handled commit on this connection.
  #   * slot_confirmed_lsn, the slot's own confirmed_flush_lsn read at connect.
  #     Postgres already holds that position, so reporting it is always safe.
  #   * The server's position from the last keepalive, exactly, but only when no
  #     transaction is open. A keepalive follows all data it covers, so that
  #     data has been processed. That is what lets a quiet publication advance
  #     the slot instead of staying pinned at the last commit.
  #
  # Only keepalives update last_wal_end, and Postgres sends few of them while
  # it is busy streaming, so confirmed_lsn can be well ahead of it. Taking the
  # maximum keeps handled commits moving the slot forward either way.
  defp ack_position(state = %{ack_after_processing: true}) do
    [state.confirmed_lsn, state.slot_confirmed_lsn, idle_position(state)]
    |> Enum.reject(&is_nil/1)
    |> Enum.max(fn -> nil end)
  end

  defp ack_position(state),
    do: received_position(max(state.confirmed_lsn || 0, state.last_wal_end), state)

  defp received_position(0, _state), do: nil
  defp received_position(lsn, %{slot_confirmed_lsn: slot_lsn}), do: max(lsn, slot_lsn || 0)

  # The server's keepalive position, usable only while no transaction is open.
  defp idle_position(%{in_txn: false, last_wal_end: wal_end}) when wal_end > 0, do: wal_end
  defp idle_position(_state), do: nil

  # Postgrex calls handle_result/2 for queries only, never after a successful
  # START_REPLICATION, so the first replication message is the signal that the
  # stream is up. Status updates may only be sent from then on.
  defp mark_streaming(state = %{streaming?: true}), do: state

  defp mark_streaming(state) do
    emit(state, :streaming)
    record_streamed(state)

    schedule_standby_status(%{state | streaming?: true})
  end

  # Every replication event carries the system time and identifies the app and
  # slot; `measurements` and `metadata` add to that.
  defp emit(state, event, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute(
      [:walex, :replication, event],
      Map.put(measurements, :system_time, System.system_time()),
      Map.merge(%{app_name: state.app_name, slot_name: state.slot_name}, metadata)
    )
  end

  # From the first streamed message on, a missing slot means it was lost, not
  # that this is a first start (see handle_slot/2).
  defp record_streamed(%{ack_after_processing: true, app_name: app_name, slot_name: slot_name}),
    do: Config.record_slot_streamed(app_name, slot_name)

  defp record_streamed(_state), do: :ok

  defp reset_stream(state) do
    Enum.each(
      [state.standby_timer, state.resume_timer, state.slot_check_timer],
      &cancel_timer/1
    )

    Map.merge(state, @stream_defaults)
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer)

  # The token travels in the message so a handler can tell a current timer from
  # a stale one (see handle_info/2).
  defp arm_timer(tag, delay) do
    token = make_ref()
    {Process.send_after(self(), {tag, token}, delay), token}
  end

  # Text LSN ("16/B374D848") as returned by Postgres, to the 64-bit wire form.
  # Returns nil for NULL, malformed input, or 0/0.
  defp parse_lsn(lsn) when is_binary(lsn) do
    case ReplicationConnection.decode_lsn(lsn) do
      {:ok, value} when value > 0 -> value
      _ -> nil
    end
  end

  defp parse_lsn(_lsn), do: nil

  # The slot query's row as a map by column name, or nil when there is no slot.
  # Only the ack_after_processing query selects wal_status.
  defp slot_row([], _columns), do: nil
  defp slot_row([row], columns), do: columns |> Enum.zip(row) |> Map.new()
  defp slot_row(rows, _columns), do: {:unexpected_rows, rows}

  # With ack_after_processing a slot this instance has streamed from cannot
  # simply be created again: the new slot starts at the current position, so
  # every change committed since the last acknowledgement would be skipped.
  # Only this process's lifetime is covered (the marker lives in the config
  # agent), so a cold start cannot tell a lost slot from a first deploy.
  defp handle_slot(nil, state = %{ack_after_processing: true}) do
    if Config.slot_streamed?(state.app_name, state.slot_name),
      do: slot_unavailable(state, :missing),
      else: create_durable_slot(state)
  end

  defp handle_slot(nil, state), do: create_durable_slot(state)

  # An invalidated slot can no longer stream (START_REPLICATION fails, which
  # would otherwise be an immediate reconnect, over and over).
  defp handle_slot(%{"wal_status" => "lost"}, state = %{ack_after_processing: true}),
    do: slot_unavailable(state, :lost)

  defp handle_slot(%{"active" => active, "confirmed_flush_lsn" => confirmed}, state) do
    state = %{state | slot_confirmed_lsn: parse_lsn(confirmed), slot_checks: 0}
    use_slot(active, state)
  end

  defp handle_slot(slot, _state),
    do: raise("Failed to check if durable slot already exists. #{inspect(slot)}")

  defp use_slot("f", state) do
    Logger.info("Activating inactive replication slot: #{state.slot_name}")
    start_replication(state)
  end

  defp use_slot("t", state) do
    Logger.info(
      "Replication slot #{state.slot_name} is active. Waiting for it to become inactive."
    )

    {:noreply, schedule_slot_check(state, @slot_check_interval_ms)}
  end

  defp create_durable_slot(state),
    do: {:query, QueryBuilder.create_durable_slot(state), %{state | step: :create_slot}}

  # Nothing is created or streamed; the slot is checked again, less often each
  # time, so it resumes once an operator restores or recreates it. Not raising
  # keeps this from using up the supervisors' restarts.
  defp slot_unavailable(state, reason) do
    checks = state.slot_checks + 1
    delay = failure_backoff(checks, state.failure_backoff_base_ms)

    Logger.error(
      "#{slot_unavailable_message(reason, state.slot_name)} Checking again in #{delay}ms."
    )

    emit(state, :slot_missing, %{}, %{reason: reason})
    {:noreply, schedule_slot_check(%{state | slot_checks: checks}, delay)}
  end

  defp slot_unavailable_message(:missing, slot_name) do
    "Replication slot #{slot_name} is missing, though this instance has streamed from it " <>
      "(dropped, or lost in a failover, upgrade or provider cleanup). It is not recreated: " <>
      "a new slot would skip every change since the last acknowledgement. Restore or " <>
      "recreate it, or restart WalEx to accept the gap."
  end

  defp slot_unavailable_message(:lost, slot_name) do
    "Replication slot #{slot_name} has been invalidated (wal_status 'lost'): the changes " <>
      "it retained are gone. Drop and recreate it, accepting the gap."
  end

  # CREATE_REPLICATION_SLOT reports the new slot's confirmed_flush_lsn as
  # consistent_point.
  defp consistent_point([%Result{columns: columns, rows: [row | _]} | _])
       when is_list(columns) do
    case Enum.find_index(columns, &(&1 == "consistent_point")) do
      nil -> nil
      index -> row |> Enum.at(index) |> parse_lsn()
    end
  end

  defp consistent_point(_results), do: nil

  # txn_lsn is kept so that a failure on a change can be attributed to its
  # transaction (see failing_lsn/2).
  defp track_transaction(state, %Messages.Begin{final_lsn: final_lsn}),
    do: %{state | in_txn: true, txn_lsn: Decoder.lsn_to_integer(final_lsn)}

  # The commit's end LSN is where the next record starts, so confirming it never
  # covers a later transaction. Adding 1 would, since Postgres skips on restart
  # any transaction whose commit record starts before the confirmed position.
  # With ack_after_processing the commit was handled when this runs; otherwise
  # it was only received, which is all that mode acknowledges.
  defp track_transaction(
         state,
         %Messages.Commit{end_lsn: {_xlog_file, _xlog_offset} = end_lsn}
       ) do
    %{state | in_txn: false, txn_lsn: nil, confirmed_lsn: Decoder.lsn_to_integer(end_lsn)}
  end

  defp track_transaction(state, %Messages.Commit{}), do: %{state | in_txn: false, txn_lsn: nil}
  defp track_transaction(state, _message), do: state

  defp schedule_standby_status(state = %{standby_timer: nil, standby_status_interval: interval})
       when is_integer(interval) and interval > 0 do
    {timer, token} = arm_timer(:standby_status, interval)
    %{state | standby_timer: timer, standby_token: token}
  end

  defp schedule_standby_status(state), do: state

  defp schedule_slot_check(state, delay) do
    {timer, token} = arm_timer(:check_slot_status, delay)
    %{state | slot_check_timer: timer, slot_check_token: token}
  end

  defp clear_slot_check(state), do: %{state | slot_check_timer: nil, slot_check_token: nil}

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end
