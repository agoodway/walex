defmodule WalEx.Replication.ReplayTest do
  @moduledoc """
  End-to-end check of the `ack_after_processing` delivery guarantee against a
  real Postgres: a failing handler or a crashed process must not lose the
  transaction, a redelivery carries the same `{lsn, change_index}` keys, and
  handled transactions must not be redelivered.

  Tagged `:replay` and excluded by default. It creates a **durable** slot, so
  point the `PG*` environment variables at a disposable database prepared with
  `mix walex.setup`, then run:

      mix test --only replay
  """
  use ExUnit.Case, async: false

  import WalEx.Support.TestHelpers

  alias WalEx.{Config.Registry, Decoder}
  alias WalEx.Events.EventModules
  alias WalEx.Replication.{Publisher, Server}

  @moduletag :replay
  @moduletag timeout: 120_000

  @app_name :replay_app
  # Unique per run, so a leftover slot from a crashed run (or a developer's own
  # slot) is never adopted or dropped.
  @slot_name "walex_test_replay_#{System.system_time(:millisecond)}"
  @table :walex_replay_test

  @db_configs pg_config() ++ [database: "todos_test"]

  @walex_configs @db_configs ++
                   [
                     name: @app_name,
                     publication: "events",
                     subscriptions: ["user"],
                     modules: [__MODULE__.Handler],
                     slot_name: @slot_name,
                     durable_slot: true,
                     ack_after_processing: true,
                     standby_status_interval: 100
                   ]

  defmodule Handler do
    @moduledoc false
    use WalEx.Event, name: :replay_app

    # Records every delivery. The first delivery of any row whose email
    # contains "-fail-" raises, but only after several standby status
    # intervals have passed: without ack_after_processing, WalEx would have
    # acknowledged the transaction by then and Postgres would not redeliver
    # it, so this is what makes the test fail if the guarantee breaks. The
    # ETS table belongs to the test process, so the counts survive WalEx's
    # processes restarting. A row whose email contains "-kill-" kills the
    # replication server on its first delivery, once, while the handler is
    # still running, so its acknowledgement can never be sent.
    on_insert(:user, %{}, [], fn events ->
      [{:test_pid, test_pid}] = :ets.lookup(:walex_replay_test, :test_pid)

      for %{new_record: %{email: email}, lsn: lsn, change_index: index} <- events do
        attempt = :ets.update_counter(:walex_replay_test, email, 1, {email, 0})
        send(test_pid, {:delivered, email, attempt, lsn, index})

        if String.contains?(email, "-kill-") and
             :ets.insert_new(:walex_replay_test, {:killed, true}) do
          :set_gen_server
          |> Registry.set_name(Server, :replay_app)
          |> GenServer.whereis()
          |> Process.exit(:kill)
        end

        if attempt == 1 and String.contains?(email, "-fail-") do
          Process.sleep(1_500)
          raise "simulated handler failure for #{email}"
        end

        if attempt <= 3 and String.contains?(email, "-flaky-") do
          raise "simulated handler failure #{attempt} for #{email}"
        end
      end
    end)
  end

  setup do
    :ets.new(@table, [:named_table, :public])
    :ets.insert(@table, {:test_pid, self()})

    # Outlives the test process (unlike the ETS table) so on_exit can read it.
    {:ok, inserted} = Agent.start(fn -> [] end)
    :ets.insert(@table, {:inserted, inserted})

    {:ok, conn} = Postgrex.start_link(@db_configs)
    drop_slot(conn)

    on_exit(fn ->
      ids = Agent.get(inserted, & &1)
      Agent.stop(inserted)

      {:ok, cleanup} = Postgrex.start_link(@db_configs)
      drop_slot(cleanup)
      Postgrex.query!(cleanup, ~s[DELETE FROM "user" WHERE id = ANY($1)], [ids])
      GenServer.stop(cleanup)
    end)

    %{conn: conn}
  end

  test "a failed transaction is redelivered and handled ones are not", %{conn: conn} do
    suffix = System.unique_integer([:positive])
    ok_email = "replay-ok-#{suffix}@example.com"
    fail_email = "replay-fail-#{suffix}@example.com"

    {:ok, _supervisor} = start_walex_supervised(@walex_configs)
    wait_until(fn -> slot_active?(conn) end, "the slot to become active")

    # A transaction whose handler succeeds is delivered once and acknowledged.
    insert_user(conn, ok_email)
    assert_receive {:delivered, ^ok_email, 1, ok_lsn, _index}, 10_000

    wait_until(
      fn -> confirmed_past?(conn, ok_lsn) end,
      "confirmed_flush_lsn to pass the handled transaction"
    )

    # A transaction whose handler raises is not acknowledged: WalEx
    # disconnects, and Postgres redelivers it from the durable slot after the
    # reconnect. Nothing crashes.
    insert_user(conn, fail_email)
    assert_receive {:delivered, ^fail_email, 1, fail_lsn, _index}, 10_000
    assert_receive {:delivered, ^fail_email, 2, ^fail_lsn, _index}, 60_000

    wait_until(
      fn -> confirmed_past?(conn, fail_lsn) end,
      "confirmed_flush_lsn to pass the redelivered transaction"
    )

    # The already-handled transaction was not redelivered.
    refute_received {:delivered, ^ok_email, _, _, _index}
    assert [{^ok_email, 1}] = :ets.lookup(@table, ok_email)
  end

  # The duplicate-delivery half of at-least-once: the handler finishes, but the
  # server that would acknowledge it is gone. Postgres redelivers the whole
  # transaction, and every change keeps its {lsn, change_index} dedupe key.
  test "a multi-row transaction whose Server is killed mid-handler is redelivered identically",
       %{conn: conn} do
    suffix = System.unique_integer([:positive])
    emails = for i <- 0..2, do: "replay-kill-#{suffix}-#{i}@example.com"

    {:ok, supervisor} = start_walex_supervised(@walex_configs)
    wait_until(fn -> slot_active?(conn) end, "the slot to become active")
    [_event_modules, _publisher, server] = pipeline_pids()

    {:ok, :ok} =
      Postgrex.transaction(conn, fn txn_conn -> Enum.each(emails, &insert_user(txn_conn, &1)) end)

    first = receive_deliveries(emails, 1, 10_000)
    second = receive_deliveries(emails, 2, 60_000)

    assert first == second
    assert [0, 1, 2] == first |> Enum.map(fn {_email, _lsn, index} -> index end) |> Enum.sort()
    assert [{_email, lsn, _index} | _] = first
    assert Enum.all?(first, fn {_email, other_lsn, _index} -> other_lsn == lsn end)

    wait_until(
      fn -> confirmed_past?(conn, lsn) end,
      "confirmed_flush_lsn to pass the redelivered transaction"
    )

    refute server == Enum.at(pipeline_pids(), 2)
    assert Process.alive?(supervisor)
  end

  # The `{email, lsn, change_index}` of each email's `attempt`-th delivery,
  # sorted by email.
  defp receive_deliveries(emails, attempt, timeout) do
    emails
    |> Enum.map(fn email ->
      assert_receive {:delivered, ^email, ^attempt, lsn, index}, timeout
      {email, lsn, index}
    end)
    |> Enum.sort()
  end

  # The keepalives and status ticks that arrive while a transaction is still
  # open report a position equal to that transaction's COMMIT record start.
  # Reporting it plus one would make Postgres skip the transaction on restart.
  test "a transaction held open across status ticks is redelivered after a failure", %{conn: conn} do
    email = "replay-fail-open-#{System.unique_integer([:positive])}@example.com"

    {:ok, _supervisor} = start_walex_supervised(@walex_configs)
    wait_until(fn -> slot_active?(conn) end, "the slot to become active")

    # Caveat: unrelated background WAL between the emitted message and COMMIT can
    # make this test non-discriminating (it passes on old code), never falsely fail.
    # A flushed non-transactional message right after the INSERT makes the
    # walsender read up to the end of the INSERT, so it reports that position,
    # which is exactly where this transaction's COMMIT record will start.
    other_conn = start_supervised!({Postgrex, @db_configs}, id: :replay_other_conn)

    {:ok, :ok} =
      Postgrex.transaction(conn, fn txn_conn ->
        insert_user(txn_conn, email)

        Postgrex.query!(
          other_conn,
          "SELECT pg_logical_emit_message(false, 'walex', 'x', true)",
          []
        )

        # Several 100ms status intervals pass before COMMIT.
        Process.sleep(1_000)
        :ok
      end)

    assert_receive {:delivered, ^email, 1, lsn, _index}, 10_000
    assert_receive {:delivered, ^email, 2, ^lsn, _index}, 60_000

    wait_until(
      fn -> confirmed_past?(conn, lsn) end,
      "confirmed_flush_lsn to pass the redelivered transaction"
    )
  end

  # Each failure disconnects and reconnects without crashing anything, so the
  # failures never reach a supervisor's restart limit.
  test "a handler that keeps failing is retried with backoff, then delivered", %{conn: conn} do
    email = "replay-flaky-#{System.unique_integer([:positive])}@example.com"

    # Keeps the backoff short; Server reads this when it starts.
    Application.put_env(:walex, :failure_backoff_base_ms, 50)
    on_exit(fn -> Application.delete_env(:walex, :failure_backoff_base_ms) end)

    test_pid = self()
    id = "replay-handler-exception-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      id,
      [[:walex, :event_handler, :exception], [:walex, :replication, :handler_failed]],
      fn
        [:walex, :event_handler, :exception], _measurements, metadata, _ ->
          send(test_pid, {:handler_exception, metadata.module})

        [:walex, :replication, :handler_failed], measurements, _metadata, _ ->
          send(test_pid, {:handler_failed, measurements.consecutive_failures})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    {:ok, supervisor} = start_walex_supervised(@walex_configs)
    supervisor_ref = Process.monitor(supervisor)
    wait_until(fn -> slot_active?(conn) end, "the slot to become active")
    pids = pipeline_pids()
    assert Enum.all?(pids, &is_pid/1)

    insert_user(conn, email)

    assert_receive {:delivered, ^email, 1, lsn, _index}, 10_000

    for attempt <- 2..4 do
      assert_receive {:delivered, ^email, ^attempt, ^lsn, _index}, 30_000
    end

    wait_until(
      fn -> confirmed_past?(conn, lsn) end,
      "confirmed_flush_lsn to pass the eventually handled transaction"
    )

    for _failure <- 1..3, do: assert_received({:handler_exception, Handler})
    refute_received {:handler_exception, _}

    # The streak is read back from the config agent at each reconnect, so it
    # escalates rather than starting over.
    for count <- 1..3, do: assert_received({:handler_failed, ^count})
    refute_received {:handler_failed, _}
    refute_received {:DOWN, ^supervisor_ref, _, _, _}
    assert Process.alive?(supervisor)
    assert pids == pipeline_pids()
    assert [{^email, 4}] = :ets.lookup(@table, email)
  end

  defp pipeline_pids do
    for module <- [EventModules, Publisher, Server] do
      :set_gen_server |> Registry.set_name(module, @app_name) |> GenServer.whereis()
    end
  end

  defp insert_user(conn, email) do
    %{rows: [[id]]} =
      Postgrex.query!(
        conn,
        ~s[INSERT INTO "user" (email, name) VALUES ($1, 'Replay') RETURNING id],
        [email]
      )

    # Recorded so cleanup deletes exactly the rows this test inserted.
    [{:inserted, inserted}] = :ets.lookup(@table, :inserted)
    Agent.update(inserted, &[id | &1])
  end

  defp slot_active?(conn) do
    match?(
      %{rows: [[true]]},
      Postgrex.query!(conn, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", [
        @slot_name
      ])
    )
  end

  # Postgres skips, on restart, any transaction whose commit record starts
  # before confirmed_flush_lsn, so "past the commit LSN" means acknowledged.
  defp confirmed_past?(conn, lsn) do
    match?(
      %{rows: [[true]]},
      Postgrex.query!(
        conn,
        "SELECT confirmed_flush_lsn > $2::pg_lsn FROM pg_replication_slots WHERE slot_name = $1",
        [@slot_name, Decoder.lsn_to_integer(lsn)]
      )
    )
  end

  defp drop_slot(conn, attempts \\ 50) do
    Postgrex.query!(
      conn,
      """
      SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
      WHERE slot_name = $1 AND active_pid IS NOT NULL
      """,
      [@slot_name]
    )

    %{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
        WHERE slot_name = $1 AND NOT active
        """,
        [@slot_name]
      )

    remaining =
      Postgrex.query!(conn, "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", [
        @slot_name
      ])

    if remaining.num_rows > 0 and attempts > 0 do
      Process.sleep(100)
      drop_slot(conn, attempts - 1)
    else
      rows
    end
  end
end
