defmodule WalEx.Replication.ReplayTest do
  @moduledoc """
  End-to-end check of the `ack_after_processing` delivery guarantee against a
  real Postgres: a handler crash must not lose the transaction, and handled
  transactions must not be redelivered.

  Tagged `:replay` and excluded by default. It creates a **durable** slot, so
  point the `PG*` environment variables at a disposable database prepared with
  `mix walex.setup`, then run:

      mix test --only replay
  """
  use ExUnit.Case, async: false

  import WalEx.Support.TestHelpers

  @moduletag :replay
  @moduletag timeout: 120_000

  @app_name :replay_app
  @slot_name "walex_replay_slot"
  @table :walex_replay_test

  @db_configs [
    hostname: System.get_env("PGHOST", "localhost"),
    username: System.get_env("PGUSER", "postgres"),
    password: System.get_env("PGPASSWORD", "postgres"),
    database: "todos_test",
    port: String.to_integer(System.get_env("PGPORT", "5432"))
  ]

  @walex_configs @db_configs ++
                   [
                     name: @app_name,
                     publication: "events",
                     subscriptions: ["user"],
                     modules: [WalEx.Replication.ReplayTest.Handler],
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
    # processes restarting.
    on_insert(:user, %{}, [], fn events ->
      [{:test_pid, test_pid}] = :ets.lookup(:walex_replay_test, :test_pid)

      for %{new_record: %{email: email}, lsn: lsn} <- events do
        attempt = :ets.update_counter(:walex_replay_test, email, 1, {email, 0})
        send(test_pid, {:delivered, email, attempt, lsn})

        if attempt == 1 and String.contains?(email, "-fail-") do
          Process.sleep(1_500)
          raise "simulated handler failure for #{email}"
        end
      end
    end)
  end

  setup do
    :ets.new(@table, [:named_table, :public])
    :ets.insert(@table, {:test_pid, self()})

    {:ok, conn} = Postgrex.start_link(@db_configs)
    drop_slot(conn)

    on_exit(fn ->
      {:ok, cleanup} = Postgrex.start_link(@db_configs)
      drop_slot(cleanup)
      Postgrex.query!(cleanup, ~s[DELETE FROM "user" WHERE email LIKE 'replay-%'], [])
      GenServer.stop(cleanup)
    end)

    %{conn: conn}
  end

  test "a crashed transaction is redelivered and handled ones are not", %{conn: conn} do
    suffix = System.unique_integer([:positive])
    ok_email = "replay-ok-#{suffix}@example.com"
    fail_email = "replay-fail-#{suffix}@example.com"

    {:ok, _supervisor} = start_walex_supervised(@walex_configs)
    wait_until(fn -> slot_active?(conn) end, "the slot to become active")

    # A transaction whose handler succeeds is delivered once and acknowledged.
    insert_user(conn, ok_email)
    assert_receive {:delivered, ^ok_email, 1, ok_lsn}, 10_000

    wait_until(
      fn -> confirmed_past?(conn, ok_lsn) end,
      "confirmed_flush_lsn to pass the handled transaction"
    )

    # A transaction whose handler raises crashes the replication processes.
    # Nothing past the first transaction was acknowledged, so Postgres
    # redelivers it from the durable slot after WalEx reconnects.
    insert_user(conn, fail_email)
    assert_receive {:delivered, ^fail_email, 1, fail_lsn}, 10_000
    assert_receive {:delivered, ^fail_email, 2, ^fail_lsn}, 60_000

    wait_until(
      fn -> confirmed_past?(conn, fail_lsn) end,
      "confirmed_flush_lsn to pass the redelivered transaction"
    )

    # The already-handled transaction was not redelivered.
    refute_received {:delivered, ^ok_email, _, _}
    assert [{^ok_email, 1}] = :ets.lookup(@table, ok_email)
  end

  defp insert_user(conn, email) do
    Postgrex.query!(conn, ~s[INSERT INTO "user" (email, name) VALUES ($1, 'Replay')], [email])
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
        [@slot_name, Bitwise.bsl(elem(lsn, 0), 32) + elem(lsn, 1)]
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

  defp wait_until(fun, description, timeout \\ 30_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, description, deadline)
  end

  defp do_wait_until(fun, description, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("timed out waiting for #{description}")

      true ->
        Process.sleep(100)
        do_wait_until(fun, description, deadline)
    end
  end
end
