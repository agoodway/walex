defmodule WalEx.Replication.AckAfterProcessingTest do
  use ExUnit.Case, async: false

  import WalEx.Support.TestHelpers,
    only: [
      ack_lsn: 1,
      attach_telemetry: 1,
      begin_msg: 1,
      commit_msg: 1,
      keepalive: 2,
      server_state: 2,
      start_config!: 2,
      streaming_state: 1,
      streaming_state: 2,
      tick: 1,
      xlog: 1
    ]

  import ExUnit.CaptureLog

  alias Postgrex.Result
  alias WalEx.{Config, Config.Registry, Decoder, Events, Events.EventModules}
  alias WalEx.Decoder.Messages
  alias WalEx.Replication.{Publisher, Server}

  defmodule OkHandler do
    def process_all(_txn), do: :ok
  end

  defmodule RaisingHandler do
    def process_all(_txn), do: raise("handler failed")
  end

  setup do
    start_supervised!(Registry.child_spec())
    :ok
  end

  defp app_name(suffix), do: String.to_atom("ack_after_processing_test_#{suffix}")

  defp pipeline_pid(module, app),
    do: :set_gen_server |> Registry.set_name(module, app) |> GenServer.whereis()

  describe "Server keepalive acknowledgements" do
    setup do
      app = app_name("keepalive_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      %{app: app}
    end

    test "with no transaction open, reports wal_end exactly", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(keepalive(1_000, 1), streaming_state(app))

      assert 1_000 == ack_lsn(messages)
    end

    test "with a transaction open, reports the last confirmed commit", %{app: app} do
      # wal_end is intentionally larger than the confirmed position; the ack
      # must be driven by the confirmed commit while a transaction is open.
      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 streaming_state(app, in_txn: true, confirmed_lsn: 2_001)
               )

      assert 2_001 == ack_lsn(messages)
    end

    test "does not send a status update when keepalive reply flag is 0", %{app: app} do
      assert {:noreply, [], _state} =
               Server.handle_data(keepalive(9_999, 0), streaming_state(app, confirmed_lsn: 124))
    end

    test "with ack_after_processing false, reports the larger of wal_end and the received commit" do
      app = app_name("server_disabled")
      start_config!(app, durable_slot: true, ack_after_processing: false)

      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(5_000, 1),
                 streaming_state(app, in_txn: true, confirmed_lsn: 42)
               )

      assert 5_000 == ack_lsn(messages)

      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(5_000, 1),
                 streaming_state(app, in_txn: true, confirmed_lsn: 7_000)
               )

      assert 7_000 == ack_lsn(messages)
    end
  end

  describe "liveness while a transaction is open and nothing is confirmed" do
    setup do
      app = app_name("liveness_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      %{app: app}
    end

    test "answers a reply request with LSN 0 when the slot position is unknown", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(keepalive(9_999, 1), streaming_state(app, in_txn: true))

      assert 0 == ack_lsn(messages)
    end

    test "answers a reply request with the slot's confirmed_flush_lsn when known", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 streaming_state(app, in_txn: true, slot_confirmed_lsn: 4_000)
               )

      assert 4_000 == ack_lsn(messages)
    end

    test "never reports below the slot's confirmed_flush_lsn", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 streaming_state(app, in_txn: true, confirmed_lsn: 101, slot_confirmed_lsn: 4_000)
               )

      assert 4_000 == ack_lsn(messages)
    end

    test "sends no unsolicited periodic update when nothing is safe to report", %{app: app} do
      assert {:noreply, [], _state} =
               tick(streaming_state(app, in_txn: true, last_wal_end: 9_999))
    end
  end

  describe "reading the slot position at connect" do
    defp slot_state(app, step) do
      start_config!(app, durable_slot: true, ack_after_processing: true)
      server_state(app, step: step)
    end

    defp slot_row(active, confirmed) do
      [
        %Result{
          columns: ["active", "confirmed_flush_lsn"],
          rows: [[active, confirmed]],
          num_rows: 1
        }
      ]
    end

    test "a server older than PostgreSQL 13 raises a version message" do
      app = app_name("slot_old_pg")
      error = %Postgrex.Error{postgres: %{code: :undefined_column, message: "no wal_status"}}

      assert_raise RuntimeError, ~r/ack_after_processing requires PostgreSQL 13\+/, fn ->
        Server.handle_result(error, slot_state(app, :slot_exists))
      end
    end

    test "an existing inactive slot records confirmed_flush_lsn and starts replication" do
      app = app_name("slot_inactive")

      assert {:stream, _query, [], %{slot_confirmed_lsn: lsn}} =
               Server.handle_result(slot_row("f", "16/B374D848"), slot_state(app, :slot_exists))

      assert lsn == 0x16 * 0x1_0000_0000 + 0xB374D848
    end

    test "an existing active slot records confirmed_flush_lsn while it waits" do
      app = app_name("slot_active")

      assert {:noreply, %{slot_confirmed_lsn: 0x2A}} =
               Server.handle_result(slot_row("t", "0/2A"), slot_state(app, :slot_exists))
    end

    test "a NULL or malformed confirmed_flush_lsn is treated as unknown" do
      app = app_name("slot_null")
      slot_exists = slot_state(app, :slot_exists)

      for value <- [nil, "garbage", "0/0"] do
        assert {:stream, _query, [], %{slot_confirmed_lsn: nil}} =
                 Server.handle_result(slot_row("f", value), slot_exists)
      end
    end

    test "a newly created slot records consistent_point" do
      app = app_name("slot_created")

      result = %Result{
        columns: ["slot_name", "consistent_point", "snapshot_name", "output_plugin"],
        rows: [["slot_created_walex", "1/10", nil, "pgoutput"]],
        num_rows: 1
      }

      assert {:stream, _query, [], %{slot_confirmed_lsn: lsn}} =
               Server.handle_result([result], slot_state(app, :create_slot))

      assert lsn == 0x1_0000_0010
    end
  end

  describe "a durable slot that is missing or invalidated" do
    # Recreating the slot after this instance has streamed from it would skip
    # every change committed since the last acknowledgement.
    setup do
      app = app_name("slot_missing_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      attach_telemetry([[:walex, :replication, :slot_missing]])
      %{app: app, state: server_state(app, step: :slot_exists, failure_backoff_base_ms: 10)}
    end

    defp capture_log_result(fun) do
      parent = self()
      capture_log(fn -> send(parent, {:result, fun.()}) end)
      assert_received {:result, result}
      result
    end

    defp no_slot,
      do: [
        %Result{columns: ["active", "confirmed_flush_lsn", "wal_status"], rows: [], num_rows: 0}
      ]

    defp slot_with_status(wal_status),
      do: [
        %Result{
          columns: ["active", "confirmed_flush_lsn", "wal_status"],
          rows: [["f", "0/2A", wal_status]],
          num_rows: 1
        }
      ]

    test "is created on the first connect", %{state: state} do
      assert {:query, _query, %{step: :create_slot}} = Server.handle_result(no_slot(), state)
      refute_received {:telemetry, _, _, _}
    end

    test "streaming records that this instance has used the slot", %{app: app} do
      {:noreply, [], _state} = Server.handle_data(keepalive(10, 0), server_state(app, []))
      assert Config.slot_streamed?(app, server_state(app, []).slot_name)
    end

    test "once streamed from, is not recreated and is checked again later",
         %{app: app, state: state} do
      Config.record_slot_streamed(app, state.slot_name)

      log =
        capture_log(fn ->
          assert {:noreply, %{step: :slot_exists}} = Server.handle_result(no_slot(), state)
        end)

      assert log =~ "missing"
      slot_name = state.slot_name

      assert_received {:telemetry, [:walex, :replication, :slot_missing], %{system_time: _},
                       %{app_name: ^app, slot_name: ^slot_name, reason: :missing}}

      assert_receive {:check_slot_status, _token}, 1_000
    end

    test "an invalidated slot is not streamed from, whether or not it was used", %{state: state} do
      capture_log(fn ->
        assert {:noreply, _state} = Server.handle_result(slot_with_status("lost"), state)
      end)

      assert_received {:telemetry, [:walex, :replication, :slot_missing], _, %{reason: :lost}}
      assert_receive {:check_slot_status, _token}, 1_000
    end

    test "a slot that is back after a check resumes streaming", %{app: app, state: state} do
      Config.record_slot_streamed(app, state.slot_name)
      capture_log(fn -> Server.handle_result(no_slot(), state) end)

      assert {:stream, _query, [], %{slot_confirmed_lsn: 0x2A}} =
               Server.handle_result(slot_with_status("reserved"), state)
    end

    # Process.cancel_timer/1 cannot recall a message already in the mailbox, so
    # every check is tokened and a reconnect makes earlier ones stale.
    test "a check armed before a reconnect is ignored afterwards", %{app: app, state: state} do
      Config.record_slot_streamed(app, state.slot_name)
      {:noreply, blocked} = capture_log_result(fn -> Server.handle_result(no_slot(), state) end)
      assert_receive {:check_slot_status, old_token}, 1_000
      assert_received {:telemetry, [:walex, :replication, :slot_missing], _, _}

      {:noreply, disconnected} = Server.handle_disconnect(blocked)
      assert {:query, _query, connected} = Server.handle_connect(disconnected)

      assert {:noreply, ^connected} =
               Server.handle_info({:check_slot_status, old_token}, connected)

      refute_received {:telemetry, [:walex, :replication, :slot_missing], _, _}
    end

    test "the current check queries the slot again", %{app: app, state: state} do
      Config.record_slot_streamed(app, state.slot_name)
      {:noreply, blocked} = capture_log_result(fn -> Server.handle_result(no_slot(), state) end)
      assert_receive {:check_slot_status, token}, 1_000

      assert {:query, _query, %{step: :slot_exists}} =
               Server.handle_info({:check_slot_status, token}, blocked)
    end

    test "a check that arrives after streaming started is ignored", %{app: app} do
      streaming = streaming_state(app, step: :streaming)

      assert {:noreply, ^streaming} =
               Server.handle_info({:check_slot_status, make_ref()}, streaming)
    end

    test "an unknown message is ignored", %{app: app} do
      state = streaming_state(app, [])

      log =
        capture_log([level: :debug], fn ->
          assert {:noreply, ^state} = Server.handle_info(:bogus, state)
        end)

      assert log =~ "bogus"
    end

    test "without ack_after_processing, a missing slot is still created" do
      app = app_name("slot_missing_default")
      start_config!(app, durable_slot: true)
      state = server_state(app, step: :slot_exists)
      Config.record_slot_streamed(app, state.slot_name)

      assert {:query, _query, %{step: :create_slot}} =
               Server.handle_result(
                 [%Result{columns: ["active", "confirmed_flush_lsn"], rows: [], num_rows: 0}],
                 state
               )
    end
  end

  describe "re-checking the config when Server starts" do
    # The Config agent outlives Server, so a restart must not trust that the
    # config is still the one validated at startup.
    setup do
      app = app_name("init_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      %{app: app}
    end

    defp config_agent(app), do: Registry.set_name(:set_agent, Config, app)

    test "starts with the validated config", %{app: app} do
      assert {:ok, %{ack_after_processing: true, durable_slot: true, message_middleware: mw}} =
               Server.init(app_name: app)

      assert mw == (&Publisher.process_message_sync/2)
    end

    test "still starts after a rejected replacement", %{app: app} do
      assert_raise ArgumentError, fn ->
        Config.replace_config(app, :message_middleware, &Publisher.process_message_async/2)
      end

      assert {:ok, %{message_middleware: mw}} = Server.init(app_name: app)
      assert mw == (&Publisher.process_message_sync/2)
    end

    test "refuses to start with an asynchronous middleware", %{app: app} do
      Agent.update(config_agent(app), fn config ->
        Keyword.put(config, :message_middleware, &Publisher.process_message_async/2)
      end)

      assert_raise ArgumentError, ~r/invalid WalEx config.*synchronous message_middleware/, fn ->
        Server.init(app_name: app)
      end
    end

    test "refuses to start with a temporary slot", %{app: app} do
      Agent.update(config_agent(app), &Keyword.put(&1, :durable_slot, false))

      assert_raise ArgumentError, ~r/invalid WalEx config.*requires durable_slot: true/, fn ->
        Server.init(app_name: app)
      end
    end

    test "is not checked when ack_after_processing is off" do
      app = app_name("init_off")
      start_config!(app, message_middleware: &Publisher.process_message_async/2)

      assert {:ok, %{ack_after_processing: false}} = Server.init(app_name: app)
    end
  end

  describe "confirming commits through the real pipeline" do
    defp start_pipeline!(app, handler, extra \\ []) do
      start_config!(app, [durable_slot: true, modules: [handler]] ++ extra)
      start_supervised!({EventModules, app_name: app})
      start_supervised!({Events, app_name: app})
      start_supervised!({Publisher, app_name: app})
    end

    test "a commit is confirmed only once its handlers have run" do
      app = app_name("pipeline_ok")
      start_pipeline!(app, OkHandler, ack_after_processing: true)
      lsn = 0x1_0000_0010

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))
      assert %{in_txn: true, confirmed_lsn: nil} = after_begin

      {:noreply, messages, after_commit} = Server.handle_data(xlog(commit_msg(lsn)), after_begin)
      assert %{in_txn: false, confirmed_lsn: confirmed} = after_commit
      assert confirmed == lsn + 8
      assert confirmed == ack_lsn(messages)
    end

    test "a commit whose handler raises disconnects without crashing the pipeline" do
      app = app_name("pipeline_raise")
      start_pipeline!(app, RaisingHandler, ack_after_processing: true)
      pids = Enum.map([EventModules, Events, Publisher], &pipeline_pid(&1, app))
      lsn = 0x2_0000_0020

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))

      # Postgrex keeps the pre-message state on {:disconnect, _}, so the commit
      # is never confirmed, and Postgres redelivers it after the reconnect.
      capture_log(fn ->
        assert {:disconnect, {:handler_failed, %{module: RaisingHandler, kind: :error}}} =
                 Server.handle_data(xlog(commit_msg(lsn)), after_begin)
      end)

      assert pids == Enum.map([EventModules, Events, Publisher], &pipeline_pid(&1, app))
      assert Enum.all?(pids, &Process.alive?/1)
    end

    test "a handler failure is counted and reported" do
      app = app_name("pipeline_raise_telemetry")
      start_pipeline!(app, RaisingHandler, ack_after_processing: true)
      attach_telemetry([[:walex, :replication, :handler_failed]])
      lsn = 0x2_0000_0040

      # Delivered twice, as Postgres would after the reconnect.
      capture_log(fn ->
        for _attempt <- 1..2 do
          {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))
          {:disconnect, _} = Server.handle_data(xlog(commit_msg(lsn)), after_begin)
        end
      end)

      assert_receive {:telemetry, [:walex, :replication, :handler_failed],
                      %{system_time: _, consecutive_failures: 1},
                      %{
                        app_name: ^app,
                        slot_name: "ack_after_processing_test_pipeline_raise_telemetry_walex",
                        reason: {:handler_failed, %{module: RaisingHandler}}
                      }}

      assert_receive {:telemetry, [:walex, :replication, :handler_failed],
                      %{consecutive_failures: 2}, _}
    end

    test "the failure position is recorded with the count" do
      app = app_name("pipeline_failure_lsn")
      start_pipeline!(app, RaisingHandler, ack_after_processing: true)
      lsn = 0x2_0000_0060

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))

      capture_log(fn ->
        {:disconnect, _} = Server.handle_data(xlog(commit_msg(lsn)), after_begin)
      end)

      assert 1 == Config.failure_count(app)
      assert lsn == Config.failure_lsn(app)
    end

    # Publisher state built directly: the decoder only sees the INSERT below.
    defp open_relation!(app, columns) do
      relation = %Messages.Relation{
        id: 7,
        namespace: "public",
        name: "todos",
        replica_identity: :default,
        columns: columns
      }

      :ok = Publisher.process_message_sync(relation, app)
    end

    defp insert_frame(value),
      do: <<?I, 7::32, ?N, 1::16, ?t, byte_size(value)::32, value::binary>>

    test "a change the Publisher fails to build disconnects, is counted and is not a handler failure" do
      app = app_name("pipeline_publisher_failed")
      start_pipeline!(app, OkHandler, ack_after_processing: true)
      open_relation!(app, [%Messages.Relation.Column{name: "amount", type: "numeric"}])
      attach_telemetry([[:walex, :replication, :handler_failed]])
      pids = Enum.map([EventModules, Events, Publisher], &pipeline_pid(&1, app))
      lsn = 0x2_0000_0080

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))

      capture_log(fn ->
        assert {:disconnect, {:publisher_failed, %{kind: :error, exception: Decimal.Error}}} =
                 Server.handle_data(xlog(insert_frame("not a number")), after_begin)
      end)

      assert 1 == Config.failure_count(app)
      assert lsn == Config.failure_lsn(app)
      assert pids == Enum.map([EventModules, Events, Publisher], &pipeline_pid(&1, app))
      refute_received {:telemetry, [:walex, :replication, :handler_failed], _, _}
    end

    # A RELATION whose column list is cut short: the decoder raises on it.
    @malformed_frame <<?R, 7::32, "public", 0, "t", 0, ?d, 1::16, 1, "x">>

    test "a frame the decoder raises on disconnects and is counted" do
      app = app_name("pipeline_decode_failed")
      start_pipeline!(app, OkHandler, ack_after_processing: true)
      lsn = 0x2_0000_00A0

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))

      capture_log(fn ->
        assert {:disconnect, {:decode_failed, %{kind: :error, exception: MatchError}}} =
                 Server.handle_data(xlog(@malformed_frame), after_begin)
      end)

      assert 1 == Config.failure_count(app)
      assert lsn == Config.failure_lsn(app)
    end

    test "without ack_after_processing, a frame the decoder raises on still crashes" do
      app = app_name("pipeline_decode_default")
      start_pipeline!(app, OkHandler, ack_after_processing: false)

      assert_raise MatchError, fn ->
        Server.handle_data(xlog(@malformed_frame), streaming_state(app))
      end
    end

    test "a change that does not match its relation's columns disconnects and is counted" do
      app = app_name("pipeline_column_mismatch")
      start_pipeline!(app, OkHandler, ack_after_processing: true)

      open_relation!(app, [
        %Messages.Relation.Column{name: "id", type: "int4"},
        %Messages.Relation.Column{name: "title", type: "text"}
      ])

      lsn = 0x2_0000_00A0

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))

      capture_log(fn ->
        assert {:disconnect, {:column_mismatch, 7}} =
                 Server.handle_data(xlog(insert_frame("1")), after_begin)
      end)

      assert 1 == Config.failure_count(app)
      assert lsn == Config.failure_lsn(app)
    end

    test "with ack_after_processing false, commits are recorded as received" do
      app = app_name("pipeline_disabled")
      start_pipeline!(app, OkHandler)
      lsn = 0x3_0000_0030

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), streaming_state(app))
      {:noreply, after_commit} = Server.handle_data(xlog(commit_msg(lsn)), after_begin)

      confirmed = lsn + 8
      assert %{in_txn: false, confirmed_lsn: ^confirmed} = after_commit
    end
  end

  # Commits handled before the failing one are redelivered after every
  # reconnect (they may not have been confirmed yet), so handling them is not
  # progress: only getting past the failing transaction ends the streak.
  describe "ending a failure streak" do
    @failing_lsn 0x5_0000_0050

    setup do
      app = app_name("streak_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      %{app: app}
    end

    # Fails the transaction committed at @failing_lsn, handles all others.
    defp failing_at_x(%Messages.Commit{lsn: lsn}, _app) do
      if Decoder.lsn_to_integer(lsn) == @failing_lsn,
        do: {:error, {:handler_failed, %{}}},
        else: :ok
    end

    defp failing_at_x(_message, _app), do: :ok

    # The state handle_connect/1 leaves behind, whether it queries or waits.
    defp connect(state) do
      case Server.handle_connect(state) do
        {:query, _query, connected} -> connected
        {:noreply, connected} -> connected
      end
    end

    # Delivers one transaction to a fresh connection's state.
    defp deliver(app, lsn) do
      connected = connect(streaming_state(app, message_middleware: &failing_at_x/2))
      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), connected)
      Server.handle_data(xlog(commit_msg(lsn)), after_begin)
    end

    test "only a commit at or past the failing one resets the count", %{app: app} do
      assert {:disconnect, _} = deliver(app, @failing_lsn)
      assert 1 == Config.failure_count(app)

      assert {:noreply, _, _} = deliver(app, @failing_lsn - 0x100)
      assert 1 == Config.failure_count(app)

      assert {:disconnect, _} = deliver(app, @failing_lsn)
      assert 2 == Config.failure_count(app)

      assert {:noreply, _, _} = deliver(app, @failing_lsn + 0x100)
      assert 0 == Config.failure_count(app)
      assert nil == Config.failure_lsn(app)
    end

    test "the failing transaction itself being handled resets the count", %{app: app} do
      Config.record_failure(app, @failing_lsn + 0x10)
      connected = connect(streaming_state(app, message_middleware: fn _, _ -> :ok end))

      {:noreply, after_begin} =
        Server.handle_data(xlog(begin_msg(@failing_lsn + 0x10)), connected)

      assert {:noreply, _, %{consecutive_failures: 0}} =
               Server.handle_data(xlog(commit_msg(@failing_lsn + 0x10)), after_begin)

      assert 0 == Config.failure_count(app)
    end

    test "a failure on a change records the open transaction's position", %{app: app} do
      failing_change = fn
        %Messages.Begin{}, _app -> :ok
        _message, _app -> {:error, {:unknown_relation, 42}}
      end

      {:noreply, after_begin} =
        Server.handle_data(
          xlog(begin_msg(@failing_lsn)),
          streaming_state(app, message_middleware: failing_change)
        )

      insert = <<?I, 42::32, ?N, 0::16>>
      assert {:disconnect, _} = Server.handle_data(xlog(insert), after_begin)
      assert @failing_lsn == Config.failure_lsn(app)
    end
  end

  describe "proactive ack after a handled commit" do
    setup do
      app = app_name("proactive_#{System.unique_integer([:positive])}")
      start_pipeline!(app, OkHandler, ack_after_processing: true)
      %{app: app}
    end

    defp commit!(state, lsn) do
      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), state)
      Server.handle_data(xlog(commit_msg(lsn)), after_begin)
    end

    test "replies with the commit's end LSN when no status was sent yet", %{app: app} do
      assert {:noreply, messages, %{last_status_at: at}} = commit!(streaming_state(app), 0x10)
      assert 0x18 == ack_lsn(messages)
      assert is_integer(at)
    end

    test "does not reply to a second commit inside the throttle window", %{app: app} do
      now = System.monotonic_time(:millisecond)

      assert {:noreply, after_commit} = commit!(streaming_state(app, last_status_at: now), 0x20)
      assert 0x28 == after_commit.confirmed_lsn
    end

    test "replies again once the throttle window has passed", %{app: app} do
      long_ago = System.monotonic_time(:millisecond) - 10_000

      assert {:noreply, messages, _state} =
               commit!(streaming_state(app, last_status_at: long_ago), 0x30)

      assert 0x38 == ack_lsn(messages)
    end

    test "a keepalive reply counts toward the throttle", %{app: app} do
      {:noreply, [_], sent} = Server.handle_data(keepalive(0x100, 1), streaming_state(app))

      assert {:noreply, _state} = commit!(sent, 0x40)
    end

    test "a periodic tick counts toward the throttle", %{app: app} do
      {:noreply, [_], sent} = tick(streaming_state(app, confirmed_lsn: 0x10))

      assert {:noreply, _state} = commit!(sent, 0x50)
    end

    test "a keepalive's LSN-0 fallback counts toward the throttle", %{app: app} do
      {:noreply, [_], sent} =
        Server.handle_data(keepalive(0, 1), streaming_state(app, in_txn: true))

      assert is_integer(sent.last_status_at)
    end
  end

  describe "proactive ack with ack_after_processing off" do
    test "never replies after a commit" do
      app = app_name("proactive_off_#{System.unique_integer([:positive])}")
      start_pipeline!(app, OkHandler)

      assert {:noreply, _state} = commit!(streaming_state(app), 0x60)
    end
  end

  describe "a middleware error" do
    @failing {:error, {:commit_mismatch, "boom"}}

    # Postgrex keeps the pre-message state on {:disconnect, _} (see
    # replication_connection.ex), so confirmed_lsn cannot advance past a failure.
    # Postgrex logs the disconnect reason, so Server does not log it again.
    test "with ack_after_processing, disconnects without confirming the commit" do
      app = app_name("middleware_error_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)

      failing =
        streaming_state(app, in_txn: true, message_middleware: fn _, _ -> @failing end)

      assert {:disconnect, {:commit_mismatch, "boom"}} =
               Server.handle_data(xlog(commit_msg(0x20)), failing)

      # Counted, so the reconnect backs off.
      assert 1 == Config.failure_count(app)
    end

    test "that is neither :ok nor an error fails closed with ack_after_processing" do
      app = app_name("middleware_other_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      odd = streaming_state(app, in_txn: true, message_middleware: fn _, _ -> nil end)

      assert {:disconnect, {:unexpected_middleware_result, nil}} =
               Server.handle_data(xlog(commit_msg(0x20)), odd)

      assert 1 == Config.failure_count(app)
    end

    test "without ack_after_processing, is ignored" do
      app = app_name("middleware_error_off_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true)
      ignoring = streaming_state(app, in_txn: true, message_middleware: fn _, _ -> @failing end)

      # The commit still counts as received, which is what this mode acknowledges.
      assert {:noreply, %{in_txn: false, confirmed_lsn: 0x28}} =
               Server.handle_data(xlog(commit_msg(0x20)), ignoring)

      assert 0 == Config.failure_count(app)
    end
  end
end
