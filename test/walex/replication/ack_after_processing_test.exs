defmodule WalEx.Replication.AckAfterProcessingTest do
  use ExUnit.Case, async: false

  alias WalEx.Config
  alias WalEx.Events
  alias WalEx.Events.EventModules
  alias WalEx.Replication.{Publisher, Server}

  @base_db [
    hostname: "localhost",
    username: "postgres",
    password: "postgres",
    database: "postgres",
    port: 5432,
    ssl: false,
    ssl_opts: [verify: :verify_none]
  ]

  defmodule OkHandler do
    def process_all(_txn), do: :ok
  end

  defmodule RaisingHandler do
    def process_all(_txn), do: raise("handler failed")
  end

  setup do
    start_supervised!(WalEx.Config.Registry.child_spec())
    :ok
  end

  defp app_name(suffix), do: String.to_atom("ack_after_processing_test_#{suffix}")

  defp start_config!(app_name, extra) do
    configs =
      [
        name: app_name,
        publication: "publication",
        subscriptions: ["subscriptions"]
      ] ++ @base_db ++ extra

    {:ok, _pid} = Config.start_link(configs: configs)
  end

  defp state(app, extra \\ []) do
    Map.merge(
      %{
        app_name: app,
        step: :streaming,
        slot_name: "ack_slot",
        publication: "publication",
        streaming?: true,
        standby_timer: nil,
        standby_token: nil,
        ack_after_processing: Config.get_configs(app, :ack_after_processing),
        standby_status_interval: nil,
        slot_confirmed_lsn: nil,
        confirmed_lsn: nil,
        last_wal_end: 0,
        in_txn: false,
        message_middleware: Config.get_configs(app, :message_middleware)
      },
      Map.new(extra)
    )
  end

  defp keepalive(wal_end, reply), do: <<?k, wal_end::64, 0::64, reply::8>>

  # A standby timer message carrying the token the state expects.
  defp tick(state) do
    token = make_ref()
    Server.handle_info({:standby_status, token}, %{state | standby_token: token})
  end

  defp xlog(payload), do: <<?w, 0::64, 0::64, 0::64, payload::binary>>
  defp begin_msg(lsn), do: <<?B, lsn::64, 0::64, 1::32>>
  defp commit_msg(lsn), do: <<?C, 0::8, lsn::64, lsn::64, 0::64>>

  defp extract_ack_lsn([<<?r, write_lsn::64, _flush::64, _apply::64, _ts::64, _reply::8>>]),
    do: write_lsn

  describe "Server keepalive acknowledgements" do
    setup do
      app = app_name("keepalive_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      %{app: app}
    end

    test "with no transaction open, reports wal_end + 1", %{app: app} do
      assert {:noreply, messages, _state} = Server.handle_data(keepalive(1_000, 1), state(app))
      assert 1_001 == extract_ack_lsn(messages)
    end

    test "with a transaction open, reports the last confirmed commit", %{app: app} do
      # wal_end is intentionally larger than the confirmed position; the ack
      # must be driven by the confirmed commit while a transaction is open.
      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 state(app, in_txn: true, confirmed_lsn: 2_001)
               )

      assert 2_001 == extract_ack_lsn(messages)
    end

    test "does not send a status update when keepalive reply flag is 0", %{app: app} do
      assert {:noreply, [], _state} =
               Server.handle_data(keepalive(9_999, 0), state(app, confirmed_lsn: 124))
    end

    test "with ack_after_processing false, ignores confirmed commits" do
      app = app_name("server_disabled")
      start_config!(app, durable_slot: true, ack_after_processing: false)

      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(5_000, 1),
                 state(app, in_txn: true, confirmed_lsn: 42)
               )

      assert 5_001 == extract_ack_lsn(messages)
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
               Server.handle_data(keepalive(9_999, 1), state(app, in_txn: true))

      assert 0 == extract_ack_lsn(messages)
    end

    test "answers a reply request with the slot's confirmed_flush_lsn when known", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 state(app, in_txn: true, slot_confirmed_lsn: 4_000)
               )

      assert 4_000 == extract_ack_lsn(messages)
    end

    test "never reports below the slot's confirmed_flush_lsn", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 state(app, in_txn: true, confirmed_lsn: 101, slot_confirmed_lsn: 4_000)
               )

      assert 4_000 == extract_ack_lsn(messages)
    end

    test "sends no unsolicited periodic update when nothing is safe to report", %{app: app} do
      assert {:noreply, [], _state} =
               tick(state(app, in_txn: true, last_wal_end: 9_999))
    end
  end

  describe "reading the slot position at connect" do
    defp slot_state(app, step) do
      %{
        app_name: app,
        step: step,
        slot_name: "ack_slot",
        publication: "publication",
        durable_slot: true,
        slot_confirmed_lsn: nil
      }
    end

    defp slot_row(active, confirmed) do
      [
        %Postgrex.Result{
          columns: ["active", "confirmed_flush_lsn"],
          rows: [[active, confirmed]],
          num_rows: 1
        }
      ]
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

      for value <- [nil, "garbage", "0/0"] do
        assert {:stream, _query, [], %{slot_confirmed_lsn: nil}} =
                 Server.handle_result(slot_row("f", value), slot_state(app, :slot_exists))
      end
    end

    test "a newly created slot records consistent_point" do
      app = app_name("slot_created")

      result = %Postgrex.Result{
        columns: ["slot_name", "consistent_point", "snapshot_name", "output_plugin"],
        rows: [["ack_slot", "1/10", nil, "pgoutput"]],
        num_rows: 1
      }

      assert {:stream, _query, [], %{slot_confirmed_lsn: lsn}} =
               Server.handle_result([result], slot_state(app, :create_slot))

      assert lsn == 0x1_0000_0010
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

    defp config_agent(app), do: WalEx.Config.Registry.set_name(:set_agent, Config, app)

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

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), state(app))
      assert %{in_txn: true, confirmed_lsn: nil} = after_begin

      {:noreply, after_commit} = Server.handle_data(xlog(commit_msg(lsn)), after_begin)
      assert %{in_txn: false, confirmed_lsn: confirmed} = after_commit
      assert confirmed == lsn + 1
    end

    test "a commit whose handler raises is never confirmed" do
      app = app_name("pipeline_raise")
      start_pipeline!(app, RaisingHandler, ack_after_processing: true)
      lsn = 0x2_0000_0020

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), state(app))

      # The synchronous call chain exits, so Server never gets to record the
      # commit; in production it crashes and Postgres redelivers.
      assert catch_exit(Server.handle_data(xlog(commit_msg(lsn)), after_begin))
    end

    test "with ack_after_processing false, commits are not recorded" do
      app = app_name("pipeline_disabled")
      start_pipeline!(app, OkHandler)
      lsn = 0x3_0000_0030

      {:noreply, after_begin} = Server.handle_data(xlog(begin_msg(lsn)), state(app))
      {:noreply, after_commit} = Server.handle_data(xlog(commit_msg(lsn)), after_begin)

      assert %{in_txn: false, confirmed_lsn: nil} = after_commit
    end
  end
end
