defmodule WalEx.Replication.ServerLifecycleTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  import WalEx.Support.TestHelpers,
    only: [attach_telemetry: 1, server_state: 1, server_state: 2]

  alias WalEx.Support.TestHelpers

  alias WalEx.{Config, Config.Registry}
  alias WalEx.Events.EventModules
  alias WalEx.Replication.Server

  defmodule HookModule do
    use WalEx.Event, name: :lifecycle_app

    on_connect(fn info ->
      send(Process.whereis(:lifecycle_test), {:hook, info})
    end)
  end

  setup do
    start_supervised!(Registry.child_spec())
    Process.register(self(), :lifecycle_test)
    :ok
  end

  defp start_config!(app, extra \\ []),
    do:
      TestHelpers.start_config!(
        app,
        [slot_name: "lifecycle_slot", modules: [HookModule]] ++ extra
      )

  describe "handle_connect/1" do
    test "starts the connection sequence" do
      start_config!(:lifecycle_app)

      assert {:query, query, %{step: :publication_exists}} =
               Server.handle_connect(server_state(:lifecycle_app))

      assert is_binary(IO.iodata_to_binary(query))
    end

    test "emits a connected event, flagging reconnects" do
      start_config!(:lifecycle_app)
      attach_telemetry([[:walex, :replication, :connected]])

      {:query, _q, after_first} = Server.handle_connect(server_state(:lifecycle_app))

      assert_receive {:telemetry, [:walex, :replication, :connected], %{system_time: _},
                      %{app_name: :lifecycle_app, slot_name: "lifecycle_slot", reconnect?: false}}

      {:query, _q, _} = Server.handle_connect(after_first)

      assert_receive {:telemetry, [:walex, :replication, :connected], %{}, %{reconnect?: true}}
    end

    test "flags a reconnect after the replication processes restart" do
      start_config!(:lifecycle_app)
      attach_telemetry([[:walex, :replication, :connected]])

      {:query, _q, _} = Server.handle_connect(server_state(:lifecycle_app))
      assert_receive {:telemetry, [:walex, :replication, :connected], %{}, %{reconnect?: false}}

      # A crash restarts Server with a brand-new state; the count must survive.
      {:query, _q, _} = Server.handle_connect(server_state(:lifecycle_app))
      assert_receive {:telemetry, [:walex, :replication, :connected], %{}, %{reconnect?: true}}
    end

    test "runs the on_connect hook of the configured modules" do
      start_config!(:lifecycle_app)
      start_supervised!({EventModules, app_name: :lifecycle_app})

      {:query, _q, after_first} = Server.handle_connect(server_state(:lifecycle_app))
      assert_receive {:hook, %{app_name: :lifecycle_app, reconnect?: false}}

      {:query, _q, _} = Server.handle_connect(after_first)
      assert_receive {:hook, %{reconnect?: true}}
    end

    test "still connects when no event modules are running" do
      start_config!(:lifecycle_app)

      assert {:query, _q, _state} = Server.handle_connect(server_state(:lifecycle_app))
    end
  end

  describe "handle_connect/1 after consecutive processing failures" do
    test "defers the connection sequence until the backoff has passed" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)

      assert {:noreply, %{resume_token: token, consecutive_failures: 1} = deferred} =
               Server.handle_connect(server_state(:lifecycle_app, failure_backoff_base_ms: 10))

      assert is_reference(token)
      assert_receive {:resume_connect, ^token}, 1_000

      assert {:query, query, %{step: :publication_exists, resume_token: nil}} =
               Server.handle_info({:resume_connect, token}, deferred)

      assert is_binary(IO.iodata_to_binary(query))
    end

    test "queries at once, without a timer or warning, when the deadline has passed" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)
      Config.resume_deadline(:lifecycle_app, System.monotonic_time(:millisecond) - 5)

      log =
        capture_log([level: :warning], fn ->
          assert {:query, _query, %{step: :publication_exists, resume_token: nil}} =
                   Server.handle_connect(server_state(:lifecycle_app))
        end)

      refute log =~ "Resuming replication"
      refute_receive {:resume_connect, _}, 50
    end

    test "still reports the connection right away" do
      start_config!(:lifecycle_app)
      attach_telemetry([[:walex, :replication, :connected]])
      Config.record_failure(:lifecycle_app, 100)

      {:noreply, _deferred} = Server.handle_connect(server_state(:lifecycle_app))

      assert_receive {:telemetry, [:walex, :replication, :connected], %{}, _}
    end

    test "ignores a resume message from an earlier connection" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)

      {:noreply, deferred} = Server.handle_connect(server_state(:lifecycle_app))

      assert {:noreply, ^deferred} = Server.handle_info({:resume_connect, make_ref()}, deferred)
    end

    test "a disconnect while waiting drops the pending resume" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)

      {:noreply, %{resume_token: token, resume_timer: timer} = deferred} =
        Server.handle_connect(server_state(:lifecycle_app))

      assert is_integer(Process.read_timer(timer))

      {:noreply, disconnected} = Server.handle_disconnect(deferred)

      assert %{resume_token: nil, resume_timer: nil} = disconnected
      refute Process.read_timer(timer)

      assert {:noreply, ^disconnected} =
               Server.handle_info({:resume_connect, token}, disconnected)
    end

    # A drop of the idle connection during the wait (idle_session_timeout, a
    # proxy) must not restart the full delay, or a long one never ends.
    test "a reconnect during the wait resumes at the original deadline" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)
      state = server_state(:lifecycle_app, failure_backoff_base_ms: 1_000)

      {:noreply, %{resume_timer: first}} = Server.handle_connect(state)
      assert Process.read_timer(first) > 900
      Process.sleep(300)

      {:noreply, %{resume_timer: second}} = Server.handle_connect(state)
      assert Process.read_timer(second) <= 700
    end

    test "a new failure arms the full delay again" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)
      state = server_state(:lifecycle_app, failure_backoff_base_ms: 1_000)

      {:noreply, _deferred} = Server.handle_connect(state)
      Process.sleep(300)
      Config.record_failure(:lifecycle_app, 100)

      {:noreply, %{resume_timer: timer}} = Server.handle_connect(state)
      assert Process.read_timer(timer) > 1_900
    end

    test "records the failure position read at connect" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)

      assert {:noreply, %{failure_lsn: 100}} = Server.handle_connect(server_state(:lifecycle_app))
    end

    # A slot check armed by an earlier connection must not start the slot
    # sequence early, or the later resume would issue a query mid-sequence.
    test "ignores a slot check while waiting" do
      start_config!(:lifecycle_app)
      Config.record_failure(:lifecycle_app, 100)

      {:noreply, deferred} = Server.handle_connect(server_state(:lifecycle_app))

      assert {:noreply, ^deferred} =
               Server.handle_info({:check_slot_status, make_ref()}, deferred)
    end
  end

  describe "failure_backoff/2" do
    test "doubles from the base with each consecutive failure" do
      assert 1_000 == Server.failure_backoff(1, 1_000)
      assert 2_000 == Server.failure_backoff(2, 1_000)
      assert 4_000 == Server.failure_backoff(3, 1_000)
      assert 32_000 == Server.failure_backoff(6, 1_000)
    end

    test "is capped at 60 seconds" do
      assert 60_000 == Server.failure_backoff(7, 1_000)
      assert 60_000 == Server.failure_backoff(1_000, 1_000)
    end
  end

  describe "the :failure_backoff_base_ms override" do
    setup do
      on_exit(fn -> Application.delete_env(:walex, :failure_backoff_base_ms) end)
    end

    test "is used when it is a positive integer" do
      start_config!(:lifecycle_app)
      Application.put_env(:walex, :failure_backoff_base_ms, 50)

      assert %{failure_backoff_base_ms: 50} = server_state(:lifecycle_app)
    end

    for bad <- [0, -1, 1.5, "100"] do
      test "falls back to 1000ms with a warning for #{inspect(bad)}" do
        start_config!(:lifecycle_app)
        Application.put_env(:walex, :failure_backoff_base_ms, unquote(bad))

        log =
          capture_log(fn ->
            assert %{failure_backoff_base_ms: 1_000} = server_state(:lifecycle_app)
          end)

        assert log =~ "failure_backoff_base_ms"
      end
    end
  end

  describe "handle_data/2" do
    test "emits a streaming event on the first replication message only" do
      start_config!(:lifecycle_app)
      attach_telemetry([[:walex, :replication, :streaming]])
      keepalive = <<?k, 10::64, 0::64, 0::8>>

      {:noreply, [], streaming} = Server.handle_data(keepalive, server_state(:lifecycle_app))

      assert_receive {:telemetry, [:walex, :replication, :streaming], %{system_time: _},
                      %{app_name: :lifecycle_app, slot_name: "lifecycle_slot"}}

      {:noreply, [], _} = Server.handle_data(keepalive, streaming)
      refute_receive {:telemetry, [:walex, :replication, :streaming], _, _}
    end
  end

  describe "handle_disconnect/1" do
    test "emits a disconnected event" do
      start_config!(:lifecycle_app)
      attach_telemetry([[:walex, :replication, :disconnected]])

      assert {:noreply, %{step: :disconnected}} =
               Server.handle_disconnect(server_state(:lifecycle_app, step: :streaming))

      assert_receive {:telemetry, [:walex, :replication, :disconnected], %{system_time: _},
                      %{app_name: :lifecycle_app, slot_name: "lifecycle_slot"}}
    end
  end
end
