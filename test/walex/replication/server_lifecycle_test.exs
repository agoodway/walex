defmodule WalEx.Replication.ServerLifecycleTest do
  use ExUnit.Case, async: false

  alias WalEx.Config
  alias WalEx.Events.EventModules
  alias WalEx.Replication.Server

  defmodule HookModule do
    use WalEx.Event, name: :lifecycle_app

    on_connect(fn info ->
      send(Process.whereis(:lifecycle_test), {:hook, info})
    end)
  end

  @hostname System.get_env("PGHOST", "localhost")
  @username System.get_env("PGUSER", "postgres")
  @password System.get_env("PGPASSWORD", "postgres")
  @port String.to_integer(System.get_env("PGPORT", "5432"))

  @base_db [
    hostname: @hostname,
    username: @username,
    password: @password,
    database: "postgres",
    port: @port,
    ssl: false,
    ssl_opts: [verify: :verify_none]
  ]

  setup do
    start_supervised!(WalEx.Config.Registry.child_spec())
    Process.register(self(), :lifecycle_test)
    :ok
  end

  defp start_config!(app, extra \\ []) do
    configs =
      [
        name: app,
        publication: "publication",
        subscriptions: ["subscriptions"],
        slot_name: "lifecycle_slot",
        modules: [HookModule]
      ] ++ @base_db ++ extra

    {:ok, _pid} = Config.start_link(configs: configs)
  end

  defp state(app, extra \\ []) do
    Map.merge(
      %{
        app_name: app,
        step: :disconnected,
        slot_name: "lifecycle_slot",
        publication: "publication",
        streaming?: false,
        connects: 0,
        last_wal_end: 0,
        standby_timer: nil,
        message_middleware: fn _message, _app -> :ok end
      },
      Map.new(extra)
    )
  end

  defp attach(events) do
    id = "lifecycle-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach_many(
      id,
      events,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  describe "handle_connect/1" do
    test "starts the connection sequence" do
      start_config!(:lifecycle_app)

      assert {:query, query, %{step: :publication_exists, connects: 1}} =
               Server.handle_connect(state(:lifecycle_app))

      assert is_binary(IO.iodata_to_binary(query))
    end

    test "emits a connected event, flagging reconnects" do
      start_config!(:lifecycle_app)
      attach([[:walex, :replication, :connected]])

      {:query, _q, after_first} = Server.handle_connect(state(:lifecycle_app))

      assert_receive {:telemetry, [:walex, :replication, :connected], %{},
                      %{app_name: :lifecycle_app, slot_name: "lifecycle_slot", reconnect?: false}}

      {:query, _q, _} = Server.handle_connect(after_first)

      assert_receive {:telemetry, [:walex, :replication, :connected], %{}, %{reconnect?: true}}
    end

    test "runs the on_connect hook of the configured modules" do
      start_config!(:lifecycle_app)
      start_supervised!({EventModules, app_name: :lifecycle_app})

      {:query, _q, after_first} = Server.handle_connect(state(:lifecycle_app))
      assert_receive {:hook, %{app_name: :lifecycle_app, reconnect?: false}}

      {:query, _q, _} = Server.handle_connect(after_first)
      assert_receive {:hook, %{reconnect?: true}}
    end

    test "still connects when no event modules are running" do
      start_config!(:lifecycle_app)

      assert {:query, _q, _state} = Server.handle_connect(state(:lifecycle_app))
    end
  end

  describe "handle_disconnect/1" do
    test "emits a disconnected event" do
      start_config!(:lifecycle_app)
      attach([[:walex, :replication, :disconnected]])

      assert {:noreply, %{step: :disconnected}} =
               Server.handle_disconnect(state(:lifecycle_app, step: {:streaming, 0}))

      assert_receive {:telemetry, [:walex, :replication, :disconnected], %{},
                      %{app_name: :lifecycle_app, slot_name: "lifecycle_slot"}}
    end
  end
end
