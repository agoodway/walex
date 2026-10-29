defmodule WalEx.Replication.ServerStreamStateTest do
  use ExUnit.Case, async: false

  alias WalEx.Config
  alias WalEx.Replication.Server

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
    :ok
  end

  defp start_config!(app, extra \\ []) do
    configs =
      [
        name: app,
        publication: "publication",
        subscriptions: ["subscriptions"],
        slot_name: "lifecycle_slot"
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

  defp keepalive(wal_end, reply), do: <<?k, wal_end::64, 0::64, reply::8>>

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
    test "forgets everything about the previous connection" do
      start_config!(:lifecycle_app, standby_status_interval: 60_000)
      timer = Process.send_after(self(), :never, 60_000)

      stale =
        state(:lifecycle_app,
          streaming?: true,
          last_wal_end: 99,
          standby_timer: timer,
          connects: 1
        )

      assert {:query, _query, fresh} = Server.handle_connect(stale)

      assert %{streaming?: false, last_wal_end: 0, standby_timer: nil} = fresh
      assert Process.read_timer(timer) == false
    end
  end

  describe "handle_disconnect/1" do
    test "emits a disconnected event and drops the stream state" do
      start_config!(:lifecycle_app)
      attach([[:walex, :replication, :disconnected]])
      timer = Process.send_after(self(), :never, 60_000)

      assert {:noreply, %{streaming?: false, standby_timer: nil, step: :disconnected}} =
               Server.handle_disconnect(
                 state(:lifecycle_app, streaming?: true, standby_timer: timer)
               )

      assert_receive {:telemetry, [:walex, :replication, :disconnected], %{},
                      %{app_name: :lifecycle_app, slot_name: "lifecycle_slot"}}

      assert Process.read_timer(timer) == false
    end
  end

  describe "detecting that streaming has started" do
    # Postgrex never calls handle_result/2 after a successful START_REPLICATION,
    # so the first replication message is the only signal.
    test "the first keepalive marks the connection as streaming and arms the timer" do
      start_config!(:lifecycle_app, standby_status_interval: 60_000)

      assert {:noreply, [], %{streaming?: true, standby_timer: ref}} =
               Server.handle_data(keepalive(10, 0), state(:lifecycle_app))

      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "later messages do not arm a second timer" do
      start_config!(:lifecycle_app, standby_status_interval: 60_000)

      {:noreply, [], first} = Server.handle_data(keepalive(10, 0), state(:lifecycle_app))
      {:noreply, [], second} = Server.handle_data(keepalive(20, 0), first)

      assert second.standby_timer == first.standby_timer
      Process.cancel_timer(first.standby_timer)
    end

    test "without an interval nothing is scheduled" do
      start_config!(:lifecycle_app)

      assert {:noreply, [], %{streaming?: true, standby_timer: nil}} =
               Server.handle_data(keepalive(10, 0), state(:lifecycle_app))
    end

    test "a tick after the connection was lost sends nothing and stops ticking" do
      start_config!(:lifecycle_app, standby_status_interval: 60_000)

      assert {:noreply, %{standby_timer: nil}} =
               Server.handle_info(
                 :standby_status,
                 state(:lifecycle_app, streaming?: false, last_wal_end: 50)
               )
    end
  end
end
