defmodule WalEx.Replication.ServerStreamStateTest do
  use ExUnit.Case, async: false

  import WalEx.Support.TestHelpers,
    only: [attach_telemetry: 1, keepalive: 2, server_state: 1, server_state: 2, tick: 1]

  alias WalEx.Support.TestHelpers

  alias WalEx.Config.Registry
  alias WalEx.Replication.Server

  setup do
    start_supervised!(Registry.child_spec())
    :ok
  end

  defp start_config!(app, extra \\ []),
    do: TestHelpers.start_config!(app, [slot_name: "lifecycle_slot"] ++ extra)

  describe "handle_connect/1" do
    test "forgets everything about the previous connection" do
      start_config!(:lifecycle_app, standby_status_interval: 60_000)
      timer = Process.send_after(self(), :never, 60_000)

      stale =
        server_state(:lifecycle_app,
          streaming?: true,
          last_wal_end: 99,
          in_txn: true,
          standby_timer: timer,
          slot_confirmed_lsn: 1_234,
          confirmed_lsn: 5_678,
          last_reported_lsn: 5_678
        )

      assert {:query, _query, fresh} = Server.handle_connect(stale)

      assert %{
               streaming?: false,
               last_wal_end: 0,
               in_txn: false,
               standby_timer: nil,
               slot_confirmed_lsn: nil,
               confirmed_lsn: nil,
               last_reported_lsn: nil,
               last_status_at: nil
             } = fresh

      assert Process.read_timer(timer) == false
    end
  end

  describe "handle_disconnect/1" do
    test "emits a disconnected event and drops the stream state" do
      start_config!(:lifecycle_app)
      attach_telemetry([[:walex, :replication, :disconnected]])
      timer = Process.send_after(self(), :never, 60_000)

      assert {:noreply, %{streaming?: false, standby_timer: nil, step: :disconnected}} =
               Server.handle_disconnect(
                 server_state(:lifecycle_app, streaming?: true, standby_timer: timer)
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
               Server.handle_data(keepalive(10, 0), server_state(:lifecycle_app))

      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "later messages do not arm a second timer" do
      start_config!(:lifecycle_app, standby_status_interval: 60_000)

      {:noreply, [], first} = Server.handle_data(keepalive(10, 0), server_state(:lifecycle_app))
      {:noreply, [], second} = Server.handle_data(keepalive(20, 0), first)

      assert second.standby_timer == first.standby_timer
      Process.cancel_timer(first.standby_timer)
    end

    test "without an interval nothing is scheduled" do
      start_config!(:lifecycle_app)

      assert {:noreply, [], %{streaming?: true, standby_timer: nil}} =
               Server.handle_data(keepalive(10, 0), server_state(:lifecycle_app))
    end

    test "a tick after the connection was lost sends nothing and stops ticking" do
      start_config!(:lifecycle_app, standby_status_interval: 60_000)

      assert {:noreply, %{standby_timer: nil}} =
               tick(server_state(:lifecycle_app, streaming?: false, last_wal_end: 50))
    end
  end
end
