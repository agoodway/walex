defmodule WalEx.Replication.StandbyStatusTest do
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

  defp start_config!(app, extra) do
    configs =
      [name: app, publication: "publication", subscriptions: ["subscriptions"]] ++
        @base_db ++ extra

    {:ok, _pid} = Config.start_link(configs: configs)
  end

  defp state(app, extra \\ []) do
    Map.merge(
      %{app_name: app, streaming?: true, last_wal_end: 0, standby_timer: nil},
      Map.new(extra)
    )
  end

  defp keepalive(wal_end, reply), do: <<?k, wal_end::64, 0::64, reply::8>>
  defp ack_lsn([<<?r, lsn::64, _::64, _::64, _::64, _::8>>]), do: lsn

  describe "config" do
    test "standby_status_interval defaults to disabled" do
      start_config!(:sb_default, [])
      assert Config.get_configs(:sb_default, :standby_status_interval) == nil
    end

    test "standby_status_interval accepts a positive integer" do
      start_config!(:sb_set, standby_status_interval: 5_000)
      assert Config.get_configs(:sb_set, :standby_status_interval) == 5_000
    end

    test "standby_status_interval rejects anything else" do
      for bad <- [0, -1, "5s", 1.5] do
        assert_raise RuntimeError, ~r/standby_status_interval/, fn ->
          start_config!(:sb_bad, standby_status_interval: bad)
        end
      end
    end
  end

  describe "tracking the WAL position" do
    test "a keepalive without a reply request records wal_end" do
      app = :sb_track_k
      start_config!(app, [])

      assert {:noreply, [], %{last_wal_end: 700}} =
               Server.handle_data(keepalive(700, 0), state(app))
    end

    test "the position never moves backwards" do
      app = :sb_track_back
      start_config!(app, [])

      assert {:noreply, [], %{last_wal_end: 900}} =
               Server.handle_data(keepalive(700, 0), state(app, last_wal_end: 900))
    end
  end

  describe "periodic standby status" do
    test "does not arm a timer when no interval is configured" do
      app = :sb_off
      start_config!(app, [])

      assert {:noreply, _msgs, %{standby_timer: nil}} =
               Server.handle_info(:standby_status, state(app, last_wal_end: 100))
    end

    test "sends wal_end + 1 and reschedules while streaming" do
      app = :sb_send
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, msgs, %{standby_timer: ref}} =
               Server.handle_info(:standby_status, state(app, last_wal_end: 100))

      assert ack_lsn(msgs) == 101
      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "sends nothing before any WAL position is known, but keeps ticking" do
      app = :sb_unknown
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, [], %{standby_timer: ref}} =
               Server.handle_info(:standby_status, state(app))

      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "stops ticking when replication is not streaming" do
      app = :sb_stopped
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, %{standby_timer: nil}} =
               Server.handle_info(
                 :standby_status,
                 state(app, streaming?: false, last_wal_end: 5)
               )
    end
  end
end
