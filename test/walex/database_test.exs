defmodule WalEx.DatabaseTest do
  use ExUnit.Case, async: false
  import WalEx.Support.TestHelpers
  import ExUnit.CaptureLog
  alias Postgrex.Error
  alias WalEx.Supervisor, as: WalExSupervisor

  require Logger

  @pg_config pg_config()
  @database "todos_test"

  @base_configs [
    name: :todos,
    hostname: @pg_config[:hostname],
    username: @pg_config[:username],
    password: @pg_config[:password],
    database: @database,
    port: @pg_config[:port],
    subscriptions: ["user", "todo"],
    publication: "events",
    modules: [TestModule]
  ]

  # The slots these tests create; `pg_drop_slots/2` touches no others.
  @owned_slots ["todos_walex", "walex_test_userdefined", "walex_test_durable"]

  @replication_slot %{
    "active" => true,
    "slot_name" => "todos_walex",
    "slot_type" => "logical",
    "temporary" => true
  }

  describe "logical replication" do
    setup do
      {{:ok, database_pid}, _log} = with_log(fn -> start_database() end)
      pg_drop_slots(database_pid, @owned_slots)

      %{database_pid: database_pid}
    end

    test "should have logical replication set up", %{database_pid: pid} do
      assert is_pid(pid)
      assert [%{"wal_level" => "logical"}] == query(pid, "SHOW wal_level;")
    end

    test "error early if publication doesn't exists" do
      Process.flag(:trap_exit, true)
      config = Keyword.put(@base_configs, :publication, "non_existent_publication")

      capture_log(fn ->
        {_pid, ref} =
          spawn_monitor(fn -> WalExSupervisor.start_link(config) end)

        assert_receive {:DOWN, ^ref, _, _, {:shutdown, _}}
      end)
    end

    test "an injected publication is only ever a name, so no second statement runs", %{
      database_pid: pid
    } do
      Process.flag(:trap_exit, true)
      marker = "walex_s1_marker"

      on_exit(fn ->
        {:ok, conn} = Postgrex.start_link(Keyword.delete(@base_configs, :name))
        Postgrex.query!(conn, "DROP TABLE IF EXISTS #{marker}", [])
      end)

      for injected <- [
            "events'; CREATE TABLE #{marker}(id int); --",
            "events\\'; CREATE TABLE #{marker}(id int); --"
          ] do
        config = Keyword.put(@base_configs, :publication, injected)

        capture_log(fn ->
          # The backslash variant is refused by the validator; the plain one is
          # escaped, looked up as a name, and not found.
          assert {:error, {:shutdown, {:failed_to_start_child, failed, reason}}} =
                   WalExSupervisor.start_link(config)

          assert failed in [WalEx.Config, WalEx.Replication.Supervisor]
          assert inspect(reason) =~ ~r/ArgumentError|Publication doesn't exist/
        end)
      end

      assert [%{"found" => false}] =
               query(pid, "SELECT to_regclass('#{marker}') IS NOT NULL AS found")
    end

    test "a mixed-case publication name streams", %{database_pid: pid} do
      Postgrex.query!(pid, ~s[DROP PUBLICATION IF EXISTS "WalexMixed"], [])
      Postgrex.query!(pid, ~s[CREATE PUBLICATION "WalexMixed" FOR TABLE "user"], [])

      on_exit(fn ->
        {:ok, conn} = Postgrex.start_link(Keyword.delete(@base_configs, :name))
        Postgrex.query!(conn, ~s[DROP PUBLICATION IF EXISTS "WalexMixed"], [])
      end)

      capture_log(fn ->
        config = Keyword.put(@base_configs, :publication, "WalexMixed")
        assert {:ok, _pid} = start_walex_supervised(config)
        assert [_slot] = pg_replication_slots(pid)
      end)

      # pgoutput resolves publication_names on the first decoded change, so only
      # a delivered event proves the name was quoted.
      assert_update_user(pid)
    end

    test "should start replication slot", %{database_pid: database_pid} do
      capture_log(fn ->
        assert {:ok, replication_pid} = start_walex_supervised(@base_configs)
        assert is_pid(replication_pid)
        assert [@replication_slot] = pg_replication_slots(database_pid)
      end)
    end

    test "user-defined slot_name", %{database_pid: database_pid} do
      slot_name = "walex_test_userdefined"

      config = Keyword.put(@base_configs, :slot_name, slot_name)

      capture_log(fn ->
        assert {:ok, replication_pid} = start_walex_supervised(config)

        assert is_pid(replication_pid)
        assert [slot] = pg_replication_slots(database_pid)
        assert Map.fetch!(slot, "slot_name") == slot_name
      end)
    end

    test "should re-initiate after forcing database process termination" do
      capture_log(fn ->
        assert {:ok, supervisor_pid} = start_walex_supervised(@base_configs, TestSupervisor)
        database_pid = get_database_pid(supervisor_pid)

        assert is_pid(database_pid)
        assert [@replication_slot] = pg_replication_slots(database_pid)

        assert Process.exit(database_pid, :kill)
               |> tap_debug("Forcefully killed database connection: ")

        refute Process.info(database_pid)

        new_database_pid = get_database_pid(supervisor_pid)

        assert is_pid(new_database_pid)
        refute database_pid == new_database_pid
        assert_update_user(new_database_pid)
      end)
    end

    test "should re-initiate after database connection restarted by supervisor" do
      capture_log(fn ->
        assert {:ok, supervisor_pid} = start_walex_supervised(@base_configs, TestSupervisor)
        database_pid = get_database_pid(supervisor_pid)

        Supervisor.terminate_child(supervisor_pid, DBConnection.ConnectionPool)
        |> tap_debug("Supervisor terminated database connection: ")

        assert :undefined == get_database_pid(supervisor_pid)

        refute Process.info(database_pid)

        Supervisor.restart_child(supervisor_pid, DBConnection.ConnectionPool)
        |> tap_debug("Supervisor restarted database connection: ")

        wait_for_restart()

        restarted_database_pid = get_database_pid(supervisor_pid)

        assert is_pid(restarted_database_pid)
        refute database_pid == restarted_database_pid
        assert_update_user(restarted_database_pid)

        assert [@replication_slot | _replication_slots] =
                 pg_replication_slots(restarted_database_pid)
      end)
    end

    test "should re-initiate after database connection terminated" do
      capture_log(fn ->
        assert {:ok, supervisor_pid} = start_walex_supervised(@base_configs, TestSupervisor)
        database_pid = get_database_pid(supervisor_pid)

        assert {:error, error} =
                 terminate_database_connection(database_pid, @pg_config[:username])

        assert match?(%DBConnection.ConnectionError{reason: :closed}, error) or
                 match?(%Error{postgres: %{code: :admin_shutdown}}, error)

        assert_update_user(database_pid)

        assert [@replication_slot | _replication_slots] = pg_replication_slots(database_pid)
      end)
    end

    @tag :pg_restart
    test "should re-initiate after database restart" do
      capture_log(fn ->
        assert {:ok, supervisor_pid} = start_walex_supervised(@base_configs, TestSupervisor)
        database_pid = get_database_pid(supervisor_pid)

        assert is_pid(database_pid)
        assert [@replication_slot | _replication_slots] = pg_replication_slots(database_pid)
        started_before = postmaster_start_time(database_pid)

        assert Process.exit(database_pid, :kill)
               |> tap_debug("Forcefully killed database connection: ")

        assert :ok == pg_restart()

        new_database_pid = get_database_pid(supervisor_pid)

        assert is_pid(new_database_pid)
        refute database_pid == new_database_pid
        # A fast shutdown returns before the pool has reconnected, so wait for it
        # instead of racing the restart.
        wait_until(fn -> slot_active?(new_database_pid) end, "the slot to become active again")

        # Without this, restarting some other server would pass the test.
        assert postmaster_start_time(new_database_pid) != started_before
        assert_update_user(new_database_pid)
      end)
    end

    test "durable replication slot", %{database_pid: database_pid} do
      capture_log(fn ->
        assert [] = pg_replication_slots(database_pid)

        slot_name = "walex_test_durable"
        durable_opts = Keyword.merge(@base_configs, durable_slot: true, slot_name: slot_name)

        durable_slot = %{
          "active" => true,
          "slot_name" => slot_name,
          "slot_type" => "logical",
          "temporary" => false
        }

        inactive_slot = %{durable_slot | "active" => false}

        start_supervised!({WalExSupervisor, durable_opts},
          restart: :temporary,
          id: :ok_supervisor
        )

        assert [^durable_slot] = pg_replication_slots(database_pid)

        other_app_opts = Keyword.replace!(durable_opts, :name, :other_app)

        # Start another supervisor with the same slot name
        {:ok, pid} =
          start_supervised({WalExSupervisor, other_app_opts},
            restart: :temporary,
            id: :other_app_supervisor
          )

        # Wait for the retry mechanism to complete
        Process.sleep(10_000)

        # Check that the other app is still running and waiting
        assert Process.alive?(pid)

        # The original slot should still be active
        assert [^durable_slot] = pg_replication_slots(database_pid)

        # Stop the first supervisor
        stop_supervised(:ok_supervisor)

        # Sleep to make sure that Postgres detects that the connection is closed
        # and the second supervisor has time to activate the slot
        Process.sleep(10_000)

        # The slot should now be active under the second supervisor
        assert [^durable_slot] = pg_replication_slots(database_pid)

        # Clean up
        stop_supervised(:other_app_supervisor)

        # Wait for the slot to become inactive after stopping all supervisors
        Process.sleep(5_000)
        [slot] = pg_replication_slots(database_pid)
        assert slot == inactive_slot
      end)
    end
  end

  def pg_restart do
    case pg_restart_method() do
      :docker ->
        Logger.debug("Restarting docker postgres.")
        pg_restart(:docker)

      :system ->
        Logger.debug("Restarting system postgres.")
        pg_restart(:system)
    end
  end

  def pg_restart(:docker) do
    case(System.shell("docker compose -f docker-compose.dbs.yml restart db -t 2")) do
      {_, 0} ->
        :ok

      {output, _} ->
        Logger.error("Error restarting PostgreSQL via docker-compose: #{inspect(output)}")
        raise "Error restarting PostgreSQL via docker-compose."
    end
  end

  def pg_restart(:system) do
    case :os.type() do
      {:unix, :darwin} ->
        Logger.debug("MacOS detected.")

        restart_postgres()

      {:unix, :linux} ->
        Logger.debug("Linux detected.")

        restart_postgres()

      other ->
        Logger.debug("Unsupported operating system: #{inspect(other)}")
        :ok
    end
  end

  defp pg_ctl_path do
    case pg_installation_type() do
      nil ->
        raise "PostgreSQL not installed via Postgres.app or homebrew."

      type ->
        Logger.debug("PostgreSQL installation detected: #{type}.")
        pg_installation_path(type)
    end
  end

  defp pg_data_directory(version) do
    postgres_data_directory =
      case pg_installation_type() do
        :linux ->
          "/var/lib/postgresql/#{version}/main/"

        :mac_homebrew ->
          "/usr/local/var/postgres-#{version}"

        :mac_apple_silicon_homebrew ->
          "/opt/homebrew/var/postgresql"

        :mac_app ->
          System.user_home!() <> "/Library/Application\ Support/Postgres/var-#{version}"
      end

    if File.exists?(postgres_data_directory) do
      postgres_data_directory
    else
      raise "pg data directory not found. Make sure PostgreSQL is installed correctly."
    end
  end

  defp pg_bin_path(postgres_path, version) do
    postgres_bin_path = Path.join([postgres_path, version, "bin", "pg_ctl"])

    if File.exists?(postgres_bin_path) do
      postgres_bin_path
    else
      raise "pg_ctl binary not found. Make sure PostgreSQL is installed correctly."
    end
  end

  defp restart_postgres do
    postgres_path = pg_ctl_path()
    version = pg_version(postgres_path)
    postgres_bin_path = pg_bin_path(postgres_path, version)
    data_directory = pg_data_directory(version)

    case pg_stop(postgres_bin_path, data_directory) do
      {:ok,
       %Rambo{
         status: 0,
         out: _message,
         err: ""
       }} ->
        unless pg_isready?() do
          pg_start(postgres_bin_path, data_directory)
          Logger.debug("Waiting after pg_start")
          :timer.sleep(4000)
          pg_isready?()
        end

      {:error,
       %Rambo{
         status: 1,
         out: "",
         err: error
       }} ->
        Logger.debug("PostgreSQL not stopped: " <> inspect(error))

      _ ->
        Logger.debug("PostgreSQL not stopped.")
    end

    :ok
  end

  defp pg_stop(postgres_bin_path, data_directory) do
    Logger.debug("PostgreSQL stopping.")

    case pg_installation_type() do
      :mac_apple_silicon_homebrew ->
        Rambo.run("brew", ["services", "stop", "postgresql"])

      _ ->
        Rambo.run(postgres_bin_path, [
          "stop",
          "-m",
          "immediate",
          "-D",
          "#{data_directory}"
        ])
    end
  end

  defp pg_start(postgres_bin_path, data_directory) do
    Logger.debug("PostgreSQL starting.")

    case pg_installation_type() do
      :mac_apple_silicon_homebrew ->
        Rambo.run("brew", ["services", "start", "postgresql"])

      _ ->
        # For some reason starting pg hangs so we run in async as not to block...
        Task.async(fn ->
          Rambo.run(
            postgres_bin_path,
            [
              "start",
              "-D",
              "#{data_directory}"
            ]
          )
        end)
    end
  end

  defp pg_isready? do
    # The exit status says it all; the output embeds the host and port.
    case Rambo.run("pg_isready", [
           "-h",
           @pg_config[:hostname],
           "-p",
           Integer.to_string(@pg_config[:port])
         ]) do
      {:ok, %Rambo{status: 0}} ->
        Logger.debug("PostgreSQL is running.")
        true

      error ->
        Logger.debug("PostgreSQL is not running: " <> inspect(error))
        false
    end
  end

  defp pg_version(postgres_path) do
    case Rambo.run("ls", [postgres_path]) do
      {:ok, %Rambo{status: 0, out: versions, err: ""}} when is_binary(versions) ->
        versions
        |> String.split("\n")
        |> Enum.filter(&String.match?(&1, ~r/^[0-9._]+$/))
        |> Enum.max()

      _error ->
        raise "PostgreSQL version not found."
    end
  end

  defp start_database do
    Postgrex.start_link(
      hostname: @pg_config[:hostname],
      username: @pg_config[:username],
      password: @pg_config[:password],
      database: @database,
      port: @pg_config[:port]
    )
  end

  defp postmaster_start_time(database_pid) do
    [%{"started" => started}] =
      query(database_pid, "SELECT pg_postmaster_start_time() AS started")

    started
  end

  defp slot_active?(database_pid) do
    match?(
      {:ok, %{rows: [[true]]}},
      Postgrex.query(
        database_pid,
        "SELECT active FROM pg_replication_slots WHERE slot_name = 'todos_walex'",
        []
      )
    )
  end

  defp assert_update_user(database_pid) do
    capture_log =
      capture_log(fn ->
        update_user(database_pid)

        :timer.sleep(1000)
      end)

    assert capture_log =~ "on_update event occurred"
    assert capture_log =~ "%WalEx.Event"
  end
end

defmodule TestSupervisor do
  use Supervisor

  alias WalEx.Supervisor, as: WalExSupervisor

  def start_link(configs) do
    Supervisor.start_link(__MODULE__, configs, name: __MODULE__)
  end

  def init(configs) do
    children = [
      {Postgrex, configs},
      {WalExSupervisor, configs}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule TestModule do
  require Logger
  use WalEx.Event, name: :todos

  on_update(
    :user,
    [],
    fn events -> Logger.info("on_update event occurred: #{inspect(events, pretty: true)}") end
  )
end
