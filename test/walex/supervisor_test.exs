defmodule WalEx.SupervisorTest do
  use ExUnit.Case, async: false
  import WalEx.Support.TestHelpers
  import ExUnit.CaptureLog

  alias WalEx.{Changes.Transaction, Config, Events, Events.EventModules}
  alias WalEx.Events.Supervisor, as: EventsSupervisor
  alias WalEx.Replication
  alias WalEx.Supervisor, as: WalExSupervisor

  defmodule Handler do
    def process_all(_txn), do: :ok
  end

  @base_configs [
    name: :test_name,
    hostname: "hostname",
    username: "username",
    password: "password",
    database: "todos_test",
    port: 5432,
    subscriptions: ["subscriptions"],
    publication: "publication"
  ]

  describe "child specs" do
    test "the events supervisor is not handed the database password or signing secret" do
      opts = @base_configs ++ [webhook_signing_secret: "s3cr3t-signing"]

      {:ok, {_flags, specs}} = WalExSupervisor.init(opts)
      %{start: start} = Enum.find(specs, &(&1.id == EventsSupervisor))

      refute inspect(start) =~ "password"
      refute inspect(start) =~ "s3cr3t-signing"
    end
  end

  describe "start_link/2" do
    test "should start Supervisor and child processes" do
      capture_log(fn ->
        assert {:ok, walex_supervisor_pid} = start_walex_supervised(@base_configs)
        assert is_pid(walex_supervisor_pid)

        assert %{active: 3, specs: 3, supervisors: 2, workers: 1} =
                 Supervisor.count_children(walex_supervisor_pid)

        replication_supervisor_pid =
          find_child_pid(walex_supervisor_pid, Replication.Supervisor)

        assert is_pid(replication_supervisor_pid)

        replication_publisher_pid =
          find_child_pid(replication_supervisor_pid, Replication.Publisher)

        assert is_pid(replication_publisher_pid)

        replication_server_pid =
          find_child_pid(replication_supervisor_pid, Replication.Server)

        assert is_pid(replication_server_pid)
      end)
    end

    test "should raise if any required config is missing" do
      assert_raise RuntimeError,
                   "Following required configs are missing: [:hostname, :username, :password, :port, :database, :subscriptions, :publication, :name]",
                   fn -> WalExSupervisor.start_link([]) end
    end

    test "should start multiple supervision trees" do
      capture_log(fn ->
        configs_1 = @base_configs
        configs_2 = Keyword.put(configs_1, :name, :other_name)
        configs_3 = Keyword.put(configs_1, :name, :another_name)

        assert {:ok, pid_1} = start_walex_supervised(configs_1)
        assert {:ok, pid_2} = start_walex_supervised(configs_2)
        assert {:ok, pid_3} = start_walex_supervised(configs_3)

        assert is_pid(pid_1)
        assert is_pid(pid_2)
        assert is_pid(pid_3)
      end)
    end

    # Without ack_after_processing a restart of replication drops the temporary
    # slot, so it is left alone, as it always was.
    test "without ack_after_processing, replication survives an events supervisor restart" do
      capture_log(fn ->
        {:ok, walex_supervisor_pid} = start_walex_supervised(@base_configs)
        events_pid = find_child_pid(walex_supervisor_pid, EventsSupervisor)
        replication_pid = find_child_pid(walex_supervisor_pid, Replication.Supervisor)

        ref = Process.monitor(events_pid)
        Process.exit(events_pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^events_pid, _reason}

        wait_until(
          fn ->
            find_child_pid(walex_supervisor_pid, EventsSupervisor) not in [
              events_pid,
              :restarting,
              :undefined
            ]
          end,
          "the events supervisor to restart"
        )

        assert replication_pid == find_child_pid(walex_supervisor_pid, Replication.Supervisor)
      end)
    end

    # Replication depends on the event processes, so with ack_after_processing
    # it restarts with them.
    test "with ack_after_processing, restarts replication when the events supervisor restarts" do
      configs =
        Keyword.merge(@base_configs,
          name: :ack_rest_for_one,
          durable_slot: true,
          ack_after_processing: true,
          modules: [Handler]
        )

      capture_log(fn ->
        {:ok, walex_supervisor_pid} = start_walex_supervised(configs)
        events_pid = find_child_pid(walex_supervisor_pid, EventsSupervisor)
        replication_pid = find_child_pid(walex_supervisor_pid, Replication.Supervisor)

        ref = Process.monitor(replication_pid)
        Process.exit(events_pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^replication_pid, _reason}, 5_000

        wait_until(
          fn -> is_pid(find_child_pid(walex_supervisor_pid, Replication.Supervisor)) end,
          "the replication supervisor to restart"
        )

        assert Process.alive?(walex_supervisor_pid)
      end)
    end

    test "with ack_after_processing, fails to start when a listed module cannot be loaded" do
      configs =
        Keyword.merge(@base_configs,
          name: :missing_module_sup,
          durable_slot: true,
          ack_after_processing: true,
          modules: [WalEx.SupervisorTest.NoSuchModule]
        )

      capture_log(fn ->
        assert {:error, reason} = start_walex_supervised(configs)
        assert inspect(reason) =~ "NoSuchModule"
      end)
    end

    test "with ack_after_processing, fails to start when no event module resolves" do
      configs =
        Keyword.merge(@base_configs,
          name: :no_module_sup,
          durable_slot: true,
          ack_after_processing: true
        )

      capture_log(fn ->
        assert {:error, reason} = start_walex_supervised(configs)
        assert inspect(reason) =~ "no event handler module"
      end)
    end

    test "without ack_after_processing, starts with no event module" do
      capture_log(fn ->
        assert {:ok, _pid} = start_walex_supervised(Keyword.put(@base_configs, :name, :no_mod))
      end)
    end

    test "without modules, a module added later is still delivered to" do
      capture_log(fn ->
        assert {:ok, pid} = start_walex_supervised(Keyword.put(@base_configs, :name, :late_mod))

        events_supervisor = find_child_pid(pid, EventsSupervisor)
        assert is_pid(find_child_pid(events_supervisor, EventModules))

        Config.add_config(:late_mod, :modules, [Handler])

        txn = %Transaction{
          changes: [:change],
          commit_timestamp: ~U[2025-01-01 00:00:00Z]
        }

        assert :ok = Events.process(txn, :late_mod)
      end)
    end

    test "should start WalEx.Registry" do
      capture_log(fn ->
        assert {:ok, _pid} = start_walex_supervised(@base_configs)
        assert is_pid(Process.whereis(:walex_registry))
      end)
    end
  end
end
