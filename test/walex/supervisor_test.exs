defmodule WalEx.SupervisorTest do
  use ExUnit.Case, async: false
  import WalEx.Support.TestHelpers
  import ExUnit.CaptureLog

  alias WalEx.Supervisor, as: WalExSupervisor
  alias WalEx.Replication

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

  describe "start_link/2" do
    test "should start Supervisor and child processes" do
      capture_log(fn ->
        assert {:ok, walex_supervisor_pid} = WalExSupervisor.start_link(@base_configs)
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

        assert {:ok, pid_1} = WalExSupervisor.start_link(configs_1)
        assert {:ok, pid_2} = WalExSupervisor.start_link(configs_2)
        assert {:ok, pid_3} = WalExSupervisor.start_link(configs_3)

        assert is_pid(pid_1)
        assert is_pid(pid_2)
        assert is_pid(pid_3)
      end)
    end

    test "should start WalEx.Registry" do
      capture_log(fn ->
        assert {:ok, _pid} = WalExSupervisor.start_link(@base_configs)
        assert is_pid(Process.whereis(:walex_registry))
      end)
    end
  end
end
