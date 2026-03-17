defmodule WalEx.EventDslTest do
  use ExUnit.Case, async: false
  import WalEx.Support.TestHelpers
  import ExUnit.CaptureLog

  alias WalEx.Events.EventModules, as: EventsEventModules
  alias WalEx.Events.Supervisor, as: EventsSupervisor
  alias WalEx.Supervisor, as: WalExSupervisor

  @app_name :test_app
  @hostname System.get_env("PGHOST", "localhost")
  @username System.get_env("PGUSER", "postgres")
  @password System.get_env("PGPASSWORD", "postgres")
  @database "todos_test"
  @port String.to_integer(System.get_env("PGPORT", "5432"))

  @dsl_base_configs [
    name: @app_name,
    hostname: @hostname,
    username: @username,
    password: @password,
    database: @database,
    port: @port,
    subscriptions: ["user", "todo"],
    publication: ["events"],
    modules: [TestApp.DslTestModule]
  ]

  describe "on_event/2" do
    setup do
      {{:ok, database_pid}, _log} = with_log(fn -> start_database(@dsl_base_configs) end)

      {{:ok, supervisor_pid}, _log} =
        with_log(fn -> WalExSupervisor.start_link(@dsl_base_configs) end)

      %{database_pid: database_pid, supervisor_pid: supervisor_pid}
    end

    test "should receive and return all Events", %{
      supervisor_pid: supervisor_pid,
      database_pid: database_pid
    } do
      destinations_supervisor_pid = find_child_pid(supervisor_pid, EventsSupervisor)

      assert is_pid(destinations_supervisor_pid)

      events_pid =
        find_child_pid(destinations_supervisor_pid, EventsEventModules)

      assert is_pid(events_pid)

      capture_log =
        ExUnit.CaptureLog.capture_log(fn ->
          update_user(database_pid)

          :timer.sleep(1000)
        end)

      assert capture_log =~ "on_event event occurred"
      assert capture_log =~ "%WalEx.Event"
    end
  end

  describe "on_update/4" do
    setup do
      {{:ok, database_pid}, _log} = with_log(fn -> start_database(@dsl_base_configs) end)

      {{:ok, supervisor_pid}, _log} =
        with_log(fn -> WalExSupervisor.start_link(@dsl_base_configs) end)

      %{database_pid: database_pid, supervisor_pid: supervisor_pid}
    end

    test "should receive and return 'user' update Events", %{
      supervisor_pid: supervisor_pid,
      database_pid: database_pid
    } do
      destinations_supervisor_pid = find_child_pid(supervisor_pid, EventsSupervisor)

      assert is_pid(destinations_supervisor_pid)

      events_pid =
        find_child_pid(destinations_supervisor_pid, EventsEventModules)

      assert is_pid(events_pid)

      capture_log =
        ExUnit.CaptureLog.capture_log(fn ->
          update_user(database_pid)

          :timer.sleep(1000)
        end)

      assert capture_log =~ "on_update event occurred"
      assert capture_log =~ "%WalEx.Event"
    end
  end
end

defmodule TestApp.DslTestModule do
  require Logger
  use WalEx.Event, name: :test_app

  on_event(
    :all,
    fn events -> Logger.info("on_event event occurred: #{inspect(events, pretty: true)}") end
  )

  on_update(
    :user,
    [],
    fn events -> Logger.info("on_update event occurred: #{inspect(events, pretty: true)}") end
  )
end
