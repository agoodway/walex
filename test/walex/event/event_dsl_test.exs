defmodule WalEx.EventDslTest do
  use ExUnit.Case, async: false
  import WalEx.Support.TestHelpers
  import ExUnit.CaptureLog

  alias WalEx.Events.EventModules, as: EventsEventModules
  alias WalEx.Events.Supervisor, as: EventsSupervisor

  @app_name :test_app
  @pg_config pg_config()
  @database "todos_test"

  @dsl_base_configs [
    name: @app_name,
    hostname: @pg_config[:hostname],
    username: @pg_config[:username],
    password: @pg_config[:password],
    database: @database,
    port: @pg_config[:port],
    subscriptions: ["user", "todo"],
    publication: ["events"],
    modules: [TestApp.DslTestModule, TestApp.DslNoFilterModule]
  ]

  describe "on_event/2" do
    setup do
      {{:ok, database_pid}, _log} = with_log(fn -> start_database(@dsl_base_configs) end)

      {{:ok, supervisor_pid}, _log} =
        with_log(fn -> start_walex_supervised(@dsl_base_configs) end)

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
        capture_log(fn ->
          update_user(database_pid)

          :timer.sleep(1000)
        end)

      assert capture_log =~ "on_event event occurred"
      assert capture_log =~ "%WalEx.Event"
    end
  end

  describe "on_event/2 without filters" do
    test "on_event, on_insert, on_update and on_delete compile without explicit filters" do
      source = """
      defmodule TestApp.DslCompileNoFilters do
        use WalEx.Event, name: :test_app

        on_event(:user, fn events -> events end)
        on_insert(:user, fn events -> events end)
        on_update(:user, fn events -> events end)
        on_delete(:user, fn events -> events end)
      end
      """

      assert [{TestApp.DslCompileNoFilters, _bin}] = Code.compile_string(source)
    end

    test "the filterless handler receives events" do
      {{:ok, database_pid}, _log} = with_log(fn -> start_database(@dsl_base_configs) end)
      {{:ok, _pid}, _log} = with_log(fn -> start_walex_supervised(@dsl_base_configs) end)

      capture_log =
        capture_log(fn ->
          update_user(database_pid)

          :timer.sleep(1000)
        end)

      assert capture_log =~ "filterless on_event occurred: 1"
    end
  end

  describe "on_update/4" do
    setup do
      {{:ok, database_pid}, _log} = with_log(fn -> start_database(@dsl_base_configs) end)

      {{:ok, supervisor_pid}, _log} =
        with_log(fn -> start_walex_supervised(@dsl_base_configs) end)

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
        capture_log(fn ->
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

defmodule TestApp.DslNoFilterModule do
  require Logger
  use WalEx.Event, name: :test_app

  on_event(:user, fn events ->
    Logger.info("filterless on_event occurred: #{length(events)}")
  end)
end
