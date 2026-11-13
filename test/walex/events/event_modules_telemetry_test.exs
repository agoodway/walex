defmodule WalEx.Events.EventModulesTelemetryTest do
  use ExUnit.Case, async: false

  alias WalEx.Changes.Transaction
  alias WalEx.Config
  alias WalEx.Events.EventModules

  @hostname System.get_env("PGHOST", "localhost")
  @username System.get_env("PGUSER", "postgres")
  @password System.get_env("PGPASSWORD", "postgres")
  @port String.to_integer(System.get_env("PGPORT", "5432"))

  defmodule Succeeding do
    def process_all(_txn), do: :ok
  end

  defmodule Failing do
    def process_all(_txn), do: raise("handler failed")
  end

  setup do
    start_supervised!(WalEx.Config.Registry.child_spec())
    test_pid = self()
    id = "event-handler-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      id,
      [
        [:walex, :event_handler, :start],
        [:walex, :event_handler, :stop],
        [:walex, :event_handler, :exception]
      ],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp start_app!(app, modules) do
    {:ok, _} =
      Config.start_link(
        configs: [
          name: app,
          hostname: @hostname,
          username: @username,
          password: @password,
          database: "postgres",
          port: @port,
          ssl: false,
          ssl_opts: [verify: :verify_none],
          publication: "publication",
          subscriptions: ["subscriptions"],
          modules: modules
        ]
      )

    start_supervised!({EventModules, app_name: app})
  end

  defp txn, do: %Transaction{changes: [], commit_timestamp: ~U[2025-01-01 00:00:00Z]}

  test "a successful handler call emits start and stop events" do
    start_app!(:handler_ok_app, [Succeeding])

    assert :ok = EventModules.process(txn(), :handler_ok_app)

    assert_receive {:telemetry, [:walex, :event_handler, :start], %{system_time: _},
                    %{app_name: :handler_ok_app, module: Succeeding, function: :process_all}}

    assert_receive {:telemetry, [:walex, :event_handler, :stop], %{duration: _},
                    %{app_name: :handler_ok_app, module: Succeeding, function: :process_all}}
  end

  test "a failing handler emits an exception event and still raises" do
    start_app!(:handler_fail_app, [Failing])

    assert {{%RuntimeError{message: "handler failed"}, _stack}, _call} =
             catch_exit(EventModules.process(txn(), :handler_fail_app))

    assert_receive {:telemetry, [:walex, :event_handler, :exception], %{duration: _},
                    %{
                      app_name: :handler_fail_app,
                      module: Failing,
                      function: :process_all,
                      kind: :error,
                      reason: %RuntimeError{message: "handler failed"}
                    }}
  end
end
