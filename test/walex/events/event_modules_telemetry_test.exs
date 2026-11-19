defmodule WalEx.Events.EventModulesTelemetryTest do
  use ExUnit.Case, async: false

  alias WalEx.Changes.Transaction
  alias WalEx.{Config, Config.Registry}
  alias WalEx.Events.EventModules

  import ExUnit.CaptureLog
  import WalEx.Support.TestHelpers, only: [attach_telemetry: 1, start_config!: 2]

  defmodule Succeeding do
    def process_all(_txn), do: :ok
  end

  defmodule Failing do
    def process_all(_txn), do: raise("handler failed")
  end

  # Reports every call, so tests can see which handlers ran.
  defmodule Recording do
    def process_all(_txn), do: send(:event_modules_test, {:ran, __MODULE__, :process_all})
    def process_insert(_txn), do: send(:event_modules_test, {:ran, __MODULE__, :process_insert})
  end

  defmodule Throwing do
    def process_all(_txn), do: throw(:handler_threw)
  end

  setup do
    start_supervised!(Registry.child_spec())

    attach_telemetry([
      [:walex, :event_handler, :start],
      [:walex, :event_handler, :stop],
      [:walex, :event_handler, :exception]
    ])
  end

  defp start_app!(app, modules, extra \\ []) do
    start_config!(app, [modules: modules] ++ extra)

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

  describe "with ack_after_processing" do
    @ack [ack_after_processing: true, durable_slot: true]

    setup do
      Process.register(self(), :event_modules_test)
      :ok
    end

    test "a module that cannot be loaded is a handler failure, not a skipped handler" do
      start_app!(:handler_ack_missing_app, [Succeeding], @ack)
      # add_config/3 does not check that the module exists.
      Config.add_config(:handler_ack_missing_app, :modules, WalEx.DoesNotExist)

      capture_log(fn ->
        assert {:error, {:handler_failed, %{module: WalEx.DoesNotExist}}} =
                 EventModules.process(txn(), :handler_ack_missing_app)
      end)
    end

    test "a raising handler is an error reply, and the exception event still fires" do
      start_app!(:handler_ack_fail_app, [Failing], @ack)

      log =
        capture_log(fn ->
          assert {:error,
                  {:handler_failed,
                   %{
                     module: Failing,
                     function: :process_all,
                     kind: :error,
                     exception: RuntimeError
                   }}} = EventModules.process(txn(), :handler_ack_fail_app)
        end)

      # Logged once here, with the stacktrace that only this process has.
      assert log =~ "handler failed"
      assert log =~ "event_modules_telemetry_test.exs"

      assert_receive {:telemetry, [:walex, :event_handler, :exception], %{duration: _},
                      %{module: Failing, function: :process_all, kind: :error}}
    end

    test "EventModules stays up after a handler raises" do
      pid = start_app!(:handler_ack_alive_app, [Failing], @ack)

      capture_log(fn ->
        assert {:error, _} = EventModules.process(txn(), :handler_ack_alive_app)
        assert {:error, _} = EventModules.process(txn(), :handler_ack_alive_app)
      end)

      assert Process.alive?(pid)
    end

    test "a throw is reported with its kind" do
      start_app!(:handler_ack_throw_app, [Throwing], @ack)

      capture_log(fn ->
        assert {:error, {:handler_failed, %{kind: :throw, exception: nil} = details}} =
                 EventModules.process(txn(), :handler_ack_throw_app)

        # Structural only: no message or thrown value, which can carry row data.
        assert [:exception, :function, :kind, :module] == details |> Map.keys() |> Enum.sort()
      end)
    end

    test "no further handlers run for the transaction after one fails" do
      start_app!(:handler_ack_stop_app, [Failing, Recording], @ack)

      capture_log(fn ->
        assert {:error, _} = EventModules.process(txn(), :handler_ack_stop_app)
      end)

      refute_received {:ran, _, _}
    end

    test "every handler runs when none fails" do
      start_app!(:handler_ack_ok_app, [Recording, Succeeding], @ack)

      assert :ok = EventModules.process(txn(), :handler_ack_ok_app)
      assert_received {:ran, Recording, :process_all}
      assert_received {:ran, Recording, :process_insert}
    end
  end
end
