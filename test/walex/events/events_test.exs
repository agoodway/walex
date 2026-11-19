defmodule WalEx.EventsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import WalEx.Support.TestHelpers, only: [start_config!: 2]

  alias WalEx.{Changes.Transaction, Config.Registry, Events, Events.EventModules}

  # Fails through a linked process rather than by raising, which kills
  # EventModules despite the catch around each handler.
  defmodule LinkedCrash do
    def process_all(_txn) do
      spawn_link(fn -> exit(:linked_crash) end)
      Process.sleep(1_000)
    end
  end

  # Kills the EventModules worker itself mid-handler.
  defmodule KillsWorker do
    def process_all(_txn), do: Process.exit(self(), :kill)
  end

  defmodule Succeeding do
    def process_all(_txn), do: :ok
  end

  @ack [ack_after_processing: true, durable_slot: true]

  setup do
    start_supervised!(Registry.child_spec())
    :ok
  end

  defp txn, do: %Transaction{changes: [:change], commit_timestamp: ~U[2025-01-01 00:00:00Z]}

  defp start_events!(app, extra) do
    start_config!(app, extra)
    events = start_supervised!({Events, app_name: app})

    if Keyword.get(extra, :modules, []) != [],
      do: start_supervised!({EventModules, app_name: app})

    events
  end

  describe "a handler that fails through a linked process" do
    test "with ack_after_processing is a handler failure, and Events stays up" do
      events = start_events!(:events_linked_ack, [modules: [LinkedCrash]] ++ @ack)

      capture_log(fn ->
        assert {:error, {:handler_failed, %{kind: :exit}}} =
                 Events.process(txn(), :events_linked_ack)
      end)

      assert Process.alive?(events)
    end

    test "with ack_after_processing, the worker being killed is a handler failure" do
      events = start_events!(:events_killed_ack, [modules: [KillsWorker]] ++ @ack)

      capture_log(fn ->
        assert {:error, {:handler_failed, %{kind: :exit}}} =
                 Events.process(txn(), :events_killed_ack)
      end)

      assert Process.alive?(events)
    end

    test "without ack_after_processing still crashes, as it always has" do
      start_events!(:events_linked_default, modules: [LinkedCrash])

      capture_log(fn -> catch_exit(Events.process(txn(), :events_linked_default)) end)
    end
  end

  describe "with no modules configured" do
    test "a transaction is delivered to nobody, successfully" do
      events = start_events!(:events_no_modules, [modules: []] ++ @ack)

      assert :ok = Events.process(txn(), :events_no_modules)
      assert Process.alive?(events)
    end

    test "without ack_after_processing too" do
      start_events!(:events_no_modules_default, modules: [])

      assert :ok = Events.process(txn(), :events_no_modules_default)
    end
  end

  test "a configured module is still called" do
    start_events!(:events_with_module, [modules: [Succeeding]] ++ @ack)

    assert :ok = Events.process(txn(), :events_with_module)
  end
end
