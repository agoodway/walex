defmodule WalEx.EventTruncateTest do
  use ExUnit.Case, async: false

  alias WalEx.{Changes, Config, Config.Registry, Event}

  @app :event_truncate_app

  defmodule AllHandler do
    use WalEx.Event, name: :event_truncate_app

    on_event(:all, fn events -> {:all, events} end)
  end

  defmodule TableHandler do
    use WalEx.Event, name: :event_truncate_app

    on_event(:todos, %{}, [], fn events -> {:todos, events} end)
  end

  setup do
    start_supervised!(Registry.child_spec())

    {:ok, _} =
      Config.start_link(
        configs: [
          name: @app,
          hostname: "localhost",
          username: "postgres",
          password: "postgres",
          database: "postgres",
          port: 5432,
          publication: "publication",
          subscriptions: ["todos"]
        ]
      )

    :ok
  end

  # TRUNCATE has no Event representation, so it must not reach handlers as nil.
  defp txn_with_truncate do
    %Changes.Transaction{
      commit_timestamp: nil,
      changes: [
        %Changes.TruncatedRelation{
          type: "TRUNCATE",
          schema: "public",
          table: "todos",
          lsn: {0, 0x10},
          change_index: 1
        },
        %Changes.NewRecord{
          type: "INSERT",
          schema: "public",
          table: "todos",
          columns: [],
          record: %{id: 1},
          lsn: {0, 0x10},
          change_index: 0
        }
      ]
    }
  end

  defp truncate_only_txn do
    %Changes.Transaction{txn_with_truncate() | changes: [hd(txn_with_truncate().changes)]}
  end

  test "on_event(:all) receives no nil for a truncate" do
    assert {:all, [%Event{type: :insert}]} = AllHandler.process_all(txn_with_truncate())
    assert {:error, :no_events} = AllHandler.process_all(truncate_only_txn())
  end

  test "on_event(table) receives no nil for a truncate" do
    assert {:todos, [%Event{type: :insert}]} =
             TableHandler.process_all(txn_with_truncate())

    assert {:error, :no_events} = TableHandler.process_all(truncate_only_txn())
  end
end
