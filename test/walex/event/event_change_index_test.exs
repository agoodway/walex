defmodule WalEx.EventChangeIndexTest do
  use ExUnit.Case, async: false

  alias WalEx.{Changes, Config, Config.Registry, Event}

  @app :event_change_index_app

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

  defp change(struct, type, index),
    do:
      struct(struct,
        type: type,
        table: "todos",
        schema: "public",
        columns: [],
        record: %{},
        old_record: %{},
        lsn: {0, 0x10},
        change_index: index
      )

  test "every cast change carries its change_index and lsn" do
    events =
      Event.cast_events(
        [
          change(Changes.DeletedRecord, "DELETE", 2),
          change(Changes.UpdatedRecord, "UPDATE", 1),
          change(Changes.NewRecord, "INSERT", 0)
        ],
        @app
      )

    assert [{:delete, 2}, {:update, 1}, {:insert, 0}] =
             Enum.map(events, &{&1.type, &1.change_index})

    assert Enum.all?(events, &(&1.lsn == {0, 0x10}))
  end
end
