defmodule WalEx.Replication.PublisherTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias WalEx.{Config, Config.Registry, Events, Events.EventModules}
  alias WalEx.Decoder.Messages
  alias WalEx.Replication.Publisher

  @app :publisher_test_app
  @ts ~U[2025-01-01 00:00:00Z]

  setup context do
    start_supervised!(Registry.child_spec())

    # Tag a test (or describe) with `ack: true` to run under ack_after_processing.
    ack_configs = if context[:ack], do: [ack_after_processing: true, durable_slot: true], else: []

    {:ok, _} =
      Config.start_link(
        configs:
          [
            name: @app,
            hostname: "localhost",
            username: "postgres",
            password: "postgres",
            database: "postgres",
            port: 5432,
            publication: "publication",
            subscriptions: ["subscriptions"],
            modules: [__MODULE__.Handler]
          ] ++ ack_configs
      )

    start_supervised!({Publisher, app_name: @app})
    :ok
  end

  defmodule Handler do
    def process_all(_txn), do: send(:publisher_test_dispatch, :dispatched)
  end

  defp commit(lsn), do: %Messages.Commit{flags: [], lsn: lsn, end_lsn: lsn, commit_timestamp: @ts}
  defp begin_msg(lsn), do: %Messages.Begin{final_lsn: lsn, commit_timestamp: @ts, xid: 1}

  defp insert_msg(relation_id),
    do: %Messages.Insert{relation_id: relation_id, tuple_data: {"1"}}

  defp update_msg(relation_id),
    do: %Messages.Update{relation_id: relation_id, old_tuple_data: {"1"}, tuple_data: {"2"}}

  defp delete_msg(relation_id),
    do: %Messages.Delete{relation_id: relation_id, old_tuple_data: {"1"}}

  defp truncate_msg(relation_ids),
    do: %Messages.Truncate{truncated_relations: relation_ids, number_of_relations: 1, options: []}

  defp change_msg(:insert, id), do: insert_msg(id)
  defp change_msg(:update, id), do: update_msg(id)
  defp change_msg(:delete, id), do: delete_msg(id)
  defp change_msg(:truncate, id), do: truncate_msg([id])

  # One column, matching the one-value tuples of the change messages above.
  defp relation_msg(id, columns \\ [%Messages.Relation.Column{name: "id", type: "int4"}]),
    do: %Messages.Relation{
      id: id,
      namespace: "public",
      name: "todos",
      replica_identity: :default,
      columns: columns
    }

  defp publisher_pid, do: GenServer.whereis(Registry.set_name(:set_gen_server, Publisher, @app))

  # :sys works with the registry's via tuple, and also waits for queued casts.
  defp publisher_state, do: :sys.get_state(Registry.set_name(:set_gen_server, Publisher, @app))

  describe "a COMMIT that does not match the open transaction" do
    @tag ack: true
    test "is an error and logs a warning" do
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      log =
        capture_log(fn ->
          assert {:error, {:commit_mismatch, %{commit_lsn: {1, 0xFF}, open_lsn: {0, 0x10}}}} =
                   Publisher.process_message_sync(commit({1, 0xFF}), @app)
        end)

      assert log =~ "Dropping COMMIT at 1/FF: the open transaction is at 0/10"
    end

    @tag ack: true
    test "with no open transaction is an error and logs a warning" do
      log =
        capture_log(fn ->
          assert {:error, {:commit_mismatch, %{commit_lsn: {0, 0x20}, open_lsn: nil}}} =
                   Publisher.process_message_sync(commit({0, 0x20}), @app)
        end)

      assert log =~ "Dropping COMMIT at 0/20: no transaction is open"
    end

    @tag ack: true
    test "clears the open transaction" do
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      capture_log(fn ->
        {:error, _} = Publisher.process_message_sync(commit({1, 0xFF}), @app)
      end)

      assert %{transaction: nil} = publisher_state()
    end

    test "is only logged when processed asynchronously" do
      log =
        capture_log(fn ->
          Publisher.process_message_async(commit({0, 0x20}), @app)
          publisher_state()
        end)

      assert log =~ "Dropping COMMIT at 0/20"
    end
  end

  describe "a change for an unknown relation" do
    for kind <- [:insert, :update, :delete, :truncate] do
      @tag ack: true
      test "#{kind} is an error from the sync path and clears the transaction" do
        :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

        capture_log(fn ->
          assert {:error, {:unknown_relation, 42}} =
                   Publisher.process_message_sync(change_msg(unquote(kind), 42), @app)
        end)

        assert %{transaction: nil} = publisher_state()
      end
    end

    @tag ack: true
    test "a truncate naming one unknown relation among known ones is an error" do
      :ok = Publisher.process_message_sync(relation_msg(7), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      capture_log(fn ->
        assert {:error, {:unknown_relation, 42}} =
                 Publisher.process_message_sync(truncate_msg([7, 42]), @app)
      end)
    end

    test "is skipped with a warning from the async path" do
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      log =
        capture_log(fn ->
          Publisher.process_message_async(insert_msg(42), @app)
          publisher_state()
        end)

      assert log =~ "unknown relation 42"
      assert %{transaction: {{0, 0x10}, _txn}} = publisher_state()
    end

    test "a truncate from the async path skips only the unknown relation" do
      :ok = Publisher.process_message_sync(relation_msg(7), @app)
      :ok = Publisher.process_message_sync(relation_msg(8), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      log =
        capture_log(fn ->
          Publisher.process_message_async(truncate_msg([7, 42, 8]), @app)
          publisher_state()
        end)

      assert log =~ "unknown relation 42"

      assert [%{type: "TRUNCATE", change_index: 1}, %{type: "TRUNCATE", change_index: 0}] =
               open_changes()
    end
  end

  describe "a change whose tuple does not match its relation's columns" do
    @two_columns [
      %Messages.Relation.Column{name: "id", type: "int4"},
      %Messages.Relation.Column{name: "title", type: "text"}
    ]

    for kind <- [:insert, :update, :delete] do
      @tag ack: true
      test "#{kind} is an error from the sync path and clears the transaction" do
        :ok = Publisher.process_message_sync(relation_msg(7, @two_columns), @app)
        :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

        log =
          capture_log(fn ->
            assert {:error, {:column_mismatch, 7}} =
                     Publisher.process_message_sync(change_msg(unquote(kind), 7), @app)
          end)

        assert log =~ "relation 7"
        assert %{transaction: nil} = publisher_state()
      end
    end

    @tag ack: true
    test "a tuple with more values than columns is an error from the sync path" do
      :ok = Publisher.process_message_sync(relation_msg(7, []), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      capture_log(fn ->
        assert {:error, {:column_mismatch, 7}} =
                 Publisher.process_message_sync(insert_msg(7), @app)
      end)
    end

    test "an update without an old tuple is not a mismatch" do
      :ok = Publisher.process_message_sync(relation_msg(7), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      update = %Messages.Update{relation_id: 7, old_tuple_data: nil, tuple_data: {"2"}}
      assert :ok = Publisher.process_message_sync(update, @app)
    end

    test "is kept, as before, with a warning from the async path" do
      :ok = Publisher.process_message_sync(relation_msg(7, @two_columns), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      log =
        capture_log(fn ->
          Publisher.process_message_async(insert_msg(7), @app)
          publisher_state()
        end)

      assert log =~ "relation 7"
      assert [%{type: "INSERT", record: %{id: 1}}] = open_changes()
    end
  end

  describe "a failure while building a change" do
    @describetag ack: true

    # Decimal.new/1 raises on a value that is not a number.
    @numeric [%Messages.Relation.Column{name: "amount", type: "numeric"}]

    defp raising_insert do
      :ok = Publisher.process_message_sync(relation_msg(7, @numeric), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)
      %Messages.Insert{relation_id: 7, tuple_data: {"not a number"}}
    end

    test "is an error from the sync path that names only the exception module" do
      insert = raising_insert()
      pid = publisher_pid()

      log =
        capture_log(fn ->
          assert {:error, {:publisher_failed, %{kind: :error, exception: Decimal.Error}}} =
                   Publisher.process_message_sync(insert, @app)
        end)

      assert log =~ "Decimal.Error"
      assert %{transaction: nil} = publisher_state()
      assert pid == publisher_pid()
    end

    test "leaves the Publisher able to handle the redelivery" do
      insert = raising_insert()
      capture_log(fn -> {:error, _} = Publisher.process_message_sync(insert, @app) end)

      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      assert :ok =
               Publisher.process_message_sync(
                 %Messages.Insert{relation_id: 7, tuple_data: {"1.5"}},
                 @app
               )
    end
  end

  describe "a failure while building a change, without ack_after_processing" do
    @numeric [%Messages.Relation.Column{name: "amount", type: "numeric"}]

    test "crashes the Publisher on the sync path, as it always did" do
      :ok = Publisher.process_message_sync(relation_msg(7, @numeric), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)
      insert = %Messages.Insert{relation_id: 7, tuple_data: {"not a number"}}
      pid = publisher_pid()

      capture_log(fn ->
        assert {{%Decimal.Error{}, _stacktrace}, _call} =
                 catch_exit(Publisher.process_message_sync(insert, @app))
      end)

      refute pid == publisher_pid()
    end
  end

  # Custom middleware may call process_message_sync/2 in default mode, so it
  # keeps master's behaviour there: always :ok, and only the bad change is lost.
  describe "the sync path without ack_after_processing" do
    @two_columns [
      %Messages.Relation.Column{name: "id", type: "int4"},
      %Messages.Relation.Column{name: "title", type: "text"}
    ]

    test "skips a change for an unknown relation and keeps the transaction" do
      :ok = Publisher.process_message_sync(relation_msg(7), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)
      :ok = Publisher.process_message_sync(insert_msg(7), @app)

      log =
        capture_log(fn ->
          assert :ok = Publisher.process_message_sync(insert_msg(42), @app)
        end)

      assert log =~ "unknown relation 42"
      assert [%{type: "INSERT", change_index: 0}] = open_changes()
    end

    test "keeps a change whose tuple does not match its columns" do
      :ok = Publisher.process_message_sync(relation_msg(7, @two_columns), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)

      log =
        capture_log(fn ->
          assert :ok = Publisher.process_message_sync(insert_msg(7), @app)
        end)

      assert log =~ "relation 7"
      assert [%{type: "INSERT", record: %{id: 1}}] = open_changes()
    end

    test "replies :ok to a dropped COMMIT" do
      capture_log(fn ->
        assert :ok = Publisher.process_message_sync(commit({0, 0x20}), @app)
      end)
    end
  end

  describe "change_index" do
    @sequence [:insert, :update, :delete, :truncate, :insert]

    defp open_changes do
      {_lsn, %{changes: changes}} = publisher_state().transaction
      changes
    end

    defp run_sequence(lsn) do
      :ok = Publisher.process_message_sync(relation_msg(7), @app)
      :ok = Publisher.process_message_sync(begin_msg(lsn), @app)

      for kind <- @sequence do
        :ok = Publisher.process_message_sync(change_msg(kind, 7), @app)
      end

      open_changes()
    end

    test "numbers changes 0..n-1 in commit order, truncates included" do
      # The open list is newest-first, so the indexes count down from the head.
      assert [4, 3, 2, 1, 0] = Enum.map(run_sequence({0, 0x10}), & &1.change_index)
    end

    test "a truncate naming several relations numbers each one" do
      :ok = Publisher.process_message_sync(relation_msg(7), @app)
      :ok = Publisher.process_message_sync(relation_msg(8), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)
      :ok = Publisher.process_message_sync(insert_msg(7), @app)
      :ok = Publisher.process_message_sync(truncate_msg([7, 8]), @app)

      assert [2, 1, 0] = Enum.map(open_changes(), & &1.change_index)
    end

    # After a network drop mid-transaction the Publisher survives with the
    # partial change list; the redelivery's BEGIN must replace it, not add to it.
    test "a redelivery to a Publisher holding a half-open transaction starts over" do
      :ok = Publisher.process_message_sync(relation_msg(7), @app)
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)
      :ok = Publisher.process_message_sync(insert_msg(7), @app)
      :ok = Publisher.process_message_sync(update_msg(7), @app)

      assert [4, 3, 2, 1, 0] = Enum.map(run_sequence({0, 0x10}), & &1.change_index)
    end

    test "a redelivered transaction yields identical {lsn, change_index} pairs" do
      pairs = fn changes -> Enum.map(changes, &{&1.lsn, &1.change_index}) end

      first = pairs.(run_sequence({0, 0x10}))

      stop_supervised!(Publisher)
      start_supervised!({Publisher, app_name: @app})

      assert first == pairs.(run_sequence({0, 0x10}))
    end
  end

  describe "a matching COMMIT" do
    setup do
      Process.register(self(), :publisher_test_dispatch)
      start_supervised!({EventModules, app_name: @app})
      start_supervised!({Events, app_name: @app})
      :ok
    end

    test "is dispatched and clears the open transaction" do
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @app)
      assert :ok = Publisher.process_message_sync(commit({0, 0x10}), @app)
      assert_received :dispatched
      assert %{transaction: nil} = publisher_state()

      log =
        capture_log(fn ->
          assert :ok = Publisher.process_message_sync(commit({0, 0x10}), @app)
        end)

      assert log =~ "no transaction is open"
    end
  end

  describe "a matching COMMIT whose handler fails, with ack_after_processing" do
    @ack_app :publisher_test_ack_app

    defmodule RaisingHandler do
      def process_all(_txn), do: raise("handler failed")
    end

    setup do
      {:ok, _} =
        Config.start_link(
          configs: [
            name: @ack_app,
            hostname: "localhost",
            username: "postgres",
            password: "postgres",
            database: "postgres",
            port: 5432,
            publication: "publication",
            subscriptions: ["subscriptions"],
            modules: [RaisingHandler],
            durable_slot: true,
            ack_after_processing: true
          ]
        )

      start_supervised!({EventModules, app_name: @ack_app}, id: :ack_event_modules)
      start_supervised!({Events, app_name: @ack_app}, id: :ack_events)
      start_supervised!({Publisher, app_name: @ack_app}, id: :ack_publisher)
      :ok
    end

    test "is an error from the sync path and clears the open transaction" do
      :ok = Publisher.process_message_sync(begin_msg({0, 0x10}), @ack_app)

      capture_log(fn ->
        assert {:error, {:handler_failed, %{module: RaisingHandler, function: :process_all}}} =
                 Publisher.process_message_sync(commit({0, 0x10}), @ack_app)
      end)

      ack_publisher = Registry.set_name(:set_gen_server, Publisher, @ack_app)
      assert %{transaction: nil} = :sys.get_state(ack_publisher)
    end
  end
end
