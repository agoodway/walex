defmodule WalEx.Replication.PublisherTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias WalEx.Config
  alias WalEx.Decoder.Messages
  alias WalEx.Replication.Publisher

  @app :publisher_test_app
  @ts ~U[2025-01-01 00:00:00Z]

  setup do
    start_supervised!(WalEx.Config.Registry.child_spec())

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
          subscriptions: ["subscriptions"]
        ]
      )

    start_supervised!({Publisher, app_name: @app})
    :ok
  end

  defp commit(lsn), do: %Messages.Commit{flags: [], lsn: lsn, end_lsn: lsn, commit_timestamp: @ts}

  test "logs a warning when a COMMIT does not match the open transaction" do
    :ok =
      Publisher.process_message_sync(
        %Messages.Begin{final_lsn: {0, 0x10}, commit_timestamp: @ts, xid: 1},
        @app
      )

    log = capture_log(fn -> :ok = Publisher.process_message_sync(commit({1, 0xFF}), @app) end)

    assert log =~ "Dropping COMMIT at 1/FF: the open transaction is at 0/10"
  end

  test "logs a warning when a COMMIT arrives with no open transaction" do
    log = capture_log(fn -> :ok = Publisher.process_message_sync(commit({0, 0x20}), @app) end)

    assert log =~ "Dropping COMMIT at 0/20: no transaction is open"
  end
end
