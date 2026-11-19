{:ok, _} = Application.ensure_all_started(:postgrex)

alias WalEx.Support.TestHelpers

# The database-restart test restarts a real Postgres, so it only runs against
# one verified to be the server under test (see `TestHelpers.pg_restart_plan/0`).
# Otherwise it used to pass while restarting nothing, or restart someone else's
# server, so skip it, loudly.
exclude =
  case TestHelpers.pg_restart_plan() do
    {:ok, _method} ->
      []

    {:skip, reason} ->
      IO.puts("Skipping tests tagged :pg_restart: #{reason}")
      [:pg_restart]
  end

ExUnit.start(capture_log: true, exclude: [:replay | exclude])
