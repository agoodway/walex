{:ok, _} = Application.ensure_all_started(:postgrex)

# The database-restart test needs a Postgres it can actually restart. Without
# one it used to pass while restarting nothing, so skip it, loudly.
exclude =
  if WalEx.Support.TestHelpers.pg_restart_method() do
    []
  else
    IO.puts("""
    Skipping tests tagged :pg_restart: no restartable Postgres found. Start the \
    docker-compose.dbs.yml `db` service, or install Postgres locally, to run them.
    """)

    [:pg_restart]
  end

ExUnit.start(capture_log: true, exclude: [:replay | exclude])
