defmodule Mix.Tasks.Walex.Helpers do
  @moduledoc """
  Shared helpers for the `walex.setup` and `walex.drop` Mix tasks.

  Wraps `psql` invocations and `Postgrex.query!/3` calls used to bootstrap and
  tear down the test database, honoring the `PG*` environment variables for
  custom Postgres connection settings.
  """

  @test_database "todos_test"

  @doc "The database the test suite runs against, created and dropped by the tasks."
  @spec test_database() :: String.t()
  def test_database, do: @test_database

  @doc "Connection options from the `PG*` variables, read at call time so later changes are honoured."
  @spec config() :: keyword()
  def config do
    [
      hostname: System.get_env("PGHOST", "localhost"),
      username: System.get_env("PGUSER", "postgres"),
      password: System.get_env("PGPASSWORD", "postgres"),
      port: String.to_integer(System.get_env("PGPORT", "5432"))
    ]
  end

  @doc "The `config/0` options as the environment `psql` expects."
  @spec psql_env() :: [{String.t(), String.t()}]
  def psql_env do
    config = config()

    [
      {"PGHOST", config[:hostname]},
      {"PGUSER", config[:username]},
      {"PGPASSWORD", config[:password]},
      {"PGPORT", Integer.to_string(config[:port])}
    ]
  end

  @doc "Creates the given database via `psql` against the `postgres` maintenance database."
  def create_database(database_name) do
    "-d postgres -c \"CREATE DATABASE #{database_name};\""
    |> database_cmd()
  end

  @doc "Drops the given database via `psql` against the `postgres` maintenance database."
  def drop_database(database_name) do
    "-d postgres -c \"DROP DATABASE #{database_name};\""
    |> database_cmd()
  end

  @doc "Runs `CREATE EXTENSION IF NOT EXISTS` against the connected Postgrex pid."
  def create_extension(pid, extension) do
    extension_query = "CREATE EXTENSION IF NOT EXISTS \"#{extension}\";"

    Postgrex.query!(pid, extension_query, [])
  end

  @doc "Shells out to `psql` with PGHOST/PGUSER/PGPASSWORD/PGPORT taken from env or defaults."
  def database_cmd(cmd) do
    db_cmd = "psql " <> cmd

    case System.shell(db_cmd, env: psql_env()) do
      {output, 0} ->
        output

      {output, _status} ->
        output
    end
  end
end
