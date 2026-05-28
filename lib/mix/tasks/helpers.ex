defmodule Mix.Tasks.Walex.Helpers do
  @moduledoc """
  Shared helpers for the `walex.setup` and `walex.drop` Mix tasks.

  Wraps `psql` invocations and `Postgrex.query!/3` calls used to bootstrap and
  tear down the test database, honoring the `PG*` environment variables for
  custom Postgres connection settings.
  """

  @config %{
    hostname: System.get_env("PGHOST", "localhost"),
    username: System.get_env("PGUSER", "postgres"),
    password: System.get_env("PGPASSWORD", "postgres"),
    port: String.to_integer(System.get_env("PGPORT", "5432"))
  }

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

    env = [
      {"PGHOST", @config.hostname},
      {"PGUSER", @config.username},
      {"PGPASSWORD", @config.password},
      {"PGPORT", Integer.to_string(@config.port)}
    ]

    case System.shell(db_cmd, env: env) do
      {output, 0} ->
        output

      {output, _status} ->
        output
    end
  end
end
