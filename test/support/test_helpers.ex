defmodule WalEx.Support.TestHelpers do
  @moduledoc """
  Shared helpers for the WalEx test suite: Postgrex connection setup,
  query convenience wrappers, and small fixtures (e.g. `update_user/1`)
  that exercise the replication pipeline against the test database.
  """

  require Logger

  def tap_debug(to_tap, label) do
    to_tap |> tap(&Logger.debug(label <> inspect(&1)))
  end

  def find_child_pid(supervisor_pid, child_module) do
    supervisor_pid
    |> Supervisor.which_children()
    |> find_pid(child_module)
  end

  defp find_pid(children, module_name) do
    {_, pid, _, _} = Enum.find(children, fn {module, _, _, _} -> module == module_name end)
    pid
  end

  def get_database_pid(supervisor_pid) do
    find_child_pid(supervisor_pid, DBConnection.ConnectionPool)
  end

  @pg_installations [
    linux: "/usr/lib/postgresql",
    mac_homebrew: "/usr/local/Cellar/postgresql",
    mac_apple_silicon_homebrew: "/opt/homebrew/Cellar/postgresql",
    mac_app: "/Applications/Postgres.app/Contents/Versions"
  ]

  @doc "The local PostgreSQL installation type, or nil when none is found."
  def pg_installation_type do
    Enum.find_value(@pg_installations, fn {type, path} -> File.exists?(path) && type end)
  end

  @doc "The base path of a local PostgreSQL installation type."
  def pg_installation_path(type), do: Keyword.fetch!(@pg_installations, type)

  @doc """
  How the database-restart test can restart Postgres: `:docker` when the
  `docker-compose.dbs.yml` `db` service is running, `:system` when a local
  installation is found, otherwise nil.

  `docker compose ps` exits 0 even when the service isn't running, so ask for
  running containers only and check that one is listed.
  """
  def pg_restart_method do
    cond do
      docker_compose_db_running?() -> :docker
      pg_installation_type() -> :system
      true -> nil
    end
  end

  defp docker_compose_db_running? do
    case System.shell("docker compose -f docker-compose.dbs.yml ps --status running --quiet db") do
      {output, 0} -> String.trim(output) != ""
      _ -> false
    end
  end

  def start_walex_supervised(configs, supervisor \\ WalEx.Supervisor) do
    ExUnit.Callbacks.start_supervised(%{
      id: make_ref(),
      start: {supervisor, :start_link, [configs]},
      type: :supervisor,
      restart: :temporary
    })
  end

  def start_database(configs) do
    case Postgrex.start_link(configs) do
      {:ok, conn} ->
        Logger.info("Database is running.")

        {:ok, conn}

      {:error, {:already_started, conn}} ->
        Logger.info("Database is already running.")

        {:ok, conn}

      {:error, reason} ->
        Logger.error("Error connecting to the database. Reason: #{inspect(reason)}")

        {:error, reason}
    end
  end

  def terminate_database_connection(database_pid, username) do
    query =
      "SELECT pg_terminate_backend(pg_backend_pid()) FROM pg_stat_activity WHERE usename = $1"

    Postgrex.query(database_pid, query, [username])
  end

  def wait_for_restart do
    Logger.debug("waiting")
    :timer.sleep(3000)
    Logger.debug("done waiting")
  end

  def query(pid, query) do
    pid
    |> Postgrex.query!(query, [])
    |> map_rows_to_columns()
  end

  defp map_rows_to_columns(%Postgrex.Result{columns: columns, rows: rows}) do
    Enum.map(rows, fn row -> Enum.zip(columns, row) |> Map.new() end)
  end

  def pg_replication_slots(database_pid) do
    pg_replication_slots_query =
      "SELECT slot_name, slot_type, active, temporary FROM \"pg_replication_slots\" WHERE database = current_database();"

    query(database_pid, pg_replication_slots_query)
  end

  def pg_drop_slots(database_pid, attempts \\ 100) do
    terminate_slot_backends = """
    SELECT pg_terminate_backend(active_pid)
    FROM pg_replication_slots
    WHERE database = current_database() AND active_pid IS NOT NULL;
    """

    drop_slots = """
    SELECT pg_drop_replication_slot(slot_name)
    FROM pg_replication_slots
    WHERE database = current_database() AND NOT active;
    """

    query(database_pid, terminate_slot_backends)
    result = query(database_pid, drop_slots)

    if attempts > 0 and
         query(
           database_pid,
           "SELECT 1 FROM pg_replication_slots WHERE database = current_database();"
         ) != [] do
      :timer.sleep(50)
      pg_drop_slots(database_pid, attempts - 1)
    else
      result
    end
  end

  def update_user(database_pid) do
    update_user = """
      UPDATE \"user\" SET age = 30, books = ARRAY['book1, 2 and 3', 'book4'], favorite_numbers = ARRAY[1, 2, 3], meta = '{"key": {"foo": "bar"}, "list": [1, 2, 3]}'::JSONB WHERE id = 1
    """

    Postgrex.query!(database_pid, update_user, [])
  end
end
