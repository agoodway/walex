defmodule WalEx.Support.TestHelpers do
  @moduledoc """
  Shared helpers for the WalEx test suite: Postgrex connection setup,
  query convenience wrappers, and small fixtures (e.g. `update_user/1`)
  that exercise the replication pipeline against the test database.
  """

  import ExUnit.Assertions, only: [flunk: 1]

  require Logger

  alias ExUnit.Callbacks
  alias Mix.Tasks.Walex.Helpers, as: PgHelpers
  alias Postgrex.Result
  alias WalEx.{Config, Replication.Server}
  alias WalEx.Supervisor, as: WalExSupervisor

  @doc "Connection options (hostname, username, password, port) from the `PG*` variables."
  def pg_config, do: PgHelpers.config()

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
  How the database-restart test can restart Postgres, or nil when it must not.
  See `pg_restart_plan/0`.
  """
  def pg_restart_method do
    case pg_restart_plan() do
      {:ok, method} -> method
      {:skip, _reason} -> nil
    end
  end

  @doc """
  `{:ok, :docker}` when the `docker-compose.dbs.yml` `db` service publishes the
  port the tests connect to (`PGPORT`), `{:ok, :system}` when the developer
  opted in with `WALEX_PG_RESTART=system` and a local installation exists,
  otherwise `{:skip, reason}`.

  Restarting is destructive, so it has to be tied to the server under test: a
  running compose container on some other port, or a developer's own local
  Postgres, must never be restarted by a plain `mix test`.
  """
  def pg_restart_plan do
    pg_restart_plan(%{
      host: pg_config()[:hostname],
      port: pg_config()[:port],
      compose_ports: docker_compose_db_ports(),
      system: System.get_env("WALEX_PG_RESTART"),
      installation: pg_installation_type()
    })
  end

  @local_hosts ["localhost", "127.0.0.1", "::1"]

  @doc false
  def pg_restart_plan(target = %{host: host, port: port, compose_ports: compose_ports}) do
    cond do
      host in @local_hosts and port in compose_ports ->
        {:ok, :docker}

      target.system == "system" and target.installation != nil ->
        {:ok, :system}

      true ->
        {:skip, skip_reason(target, host in @local_hosts)}
    end
  end

  defp skip_reason(%{host: host, port: port, compose_ports: compose_ports}, local?) do
    cond do
      not local? ->
        "PGHOST=#{host} is not local, so it cannot be restarted from here."

      compose_ports != [] ->
        "the docker-compose.dbs.yml `db` service publishes port(s) #{Enum.join(compose_ports, ", ")}, " <>
          "but the tests use PGPORT=#{port}; refusing to restart a different Postgres."

      true ->
        "no docker-compose.dbs.yml `db` service is running on PGPORT=#{port}. Start it " <>
          "(publishing that port), or set WALEX_PG_RESTART=system to let the test restart " <>
          "a local Postgres installation."
    end
  end

  # The host ports the compose `db` service publishes for 5432, empty when it is
  # not running or docker is unavailable. `docker compose ps` exits 0 for a
  # stopped service, but `port` prints nothing for it.
  defp docker_compose_db_ports do
    case System.shell("docker compose -f docker-compose.dbs.yml port db 5432 2>/dev/null") do
      {output, 0} -> parse_published_ports(output)
      _ -> []
    end
  end

  @doc false
  def parse_published_ports(output) do
    ~r/:(\d+)\s*$/m
    |> Regex.scan(output, capture: :all_but_first)
    |> Enum.map(fn [port] -> String.to_integer(port) end)
  end

  def start_walex_supervised(configs, supervisor \\ WalExSupervisor) do
    Callbacks.start_supervised(%{
      id: make_ref(),
      start: {supervisor, :start_link, [configs]},
      type: :supervisor,
      restart: :temporary
    })
  end

  def start_database(configs) do
    configs
    |> Keyword.delete(:name)
    |> Postgrex.start_link()
    |> case do
      {:ok, conn} ->
        Logger.info("Database is running.")

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

  defp map_rows_to_columns(%Result{columns: columns, rows: rows}) do
    Enum.map(rows, fn row -> Enum.zip(columns, row) |> Map.new() end)
  end

  def pg_replication_slots(database_pid) do
    pg_replication_slots_query =
      "SELECT slot_name, slot_type, active, temporary FROM \"pg_replication_slots\" WHERE database = current_database();"

    query(database_pid, pg_replication_slots_query)
  end

  @doc """
  Terminates the walsenders of, then drops, the named replication slots, retrying
  until they are gone. Only the slots the suite owns are touched, never every
  slot in the database, so a developer's own slot in the same database survives.
  Raises if a slot is still there after `attempts` tries, instead of letting the
  next test run against a stale one.
  """
  def pg_drop_slots(database_pid, slot_names, attempts \\ 100)

  def pg_drop_slots(_database_pid, [], _attempts) do
    raise ArgumentError, "pg_drop_slots/3 needs the names of the slots to drop"
  end

  def pg_drop_slots(database_pid, slot_names, attempts) do
    owned = "database = current_database() AND slot_name::text = ANY($1::text[])"

    Postgrex.query!(
      database_pid,
      "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots " <>
        "WHERE #{owned} AND active_pid IS NOT NULL",
      [slot_names]
    )

    Postgrex.query!(
      database_pid,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots " <>
        "WHERE #{owned} AND NOT active",
      [slot_names]
    )

    remaining =
      Postgrex.query!(
        database_pid,
        "SELECT slot_name FROM pg_replication_slots WHERE #{owned}",
        [slot_names]
      )

    retry_drop_slots(remaining.rows, database_pid, slot_names, attempts)
  end

  defp retry_drop_slots([], _database_pid, _slot_names, _attempts), do: :ok

  defp retry_drop_slots(rows, _database_pid, _slot_names, 0) do
    raise "could not drop replication slots #{inspect(List.flatten(rows))}"
  end

  defp retry_drop_slots(_rows, database_pid, slot_names, attempts) do
    :timer.sleep(50)
    pg_drop_slots(database_pid, slot_names, attempts - 1)
  end

  def update_user(database_pid) do
    update_user = """
      UPDATE \"user\" SET age = 30, books = ARRAY['book1, 2 and 3', 'book4'], favorite_numbers = ARRAY[1, 2, 3], meta = '{"key": {"foo": "bar"}, "list": [1, 2, 3]}'::JSONB WHERE id = 1
    """

    Postgrex.query!(database_pid, update_user, [])
  end

  @doc """
  A realistic `WalEx.Replication.Server` state: what `Server.init/1` builds for
  `app` (whose Config agent and Registry must already be running) with
  `overrides` merged on top. Building from `init/1` keeps tests from drifting
  away from the real state shape. Note that `init/1` runs in the calling test
  process, so it also sets `Process.flag(:message_queue_data, :off_heap)` there.
  """
  def server_state(app, overrides \\ []) do
    {:ok, state} = Server.init(app_name: app)
    Map.merge(state, Map.new(overrides))
  end

  @doc """
  A `server_state/2` that is streaming. `streaming?: true` is what the server
  checks; `step: :streaming` is set for readability and is not what a live
  connection holds at this point.
  """
  def streaming_state(app, overrides \\ []) do
    server_state(app, Keyword.merge([step: :streaming, streaming?: true], overrides))
  end

  @doc """
  Database connection options for configs that never connect, or connect to
  the test database. The port follows `PGPORT`, like every other test.
  """
  def base_db_configs do
    pg_config() ++ [database: "postgres", ssl: false, ssl_opts: [verify: :verify_none]]
  end

  @doc "Starts a Config agent for `app` with test defaults, `extra` appended."
  def start_config!(app, extra \\ []) do
    configs =
      [name: app, publication: "publication", subscriptions: ["subscriptions"]] ++
        base_db_configs() ++ extra

    {:ok, _pid} = Config.start_link(configs: configs)
  end

  @doc "Forwards `events` to the test process as `{:telemetry, event, measurements, metadata}`."
  def attach_telemetry(events) do
    id = "walex-test-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach_many(
      id,
      events,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    Callbacks.on_exit(fn -> :telemetry.detach(id) end)
  end

  @doc "A primary keepalive frame (`k`); `reply` 1 asks for a status update."
  def keepalive(wal_end, reply), do: <<?k, wal_end::64, 0::64, reply::8>>

  @doc "An XLogData frame (`w`) wrapping a pgoutput payload."
  def xlog(payload, wal_end \\ 0), do: <<?w, 0::64, wal_end::64, 0::64, payload::binary>>

  @doc "A pgoutput BEGIN announcing the commit LSN `final_lsn`."
  def begin_msg(final_lsn), do: <<?B, final_lsn::64, 0::64, 1::32>>

  @doc "A pgoutput COMMIT at `lsn`, ending at `end_lsn` (8 bytes later by default)."
  def commit_msg(lsn, end_lsn \\ nil), do: <<?C, 0::8, lsn::64, end_lsn || lsn + 8::64, 0::64>>

  @doc "The write LSN of a standby status update (`r`), or of the only one in a list."
  def ack_lsn([message]), do: ack_lsn(message)
  def ack_lsn(<<?r, lsn::64, _flush::64, _apply::64, _clock::64, _reply::8>>), do: lsn

  @doc "Delivers the standby timer message the state is waiting for."
  def tick(state) do
    token = make_ref()
    Server.handle_info({:standby_status, token}, %{state | standby_token: token})
  end

  @doc "Polls `fun` until it returns `true`, failing the test after `timeout` ms."
  def wait_until(fun, description, timeout \\ 30_000) do
    do_wait_until(fun, description, System.monotonic_time(:millisecond) + timeout)
  end

  defp do_wait_until(fun, description, deadline) do
    do_wait_until(fun.(), fun, description, deadline)
  end

  defp do_wait_until(true, _fun, _description, _deadline), do: :ok

  defp do_wait_until(_result, fun, description, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      flunk("timed out waiting for #{description}")
    else
      Process.sleep(100)
      do_wait_until(fun, description, deadline)
    end
  end
end
