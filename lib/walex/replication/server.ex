# This file steals liberally from https://github.com/chasers/postgrex_replication_demo/blob/main/lib/replication.ex
# which in turn draws on https://hexdocs.pm/postgrex/Postgrex.ReplicationConnection.html#module-logical-replication

defmodule WalEx.Replication.Server do
  @moduledoc """
  This module is responsible for setting up the replication connection
  """
  use Postgrex.ReplicationConnection

  alias WalEx.Config
  alias WalEx.Config.Registry, as: WalExRegistry
  alias WalEx.Decoder
  alias WalEx.Replication.QueryBuilder

  require Logger

  @max_retries 10
  @initial_backoff 1000

  def start_link(opts) do
    app_name = Keyword.get(opts, :app_name)
    opts = set_pgx_replication_conn_opts(app_name)

    Postgrex.ReplicationConnection.start_link(__MODULE__, [app_name: app_name], opts)
  end

  @impl true
  def init(opts) do
    app_name = Keyword.get(opts, :app_name)

    [
      slot_name: slot_name,
      publication: publication,
      durable_slot: durable_slot,
      message_middleware: message_middleware
    ] =
      Config.get_configs(app_name, [
        :slot_name,
        :publication,
        :durable_slot,
        :message_middleware
      ])

    state = %{
      step: :disconnected,
      app_name: app_name,
      slot_name: slot_name,
      publication: publication,
      durable_slot: durable_slot,
      message_middleware: message_middleware
    }

    Process.flag(:message_queue_data, :off_heap)

    {:ok, state}
  end

  @impl true
  def handle_connect(state) do
    query = QueryBuilder.publication_exists(state)
    {:query, query, %{state | step: :publication_exists}}
  end

  @impl true
  def handle_result([%Postgrex.Result{num_rows: 1}], state = %{step: :publication_exists}) do
    if state.durable_slot do
      query = QueryBuilder.slot_exists(state)
      {:query, query, %{state | step: :slot_exists}}
    else
      query = QueryBuilder.create_temporary_slot(state)
      {:query, query, %{state | step: :create_slot}}
    end
  end

  @impl true
  def handle_result(results, state = %{step: :publication_exists}) do
    case results do
      [%Postgrex.Result{num_rows: 0}] ->
        raise "Publication doesn't exist. publication: #{inspect(state.publication)}"

      _ ->
        raise "Unexpected result when checking if publication exists. #{inspect(results)}"
    end
  end

  @impl true
  def handle_result([%Postgrex.Result{num_rows: 0}], state = %{step: :slot_exists}) do
    query = QueryBuilder.create_durable_slot(state)
    {:query, query, %{state | step: :create_slot}}
  end

  @impl true
  def handle_result(
        [%Postgrex.Result{columns: ["active"], rows: [[active]]}],
        state = %{step: :slot_exists}
      ) do
    case active do
      "f" ->
        Logger.info("Activating inactive replication slot: #{state.slot_name}")
        start_replication_with_retry(state, 0, @initial_backoff)

      "t" ->
        Logger.info(
          "Replication slot #{state.slot_name} is active. Waiting for it to become inactive."
        )

        schedule_slot_check()

        {:noreply, state}
    end
  end

  @impl true
  def handle_result(results, %{step: :slot_exists}) do
    raise "Failed to check if durable slot already exists. #{inspect(results)}"
  end

  @impl true
  def handle_result([%Postgrex.Result{} | _results], state = %{step: :create_slot}) do
    start_replication_with_retry(state, 0, @initial_backoff)
  end

  @impl true
  def handle_result(error = %Postgrex.Error{}, %{step: :create_slot}) do
    # if durable slot, can happen if multiple instances try to create the same slot
    raise "Failed to create replication slot, #{inspect(error)}"
  end

  @impl true
  def handle_result(
        %Postgrex.Error{postgres: %{code: :object_in_use}},
        state = %{step: {:start_replication, retry_count, backoff}}
      ) do
    Logger.warning("Replication slot in use, retrying... (attempt #{retry_count + 1})")
    Process.sleep(backoff)
    start_replication_with_retry(state, retry_count + 1, backoff * 2)
  end

  @impl true
  def handle_result(_, state = %{step: {:start_replication, _retry_count, _backoff}}) do
    Logger.info("Successfully started replication slot: #{state.slot_name}")
    {:noreply, %{state | step: :streaming}}
  end

  @impl true
  def handle_data(<<?w, _wal_start::64, _wal_end::64, _clock::64, rest::binary>>, state) do
    rest
    |> Decoder.decode_message()
    |> state.message_middleware.(state.app_name)

    {:noreply, state}
  end

  @impl true
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
    messages =
      case reply do
        1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
        0 -> []
      end

    {:noreply, messages, state}
  end

  @impl true
  def handle_info(:check_slot_status, state) do
    query = QueryBuilder.slot_exists(state)
    {:query, query, %{state | step: :slot_exists}}
  end

  defp set_pgx_replication_conn_opts(app_name) do
    database_configs_keys = [
      :hostname,
      :username,
      :password,
      :port,
      :database,
      :ssl,
      :ssl_opts,
      :socket_options
    ]

    extra_opts = [auto_reconnect: true]
    database_configs = Config.get_configs(app_name, database_configs_keys)

    replications_name = [
      name: WalExRegistry.set_name(:set_gen_server, __MODULE__, app_name)
    ]

    extra_opts ++ database_configs ++ replications_name
  end

  defp start_replication_with_retry(state, retry_count, backoff)
       when retry_count < @max_retries do
    query = QueryBuilder.start_replication_slot(state)
    {:stream, query, [], %{state | step: {:start_replication, retry_count, backoff}}}
  end

  defp start_replication_with_retry(state, _retry_count, _backoff) do
    Logger.warning(
      "Failed to start replication slot after maximum retries. Scheduling another check."
    )

    schedule_slot_check()

    {:noreply, state}
  end

  defp schedule_slot_check do
    # Check again after 5 seconds
    Process.send_after(self(), :check_slot_status, 5000)
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time, do: System.os_time(:microsecond) - @epoch
end
