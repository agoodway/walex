defmodule WalEx.Supervisor do
  @moduledoc false

  use Supervisor

  alias WalEx.Config, as: WalExConfig
  alias WalEx.Config.Registry, as: WalExRegistry
  alias WalEx.Events.Supervisor, as: EventsSupervisor
  alias WalEx.Replication.Supervisor, as: ReplicationSupervisor

  @spec start_link(opts :: WalExConfig.start_opts()) :: Supervisor.on_start()
  def start_link(opts) do
    app_name = Keyword.get(opts, :name)

    validate_opts(opts)

    {:ok, _pid} = WalExRegistry.start_registry()

    name = WalExRegistry.set_name(:set_supervisor, __MODULE__, app_name)

    # The config agent resolves :modules itself, so it sees the list as given.
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    opts
    |> set_children()
    |> Supervisor.init(strategy: strategy(WalExConfig.ack_after_processing?(opts)))
  end

  # Each child depends on the ones started before it: replication dispatches
  # to the event processes, and both read the config agent. With
  # ack_after_processing, restarting one therefore restarts the ones after it
  # too; the durable slot keeps everything unacknowledged. Otherwise a
  # replication restart drops the temporary slot and the changes in between,
  # so it is only restarted when it fails itself, as it always was.
  defp strategy(true), do: :rest_for_one
  defp strategy(false), do: :one_for_one

  defp validate_opts(opts) do
    missing_configs = missing_db_configs(opts) ++ missing_event_configs(opts)

    unless Enum.empty?(missing_configs) do
      raise "Following required configs are missing: #{inspect(missing_configs)}"
    end
  end

  @db_configs [:hostname, :username, :password, :port, :database]

  defp missing_db_configs(opts), do: missing_db_configs(opts, Keyword.get(opts, :url))

  defp missing_db_configs(opts, nil), do: Enum.reject(@db_configs, &Keyword.has_key?(opts, &1))
  defp missing_db_configs(_opts, _has_url), do: []

  defp missing_event_configs(opts) do
    other_configs = [:subscriptions, :publication, :name]

    Enum.filter(other_configs, &(not Keyword.has_key?(opts, &1)))
  end

  defp set_children(configs) do
    app_name = Keyword.get(configs, :name)

    [
      {WalExConfig, configs: configs},
      {EventsSupervisor, app_name: app_name},
      {ReplicationSupervisor, app_name: app_name}
    ]
  end
end
