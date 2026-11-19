defmodule WalEx.Events.Supervisor do
  @moduledoc """
  Supervises the per-app event GenServers (`WalEx.Events` and
  `WalEx.Events.EventModules`). `EventModules` always runs, even with no
  modules configured, so that modules added at runtime with
  `WalEx.Config.add_config/3` have a process to deliver through.
  """

  use Supervisor

  alias WalEx.{Config, Events, Events.EventModules}
  alias WalEx.Config.Registry, as: WalExRegistry

  def start_link(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    name = WalExRegistry.set_name(:set_supervisor, __MODULE__, app_name)

    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)

    check_handlers!(
      Config.get_event_modules(app_name),
      Config.ack_after_processing?(app_name),
      app_name
    )

    children = [{Events, app_name: app_name}, {EventModules, app_name: app_name}]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
  end

  # With no handler every transaction would be acknowledged unhandled, which
  # is most likely a misspelled subscription or module name.
  defp check_handlers!(modules, true, app_name) when modules in [nil, []] do
    raise ArgumentError,
          "ack_after_processing requires at least one event handler module, " <>
            "but no event handler module resolved for #{inspect(app_name)}"
  end

  defp check_handlers!(_modules, _ack_after_processing, _app_name), do: :ok
end
