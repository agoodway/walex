defmodule WalEx.Config.Registry do
  @moduledoc """
  Shared `Registry` used to look up per-app WalEx processes
  (config agent, GenServers, supervisors) by `{module, app_name}`.

  All WalEx processes register themselves via `set_name/3` so they can be
  located across the supervision tree without exporting module attributes.
  """

  @walex_registry :walex_registry

  @doc "Child spec for embedding the registry under another supervisor."
  def child_spec do
    {Registry, keys: :unique, name: @walex_registry}
  end

  @doc "Starts the registry if not already running. Idempotent across applications."
  def start_registry do
    case Process.whereis(@walex_registry) do
      nil ->
        Registry.start_link(keys: :unique, name: @walex_registry)

      pid ->
        {:ok, pid}
    end
  end

  @doc """
  Returns the `{:via, Registry, ...}` tuple that registers/looks up a process
  for `{module, app_name}`. The first argument is a tag (`:set_agent`,
  `:set_gen_server`, `:set_supervisor`) kept for call-site readability.
  """
  def set_name(:set_agent, module, app_name), do: set_name(module, app_name)
  def set_name(:set_gen_server, module, app_name), do: set_name(module, app_name)
  def set_name(:set_supervisor, module, app_name), do: set_name(module, app_name)

  defp set_name(module, app_name), do: {:via, Registry, {@walex_registry, {module, app_name}}}

  @doc "Fetches the current state of the registered agent for `{module, app_name}`."
  def get_state(:get_agent, module, app_name) do
    Agent.get({:via, Registry, {@walex_registry, {module, app_name}}}, & &1)
  end
end
