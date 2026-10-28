defmodule WalEx.Events.EventModules do
  @moduledoc """
  Process events (call modules containing process functions)
  """
  use GenServer

  require Logger

  alias WalEx.Config.Registry

  def start_link(opts) do
    name =
      opts
      |> Keyword.get(:app_name)
      |> registry_name

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Runs the `on_connect/1` hook of every configured module, asynchronously.

  Called by the replication connection each time it connects. Returns `:ok`
  immediately, also when this process is not running.
  """
  def connected(app_name, info) do
    GenServer.cast(registry_name(app_name), {:connected, info})
  end

  def process(txn, app_name) do
    name = registry_name(app_name)

    GenServer.call(name, {:process, txn, app_name}, :infinity)
  end

  defp registry_name(app_name) do
    Registry.set_name(:set_gen_server, __MODULE__, app_name)
  end

  @impl true
  def init(_) do
    Process.flag(:message_queue_data, :off_heap)
    {:ok, %{}}
  end

  @impl true
  def handle_call({:process, txn, server}, _from, state) do
    server
    |> WalEx.Config.get_configs(:modules)
    |> process_events(txn)

    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:connected, %{app_name: app_name} = info}, state) do
    app_name
    |> WalEx.Config.get_configs(:modules)
    |> List.wrap()
    |> Enum.each(&run_connect_hook(&1, info))

    {:noreply, state}
  end

  defp run_connect_hook(module, info) do
    if Keyword.has_key?(module.__info__(:functions), :process_connect) do
      module.process_connect(info)
    end
  rescue
    error ->
      Logger.error("on_connect in #{inspect(module)} failed: #{Exception.message(error)}")
  catch
    kind, reason ->
      Logger.error("on_connect in #{inspect(module)} failed: #{inspect({kind, reason})}")
  end

  defp process_events(nil, %{changes: [], commit_timestamp: _}), do: nil

  defp process_events(modules, txn) when is_list(modules) do
    process_modules(modules, txn)
  end

  defp process_modules(modules, txn) do
    functions = ~w(process_all process_insert process_update process_delete)a

    Enum.each(modules, &process_module(&1, functions, txn))
  end

  defp process_module(module_name, functions, txn) do
    Enum.each(functions, &apply_process_macro(&1, module_name, txn))
  end

  defp apply_process_macro(function, module, txn) do
    if Keyword.has_key?(module.__info__(:functions), function) do
      apply(module, function, [txn])
    end
  end
end
