defmodule WalEx.Events.EventModules do
  @moduledoc """
  Process events (call modules containing process functions)
  """
  use GenServer

  require Logger

  alias WalEx.{Changes.Transaction, Config, Config.Registry, Helpers}

  def start_link(opts) do
    name =
      opts
      |> Keyword.get(:app_name)
      |> registry_name

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Runs the `on_connect/1` hook of every configured module.

  Called by the replication connection each time it connects. With
  `ack_after_processing` the hooks have run when this returns, so they finish
  before streaming starts. Otherwise the hooks run asynchronously (best
  effort) and this returns immediately.

  When this process is not registered (it is restarting) there is nothing to
  run, and this returns `:ok` unless modules are configured. With
  `ack_after_processing` and modules configured, it raises instead: the hooks
  must not be skipped, and the replication server, crashing, is restarted
  once this process is back. If the process dies while a synchronous call is in
  flight (it was registered at the check, then went away), the caller exits.
  That crashes the replication server, which reconnects and runs the hooks
  again: the fail-safe outcome with `ack_after_processing`.
  """
  @spec connected(atom(), %{app_name: atom(), slot_name: String.t(), reconnect?: boolean()}) ::
          :ok
  def connected(app_name, info) do
    name = registry_name(app_name)

    name
    |> GenServer.whereis()
    |> notify_connected(Config.ack_after_processing?(app_name), name, app_name, info)
  end

  defp notify_connected(nil, true, _name, app_name, _info) do
    case Config.get_event_modules(app_name) do
      [_ | _] = modules ->
        raise "on_connect hooks of #{inspect(modules)} cannot run: EventModules is not running"

      _no_modules ->
        :ok
    end
  end

  defp notify_connected(nil, false, _name, _app_name, _info), do: :ok

  defp notify_connected(_pid, true, name, _app_name, info),
    do: GenServer.call(name, {:connected, info}, :infinity)

  defp notify_connected(_pid, false, name, _app_name, info),
    do: GenServer.cast(name, {:connected, info})

  @doc """
  Runs every configured handler for `txn`.

  With `ack_after_processing`, a handler that raises, throws or exits is
  reported as `{:error, {:handler_failed, details}}` and the remaining handlers
  for the transaction are skipped, so the transaction can be redelivered
  without crashing anything. Otherwise the failure crashes this process, as it
  always has.
  """
  @spec process(Transaction.t(), atom()) :: :ok | {:error, term()}
  def process(txn, app_name) do
    name = registry_name(app_name)

    GenServer.call(name, {:process, txn, app_name}, :infinity)
  end

  defp registry_name(app_name) do
    Registry.set_name(:set_gen_server, __MODULE__, app_name)
  end

  @impl true
  def init(opts) do
    Process.flag(:message_queue_data, :off_heap)

    {:ok,
     %{ack_after_processing: opts |> Keyword.get(:app_name) |> Config.ack_after_processing?()}}
  end

  @impl true
  def handle_call({:process, txn, app_name}, _from, state) do
    result =
      app_name
      |> Config.get_event_modules()
      |> List.wrap()
      |> process_events(txn, app_name, state.ack_after_processing)

    {:reply, result, state}
  end

  def handle_call({:connected, info}, _from, state) do
    run_connect_hooks(info)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:connected, info}, state) do
    run_connect_hooks(info)
    {:noreply, state}
  end

  defp run_connect_hooks(info = %{app_name: app_name}) do
    app_name
    |> Config.get_event_modules()
    |> List.wrap()
    |> Enum.each(&run_connect_hook(&1, info))
  end

  defp run_connect_hook(module, info) do
    if defines?(module, :process_connect) do
      module.process_connect(info)
    end
  catch
    kind, reason ->
      Logger.error("on_connect in #{inspect(module)} failed: #{hook_failure(kind, reason)}")
  end

  defp hook_failure(:error, reason),
    do: :error |> Exception.normalize(reason) |> Exception.message()

  defp hook_failure(kind, reason), do: inspect({kind, reason})

  # Handlers are optional. An unloadable module raises here on purpose, so a
  # misconfigured module fails loudly instead of being silently skipped.
  defp defines?(module, function),
    do: Keyword.has_key?(module.__info__(:functions), function)

  @functions ~w(process_all process_insert process_update process_delete)a

  defp process_events([], _txn, _app_name, _ack_after_processing), do: :ok

  defp process_events(modules, txn, app_name, ack_after_processing) do
    handlers = for module <- modules, function <- @functions, do: {module, function}

    Enum.reduce_while(handlers, :ok, fn {module, function}, :ok ->
      case run_handler(module, function, txn, app_name, ack_after_processing) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp run_handler(module, function, txn, app_name, false) do
    apply_process_macro(function, module, txn, app_name)
    :ok
  end

  # The catch sits outside the telemetry span, so the span still reports the
  # failure as an [:walex, :event_handler, :exception] event. This is the only
  # place that has the stacktrace, so the failure is logged here.
  defp run_handler(module, function, txn, app_name, true) do
    apply_process_macro(function, module, txn, app_name)
    :ok
  catch
    kind, reason ->
      failure =
        Helpers.log_caught_failure(
          "#{inspect(module)}.#{function}/1 failed, its transaction will be redelivered:",
          kind,
          reason,
          __STACKTRACE__
        )

      {:error, {:handler_failed, Map.merge(%{module: module, function: function}, failure)}}
  end

  # Each handler call is a telemetry span: [:walex, :event_handler, :start],
  # [..., :stop], and [..., :exception] when it raises. The exception is
  # re-raised, so run_handler/5 decides how the failure is handled.
  defp apply_process_macro(function, module, txn, app_name) do
    if defines?(module, function) do
      metadata = %{app_name: app_name, module: module, function: function}

      :telemetry.span([:walex, :event_handler], metadata, fn ->
        {apply(module, function, [txn]), metadata}
      end)
    end
  end
end
