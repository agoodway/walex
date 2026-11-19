defmodule WalEx.Events do
  @moduledoc """
  Process events
  """

  use GenServer

  alias WalEx.{Changes.Transaction, Config, Config.Registry, Events.EventModules}

  require Logger

  def start_link(opts) do
    name =
      opts
      |> Keyword.get(:app_name)
      |> registry_name

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Hands `txn` to the event handlers and returns their result, so that a
  synchronous caller can tell whether the transaction was handled.
  """
  @spec process(Transaction.t(), atom()) :: :ok | {:error, term()}
  def process(txn, app_name),
    do: GenServer.call(registry_name(app_name), {:process, txn, app_name}, :infinity)

  defp registry_name(app_name) do
    Registry.set_name(:set_gen_server, __MODULE__, app_name)
  end

  @impl true
  def init(opts) do
    Process.flag(:message_queue_data, :off_heap)

    # Cannot change at runtime, see Config.replace_config/3.
    {:ok,
     %{ack_after_processing: opts |> Keyword.get(:app_name) |> Config.ack_after_processing?()}}
  end

  @impl true
  def handle_call({:process, txn, app_name}, _from, state) do
    result =
      app_name
      |> Config.get_event_modules()
      |> process_destinations(txn, app_name, state.ack_after_processing)

    {:reply, result, state}
  end

  # With no modules there is nothing to deliver to, which is a success.
  defp process_destinations([_ | _], txn, app_name, ack_after_processing),
    do: deliver(txn, app_name, ack_after_processing)

  defp process_destinations(_no_modules, _txn, _app_name, _ack_after_processing), do: :ok

  defp deliver(txn, app_name, false), do: EventModules.process(txn, app_name)

  # EventModules catches what a handler raises, throws or exits with, but not
  # an exit signal from a process the handler linked to, which kills it. That
  # surfaces here as an exit from the call. Reporting it like any other handler
  # failure keeps this process up and the transaction unacknowledged, and
  # lets the replication server back off before it is redelivered. The crash
  # itself is logged by EventModules' own crash report.
  defp deliver(txn, app_name, true) do
    EventModules.process(txn, app_name)
  catch
    :exit, _reason ->
      Logger.error("Event handlers exited, the transaction will be redelivered")

      {:error,
       {:handler_failed, %{module: EventModules, function: :process, kind: :exit, exception: nil}}}
  end
end
