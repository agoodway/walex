defmodule WalEx.Replication.Supervisor do
  @moduledoc """
  Supervises the per-app replication pipeline (`WalEx.Replication.Publisher`
  and `WalEx.Replication.Server`).

  Uses a `:one_for_all` strategy because the Publisher's in-memory
  Relation/Type tables must stay in sync with the Server's LSN stream — see
  the inline comments in `init/1` for the failure-mode analysis.
  """

  use Supervisor

  alias WalEx.Config.Registry, as: WalExRegistry
  alias WalEx.Replication.{Publisher, Server}

  @doc "Starts the replication supervisor registered under the app name."
  def start_link(opts) do
    app_name = Keyword.get(opts, :app_name)
    name = WalExRegistry.set_name(:set_supervisor, __MODULE__, app_name)

    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    app_name =
      opts
      |> Keyword.get(:app_name)

    children = [
      {Publisher, app_name: app_name},
      {Server, app_name: app_name}
    ]

    # one_for_all (or rest_for_one) is required here, reason being:
    #
    # if Publisher crashes:
    #   We lost the current state.
    #   This means that until Postgres decides to send us all the needed Relations and Types messages again,
    #   we won't be able to decode any events from the Server.
    #   In the mid time everything would look ok but all events would get discarded.
    #   The only way to guarantee to get those back is to restart the Server.

    # if Server crashes:
    #   The replication will restart at restart_lsn.
    #   All events from then to the LSN at which the Server crashed will get replayed.
    #   The means that the message inbox of the Publisher will become potentially inconsistent
    #   and will likely contain duplicate messages.
    #   If this is undesirable, one_for_all is required otherwise rest_for_one is fine.
    Supervisor.init(children, strategy: :one_for_all, max_restarts: 10)
  end
end
