defmodule WalEx.Replication.QueryBuilder do
  @moduledoc """
  SQL and replication command strings used by `WalEx.Replication.Server` to
  verify publication state and start logical decoding over the `pgoutput` plugin.

  Each function takes a config map containing the relevant fields
  (`:publication`, `:slot_name`) and returns a raw query string.
  """

  @doc "Query that returns `1` if the configured `publication` exists."
  def publication_exists(%{publication: publication}) do
    "SELECT 1 FROM pg_publication WHERE pubname = '#{publication}' LIMIT 1;"
  end

  @doc "Query that returns the `active` flag for the configured replication slot."
  def slot_exists(%{slot_name: slot_name}) do
    "SELECT active FROM pg_replication_slots WHERE slot_name = '#{slot_name}' LIMIT 1;"
  end

  @doc "Replication command to create a temporary logical slot dropped on disconnect."
  def create_temporary_slot(%{slot_name: slot_name}) do
    "CREATE_REPLICATION_SLOT #{slot_name} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT;"
  end

  @doc "Replication command to create a durable logical slot that survives restarts."
  def create_durable_slot(%{slot_name: slot_name}) do
    "CREATE_REPLICATION_SLOT #{slot_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT;"
  end

  @doc "Replication command that starts streaming WAL changes from the slot for the publication."
  def start_replication_slot(%{slot_name: slot_name, publication: publication}) do
    "START_REPLICATION SLOT #{slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{publication}')"
  end
end
