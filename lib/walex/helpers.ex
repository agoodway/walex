defmodule WalEx.Helpers do
  @moduledoc """
  Internal formatting helpers used when building `WalEx.Event` structs.

  Produces the `type` string (e.g. `"user.insert"`) attached to each event
  and the `source` string (`"WalEx/<version>"`) included on every event for
  downstream attribution.
  """

  @doc """
  Returns the dotted `"<table>.<change>"` event type for a given change kind.

  ## Examples

      iex> WalEx.Helpers.set_type("user", :insert)
      "user.insert"

  """
  def set_type(table, :insert), do: to_string(table) <> ".insert"
  def set_type(table, :update), do: to_string(table) <> ".update"
  def set_type(table, :delete), do: to_string(table) <> ".delete"

  @doc """
  Returns the source identifier (`"WalEx/<version>"`) for outgoing events.
  """
  def set_source, do: get_source_name() <> "/" <> get_source_version()

  @doc "Returns the source name component (`\"WalEx\"`) used in event attribution."
  def get_source_name, do: "WalEx"

  @doc "Returns the running WalEx version as a string (from `Application.spec/2`)."
  def get_source_version, do: Application.spec(:walex)[:vsn] |> to_string()
end
