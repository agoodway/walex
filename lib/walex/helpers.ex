defmodule WalEx.Helpers do
  @moduledoc """
  Internal formatting helpers used when building `WalEx.Event` structs.

  Produces the `type` string (e.g. `"user.insert"`) attached to each event
  and the `source` string (`"WalEx/<version>"`) included on every event for
  downstream attribution.
  """

  require Logger

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

  @doc false
  # Logs a failure caught with `catch kind, reason` (the only place that has the
  # stacktrace) and returns the details that travel on: the kind and the
  # exception module only, since a message or thrown value can hold row data.
  @spec log_caught_failure(String.t(), :error | :exit | :throw, term(), Exception.stacktrace()) ::
          %{kind: :error | :exit | :throw, exception: module() | nil}
  def log_caught_failure(message, kind, reason, stacktrace) do
    Logger.error(message <> "\n" <> Exception.format(kind, reason, stacktrace))

    %{kind: kind, exception: exception_module(kind, reason, stacktrace)}
  end

  @doc false
  # The exception module of a caught `kind`/`reason` (e.g. `RuntimeError`), or
  # `nil` for a throw or an exit.
  @spec exception_module(:error | :exit | :throw, term(), Exception.stacktrace()) ::
          module() | nil
  def exception_module(:error, reason, stacktrace),
    do: Exception.normalize(:error, reason, stacktrace).__struct__

  def exception_module(_kind, _reason, _stacktrace), do: nil
end
