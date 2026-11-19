defmodule WalEx.Replication.QueryBuilder do
  @moduledoc """
  SQL and replication command strings used by `WalEx.Replication.Server` to
  verify publication state and start logical decoding over the `pgoutput` plugin.

  Each function takes a config map containing the relevant fields
  (`:publication`, `:slot_name`) and returns a raw query string.

  `:publication` is one name or a list of names; `WalEx.Config` only accepts
  non-empty names without backslash or NUL, and a slot name reduced to `[a-z0-9_]`.
  Postgrex has no quoting helpers, so this module still escapes what it
  interpolates, as a second layer in case a config bypasses that validation.
  """

  alias WalEx.Config

  @doc "Query that returns `1` if every configured `publication` exists."
  def publication_exists(%{publication: publication}) do
    case List.wrap(publication) do
      [name] ->
        "SELECT 1 FROM pg_publication WHERE pubname = #{literal(name)} LIMIT 1;"

      names ->
        "SELECT 1 WHERE (SELECT count(*) FROM pg_publication WHERE pubname IN (#{Enum.map_join(names, ", ", &literal/1)})) = #{length(Enum.uniq(names))};"
    end
  end

  @doc """
  Query that returns the `active` flag and `confirmed_flush_lsn` for the
  configured replication slot, and with `ack_after_processing` also its
  `wal_status` (Postgres 13+), to detect an invalidated slot. Only this
  database's `pgoutput` slots are considered.
  """
  def slot_exists(%{slot_name: slot_name, ack_after_processing: true}) do
    "SELECT active, confirmed_flush_lsn, wal_status FROM pg_replication_slots WHERE #{slot_filter(slot_name)} LIMIT 1;"
  end

  def slot_exists(%{slot_name: slot_name}) do
    "SELECT active, confirmed_flush_lsn FROM pg_replication_slots WHERE #{slot_filter(slot_name)} LIMIT 1;"
  end

  @doc "Replication command to create a temporary logical slot dropped on disconnect."
  def create_temporary_slot(%{slot_name: slot_name}) do
    "CREATE_REPLICATION_SLOT #{quoted_slot(slot_name)} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT;"
  end

  @doc "Replication command to create a durable logical slot that survives restarts."
  def create_durable_slot(%{slot_name: slot_name}) do
    "CREATE_REPLICATION_SLOT #{quoted_slot(slot_name)} LOGICAL pgoutput NOEXPORT_SNAPSHOT;"
  end

  @doc "Replication command that starts streaming WAL changes from the slot for the publication."
  def start_replication_slot(%{slot_name: slot_name, publication: publication}) do
    names = publication |> List.wrap() |> Enum.map_join(",", &quoted/1)

    "START_REPLICATION SLOT #{quoted_slot(slot_name)} LOGICAL 0/0 (proto_version '1', publication_names #{literal(names)})"
  end

  defp slot_filter(slot_name),
    do:
      "slot_name = #{literal(slot_name)} AND database = current_database() AND plugin = 'pgoutput'"

  # A single-quoted SQL string literal. With standard_conforming_strings off a
  # backslash can escape the closing quote, and NUL is never valid, so neither is
  # escaped: they are refused.
  defp literal(value) do
    value = to_string(value)

    if String.contains?(value, ["\\", <<0>>]),
      do: raise(ArgumentError, "refusing backslash or NUL in a SQL literal: #{inspect(value)}"),
      else: "'" <> String.replace(value, "'", "''") <> "'"
  end

  # Always double-quoted so pgoutput resolves the name exactly as stored
  # (unquoted names would be folded to lower case).
  defp quoted(value), do: ~s["#{String.replace(to_string(value), ~s["], ~s[""])}"]

  # Slot names are sanitized to [a-z0-9_]+ by WalEx.Config, so refuse anything
  # else rather than escape it. Double-quoted so keyword names (`logical`,
  # `slot`, ...) still parse in the replication grammar.
  defp quoted_slot(slot_name) do
    slot_name = to_string(slot_name)

    if Config.valid_slot_name?(slot_name),
      do: ~s["#{slot_name}"],
      else: raise(ArgumentError, "invalid replication slot name: #{inspect(slot_name)}")
  end
end
