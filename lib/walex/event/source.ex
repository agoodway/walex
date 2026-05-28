defmodule WalEx.Event.Source do
  @moduledoc """
  Attribution metadata attached to every `WalEx.Event`.

  Captures the WalEx version, database, schema, table, and column-type map
  so downstream consumers can identify the origin of a change without
  re-querying Postgres.
  """

  @derive Jason.Encoder
  defstruct([:name, :version, :db, :schema, :table, :columns])

  @type t :: %WalEx.Event.Source{
          name: String.t() | nil,
          version: String.t(),
          db: String.t(),
          schema: String.t(),
          table: String.t(),
          columns: map()
        }
end
