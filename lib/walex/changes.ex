# This file steals liberally from https://github.com/supabase/realtime,
# which in turn draws on https://github.com/cainophile/cainophile

require Protocol

defmodule WalEx.Changes do
  @moduledoc """
  Internal change records produced after a logical replication transaction
  has been decoded and grouped by `WalEx.Replication.Publisher`.

  A `Transaction` wraps an ordered list of per-row change records — one of
  `NewRecord`, `UpdatedRecord`, `DeletedRecord`, or `TruncatedRelation`.
  These are subsequently mapped into the user-facing `WalEx.Event` structs.
  """

  alias WalEx.{Changes, Decoder.Messages}

  @typedoc "Postgres LSN as `{xlog_file, xlog_offset}` (the decoder's `t:WalEx.Decoder.Messages.lsn/0`)."
  @type lsn :: Messages.lsn()

  @typedoc """
  Position of a change within its transaction, counting from `0` in commit
  order. `{lsn, change_index}` identifies a change and stays the same when the
  transaction is redelivered.
  """
  @type change_index :: non_neg_integer()

  defmodule Transaction do
    @moduledoc "Decoded transaction: its row changes, newest first (sort by `change_index` for commit order), plus the commit timestamp."
    defstruct [:changes, :commit_timestamp]

    @type t :: %__MODULE__{
            changes: [struct()],
            commit_timestamp: DateTime.t() | nil
          }
  end

  defmodule NewRecord do
    @moduledoc "A row inserted within a transaction."
    defstruct [:type, :record, :schema, :table, :columns, :commit_timestamp, :lsn, :change_index]

    @type t :: %__MODULE__{
            type: String.t(),
            record: map() | nil,
            schema: String.t() | nil,
            table: String.t() | nil,
            columns: [struct()] | nil,
            commit_timestamp: DateTime.t() | nil,
            lsn: Changes.lsn() | nil,
            change_index: Changes.change_index() | nil
          }
  end

  defmodule UpdatedRecord do
    @moduledoc "A row updated within a transaction; includes the previous values when `REPLICA IDENTITY FULL` is set."
    defstruct [
      :type,
      :old_record,
      :record,
      :schema,
      :table,
      :columns,
      :commit_timestamp,
      :lsn,
      :change_index
    ]

    @type t :: %__MODULE__{
            type: String.t(),
            old_record: map() | nil,
            record: map() | nil,
            schema: String.t() | nil,
            table: String.t() | nil,
            columns: [struct()] | nil,
            commit_timestamp: DateTime.t() | nil,
            lsn: Changes.lsn() | nil,
            change_index: Changes.change_index() | nil
          }
  end

  defmodule DeletedRecord do
    @moduledoc "A row deleted within a transaction; `old_record` contains the previous values when available."
    defstruct [
      :type,
      :old_record,
      :schema,
      :table,
      :columns,
      :commit_timestamp,
      :lsn,
      :change_index
    ]

    @type t :: %__MODULE__{
            type: String.t(),
            old_record: map() | nil,
            schema: String.t() | nil,
            table: String.t() | nil,
            columns: [struct()] | nil,
            commit_timestamp: DateTime.t() | nil,
            lsn: Changes.lsn() | nil,
            change_index: Changes.change_index() | nil
          }
  end

  defmodule TruncatedRelation do
    @moduledoc "A `TRUNCATE` of one of the subscribed tables."
    defstruct [:type, :schema, :table, :commit_timestamp, :lsn, :change_index]

    @type t :: %__MODULE__{
            type: String.t(),
            schema: String.t() | nil,
            table: String.t() | nil,
            commit_timestamp: DateTime.t() | nil,
            lsn: Changes.lsn() | nil,
            change_index: Changes.change_index() | nil
          }
  end
end

Protocol.derive(Jason.Encoder, WalEx.Changes.Transaction)
Protocol.derive(Jason.Encoder, WalEx.Changes.NewRecord)
Protocol.derive(Jason.Encoder, WalEx.Changes.UpdatedRecord)
Protocol.derive(Jason.Encoder, WalEx.Changes.DeletedRecord)
Protocol.derive(Jason.Encoder, WalEx.Changes.TruncatedRelation)
