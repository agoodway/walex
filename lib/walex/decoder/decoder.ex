# This file steals liberally from https://github.com/supabase/realtime,
# which in turn draws on https://github.com/cainophile/pgoutput_decoder/blob/master/lib/pgoutput_decoder.ex

require Protocol

defmodule WalEx.Decoder do
  @moduledoc """
  Binary decoder for the Postgres logical replication `pgoutput` plugin.

  `decode_message/1` parses a single replication message (Begin, Commit,
  Origin, Relation, Insert, Update, Delete, Truncate, Type, or Unsupported)
  and returns the matching struct from `WalEx.Decoder.Messages`.
  """

  defmodule Messages do
    @moduledoc """
    Decoded representations of each `pgoutput` replication message.

    See the [pgoutput message format](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
    for the wire-level semantics of each struct.
    """

    @typedoc "Postgres LSN tuple `{xlog_file, xlog_offset}`."
    @type lsn :: {non_neg_integer(), non_neg_integer()}

    defmodule Begin do
      @moduledoc "`B` — start of a transaction; carries the final LSN, commit timestamp, and xid."
      defstruct [:final_lsn, :commit_timestamp, :xid]

      @type t :: %__MODULE__{
              final_lsn: WalEx.Decoder.Messages.lsn() | nil,
              commit_timestamp: DateTime.t() | nil,
              xid: non_neg_integer() | nil
            }
    end

    defmodule Commit do
      @moduledoc "`C` — end of a transaction; carries the commit LSN and timestamp."
      defstruct [:flags, :lsn, :end_lsn, :commit_timestamp]

      @type t :: %__MODULE__{
              flags: [atom()],
              lsn: WalEx.Decoder.Messages.lsn() | nil,
              end_lsn: WalEx.Decoder.Messages.lsn() | nil,
              commit_timestamp: DateTime.t() | nil
            }
    end

    defmodule Origin do
      @moduledoc "`O` — origin marker emitted when a transaction has a logical replication origin."
      defstruct [:origin_commit_lsn, :name]

      @type t :: %__MODULE__{
              origin_commit_lsn: WalEx.Decoder.Messages.lsn() | nil,
              name: String.t() | nil
            }
    end

    defmodule Relation do
      @moduledoc "`R` — relation (table) metadata: identifier, schema, name, replica identity, columns."
      defstruct [:id, :namespace, :name, :replica_identity, :columns]

      @type replica_identity :: :default | :nothing | :all_columns | :index

      @type t :: %__MODULE__{
              id: non_neg_integer() | nil,
              namespace: String.t() | nil,
              name: String.t() | nil,
              replica_identity: replica_identity() | nil,
              columns: [struct()] | nil
            }

      defmodule Column do
        @moduledoc "Per-column metadata within a `Relation` message."
        defstruct [:flags, :name, :type, :type_modifier]

        @type t :: %__MODULE__{
                flags: [atom()],
                name: String.t() | nil,
                type: String.t() | nil,
                type_modifier: non_neg_integer() | nil
              }
      end
    end

    defmodule Insert do
      @moduledoc "`I` — a row insert; `tuple_data` holds the new values."
      defstruct [:relation_id, :tuple_data]

      @type t :: %__MODULE__{
              relation_id: non_neg_integer() | nil,
              tuple_data: tuple() | nil
            }
    end

    defmodule Update do
      @moduledoc "`U` — a row update; carries new values and, if available, the key or old tuple."
      defstruct [:relation_id, :changed_key_tuple_data, :old_tuple_data, :tuple_data]

      @type t :: %__MODULE__{
              relation_id: non_neg_integer() | nil,
              changed_key_tuple_data: tuple() | nil,
              old_tuple_data: tuple() | nil,
              tuple_data: tuple() | nil
            }
    end

    defmodule Delete do
      @moduledoc "`D` — a row delete; carries either the key or the full old tuple, depending on `REPLICA IDENTITY`."
      defstruct [:relation_id, :changed_key_tuple_data, :old_tuple_data]

      @type t :: %__MODULE__{
              relation_id: non_neg_integer() | nil,
              changed_key_tuple_data: tuple() | nil,
              old_tuple_data: tuple() | nil
            }
    end

    defmodule Truncate do
      @moduledoc "`T` — a `TRUNCATE` of one or more relations; options indicate `CASCADE` / `RESTART IDENTITY`."
      defstruct [:number_of_relations, :options, :truncated_relations]

      @type option :: :cascade | :restart_identity

      @type t :: %__MODULE__{
              number_of_relations: non_neg_integer() | nil,
              options: [option()],
              truncated_relations: [non_neg_integer()]
            }
    end

    defmodule Type do
      @moduledoc "`Y` — a type registration for a non-built-in OID."
      defstruct [:id, :namespace, :name]

      @type t :: %__MODULE__{
              id: non_neg_integer() | nil,
              namespace: String.t() | nil,
              name: String.t() | nil
            }
    end

    defmodule Unsupported do
      @moduledoc "Catch-all for replication messages WalEx does not yet decode; carries the raw binary."
      defstruct [:data]

      @type t :: %__MODULE__{data: binary() | nil}
    end
  end

  require Logger

  @pg_epoch DateTime.from_iso8601("2000-01-01T00:00:00Z")

  alias Messages.{
    Begin,
    Commit,
    Delete,
    Insert,
    Origin,
    Relation,
    Relation.Column,
    Truncate,
    Type,
    Unsupported,
    Update
  }

  alias WalEx.OidDatabase

  @doc """
  Parses logical replication messages from Postgres

  ## Examples

      iex> decode_message(<<73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48>>)
      %WalEx.Decoder.Messages.Insert{relation_id: 24576, tuple_data: {"baz", "560"}}

  """
  def decode_message(message) when is_binary(message) do
    # Logger.debug("Message before conversion " <> message)
    decode_message_impl(message)
  end

  defp decode_message_impl(<<"B", lsn::binary-8, timestamp::integer-64, xid::integer-32>>) do
    %Begin{
      final_lsn: decode_lsn(lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp),
      xid: xid
    }
  end

  defp decode_message_impl(
         <<"C", _flags::binary-1, lsn::binary-8, end_lsn::binary-8, timestamp::integer-64>>
       ) do
    %Commit{
      flags: [],
      lsn: decode_lsn(lsn),
      end_lsn: decode_lsn(end_lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp)
    }
  end

  # TODO: Verify this is correct with real data from Postgres
  defp decode_message_impl(<<"O", lsn::binary-8, name::binary>>) do
    %Origin{
      origin_commit_lsn: decode_lsn(lsn),
      name: name
    }
  end

  defp decode_message_impl(<<"R", id::integer-32, rest::binary>>) do
    [
      namespace
      | [name | [<<replica_identity::binary-1, _number_of_columns::integer-16, columns::binary>>]]
    ] = String.split(rest, <<0>>, parts: 3)

    # TODO: Handle case where pg_catalog is blank, we should still return the schema as pg_catalog
    friendly_replica_identity =
      case replica_identity do
        "d" -> :default
        "n" -> :nothing
        "f" -> :all_columns
        "i" -> :index
      end

    %Relation{
      id: id,
      namespace: namespace,
      name: name,
      replica_identity: friendly_replica_identity,
      columns: decode_columns(columns)
    }
  end

  defp decode_message_impl(
         <<"I", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>
       ) do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    %Insert{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }
  end

  defp decode_message_impl(
         <<"U", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>
       ) do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    %Update{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }
  end

  defp decode_message_impl(
         <<"U", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16,
           tuple_data::binary>>
       )
       when key_or_old == "O" or key_or_old == "K" do
    {<<"N", new_number_of_columns::integer-16, new_tuple_binary::binary>>, old_decoded_tuple_data} =
      decode_tuple_data(tuple_data, number_of_columns)

    {<<>>, decoded_tuple_data} = decode_tuple_data(new_tuple_binary, new_number_of_columns)

    base_update_msg = %Update{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }

    case key_or_old do
      "K" -> Map.put(base_update_msg, :changed_key_tuple_data, old_decoded_tuple_data)
      "O" -> Map.put(base_update_msg, :old_tuple_data, old_decoded_tuple_data)
    end
  end

  defp decode_message_impl(
         <<"D", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16,
           tuple_data::binary>>
       )
       when key_or_old == "K" or key_or_old == "O" do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    base_delete_msg = %Delete{
      relation_id: relation_id
    }

    case key_or_old do
      "K" -> Map.put(base_delete_msg, :changed_key_tuple_data, decoded_tuple_data)
      "O" -> Map.put(base_delete_msg, :old_tuple_data, decoded_tuple_data)
    end
  end

  defp decode_message_impl(
         <<"T", number_of_relations::integer-32, options::integer-8, column_ids::binary>>
       ) do
    truncated_relations =
      for relation_id_bin <- column_ids |> :binary.bin_to_list() |> Enum.chunk_every(4),
          do: relation_id_bin |> :binary.list_to_bin() |> :binary.decode_unsigned()

    decoded_options =
      case options do
        0 -> []
        1 -> [:cascade]
        2 -> [:restart_identity]
        3 -> [:cascade, :restart_identity]
      end

    %Truncate{
      number_of_relations: number_of_relations,
      options: decoded_options,
      truncated_relations: truncated_relations
    }
  end

  defp decode_message_impl(<<"Y", data_type_id::integer-32, namespace_and_name::binary>>) do
    [namespace, name_with_null] = :binary.split(namespace_and_name, <<0>>)
    name = String.slice(name_with_null, 0..-2//1)

    %Type{
      id: data_type_id,
      namespace: namespace,
      name: name
    }
  end

  defp decode_message_impl(binary), do: %Unsupported{data: binary}

  defp decode_tuple_data(binary, columns_remaining, accumulator \\ [])

  defp decode_tuple_data(remaining_binary, 0, accumulator) when is_binary(remaining_binary),
    do: {remaining_binary, accumulator |> Enum.reverse() |> List.to_tuple()}

  defp decode_tuple_data(<<"n", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [nil | accumulator])

  defp decode_tuple_data(<<"u", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [:unchanged_toast | accumulator])

  defp decode_tuple_data(
         <<"t", column_length::integer-32, rest::binary>>,
         columns_remaining,
         accumulator
       ),
       do:
         decode_tuple_data(
           :erlang.binary_part(rest, {byte_size(rest), -(byte_size(rest) - column_length)}),
           columns_remaining - 1,
           [:erlang.binary_part(rest, {0, column_length}) | accumulator]
         )

  defp decode_columns(binary, accumulator \\ [])
  defp decode_columns(<<>>, accumulator), do: Enum.reverse(accumulator)

  defp decode_columns(<<flags::integer-8, rest::binary>>, accumulator) do
    [name | [<<data_type_id::integer-32, type_modifier::integer-32, columns::binary>>]] =
      String.split(rest, <<0>>, parts: 2)

    decoded_flags =
      case flags do
        1 -> [:key]
        _ -> []
      end

    decode_columns(columns, [
      %Column{
        name: name,
        flags: decoded_flags,
        type: OidDatabase.name_for_type_id(data_type_id),
        type_modifier: type_modifier
      }
      | accumulator
    ])
  end

  defp pgtimestamp_to_timestamp(microsecond_offset) when is_integer(microsecond_offset) do
    {:ok, epoch, 0} = @pg_epoch

    DateTime.add(epoch, microsecond_offset, :microsecond)
  end

  defp decode_lsn(<<xlog_file::integer-32, xlog_offset::integer-32>>),
    do: {xlog_file, xlog_offset}
end

Protocol.derive(Jason.Encoder, WalEx.Decoder.Messages.Relation.Column)
