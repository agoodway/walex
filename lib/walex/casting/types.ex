defmodule WalEx.Casting.Types do
  @moduledoc """
  Cast from Postgres to Elixir types

  Implementation inspired by Cainophile, Supabase Realtime and Sequin
  """

  # Postgres numeric has arbitrary precision; Decimal 3.x defaults to
  # decimal128 limits (34 digits) and rejects larger values, so keep parsing
  # unbounded to faithfully represent whatever Postgres sends.
  @decimal_opts [max_digits: :infinity, max_exponent: :infinity]

  @doc """
  Casts a PostgreSQL string value to its appropriate Elixir type.

  ## Examples

      iex> cast_record("t", "bool")
      true

      iex> cast_record("123", "int4")
      123

      iex> cast_record("123.45", "numeric")
      #Decimal<123.45>

      iex> cast_record("{1,2,3}", "_int4")
      [1, 2, 3]

      iex> cast_record("2024-01-15T10:30:00Z", "timestamptz")
      #DateTime<2024-01-15 10:30:00Z>

  Special values like NaN and Infinity are handled:

      iex> cast_record("NaN", "float8")
      :nan

  Returns the original value if casting fails.
  """
  def cast_record("t", "bool"), do: true
  def cast_record("f", "bool"), do: false

  # Handle interval type before general integer pattern
  def cast_record(record, "interval") when is_binary(record), do: record

  # Handle special numeric values before general numeric patterns
  def cast_record("NaN", type) when type in ["float4", "float8", "numeric"], do: :nan
  def cast_record("Infinity", type) when type in ["float4", "float8", "numeric"], do: :infinity

  def cast_record("-Infinity", type) when type in ["float4", "float8", "numeric"],
    do: :neg_infinity

  def cast_record(record, <<"int", _::binary>>) when is_binary(record) do
    case Integer.parse(record) do
      {int, _} ->
        int

      :error ->
        record
    end
  end

  def cast_record(record, <<"float", _::binary>>) when is_binary(record) do
    case Float.parse(record) do
      {float, _} ->
        float

      :error ->
        record
    end
  end

  def cast_record(record, "numeric") when is_binary(record),
    do: Decimal.new(record, @decimal_opts)

  def cast_record(record, "decimal"), do: cast_record(record, "numeric")

  def cast_record(record, "timestamp") when is_binary(record) do
    case NaiveDateTime.from_iso8601(record) do
      {:ok, %NaiveDateTime{} = naive} ->
        DateTime.from_naive!(naive, "Etc/UTC")

      _ ->
        record
    end
  end

  def cast_record(record, "timestamptz") when is_binary(record) do
    case DateTime.from_iso8601(record) do
      {:ok, %DateTime{} = date_time, _offset} ->
        date_time

      _ ->
        record
    end
  end

  def cast_record(record, "jsonb") when is_binary(record) do
    case Jason.decode(record) do
      {:ok, json} ->
        json

      _ ->
        record
    end
  end

  def cast_record(record, "json"), do: cast_record(record, "jsonb")

  def cast_record(record, "uuid") when is_binary(record), do: record

  def cast_record(record, "date") when is_binary(record) do
    case Date.from_iso8601(record) do
      {:ok, date} -> date
      _ -> record
    end
  end

  def cast_record(record, "time") when is_binary(record) do
    case Time.from_iso8601(record) do
      {:ok, time} -> time
      _ -> record
    end
  end

  def cast_record(record, "timetz") when is_binary(record) do
    # PostgreSQL timetz format includes timezone offset
    # For now, just parse as regular time
    case Time.from_iso8601(String.slice(record, 0..7)) do
      {:ok, time} -> time
      _ -> record
    end
  end

  def cast_record(record, "money") when is_binary(record) do
    # Remove currency symbol and convert to decimal
    record
    |> String.replace(~r/[^\d.-]/, "")
    |> Decimal.new(@decimal_opts)
  end

  def cast_record(record, "bytea") when is_binary(record) do
    # PostgreSQL bytea hex format starts with \x
    if String.starts_with?(record, "\\x") do
      record
      |> String.slice(2..-1//1)
      |> Base.decode16!(case: :mixed)
    else
      record
    end
  end

  def cast_record(record, "inet") when is_binary(record), do: record
  def cast_record(record, "cidr") when is_binary(record), do: record
  def cast_record(record, "macaddr") when is_binary(record), do: record
  def cast_record(record, "macaddr8") when is_binary(record), do: record

  def cast_record(record, "xml") when is_binary(record), do: record

  # Geometric types - return as strings for now
  def cast_record(record, "point") when is_binary(record), do: record
  def cast_record(record, "line") when is_binary(record), do: record
  def cast_record(record, "lseg") when is_binary(record), do: record
  def cast_record(record, "box") when is_binary(record), do: record
  def cast_record(record, "path") when is_binary(record), do: record
  def cast_record(record, "polygon") when is_binary(record), do: record
  def cast_record(record, "circle") when is_binary(record), do: record

  # Range types - return as strings for now
  def cast_record(record, "int4range") when is_binary(record), do: record
  def cast_record(record, "int8range") when is_binary(record), do: record
  def cast_record(record, "numrange") when is_binary(record), do: record
  def cast_record(record, "tsrange") when is_binary(record), do: record
  def cast_record(record, "tstzrange") when is_binary(record), do: record
  def cast_record(record, "daterange") when is_binary(record), do: record

  # Text search types
  def cast_record(record, "tsvector") when is_binary(record), do: record
  def cast_record(record, "tsquery") when is_binary(record), do: record

  # Other specialized types
  def cast_record(record, "bit") when is_binary(record), do: record
  def cast_record(record, "varbit") when is_binary(record), do: record
  def cast_record(record, "oid") when is_binary(record), do: record
  def cast_record(record, "regclass") when is_binary(record), do: record
  def cast_record(record, "regproc") when is_binary(record), do: record
  def cast_record(record, "regtype") when is_binary(record), do: record
  def cast_record(record, "regrole") when is_binary(record), do: record
  def cast_record(record, "regnamespace") when is_binary(record), do: record

  # PostgreSQL internal types
  def cast_record(record, "name") when is_binary(record), do: record
  def cast_record(record, "pg_lsn") when is_binary(record), do: record
  def cast_record(record, "pg_snapshot") when is_binary(record), do: record
  def cast_record(record, "txid_snapshot") when is_binary(record), do: record

  # Array type casting - integer arrays with support for multidimensional arrays
  def cast_record(array_string, <<"_int", _::binary>>) when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        cast_array_elements(elements, &String.to_integer/1)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - float arrays
  def cast_record(array_string, <<"_float", _::binary>>) when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        cast_array_elements(elements, &String.to_float/1)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - text/varchar arrays
  def cast_record(array_string, column_type)
      when is_binary(array_string) and column_type in ["_text", "_varchar"] do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  # Array type casting - boolean arrays
  def cast_record(array_string, "_bool") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil -> nil
          "t" -> true
          "f" -> false
          other -> other
        end)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - numeric/decimal arrays
  def cast_record(array_string, "_numeric") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil -> nil
          elem -> Decimal.new(elem, @decimal_opts)
        end)

      {:error, _} ->
        array_string
    end
  end

  def cast_record(array_string, "_decimal"), do: cast_record(array_string, "_numeric")

  # Array type casting - timestamptz arrays
  def cast_record(array_string, "_timestamptz") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil ->
            nil

          elem ->
            case DateTime.from_iso8601(elem) do
              {:ok, %DateTime{} = dt, _offset} -> dt
              _ -> elem
            end
        end)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - timestamp arrays
  def cast_record(array_string, "_timestamp") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil ->
            nil

          elem ->
            case NaiveDateTime.from_iso8601(elem) do
              {:ok, %NaiveDateTime{} = naive} ->
                DateTime.from_naive!(naive, "Etc/UTC")

              _ ->
                elem
            end
        end)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - UUID arrays
  def cast_record(array_string, "_uuid") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  # Array type casting - JSONB arrays
  def cast_record(array_string, "_jsonb") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil ->
            nil

          elem ->
            case Jason.decode(elem) do
              {:ok, json} -> json
              _ -> elem
            end
        end)

      {:error, _} ->
        array_string
    end
  end

  def cast_record(array_string, "_json"), do: cast_record(array_string, "_jsonb")

  # Array type casting - date arrays
  def cast_record(array_string, "_date") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil ->
            nil

          elem ->
            case Date.from_iso8601(elem) do
              {:ok, date} -> date
              _ -> elem
            end
        end)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - time arrays
  def cast_record(array_string, "_time") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil ->
            nil

          elem ->
            case Time.from_iso8601(elem) do
              {:ok, time} -> time
              _ -> elem
            end
        end)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - network address arrays (inet, cidr, macaddr)
  def cast_record(array_string, "_inet") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  def cast_record(array_string, "_cidr") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  def cast_record(array_string, "_macaddr") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  # Array type casting - money arrays
  def cast_record(array_string, "_money") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil ->
            nil

          elem ->
            elem
            |> String.replace(~r/[^\d.-]/, "")
            |> Decimal.new(@decimal_opts)
        end)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - bytea arrays
  def cast_record(array_string, "_bytea") when is_binary(array_string) do
    case WalEx.Casting.ArrayParser.parse(array_string) do
      {:ok, elements} ->
        Enum.map(elements, fn
          nil ->
            nil

          elem ->
            if String.starts_with?(elem, "\\x") do
              elem
              |> String.slice(2..-1//1)
              |> Base.decode16!(case: :mixed)
            else
              elem
            end
        end)

      {:error, _} ->
        array_string
    end
  end

  # Fallback - return record unchanged if no specific casting is defined
  def cast_record(record, _column_type) do
    record
  end

  @doc false
  # Helper function to recursively cast array elements, supporting nested arrays
  defp cast_array_elements(elements, cast_fn) do
    Enum.map(elements, fn
      nil ->
        nil

      elem when is_list(elem) ->
        # Handle nested arrays recursively
        cast_array_elements(elem, cast_fn)

      elem ->
        cast_fn.(elem)
    end)
  end
end
