defmodule WalEx.Casting.TypesTest do
  use ExUnit.Case, async: true
  alias WalEx.Casting.Types

  describe "boolean casting" do
    test "casts 't' to true" do
      assert Types.cast_record("t", "bool") == true
    end

    test "casts 'f' to false" do
      assert Types.cast_record("f", "bool") == false
    end
  end

  describe "integer casting" do
    test "casts int2" do
      assert Types.cast_record("123", "int2") == 123
      assert Types.cast_record("-456", "int2") == -456
    end

    test "casts int4" do
      assert Types.cast_record("123456", "int4") == 123_456
      assert Types.cast_record("-789012", "int4") == -789_012
    end

    test "casts int8" do
      assert Types.cast_record("9223372036854775807", "int8") == 9_223_372_036_854_775_807
    end

    test "returns original on invalid integer" do
      assert Types.cast_record("not_a_number", "int4") == "not_a_number"
    end
  end

  describe "float casting" do
    test "casts float4" do
      assert Types.cast_record("123.45", "float4") == 123.45
      assert Types.cast_record("-67.89", "float4") == -67.89
    end

    test "casts float8" do
      assert Types.cast_record("123.456789", "float8") == 123.456789
    end

    test "returns original on invalid float" do
      assert Types.cast_record("not_a_float", "float8") == "not_a_float"
    end
  end

  describe "numeric/decimal casting" do
    test "casts numeric" do
      result = Types.cast_record("123.456", "numeric")
      assert %Decimal{} = result
      assert Decimal.to_string(result) == "123.456"
    end

    test "casts decimal" do
      result = Types.cast_record("789.012", "decimal")
      assert %Decimal{} = result
      assert Decimal.to_string(result) == "789.012"
    end
  end

  describe "timestamp casting" do
    test "casts timestamp" do
      result = Types.cast_record("2024-01-15 10:30:00.123456", "timestamp")
      assert %DateTime{} = result
      assert result.year == 2024
      assert result.month == 1
      assert result.day == 15
    end

    test "casts timestamptz" do
      result = Types.cast_record("2024-01-15 10:30:00.123456+00", "timestamptz")
      assert %DateTime{} = result
      assert result.year == 2024
      assert result.time_zone == "Etc/UTC"
    end

    test "returns original on invalid timestamp" do
      assert Types.cast_record("not_a_timestamp", "timestamp") == "not_a_timestamp"
    end
  end

  describe "date and time casting" do
    test "casts date" do
      result = Types.cast_record("2024-01-15", "date")
      assert %Date{} = result
      assert result.year == 2024
      assert result.month == 1
      assert result.day == 15
    end

    test "casts time" do
      result = Types.cast_record("10:30:45", "time")
      assert %Time{} = result
      assert result.hour == 10
      assert result.minute == 30
      assert result.second == 45
    end

    test "casts timetz" do
      result = Types.cast_record("10:30:45+02", "timetz")
      assert %Time{} = result
      assert result.hour == 10
      assert result.minute == 30
    end
  end

  describe "json/jsonb casting" do
    test "casts jsonb" do
      result = Types.cast_record(~s({"key": "value", "number": 123}), "jsonb")
      assert result == %{"key" => "value", "number" => 123}
    end

    test "casts json" do
      result = Types.cast_record(~s({"key": "value"}), "json")
      assert result == %{"key" => "value"}
    end

    test "returns original on invalid json" do
      assert Types.cast_record("not_json", "jsonb") == "not_json"
    end
  end

  describe "UUID casting" do
    test "returns UUID as-is" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      assert Types.cast_record(uuid, "uuid") == uuid
    end
  end

  describe "money casting" do
    test "casts money values" do
      result = Types.cast_record("$123.45", "money")
      assert %Decimal{} = result
      assert Decimal.to_string(result) == "123.45"
    end

    test "handles negative money" do
      result = Types.cast_record("-$67.89", "money")
      assert %Decimal{} = result
      assert Decimal.to_string(result) == "-67.89"
    end
  end

  describe "bytea casting" do
    test "decodes hex bytea" do
      # "Hello" in hex
      assert Types.cast_record("\\x48656c6c6f", "bytea") == "Hello"
    end

    test "returns original for non-hex bytea" do
      assert Types.cast_record("not_hex", "bytea") == "not_hex"
    end
  end

  describe "network types" do
    test "returns inet as-is" do
      assert Types.cast_record("192.168.1.1", "inet") == "192.168.1.1"
    end

    test "returns cidr as-is" do
      assert Types.cast_record("192.168.0.0/24", "cidr") == "192.168.0.0/24"
    end

    test "returns macaddr as-is" do
      assert Types.cast_record("08:00:2b:01:02:03", "macaddr") == "08:00:2b:01:02:03"
    end
  end

  describe "integer array casting" do
    test "casts integer arrays" do
      assert Types.cast_record("{1,2,3}", "_int4") == [1, 2, 3]
      assert Types.cast_record("{-1,0,100}", "_int8") == [-1, 0, 100]
    end

    test "handles empty integer arrays" do
      assert Types.cast_record("{}", "_int4") == []
    end
  end

  describe "float array casting" do
    test "casts float arrays" do
      assert Types.cast_record("{1.5,2.7,3.9}", "_float4") == [1.5, 2.7, 3.9]
      assert Types.cast_record("{-1.1,0.0,100.99}", "_float8") == [-1.1, 0.0, 100.99]
    end

    test "handles empty float arrays" do
      assert Types.cast_record("{}", "_float8") == []
    end
  end

  describe "text array casting" do
    test "casts text arrays" do
      assert Types.cast_record("{hello,world}", "_text") == ["hello", "world"]
      assert Types.cast_record("{one,two,three}", "_varchar") == ["one", "two", "three"]
    end

    test "handles quoted strings with commas" do
      result = Types.cast_record(~s({\"hello, world\",\"foo, bar\"}), "_text")
      assert result == ["hello, world", "foo, bar"]
    end

    test "handles varchar arrays with commas in quoted strings" do
      # Test case: book titles that contain commas and numbers
      result = Types.cast_record(~s({"book1, 2 and 3","book4"}), "_varchar")
      assert result == ["book1, 2 and 3", "book4"]
    end

    test "handles empty text arrays" do
      assert Types.cast_record("{}", "_text") == []
    end
  end

  describe "boolean array casting" do
    test "casts boolean arrays" do
      assert Types.cast_record("{t,f,t}", "_bool") == [true, false, true]
    end

    test "handles empty boolean arrays" do
      assert Types.cast_record("{}", "_bool") == []
    end
  end

  describe "numeric array casting" do
    test "casts numeric arrays" do
      result = Types.cast_record("{123.45,67.89}", "_numeric")
      assert [d1, d2] = result
      assert Decimal.to_string(d1) == "123.45"
      assert Decimal.to_string(d2) == "67.89"
    end

    test "casts decimal arrays" do
      result = Types.cast_record("{1.1,2.2}", "_decimal")
      assert length(result) == 2
    end
  end

  describe "timestamp array casting" do
    test "casts timestamptz arrays" do
      result =
        Types.cast_record(
          ~s({"2024-01-15 10:30:00.123+00","2024-01-16 11:45:00.456+00"}),
          "_timestamptz"
        )

      assert [dt1, dt2] = result
      assert %DateTime{} = dt1
      assert dt1.year == 2024
      assert %DateTime{} = dt2
      assert dt2.day == 16
    end

    test "handles empty timestamp arrays" do
      assert Types.cast_record("{}", "_timestamptz") == []
    end
  end

  describe "UUID array casting" do
    test "casts UUID arrays" do
      result =
        Types.cast_record(
          "{550e8400-e29b-41d4-a716-446655440000,550e8400-e29b-41d4-a716-446655440001}",
          "_uuid"
        )

      assert length(result) == 2
      assert Enum.at(result, 0) == "550e8400-e29b-41d4-a716-446655440000"
    end
  end

  describe "complex array scenarios" do
    test "handles arrays with NULL values" do
      assert Types.cast_record("{1,NULL,3}", "_int4") == [1, nil, 3]
      assert Types.cast_record("{NULL,NULL}", "_int4") == [nil, nil]
    end

    test "handles nested integer arrays" do
      result = Types.cast_record("{{1,2},{3,4}}", "_int4")
      assert result == [[1, 2], [3, 4]]
    end

    test "handles deeply nested arrays" do
      result = Types.cast_record("{{{1,2}}}", "_int4")
      assert result == [[[1, 2]]]
    end

    test "handles text arrays with special characters" do
      result = Types.cast_record(~s({"hello, world","foo\\\\bar"}), "_text")
      assert result == ["hello, world", "foo\\bar"]
    end

    test "handles mixed content in JSONB arrays" do
      result = Types.cast_record(~s({"{\\\"a\\\": 1}","[1,2,3]","null"}), "_jsonb")
      assert result == [%{"a" => 1}, [1, 2, 3], nil]
    end
  end

  describe "edge cases for specific types" do
    test "handles very large integers" do
      large_int = "9223372036854775807"
      assert Types.cast_record(large_int, "int8") == 9_223_372_036_854_775_807
    end

    test "handles high precision decimals" do
      result = Types.cast_record("123.4567890123456789", "numeric")
      assert Decimal.to_string(result) == "123.4567890123456789"
    end

    test "handles special numeric values" do
      assert Types.cast_record("NaN", "float4") == :nan
      assert Types.cast_record("NaN", "float8") == :nan
      assert Types.cast_record("NaN", "numeric") == :nan

      assert Types.cast_record("Infinity", "float4") == :infinity
      assert Types.cast_record("Infinity", "float8") == :infinity
      assert Types.cast_record("Infinity", "numeric") == :infinity

      assert Types.cast_record("-Infinity", "float4") == :neg_infinity
      assert Types.cast_record("-Infinity", "float8") == :neg_infinity
      assert Types.cast_record("-Infinity", "numeric") == :neg_infinity
    end

    test "handles interval type" do
      # Intervals are kept as strings for now
      assert Types.cast_record("1 year 2 months 3 days", "interval") == "1 year 2 months 3 days"
    end

    test "handles network addresses" do
      assert Types.cast_record("192.168.1.1/24", "cidr") == "192.168.1.1/24"
      assert Types.cast_record("08:00:2b:01:02:03", "macaddr") == "08:00:2b:01:02:03"
      assert Types.cast_record("08:00:2b:01:02:03:04:05", "macaddr8") == "08:00:2b:01:02:03:04:05"
    end

    test "handles range types" do
      assert Types.cast_record("[2023-01-01,2023-12-31]", "daterange") ==
               "[2023-01-01,2023-12-31]"

      assert Types.cast_record("[1,10)", "int4range") == "[1,10)"
      assert Types.cast_record("[1.5,2.5]", "numrange") == "[1.5,2.5]"
    end

    test "handles geometric types as strings" do
      assert Types.cast_record("(1,2)", "point") == "(1,2)"
      assert Types.cast_record("((1,1),(2,2))", "box") == "((1,1),(2,2))"
      assert Types.cast_record("<(1,2),3>", "circle") == "<(1,2),3>"
    end

    test "handles XML data" do
      xml = "<root><child>value</child></root>"
      assert Types.cast_record(xml, "xml") == xml
    end
  end

  describe "fallback casting" do
    test "returns original for unknown types" do
      assert Types.cast_record("some_value", "unknown_type") == "some_value"
    end
  end
end
