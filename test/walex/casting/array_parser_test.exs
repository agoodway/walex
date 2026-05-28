defmodule WalEx.Casting.ArrayParserTest do
  use ExUnit.Case, async: true
  alias WalEx.Casting.ArrayParser

  describe "parse/1" do
    test "parses empty arrays" do
      assert ArrayParser.parse("{}") == {:ok, []}
    end

    test "parses simple integer arrays" do
      assert ArrayParser.parse("{1,2,3}") == {:ok, ["1", "2", "3"]}
    end

    test "parses arrays with spaces" do
      assert ArrayParser.parse("{1, 2, 3}") == {:ok, ["1", " 2", " 3"]}
    end

    test "parses arrays with NULL values" do
      assert ArrayParser.parse("{1,NULL,3}") == {:ok, ["1", nil, "3"]}
      assert ArrayParser.parse("{NULL,NULL}") == {:ok, [nil, nil]}
    end

    test "parses quoted strings" do
      assert ArrayParser.parse(~s({"hello","world"})) == {:ok, ["hello", "world"]}
    end

    test "parses quoted strings with commas" do
      assert ArrayParser.parse(~s({"hello, world","foo"})) == {:ok, ["hello, world", "foo"]}
    end

    test "parses quoted strings with escaped quotes" do
      assert ArrayParser.parse(~s({"say \\"hello\\"","world"})) ==
               {:ok, ["say \"hello\"", "world"]}
    end

    test "parses arrays with backslashes" do
      assert ArrayParser.parse(~s({"a\\\\b","c\\\\d"})) == {:ok, ["a\\b", "c\\d"]}
    end

    test "parses arrays with empty strings" do
      assert ArrayParser.parse(~s({"","x",""})) == {:ok, ["", "x", ""]}
      assert ArrayParser.parse(~s({"","",""})) == {:ok, ["", "", ""]}
    end

    test "parses arrays with whitespace" do
      assert ArrayParser.parse(~s({ "a", "b" , "c" })) ==
               {:ok, [" \"a\"", " \"b\" ", " \"c\" "]}
    end

    test "parses arrays with JSON-like strings" do
      assert ArrayParser.parse(~s({"{\\\"key\\\": \\\"value\\\"}","[1,2,3]"})) ==
               {:ok, ["{\"key\": \"value\"}", "[1,2,3]"]}
    end

    test "parses arrays with quoted braces" do
      assert ArrayParser.parse(~s({"{nested}","not{nested"})) == {:ok, ["{nested}", "not{nested"]}
    end

    test "parses nested arrays" do
      assert ArrayParser.parse("{{1,2},{3,4}}") == {:ok, [["1", "2"], ["3", "4"]]}
    end

    test "parses deeply nested arrays" do
      assert ArrayParser.parse("{{{1,2}},{{3,4}}}") == {:ok, [[["1", "2"]], [["3", "4"]]]}
    end

    test "parses mixed nested arrays" do
      assert ArrayParser.parse("{1,{2,3},4}") == {:ok, ["1", ["2", "3"], "4"]}
    end

    test "parses arrays with quoted nested structures" do
      assert ArrayParser.parse(~s({"{1,2}","normal"})) == {:ok, ["{1,2}", "normal"]}
    end

    test "handles invalid array format" do
      assert ArrayParser.parse("not_an_array") ==
               {:error, "Invalid array format - must start with {"}
    end

    test "handles unclosed arrays" do
      assert ArrayParser.parse("{1,2,3") ==
               {:error, "Unexpected end of array - missing closing }"}
    end

    test "handles unterminated quoted strings" do
      assert ArrayParser.parse(~s({"hello)) ==
               {:error, "Unexpected end of array - unterminated quoted string"}
    end

    test "handles unclosed nested arrays" do
      assert ArrayParser.parse("{{1,2}") ==
               {:error, "Unexpected end of array - missing closing }"}
    end
  end
end
