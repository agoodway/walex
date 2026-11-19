defmodule WalEx.Replication.QueryBuilderTest do
  use ExUnit.Case, async: true

  alias WalEx.Replication.QueryBuilder

  describe "lookups" do
    test "escape single quotes in literals" do
      assert QueryBuilder.publication_exists(%{publication: "a'b"}) =~ "pubname = 'a''b'"

      assert QueryBuilder.slot_exists(%{slot_name: "a'b"}) =~ "slot_name = 'a''b'"
    end

    test "publication_exists accepts a list" do
      assert QueryBuilder.publication_exists(%{publication: ["a", "b"]}) =~
               "pubname IN ('a', 'b')"
    end

    test "slot_exists is scoped to this database and pgoutput" do
      for config <- [%{slot_name: "s"}, %{slot_name: "s", ack_after_processing: true}] do
        sql = QueryBuilder.slot_exists(config)
        assert sql =~ "database = current_database()"
        assert sql =~ "plugin = 'pgoutput'"
      end
    end
  end

  describe "start_replication_slot/1" do
    test "quotes publication names as identifiers" do
      assert QueryBuilder.start_replication_slot(%{slot_name: "s", publication: "Events"}) =~
               ~s[publication_names '"Events"']
    end

    test "joins a list and doubles quotes" do
      sql = QueryBuilder.start_replication_slot(%{slot_name: "s", publication: ["a", ~s[b"'c]]})
      assert sql =~ ~s[publication_names '"a","b""''c"']
    end
  end

  describe "literals" do
    test "refuse backslashes and NUL (unsafe with standard_conforming_strings off)" do
      for bad <- ["a\\b", "a\0b", "x\\'; SELECT 1; --"] do
        assert_raise ArgumentError, fn -> QueryBuilder.publication_exists(%{publication: bad}) end

        assert_raise ArgumentError, fn ->
          QueryBuilder.publication_exists(%{publication: ["ok", bad]})
        end

        assert_raise ArgumentError, fn -> QueryBuilder.slot_exists(%{slot_name: bad}) end
      end
    end
  end

  describe "slot commands" do
    test "double-quote the slot name so keyword names parse" do
      assert ~s[CREATE_REPLICATION_SLOT "logical" TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT;] ==
               QueryBuilder.create_temporary_slot(%{slot_name: "logical"})

      assert ~s[CREATE_REPLICATION_SLOT "slot" LOGICAL pgoutput NOEXPORT_SNAPSHOT;] ==
               QueryBuilder.create_durable_slot(%{slot_name: "slot"})

      assert QueryBuilder.start_replication_slot(%{slot_name: "slot", publication: "p"}) =~
               ~s[START_REPLICATION SLOT "slot" LOGICAL]
    end

    test "refuse a slot name outside [a-z0-9_]" do
      for bad <- [~s[a"b], "a b", "a;b", ""] do
        assert_raise ArgumentError, fn -> QueryBuilder.create_durable_slot(%{slot_name: bad}) end
      end
    end
  end
end
