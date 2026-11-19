defmodule WalEx.Support.TestHelpersTest do
  use ExUnit.Case, async: true

  alias WalEx.Support.TestHelpers

  describe "parse_published_ports/1" do
    test "reads the host port of every binding" do
      assert [55_432, 55_432] ==
               TestHelpers.parse_published_ports("0.0.0.0:55432\n[::]:55432\n")
    end

    test "is empty when the service publishes nothing" do
      assert [] == TestHelpers.parse_published_ports("")
    end
  end

  describe "pg_restart_plan/1" do
    @target %{host: "localhost", port: 55_432}

    test "restarts compose only when its published port is the one under test" do
      assert {:ok, :docker} =
               TestHelpers.pg_restart_plan(
                 Map.merge(@target, %{compose_ports: [55_432], system: nil, installation: nil})
               )
    end

    test "skips loudly when compose publishes a different port" do
      assert {:skip, reason} =
               TestHelpers.pg_restart_plan(
                 Map.merge(@target, %{compose_ports: [5432], system: nil, installation: nil})
               )

      assert reason =~ "5432"
      assert reason =~ "55432"
    end

    test "skips when the target host is not local, even if the port matches" do
      assert {:skip, _} =
               TestHelpers.pg_restart_plan(%{
                 host: "db.example",
                 port: 55_432,
                 compose_ports: [55_432],
                 system: nil,
                 installation: nil
               })
    end

    test "never restarts a local install unless opted in" do
      assert {:skip, reason} =
               TestHelpers.pg_restart_plan(
                 Map.merge(@target, %{compose_ports: [], system: nil, installation: :linux})
               )

      assert reason =~ "WALEX_PG_RESTART=system"
    end

    test "restarts a local install when opted in" do
      assert {:ok, :system} =
               TestHelpers.pg_restart_plan(
                 Map.merge(@target, %{compose_ports: [], system: "system", installation: :linux})
               )
    end

    test "opted in but nothing installed is a skip" do
      assert {:skip, _} =
               TestHelpers.pg_restart_plan(
                 Map.merge(@target, %{compose_ports: [], system: "system", installation: nil})
               )
    end
  end

  describe "pg_drop_slots/3" do
    test "refuses an empty list of owned slots" do
      assert_raise ArgumentError, fn -> TestHelpers.pg_drop_slots(self(), []) end
    end
  end
end
