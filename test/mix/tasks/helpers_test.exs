defmodule Mix.Tasks.Walex.HelpersTest do
  # Changes the process environment, so it cannot run alongside other tests.
  use ExUnit.Case, async: false

  alias Mix.Tasks.Walex.Helpers

  @pg_env ~w(PGHOST PGUSER PGPASSWORD PGPORT)

  setup do
    saved = Map.new(@pg_env, &{&1, System.get_env(&1)})

    on_exit(fn ->
      Enum.each(saved, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end)
  end

  test "the connection config follows the environment at call time, not compile time" do
    System.put_env("PGPORT", "55001")
    assert Helpers.config()[:port] == 55_001

    System.put_env("PGPORT", "55002")
    assert Helpers.config()[:port] == 55_002
  end

  test "the config defaults when the environment is unset" do
    Enum.each(@pg_env, &System.delete_env/1)

    assert Helpers.config() == [
             hostname: "localhost",
             username: "postgres",
             password: "postgres",
             port: 5432
           ]
  end

  test "psql is invoked with the current PG environment" do
    System.put_env("PGHOST", "db.example")
    System.put_env("PGPORT", "55003")

    env = Map.new(Helpers.psql_env())
    assert env["PGHOST"] == "db.example"
    assert env["PGPORT"] == "55003"
  end
end
