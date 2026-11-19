defmodule Mix.Tasks.Walex.Drop do
  @moduledoc """
  Drops the database
  """
  use Mix.Task
  alias Mix.Tasks.Walex.Helpers

  @shortdoc "Tear down test database and tables"
  @doc "Drops the `todos_test` database used by the test suite."
  def run(_) do
    Mix.Task.run("app.start")
    Helpers.drop_database(Helpers.test_database())
  end
end
