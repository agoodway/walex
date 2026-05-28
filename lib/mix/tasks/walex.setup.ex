defmodule Mix.Tasks.Walex.Setup do
  @moduledoc """
  Creates, migrates and seeds the database
  """
  use Mix.Task
  alias Mix.Tasks.Walex.Helpers

  @test_database "todos_test"
  @base_configs [
    hostname: System.get_env("PGHOST", "localhost"),
    username: System.get_env("PGUSER", "postgres"),
    password: System.get_env("PGPASSWORD", "postgres"),
    database: @test_database
  ]

  @shortdoc "Set up test database and tables"
  @doc """
  Creates the `todos_test` database, installs extensions, builds the `user`
  and `todo` tables with seed data, and configures logical replication
  (publication `events`, `REPLICA IDENTITY FULL`).
  """
  def run(_) do
    Mix.Task.run("app.start")
    setup_test_database()
  end

  defp setup_test_database do
    Helpers.create_database(@test_database)
    {:ok, pid} = Postgrex.start_link(@base_configs)

    create_database_logic(pid)
    create_database_tables(pid)
    seed_database_tables(pid)
    set_up_logical_replication(pid)
  end

  defp create_database_logic(pid) do
    Helpers.create_extension(pid, "citext")
    Helpers.create_extension(pid, "uuid-ossp")
    create_updated_at_function(pid)
  end

  defp create_database_tables(pid) do
    create_user_table(pid)
    create_updated_at_trigger(pid, "user")
    create_todo_table(pid)
    create_updated_at_trigger(pid, "todo")
  end

  defp set_up_logical_replication(pid) do
    set_wal_level_to_logical(pid)
    set_event_publications(pid)
    set_replica_identity(pid, "user")
    set_replica_identity(pid, "todo")
  end

  defp set_wal_level_to_logical(pid) do
    set_wal_level = "ALTER SYSTEM SET wal_level = \'logical\';"

    Postgrex.query!(pid, set_wal_level, [])
  end

  defp set_event_publications(pid) do
    event_publications = "CREATE PUBLICATION events FOR TABLE \"user\", \"todo\";"

    Postgrex.query!(pid, event_publications, [])
  end

  defp set_replica_identity(pid, table_name) do
    replica_identity = "ALTER TABLE \"#{table_name}\" REPLICA IDENTITY FULL;"

    Postgrex.query!(pid, replica_identity, [])
  end

  defp create_updated_at_function(pid) do
    create_updated_at_function_statement = """
      CREATE OR REPLACE FUNCTION set_current_timestamp_updated_at()
      RETURNS TRIGGER AS $$
      DECLARE
        _new record;
      BEGIN
        _new := NEW;
        _new."updated_at" = NOW();
        RETURN _new;
      END;
      $$ LANGUAGE plpgsql;
    """

    Postgrex.query!(pid, create_updated_at_function_statement, [])
  end

  defp create_updated_at_trigger(pid, table_name) do
    create_updated_at_trigger_statement = """
      CREATE TRIGGER set_#{table_name}_updated_at
      BEFORE UPDATE ON \"#{table_name}\"
      FOR EACH ROW
      EXECUTE PROCEDURE set_current_timestamp_updated_at();
    """

    Postgrex.query!(pid, create_updated_at_trigger_statement, [])
  end

  defp create_user_table(pid) do
    create_user_table_statement = """
      CREATE TABLE "user" (
        id SERIAL PRIMARY KEY,
        email citext UNIQUE NOT NULL,
        name VARCHAR  NOT NULL,
        age INTEGER DEFAULT 0,
        books VARCHAR[] DEFAULT '{}'::VARCHAR[],
        favorite_numbers INTEGER[] DEFAULT '{}'::INTEGER[],
        meta JSONB DEFAULT '{}'::JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    """

    Postgrex.query!(pid, create_user_table_statement, [])
  end

  defp create_todo_table(pid) do
    create_table_statement = """
      CREATE TABLE todo (
          id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
          user_id INTEGER REFERENCES "user"(id) ON DELETE CASCADE,
          description TEXT NOT NULL,
          due_date DATE,
          is_completed BOOLEAN DEFAULT FALSE,
          priority INTEGER DEFAULT 0,
          tags VARCHAR[] DEFAULT '{}'::VARCHAR[],
          rules JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    """

    Postgrex.query!(pid, create_table_statement, [])
  end

  defp seed_database_tables(pid) do
    seed_users(pid)
    seed_todos(pid)
  end

  defp seed_users(pid) do
    users = """
      INSERT INTO \"user\" (email, name, age)
      VALUES
        ('john.doe@example.com', 'John Doe', 28),
        ('jane.smith@example.com', 'Jane Smith', 32),
        ('bob.jones@example.com', 'Bob Jones', 25),
        ('alice.davis@example.com', 'Alice Davis', 34),
        ('charlie.brown@example.com', 'Charlie Brown', 30);
    """

    Postgrex.query!(pid, users, [])
  end

  defp seed_todos(pid) do
    todos = """
      INSERT INTO todo (user_id, description, due_date, is_completed, priority, tags, rules)
      VALUES
        -- User 1 todos
        (1, 'Buy groceries', '2023-01-10', false, 2, ARRAY['groceries', 'shopping'], '{\"reminder\": true, \"repeat\": \"weekly\"}'::JSONB),
        (1, 'Finish work project', '2023-01-15', true, 1, ARRAY['work', 'project'], '{\"priority\": \"high\"}'::JSONB),
        (1, 'Exercise', NULL, false, 3, ARRAY['health', 'fitness'], '{}'::JSONB),

        -- User 2 todos
        (2, 'Read a book', '2023-02-01', true, 2, ARRAY['reading', 'books'], '{\"genre\": \"mystery\"}'::JSONB),
        (2, 'Write a blog post', '2023-02-10', false, 1, ARRAY['writing', 'blog'], '{\"format\": \"tutorial\"}'::JSONB),
        (2, 'Plan vacation', '2023-03-01', false, 3, ARRAY['travel', 'vacation'], '{\"destination\": \"beach\"}'::JSONB),

        -- User 3 todos
        (3, 'Learn a new programming language', '2023-01-20', false, 2, ARRAY['coding', 'programming'], '{\"level\": \"intermediate\"}'::JSONB),
        (3, 'Cook a new recipe', '2023-02-05', false, 1, ARRAY['cooking', 'recipe'], '{\"cuisine\": \"Italian\"}'::JSONB),
        (3, 'Study for exams', '2023-02-28', true, 3, ARRAY['education', 'exams'], '{\"subject\": \"math\"}'::JSONB),

        -- User 4 todos
        (4, 'Explore hiking trails', NULL, true, 2, ARRAY['outdoors', 'hiking'], '{}'::JSONB),
        (4, 'Complete home improvement projects', '2023-03-15', true, 1, ARRAY['home', 'projects'], '{\"room\": \"kitchen\"}'::JSONB),
        (4, 'Attend a music concert', '2023-04-01', false, 3, ARRAY['music', 'concert'], '{\"genre\": \"rock\"}'::JSONB),

        -- User 5 todos
        (5, 'Volunteer at local community center', '2023-02-10', false, 2, ARRAY['community', 'volunteer'], '{\"activity\": \"food drive\"}'::JSONB),
        (5, 'Practice mindfulness', NULL, false, 1, ARRAY['mindfulness', 'meditation'], '{}'::JSONB),
        (5, 'Attend a language exchange meetup', '2023-03-05', true, 3, ARRAY['language', 'meetup'], '{\"languages\": [\"Spanish\", \"French\"]}'::JSONB)
      ;
    """

    Postgrex.query!(pid, todos, [])
  end
end
