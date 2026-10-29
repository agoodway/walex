defmodule WalEx.Events.EventModulesConnectTest do
  use ExUnit.Case, async: false

  alias WalEx.Config
  alias WalEx.Events.EventModules

  @hostname System.get_env("PGHOST", "localhost")
  @username System.get_env("PGUSER", "postgres")
  @password System.get_env("PGPASSWORD", "postgres")
  @port String.to_integer(System.get_env("PGPORT", "5432"))

  defmodule WithHook do
    use WalEx.Event, name: :connect_hook_app

    on_connect(fn info -> send(Process.whereis(:connect_hook_test), {:with_hook, info}) end)
  end

  defmodule Raising do
    use WalEx.Event, name: :connect_hook_app

    on_connect(fn _info -> raise "boom" end)
  end

  defmodule WithoutHook do
    use WalEx.Event, name: :connect_hook_app

    on_event(:all, fn _events -> :ok end)
  end

  setup do
    start_supervised!(WalEx.Config.Registry.child_spec())
    Process.register(self(), :connect_hook_test)

    {:ok, _} =
      Config.start_link(
        configs: [
          name: :connect_hook_app,
          hostname: @hostname,
          username: @username,
          password: @password,
          database: "postgres",
          port: @port,
          ssl: false,
          ssl_opts: [verify: :verify_none],
          publication: "publication",
          subscriptions: ["subscriptions"],
          modules: [Raising, WithoutHook, WithHook]
        ]
      )

    pid = start_supervised!({EventModules, app_name: :connect_hook_app})
    %{pid: pid}
  end

  test "on_connect defines process_connect/1 on the module" do
    assert function_exported?(WithHook, :process_connect, 1)
    refute function_exported?(WithoutHook, :process_connect, 1)
  end

  test "connected/2 runs the hook of every module that has one" do
    EventModules.connected(:connect_hook_app, %{app_name: :connect_hook_app, reconnect?: true})

    assert_receive {:with_hook, %{reconnect?: true}}
  end

  test "a hook that raises neither stops the others nor the process", %{pid: pid} do
    EventModules.connected(:connect_hook_app, %{app_name: :connect_hook_app, reconnect?: false})

    assert_receive {:with_hook, %{reconnect?: false}}
    assert Process.alive?(pid)
  end

  test "connected/2 returns :ok even when nothing is listening" do
    assert :ok = EventModules.connected(:nobody_home, %{app_name: :nobody_home})
  end
end
