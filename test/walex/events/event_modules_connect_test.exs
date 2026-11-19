defmodule WalEx.Events.EventModulesConnectTest do
  use ExUnit.Case, async: false

  import WalEx.Support.TestHelpers, only: [pg_config: 0, start_config!: 2]

  alias WalEx.{Config, Config.Registry}
  alias WalEx.Events.EventModules

  @pg_config pg_config()

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

  defmodule SlowHook do
    use WalEx.Event, name: :connect_hook_app

    on_connect(fn info ->
      send(Process.whereis(:connect_hook_test), {:slow_hook, info})
      Process.sleep(100)
    end)
  end

  setup context do
    start_supervised!(Registry.child_spec())
    Process.register(self(), :connect_hook_test)

    {:ok, _} =
      Config.start_link(
        configs:
          [
            name: :connect_hook_app,
            hostname: @pg_config[:hostname],
            username: @pg_config[:username],
            password: @pg_config[:password],
            database: "postgres",
            port: @pg_config[:port],
            ssl: false,
            ssl_opts: [verify: :verify_none],
            publication: "publication",
            subscriptions: ["subscriptions"],
            modules: Map.get(context, :modules, [Raising, WithoutHook, WithHook])
          ] ++ Map.get(context, :extra_config, [])
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
    start_config!(:nobody_home, modules: [WithHook])
    assert :ok = EventModules.connected(:nobody_home, %{app_name: :nobody_home})
  end

  test "with ack_after_processing, connected/2 raises when configured modules are not running" do
    start_config!(:hooks_down,
      modules: [WithHook],
      ack_after_processing: true,
      durable_slot: true
    )

    assert_raise RuntimeError, ~r/not running/, fn ->
      EventModules.connected(:hooks_down, %{app_name: :hooks_down})
    end
  end

  describe "timing" do
    @info %{app_name: :connect_hook_app, reconnect?: false}

    @tag modules: [SlowHook]
    @tag extra_config: [ack_after_processing: true, durable_slot: true]
    test "with ack_after_processing, connected/2 returns only after the hook ran" do
      assert :ok = EventModules.connected(:connect_hook_app, @info)

      # No timeout: the hook must already have run when connected/2 returned.
      assert_received {:slow_hook, %{reconnect?: false}}
    end

    @tag modules: [SlowHook]
    test "without ack_after_processing, connected/2 returns before the hook finishes" do
      assert :ok = EventModules.connected(:connect_hook_app, @info)

      refute_received {:slow_hook, _}
      assert_receive {:slow_hook, _}
    end
  end
end
