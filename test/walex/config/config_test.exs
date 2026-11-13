defmodule WalEx.ConfigTest do
  use ExUnit.Case, async: false

  alias WalEx.Config
  alias WalEx.Config.Registry, as: WalExRegistry
  alias WalEx.Replication.Publisher

  @app_name :my_app
  @hostname "hostname"
  @username "username"
  @password "password"
  @database "database"

  @base_configs [
    name: @app_name,
    hostname: @hostname,
    username: @username,
    password: @password,
    database: @database,
    port: 5432,
    ssl: false,
    ssl_opts: [verify: :verify_none],
    socket_options: [],
    subscriptions: ["subscriptions"],
    publication: "publication",
    modules: [MyApp.CustomModule]
  ]

  setup_all do
    {:ok, _pid} = WalExRegistry.start_registry()
    :timer.sleep(1000)
    :ok
  end

  describe "start_link/2" do
    test "should start a process" do
      assert {:ok, pid} = Config.start_link(configs: @base_configs)
      assert is_pid(pid)
    end

    test "should accept database url as config and split it into the right configs" do
      configs = [
        name: @app_name,
        url: "postgres://username:password@hostname:5432/database"
      ]

      assert {:ok, pid} = Config.start_link(configs: configs)
      assert is_pid(pid)

      assert [
               hostname: @hostname,
               username: @username,
               password: @password,
               database: @database,
               port: 5432,
               ssl: false,
               ssl_opts: [verify: :verify_none]
             ] ==
               Config.get_configs(
                 @app_name,
                 [
                   :hostname,
                   :username,
                   :password,
                   :database,
                   :port,
                   :ssl,
                   :ssl_opts
                 ]
               )
    end
  end

  describe "get_configs/" do
    setup do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      :ok
    end

    test "should return all configs" do
      assert [
               name: @app_name,
               hostname: @hostname,
               username: @username,
               password: @password,
               port: 5432,
               database: @database,
               ssl: false,
               ssl_opts: [verify: :verify_none],
               socket_options: [],
               subscriptions: ["subscriptions"],
               publication: "publication",
               modules: [MyApp.CustomModule],
               webhook_signing_secret: nil,
               event_relay: nil,
               slot_name: "my_app_walex",
               durable_slot: false,
               ack_after_processing: false,
               standby_status_interval: nil,
               message_middleware: &Publisher.process_message_async/2
             ] == Config.get_configs(@app_name)
    end
  end

  describe "get_configs/2" do
    setup do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      :ok
    end

    test "should return only selected configs when second parameter is an atom" do
      assert ["subscriptions"] == Config.get_configs(@app_name, :subscriptions)
    end

    test "should return only selected configs when second parameter is a list" do
      assert [
               hostname: @hostname,
               ssl: false,
               ssl_opts: [verify: :verify_none]
             ] ==
               Config.get_configs(@app_name, [:hostname, :ssl, :ssl_opts])
    end

    test "should filter configs by process name" do
      configs =
        @base_configs
        |> Keyword.replace(:name, :other_name)
        |> Keyword.replace(:database, "other_database")

      assert {:ok, pid} = Config.start_link(configs: configs)
      assert is_pid(pid)

      assert [
               name: @app_name,
               database: @database,
               ssl: false,
               ssl_opts: [verify: :verify_none]
             ] ==
               Config.get_configs(@app_name, [:name, :database, :ssl, :ssl_opts])

      assert [
               name: :other_name,
               database: "other_database",
               ssl: false,
               ssl_opts: [verify: :verify_none]
             ] ==
               Config.get_configs(:other_name, [:name, :database, :ssl, :ssl_opts])
    end
  end

  describe "add_config/3" do
    setup do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      :ok
    end

    test "should add new values when new_values is a list" do
      Config.add_config(@app_name, :subscriptions, [
        "new_subscriptions_1",
        "new_subscriptions_2"
      ])

      assert ["subscriptions", "new_subscriptions_1", "new_subscriptions_2"] ==
               Config.get_configs(@app_name)[:subscriptions]
    end

    test "should add new values when new_value is not a list" do
      Config.add_config(@app_name, :subscriptions, "new_subscriptions")

      assert ["subscriptions", "new_subscriptions"] ==
               Config.get_configs(@app_name)[:subscriptions]
    end
  end

  describe "remove_config/3" do
    setup do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      :ok
    end

    test "should remove existing value from list if it exists" do
      Config.add_config(@app_name, :subscriptions, [
        "new_subscriptions_1",
        "new_subscriptions_2"
      ])

      assert ["subscriptions", "new_subscriptions_1", "new_subscriptions_2"] ==
               Config.get_configs(@app_name)[:subscriptions]

      Config.remove_config(@app_name, :subscriptions, "subscriptions")

      assert ["new_subscriptions_1", "new_subscriptions_2"] ==
               Config.get_configs(@app_name)[:subscriptions]
    end
  end

  describe "replace_config/3" do
    setup do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      :ok
    end

    test "should replace existing value when value is not a list" do
      assert @password == Config.get_configs(@app_name)[:password]

      Config.replace_config(@app_name, :password, "new_password")

      assert "new_password" == Config.get_configs(@app_name)[:password]
    end

    test "should not allow options that are read once at startup" do
      for key <- [:ack_after_processing, :standby_status_interval] do
        assert_raise FunctionClauseError, fn ->
          Config.replace_config(@app_name, key, true)
        end
      end
    end

    test "should allow replacing durable_slot and message_middleware without ack_after_processing" do
      middleware = fn _message, _app -> :ok end

      assert :ok = Config.replace_config(@app_name, :message_middleware, middleware)
      assert :ok = Config.replace_config(@app_name, :durable_slot, true)

      assert middleware == Config.get_configs(@app_name, :message_middleware)
      assert true == Config.get_configs(@app_name, :durable_slot)
    end
  end

  describe "replace_config/3 with ack_after_processing" do
    @ack_app :replace_ack_app

    setup do
      configs =
        @base_configs
        |> Keyword.put(:name, @ack_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)

      {:ok, _pid} = Config.start_link(configs: configs)
      :ok
    end

    test "rejects an asynchronous message_middleware and keeps the old one" do
      before = Config.get_configs(@ack_app)

      assert_raise ArgumentError, ~r/cannot replace :message_middleware/, fn ->
        Config.replace_config(
          @ack_app,
          :message_middleware,
          &Publisher.process_message_async/2
        )
      end

      assert before == Config.get_configs(@ack_app)
    end

    test "rejects a custom message_middleware" do
      assert_raise ArgumentError, ~r/synchronous message_middleware/, fn ->
        Config.replace_config(@ack_app, :message_middleware, fn _message, _app -> :ok end)
      end

      assert (&Publisher.process_message_sync/2) ==
               Config.get_configs(@ack_app, :message_middleware)
    end

    test "rejects durable_slot: false and keeps the slot durable" do
      assert_raise ArgumentError,
                   ~r/cannot replace :durable_slot.*requires durable_slot: true/,
                   fn ->
                     Config.replace_config(@ack_app, :durable_slot, false)
                   end

      assert true == Config.get_configs(@ack_app, :durable_slot)
    end

    test "accepts values that keep the guarantee" do
      assert :ok =
               Config.replace_config(
                 @ack_app,
                 :message_middleware,
                 &Publisher.process_message_sync/2
               )

      assert :ok = Config.replace_config(@ack_app, :durable_slot, true)
      assert :ok = Config.replace_config(@ack_app, :password, "new_password")
    end

    test "the config agent survives a rejected replacement" do
      pid = GenServer.whereis(WalExRegistry.set_name(:set_agent, Config, @ack_app))

      assert_raise ArgumentError, fn ->
        Config.replace_config(@ack_app, :durable_slot, false)
      end

      assert Process.alive?(pid)
    end
  end

  describe "record_connection/1" do
    test "returns the number of earlier connections" do
      {:ok, _pid} = Config.start_link(configs: @base_configs)

      assert 0 == Config.record_connection(@app_name)
      assert 1 == Config.record_connection(@app_name)
      assert 2 == Config.record_connection(@app_name)
    end
  end

  # # Need to find a way to load the MyApp.Events.Subscriptions
  # describe "build_module_names/3" do
  #   setup do
  #     {:ok, _pid} = Config.start_link(configs: @base_configs)
  #     :ok
  #   end

  #   test "should create list of modules from subscriptions config when no modules" do
  #     subscriptions = ["subscriptions"]

  #     assert [:"MyApp.Events.Subscriptions"] ==
  #              Config.build_module_names(@app_name, [], subscriptions)
  #   end

  #   test "should create list of modules from modules config when no subscriptions" do
  #     modules = [MyApp.CustomModule]

  #     assert modules == Config.build_module_names(@app_name, modules, [])
  #   end

  #   test "should create list of modules when both modules and subscriptions config" do
  #     subscriptions = ["subscriptions"]
  #     modules = [MyApp.CustomModule]

  #     assert [MyApp.CustomModule, :"MyApp.Events.Subscriptions"] ==
  #              Config.build_module_names(@app_name, modules, subscriptions)
  #   end
  # end

  describe "ack_after_processing config" do
    test "should default to false" do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      assert false == Config.get_configs(@app_name, :ack_after_processing)
    end

    test "should be true when durable_slot is also true" do
      configs =
        @base_configs
        |> Keyword.put(:name, :ack_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)

      {:ok, _pid} = Config.start_link(configs: configs)
      assert true == Config.get_configs(:ack_app, :ack_after_processing)
    end

    test "should raise when ack_after_processing is true but durable_slot is false" do
      configs =
        @base_configs
        |> Keyword.put(:name, :bad_ack_app)
        |> Keyword.put(:durable_slot, false)
        |> Keyword.put(:ack_after_processing, true)

      assert_raise RuntimeError,
                   "ack_after_processing requires durable_slot: true — temporary slots do not retain WAL on reconnect",
                   fn -> Config.start_link(configs: configs) end
    end

    test "should raise when ack_after_processing is true and durable_slot is absent" do
      configs =
        @base_configs
        |> Keyword.put(:name, :bad_ack_app2)
        |> Keyword.put(:ack_after_processing, true)

      assert_raise RuntimeError,
                   "ack_after_processing requires durable_slot: true — temporary slots do not retain WAL on reconnect",
                   fn -> Config.start_link(configs: configs) end
    end

    test "should raise when ack_after_processing is combined with a custom message_middleware" do
      configs =
        @base_configs
        |> Keyword.put(:name, :bad_ack_app3)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)
        |> Keyword.put(:message_middleware, fn _message, _app -> :ok end)

      assert_raise RuntimeError, ~r/cannot be combined with a custom message_middleware/, fn ->
        Config.start_link(configs: configs)
      end
    end

    test "should default message_middleware to synchronous processing" do
      configs =
        @base_configs
        |> Keyword.put(:name, :ack_sync_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)

      {:ok, _pid} = Config.start_link(configs: configs)

      assert (&Publisher.process_message_sync/2) ==
               Config.get_configs(:ack_sync_app, :message_middleware)
    end

    test "should keep a custom message_middleware when ack_after_processing is false" do
      middleware = fn _message, _app -> :ok end

      configs =
        @base_configs
        |> Keyword.put(:name, :custom_middleware_app)
        |> Keyword.put(:message_middleware, middleware)

      {:ok, _pid} = Config.start_link(configs: configs)
      assert middleware == Config.get_configs(:custom_middleware_app, :message_middleware)
    end
  end

  describe "to_module_name/1" do
    setup do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      :ok
    end

    test "should convert standard atom into Module atom" do
      assert "MyApp" == Config.to_module_name(@app_name)
    end

    test "should convert binary string into Module atom" do
      assert "MyApp" == Config.to_module_name("my_app")
    end

    test "should convert remove 'Elixir.' from module name" do
      assert "Elixir.MyApp" == Config.to_module_name(:"Elixir.MyApp")
    end
  end
end

defmodule MyApp.CustomModule do
  # use WalEx.Event, name: :test_name
end

# defmodule MyApp.Events.Subscriptions do
#   use WalEx.Event, name: :my_app
# end
