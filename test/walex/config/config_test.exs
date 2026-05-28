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
