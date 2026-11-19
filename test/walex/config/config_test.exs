defmodule WalEx.ConfigTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

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
        url: "postgres://username:password@hostname:5432/database",
        publication: "publication"
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

  describe "remove_config/3 with ack_after_processing" do
    setup do
      {:ok, _pid} =
        Config.start_link(
          configs:
            Keyword.merge(@base_configs,
              ack_after_processing: true,
              durable_slot: true,
              modules: [WalEx.ConfigTest]
            )
        )

      :ok
    end

    test "rejects removing the last module and leaves the config unchanged" do
      assert_raise ArgumentError, ~r/last event handler module/, fn ->
        Config.remove_config(@app_name, :modules, WalEx.ConfigTest)
      end

      assert [WalEx.ConfigTest] == Config.get_event_modules(@app_name)
    end

    test "still removes a module while others remain" do
      Config.add_config(@app_name, :modules, [WalEx.Config])
      assert :ok = Config.remove_config(@app_name, :modules, WalEx.ConfigTest)
      assert [WalEx.Config] == Config.get_event_modules(@app_name)
    end

    test "subscriptions can still be removed" do
      assert :ok = Config.remove_config(@app_name, :subscriptions, "subscriptions")
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

    test "durable_slot follows the startup boolean policy" do
      assert :ok = Config.replace_config(@app_name, :durable_slot, nil)
      assert false == Config.get_configs(@app_name, :durable_slot)

      for value <- ["false", "true", 1] do
        assert_raise ArgumentError, ~r/durable_slot must be true or false/, fn ->
          Config.replace_config(@app_name, :durable_slot, value)
        end
      end

      assert false == Config.get_configs(@app_name, :durable_slot)
    end

    test "should allow replacing connection and slot keys without ack_after_processing" do
      for {key, value} <- [
            slot_name: "s",
            publication: "p",
            database: "d",
            hostname: "h",
            port: 1
          ] do
        assert :ok = Config.replace_config(@app_name, key, value)
      end
    end
  end

  describe "replication target validation" do
    @rejected [
      "",
      "a\\b",
      "a\0b",
      "events\n",
      "a\tb",
      "a\x7Fb",
      "events'; SELECT 1; --\\",
      String.duplicate("a", 64),
      String.duplicate("é", 32),
      :events,
      123
    ]

    test "build rejects an invalid publication name" do
      for bad <- @rejected do
        assert_raise ArgumentError, ~r/publication/, fn ->
          Config.start_link(configs: Keyword.put(@base_configs, :publication, bad))
        end
      end
    end

    test "the error for a single bad name shows the name, not a list" do
      error =
        assert_raise ArgumentError, fn ->
          Config.start_link(configs: Keyword.put(@base_configs, :publication, "a\\b"))
        end

      assert error.message =~ ~s[got: "a\\\\b"]
      refute error.message =~ "[\"a"
    end

    test "build rejects an invalid name inside a publication list" do
      for bad <- [["ok", "x\\y"], ["ok", :events], [:events], [], nil] do
        assert_raise ArgumentError, ~r/publication/, fn ->
          Config.start_link(configs: Keyword.put(@base_configs, :publication, bad))
        end
      end
    end

    test "build accepts any name Postgres can hold, plus lists" do
      valid_names = [
        "events",
        "my-pub",
        "my.pub",
        "1abc",
        "My Pub",
        "événements",
        "it's",
        String.duplicate("a", 63),
        ["a", "b-c"]
      ]

      for {valid, i} <- Enum.with_index(valid_names) do
        configs =
          @base_configs
          |> Keyword.put(:publication, valid)
          |> Keyword.put(:name, :"pub_ok_#{i}")

        assert {:ok, _pid} = Config.start_link(configs: configs)
      end
    end

    test "build rejects a slot_name that sanitizes to nothing" do
      assert_raise ArgumentError, ~r/slot_name.*\[a-z0-9_\]/, fn ->
        Config.start_link(configs: Keyword.put(@base_configs, :slot_name, "---"))
      end
    end

    test "replace_config rejects an invalid publication and keeps the old one" do
      configs = Keyword.put(@base_configs, :name, :replace_pub_app)
      {:ok, _pid} = Config.start_link(configs: configs)

      assert_raise ArgumentError, ~r/publication/, fn ->
        Config.replace_config(:replace_pub_app, :publication, "x\\y")
      end

      assert "publication" == Config.get_configs(:replace_pub_app, :publication)
      assert :ok = Config.replace_config(:replace_pub_app, :publication, ["a", "b"])
    end

    test "replace_config sanitizes slot_name like startup and rejects an empty result" do
      configs = Keyword.put(@base_configs, :name, :replace_slot_app)
      {:ok, _pid} = Config.start_link(configs: configs)

      assert :ok = Config.replace_config(:replace_slot_app, :slot_name, "A.b'; drop")
      assert "b_drop" == Config.get_configs(:replace_slot_app, :slot_name)

      for bad <- ["---", "", nil] do
        assert_raise ArgumentError, ~r/slot_name/, fn ->
          Config.replace_config(:replace_slot_app, :slot_name, bad)
        end
      end

      assert "b_drop" == Config.get_configs(:replace_slot_app, :slot_name)
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

    test "rejects connection and replication-target keys and keeps them unchanged" do
      before = Config.get_configs(@ack_app)

      for {key, value} <- [
            slot_name: "other_slot",
            publication: "other_pub",
            database: "other_db",
            hostname: "other_host",
            port: 6543
          ] do
        assert_raise ArgumentError,
                     ~r/cannot replace #{inspect(key)}.*ack_after_processing/,
                     fn ->
                       Config.replace_config(@ack_app, key, value)
                     end
      end

      assert before == Config.get_configs(@ack_app)
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

  describe "the failure streak" do
    test "counts consecutive failures until reset" do
      {:ok, _pid} = Config.start_link(configs: @base_configs)

      assert 0 == Config.failure_count(@app_name)
      assert 1 == Config.record_failure(@app_name, 100)
      assert 2 == Config.record_failure(@app_name, 100)
      assert 2 == Config.failure_count(@app_name)
      assert :ok == Config.reset_failures(@app_name)
      assert 0 == Config.failure_count(@app_name)
      assert 1 == Config.record_failure(@app_name, 100)
    end

    test "remembers where the last failure happened" do
      {:ok, _pid} = Config.start_link(configs: @base_configs)

      assert nil == Config.failure_lsn(@app_name)
      Config.record_failure(@app_name, 100)
      assert 100 == Config.failure_lsn(@app_name)
      Config.record_failure(@app_name, nil)
      assert nil == Config.failure_lsn(@app_name)
      Config.reset_failures(@app_name)
      assert nil == Config.failure_lsn(@app_name)
    end

    test "is independent of the connection count" do
      {:ok, _pid} = Config.start_link(configs: @base_configs)

      Config.record_failure(@app_name, 100)
      assert 0 == Config.record_connection(@app_name)
    end
  end

  describe "internal runtime state" do
    @internal_keys ~w(connection_count consecutive_failures failure_lsn resume_deadline streamed_slot __runtime__)a

    setup do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      :ok
    end

    defp record_everything do
      Config.record_connection(@app_name)
      Config.record_failure(@app_name, 100)
      Config.resume_deadline(@app_name, 5_000)
      Config.record_slot_streamed(@app_name, "my_app_walex")
    end

    test "get_configs/1 does not list it" do
      before_keys = @app_name |> Config.get_configs() |> Keyword.keys()
      record_everything()
      after_keys = @app_name |> Config.get_configs() |> Keyword.keys()

      assert before_keys == after_keys
      assert [] == Enum.filter(after_keys, &(&1 in @internal_keys))
    end

    test "get_configs/2 does not return it" do
      record_everything()

      assert [] == Config.get_configs(@app_name, @internal_keys)
      assert nil == Config.get_configs(@app_name, :connection_count)
      assert nil == Config.get_configs(@app_name, :streamed_slot)
    end

    test "the accessors and replace_config keep working" do
      record_everything()

      assert 1 == Config.failure_count(@app_name)
      assert 100 == Config.failure_lsn(@app_name)
      assert 5_000 == Config.resume_deadline(@app_name, 9_999)
      assert Config.slot_streamed?(@app_name, "my_app_walex")
      assert 1 == Config.record_connection(@app_name)

      assert :ok == Config.replace_config(@app_name, :password, "other")
      assert :ok == @app_name |> Config.get_configs() |> Config.validate_config()
      assert 1 == Config.failure_count(@app_name)
    end
  end

  describe "ssl without ssl_opts" do
    test "warns once at startup that certificates are not verified" do
      configs = @base_configs |> Keyword.delete(:ssl_opts) |> Keyword.put(:ssl, true)

      log = capture_log(fn -> {:ok, _pid} = Config.start_link(configs: configs) end)

      assert 1 == length(String.split(log, "verify: :verify_none")) - 1
      assert log =~ "ssl_opts"
    end

    test "is silent with ssl_opts, or without ssl" do
      log =
        capture_log(fn ->
          {:ok, pid} = Config.start_link(configs: Keyword.put(@base_configs, :ssl, true))
          Agent.stop(pid)

          {:ok, _pid} = Config.start_link(configs: Keyword.drop(@base_configs, [:ssl_opts]))
        end)

      refute log =~ "verify_none"
    end

    test "keeps the default verify_none" do
      configs = @base_configs |> Keyword.delete(:ssl_opts) |> Keyword.put(:ssl, true)
      capture_log(fn -> {:ok, _pid} = Config.start_link(configs: configs) end)

      assert [verify: :verify_none] == Config.get_configs(@app_name, :ssl_opts)
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

    test "standby_status_interval defaults to nil without ack_after_processing" do
      {:ok, _pid} = Config.start_link(configs: @base_configs)
      assert nil == Config.get_configs(@app_name, :standby_status_interval)
    end

    test "standby_status_interval defaults to 10_000 with ack_after_processing" do
      configs =
        @base_configs
        |> Keyword.put(:name, :ack_default_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)

      {:ok, _pid} = Config.start_link(configs: configs)
      assert 10_000 == Config.get_configs(:ack_default_app, :standby_status_interval)
    end

    test "an explicit standby_status_interval wins over the ack default" do
      configs =
        @base_configs
        |> Keyword.put(:name, :ack_explicit_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)
        |> Keyword.put(:standby_status_interval, 2_500)

      {:ok, _pid} = Config.start_link(configs: configs)
      assert 2_500 == Config.get_configs(:ack_explicit_app, :standby_status_interval)
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

      assert_raise ArgumentError,
                   ~r/ack_after_processing requires durable_slot: true/,
                   fn -> Config.start_link(configs: configs) end
    end

    test "should raise when ack_after_processing is true and durable_slot is absent" do
      configs =
        @base_configs
        |> Keyword.put(:name, :bad_ack_app2)
        |> Keyword.put(:ack_after_processing, true)

      assert_raise ArgumentError,
                   ~r/ack_after_processing requires durable_slot: true/,
                   fn -> Config.start_link(configs: configs) end
    end

    test "should raise when ack_after_processing is combined with a custom message_middleware" do
      configs =
        @base_configs
        |> Keyword.put(:name, :bad_ack_app3)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)
        |> Keyword.put(:message_middleware, fn _message, _app -> :ok end)

      assert_raise ArgumentError, ~r/synchronous message_middleware/, fn ->
        Config.start_link(configs: configs)
      end
    end

    test "accepts the explicit synchronous middleware with ack_after_processing" do
      configs =
        @base_configs
        |> Keyword.put(:name, :explicit_sync_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)
        |> Keyword.put(:message_middleware, &Publisher.process_message_sync/2)

      {:ok, _pid} = Config.start_link(configs: configs)

      assert (&Publisher.process_message_sync/2) ==
               Config.get_configs(:explicit_sync_app, :message_middleware)
    end

    test "rejects the asynchronous middleware with ack_after_processing" do
      configs =
        @base_configs
        |> Keyword.put(:name, :explicit_async_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)
        |> Keyword.put(:message_middleware, &Publisher.process_message_async/2)

      assert_raise ArgumentError, ~r/synchronous message_middleware/, fn ->
        Config.start_link(configs: configs)
      end
    end

    for key <- [:ack_after_processing, :durable_slot],
        value <- ["true", 1, :yes, "false"] do
      test "rejects non-boolean #{key}: #{inspect(value)}" do
        configs =
          @base_configs
          |> Keyword.put(:name, :strict_bool_app)
          |> Keyword.put(unquote(key), unquote(value))

        assert_raise ArgumentError, ~r/#{unquote(key)} must be true or false/, fn ->
          Config.start_link(configs: configs)
        end
      end
    end

    test "treats nil (e.g. an unset env var) as false for both booleans" do
      configs =
        @base_configs
        |> Keyword.put(:name, :nil_bool_app)
        |> Keyword.put(:durable_slot, nil)
        |> Keyword.put(:ack_after_processing, nil)

      {:ok, _pid} = Config.start_link(configs: configs)
      assert false == Config.get_configs(:nil_bool_app, :durable_slot)
      assert false == Config.get_configs(:nil_bool_app, :ack_after_processing)
    end

    test "accepts explicit false for both booleans" do
      configs =
        @base_configs
        |> Keyword.put(:name, :false_bool_app)
        |> Keyword.put(:durable_slot, false)
        |> Keyword.put(:ack_after_processing, false)

      {:ok, _pid} = Config.start_link(configs: configs)
      assert false == Config.get_configs(:false_bool_app, :durable_slot)
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

    test "raises at startup for an explicitly listed module that cannot be loaded" do
      configs =
        @base_configs
        |> Keyword.put(:name, :missing_module_ack_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)
        |> Keyword.put(:modules, [MyApp.CustomModule, MyApp.Typo])

      assert_raise ArgumentError, ~r/MyApp\.Typo/, fn -> Config.start_link(configs: configs) end
    end

    test "keeps subscription-derived modules optional" do
      configs =
        @base_configs
        |> Keyword.put(:name, :derived_module_ack_app)
        |> Keyword.put(:durable_slot, true)
        |> Keyword.put(:ack_after_processing, true)
        |> Keyword.put(:subscriptions, ["no_such_table"])

      {:ok, _pid} = Config.start_link(configs: configs)
      assert [MyApp.CustomModule] == Config.get_configs(:derived_module_ack_app, :modules)
    end

    test "without ack_after_processing drops a missing module with a warning" do
      configs =
        @base_configs
        |> Keyword.put(:name, :missing_module_default_app)
        |> Keyword.put(:modules, [MyApp.CustomModule, MyApp.Typo])

      log =
        capture_log(fn ->
          {:ok, _pid} = Config.start_link(configs: configs)
        end)

      assert log =~ "MyApp.Typo"
      assert [MyApp.CustomModule] == Config.get_configs(:missing_module_default_app, :modules)
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
