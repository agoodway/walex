defmodule WalEx.Config do
  @moduledoc """
  Per-app configuration store backed by `Agent`.

  Each WalEx instance is identified by `:name` and keeps its config (database
  credentials, publication, subscriptions, modules, slot settings, etc.) under
  a registered agent so it can be read by replication processes and updated at
  runtime via `add_config/3`, `remove_config/3`, and `replace_config/3`.
  """
  use Agent

  alias WalEx.Config.Registry, as: WalExRegistry
  alias WalEx.Replication.Publisher

  @allowed_config_value ~w(database hostname name password port publication username webhook_signing_secret slot_name durable_slot message_middleware)a
  @allowed_config_values ~w(modules subscriptions)a

  @type start_opts :: [
          {:database, binary}
          | {:hostname, binary}
          | {:name, binary}
          | {:password, binary}
          | {:port, binary}
          | {:publication, binary}
          | {:username, binary}
          | {:webhook_signing_secret, binary}
          | {:slot_name, binary}
          | {:durable_slot, boolean}
          | {:ack_after_processing, boolean}
          | {:standby_status_interval, pos_integer}
          | {:message_middleware, (term, term -> :ok)}
          | {:event_relay, keyword()}
          | {:modules, [module]}
          | {:subscriptions, [binary()]}
        ]

  @doc """
  Starts the per-app config agent. Expects `opts[:configs]` to contain a keyword
  list of user configuration; the agent is registered under the app name.
  """
  @spec start_link(opts :: start_opts()) :: Agent.on_start()
  def start_link(opts) do
    configs =
      opts
      |> Keyword.get(:configs)
      |> build_app_configs()

    app_name = Keyword.get(configs, :name)

    name = WalExRegistry.set_name(:set_agent, __MODULE__, app_name)

    Agent.start_link(fn -> configs end, name: name)
  end

  @doc "Returns `true` if `configs` is a keyword list with a non-nil entry for `key`."
  def has_config?(configs, key) when is_list(configs) do
    Keyword.has_key?(configs, key) and not is_nil(Keyword.get(configs, key))
  end

  def has_config?(_configs, _key), do: false

  @doc """
  Reads stored config for an app.

  With one argument returns the full keyword list. With a `key` atom returns
  that single value. With a list of keys returns a keyword list of those
  entries in the requested order.
  """
  def get_configs(app_name) do
    WalExRegistry.get_state(:get_agent, __MODULE__, app_name)
  end

  def get_configs(app_name, key) when is_atom(key) do
    WalExRegistry.get_state(:get_agent, __MODULE__, app_name)
    |> Keyword.get(key)
  end

  def get_configs(app_name, keys) when is_list(keys) and keys != [] do
    order_map = keys |> Enum.with_index() |> Map.new()

    WalExRegistry.get_state(:get_agent, __MODULE__, app_name)
    |> Keyword.take(keys)
    |> Enum.sort_by(fn {k, _} -> Map.get(order_map, k) end)
  end

  @doc "Shortcut for `get_configs(app_name, :database)`."
  def get_database(app_name), do: get_configs(app_name, :database)

  @doc "Shortcut for `get_configs(app_name, :modules)`."
  def get_event_modules(app_name), do: get_configs(app_name, :modules)

  @doc false
  # Records a replication connection for `app_name` and returns how many
  # connections were recorded before this one. The count lives in this agent
  # rather than in WalEx.Replication.Server because the replication processes
  # are restarted together after a crash, and a restart must still be reported
  # as a reconnect.
  @spec record_connection(atom()) :: non_neg_integer()
  def record_connection(app_name) do
    Agent.get_and_update(set_agent(app_name), fn config ->
      previous = Keyword.get(config, :connection_count, 0)
      {previous, Keyword.put(config, :connection_count, previous + 1)}
    end)
  end

  @doc """
  Appends one or more values to a multi-valued config key
  (`:modules` or `:subscriptions`). Duplicates are removed.
  """
  def add_config(app_name, key, new_values)
      when is_list(new_values) and key in @allowed_config_values do
    Agent.update(set_agent(app_name), fn config ->
      updated_values =
        config
        |> Keyword.get(key, [])
        |> Enum.concat(new_values)
        |> Enum.uniq()

      Keyword.put(config, key, updated_values)
    end)
  end

  def add_config(app_name, key, new_value) when key in @allowed_config_values do
    add_config(app_name, key, [new_value])
  end

  @doc """
  Removes a value from a multi-valued config key
  (`:modules` or `:subscriptions`).
  """
  def remove_config(app_name, key, new_value) when key in @allowed_config_values do
    Agent.update(set_agent(app_name), fn config ->
      updated_values =
        config
        |> Keyword.get(key, [])
        |> Enum.reject(&(&1 == new_value))
        |> Enum.uniq()

      Keyword.put(config, key, updated_values)
    end)
  end

  @doc """
  Replaces a single-valued config key
  (e.g. `:password`, `:hostname`, `:publication`).

  When `ack_after_processing` is enabled, `:durable_slot` and
  `:message_middleware` can only be replaced with values that keep the
  delivery guarantee: `durable_slot: true`, and the synchronous default
  middleware. Anything else raises `ArgumentError` and leaves the config
  unchanged.
  """
  def replace_config(app_name, key, new_value) when key in @allowed_config_value do
    result =
      Agent.get_and_update(set_agent(app_name), fn config ->
        updated = Keyword.put(config, key, new_value)

        case validate_ack_after_processing(updated) do
          :ok -> {:ok, updated}
          error -> {error, config}
        end
      end)

    case result do
      :ok -> :ok
      {:error, message} -> raise ArgumentError, "cannot replace #{inspect(key)}: #{message}"
    end
  end

  @doc false
  # Checks a resolved config (as stored in the agent) for combinations that
  # break the ack_after_processing guarantee. Used by replace_config/3 and by
  # WalEx.Replication.Server on every start, because the agent outlives the
  # replication processes.
  @spec validate_ack_after_processing(keyword()) :: :ok | {:error, String.t()}
  def validate_ack_after_processing(config) do
    cond do
      Keyword.get(config, :ack_after_processing) != true ->
        :ok

      Keyword.get(config, :durable_slot) != true ->
        {:error,
         "ack_after_processing requires durable_slot: true — temporary slots do not retain WAL on reconnect"}

      Keyword.get(config, :message_middleware) != (&Publisher.process_message_sync/2) ->
        {:error,
         "ack_after_processing requires the default synchronous message_middleware " <>
           "(&WalEx.Replication.Publisher.process_message_sync/2) — acknowledgements assume " <>
           "WalEx dispatches each message synchronously"}

      true ->
        :ok
    end
  end

  defp build_app_configs(configs) do
    db_configs_from_url =
      configs
      |> Keyword.get(:url, "")
      |> parse_url()

    name = Keyword.get(configs, :name)
    subscriptions = Keyword.get(configs, :subscriptions, [])
    modules = Keyword.get(configs, :modules, [])
    module_names = build_module_names(name, modules, subscriptions)
    ack_after_processing = parse_ack_after_processing(configs)

    [
      name: name,
      hostname: Keyword.get(configs, :hostname, db_configs_from_url[:hostname]),
      username: Keyword.get(configs, :username, db_configs_from_url[:username]),
      password: Keyword.get(configs, :password, db_configs_from_url[:password]),
      port: Keyword.get(configs, :port, db_configs_from_url[:port]),
      database: Keyword.get(configs, :database, db_configs_from_url[:database]),
      ssl: Keyword.get(configs, :ssl, false),
      ssl_opts: Keyword.get(configs, :ssl_opts, verify: :verify_none),
      socket_options: Keyword.get(configs, :socket_options, []),
      subscriptions: subscriptions,
      publication: Keyword.get(configs, :publication),
      modules: module_names,
      webhook_signing_secret: Keyword.get(configs, :webhook_signing_secret),
      event_relay: Keyword.get(configs, :event_relay),
      slot_name: Keyword.get(configs, :slot_name) |> parse_slot_name(name),
      durable_slot: Keyword.get(configs, :durable_slot, false) == true,
      ack_after_processing: ack_after_processing,
      standby_status_interval: parse_standby_status_interval(configs),
      message_middleware:
        Keyword.get(configs, :message_middleware)
        |> parse_message_middleware(ack_after_processing)
    ]
  end

  @doc """
  Resolves the event-handler modules for an app: combines explicit `:modules`
  with the convention-derived modules for each subscription, deduplicates, and
  keeps only those that actually compile.
  """
  def build_module_names(name, modules, subscriptions)
      when is_list(modules) and is_list(subscriptions) do
    subscriptions
    |> map_subscriptions_to_modules(name)
    |> Enum.concat(modules)
    |> Enum.uniq()
    |> map_existing_modules()
    |> Enum.sort()
  end

  def build_module_names(_name, _modules, _subscriptions), do: nil

  @doc "Maps each subscription name to its conventional `<App>.Events.<Subscription>` module atom."
  def map_subscriptions_to_modules(subscriptions, name) do
    Enum.map(subscriptions, fn subscription ->
      (to_module_name(name) <> "." <> "Events" <> "." <> to_module_name(subscription))
      |> String.to_atom()
    end)
  end

  @doc "Converts `snake_case` (or already-cased) names into `CamelCase` module-name fragments."
  def to_module_name(module_name) when is_atom(module_name) or is_binary(module_name) do
    module_name
    |> to_string()
    |> String.split(["_"])
    |> Enum.map_join(&capitalize/1)
  end

  defp capitalize(name) do
    if String.at(name, 0) == String.upcase(String.at(name, 0)) do
      name
    else
      String.capitalize(name)
    end
  end

  defp module_exists?(module_name) do
    case Code.ensure_compiled(module_name) do
      {:module, _module} ->
        true

      {:error, _error} ->
        false
    end
  end

  defp map_existing_modules(modules), do: Enum.filter(modules, &module_exists?/1)

  defp parse_url(""), do: []

  defp parse_url(url) when is_binary(url) do
    info = URI.parse(url)

    if is_nil(info.host), do: raise("host is not present")

    if is_nil(info.path) or not (info.path =~ ~r"^/([^/])+$"),
      do: raise("path should be a database name")

    destructure [username, password], info.userinfo && String.split(info.userinfo, ":")
    "/" <> database = info.path

    url_opts = set_url_opts(username, password, database, info)

    for {k, v} <- url_opts,
        not is_nil(v),
        do: {k, if(is_binary(v), do: URI.decode(v), else: v)}
  end

  defp parse_slot_name(nil, app_name) do
    app_name
    |> to_string()
    |> remove_prefix()
    |> String.downcase()
    |> replace_special_chars()
    |> Kernel.<>("_walex")
  end

  defp parse_slot_name(slot_name, _) do
    slot_name
    |> to_string()
    |> remove_prefix()
    |> String.downcase()
    |> replace_special_chars()
  end

  defp remove_prefix(name) do
    name
    |> String.split(".")
    |> List.last()
  end

  defp replace_special_chars(name) do
    name
    |> String.replace(~r/[^a-z0-9]+/, "_")
    |> String.replace(~r/_+/, "_")
    |> String.trim("_")
  end

  defp parse_ack_after_processing(configs) do
    durable_slot = Keyword.get(configs, :durable_slot, false) == true
    ack_after_processing = Keyword.get(configs, :ack_after_processing, false) == true
    custom_middleware = not is_nil(Keyword.get(configs, :message_middleware))

    cond do
      ack_after_processing and not durable_slot ->
        raise "ack_after_processing requires durable_slot: true — temporary slots do not retain WAL on reconnect"

      ack_after_processing and custom_middleware ->
        raise "ack_after_processing cannot be combined with a custom message_middleware — " <>
                "acknowledgements assume WalEx dispatches each message synchronously"

      true ->
        ack_after_processing
    end
  end

  defp parse_standby_status_interval(configs) do
    case Keyword.get(configs, :standby_status_interval) do
      nil ->
        nil

      interval when is_integer(interval) and interval > 0 ->
        interval

      other ->
        raise "standby_status_interval must be a positive integer (milliseconds), got: #{inspect(other)}"
    end
  end

  defp parse_message_middleware(message_middleware, _ack_after_processing)
       when is_function(message_middleware, 2),
       do: message_middleware

  defp parse_message_middleware(nil, true), do: &Publisher.process_message_sync/2
  defp parse_message_middleware(nil, false), do: &Publisher.process_message_async/2

  defp set_url_opts(username, password, database, info) do
    url_opts = [
      username: username,
      password: password,
      database: database,
      port: info.port
    ]

    put_hostname_if_present(url_opts, info.host)
  end

  defp put_hostname_if_present(keyword, ""), do: keyword

  defp put_hostname_if_present(keyword, hostname) when is_binary(hostname) do
    Keyword.put(keyword, :hostname, hostname)
  end

  defp set_agent(app_name), do: WalExRegistry.set_name(:set_agent, __MODULE__, app_name)
end
