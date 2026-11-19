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

  require Logger

  @allowed_config_value ~w(database hostname name password port publication username webhook_signing_secret slot_name durable_slot message_middleware)a
  @allowed_config_values ~w(modules subscriptions)a
  @boolean_keys ~w(durable_slot ack_after_processing)a
  @default_ack_standby_status_interval 10_000
  # Bookkeeping the replication processes keep in the agent (connection count,
  # failure streak, ...) lives in one map under this key, so that get_configs/1,2
  # can hide it with a single Keyword.delete/2.
  @runtime_key :__runtime__
  # Where the replication connection points. Changing these at runtime would move
  # an ack_after_processing app onto a different slot/publication/database while
  # the stored acknowledgements still describe the old one.
  @ack_locked_keys ~w(slot_name publication database hostname port)a

  @type start_opts :: [
          {:database, binary}
          | {:hostname, binary}
          | {:name, binary}
          | {:password, binary}
          | {:port, binary}
          | {:publication, binary | [binary]}
          | {:username, binary}
          | {:webhook_signing_secret, binary}
          | {:slot_name, binary}
          | {:durable_slot, boolean}
          | {:ack_after_processing, boolean}
          | {:standby_status_interval, pos_integer}
          | {:message_middleware, (term, term -> :ok | {:error, term})}
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
  def has_config?(configs, key) when is_list(configs),
    do: not is_nil(Keyword.get(configs, key))

  def has_config?(_configs, _key), do: false

  @doc """
  Reads stored config for an app.

  With one argument returns the full keyword list. With a `key` atom returns
  that single value. With a list of keys returns a keyword list of those
  entries in the requested order.
  """
  def get_configs(app_name) do
    app_name |> raw_state() |> Keyword.delete(@runtime_key)
  end

  def get_configs(app_name, key) when is_atom(key) do
    app_name |> get_configs() |> Keyword.get(key)
  end

  def get_configs(app_name, keys) when is_list(keys) and keys != [] do
    order_map = keys |> Enum.with_index() |> Map.new()

    app_name
    |> get_configs()
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
      previous = runtime(config, :connection_count, 0)
      {previous, put_runtime(config, connection_count: previous + 1)}
    end)
  end

  @doc false
  # Records a failed message for `app_name` and returns how many have failed in
  # a row, counting this one. `lsn` is the failing transaction's commit LSN (an
  # integer), or nil when it is unknown; the streak only ends once a commit at
  # or past it is handled. The replication server backs off by the count before
  # reconnecting. It lives here, like the connection count, so that a restart
  # of the replication processes does not reset the backoff.
  @spec record_failure(atom(), non_neg_integer() | nil) :: pos_integer()
  def record_failure(app_name, lsn) do
    Agent.get_and_update(set_agent(app_name), fn config ->
      count = runtime(config, :consecutive_failures, 0) + 1

      {count,
       put_runtime(config, consecutive_failures: count, failure_lsn: lsn, resume_deadline: nil)}
    end)
  end

  @doc false
  # The monotonic time (ms) at which replication resumes after the failure
  # recorded last. The first caller after a failure sets it to `candidate`;
  # later ones get that same deadline back, so a reconnect during the wait
  # does not restart it.
  @spec resume_deadline(atom(), integer()) :: integer()
  def resume_deadline(app_name, candidate) do
    Agent.get_and_update(set_agent(app_name), fn config ->
      case runtime(config, :resume_deadline) do
        nil -> {candidate, put_runtime(config, resume_deadline: candidate)}
        deadline -> {deadline, config}
      end
    end)
  end

  @doc false
  # Records that this instance has streamed from `slot_name`. From then on a
  # missing slot means it was lost, not that this is a first start.
  @spec record_slot_streamed(atom(), String.t()) :: :ok
  def record_slot_streamed(app_name, slot_name),
    do: Agent.update(set_agent(app_name), &put_runtime(&1, streamed_slot: slot_name))

  @doc false
  # Whether record_slot_streamed/2 was called for `slot_name`.
  @spec slot_streamed?(atom(), String.t()) :: boolean()
  def slot_streamed?(app_name, slot_name),
    do: app_name |> raw_state() |> runtime(:streamed_slot) == slot_name

  @doc false
  # The number of failures recorded by record_failure/2 since the last reset.
  @spec failure_count(atom()) :: non_neg_integer()
  def failure_count(app_name), do: app_name |> raw_state() |> runtime(:consecutive_failures, 0)

  @doc false
  # The commit LSN of the last failure recorded by record_failure/2, or nil.
  @spec failure_lsn(atom()) :: non_neg_integer() | nil
  def failure_lsn(app_name), do: app_name |> raw_state() |> runtime(:failure_lsn)

  @doc false
  # Ends the streak kept by record_failure/2.
  @spec reset_failures(atom()) :: :ok
  def reset_failures(app_name) do
    Agent.update(
      set_agent(app_name),
      &put_runtime(&1, consecutive_failures: 0, failure_lsn: nil, resume_deadline: nil)
    )
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

  With `ack_after_processing` enabled, removing the last of `:modules` raises
  `ArgumentError` and leaves the config unchanged: transactions would be
  acknowledged with no handler, and a restart of the events supervisor would
  fail.
  """
  @spec remove_config(atom(), atom(), term()) :: :ok
  def remove_config(app_name, key, new_value) when key in @allowed_config_values do
    result =
      Agent.get_and_update(set_agent(app_name), fn config ->
        updated_values =
          config
          |> Keyword.get(key, [])
          |> Enum.reject(&(&1 == new_value))
          |> Enum.uniq()

        case check_removable(key, updated_values, ack_after_processing?(config)) do
          :ok -> {:ok, Keyword.put(config, key, updated_values)}
          error -> {error, config}
        end
      end)

    case result do
      :ok -> :ok
      {:error, message} -> raise ArgumentError, "cannot remove #{inspect(key)}: #{message}"
    end
  end

  defp check_removable(:modules, [], true),
    do:
      {:error,
       "ack_after_processing requires at least one event handler module, and this is the last event handler module"}

  defp check_removable(_key, _remaining, _ack_after_processing), do: :ok

  @doc """
  Replaces a single-valued config key
  (e.g. `:password`, `:hostname`, `:publication`).

  When `ack_after_processing` is enabled, `:durable_slot` and
  `:message_middleware` can only be replaced with values that keep the
  delivery guarantee: `durable_slot: true`, and the synchronous default
  middleware. The replication target (`:slot_name`, `:publication`, `:database`,
  `:hostname`, `:port`) cannot be replaced at all in that mode. Anything else
  raises `ArgumentError` and leaves the config unchanged.

  `:durable_slot` follows the startup rule in either mode: `true` or `false`,
  with `nil` read as `false`; anything else raises `ArgumentError`.
  """
  @spec replace_config(atom(), atom(), term()) :: :ok
  def replace_config(app_name, key, new_value) when key in @allowed_config_value do
    new_value = normalize_value(key, new_value)

    result =
      Agent.get_and_update(set_agent(app_name), fn config ->
        with :ok <- check_replaceable(key, ack_after_processing?(config)),
             updated = Keyword.put(config, key, new_value),
             :ok <- validate_config(updated) do
          {:ok, updated}
        else
          error -> {error, config}
        end
      end)

    case result do
      :ok -> :ok
      {:error, message} -> raise ArgumentError, "cannot replace #{inspect(key)}: #{message}"
    end
  end

  @doc """
  Whether a resolved config (as stored in the agent), or the running app
  `app_name`, has `ack_after_processing` enabled. Anything but `true` counts
  as off.
  """
  @spec ack_after_processing?(keyword() | atom()) :: boolean()
  def ack_after_processing?(config) when is_list(config),
    do: Keyword.get(config, :ack_after_processing) == true

  def ack_after_processing?(app_name) when is_atom(app_name),
    do: app_name |> get_configs() |> ack_after_processing?()

  @doc false
  # Checks a resolved config (as stored in the agent). Used by build_app_configs/1
  # and replace_config/3, and by WalEx.Replication.Server on every start, because
  # the agent outlives the replication processes.
  #
  # The slot and publication names end up inside SQL and replication commands,
  # so they are checked in every mode; the rest guards the ack_after_processing
  # guarantee.
  @spec validate_config(keyword()) :: :ok | {:error, String.t()}
  def validate_config(config) do
    with :ok <- validate_slot_name(Keyword.get(config, :slot_name)),
         :ok <- validate_publication(Keyword.get(config, :publication)) do
      validate_ack_requirements(config)
    end
  end

  # parse_slot_name/2 reduces names to this form. The query builder checks it
  # again because it interpolates the name into replication commands.
  @doc false
  @spec valid_slot_name?(String.t()) :: boolean()
  def valid_slot_name?(slot_name), do: Regex.match?(~r/\A[a-z0-9_]+\z/, slot_name)

  defp validate_slot_name(slot_name) when is_binary(slot_name) do
    if valid_slot_name?(slot_name),
      do: :ok,
      else: {:error, invalid_slot_name_message(slot_name)}
  end

  defp validate_slot_name(slot_name), do: {:error, invalid_slot_name_message(slot_name)}

  defp invalid_slot_name_message(slot_name),
    do:
      "slot_name must be non-empty and only contain [a-z0-9_] " <>
        "(it has no letters or digits once sanitized), got: #{inspect(slot_name)}"

  # The query builder quotes and escapes whatever passes here, so this only
  # rules out what it cannot represent: empty or oversized names (Postgres
  # truncates identifiers at 63 bytes), ASCII control characters (NUL, newline,
  # tab, ...), and backslashes.
  defp validate_publication([]), do: {:error, publication_message("is empty", [])}
  defp validate_publication(nil), do: {:error, publication_message("is not set", nil)}

  defp validate_publication(names) when is_list(names) do
    case Enum.reject(names, &valid_publication_name?/1) do
      [] -> :ok
      [invalid | _] -> {:error, publication_message("has an invalid name", invalid)}
    end
  end

  defp validate_publication(name) when is_binary(name), do: validate_publication([name])

  defp validate_publication(other),
    do: {:error, publication_message("must be a string or a list of strings", other)}

  defp valid_publication_name?(name) when is_binary(name),
    do: name != "" and byte_size(name) <= 63 and not Regex.match?(~r/[\x00-\x1F\x7F\\]/, name)

  defp valid_publication_name?(_name), do: false

  defp publication_message(problem, value),
    do:
      "publication #{problem}: a publication is a non-empty name of at most 63 bytes " <>
        "without backslash or ASCII control characters (or a list of them), got: #{inspect(value)}"

  defp validate_ack_requirements(config) do
    cond do
      not ack_after_processing?(config) ->
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

  defp check_replaceable(key, true) when key in @ack_locked_keys,
    do: {:error, "#{inspect(key)} cannot change while ack_after_processing is enabled"}

  defp check_replaceable(_key, _ack_after_processing), do: :ok

  defp build_app_configs(configs) do
    db_configs_from_url =
      configs
      |> Keyword.get(:url, "")
      |> parse_url()

    name = Keyword.get(configs, :name)
    subscriptions = Keyword.get(configs, :subscriptions, [])
    modules = Keyword.get(configs, :modules, [])
    ack_after_processing = parse_boolean!(configs, :ack_after_processing)
    check_listed_modules(modules, ack_after_processing)
    module_names = build_module_names(name, modules, subscriptions)

    resolved = [
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
      durable_slot: parse_boolean!(configs, :durable_slot),
      ack_after_processing: ack_after_processing,
      standby_status_interval:
        configs
        |> Keyword.get(:standby_status_interval)
        |> parse_standby_status_interval(ack_after_processing),
      message_middleware:
        Keyword.get(configs, :message_middleware)
        |> parse_message_middleware(ack_after_processing)
    ]

    warn_unverified_ssl(configs)

    case validate_config(resolved) do
      :ok -> resolved
      {:error, message} -> raise ArgumentError, message
    end
  end

  # WalEx has always defaulted to verify: :verify_none, which Postgrex does not;
  # changing it would break ssl: true setups that have no CA configured, so it
  # stays, but is announced once per start.
  defp warn_unverified_ssl(configs) do
    if Keyword.get(configs, :ssl, false) == true and not Keyword.has_key?(configs, :ssl_opts) do
      Logger.warning(
        "WalEx ssl: true without ssl_opts connects with verify: :verify_none, so the " <>
          "server certificate is not verified. Pass ssl_opts (for example " <>
          "ssl_opts: [verify: :verify_peer, cacertfile: path]) to verify it."
      )
    end
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
      # Bounded: one atom per configured subscription, a list set by the developer
      # in config, never external input.
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

  # Modules derived from subscriptions are optional, but an explicitly listed
  # one that cannot be loaded is most likely a typo. With ack_after_processing
  # its transactions would be acknowledged without it ever running, so that
  # fails startup; otherwise it is dropped, as it always was, with a warning.
  defp check_listed_modules(modules, ack_after_processing) when is_list(modules) do
    modules
    |> Enum.reject(&module_exists?/1)
    |> report_missing_modules(ack_after_processing)
  end

  defp check_listed_modules(_modules, _ack_after_processing), do: :ok

  defp report_missing_modules([], _ack_after_processing), do: :ok

  defp report_missing_modules(missing, true) do
    raise ArgumentError,
          "ack_after_processing requires every module in :modules to be loadable, " <>
            "cannot load: #{inspect(missing)}"
  end

  defp report_missing_modules(missing, false),
    do: Logger.warning("Ignoring :modules that cannot be loaded: #{inspect(missing)}")

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

  defp parse_slot_name(nil, app_name), do: clean_slot_name(app_name) <> "_walex"
  defp parse_slot_name(slot_name, _app_name), do: clean_slot_name(slot_name)

  defp clean_slot_name(name) do
    name
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

  defp parse_boolean!(configs, key), do: configs |> Keyword.get(key) |> to_boolean!(key)

  # nil counts as absent: an unset env var must not break apps that never
  # enabled these options.
  defp to_boolean!(nil, _key), do: false
  defp to_boolean!(value, _key) when is_boolean(value), do: value

  defp to_boolean!(other, key),
    do: raise(ArgumentError, "#{key} must be true or false, got: #{inspect(other)}")

  # Runtime values follow the same rules as startup ones, or a stored "false"
  # would read as true on the next start.
  defp normalize_value(key, value) when key in @boolean_keys, do: to_boolean!(value, key)
  defp normalize_value(:slot_name, value) when not is_nil(value), do: parse_slot_name(value, nil)
  defp normalize_value(_key, value), do: value

  # In ack mode the slot only advances through status updates, so a periodic one
  # is on by default; otherwise it stays opt-in.
  defp parse_standby_status_interval(nil, true), do: @default_ack_standby_status_interval
  defp parse_standby_status_interval(nil, false), do: nil

  defp parse_standby_status_interval(interval, _ack_after_processing)
       when is_integer(interval) and interval > 0,
       do: interval

  defp parse_standby_status_interval(other, _ack_after_processing) do
    raise "standby_status_interval must be a positive integer (milliseconds), got: #{inspect(other)}"
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

  defp raw_state(app_name), do: WalExRegistry.get_state(:get_agent, __MODULE__, app_name)

  defp runtime(config, key, default \\ nil),
    do: config |> Keyword.get(@runtime_key, %{}) |> Map.get(key, default)

  defp put_runtime(config, entries) do
    Keyword.update(config, @runtime_key, Map.new(entries), &Map.merge(&1, Map.new(entries)))
  end

  defp set_agent(app_name), do: WalExRegistry.set_name(:set_agent, __MODULE__, app_name)
end
