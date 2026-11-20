defmodule WalEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :walex,
      version: "4.9.0",
      elixir: "~> 1.19",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      aliases: aliases(),
      name: "WalEx",
      source_url: "https://github.com/cpursley/walex",
      docs: docs(),
      test_coverage: [tool: ExCoveralls],
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: compilers(),
      dialyzer: [plt_add_apps: [:mix, :ex_unit], ignore_warnings: ".dialyzer_ignore.exs"]
    ]
  end

  def cli do
    [preferred_envs: [quality: :test]]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:postgrex, ">= 0.20.0"},
      {:decimal, "~> 3.1"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},

      # Dev & Test
      {:ex_doc, "~> 0.40.4", only: :dev, runtime: false},
      {:sobelow, "~> 0.19.0", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.9", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.23.0", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5.4", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4.5", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18.5", only: [:dev, :test], runtime: false},
      {:rambo, "~> 0.3.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp description() do
    "Listen to change events on your Postgres tables then perform callback-like actions with the data."
  end

  defp docs() do
    [
      main: "readme",
      extras: [
        "README.md",
        "docs/guides/postgres-setup.md",
        "docs/guides/configuration.md",
        "docs/guides/events.md",
        "docs/guides/delivery-guarantees.md",
        "docs/guides/telemetry.md"
      ],
      groups_for_extras: [
        Guides: ~r{docs/guides/}
      ]
    ]
  end

  defp package() do
    [
      files: ~w(lib test docs/guides .formatter.exs mix.exs README* LICENSE*),
      maintainers: ["Chase Pursley"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/cpursley/walex"}
    ]
  end

  defp aliases() do
    [
      "walex.reset": ["walex.drop", "walex.setup"],
      # Run tests and check coverage
      test: ["test", "coveralls"],
      # Run to check the quality of your code
      #
      # `ex_dna --max-clones 2` allows the duplicated GenServer scaffolding
      # (start_link/process/registry_name/init) shared between WalEx.Events and
      # WalEx.Events.EventModules — they are two distinct services and extracting
      # the boilerplate via macro hurts readability for trivial gain.
      quality: [
        "compile --warnings-as-errors",
        "deps.unlock --unused",
        "format --check-formatted",
        "sobelow --config",
        "ex_dna --max-clones 2",
        "doctor",
        "credo --strict",
        "dialyzer"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp compilers do
    unless Mix.env() == :prod do
      Mix.compilers() ++ [:rambo]
    else
      Mix.compilers()
    end
  end
end
