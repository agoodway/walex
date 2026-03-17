defmodule WalEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :walex,
      version: "4.7.1",
      elixir: "~> 1.19",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      aliases: aliases(),
      name: "WalEx",
      source_url: "https://github.com/cpursley/walex",
      test_coverage: [tool: ExCoveralls],
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: compilers()
    ]
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
      {:decimal, "~> 2.3.0"},
      {:jason, "~> 1.4"},
      {:timex, "~> 3.7.13"},

      # Dev & Test
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:sobelow, "~> 0.14.1", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7.15", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18.5", only: [:dev, :test], runtime: false},
      {:rambo, "~> 0.3.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp description() do
    "Listen to change events on your Postgres tables then perform callback-like actions with the data."
  end

  defp package() do
    [
      files: ~w(lib test .formatter.exs mix.exs README* LICENSE*),
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
      quality: [
        "format --check-formatted",
        "sobelow --config",
        "credo --only warning"
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
