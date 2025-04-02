defmodule SessionsTransfer.MixProject do
  use Mix.Project

  def project do
    [
      app: :sessions_transfer,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Plausible.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:benchee, github: "bencheeorg/benchee"},
      {:ecto, "~> 3.12"},
      {:ch, "~> 0.3.2"},
      {:telemetry, "~> 1.3"},
      {:con_cache,
       git: "https://github.com/aerosol/con_cache", branch: "ensure-dirty-ops-emit-telemetry"},
      {:ex_machina, "~> 2.3", only: :test},
      {:siphash, "~> 3.2"}
    ]
  end
end
