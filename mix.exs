defmodule SessionsTransfer.MixProject do
  use Mix.Project

  def project do
    [
      app: :sessions_transfer,
      version: "0.1.0",
      elixir: "~> 1.18",
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

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:benchee, github: "bencheeorg/benchee"},
      {:ecto, "~> 3.12"},
      {:ch, "~> 0.3.2"},
      {:telemetry, "~> 1.3"},
      {:con_cache,
       git: "https://github.com/aerosol/con_cache", branch: "ensure-dirty-ops-emit-telemetry"}
    ]
  end
end
