defmodule Plausible.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    children =
      [
        Plausible.Session.BalancerSupervisor,
        Plausible.Cache.Adapter.child_specs(:sessions, :cache_sessions,
          ttl_check_interval: :timer.seconds(10),
          global_ttl: :timer.minutes(30),
          ets_options: [read_concurrency: true, write_concurrency: true]
        ),
        if data_dir = Application.get_env(:plausible, :data_dir) do
          {Plausible.Session.Persistence, base_path: Path.join(data_dir, "sessions")}
        end
      ]
      |> List.flatten()
      |> Enum.reject(&is_nil/1)

    opts = [strategy: :one_for_one, name: Plausible.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
