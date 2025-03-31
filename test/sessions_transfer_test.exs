defmodule SessionsTransferTest do
  use ExUnit.Case

  setup do
    Application.put_env(:plausible, Plausible.Cache.Adapter, sessions: [partitions: 100])
  end

  test "it works" do
    tmp_dir = Path.join(System.tmp_dir!(), "tinysock_test")
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    old = start_another_plausible(tmp_dir)
    session = put_session(old, %{user_id: "123"})
  end

  defp start_another_plausible(data_dir) do
    {:ok, pid, _node} = :peer.start_link(%{connection: {{127, 0, 0, 1}, 0}})
    add_code_paths(pid)
    transfer_configuration(pid)
    ensure_applications_started(pid)
    pid
  end

  defp add_code_paths(pid) do
    :ok = :peer.call(pid, :code, :add_paths, [:code.get_path()])
  end

  defp transfer_configuration(pid) do
    for {app_name, _, _} <- Application.loaded_applications() do
      for {key, val} <- Application.get_all_env(app_name) do
        :ok = :peer.call(pid, Application, :put_env, [app_name, key, val])
      end
    end
  end

  defp ensure_applications_started(pid) do
    {:ok, _apps} = :peer.call(pid, Application, :ensure_all_started, [:mix])
    :ok = :peer.call(pid, Mix, :env, [Mix.env()])

    for {app_name, _, _} <- Application.loaded_applications() do
      {:ok, _apps} = :peer.call(pid, Application, :ensure_all_started, [app_name])
    end
  end

  defp put_session(pid, overrides) do
    default = %Plausible.ClickhouseSessionV2{
      sign: 1,
      session_id: Ecto.UUID.generate(),
      user_id: Ecto.UUID.generate(),
      hostname: "example.com",
      site_id: Enum.random(1000..10_000),
      entry_page: "/",
      pageviews: 1,
      events: 1,
      start: DateTime.utc_now(:second),
      timestamp: DateTime.utc_now(:second),
      is_bounce: false
    }

    session = struct!(default, overrides)
    key = {session.site_id, session.user_id}
    :peer.call(pid, Plausible.Cache.Adapter, :put, [:sessions, key, session, [dirty?: true]])
  end
end
