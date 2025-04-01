defmodule Plausible.Session.Persistence do
  @moduledoc """
  Cross-deployment persistence for `:sessions` cache.

  It works by establishing a client-server architecture where:
  - The "taker" one-time task retrieves ETS data from other processes via Unix domain sockets
  - The "giver" server process responds to requests for ETS data via Unix domain sockets
  """

  require Logger

  alias Plausible.ClickhouseSessionV2
  alias Plausible.Session.Persistence.TinySock

  def took?, do: Application.get_env(:plausible, :took_sessions, false)
  defp took, do: Application.put_env(:plausible, :took_sessions, true)

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc false
  def start_link(opts) do
    base_path = Keyword.fetch!(opts, :base_path)

    taker = {Task, fn -> try_take_all_ets(base_path) end}
    giver = {TinySock, base_path: base_path, handler: &giver_handler/1}

    children = [
      # TODO?
      # Supervisor.child_spec(DumpRestore, restart: :transient),
      Supervisor.child_spec(taker, restart: :temporary),
      Supervisor.child_spec(giver, restart: :transient)
    ]

    case File.mkdir_p(base_path) do
      :ok ->
        Supervisor.start_link(children, strategy: :one_for_one)

      {:error, reason} ->
        Logger.warning(
          "#{__MODULE__} failed to create directory #{inspect(base_path)}, reason: #{inspect(reason)}"
        )

        :ignore
    end
  end

  defp session_version do
    IO.iodata_to_binary([
      ClickhouseSessionV2.module_info(:md5),
      # ClickhouseSessionV2.BoolUInt8.module_info(:md5),
      # Ch.module_info(:md5),
      # Plausible.Cache.Adapter.module_info(:md5),
      Plausible.Session.CacheStore.module_info(:md5)
    ])
  end

  defp giver_handler(message) do
    case message do
      {:list, session_version} ->
        if session_version == session_version() and took?() do
          Plausible.Cache.Adapter.get_names(:sessions)
          |> Enum.map(&ConCache.ets/1)
          |> Enum.filter(fn tab -> :ets.info(tab, :size) > 0 end)
        else
          []
        end

      {:send, tab} ->
        dumpscan(tab)

      message ->
        Logger.error(
          "#{__MODULE__} tinysock handler received unknown message: #{inspect(message)}"
        )

        :badarg
    end
  end

  defp maybe_ets_whereis(tab) when is_atom(tab), do: :ets.whereis(tab)
  defp maybe_ets_whereis(tab) when is_reference(tab), do: tab

  defp dumpscan(tab) do
    tab = maybe_ets_whereis(tab)
    :ets.safe_fixtable(tab, true)

    try do
      dumpscan_continue(:ets.first_lookup(tab), _acc = [], tab)
    after
      :ets.safe_fixtable(tab, false)
    end
  end

  defp dumpscan_continue({k, [record]}, acc, tab) do
    # TODO to map?
    {_k, %ClickhouseSessionV2{}} = record
    dumpscan_continue(:ets.next_lookup(tab, k), [record | acc], tab)
  end

  defp dumpscan_continue(:"$end_of_table", acc, _tab) do
    acc
  end

  defp try_take_all_ets(base_path) do
    counter = :counters.new(1, [:write_concurrency])

    try do
      take_all_ets(base_path, counter)
    after
      Logger.notice("#{__MODULE__} took #{:counters.get(counter, 1)} sessions")

      took()
    end
  end

  defp take_all_ets(base_path, counter) do
    # TODO sort socks by timestamp?
    with {:ok, socks} <- TinySock.list(base_path) do
      session_version = session_version()

      Enum.each(socks, fn sock ->
        with {:ok, tabs} <- TinySock.call(sock, {:list, session_version}) do
          tasks =
            Enum.map(tabs, fn tab ->
              Task.async(fn ->
                with {:ok, records} <- TinySock.call(sock, {:send, tab}) do
                  savescan(records, counter)
                end
              end)
            end)

          Task.await_many(tasks)
        end
      end)
    end
  end

  defp savescan([record | rest], counter) do
    {key, %ClickhouseSessionV2{} = session} = record
    # TODO try to Ecto.Changeset.cast?
    # TODO upsert? only if newer? i.e. more events?
    Plausible.Cache.Adapter.put(:sessions, key, session)
    :counters.add(counter, 1, 1)
    savescan(rest, counter)
  end

  defp savescan([], _counter), do: :ok
end
