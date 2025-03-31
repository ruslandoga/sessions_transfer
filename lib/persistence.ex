defmodule Plausible.Session.Persistence do
  @moduledoc """
  Cross-deployment persistence and sharing for `:sessions` cache.

  It works by establishing a client-server architecture where:
  - The "taker" one-time task retrieves ETS data from other processes via Unix domain sockets
  - The "giver" server process responds to requests for ETS data via Unix domain sockets
  """

  require Logger

  alias Plausible.ClickhouseSessionV2
  alias Plausible.Session.Persistence.TinySock

  @took_sessions_key :took_sessions
  def took?, do: Application.get_env(:plausible, @took_sessions_key, false)
  defp took, do: Application.put_env(:plausible, @took_sessions_key, true)

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

    # TODO
    File.mkdir_p!(base_path)

    taker = {Task, fn -> take_all_ets(base_path) end}
    giver = {TinySock, base_path: base_path, handler: &give_ets_handler/1}

    children = [
      # TODO?
      # Supervisor.child_spec(DumpRestore, restart: :transient),
      Supervisor.child_spec(taker, restart: :temporary),
      Supervisor.child_spec(giver, restart: :transient)
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
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

  @give_tag "GIVE-ETS"

  defp give_ets_handler(message) do
    case message do
      {@give_tag, session_version, dump_path} ->
        maybe_give_ets(session_version, dump_path)

      message ->
        Logger.error("Unknown message in #{__MODULE__}: #{inspect(message)}")
        :badarg
    end
  end

  defp maybe_give_ets(session_version, dump_path) do
    if session_version == session_version() and took?() do
      give_ets(dump_path)
    else
      []
    end
  end

  @spec give_ets(Path.t()) :: [Path.t()]
  defp give_ets(dump_path) do
    cache_names = Plausible.Cache.Adapter.get_names(:sessions)

    dumps =
      Enum.flat_map(cache_names, fn cache_name ->
        tab = ConCache.ets(cache_name)

        if :ets.info(tab, :size) == 0 do
          []
        else
          path = Path.join(dump_path, to_string(cache_name))

          Task.async(fn ->
            :ok = dumpscan(tab, path)
            path
          end)
        end
      end)

    Task.await_many(dumps)
  end

  defp dumpscan(tab, file) do
    tab = :ets.whereis(tab)
    :ets.safe_fixtable(tab, true)

    File.rm(file)
    fd = File.open!(file, [:raw, :binary, :append, :exclusive])

    try do
      dumpscan_continue(:ets.first_lookup(tab), [], 0, tab, fd)
    after
      :ets.safe_fixtable(tab, false)
      :ok = File.close(fd)
    end
  end

  defp dumpscan_continue({k, [record]}, cache, cache_len, tab, fd) do
    {_key, %ClickhouseSessionV2{}} = record

    # TODO: use json or plain maps? and then Ecto.Changeset.cast?
    bin = :erlang.term_to_binary(record)
    bin_len = byte_size(bin)
    new_cache = append_cache(cache, <<bin_len::64-little, bin::bytes>>)
    new_cache_len = cache_len + bin_len + 8

    if new_cache_len > 500_000 do
      :ok = :file.write(fd, new_cache)
      dumpscan_continue(:ets.next_lookup(tab, k), [], 0, tab, fd)
    else
      dumpscan_continue(:ets.next_lookup(tab, k), new_cache, new_cache_len, tab, fd)
    end
  end

  defp dumpscan_continue(:"$end_of_table", cache, cache_len, _tab, fd) do
    if cache_len > 0 do
      :ok = :file.write(fd, cache)
    end

    :ok
  end

  @dialyzer :no_improper_lists
  @compile {:inline, append_cache: 2}
  defp append_cache([], bin), do: bin
  defp append_cache(cache, bin), do: [cache | bin]

  defp take_all_ets(base_path) do
    try do
      # TODO sort socks by timestamp?
      with {:ok, socks} <- TinySock.list(base_path) do
        session_version = session_version()

        Enum.each(socks, fn sock ->
          rand_dump = "dump" <> Base.url_encode64(:crypto.strong_rand_bytes(6))
          dump_path = Path.join(base_path, rand_dump)
          take_ets(sock, session_version, dump_path)
        end)
      end
    after
      took()
    end
  end

  defp take_ets(sock, session_version, dump_path) do
    File.mkdir_p!(dump_path)

    try do
      with {:ok, dumps} <- TinySock.call(sock, {@give_tag, session_version, dump_path}) do
        loads =
          Enum.map(dumps, fn path ->
            Task.async(fn -> savescan(File.read!(path)) end)
          end)

        Task.await_many(loads)
      end
    after
      File.rm_rf!(dump_path)
    end
  end

  defp savescan(<<bin_len::64-little, bin::size(bin_len)-bytes, rest::bytes>>) do
    {key, %ClickhouseSessionV2{} = session} = :erlang.binary_to_term(bin, [:safe])
    # TODO upsert? only if newer? i.e. more events?
    Plausible.Cache.Adapter.put(:sessions, key, session)
    savescan(rest)
  end

  defp savescan(<<>>), do: :ok
end
