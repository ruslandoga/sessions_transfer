defmodule Bench do
  def maketab do
    tab = :ets.new(:test, [:set, :public])

    records =
      Enum.map(1..50_000, fn i ->
        session = %Plausible.ClickhouseSessionV2{
          hostname: "example-0.com",
          site_id: 1053,
          user_id: i,
          session_id: 10_371_982_002_942_482_638,
          start: ~N[2025-04-02 19:51:32],
          duration: 0,
          is_bounce: true,
          entry_page: "/",
          exit_page: "/",
          exit_page_hostname: "example-0.com",
          pageviews: 1,
          events: 1,
          sign: 1,
          "entry_meta.key": nil,
          "entry_meta.value": nil,
          utm_medium: "medium",
          utm_source: "source",
          utm_campaign: "campaign",
          utm_content: "content",
          utm_term: "term",
          referrer: "ref",
          referrer_source: "refsource",
          click_id_param: nil,
          country_code: "EE",
          subdivision1_code: nil,
          subdivision2_code: nil,
          city_geoname_id: nil,
          screen_size: "Desktop",
          operating_system: "Mac",
          operating_system_version: "11",
          browser: "browser",
          browser_version: "55",
          timestamp: ~N[2025-04-02 19:51:32],
          transferred_from: nil,
          acquisition_channel: nil
        }

        {{session.site_id, session.user_id}, session}
      end)

    :ets.insert(tab, records)

    tab
  end

  def dumpscan(tab, file, dumpfn) do
    :ets.safe_fixtable(tab, true)
    File.rm(file)
    # fd = File.open!(file, [:raw, :binary, :append, :exclusive, {:delayed_write, 524_288, 2000}])
    fd = File.open!(file, [:raw, :binary, :append, :exclusive])

    try do
      do_dumpscan(:ets.first_lookup(tab), [], 0, tab, fd, dumpfn)
    after
      :ok = File.close(fd)
      :ets.safe_fixtable(tab, false)
    end
  end

  defp do_dumpscan({k, [{_, v}]}, cache, cache_len, tab, fd, dumpfn) do
    # :ok = :file.write(fd, dumpfn.(v))
    bin = dumpfn.(v)
    bin_len = byte_size(bin)
    true = bin_len < 4_294_967_296
    new_cache = append_cache(cache, <<bin_len::32, bin::bytes>>)
    new_cache_len = cache_len + bin_len + 4

    if new_cache_len > 500_000 do
      :ok = :file.write(fd, new_cache)
      do_dumpscan(:ets.next_lookup(tab, k), [], 0, tab, fd, dumpfn)
    else
      do_dumpscan(:ets.next_lookup(tab, k), new_cache, new_cache_len, tab, fd, dumpfn)
    end
  end

  defp do_dumpscan(:"$end_of_table", cache, cache_len, _tab, fd, _dumpfn) do
    if cache_len > 0 do
      :ok = :file.write(fd, cache)
    end

    :ok
  end

  @compile {:inline, append_cache: 2}
  defp append_cache([], bin), do: bin
  defp append_cache(cache, bin), do: [cache | bin]

  def sendscan(tab, sock, dumpfn) do
    {:ok, socket} =
      :gen_tcp.connect(
        {:local, sock},
        0,
        [mode: :binary, packet: :raw, nodelay: true, active: false],
        5000
      )

    :ets.safe_fixtable(tab, true)

    try do
      do_sendscan(:ets.first_lookup(tab), [], 0, tab, socket, dumpfn)
    after
      :gen_tcp.shutdown(socket, :read_write)
      :gen_tcp.close(socket)
      :ets.safe_fixtable(tab, false)
    end
  end

  defp do_sendscan({k, [{_, v}]}, cache, cache_len, tab, socket, dumpfn) do
    bin = dumpfn.(v)
    bin_len = byte_size(bin)
    true = bin_len < 4_294_967_296
    new_cache = append_cache(cache, <<bin_len::32, bin::bytes>>)
    new_cache_len = cache_len + bin_len + 4

    if new_cache_len > 500_000 do
      :ok = :gen_tcp.send(socket, [<<"tinysock", 1::1, new_cache_len::63>> | new_cache])
      do_sendscan(:ets.next_lookup(tab, k), [], 0, tab, socket, dumpfn)
    else
      do_sendscan(:ets.next_lookup(tab, k), new_cache, new_cache_len, tab, socket, dumpfn)
    end
  end

  defp do_sendscan(:"$end_of_table", cache, cache_len, _tab, socket, _dumpfn) do
    :ok = :gen_tcp.send(socket, [<<"tinysock", 0::1, cache_len::63>> | cache])
  end

  def looprecv(socket) do
    {:ok, <<"tinysock", more::1, size::63>>} =
      :gen_tcp.recv(socket, byte_size("tinysock") + 8, :infinity)

    {:ok, _binary} = :gen_tcp.recv(socket, size, :infinity)

    case more do
      0 ->
        :gen_tcp.shutdown(socket, :read)
        :gen_tcp.close(socket)

      1 ->
        looprecv(socket)
    end
  end

  def onesend(tab, sock) do
    {:ok, socket} =
      :gen_tcp.connect(
        {:local, sock},
        0,
        [mode: :binary, packet: :raw, nodelay: true, active: false],
        5000
      )

    :ets.safe_fixtable(tab, true)

    try do
      data = :erlang.term_to_iovec(onepass(:ets.first_lookup(tab), [], tab))
      :gen_tcp.send(socket, [<<"tinysock", IO.iodata_length(data)::64-little>> | data])
    after
      :gen_tcp.shutdown(socket, :read_write)
      :gen_tcp.close(socket)
      :ets.safe_fixtable(tab, false)
    end
  end

  defp onepass({k, [{_, v}]}, acc, tab) do
    onepass(:ets.next_lookup(tab, k), [v | acc], tab)
  end

  defp onepass(:"$end_of_table", acc, _tab) do
    acc
  end

  def onerecv(socket) do
    {:ok, <<"tinysock", size::64-little>>} =
      :gen_tcp.recv(socket, byte_size("tinysock") + 8, :infinity)

    {:ok, _binary} = sock_onerecv_continue(socket, size, :infinity, [])
    :gen_tcp.shutdown(socket, :read)
    :gen_tcp.close(socket)
  end

  @five_mb 5 * 1024 * 1024

  # for larger messages (>70MB), we need to read in chunks or we get {:error, :enomem}
  defp sock_onerecv_continue(socket, size, timeout, acc) do
    with {:ok, data} <- :gen_tcp.recv(socket, min(size, @five_mb), timeout) do
      acc = [acc | data]

      case size - byte_size(data) do
        0 -> {:ok, IO.iodata_to_binary(acc)}
        left -> sock_onerecv_continue(socket, left, timeout, acc)
      end
    end
  end
end

Benchee.run(
  %{
    # "dump as json 1" =>
    #   {fn -> Bench.dumpscan(:test, "json.dump", &:json.encode/1) end,
    #    after_each: fn _ -> File.rm("json.dump") end},
    # "dump as json 2" =>
    #   {fn -> Bench.dumpscan(:test, "json.dump", &:json.encode/1, 524_288) end,
    #    after_each: fn _ -> File.rm("json.dump") end}
    # "dump as term_to_binary" =>
    #   {fn {tab, file} = input ->
    #      Bench.dumpscan(tab, file, &:erlang.term_to_binary/1)
    #      input
    #    end,
    #    before_each: fn tab -> {tab, "term_to_binary#{System.unique_integer()}.dump"} end,
    #    after_each: fn {_tab, file} -> File.rm(file) end},
    # "dump as tab2file" =>
    #   {fn {tab, file} = input ->
    #      :ets.tab2file(tab, file)
    #      input
    #    end,
    #    before_each: fn tab -> {tab, ~c"tab2file#{System.unique_integer()}.dump"} end,
    #    after_each: fn {_tab, file} -> File.rm(file) end},
    "pass through socket" =>
      {fn {tab, file} = input ->
         Bench.sendscan(tab, file, &:erlang.term_to_binary/1)
         input
       end,
       before_each: fn tab ->
         file = "dump#{System.unique_integer()}.sock"
         File.rm(file)
         parent = self()

         spawn_link(fn ->
           {:ok, l} =
             :gen_tcp.listen(0,
               ifaddr: {:local, file},
               mode: :binary,
               packet: :raw,
               nodelay: true,
               backlog: 128,
               active: false
             )

           send(parent, :go)
           {:ok, s} = :gen_tcp.accept(l)
           Bench.looprecv(s)
         end)

         receive do
           :go -> :ok
         end

         {tab, file}
       end,
       after_each: fn {_tab, file} -> File.rm(file) end},
    "onepass" =>
      {fn {tab, file} = input ->
         Bench.onesend(tab, file)
         input
       end,
       before_each: fn tab ->
         file = "dump#{System.unique_integer()}.sock"
         File.rm(file)
         parent = self()

         spawn_link(fn ->
           {:ok, l} =
             :gen_tcp.listen(0,
               ifaddr: {:local, file},
               mode: :binary,
               packet: :raw,
               nodelay: true,
               backlog: 128,
               active: false
             )

           send(parent, :go)
           {:ok, s} = :gen_tcp.accept(l)
           Bench.onerecv(s)
         end)

         receive do
           :go -> :ok
         end

         {tab, file}
       end,
       after_each: fn {_tab, file} -> File.rm(file) end}
  },
  parallel: System.schedulers(),
  before_scenario: fn _input -> Bench.maketab() end,
  after_scenario: fn tab -> :ets.delete(tab) end
  # profile_after: :tprof
)
