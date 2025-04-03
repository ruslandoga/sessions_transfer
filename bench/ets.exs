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
      :gen_tcp.connect({:local, sock}, 0,
        mode: :binary,
        packet: :raw,
        nodelay: true,
        active: false
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
    # v = Map.filter(v, &Bench.session_params_filter/1)
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
      :gen_tcp.connect({:local, sock}, 0,
        mode: :binary,
        packet: :raw,
        nodelay: true,
        active: false
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
    # v = Map.filter(v, &Bench.session_params_filter/1)
    v = values(v)
    onepass(:ets.next_lookup(tab, k), [v | acc], tab)
  end

  defp onepass(:"$end_of_table", acc, _tab) do
    acc
  end

  @doc false
  def session_params_filter({:__struct__, _}), do: false
  def session_params_filter({:__meta__, _}), do: false
  def session_params_filter({_, nil}), do: false
  def session_params_filter({_, _}), do: true

  @compile {:inline, values: 1}
  defp values(session) do
    %Plausible.ClickhouseSessionV2{
      hostname: hostname,
      site_id: site_id,
      user_id: user_id,
      session_id: session_id,
      start: start,
      duration: duration,
      is_bounce: is_bounce,
      entry_page: entry_page,
      exit_page: exit_page,
      exit_page_hostname: exit_page_hostname,
      pageviews: pageviews,
      events: events,
      sign: sign,
      "entry_meta.key": entry_meta_key,
      "entry_meta.value": entry_meta_value,
      utm_medium: utm_medium,
      utm_source: utm_source,
      utm_campaign: utm_campaign,
      utm_content: utm_content,
      utm_term: utm_term,
      referrer: referrer,
      referrer_source: referrer_source,
      click_id_param: click_id_param,
      country_code: country_code,
      subdivision1_code: subdivision1_code,
      subdivision2_code: subdivision2_code,
      city_geoname_id: city_geoname_id,
      screen_size: screen_size,
      operating_system: operating_system,
      operating_system_version: operating_system_version,
      browser: browser,
      browser_version: browser_version,
      timestamp: timestamp,
      transferred_from: transferred_from,
      acquisition_channel: acquisition_channel
    } = session

    [
      nil2list(hostname),
      nil2list(site_id),
      nil2list(user_id),
      nil2list(session_id),
      nil2list(ts2int(start)),
      nil2list(duration),
      nil2list(is_bounce),
      nil2list(entry_page),
      nil2list(exit_page),
      nil2list(exit_page_hostname),
      nil2list(pageviews),
      nil2list(events),
      nil2list(sign),
      nil2list(entry_meta_key),
      nil2list(entry_meta_value),
      nil2list(utm_medium),
      nil2list(utm_source),
      nil2list(utm_campaign),
      nil2list(utm_content),
      nil2list(utm_term),
      nil2list(referrer),
      nil2list(referrer_source),
      nil2list(click_id_param),
      nil2list(country_code),
      nil2list(subdivision1_code),
      nil2list(subdivision2_code),
      nil2list(city_geoname_id),
      nil2list(screen_size),
      nil2list(operating_system),
      nil2list(operating_system_version),
      nil2list(browser),
      nil2list(browser_version),
      nil2list(ts2int(timestamp)),
      nil2list(transferred_from),
      nil2list(acquisition_channel)
    ]
  end

  @compile {:inline, nil2list: 1}
  defp nil2list(nil), do: []
  defp nil2list(value), do: value

  {naive_epoch_gregorian, 0} = NaiveDateTime.to_gregorian_seconds(~N[1970-01-01 00:00:00])

  @compile {:inline, ts2int: 1}
  defp ts2int(%NaiveDateTime{} = naive) do
    {s, _} = NaiveDateTime.to_gregorian_seconds(naive)
    s - unquote(naive_epoch_gregorian)
  end

  defp ts2int(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp ts2int(nil = n), do: n

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
    "dump as tab2file" =>
      {fn {tab, file} = input ->
         :ets.tab2file(tab, file)
         input
       end,
       before_each: fn tab -> {tab, ~c"tab2file#{System.unique_integer()}.dump"} end,
       after_each: fn {_tab, file} -> File.rm(file) end},
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
