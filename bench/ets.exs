:ets.new(:test, [
  :named_table,
  :set,
  :public,
  write_concurrency: :auto,
  read_concurrency: true
])

Enum.each(1..100_000, fn i ->
  :ets.insert(:test, {i, %{id: i, name: "name-#{i}"}})
end)

defmodule Bench do
  def dumpscan(tab, file, dumpfn) do
    tab = :ets.whereis(tab)
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
        [
          :binary,
          packet: :raw,
          nodelay: true,
          active: false,
          buffer: 524_288,
          sndbuf: 524_288
        ],
        5000
      )

    tab = :ets.whereis(tab)
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
end

Benchee.run(
  %{
    # "dump as json 1" =>
    #   {fn -> Bench.dumpscan(:test, "json.dump", &:json.encode/1) end,
    #    after_each: fn _ -> File.rm("json.dump") end},
    # "dump as json 2" =>
    #   {fn -> Bench.dumpscan(:test, "json.dump", &:json.encode/1, 524_288) end,
    #    after_each: fn _ -> File.rm("json.dump") end}
    "dump as term_to_binary" =>
      {fn file ->
         Bench.dumpscan(:test, file, &:erlang.term_to_binary/1)
         file
       end,
       before_each: fn _ -> "term_to_binary#{System.unique_integer()}.dump" end,
       after_each: fn file -> File.rm(file) end},
    "dump as tab2file" =>
      {fn file ->
         :ets.tab2file(:test, file)
         file
       end,
       before_each: fn _ -> ~c"tab2file#{System.unique_integer()}.dump" end,
       after_each: fn file -> File.rm(file) end},
    "pass through socket" =>
      {fn file ->
         Bench.sendscan(:test, file, &:erlang.term_to_binary/1)
         file
       end,
       before_each: fn _ ->
         file = "dump#{System.unique_integer()}.sock"
         File.rm(file)
         parent = self()

         spawn_link(fn ->
           {:ok, l} =
             :gen_tcp.listen(0, [
               {:ifaddr, {:local, file}},
               :binary,
               packet: :raw,
               nodelay: true,
               backlog: 128,
               active: false,
               buffer: 524_288,
               recbuf: 524_288
             ])

           send(parent, :go)
           {:ok, s} = :gen_tcp.accept(l)
           Bench.looprecv(s)
         end)

         receive do
           :go -> :ok
         end

         file
       end,
       after_each: fn file -> File.rm(file) end}
  },
  parallel: System.schedulers()
  # profile_after: :tprof
)
