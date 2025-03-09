defmodule UDSTest do
  use ExUnit.Case, async: true

  @listen_opts [:binary, packet: :raw, nodelay: true, backlog: 128, active: false]
  @connect_opts [:binary, packet: :raw, nodelay: true, active: false]

  @moduletag :tmp_dir

  test "already dead", %{tmp_dir: tmp_dir} do
    sock_path = Path.join(tmp_dir, "my.sock")
    on_exit(fn -> File.rm!(sock_path) end)

    {:ok, listen_socket} = :gen_tcp.listen(0, [{:ifaddr, {:local, sock_path}}] ++ @listen_opts)
    assert File.stat!(sock_path)

    assert :ok = :gen_tcp.close(listen_socket)
    assert {:error, :econnrefused} = :gen_tcp.connect({:local, sock_path}, 0, @connect_opts)
  end

  test "dead on send", %{tmp_dir: tmp_dir} do
    sock_path = Path.join(tmp_dir, "my.sock")
    on_exit(fn -> File.rm!(sock_path) end)

    {:ok, listen_socket} = :gen_tcp.listen(0, [{:ifaddr, {:local, sock_path}}] ++ @listen_opts)
    assert File.stat!(sock_path)

    assert {:ok, client_socket} = :gen_tcp.connect({:local, sock_path}, 0, @connect_opts)
    assert :ok = :gen_tcp.close(listen_socket)
    assert {:error, :closed} = :gen_tcp.send(client_socket, "hello")
  end
end
