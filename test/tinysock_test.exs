defmodule TinySockTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir

  test "it works", %{tmp_dir: tmp_dir} do
    server =
      start_supervised!({TinySock, base_path: tmp_dir, handler: fn :ping -> {:reply, :pong} end})

    assert {:ok, {:local, sock_path}} = :inet.sockname(TinySock.socket(server))
    assert {:ok, [^sock_path]} = TinySock.list(tmp_dir)
    assert {:ok, :pong} = TinySock.call(sock_path, :ping)
  end
end
