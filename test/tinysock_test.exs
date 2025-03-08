defmodule TinySockTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir

  test "it works", %{tmp_dir: tmp_dir} do
    start_supervised!({TinySock, path: tmp_dir, handler: fn :ping -> {:reply, :pong} end})
    :timer.sleep(100)

    assert {:ok, [_ | _] = sock_paths} = TinySock.list(tmp_dir)

    for sock_path <- sock_paths do
      assert {:ok, :pong} = TinySock.call(sock_path, :ping)
    end
  end
end
