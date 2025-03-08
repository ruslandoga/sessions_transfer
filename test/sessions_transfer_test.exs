defmodule SessionsTransferTest do
  use ExUnit.Case

  # @tag :tmp_dir
  # test "try_lock/1", %{tmp_dir: tmp_dir} do
  #   path = Path.join([tmp_dir, "socket"])

  #   # please see https://linux.die.net/man/7/unix for UNIX_PATH_MAX and stuff
  #   # https://serverfault.com/questions/641347/check-if-a-path-exceeds-maximum-for-unix-domain-socket
  #   assert byte_size(path) < 104
  #   assert {:ok, listen_socket} = SessionsTransfer.try_lock(path)
  #   assert {:error, :eaddrinuse} = SessionsTransfer.try_lock(path)

  #   assert :ok = SessionsTransfer.unlock(listen_socket)
  #   assert {:ok, _another_listen_socket} = SessionsTransfer.try_lock(path)
  # end
end
