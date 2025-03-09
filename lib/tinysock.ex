defmodule TinySock do
  @moduledoc ~S"""
  Documentation for `TinySock`.

  ## Usage

  ```elixir
  TinySock.start_link(
    path: "/tmp",
    handler: fn
      {"DUMP-ETS", requested_version, path} ->
        if requested_version == SessionV2.module_info[:md5] do
          for tab <- [:sessions1, :sessions2, :sessions3] do
            :ok = :ets.tab2file(tab, Path.join(path, "ets#{tab}"))
          end

          :ok
        else
          {:error, :invalid_version}
        end
    end
  )

  dump_path = "/tmp/ysSEjw"
  File.mkdir_p!(dump_path)

  with :ok <- TinySock.call("/tmp/tinysockN8Yd9g", {"DUMP-ETS", SessionV2.module_info[:md5], dump_path}) do
    for "ets" <> tab <- File.ls!(dump_path) do
      :ets.file2tab(Path.join(dump_path, tab))
    end
  end
  ```
  """

  use Task
  require Logger

  @listen_opts [:binary, packet: :raw, nodelay: true, backlog: 128, active: false]
  @connect_opts [:binary, packet: :raw, nodelay: true, active: false]

  @tag_data "tinysock"
  @tag_data_size byte_size(@tag_data)

  @doc "TODO"
  def start_link(opts) do
    base_path = Keyword.fetch!(opts, :path)
    handler = Keyword.fetch!(opts, :handler)

    case File.mkdir_p(base_path) do
      :ok ->
        Task.start_link(fn -> TinySock.serve(base_path, handler) end)

      {:error, reason} ->
        Logger.error(
          "failed to create directory at #{inspect(base_path)}, reason: #{inspect(reason)}"
        )

        :ignore
    end
  end

  @doc false
  def serve(base_path, handler) do
    case sock_listen_or_retry(base_path) do
      {:ok, listen_socket, _sock_path} ->
        handle_message_loop(listen_socket, handler)

      {:error, reason} ->
        Logger.error(
          "failed to open a listen socket in #{inspect(base_path)}, reason: #{inspect(reason)}"
        )
    end
  end

  @doc "TODO"
  def list(base_path) do
    with {:ok, names} <- File.ls(base_path) do
      sock_paths =
        for @tag_data <> _rand = name <- names do
          Path.join(base_path, name)
        end

      {:ok, sock_paths}
    end
  end

  @doc "TODO"
  def call(sock_path, message, timeout \\ :timer.seconds(5)) do
    with {:ok, socket} <- sock_connect_or_rm(sock_path, timeout) do
      try do
        with :ok <- sock_send(socket, :erlang.term_to_binary(message)) do
          sock_recv(socket, timeout)
        end
      after
        sock_close(socket)
      end
    end
  end

  defp handle_message_loop(listen_socket, handler) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, socket} ->
        with {:ok, message} <- sock_recv(socket, _timeout = :timer.seconds(5)) do
          try do
            with {:reply, reply} <- handler.(message) do
              sock_send(socket, :erlang.term_to_binary(reply))
            end
          rescue
            e ->
              message = Exception.format(:error, e, __STACKTRACE__)
              Logger.error("failed to handle message, reason: " <> message)
          after
            sock_close(socket)
          end

          handle_message_loop(listen_socket, handler)
        else
          other ->
            Logger.error("failed to receive message, reason: #{inspect(other)}")
            :gen_tcp.close(socket)
            handle_message_loop(listen_socket, handler)
        end

      {:error, reason} ->
        Logger.error("failed to accept connection, shutting down, reason: #{inspect(reason)}")
    end
  end

  defp sock_listen_or_retry(base_path) do
    sock_name = @tag_data <> Base.url_encode64(:crypto.strong_rand_bytes(4), padding: false)
    sock_path = Path.join(base_path, sock_name)

    case :gen_tcp.listen(0, [{:ifaddr, {:local, sock_path}} | @listen_opts]) do
      {:ok, socket} -> {:ok, socket, sock_path}
      {:error, :eaddrinuse} -> sock_listen_or_retry(base_path)
      {:error, reason} -> {:error, reason}
    end
  end

  defp sock_connect_or_rm(sock_path, timeout) do
    case :gen_tcp.connect({:local, sock_path}, 0, @connect_opts, timeout) do
      {:ok, socket} ->
        {:ok, socket}

      {:error, :timeout} = error ->
        error

      {:error, _reason} = error ->
        _ = File.rm(sock_path)
        error
    end
  end

  defp sock_send(socket, binary) do
    :gen_tcp.send(socket, <<@tag_data, byte_size(binary)::64, binary::bytes>>)
  end

  defp sock_recv(socket, timeout) do
    with {:ok, <<@tag_data, more::1, size::63>>} <-
           :gen_tcp.recv(socket, @tag_data_size + 8, timeout),
         {:ok, binary} <- :gen_tcp.recv(socket, size, timeout) do
      try do
        case more do
          0 -> {:done, :erlang.binary_to_term(binary, [:safe])}
          1 -> {:more, binary}
        end
      rescue
        e -> {:error, e}
      end
    end
  end

  defp sock_close(socket) do
    :gen_tcp.shutdown(socket, :read_write)
    :gen_tcp.close(socket)
  end
end
