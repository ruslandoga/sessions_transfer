defmodule Plausible.Session.Persistence.TinySock do
  @moduledoc ~S"""
  Communication over Unix domain sockets.

  ## Usage

  ```elixir
  TinySock.start_link(
    base_path: "/tmp",
    handler: fn
      {"DUMP-ETS", requested_version, path} ->
        if requested_version == SessionV2.module_info(:md5) do
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
  [sock_path] = TinySock.list("/tmp")

  with :ok <- TinySock.call(sock_path, {"DUMP-ETS", SessionV2.module_info(:md5), dump_path}) do
    for "ets" <> tab <- File.ls!(dump_path) do
      :ets.file2tab(Path.join(dump_path, tab))
    end
  end
  ```
  """

  use GenServer, restart: :transient
  require Logger

  @listen_opts [:binary, packet: :raw, nodelay: true, backlog: 128, active: false]
  @connect_opts [:binary, packet: :raw, nodelay: true, active: false]

  @tag_data "tinysock"
  @tag_size byte_size(@tag_data)

  @spec listen_socket(GenServer.server()) :: :gen_tcp.socket()
  def listen_socket(server), do: Map.fetch!(:sys.get_state(server), :socket)

  def stop(server), do: GenServer.stop(server)

  @doc """
  Lists all Unix domain socket paths for TinySock servers in the given directory.
  """
  @spec list(Path.t()) :: {:ok, [Path.t()]} | {:error, File.posix()}
  def list(base_path) do
    with {:ok, names} <- File.ls(base_path) do
      sock_paths =
        for @tag_data <> _rand = name <- names do
          Path.join(base_path, name)
        end

      {:ok, sock_paths}
    end
  end

  @doc """
  Makes a call to a TinySock server at the given socket path:
  connects to the server, sends a message, waits for a response,
  closes the connection.
  """
  @spec call(Path.t(), term, timeout) :: {:ok, reply :: term} | {:error, :timeout | :inet.posix()}
  def call(sock_path, message, timeout \\ :timer.seconds(5)) do
    with {:ok, socket} <- sock_connect_or_rm(sock_path, timeout) do
      try do
        with :ok <- sock_send(socket, :erlang.term_to_binary(message)) do
          sock_recv(socket, timeout)
        end
      after
        sock_shut_and_close(socket)
      end
    end
  end

  @doc false
  def start_link(opts) do
    {gen_opts, opts} = Keyword.split(opts, [:debug, :name, :spawn_opt, :hibernate_after])
    base_path = Keyword.fetch!(opts, :base_path)
    handler = Keyword.fetch!(opts, :handler)

    case File.mkdir_p(base_path) do
      :ok ->
        GenServer.start_link(__MODULE__, {base_path, handler}, gen_opts)

      {:error, reason} ->
        Logger.warning(
          "tinysock failed to create directory at #{inspect(base_path)}, reason: #{inspect(reason)}"
        )

        :ignore
    end
  end

  @impl true
  def init({base_path, handler}) do
    case sock_listen_or_retry(base_path) do
      {:ok, socket} ->
        Process.flag(:trap_exit, true)
        state = %{socket: socket, handler: handler}
        for _ <- 1..10, do: spawn_acceptor(state)
        {:ok, state}

      {:error, reason} ->
        Logger.warning(
          "tinysock failed to bind a listen socket in #{inspect(base_path)}, reason: #{inspect(reason)}"
        )

        :ignore
    end
  end

  @impl true
  def handle_cast(:accepted, %{socket: socket} = state) do
    if socket, do: spawn_acceptor(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:EXIT, _pid, reason}, state) do
    case reason do
      :normal ->
        {:noreply, state}

      :emfile ->
        Logger.error("tinysock ran out of file descriptors, exiting")
        {:stop, reason, state}

      reason ->
        Logger.error("tinysock request handler exited with unexpected reason: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  defp spawn_acceptor(%{socket: socket, handler: handler}) do
    :proc_lib.spawn_link(__MODULE__, :accept_loop, [_parent = self(), socket, handler])
  end

  @doc false
  def accept_loop(parent, listen_socket, handler) do
    case :gen_tcp.accept(listen_socket, :timer.seconds(5)) do
      {:ok, socket} ->
        GenServer.cast(parent, :accepted)
        handle_message(socket, handler)

      {:error, :timeout} ->
        accept_loop(parent, listen_socket, handler)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        exit(reason)
    end
  end

  defp handle_message(socket, handler) do
    {:ok, message} = sock_recv(socket, _timeout = :timer.seconds(5))
    sock_send(socket, :erlang.term_to_binary(handler.(message)))
  after
    sock_shut_and_close(socket)
  end

  defp sock_listen_or_retry(base_path) do
    sock_name = @tag_data <> Base.url_encode64(:crypto.strong_rand_bytes(4), padding: false)
    sock_path = Path.join(base_path, sock_name)

    case :gen_tcp.listen(0, [{:ifaddr, {:local, sock_path}} | @listen_opts]) do
      {:ok, socket} -> {:ok, socket}
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
        Logger.notice(
          "tinysock failed to connect to #{inspect(sock_path)}, reason: #{inspect(error)}"
        )

        # removes stale socket file
        # possible - but unlikely - race condition
        _ = File.rm(sock_path)
        error
    end
  end

  @dialyzer :no_improper_lists
  @spec sock_send(:gen_tcp.socket(), iodata) :: :ok | {:error, :closed | :inet.posix()}
  defp sock_send(socket, data) do
    :gen_tcp.send(socket, [<<@tag_data, IO.iodata_length(data)::64-little>> | data])
  end

  defp sock_recv(socket, timeout) do
    with {:ok, <<@tag_data, size::64-little>>} <- :gen_tcp.recv(socket, @tag_size + 8, timeout),
         {:ok, binary} <- :gen_tcp.recv(socket, size, timeout) do
      try do
        {:ok, :erlang.binary_to_term(binary, [:safe])}
      rescue
        e -> {:error, e}
      end
    end
  end

  defp sock_shut_and_close(socket) do
    :gen_tcp.shutdown(socket, :read_write)
    :gen_tcp.close(socket)
  end
end
