defmodule Plausible.Session.Transfer.TinySock do
  @moduledoc false
  use GenServer
  require Logger

  @listen_opts [:binary, packet: :raw, nodelay: true, backlog: 1024, active: false]
  @connect_opts [:binary, packet: :raw, nodelay: true, active: false]

  @tag_data "tinysock"
  @tag_size byte_size(@tag_data)

  @spec listen_socket(GenServer.server()) :: :gen_tcp.socket()
  def listen_socket(server), do: Map.fetch!(:sys.get_state(server), :socket)

  @spec listen_socket_path(GenServer.server()) :: Path.t()
  def listen_socket_path(server) do
    {:ok, {:local, path}} = :inet.sockname(listen_socket(server))
    path
  end

  @spec mkdir(Path.t()) :: :ok | {:error, File.posix()}
  def mkdir(dir) do
    case File.stat(dir) do
      {:ok, stat} ->
        case {stat.type, stat.access} do
          {:directory, access} when access in [:read_write, :write] -> :ok
          _ -> {:error, :eacces}
        end

      {:error, _} ->
        File.mkdir_p(dir)
    end
  end

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

  @spec call(Path.t(), term, timeout) :: {:ok, reply :: term} | {:error, :timeout | :inet.posix()}
  def call(sock_path, message, timeout \\ :timer.seconds(5)) do
    with {:ok, socket} <- sock_connect_or_rm(sock_path, timeout) do
      try do
        with :ok <- sock_send(socket, :erlang.term_to_iovec(message)) do
          sock_recv(socket, timeout)
        end
      after
        sock_shut_and_close(socket)
      end
    end
  end

  def start_link(opts) do
    {gen_opts, opts} = Keyword.split(opts, [:debug, :name, :spawn_opt, :hibernate_after])
    base_path = Keyword.fetch!(opts, :base_path)
    handler = Keyword.fetch!(opts, :handler)
    GenServer.start_link(__MODULE__, {base_path, handler}, gen_opts)
  end

  @impl true
  def init({base_path, handler}) do
    case mkdir(base_path) do
      :ok ->
        case sock_listen_or_retry(base_path) do
          {:ok, socket} ->
            do_init(socket, handler)

          {:error, reason} ->
            Logger.warning(
              "tinysock failed to bind listen socket in #{inspect(base_path)}, reason: #{inspect(reason)}"
            )

            :ignore
        end

      {:error, reason} ->
        Logger.warning(
          "tinysock failed to create directory #{inspect(base_path)}, reason: #{inspect(reason)}"
        )

        :ignore
    end
  end

  defp do_init(socket, handler) do
    Process.flag(:trap_exit, true)
    state = %{socket: socket, handler: handler}
    for _ <- 1..10, do: spawn_acceptor(state)
    {:ok, state}
  end

  @impl true
  def handle_cast(:accepted, state) do
    spawn_acceptor(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    case reason do
      :normal ->
        {:noreply, state}

      :emfile ->
        Logger.error("tinysock ran out of file descriptors, stopping")
        {:stop, reason, state}

      {e, stacktrace} when is_exception(e) and is_list(stacktrace) ->
        error = Exception.format(:error, e, stacktrace)
        Logger.error("tinysock request handler #{inspect(pid)} terminating\n" <> error)
        {:noreply, state}

      reason ->
        Logger.error("tinysock request handler #{inspect(pid)} terminating\n" <> inspect(reason))
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, %{socket: socket}) do
    with {:ok, {:local, path}} <- :inet.sockname(socket), do: File.rm(path)
  end

  defp spawn_acceptor(%{socket: socket, handler: handler}) do
    :proc_lib.spawn_link(__MODULE__, :accept_loop, [_parent = self(), socket, handler])
  end

  @doc false
  def accept_loop(parent, listen_socket, handler) do
    case :gen_tcp.accept(listen_socket, :infinity) do
      {:ok, socket} ->
        GenServer.cast(parent, :accepted)
        handle_message(socket, handler)

      {:error, reason} ->
        exit(reason)
    end
  end

  defp handle_message(socket, handler) do
    {:ok, message} = sock_recv(socket, _timeout = :timer.seconds(5))
    sock_send(socket, :erlang.term_to_iovec(handler.(message)))
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

      {:error, reason} = error ->
        if reason != :timeout do
          # removes stale socket file
          # possible - but unlikely - race condition
          File.rm(sock_path)
        end

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
