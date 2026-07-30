defmodule Plausible.Session.Transfer do
  @moduledoc """
  Cross-deployment transfer for `:sessions` cache.

  It works by establishing a client-server architecture where:
  - The "replica" one-time task retrieves `:sessions` data from other OS processes via Unix domain sockets
  - The "primary" server process responds to requests for `:sessions` data via
    Unix domain sockets, but declines to be dumped until this node is draining
    (the endpoint has stopped), so that what it hands over is final rather than
    a snapshot of a still-mutating cache; replicas poll until then
  - The "drain" process flags the node as draining at the start of shutdown
  - The "alive" process waits on shutdown for at least one replica, for 15 seconds
  """

  @behaviour Supervisor

  require Logger
  alias Plausible.Session.Transfer.{Alive, Drain, TinySock}
  alias Plausible.{Cache, ClickhouseSessionV2, Session}

  @cmd_list_cache_names :list
  @cmd_dump_cache :get
  @cmd_takeover_done :done
  @not_draining :not_draining

  @drain_poll_interval 250
  @drain_poll_deadline :timer.seconds(60)

  def telemetry_event, do: [:plausible, :sessions, :takeover]

  @doc """
  Starts the `:sessions` transfer supervisor.

  Options:
  - `:name` - the name of the supervisor (default: `Plausible.Session.Transfer`)
  - `:base_path` - the base path for the Unix domain sockets (required)
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    base_path = Keyword.fetch!(opts, :base_path)
    Supervisor.start_link(__MODULE__, base_path, name: name)
  end

  @impl true
  def init(nil) do
    Logger.notice(
      "Session transfer: ignoring, no socket base path configured (make sure ENABLE_SESSION_TRANSFER/PERSISTENT_CACHE_DIR are set)"
    )

    :ignore
  end

  def init(base_path) do
    File.mkdir_p!(base_path)

    replica =
      Supervisor.child_spec(
        {Task, fn -> init_takeover(base_path) end},
        id: :transfer_replica
      )

    given_counter = :counters.new(1, [])

    primary =
      {TinySock,
       base_path: base_path, handler: fn message -> handle_replica(message, given_counter) end}

    alive =
      Supervisor.child_spec(
        {Alive,
         _until = fn ->
           result = :counters.get(given_counter, 1) > 0

           Logger.notice(
             "Session transfer delayed shut down. Checking if session takeover happened?: #{result}"
           )

           result
         end},
        shutdown: :timer.seconds(15)
      )

    Logger.notice("Session transfer init: #{base_path}")

    # Drain is deliberately last: supervisors stop children in reverse start
    # order, so its terminate/2 flags this node as draining before Alive
    # begins its hold - and the application supervisor has already stopped
    # the endpoint by then, so the flag really does mean "no more traffic".
    Supervisor.init([replica, primary, alive, Drain], strategy: :one_for_one)
  end

  @doc """
  Returns `true` if the transfer has been attempted (successfully or not).
  Returns `false` if the transfer is still in progress.

  Read from a flag rather than by introspecting the supervisor: this is
  called from dump handlers while the node is draining, and at that point
  the supervisor is busy terminating children (blocked in Alive's hold), so
  `Supervisor.which_children/1` would hang.
  """
  def attempted? do
    :persistent_term.get({__MODULE__, :attempted?}, false)
  end

  @doc """
  Returns the child specification for the `:sessions` transfer supervisor.
  See `start_link/1` for options.
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :temporary
    }
  end

  defp handle_replica(request, given_counter) do
    response =
      case request do
        {@cmd_list_cache_names, session_version} ->
          cond do
            session_version != session_version() or not attempted?() ->
              []

            # A live primary's cache is a moving target: dumping it hands the
            # replica a stale copy the moment the next event arrives, and the
            # two nodes then cancel the same sessions_v2 rows twice. Decline
            # until this node is draining, at which point the endpoint has
            # stopped and the dump is final. Replicas poll via await_drained/2.
            not Drain.draining?() ->
              @not_draining

            true ->
              Cache.Adapter.get_names(:sessions)
          end

        {@cmd_dump_cache, cache} ->
          Cache.Adapter.cache2list(cache)

        @cmd_takeover_done ->
          :counters.add(given_counter, 1, 1)
      end

    # declines happen every 250ms while a replica polls a live primary;
    # keep them out of the CE default (notice) log level
    level = if response == @not_draining, do: :debug, else: :notice

    Logger.log(
      level,
      "Session transfer message received at #{node()}: #{inspect(request, limit: 10)}"
    )

    response
  end

  defp init_takeover(base_path) do
    started = System.monotonic_time()

    base_path
    |> TinySock.list!()
    |> Enum.sort_by(&file_stat_ctime/1, :asc)
    |> Enum.each(&request_takeover/1)

    :telemetry.execute(telemetry_event(), %{duration: System.monotonic_time() - started})
  after
    # If the replica is killed mid-takeover (rapid redeploy), the flag stays
    # false and this node's partial cache is never offered to the next one.
    :persistent_term.put({__MODULE__, :attempted?}, true)
  end

  defp request_takeover(sock) do
    Logger.notice("Session transfer: requesting takeover at #{node()}")

    with {:ok, names} <- await_drained(sock, @drain_poll_deadline) do
      tasks = Enum.map(names, fn name -> Task.async(fn -> takeover_cache(sock, name) end) end)
      Task.await_many(tasks, :timer.seconds(10))
    end
  after
    Logger.notice("Session transfer: marking takeover as done at #{node()}")
    TinySock.call(sock, @cmd_takeover_done)
  end

  # The primary declines to be dumped until it is draining, so poll until it
  # is. Giving up at the deadline (deploy tooling never stopped the old node)
  # means no transfer - the same outcome as a version mismatch today.
  #
  # Note the interplay with readiness: /api/system reports 500 until
  # attempted?/0 is true, and this poll runs before that. Deploy tooling that
  # refuses to stop the old node until the new one is ready will therefore
  # wait out the full deadline and transfer nothing; the old node must be
  # stopped on a timer or signal, not gated on new-node readiness.
  defp await_drained(sock, deadline_left) do
    case TinySock.call(sock, {@cmd_list_cache_names, session_version()}) do
      {:ok, @not_draining} when deadline_left > 0 ->
        Process.sleep(@drain_poll_interval)
        await_drained(sock, deadline_left - @drain_poll_interval)

      {:ok, @not_draining} ->
        Logger.notice("Session transfer: #{sock} never started draining, giving up")
        {:ok, []}

      other ->
        other
    end
  end

  defp takeover_cache(sock, cache) do
    Logger.notice("Session transfer: requesting cache #{cache} dump at #{node()}")

    with {:ok, records} <- TinySock.call(sock, {@cmd_dump_cache, cache}) do
      Enum.each(records, fn record ->
        {key, %ClickhouseSessionV2{} = session} = record
        # insert_new, not put: a session this node already owns is live and
        # newer - overwriting it would orphan rows this node has already
        # buffered and corrupt the collapsing sign accounting. The dropped
        # import's final +1 row stands as a correctly terminated visit.
        Cache.Adapter.put_new(:sessions, key, session)
      end)

      Logger.notice("Session transfer: restored cache #{cache} at #{node()}")
    end
  end

  defp file_stat_ctime(path) do
    case File.stat(path) do
      {:ok, stat} -> stat.ctime
      {:error, _} -> nil
    end
  end

  defp session_version do
    [
      ClickhouseSessionV2.module_info(:md5),
      Cache.Adapter.module_info(:md5),
      Session.CacheStore.module_info(:md5),
      Session.Transfer.module_info(:md5)
    ]
  end
end
