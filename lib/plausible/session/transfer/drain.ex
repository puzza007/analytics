defmodule Plausible.Session.Transfer.Drain do
  @moduledoc false

  # Flags this node as draining. It is the last child of the Transfer
  # supervisor, and supervisors stop children in reverse start order, so its
  # terminate/2 runs at the very start of the Transfer shutdown sequence -
  # after the application supervisor has already stopped the endpoint, and
  # before Alive begins holding the node open for replicas. From that point
  # on the `:sessions` cache is final and safe to hand over.
  use GenServer
  require Logger

  @key {__MODULE__, :draining?}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec draining?() :: boolean()
  def draining?, do: :persistent_term.get(@key, false)

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    {:ok, nil}
  end

  @impl true
  def terminate(_reason, nil) do
    # Fail closed: the flag promises "no more traffic", which rests on the
    # application supervisor stopping the endpoint before Transfer. If a
    # child reorder ever breaks that, refusing to flag degrades the handover
    # to "no transfer" (replicas give up) instead of dumping a cache that is
    # still being written to.
    if is_nil(Process.whereis(PlausibleWeb.Endpoint)) do
      :persistent_term.put(@key, true)
    else
      Logger.error(
        "Session transfer: endpoint still running when #{inspect(__MODULE__)} stopped - " <>
          "check the application child order; refusing to serve dumps"
      )
    end
  end
end
