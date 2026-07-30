defmodule Plausible.Session.TransferTest do
  use ExUnit.Case
  import Plausible.Factory
  import Plausible.TestUtils, only: [tmp_dir: 0]

  alias Plausible.Session.Transfer.TinySock

  @tag :slow
  test "it works" do
    tmp_dir = tmp_dir()

    old = start_another_plausible(tmp_dir)
    await_transfer(old)

    Enum.each(1..250, fn _ -> process_event(old, build(:event, name: "pageview")) end)

    new = start_another_plausible(tmp_dir)
    await_transfer(new)

    assert all_sessions_sorted(new) == all_sessions_sorted(old)
  end

  # The two tests below reproduce races found by model checking
  # tla/SessionTakeover.tla. They fail by design until the races are closed, so
  # :tla_repro is in default_exclude (test_helper.exs); run them with
  # `mix test --only tla_repro`.
  #
  # "it works" above passes because it calls await_transfer(new) before doing
  # anything else, so it only ever observes a handover that has already
  # finished. These probe the window it skips.

  # tla/SessionTakeover_fanout_as_written.cfg. No :slow tag: CI runs with --include
  # slow, and an ExUnit include beats an exclude.
  @tag :tla_repro
  test "a session dumped for takeover is a snapshot the primary keeps mutating past" do
    tmp_dir = tmp_dir()

    old = start_another_plausible(tmp_dir)
    await_transfer(old)

    event = build(:event, name: "pageview")
    process_event(old, event)

    [sock] = TinySock.list!(tmp_dir)

    snapshot = dump_via_socket(sock)
    assert snapshot != [], "nothing was dumped; the rest of this test would be vacuous"

    # The primary keeps serving after being dumped: the endpoint is not stopped,
    # and Alive holds the node open for up to 15 more seconds.
    process_event(old, event)

    # A replica holding `snapshot` now has a session the primary has already
    # superseded. Its next event for that visitor cancels a version the primary
    # has already cancelled, giving sessions_v2 two sign=-1 rows for one sign=+1.
    assert dump_via_socket(sock) == snapshot,
           "the primary mutated a session after dumping it; the replica's copy is stale"
  end

  # tla/SessionTakeover_version_mismatch.cfg — the model's invariant is
  # primary-side: LatchMeansTransfer == given > 0 => dumped. This test asserts
  # the primary's policy, not the replica's manners: both legs speak the socket
  # protocol exactly the way today's replica does — request_takeover/1 sends
  # :done from an `after` block whether or not anything was handed over
  # (transfer.ex:143-145) — and the primary should treat the two differently.
  # If the fix lands replica-side instead (send :done only on success), this
  # test asserts nothing about it and should be updated in tandem.
  #
  # Framed as a differential on purpose. Releasing the hold QUICKLY on a
  # declined transfer may well be desirable — holding a node 15s for a
  # transfer that can never succeed is pure deploy latency, and maintainers
  # treat version churn as routine (see PR #5338). The defect reproduced here
  # is narrower: the primary cannot DISTINGUISH an empty handover from a
  # complete one, which is also why it releases when a started fan-out is
  # abandoned half-way (tla/SessionTakeover_fanout_await_expires.cfg).
  #
  # Shutdown is triggered with init:stop on the peer and timed to the node's
  # actual exit, deliberately: peer's default shutdown is {halt, 5000}, under
  # which peer:stop/1 makes the node erlang:halt(), skipping every
  # supervision-tree terminate callback — including Transfer.Alive.terminate/2,
  # which IS the hold. Timing a halted node measures nothing.
  @tag :tla_repro
  @tag timeout: :timer.minutes(3)
  test "a handover that transfers nothing keeps the hold that a complete one releases" do
    # Positive control: when no :done arrives at all, graceful shutdown must
    # visibly include Alive's 15s budget. This proves the measurement can see
    # the hold — and doubles as the expected post-fix shape of the empty leg.
    held_ms = graceful_stop_after_handover(:no_done)

    assert held_ms >= 10_000,
           "the 15s Alive hold was not observable via graceful shutdown " <>
             "(node died in #{held_ms}ms): the measurement is broken, or the " <>
             "primary's own boot-time replica raced to its socket and self-released"

    complete_ms = graceful_stop_after_handover(:complete)
    nothing_ms = graceful_stop_after_handover(:nothing_transferred)

    assert nothing_ms >= complete_ms + 5_000,
           "the primary released its shutdown hold after an empty handover " <>
             "(#{nothing_ms}ms) as readily as after a complete one (#{complete_ms}ms): " <>
             ":done carries no information about whether anything was transferred"
  end

  # :sessions is partitioned 100 ways (runtime.exs), so every partition has to
  # be dumped - reading only the first compares two empty lists and passes for
  # the wrong reason. Goes over the socket rather than :peer.call so that it
  # exercises the {:get, cache} snapshot semantics under test.
  defp dump_via_socket(sock) do
    :sessions
    |> Plausible.Cache.Adapter.get_names()
    |> Enum.flat_map(fn cache_name ->
      {:ok, records} = TinySock.call(sock, {:get, cache_name})
      records
    end)
    |> Enum.sort_by(fn {key, _} -> key end)
  end

  # One handover leg: boot a primary, let a hand-rolled replica either pull
  # everything (matching version, every partition) or get declined (stale
  # version), close with the :done today's replica always sends, then trigger
  # init:stop and time the node's graceful death. Returns elapsed milliseconds.
  defp graceful_stop_after_handover(handover) do
    tmp_dir = tmp_dir()
    old = start_another_plausible(tmp_dir)
    await_transfer(old)
    process_event(old, build(:event, name: "pageview"))

    [sock] = TinySock.list!(tmp_dir)

    case handover do
      :complete ->
        # a faithful replica: matching version, every partition pulled
        {:ok, names} = TinySock.call(sock, {:list, session_version()})
        assert names != [], "primary declined a matching-version handover"
        Enum.each(names, fn name -> {:ok, _} = TinySock.call(sock, {:get, name}) end)

      :nothing_transferred ->
        # transfer.ex:111 — a version mismatch declines the handover. The
        # stand-in version must be built from terms that already exist on the
        # peer: TinySock decodes with binary_to_term(_, [:safe]), which
        # refuses to intern new atoms.
        assert {:ok, []} = TinySock.call(sock, {:list, [<<"stale-version">>]})

      :no_done ->
        :ok
    end

    # outside the control leg, both replicas end identically: the :done that
    # request_takeover/1's after block always sends
    if handover != :no_done do
      {:ok, _} = TinySock.call(sock, :done)
    end

    # init:stop runs the full graceful shutdown — applications stop in reverse
    # order, the Transfer supervisor gives Alive its 15s budget, and
    # Alive.terminate/2 blocks until the latch is released or the budget runs
    # out. The peer's origin process exits when the node does; unlink first so
    # its (abnormal) exit reason cannot take the test down with it.
    ref = Process.monitor(old)
    Process.unlink(old)
    :ok = :peer.call(old, :init, :stop, [])

    {elapsed_us, _} =
      :timer.tc(fn ->
        assert_receive {:DOWN, ^ref, :process, _, _}, :timer.seconds(60)
      end)

    div(elapsed_us, 1_000)
  end

  # Mirrors Plausible.Session.Transfer.session_version/0 (transfer.ex:168-175),
  # which is private. Valid because this test node loads the very beams the
  # peer adds to its code path, so the md5s agree by construction.
  defp session_version do
    [
      Plausible.ClickhouseSessionV2.module_info(:md5),
      Plausible.Cache.Adapter.module_info(:md5),
      Plausible.Session.CacheStore.module_info(:md5),
      Plausible.Session.Transfer.module_info(:md5)
    ]
  end

  defp start_another_plausible(tmp_dir) do
    {:ok, pid, _node} = :peer.start_link(%{connection: {{127, 0, 0, 1}, 0}})
    add_code_paths(pid)
    transfer_configuration(pid)
    :ok = :peer.call(pid, Application, :put_env, [:plausible, :session_transfer_dir, tmp_dir])
    ensure_applications_started(pid)
    pid
  end

  defp add_code_paths(pid) do
    :ok = :peer.call(pid, :code, :add_paths, [:code.get_path()])
  end

  defp transfer_configuration(pid) do
    for {app_name, _, _} <- Application.loaded_applications() do
      for {key, val} <- Application.get_all_env(app_name) do
        :ok = :peer.call(pid, Application, :put_env, [app_name, key, val])
      end
    end
  end

  defp ensure_applications_started(pid) do
    {:ok, _apps} = :peer.call(pid, Application, :ensure_all_started, [:mix])
    :ok = :peer.call(pid, Mix, :env, [Mix.env()])

    for {app_name, _, _} <- Application.loaded_applications(), app_name != :dialyxir do
      {:ok, _apps} = :peer.call(pid, Application, :ensure_all_started, [app_name])
    end
  end

  @session_params %{
    referrer: "ref",
    referrer_source: "refsource",
    utm_medium: "medium",
    utm_source: "source",
    utm_campaign: "campaign",
    utm_content: "content",
    utm_term: "term",
    browser: "browser",
    browser_version: "55",
    country_code: "EE",
    screen_size: "Desktop",
    operating_system: "Mac",
    operating_system_version: "11"
  }

  defp process_event(pid, event) do
    :peer.call(pid, Plausible.Session.CacheStore, :on_event, [
      event,
      @session_params,
      _prev_user_id = nil,
      [buffer_insert: &Function.identity/1]
    ])
  end

  defp all_sessions_sorted(pid) do
    cache_names = :peer.call(pid, Plausible.Cache.Adapter, :get_names, [:sessions])

    records =
      Enum.flat_map(cache_names, fn cache_name ->
        tab = :peer.call(pid, ConCache, :ets, [cache_name])
        :peer.call(pid, :ets, :tab2list, [tab])
      end)

    Enum.sort_by(records, fn {key, _} -> key end)
  end

  defp await_transfer(pid, timeout \\ :timer.seconds(1)) do
    test = self()

    spawn_link(fn ->
      await_loop(fn -> :peer.call(pid, Plausible.Session.Transfer, :attempted?, []) end)
      send(test, :took)
    end)

    assert_receive :took, timeout
  end

  defp await_loop(f) do
    :timer.sleep(100)

    case f.() do
      true -> :done
      false -> await_loop(f)
    end
  end
end
