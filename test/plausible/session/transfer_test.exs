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
    expected = all_sessions_sorted(old)

    new = start_another_plausible(tmp_dir)

    # the new node's replica polls until the old node starts draining
    begin_graceful_stop(old)
    await_transfer(new, :timer.seconds(15))

    assert all_sessions_sorted(new) == expected
  end

  # Verifies the fix modelled in tla/SessionTakeover_fix.cfg (this test was
  # previously the red repro for tla/SessionTakeover_fanout_as_written.cfg,
  # where the dump was an ets.tab2list snapshot of a still-serving primary).
  @tag :slow
  test "a primary declines dumps until it drains, and the dump is then final" do
    tmp_dir = tmp_dir()

    old = start_another_plausible(tmp_dir)
    await_transfer(old)

    process_event(old, build(:event, name: "pageview"))

    [sock] = TinySock.list!(tmp_dir)

    # a live primary refuses: its cache is still a moving target
    assert {:ok, :not_draining} = TinySock.call(sock, {:list, session_version()})

    expected = all_sessions_sorted(old)

    # once shutdown begins the endpoint is stopped, the cache is final, and
    # Alive holds the node open for the handover
    ref = begin_graceful_stop(old)
    assert await_names(sock) != []

    assert dump_via_socket(sock) == expected,
           "the dump differs from the primary's state at drain: it was not final"

    {:ok, _} = TinySock.call(sock, :done)
    assert_receive {:DOWN, ^ref, :process, _, _}, :timer.seconds(60)
  end

  # The test below reproduces a race found by model checking
  # tla/SessionTakeover.tla. It fails by design until the race is closed, so
  # :tla_repro is in default_exclude (test_helper.exs); run it with
  # `mix test --only tla_repro`.

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
  # Each leg begins the shutdown first: dumps are only served once the primary
  # drains. Timing uses init:stop and the node's monitored exit, deliberately:
  # peer's default {halt, 5000} shutdown skips every supervision-tree
  # terminate callback — including Transfer.Alive.terminate/2, which IS the
  # hold. Timing a halted node measures nothing.
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

  # One handover leg: boot a primary, begin its graceful shutdown, then let a
  # hand-rolled replica either pull everything during the drain window
  # (matching version, every partition) or get declined (stale version), close
  # with the :done today's replica always sends, and time the node's graceful
  # death from init:stop. Returns elapsed milliseconds.
  defp graceful_stop_after_handover(handover) do
    tmp_dir = tmp_dir()
    old = start_another_plausible(tmp_dir)
    await_transfer(old)
    process_event(old, build(:event, name: "pageview"))

    [sock] = TinySock.list!(tmp_dir)

    # init:stop runs the full graceful shutdown — applications stop in reverse
    # order, the Transfer supervisor gives Alive its 15s budget, and
    # Alive.terminate/2 blocks until the latch is released or the budget runs
    # out. Dumps are served during that window.
    ref = begin_graceful_stop(old)
    started = System.monotonic_time(:millisecond)

    case handover do
      :complete ->
        # a faithful replica: matching version, every partition pulled
        names = await_names(sock)
        Enum.each(names, fn name -> {:ok, _} = TinySock.call(sock, {:get, name}) end)

      :nothing_transferred ->
        # a version mismatch declines the handover outright, drained or not.
        # The stand-in version must be built from terms that already exist on
        # the peer: TinySock decodes with binary_to_term(_, [:safe]), which
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

    assert_receive {:DOWN, ^ref, :process, _, _}, :timer.seconds(60)
    System.monotonic_time(:millisecond) - started
  end

  # The peer's origin process exits when the node does; unlink first so its
  # (abnormal) exit reason cannot take the test down with it.
  defp begin_graceful_stop(peer) do
    ref = Process.monitor(peer)
    Process.unlink(peer)
    :ok = :peer.call(peer, :init, :stop, [])
    ref
  end

  # A live primary answers :not_draining; poll like the real replica does.
  # Returns the cache names or flunks - a deadline must not slip through the
  # callers' pattern matches as a puzzling downstream error.
  defp await_names(sock, deadline_ms \\ :timer.seconds(10)) do
    case TinySock.call(sock, {:list, session_version()}) do
      {:ok, :not_draining} when deadline_ms > 0 ->
        Process.sleep(100)
        await_names(sock, deadline_ms - 100)

      {:ok, [_ | _] = names} ->
        names

      other ->
        flunk("draining primary did not hand over its cache names, got: #{inspect(other)}")
    end
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
