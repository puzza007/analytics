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

  # tla/SessionTakeover_version_mismatch.cfg
  @tag :tla_repro
  test "the shutdown latch is released even when nothing was transferred" do
    tmp_dir = tmp_dir()

    old = start_another_plausible(tmp_dir)
    process_event(old, build(:event, name: "pageview"))

    [sock] = TinySock.list!(tmp_dir)

    # session_version/0 is the md5 of ClickhouseSessionV2, Cache.Adapter,
    # CacheStore and Transfer, so it differs on ANY deploy that touches one of
    # them, and handle_replica answers with []. The stand-in has to be built
    # from terms that already exist on the peer: TinySock decodes with
    # binary_to_term(_, [:safe]), which refuses to intern new atoms.
    assert {:ok, []} = TinySock.call(sock, {:list, [<<"stale-version">>]})

    # ... yet request_takeover/1 sends :done from an `after` block regardless,
    # so the primary drops its hold having handed over nothing.
    assert {:ok, _} = TinySock.call(sock, :done)

    # Alive should now hold the node open for its full 15s cap. Tearing down a
    # peer that is NOT being held takes about a second, so 5s sits clearly
    # between the two outcomes instead of hugging either.
    {elapsed_us, _} = :timer.tc(fn -> :peer.stop(old) end)
    elapsed_ms = div(elapsed_us, 1_000)

    assert elapsed_ms > 5_000,
           "primary shut down in #{elapsed_ms}ms having transferred nothing: " <>
             "the `after` block released the 15s Alive hold"
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
