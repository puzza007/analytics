defmodule Plausible.Session.SessionAccountingRaceTest do
  # Reproduces races found by model checking tla/SessionStitch.tla. Both tests
  # fail by design until the races are closed, so :tla_repro is in
  # default_exclude (test_helper.exs); run them with `mix test --only tla_repro`.
  #
  # CacheStoreTest."event processing is sequential within session" documents the
  # corrupt row pattern these produce, and proves the Balancer prevents it for a
  # FIXED user_id. These cover the case it does not.
  use Plausible.DataCase

  alias Plausible.Session.{BalancerSupervisor, CacheStore}

  @moduletag :tla_repro

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

  # tla/SessionStitch_rotation.cfg
  test "a salt rotation lets two balancer workers cancel the same session" do
    test_pid = self()

    # handle_event calls buffer_insert after find_session but before
    # update_session_cache, so parking here holds both writers at the point
    # where each has read the session and neither has re-keyed it.
    barrier_buffer = fn sessions ->
      send(test_pid, {:at_barrier, self(), sessions})

      receive do
        :release -> {:ok, sessions}
      after
        5_000 -> raise "barrier never released"
      end
    end

    plain_buffer = fn sessions ->
      send(test_pid, {:session_rows, sessions})
      {:ok, sessions}
    end

    event1 = build(:event, name: "pageview")

    # After RotateSalts fires, user_id is derived from the new salt while
    # previous_user_id keeps the old one. An in-flight request still carries the
    # old user_id, so the two events hash to different Balancer workers even
    # though find_session/2 resolves both to the same cached session.
    rotated_user_id = user_id_on_another_worker(event1.user_id)

    event2 = build(:event, name: "pageview", user_id: event1.user_id, site_id: event1.site_id)
    event3 = build(:event, name: "pageview", user_id: rotated_user_id, site_id: event1.site_id)

    CacheStore.on_event(event1, @session_params, nil, buffer_insert: plain_buffer)
    assert_receive {:session_rows, [session1]}

    tasks = [
      Task.async(fn ->
        CacheStore.on_event(event2, @session_params, nil, buffer_insert: barrier_buffer)
      end),
      Task.async(fn ->
        # prev_user_id is what makes event3 stitch onto the same session
        CacheStore.on_event(event3, @session_params, event1.user_id,
          buffer_insert: barrier_buffer
        )
      end)
    ]

    assert_receive {:at_barrier, writer_a, rows_a}, 5_000

    # Under the race, the second writer reaches the barrier while the first is
    # still parked - holding both makes the corrupting interleaving
    # deterministic. Under a serialising fix it cannot get there (its dispatch
    # queues behind the parked worker), so after a grace period the writers are
    # released to run in turn and the test passes on the invariants below
    # instead of deadlocking on an interleaving the fix has made impossible.
    # (A machine slow enough to delay the second writer past the grace period
    # skips the race and may pass vacuously - acceptable for an opt-in repro.)
    rows_b =
      receive do
        {:at_barrier, writer_b, rows_b} ->
          send(writer_a, :release)
          send(writer_b, :release)
          rows_b
      after
        2_000 ->
          send(writer_a, :release)
          assert_receive {:at_barrier, writer_b, rows_b}, 5_000
          send(writer_b, :release)
          rows_b
      end

    Task.await_many(tasks)

    all_rows = [session1] ++ rows_a ++ rows_b

    # sum(is_bounce * sign) is bounce_rate's numerator. Both writers cancel the
    # same is_bounce=true row, so it goes negative and the greatest(..., 0)
    # clamp at the call sites becomes load-bearing.
    assert signed_bounce(all_rows) >= 0,
           "sum(is_bounce * sign) = #{signed_bounce(all_rows)}: the session state was cancelled twice"

    # Every event must survive into the collapsed state. Summed across sessions
    # rather than asserted on one, so that a fix which declines to stitch across
    # the rotation - legitimately splitting the visitor - still passes: 2 + 1
    # events over two sessions is fine; 2 over one session is a writer absorbing
    # the other's update.
    assert live_events(all_rows) == 3,
           "#{live_events(all_rows)} of 3 events survive in the collapsed sessions: " <>
             "concurrent writers derived from the same base state and absorbed an update"
  end

  # tla/SessionStitch_timeout.cfg
  test "a lock timeout leaves sessions_v2 and events_v2 disagreeing" do
    test_pid = self()

    event1 = build(:event, name: "pageview")
    event2 = build(:event, name: "pageview", user_id: event1.user_id, site_id: event1.site_id)

    # Holds the worker past CacheStore's @lock_timeout of 1_000ms, then lets go.
    # The hold is time-bounded rather than test-released because the worker must
    # go on to run the second event's closure after its caller has given up.
    blocking_buffer = fn sessions ->
      send(test_pid, :worker_held)
      Process.sleep(1_500)
      {:ok, sessions}
    end

    blocker =
      Task.async(fn ->
        CacheStore.on_event(event1, @session_params, nil, buffer_insert: blocking_buffer)
      end)

    assert_receive :worker_held, 5_000

    ingest_event = %Plausible.Ingestion.Event{
      clickhouse_event: event2,
      clickhouse_session_attrs: @session_params
    }

    # {:error, :lock_timeout} is the code as written: cache_store.ex:42-46
    # catches the caller-side GenServer.call timeout (@lock_timeout is 1_000ms)
    # and embedded.ex:31 turns it into a dropped event. {:ok, _} is what a
    # caller-waits fix returns instead. Both are consistent shapes; anything
    # else is neither the bug nor a fix.
    result =
      Plausible.Ingestion.Persistor.Embedded.persist_event(ingest_event, nil,
        session_write_buffer_insert: fn s ->
          send(test_pid, {:session_rows, s})
          {:ok, s}
        end,
        event_write_buffer_insert: fn e ->
          send(test_pid, {:event_row, e})
          {:ok, e}
        end
      )

    assert match?({:error, :lock_timeout}, result) or match?({:ok, _}, result),
           "unexpected persist_event result: #{inspect(result)}"

    Task.await(blocker, :timer.seconds(10))

    # Asserted as an equivalence so that any consistent outcome passes - the
    # balancer skipping the closure once the caller has gone, or the caller
    # waiting for it - and only the torn state fails.
    session_advanced? =
      receive do
        {:session_rows, [_cancel, %{events: 2}]} -> true
      after
        1_000 -> false
      end

    event_row_written? =
      receive do
        {:event_row, _} -> true
      after
        100 -> false
      end

    assert session_advanced? == event_row_written?,
           "sessions_v2 advanced: #{session_advanced?}, events_v2 written: #{event_row_written?}"
  end

  defp signed_bounce(rows) do
    Enum.sum_by(rows, fn row -> row.sign * if(row.is_bounce, do: 1, else: 0) end)
  end

  # Events surviving after the CollapsingMergeTree collapse: for each session,
  # the highest events value among its state (sign: 1) rows.
  defp live_events(rows) do
    rows
    |> Enum.filter(&(&1.sign == 1))
    |> Enum.group_by(& &1.session_id)
    |> Enum.map(fn {_sid, state_rows} -> state_rows |> Enum.map(& &1.events) |> Enum.max() end)
    |> Enum.sum()
  end

  # Mirrors Balancer.dispatch/3's sharding. The postcondition is asserted so a
  # change to the routing strategy fails loudly rather than silently picking a
  # user_id on the same worker, which would reproduce nothing.
  defp user_id_on_another_worker(user_id) do
    size = BalancerSupervisor.size()
    worker = :erlang.phash2(user_id, size)

    other = Enum.find((user_id + 1)..(user_id + 1_000), &(:erlang.phash2(&1, size) != worker))

    assert other, "no user_id within 1000 of #{user_id} lands on a different balancer worker"
    other
  end
end
