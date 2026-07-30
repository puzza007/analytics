# TLA+ specs for the session pipeline

Two protocols in the ingest path are hard to reason about by reading the code, because their
correctness argument bottoms out in "that interleaving cannot happen". These specs make that
argument explicit and check it.

| Spec | Protocol |
|---|---|
| `SessionStitch.tla` | session stitching, `Session.Balancer` serialisation, CollapsingMergeTree sign accounting |
| `SessionTakeover.tla` | cross-deployment `:sessions` cache handover, including the 100-partition fan-out |

Both judge correctness with the same ledger: `sessions_v2` is a
`VersionedCollapsingMergeTree(sign, events)`, so every session mutation must emit exactly one
`sign: -1` row cancelling the previous state. Cancel a state row twice and `sum(is_bounce * sign)`
goes negative and `sum(duration * sign)` inflates — the corruption described in the comment on
`CacheStoreTest."event processing is sequential within session"`.

## Running

Get [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases) (not vendored here), then:

```bash
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -config tla/SessionStitch_rotation.cfg -workers 4 tla/SessionStitch.tla
```

Use `-workers 1` for a reproducible trace. Re-translate after editing the PlusCal block — never
hand-edit the generated TLA+ between `BEGIN TRANSLATION` and `END TRANSLATION`:

```bash
java -cp tla2tools.jar pcal.trans tla/SessionStitch.tla && rm -f tla/SessionStitch.old
```

## Configs

One config per question. Each carries a `QUESTION` / `EXPECTED` header. Behaviour is switched by
boolean CONSTANTS rather than by editing the spec, so the configs read as an experiment log.

TLC halts on the first violation, so any invariant that is *expected* to fail lives in its own
config — otherwise it masks everything after it.

### `SessionStitch`

| Config | Asks |
|---|---|
| `baseline` | does the Balancer serialise everything when `user_id` is fixed? (**must pass** — the sanity floor) |
| `rotation` | the code as written, with a salt rotation mid-flight |
| `timeout` | the balancer caller times out while the closure runs anyway |
| `fix_lockonly` / `fix_rekeyonly` | does either half of the first candidate fix work alone? |
| `fix_both` / `fix_both_split` | both halves together, and the session split it leaves behind |
| `alt_missonly` / `alt_routing` / `alt_routing_rekey` / `alt_nostitch` | cheaper candidates that avoid doubling lock acquisition on the hot path |
| `liveness` | does the fix deadlock the balancer? |

### `SessionTakeover`

| Config | Asks |
|---|---|
| `1part_drainfirst` / `1part_as_written` | single partition — reproduces the pre-fan-out results, so the partitioned spec is a faithful extension |
| `fanout_as_written` | the real shape: one `Task` per partition, code as written |
| `fanout_await_expires` | `Task.await_many(tasks, 10s)` expires part-way through the fan-out |
| `fanout_drainfirst` | does drain-before-dump still hold with the real fan-out? (**passes**) |
| `version_mismatch` | any deploy touching one of the four md5-hashed modules |
| `fix_done_only_on_success` | sending `:done` only on success — honest, but does it recover the sessions? |
| `drainfirst_cost` | the traffic gap drain-first buys the fix with |
| `liveness` | can the new node fail to ever become ready? |

## Repro tests

The races these found are reproduced as ExUnit tests tagged `:tla_repro`. They fail by design until
the races are closed, so the tag is in `default_exclude` (`test/test_helper.exs`) and stays out of CI
even on runs that opt into `:slow`:

```bash
mix test --only tla_repro
```

Each test names the config it came from.

## Caveats

A spec proves things about the *protocol*, never that the code matches the spec. Every abstraction
and deliberate omission is listed in the header comment of each module — read those before trusting
a green run.
