----------------------------- MODULE SessionTakeover -----------------------------
(***************************************************************************)
(* WHAT THIS MODELS                                                         *)
(*                                                                          *)
(* On every deploy Plausible hands the node-local `:sessions` ConCache from  *)
(* the outgoing OS process to the incoming one over a Unix domain socket.    *)
(* The outgoing node ("primary") answers `list` / `get` / `done`; the        *)
(* incoming node ("replica") pulls each cache PARTITION and puts the records *)
(* into its own.  A third process ("alive") holds up the primary's shutdown  *)
(* until at least one replica has said `done`, for at most 15 seconds.       *)
(*                                                                          *)
(* Four things make this subtle, and this spec is about all four:            *)
(*                                                                          *)
(*  1. `:sessions` is partitioned 100 ways (runtime.exs:1102) and            *)
(*     `request_takeover/1` fans out ONE `Task` PER PARTITION, then waits    *)
(*     with `Task.await_many(tasks, :timer.seconds(10))`.  The tasks are     *)
(*     unordered and independent, so each partition is snapshotted at its    *)
(*     OWN instant - the replica's view is not a consistent cut of the       *)
(*     primary's.  If the 10s deadline expires the caller exits, leaving     *)
(*     SOME partitions applied and others not - and the `after` block still  *)
(*     reports `done`.  This is the PARTIAL TAKEOVER case.                   *)
(*                                                                          *)
(*  2. The replica is a plain `Task` in the supervision tree, so its         *)
(*     `start_link` returns immediately and the Phoenix endpoint (a LATER    *)
(*     child, application.ex:205) starts while the takeover is still         *)
(*     running.  The new node therefore ACCEPTS TRAFFIC during takeover -    *)
(*     and `takeover_cache/2` then does an unconditional                     *)
(*     `Cache.Adapter.put(:sessions, key, session)` over whatever the new    *)
(*     node has already created.                                            *)
(*                                                                          *)
(*  3. Each dump is `:ets.tab2list` - a SNAPSHOT.  The primary keeps serving *)
(*     traffic after it, so every session it touches between the snapshot    *)
(*     and its own exit is invisible to the replica.                        *)
(*                                                                          *)
(*  4. `request_takeover/1` sends `done` from an `after` block, so the       *)
(*     primary's shutdown hold is released even when the `list` call failed, *)
(*     returned nothing, or the fan-out timed out half-way.                  *)
(*                                                                          *)
(* Correctness is judged with the same ledger as SessionStitch.tla: the      *)
(* CollapsingMergeTree rows the two nodes buffer into `sessions_v2` must     *)
(* collapse to exactly one live row per session, and a visitor who never     *)
(* left must end up as ONE session, not two.                                 *)
(*                                                                          *)
(* REAL CODE:                                                               *)
(*   - lib/plausible/session/transfer.ex:45-79     supervisor, 15s shutdown  *)
(*   - lib/plausible/session/transfer.ex:104-123   handle_replica            *)
(*   - lib/plausible/session/transfer.ex:136-146   fan-out + await_many(10s) *)
(*   - lib/plausible/session/transfer.ex:148-159   takeover_cache, blind put *)
(*   - lib/plausible/session/transfer.ex:168-175   session_version md5s      *)
(*   - lib/plausible/session/transfer/alive.ex:22-33  terminate loop         *)
(*   - lib/plausible/application.ex:63,205         Transfer before Endpoint  *)
(*   - lib/plausible/cache/adapter.ex:161-174      phash2 partition choice   *)
(*   - config/runtime.exs:1102                     sessions: partitions: 100 *)
(*                                                                          *)
(* THE ACTORS                                                               *)
(*   - ingest    - visitor traffic.  NOT fair: a visitor may stop clicking.  *)
(*   - replica   - the one-shot takeover Task that lists then awaits.  Fair. *)
(*   - takeover  - ONE PROCESS PER PARTITION, the `Task.async` fan-out.      *)
(*     Fair, and mutually unordered: that lack of ordering is the point.     *)
(*   - shutdown  - the Alive latch plus the primary's exit.  Fair.           *)
(*   - deadline  - the supervisor's 15s shutdown cap.  Fair: it always fires.*)
(*   - awaitcap  - the 10s `Task.await_many` cap.  NOT fair: it need not     *)
(*     fire, and the behaviours where it does not are the healthy ones.      *)
(*                                                                          *)
(* ABSTRACTION CHOICES                                                      *)
(*   - ONE VISITOR PER PARTITION.  `Cache.Adapter.get_name/2` shards on      *)
(*     `phash2(key, 100)`, so a visitor lives in exactly one partition and   *)
(*     partitions are independent.  Modelling one visitor each is the        *)
(*     minimal way to expose CROSS-partition effects (partial takeover);     *)
(*     more visitors per partition only replay SessionStitch.tla's races.    *)
(*   - `Partitions` is a parameter.  Set it to a singleton to recover the    *)
(*     single-partition model exactly; the `_1part` configs do that, and     *)
(*     reproduce the earlier results.                                        *)
(*   - `TinySock.call` is atomic and never fails mid-way.  Socket errors     *)
(*     land in the `gotNames = FALSE` branch, which is modelled.             *)
(*                                                                          *)
(* OUT OF SCOPE, deliberately                                               *)
(*   - Three-or-more-node deploy chains.  The `attempted?(parent)` guard     *)
(*     (transfer.ex:111) exists for those; it is the OldAttempted constant,  *)
(*     which covers both of its values without the extra nodes.              *)
(*   - Stale socket files and `sock_connect_or_rm`: another way to reach     *)
(*     `gotNames = FALSE`, already covered.                                  *)
(*   - The 30-minute staleness check and ConCache TTL: both only DISCARD     *)
(*     sessions, so they can suppress a violation, never cause one.          *)
(*   - WriteBuffer batching.  Order-preserving and lossless, so the ledger   *)
(*     is what ClickHouse eventually sees.                                   *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Partitions,  \* :sessions cache partitions (production: 100)
          MaxEvents,   \* visitor events across the whole deploy window
          NULL

(***************************************************************************)
(* Behaviour knobs, each naming the real code it switches.                  *)
(***************************************************************************)
CONSTANTS
    \* transfer.ex:111 - `session_version == session_version()`.  The version is
    \* the md5 of four modules, so this is FALSE on any deploy that changes
    \* ClickhouseSessionV2, Cache.Adapter, Session.CacheStore or Transfer.
    VersionMatches,
    \* transfer.ex:111 - `attempted?(parent)`: the primary refuses to dump
    \* until its OWN takeover task has finished.
    OldAttempted,
    \* TRUE is the code as written: `done` comes from an `after` block
    \* (transfer.ex:143-145) whatever happened.  FALSE is the candidate fix.
    DoneAlways,
    \* TRUE is the code as written: the replica Task returns from start_link
    \* immediately, so the endpoint boots and the new node serves traffic while
    \* the takeover is still in flight.  FALSE gates readiness on completion.
    NewServesDuringTakeover,
    \* Candidate fix: the primary stops accepting traffic BEFORE it is dumped,
    \* so every snapshot is final.  Nothing in the code does this today.
    DrainBeforeTakeover,
    \* TRUE lets `Task.await_many(tasks, 10s)` (transfer.ex:141) expire, so the
    \* fan-out is abandoned with some partitions applied and others not.
    AwaitCapFires

ASSUME MaxEvents \in Nat /\ MaxEvents >= 1

Nodes == {"old", "new"}
Sids  == 1..MaxEvents   \* at most one new session per event

(* --algorithm SessionTakeover

variables
    \* Each node's :sessions entry, per partition (one visitor per partition).
    sess = [n \in Nodes |-> [p \in Partitions |-> NULL]],
    \* Whether each node accepts ingest.
    serving = [n \in Nodes |-> IF n = "old" THEN TRUE ELSE NewServesDuringTakeover],
    \* Append-only ledger of rows buffered towards sessions_v2 by BOTH nodes.
    rows = <<>>,
    \* GHOST: events genuinely folded into each session.
    folded = [s \in Sids |-> 0],
    \* GHOST: which partition each session belongs to.
    sidPart = [s \in Sids |-> NULL],
    nextSid = 1,
    \* The primary's `given_counter` (transfer.ex:54, 121).
    given = 0,
    \* Per-partition: has this partition's Task applied its dump?
    pulled = [p \in Partitions |-> FALSE],
    \* RList has answered, so the fan-out may start.
    listed = FALSE,
    gotNames = FALSE,
    \* Task.await_many/2 gave up (transfer.ex:141).
    awaitExpired = FALSE,
    \* The supervisor's 15s shutdown cap (transfer.ex:74).
    deadlineFired = FALSE,
    \* GHOST: events that arrived while NO node was accepting them.
    dropped = 0,
    eventsLeft = MaxEvents;

\* Visitor traffic.  Unfair - a visitor may simply stop.
\* One iteration is one atomic session mutation, which is what the node-local
\* Session.Balancer guarantees WITHIN a node.  It guarantees nothing across
\* nodes, which is part of what this spec tests.
process ingest = "ingest"
begin
  Ing:
    while eventsLeft > 0 do
        eventsLeft := eventsLeft - 1;
        either
            \* The `with` is disabled when no node serves, so this branch
            \* cannot fire during a drain window.
            with n \in {m \in Nodes : serving[m]}, p \in Partitions do
                if sess[n][p] = NULL then
                    rows    := Append(rows, [sid |-> nextSid, sign |-> 1, ver |-> 1]);
                    folded  := [folded EXCEPT ![nextSid] = @ + 1];
                    sidPart := [sidPart EXCEPT ![nextSid] = p];
                    sess    := [sess EXCEPT ![n][p] = [sid |-> nextSid, ver |-> 1]];
                    nextSid := nextSid + 1;
                else
                    rows := Append(Append(rows,
                              [sid |-> sess[n][p].sid, sign |-> -1, ver |-> sess[n][p].ver]),
                              [sid |-> sess[n][p].sid, sign |->  1, ver |-> sess[n][p].ver + 1]);
                    folded := [folded EXCEPT ![sess[n][p].sid] = @ + 1];
                    sess   := [sess EXCEPT ![n][p] =
                                 [sid |-> sess[n][p].sid, ver |-> sess[n][p].ver + 1]];
                end if;
            end with;
        or
            await \A m \in Nodes : ~serving[m];
            dropped := dropped + 1;
        end either;
    end while;
end process;

\* The one-shot takeover Task (transfer.ex:136-146).
fair process replica = "replica"
begin
  \* TinySock.call(sock, {:list, session_version()}).  handle_replica answers []
  \* unless the version matches AND the primary's own takeover has finished.
  RList:
    gotNames := VersionMatches /\ OldAttempted;
    listed   := TRUE;

  \* Task.await_many(tasks, :timer.seconds(10)) - wait for every partition's
  \* Task, or give up part-way through.
  RAwait:
    await \/ ~gotNames
          \/ awaitExpired
          \/ \A p \in Partitions : pulled[p];

  \* The `after` block: reached on every path, including a failed list and an
  \* expired fan-out.  transfer.ex:143-145
  RDone:
    if DoneAlways \/ (\E p \in Partitions : pulled[p]) then
        given := given + 1;
    end if;

  RServe:
    serving := [serving EXCEPT !["new"] = TRUE];
end process;

\* ONE PER PARTITION - the `Task.async` fan-out at transfer.ex:140.  Independent
\* and unordered, so each partition snapshots the primary at its own instant.
fair process takeover \in Partitions
begin
  TPull:
    await listed;
    \* With DrainBeforeTakeover the dump waits until the primary has stopped
    \* accepting traffic, so :ets.tab2list returns a final state.
    await ~DrainBeforeTakeover \/ pc["shutdown"] # "SDrain";
    if gotNames /\ ~awaitExpired then
        \* Cache.Adapter.put is unconditional: it clobbers whatever the new node
        \* already holds for this key.  transfer.ex:151-157
        sess   := [sess EXCEPT !["new"][self] = sess["old"][self]];
        pulled := [pulled EXCEPT ![self] = TRUE];
    end if;
end process;

\* Alive.terminate loops until given > 0 (alive.ex:22-33); the supervisor kills
\* it after 15s regardless (transfer.ex:74).  Fair - the node is going away.
fair process shutdown = "shutdown"
begin
  SDrain:
    if DrainBeforeTakeover then
        serving := [serving EXCEPT !["old"] = FALSE];
    end if;
  SWait:
    await given > 0 \/ deadlineFired;
  SStop:
    serving := [serving EXCEPT !["old"] = FALSE];
end process;

\* The 15s shutdown cap.  Fair, because it always eventually fires.
fair process deadline = "deadline"
begin
  DFire:
    deadlineFired := TRUE;
end process;

\* The 10s Task.await_many cap.  NOT fair: a healthy deploy never hits it.
process awaitcap = "awaitcap"
begin
  ACFire:
    if AwaitCapFires then
        either awaitExpired := TRUE;
        or     skip;
        end either;
    end if;
end process;

end algorithm; *)

\* Translate with:
\*   java -cp tla2tools.jar pcal.trans SessionTakeover.tla

\* BEGIN TRANSLATION
VARIABLES sess, serving, rows, folded, sidPart, nextSid, given, pulled,
          listed, gotNames, awaitExpired, deadlineFired, dropped, eventsLeft,
          pc

vars == << sess, serving, rows, folded, sidPart, nextSid, given, pulled,
           listed, gotNames, awaitExpired, deadlineFired, dropped, eventsLeft,
           pc >>

ProcSet == {"ingest"} \cup {"replica"} \cup (Partitions) \cup {"shutdown"} \cup {"deadline"} \cup {"awaitcap"}

Init == (* Global variables *)
        /\ sess = [n \in Nodes |-> [p \in Partitions |-> NULL]]
        /\ serving = [n \in Nodes |-> IF n = "old" THEN TRUE ELSE NewServesDuringTakeover]
        /\ rows = <<>>
        /\ folded = [s \in Sids |-> 0]
        /\ sidPart = [s \in Sids |-> NULL]
        /\ nextSid = 1
        /\ given = 0
        /\ pulled = [p \in Partitions |-> FALSE]
        /\ listed = FALSE
        /\ gotNames = FALSE
        /\ awaitExpired = FALSE
        /\ deadlineFired = FALSE
        /\ dropped = 0
        /\ eventsLeft = MaxEvents
        /\ pc = [self \in ProcSet |-> CASE self = "ingest" -> "Ing"
                                        [] self = "replica" -> "RList"
                                        [] self \in Partitions -> "TPull"
                                        [] self = "shutdown" -> "SDrain"
                                        [] self = "deadline" -> "DFire"
                                        [] self = "awaitcap" -> "ACFire"]

Ing == /\ pc["ingest"] = "Ing"
       /\ IF eventsLeft > 0
             THEN /\ eventsLeft' = eventsLeft - 1
                  /\ \/ /\ \E n \in {m \in Nodes : serving[m]}:
                             \E p \in Partitions:
                               IF sess[n][p] = NULL
                                  THEN /\ rows' = Append(rows, [sid |-> nextSid, sign |-> 1, ver |-> 1])
                                       /\ folded' = [folded EXCEPT ![nextSid] = @ + 1]
                                       /\ sidPart' = [sidPart EXCEPT ![nextSid] = p]
                                       /\ sess' = [sess EXCEPT ![n][p] = [sid |-> nextSid, ver |-> 1]]
                                       /\ nextSid' = nextSid + 1
                                  ELSE /\ rows' = Append(Append(rows,
                                                    [sid |-> sess[n][p].sid, sign |-> -1, ver |-> sess[n][p].ver]),
                                                    [sid |-> sess[n][p].sid, sign |->  1, ver |-> sess[n][p].ver + 1])
                                       /\ folded' = [folded EXCEPT ![sess[n][p].sid] = @ + 1]
                                       /\ sess' = [sess EXCEPT ![n][p] =
                                                     [sid |-> sess[n][p].sid, ver |-> sess[n][p].ver + 1]]
                                       /\ UNCHANGED << sidPart, nextSid >>
                        /\ UNCHANGED dropped
                     \/ /\ \A m \in Nodes : ~serving[m]
                        /\ dropped' = dropped + 1
                        /\ UNCHANGED <<sess, rows, folded, sidPart, nextSid>>
                  /\ pc' = [pc EXCEPT !["ingest"] = "Ing"]
             ELSE /\ pc' = [pc EXCEPT !["ingest"] = "Done"]
                  /\ UNCHANGED << sess, rows, folded, sidPart, nextSid,
                                  dropped, eventsLeft >>
       /\ UNCHANGED << serving, given, pulled, listed, gotNames, awaitExpired,
                       deadlineFired >>

ingest == Ing

RList == /\ pc["replica"] = "RList"
         /\ gotNames' = (VersionMatches /\ OldAttempted)
         /\ listed' = TRUE
         /\ pc' = [pc EXCEPT !["replica"] = "RAwait"]
         /\ UNCHANGED << sess, serving, rows, folded, sidPart, nextSid, given,
                         pulled, awaitExpired, deadlineFired, dropped,
                         eventsLeft >>

RAwait == /\ pc["replica"] = "RAwait"
          /\ \/ ~gotNames
             \/ awaitExpired
             \/ \A p \in Partitions : pulled[p]
          /\ pc' = [pc EXCEPT !["replica"] = "RDone"]
          /\ UNCHANGED << sess, serving, rows, folded, sidPart, nextSid, given,
                          pulled, listed, gotNames, awaitExpired,
                          deadlineFired, dropped, eventsLeft >>

RDone == /\ pc["replica"] = "RDone"
         /\ IF DoneAlways \/ (\E p \in Partitions : pulled[p])
               THEN /\ given' = given + 1
               ELSE /\ TRUE
                    /\ given' = given
         /\ pc' = [pc EXCEPT !["replica"] = "RServe"]
         /\ UNCHANGED << sess, serving, rows, folded, sidPart, nextSid, pulled,
                         listed, gotNames, awaitExpired, deadlineFired,
                         dropped, eventsLeft >>

RServe == /\ pc["replica"] = "RServe"
          /\ serving' = [serving EXCEPT !["new"] = TRUE]
          /\ pc' = [pc EXCEPT !["replica"] = "Done"]
          /\ UNCHANGED << sess, rows, folded, sidPart, nextSid, given, pulled,
                          listed, gotNames, awaitExpired, deadlineFired,
                          dropped, eventsLeft >>

replica == RList \/ RAwait \/ RDone \/ RServe

TPull(self) == /\ pc[self] = "TPull"
               /\ listed
               /\ ~DrainBeforeTakeover \/ pc["shutdown"] # "SDrain"
               /\ IF gotNames /\ ~awaitExpired
                     THEN /\ sess' = [sess EXCEPT !["new"][self] = sess["old"][self]]
                          /\ pulled' = [pulled EXCEPT ![self] = TRUE]
                     ELSE /\ TRUE
                          /\ UNCHANGED << sess, pulled >>
               /\ pc' = [pc EXCEPT ![self] = "Done"]
               /\ UNCHANGED << serving, rows, folded, sidPart, nextSid, given,
                               listed, gotNames, awaitExpired, deadlineFired,
                               dropped, eventsLeft >>

takeover(self) == TPull(self)

SDrain == /\ pc["shutdown"] = "SDrain"
          /\ IF DrainBeforeTakeover
                THEN /\ serving' = [serving EXCEPT !["old"] = FALSE]
                ELSE /\ TRUE
                     /\ UNCHANGED serving
          /\ pc' = [pc EXCEPT !["shutdown"] = "SWait"]
          /\ UNCHANGED << sess, rows, folded, sidPart, nextSid, given, pulled,
                          listed, gotNames, awaitExpired, deadlineFired,
                          dropped, eventsLeft >>

SWait == /\ pc["shutdown"] = "SWait"
         /\ given > 0 \/ deadlineFired
         /\ pc' = [pc EXCEPT !["shutdown"] = "SStop"]
         /\ UNCHANGED << sess, serving, rows, folded, sidPart, nextSid, given,
                         pulled, listed, gotNames, awaitExpired, deadlineFired,
                         dropped, eventsLeft >>

SStop == /\ pc["shutdown"] = "SStop"
         /\ serving' = [serving EXCEPT !["old"] = FALSE]
         /\ pc' = [pc EXCEPT !["shutdown"] = "Done"]
         /\ UNCHANGED << sess, rows, folded, sidPart, nextSid, given, pulled,
                         listed, gotNames, awaitExpired, deadlineFired,
                         dropped, eventsLeft >>

shutdown == SDrain \/ SWait \/ SStop

DFire == /\ pc["deadline"] = "DFire"
         /\ deadlineFired' = TRUE
         /\ pc' = [pc EXCEPT !["deadline"] = "Done"]
         /\ UNCHANGED << sess, serving, rows, folded, sidPart, nextSid, given,
                         pulled, listed, gotNames, awaitExpired, dropped,
                         eventsLeft >>

deadline == DFire

ACFire == /\ pc["awaitcap"] = "ACFire"
          /\ IF AwaitCapFires
                THEN /\ \/ /\ awaitExpired' = TRUE
                        \/ /\ TRUE
                           /\ UNCHANGED awaitExpired
                ELSE /\ TRUE
                     /\ UNCHANGED awaitExpired
          /\ pc' = [pc EXCEPT !["awaitcap"] = "Done"]
          /\ UNCHANGED << sess, serving, rows, folded, sidPart, nextSid, given,
                          pulled, listed, gotNames, deadlineFired, dropped,
                          eventsLeft >>

awaitcap == ACFire

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == ingest \/ replica \/ shutdown \/ deadline \/ awaitcap
           \/ (\E self \in Partitions: takeover(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(replica)
        /\ \A self \in Partitions : WF_vars(takeover(self))
        /\ WF_vars(shutdown)
        /\ WF_vars(deadline)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

(***************************************************************************)
(* Ledger arithmetic - the aggregates the dashboard computes.               *)
(***************************************************************************)
SignedVisits(s) ==
    LET f[i \in 0..Len(rows)] ==
          IF i = 0 THEN 0
          ELSE f[i-1] + (IF rows[i].sid = s THEN rows[i].sign ELSE 0)
    IN f[Len(rows)]

SignedEvents(s) ==
    LET f[i \in 0..Len(rows)] ==
          IF i = 0 THEN 0
          ELSE f[i-1] + (IF rows[i].sid = s THEN rows[i].sign * rows[i].ver ELSE 0)
    IN f[Len(rows)]

LiveSids     == {s \in Sids : folded[s] > 0}
SidsIn(p)    == {s \in LiveSids : sidPart[s] = p}
TakeoverOver == pc["replica"] = "Done"

(***************************************************************************)
(* Safety.                                                                  *)
(***************************************************************************)

TypeOK ==
    /\ given \in 0..1
    /\ nextSid \in 1..(MaxEvents + 1)
    /\ eventsLeft \in 0..MaxEvents
    /\ pulled \in [Partitions -> BOOLEAN]

\* THE OBSERVABLE SYMPTOM.  A visitor who never left must not become two
\* visits.  Per partition, because one visitor lives in one partition.
NoSessionSplit == \A p \in Partitions : Cardinality(SidsIn(p)) <= 1

\* THE UNDERLYING INVARIANT.  Every state row is cancelled at most once, so
\* sum(sign * col) is exact.
NoDoubleCancel ==
    \A i \in 1..Len(rows) :
        rows[i].sign = -1 =>
            Cardinality({j \in 1..i :
                /\ rows[j].sid  = rows[i].sid
                /\ rows[j].ver  = rows[i].ver
                /\ rows[j].sign = -1}) = 1

EventsCorrect == \A s \in LiveSids : SignedEvents(s) = folded[s]

\* No session may be mutable on two serving nodes at once - a cross-node
\* repeat of the SessionStitch double cancel, with no balancer able to
\* serialise it.
NoConcurrentOwnership ==
    \A p \in Partitions :
        ~( /\ serving["old"] /\ serving["new"]
           /\ sess["old"][p] # NULL /\ sess["new"][p] # NULL
           /\ sess["old"][p].sid = sess["new"][p].sid )

\* Nothing in flight is dropped on the floor.  Qualified on the replica having
\* FINISHED: between the primary draining and a dump landing there is a healthy
\* window a bare state predicate would misread as loss.
NoLostSession ==
    \A p \in Partitions :
        (TakeoverOver /\ ~serving["old"] /\ sess["old"][p] # NULL) =>
            (sess["new"][p] # NULL /\ sess["new"][p].sid = sess["old"][p].sid)

\* THE LATCH MUST MEAN SOMETHING.  The primary delays shutdown for up to 15s
\* specifically to wait for a handover.
LatchMeansTransfer == given > 0 => (\E p \in Partitions : pulled[p])

\* THE SHARPER VERSION, only expressible with the fan-out modelled: the latch
\* should mean the handover COMPLETED, not that it started.  Task.await_many's
\* 10s cap can abandon the fan-out mid-flight, and `done` fires anyway.
LatchMeansCompleteTransfer ==
    given > 0 => (\A p \in Partitions : pulled[p])

\* A takeover must be all-or-nothing.  Partial takeover means some visitors'
\* sessions survive the deploy and others silently do not.
NoPartialTakeover ==
    TakeoverOver =>
        \/ \A p \in Partitions : pulled[p]
        \/ \A p \in Partitions : ~pulled[p]

\* THE COST OF THE OBVIOUS FIX.  Draining the primary before the replica is
\* ready removes the corruption by throwing traffic away instead.
NoTrafficGap == dropped = 0

(***************************************************************************)
(* Liveness.  A "fix" that never lets the new node serve would satisfy every  *)
(* safety property above and take the site down.                             *)
(***************************************************************************)
NewNodeEventuallyServes == <>[](serving["new"])

(***************************************************************************)
(* WITNESSES - assert the negation of a believed-reachable state, so TLC's    *)
(* counterexample proves the model reaches it.  Never on a trusted config.    *)
(***************************************************************************)
\* "the fan-out can finish some partitions and not others"
WitnessPartialFanOut ==
    ~(\E p, q \in Partitions : pulled[p] /\ ~pulled[q])

\* "both nodes can serve the same visitor at once"
WitnessBothServing ==
    ~(\E p \in Partitions :
        serving["old"] /\ serving["new"]
        /\ sess["old"][p] # NULL /\ sess["new"][p] # NULL)

VizView == << pc, sess, serving, rows, given, pulled >>

===============================================================================
