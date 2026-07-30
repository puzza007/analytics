------------------------------ MODULE SessionStitch ------------------------------
(***************************************************************************)
(* WHAT THIS MODELS                                                         *)
(*                                                                          *)
(* Plausible attributes every ingest event to a visitor session held in a    *)
(* node-local ConCache (`:sessions`), and mirrors each session mutation into *)
(* ClickHouse `sessions_v2` as a CollapsingMergeTree cancel/state PAIR:      *)
(* a row with sign = -1 carrying the OLD state, then sign = +1 carrying the  *)
(* NEW state.  All dashboard numbers are then `sum(sign)` and               *)
(* `sum(sign * pageviews)` over those rows.  The accounting is only correct  *)
(* if every state row is cancelled AT MOST ONCE.                            *)
(*                                                                          *)
(* Concurrent events for one visitor are supposed to be serialised by        *)
(* `Session.Balancer`, which picks a worker with `phash2(user_id, N)`.       *)
(* The subtlety this spec is about: the balancer serialises on ONE key       *)
(* (`event.user_id`) but `find_session/2` reads TWO (`user_id` then          *)
(* `prev_user_id`).  `user_id` is `SipHash(current_salt, ip <> ua <> domain)`,*)
(* so it CHANGES when the salt rotates - and the two events either side of a *)
(* rotation therefore hash to DIFFERENT balancer workers while still         *)
(* resolving to the SAME cached session.  Nothing serialises them.          *)
(*                                                                          *)
(* Second question in the same spec: `CacheStore.on_event` catches the       *)
(* caller-side `GenServer.call` timeout, but the balancer has no idea the    *)
(* caller left and runs the closure anyway - mutating the session and        *)
(* buffering its rows - while `Persistor.Embedded` maps the timeout to       *)
(* `{:error, :lock_timeout}` and skips the events_v2 insert.                 *)
(*                                                                          *)
(* REAL CODE:                                                               *)
(*   - lib/plausible/session/cache_store.ex:16-47   dispatch + timeout catch *)
(*   - lib/plausible/session/cache_store.ex:60-70   handle_event, the pair   *)
(*   - lib/plausible/session/cache_store.ex:74-118   find_session, cache put  *)
(*   - lib/plausible/session/balancer.ex:14-25      phash2(user_id) worker   *)
(*   - lib/plausible/ingestion/event.ex:396-408     put_salts / put_user_id  *)
(*   - lib/plausible/ingestion/event.ex:427-438     previous_user_id         *)
(*   - lib/plausible/ingestion/persistor/embedded.ex:28-38  event row skip   *)
(*   - lib/plausible/session/salts.ex:50-72         rotate/2                 *)
(*   - lib/plausible/stats/sql/expression.ex:452-464 sum(sign), sum(sign*pv) *)
(*                                                                          *)
(* THE ACTORS                                                               *)
(*   - req \in Requests - concurrent ingest requests for ONE visitor on ONE  *)
(*     site.  Fair: each is a live HTTP request that will run to completion. *)
(*   - salt - the daily RotateSalts job.  NOT fair: it need not rotate.      *)
(*                                                                          *)
(* ABSTRACTION CHOICES                                                      *)
(*   - A cache KEY is modelled as a salt epoch, because key = {site_id,      *)
(*     user_id} and user_id is a pure function of the salt for a fixed       *)
(*     visitor.  Epoch 0 is the sentinel "no previous salt" key, always      *)
(*     empty and never locked.                                              *)
(*   - Balancer workers are 1:1 with keys.  phash2 collisions would only     *)
(*     ADD serialisation, so this is the pessimistic-for-the-code direction. *)
(*   - Sessions are (sid, ver) tokens rather than bare keys.  `ver` is the   *)
(*     `events` column, which is what the cancel row must match.  Modelling  *)
(*     identity rather than keys is what makes a stale cache entry visible.  *)
(*   - `rows` is an append-only ledger of every row buffered towards         *)
(*     sessions_v2.  Batching and flush timing in WriteBuffer are omitted:   *)
(*     the buffer preserves order and never drops, so the ledger is exactly  *)
(*     what ClickHouse eventually sees.                                      *)
(*                                                                          *)
(* OUT OF SCOPE, deliberately                                               *)
(*   - The 30-minute staleness check in find_session (cache_store.ex:88).    *)
(*     Every interleaving here spans milliseconds, so the check always       *)
(*     passes; omitting it cannot manufacture a session match.               *)
(*   - Engagement events (cache_store.ex:49-58).  They refresh the cache but *)
(*     buffer no rows, so they cannot affect sign accounting.                *)
(*   - ConCache TTL eviction.  Eviction only REMOVES stale entries, i.e. it  *)
(*     can only suppress the violation, never cause one.                     *)
(*   - Multi-node ingest.  Caches are node-local, so two nodes produce two   *)
(*     DISTINCT sessions - a visit-splitting problem, not a sign-accounting  *)
(*     one.  Modelled separately in SessionTakeover.tla.                     *)
(*   - phash2 collisions, WriteBuffer flush failures, ClickHouse merge       *)
(*     scheduling.  VersionedCollapsingMergeTree(sign, events) does not      *)
(*     change any `sum(sign * col)` result, only when rows are physically    *)
(*     removed, so aggregate correctness is decided entirely by the ledger.  *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Requests,    \* concurrent ingest requests for one visitor
          MaxEpoch,    \* salt epochs reachable; 2 = exactly one rotation
          NULL

(***************************************************************************)
(* Behaviour knobs, each naming the real code it switches.                  *)
(***************************************************************************)
CONSTANTS
    \* TRUE lets RotateSalts fire mid-flight, so requests can read different
    \* salt epochs and hash to different balancer workers.  FALSE pins every
    \* request to one epoch, which is the "balancer serialises everything"
    \* baseline that MUST pass - otherwise the model is broken, not the code.
    SaltRotates,
    \* TRUE models cache_store.ex:42-46: the caller abandons the GenServer.call
    \* while the balancer runs the closure regardless, so the session advances
    \* but embedded.ex:31 skips the events_v2 insert.
    CallerTimeout,
    \* Candidate fix 1: dispatch on BOTH user_id and prev_user_id, so every
    \* key find_session/2 may read is serialised.  Acquired atomically here;
    \* a real implementation would need an ordering to avoid deadlock.
    FixLockBothKeys,
    \* Candidate fix 2: once a session has been re-keyed onto the new user_id,
    \* drop the entry under the old one so it cannot be stitched twice.
    FixRekeyCoherent,
    (***********************************************************************)
    (* The three CHEAPER candidates.  FixLockBothKeys doubles lock          *)
    (* acquisition on the hottest path in the system - every ingest event - *)
    (* to close a race whose window opens once a day, and it needs a lock   *)
    (* ORDERING that this spec does not model (locks are taken atomically   *)
    (* here).  These are the alternatives that avoid that cost.             *)
    (***********************************************************************)
    \* Take the second worker ONLY when the current key misses, so the extra
    \* lock is off the common path (an established session hits key 1).
    \* Modelled optimistically: the peek at cache[myUid] and the lock
    \* acquisition happen in ONE atomic step, where real code would have a gap
    \* between check and lock.  If this still violates, it is definitely bad.
    FixLockOnMissOnly,
    \* Route the balancer on a value that does NOT change when the salt
    \* rotates, so one visitor always lands on one worker.  Same single lock,
    \* no extra cost.  Key 0 stands in for that stable shard - it is never
    \* used as a cache key.
    FixSaltIndependentRouting,
    \* Do not stitch across the rotation at all: if the session was only
    \* findable under prev_user_id, start a NEW one instead of updating it.
    \* Trades a double cancel for a session split, at zero steady-state cost.
    FixNoCrossRotationStitch

ASSUME MaxEpoch \in Nat /\ MaxEpoch >= 1

Keys  == 0..MaxEpoch                 \* 0 = "no previous salt" sentinel
Sids  == 1..Cardinality(Requests)    \* at most one new session per request

(* --algorithm SessionStitch

variables
    \* :sessions ConCache.  key -> [sid, ver] or NULL.
    cache = [k \in Keys |-> NULL],
    \* Session.Balancer worker locks, one per key (see abstraction note).
    lock = [k \in Keys |-> NULL],
    \* Append-only ledger of rows buffered towards sessions_v2.
    rows = <<>>,
    \* Rows buffered towards events_v2 (embedded.ex:37).
    eventRows = 0,
    \* GHOST: how many events were genuinely folded into each session.
    \* This is what sessions_v2 must report as `events` for that session.
    folded = [s \in Sids |-> 0],
    nextSid = 1,
    epoch = 1;

\* The daily RotateSalts job.  Unfair: a behaviour in which it never fires is
\* legitimate, and is exactly the SaltRotates = FALSE baseline.
process salt = "salt"
begin
  Rotate:
    while SaltRotates /\ epoch < MaxEpoch do
        epoch := epoch + 1;
    end while;
end process;

\* One in-flight ingest request.  Fair - an accepted HTTP request runs to the
\* end of the pipeline.
fair process req \in Requests
variables myUid = 0, myPrev = 0, myRoute = 0, myFound = NULL, abandoned = FALSE;
begin
  \* put_salts/2 then put_user_id/2 (event.ex:396-408).  Everything after this
  \* - geolocation, UA parse with its 200ms timeout - runs before the session
  \* is touched, which is what makes the straddle window wide enough to hit.
  ReadSalts:
    myUid  := epoch;
    myPrev := epoch - 1;
    \* Balancer.dispatch keys on user_id today, which is why the worker
    \* changes when the salt does.
    myRoute := IF FixSaltIndependentRouting THEN 0 ELSE epoch;
    if CallerTimeout then
        either abandoned := TRUE;
        or     skip;
        end either;
    end if;

  \* Balancer.dispatch: GenServer.call to worker phash2(user_id, N)+1.
  Acquire:
    await /\ lock[myRoute] = NULL
          /\ ( \/ myPrev = 0
               \/ FixNoCrossRotationStitch
               \/ ~(FixLockBothKeys \/ (FixLockOnMissOnly /\ cache[myUid] = NULL))
               \/ lock[myPrev] = NULL );
    lock := [k \in Keys |->
               IF k = myRoute THEN self
               ELSE IF /\ myPrev # 0
                       /\ ~FixNoCrossRotationStitch
                       /\ (FixLockBothKeys \/ (FixLockOnMissOnly /\ cache[myUid] = NULL))
                       /\ k = myPrev
                    THEN self
               ELSE lock[k]];

  \* find_session(event, user_id) || find_session(event, prev_user_id)
  Lookup:
    if cache[myUid] # NULL then
        myFound := cache[myUid];
    elsif ~FixNoCrossRotationStitch /\ cache[myPrev] # NULL then
        myFound := cache[myPrev];
    else
        myFound := NULL;
    end if;

  \* handle_event/4: buffer_insert of the cancel/state pair, then
  \* update_session_cache under the CURRENT user_id key.
  \* `ver` stands for pageviews/events - INCREMENTED from the base state.
  \* `bnc` stands for is_bounce - a LATCH, true until a second pageview lands
  \* (cache_store.ex:140-144), then false forever.  The two behave differently
  \* under a double cancel, which is why both are modelled.
  Write:
    if myFound = NULL then
        rows      := Append(rows,
                       [sid |-> nextSid, sign |-> 1, ver |-> 1, bnc |-> 1]);
        folded    := [folded EXCEPT ![nextSid] = @ + 1];
        cache     := [k \in Keys |->
                        IF k = myUid THEN [sid |-> nextSid, ver |-> 1, bnc |-> 1]
                        ELSE cache[k]];
        nextSid   := nextSid + 1;
    else
        rows   := Append(Append(rows,
                    [sid  |-> myFound.sid, sign |-> -1,
                     ver  |-> myFound.ver, bnc  |-> myFound.bnc]),
                    [sid  |-> myFound.sid, sign |->  1,
                     ver  |-> myFound.ver + 1, bnc |-> 0]);
        folded := [folded EXCEPT ![myFound.sid] = @ + 1];
        cache  := [k \in Keys |->
                     IF k = myUid
                       THEN [sid |-> myFound.sid, ver |-> myFound.ver + 1, bnc |-> 0]
                     ELSE IF FixRekeyCoherent /\ myPrev # 0 /\ k = myPrev
                       THEN NULL
                     ELSE cache[k]];
    end if;

  Release:
    lock := [k \in Keys |-> IF lock[k] = self THEN NULL ELSE lock[k]];

  \* embedded.ex:28-38 - the events_v2 row is written ONLY if the balancer
  \* call returned in time.  The session mutation above happened regardless.
  EventRow:
    if ~abandoned then
        eventRows := eventRows + 1;
    end if;
end process;

end algorithm; *)

\* Translate with:
\*   java -cp tla2tools.jar pcal.trans SessionStitch.tla

\* BEGIN TRANSLATION
VARIABLES cache, lock, rows, eventRows, folded, nextSid, epoch, pc, myUid,
          myPrev, myRoute, myFound, abandoned

vars == << cache, lock, rows, eventRows, folded, nextSid, epoch, pc, myUid,
           myPrev, myRoute, myFound, abandoned >>

ProcSet == {"salt"} \cup (Requests)

Init == (* Global variables *)
        /\ cache = [k \in Keys |-> NULL]
        /\ lock = [k \in Keys |-> NULL]
        /\ rows = <<>>
        /\ eventRows = 0
        /\ folded = [s \in Sids |-> 0]
        /\ nextSid = 1
        /\ epoch = 1
        (* Process req *)
        /\ myUid = [self \in Requests |-> 0]
        /\ myPrev = [self \in Requests |-> 0]
        /\ myRoute = [self \in Requests |-> 0]
        /\ myFound = [self \in Requests |-> NULL]
        /\ abandoned = [self \in Requests |-> FALSE]
        /\ pc = [self \in ProcSet |-> CASE self = "salt" -> "Rotate"
                                        [] self \in Requests -> "ReadSalts"]

Rotate == /\ pc["salt"] = "Rotate"
          /\ IF SaltRotates /\ epoch < MaxEpoch
                THEN /\ epoch' = epoch + 1
                     /\ pc' = [pc EXCEPT !["salt"] = "Rotate"]
                ELSE /\ pc' = [pc EXCEPT !["salt"] = "Done"]
                     /\ epoch' = epoch
          /\ UNCHANGED << cache, lock, rows, eventRows, folded, nextSid, myUid,
                          myPrev, myRoute, myFound, abandoned >>

salt == Rotate

ReadSalts(self) == /\ pc[self] = "ReadSalts"
                   /\ myUid' = [myUid EXCEPT ![self] = epoch]
                   /\ myPrev' = [myPrev EXCEPT ![self] = epoch - 1]
                   /\ myRoute' = [myRoute EXCEPT ![self] = IF FixSaltIndependentRouting THEN 0 ELSE epoch]
                   /\ IF CallerTimeout
                         THEN /\ \/ /\ abandoned' = [abandoned EXCEPT ![self] = TRUE]
                                 \/ /\ TRUE
                                    /\ UNCHANGED abandoned
                         ELSE /\ TRUE
                              /\ UNCHANGED abandoned
                   /\ pc' = [pc EXCEPT ![self] = "Acquire"]
                   /\ UNCHANGED << cache, lock, rows, eventRows, folded,
                                   nextSid, epoch, myFound >>

Acquire(self) == /\ pc[self] = "Acquire"
                 /\ /\ lock[myRoute[self]] = NULL
                    /\ ( \/ myPrev[self] = 0
                         \/ FixNoCrossRotationStitch
                         \/ ~(FixLockBothKeys \/ (FixLockOnMissOnly /\ cache[myUid[self]] = NULL))
                         \/ lock[myPrev[self]] = NULL )
                 /\ lock' = [k \in Keys |->
                               IF k = myRoute[self] THEN self
                               ELSE IF /\ myPrev[self] # 0
                                       /\ ~FixNoCrossRotationStitch
                                       /\ (FixLockBothKeys \/ (FixLockOnMissOnly /\ cache[myUid[self]] = NULL))
                                       /\ k = myPrev[self]
                                    THEN self
                               ELSE lock[k]]
                 /\ pc' = [pc EXCEPT ![self] = "Lookup"]
                 /\ UNCHANGED << cache, rows, eventRows, folded, nextSid,
                                 epoch, myUid, myPrev, myRoute, myFound,
                                 abandoned >>

Lookup(self) == /\ pc[self] = "Lookup"
                /\ IF cache[myUid[self]] # NULL
                      THEN /\ myFound' = [myFound EXCEPT ![self] = cache[myUid[self]]]
                      ELSE /\ IF ~FixNoCrossRotationStitch /\ cache[myPrev[self]] # NULL
                                 THEN /\ myFound' = [myFound EXCEPT ![self] = cache[myPrev[self]]]
                                 ELSE /\ myFound' = [myFound EXCEPT ![self] = NULL]
                /\ pc' = [pc EXCEPT ![self] = "Write"]
                /\ UNCHANGED << cache, lock, rows, eventRows, folded, nextSid,
                                epoch, myUid, myPrev, myRoute, abandoned >>

Write(self) == /\ pc[self] = "Write"
               /\ IF myFound[self] = NULL
                     THEN /\ rows' = Append(rows,
                                       [sid |-> nextSid, sign |-> 1, ver |-> 1, bnc |-> 1])
                          /\ folded' = [folded EXCEPT ![nextSid] = @ + 1]
                          /\ cache' = [k \in Keys |->
                                         IF k = myUid[self] THEN [sid |-> nextSid, ver |-> 1, bnc |-> 1]
                                         ELSE cache[k]]
                          /\ nextSid' = nextSid + 1
                     ELSE /\ rows' = Append(Append(rows,
                                       [sid  |-> myFound[self].sid, sign |-> -1,
                                        ver  |-> myFound[self].ver, bnc  |-> myFound[self].bnc]),
                                       [sid  |-> myFound[self].sid, sign |->  1,
                                        ver  |-> myFound[self].ver + 1, bnc |-> 0])
                          /\ folded' = [folded EXCEPT ![myFound[self].sid] = @ + 1]
                          /\ cache' = [k \in Keys |->
                                         IF k = myUid[self]
                                           THEN [sid |-> myFound[self].sid, ver |-> myFound[self].ver + 1, bnc |-> 0]
                                         ELSE IF FixRekeyCoherent /\ myPrev[self] # 0 /\ k = myPrev[self]
                                           THEN NULL
                                         ELSE cache[k]]
                          /\ UNCHANGED nextSid
               /\ pc' = [pc EXCEPT ![self] = "Release"]
               /\ UNCHANGED << lock, eventRows, epoch, myUid, myPrev, myRoute,
                               myFound, abandoned >>

Release(self) == /\ pc[self] = "Release"
                 /\ lock' = [k \in Keys |-> IF lock[k] = self THEN NULL ELSE lock[k]]
                 /\ pc' = [pc EXCEPT ![self] = "EventRow"]
                 /\ UNCHANGED << cache, rows, eventRows, folded, nextSid,
                                 epoch, myUid, myPrev, myRoute, myFound,
                                 abandoned >>

EventRow(self) == /\ pc[self] = "EventRow"
                  /\ IF ~abandoned[self]
                        THEN /\ eventRows' = eventRows + 1
                        ELSE /\ TRUE
                             /\ UNCHANGED eventRows
                  /\ pc' = [pc EXCEPT ![self] = "Done"]
                  /\ UNCHANGED << cache, lock, rows, folded, nextSid, epoch,
                                  myUid, myPrev, myRoute, myFound, abandoned >>

req(self) == ReadSalts(self) \/ Acquire(self) \/ Lookup(self)
                \/ Write(self) \/ Release(self) \/ EventRow(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == salt
           \/ (\E self \in Requests: req(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Requests : WF_vars(req(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

(***************************************************************************)
(* Ledger arithmetic - exactly the aggregates the dashboard computes.       *)
(*   visits     = greatest(sum(sign), 0)            expression.ex:452       *)
(*   pageviews  = greatest(sum(sign * pageviews),0) expression.ex:458       *)
(* `ver` stands in for the pageviews/events column.                         *)
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

\* sum(is_bounce * sign) - expression.ex:430, fragments.ex:43.  The numerator
\* of bounce_rate.  Both call sites wrap it in greatest(..., 0), and carry a
\* `:TRICKY:` comment saying pre-#4493 data could make this go negative.
SignedBounce(s) ==
    LET f[i \in 0..Len(rows)] ==
          IF i = 0 THEN 0
          ELSE f[i-1] + (IF rows[i].sid = s THEN rows[i].sign * rows[i].bnc ELSE 0)
    IN f[Len(rows)]

\* Highest `events` value any state row claims for this session.  With no lost
\* update this is the true event count.
MaxStateVer(s) ==
    LET f[i \in 0..Len(rows)] ==
          IF i = 0 THEN 0
          ELSE IF rows[i].sid = s /\ rows[i].sign = 1 /\ rows[i].ver > f[i-1]
                 THEN rows[i].ver
          ELSE f[i-1]
    IN f[Len(rows)]

LiveSids   == {s \in Sids : folded[s] > 0}
TotalFolded ==
    LET g[i \in 0..Cardinality(Sids)] ==
          IF i = 0 THEN 0 ELSE g[i-1] + folded[i]
    IN g[Cardinality(Sids)]

AllDone == \A r \in Requests : pc[r] = "Done"

(***************************************************************************)
(* Safety.                                                                  *)
(***************************************************************************)

TypeOK ==
    /\ eventRows \in 0..Cardinality(Requests)
    /\ nextSid \in 1..(Cardinality(Requests) + 1)
    /\ epoch \in 1..MaxEpoch
    /\ folded \in [Sids -> 0..Cardinality(Requests)]

\* THE OBSERVABLE SYMPTOM.  Every session the visitor had should contribute
\* exactly one live row, so `visits` counts one visit per session.
VisitsCorrect == \A s \in LiveSids : SignedVisits(s) = 1

\* THE UNDERLYING INVARIANT.  The collapsed pageview/event total for a session
\* must equal the number of events actually folded into it.  This is the one
\* that catches a double cancel: an extra -1 row leaves the +1 rows uncancelled
\* and inflates sum(sign * ver).
EventsCorrect == \A s \in LiveSids : SignedEvents(s) = folded[s]

\* WHY the above holds when it holds: a state row is cancelled at most once.
\* Fails informatively - it points at the exact (sid, ver) that was cancelled
\* twice, which is the stale cache entry two balancer workers both read.
NoDoubleCancel ==
    \A i \in 1..Len(rows) :
        rows[i].sign = -1 =>
            Cardinality({j \in 1..i :
                /\ rows[j].sid  = rows[i].sid
                /\ rows[j].ver  = rows[i].ver
                /\ rows[j].sign = -1}) = 1

\* THE SYMPTOM THAT SURVIVES THE CLAMPS.  bounce_rate's numerator must equal
\* the session's true is_bounce.  Unlike `events`, is_bounce is a LATCH, so a
\* lost update does not offset the extra cancel row - the error is net.
BounceCorrect ==
    \A s \in LiveSids :
        SignedBounce(s) = (IF folded[s] >= 2 THEN 0 ELSE 1)

\* The specific corruption PR #4493 clamped with greatest(..., 0) instead of
\* preventing.  If this fails, the clamp is load-bearing.
NoNegativeBounce == \A s \in LiveSids : SignedBounce(s) >= 0

\* LOST UPDATE.  Two workers that both read the same base state both write
\* base+1, so one event is silently absorbed and the live session under-counts.
NoLostUpdate == \A s \in LiveSids : MaxStateVer(s) = folded[s]

\* sessions_v2 and events_v2 must agree: every event folded into a session
\* must also exist as an event row.  Only meaningful once all requests are
\* done, since the two writes are not simultaneous.
EventRowsMatch == AllDone => (eventRows = TotalFolded)

\* A visitor who never left should be ONE session, not several.
NoSessionSplit == AllDone => Cardinality(LiveSids) <= 1

(***************************************************************************)
(* Liveness.  A fix that deadlocks the balancer would satisfy every safety   *)
(* property above by never writing anything; this tells the two apart.       *)
(***************************************************************************)
EveryRequestCompletes == <>[](\A r \in Requests : pc[r] = "Done")

(***************************************************************************)
(* WITNESSES.  Assert the negation of a state believed reachable; TLC's      *)
(* counterexample is the proof it is reached.  Used to show the model has    *)
(* teeth before trusting any green run.  Never enable on a trusted config.   *)
(***************************************************************************)
\* "two requests can be inside the balancer at the same time"
WitnessConcurrentInBalancer ==
    ~(\E a, b \in Requests :
        /\ a # b
        /\ pc[a] \in {"Lookup", "Write"}
        /\ pc[b] \in {"Lookup", "Write"})

\* "two requests can resolve to the same session from different keys"
WitnessStaleKeyRead ==
    ~(\E a, b \in Requests :
        /\ a # b
        /\ myUid[a] # myUid[b]
        /\ myFound[a] # NULL /\ myFound[b] # NULL
        /\ myFound[a].sid = myFound[b].sid)

(***************************************************************************)
(* Readable traces.                                                         *)
(***************************************************************************)
Alias ==
    [ pcs       |-> pc,
      epoch     |-> epoch,
      cache     |-> [k \in Keys |->
                       IF cache[k] = NULL THEN "-"
                       ELSE <<cache[k].sid, cache[k].ver>>],
      lock      |-> [k \in Keys |-> IF lock[k] = NULL THEN "-" ELSE lock[k]],
      rows      |-> [i \in 1..Len(rows) |->
                       <<rows[i].sid, rows[i].sign, rows[i].ver>>],
      folded    |-> folded,
      eventRows |-> eventRows,
      signedEv  |-> [s \in LiveSids |-> SignedEvents(s)],
      signedVis |-> [s \in LiveSids |-> SignedVisits(s)] ]

VizView == << pc, cache, rows, folded, epoch >>

===============================================================================
