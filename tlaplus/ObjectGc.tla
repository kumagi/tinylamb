------------------------------ MODULE ObjectGc ------------------------------
(***************************************************************************)
(* Data objects (TLCS segments, LSM runs, block extents) versus garbage    *)
(* collection (docs/primary-s3.md, section 13).                            *)
(*                                                                         *)
(* A writer of epoch e uploads an object, then appends a reference to it   *)
(* (a kInstallColumnar record) to its local WAL.  The reference becomes    *)
(* official when the chunk holding it lands in S3, which can happen at any *)
(* time while epoch e is unsealed, even long after the writer died.  The   *)
(* seal of epoch e discards every reference that has not landed.  Official *)
(* references can later leave retention (WAL and manifests collected).     *)
(*                                                                         *)
(* GC takes a mark (the official references and the set of sealed epochs) *)
(* and may then delete an object that is unreachable at the mark.          *)
(*                                                                         *)
(* Design: an object is deletable only if it was settled at the mark:     *)
(* the epoch that created it was sealed, or the writer of that epoch had   *)
(* declared it settled (no reference pending, none ever appended again).   *)
(* No clock is involved.  WAL references point only at objects created in  *)
(* the same epoch, keys are never reused, and an unacknowledged local WAL  *)
(* tail is never shipped in a later epoch.                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys, MaxEpoch,
    TimeBasedGc,  \* delete any unreachable object: an upload stall longer
                  \* than any grace period looks the same as abandonment
    TailReuse,    \* a sealed epoch's unshipped references ship in a later epoch
    LateSeal,     \* test "creating epoch sealed" at sweep time, not at the mark
    Dedupe,       \* content-addressed keys: an upload may hit an existing
                  \* object created in an older epoch and reuse it
    SettleEarly   \* the writer declares an object settled while a reference
                  \* to it is still pending in its local WAL

VARIABLES epoch, sealed, objs, pending, refs, mark

vars == <<epoch, sealed, objs, pending, refs, mark>>

Init ==
    /\ epoch = 1
    /\ sealed = {}
    /\ objs = [k \in Keys |-> [exists |-> FALSE, used |-> FALSE, ep |-> 0, settled |-> FALSE]]
    /\ pending = {}
    /\ refs = {}
    /\ mark = [active |-> FALSE, reach |-> {}, sealed |-> {}, settled |-> {}]

(* Conditional create under a fresh key, tagged with the creating epoch. *)
(* With Dedupe the key may already exist (a 412 taken as success).       *)
Upload(k) ==
    /\ IF Dedupe THEN TRUE ELSE ~objs[k].used
    /\ objs' = IF objs[k].exists THEN objs
               ELSE [objs EXCEPT ![k] = [exists |-> TRUE, used |-> TRUE, ep |-> epoch,
                                         settled |-> FALSE]]
    /\ UNCHANGED <<epoch, sealed, pending, refs, mark>>

(* Append the reference to the local WAL after the upload succeeded.     *)
AppendRef(k) ==
    /\ objs[k].exists
    /\ IF Dedupe THEN TRUE ELSE objs[k].ep = epoch
    /\ ~objs[k].settled
    /\ <<k, epoch>> \notin pending
    /\ k \notin refs
    /\ pending' = pending \cup {<<k, epoch>>}
    /\ UNCHANGED <<epoch, sealed, objs, refs, mark>>

(* The chunk holding the reference lands: possible while its epoch is    *)
(* unsealed, or later with TailReuse.                                     *)
Ship(r) ==
    /\ r \in pending
    /\ TailReuse \/ r[2] \notin sealed
    /\ refs' = refs \cup {r[1]}
    /\ pending' = pending \ {r}
    /\ UNCHANGED <<epoch, sealed, objs, mark>>

(* A transfer seals the current epoch and starts the next one. *)
Seal ==
    /\ epoch < MaxEpoch
    /\ sealed' = sealed \cup {epoch}
    /\ epoch' = epoch + 1
    /\ pending' = IF TailReuse THEN pending
                  ELSE {r \in pending : r[2] /= epoch}
    /\ UNCHANGED <<objs, refs, mark>>

(* The writer of the current epoch declares k settled in a manifest: no   *)
(* reference to k is pending in its local WAL, and it never appends one     *)
(* again.  This lets GC collect superseded objects of a long epoch.         *)
Settle(k) ==
    /\ objs[k].exists
    /\ objs[k].ep = epoch
    /\ ~objs[k].settled
    /\ SettleEarly \/ \A r \in pending : r[1] /= k
    /\ objs' = [objs EXCEPT ![k].settled = TRUE]
    /\ UNCHANGED <<epoch, sealed, pending, refs, mark>>

(* The last root that referenced k leaves retention.  Keys are never     *)
(* reused, so k is never referenced again.                               *)
Unref(k) ==
    /\ k \in refs
    /\ refs' = refs \ {k}
    /\ pending' = {r \in pending : r[1] /= k}
    /\ UNCHANGED <<epoch, sealed, objs, mark>>

MarkStart ==
    /\ ~mark.active
    /\ mark' = [active |-> TRUE, reach |-> refs, sealed |-> sealed,
                 settled |-> {k \in Keys : objs[k].settled}]
    /\ UNCHANGED <<epoch, sealed, objs, pending, refs>>

Sweep(k) ==
    LET sealedSet == IF LateSeal THEN sealed ELSE mark.sealed
        settledSet == IF LateSeal THEN {j \in Keys : objs[j].settled} ELSE mark.settled IN
    /\ mark.active
    /\ objs[k].exists
    /\ k \notin mark.reach
    /\ TimeBasedGc \/ objs[k].ep \in sealedSet \/ k \in settledSet
    /\ objs' = [objs EXCEPT ![k].exists = FALSE]
    /\ UNCHANGED <<epoch, sealed, pending, refs, mark>>

MarkEnd ==
    /\ mark.active
    /\ mark' = [mark EXCEPT !.active = FALSE]
    /\ UNCHANGED <<epoch, sealed, objs, pending, refs>>

Next ==
    \/ Seal \/ MarkStart \/ MarkEnd
    \/ \E k \in Keys : Upload(k) \/ AppendRef(k) \/ Settle(k) \/ Unref(k) \/ Sweep(k)
    \/ \E r \in pending : Ship(r)

Spec == Init /\ [][Next]_vars

\* Every official reference resolves to an object that exists.
RefsResolve == \A k \in refs : objs[k].exists
=============================================================================
