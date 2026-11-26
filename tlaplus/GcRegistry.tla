----------------------------- MODULE GcRegistry -----------------------------
(***************************************************************************)
(* Reader registrations versus GC sweeps (docs/primary-s3.md, 13.4).       *)
(*                                                                         *)
(* A reader (standby, restore, long query) registers what it depends on    *)
(* with a CAS on control/gc.json, then checks that each object is not in   *)
(* the recorded delete set and still exists; from then on it reads it.     *)
(* The GC marks, records the delete set with a CAS, and sends DELETEs that *)
(* may arrive at any later time, even after the GC died.  Another GC can   *)
(* take over.  Objects are immutable and keys are never reused, so a late  *)
(* DELETE cannot be stopped by If-Match.  Roots other than registrations   *)
(* are abstracted away: every unregistered object is garbage here.         *)
(*                                                                         *)
(* Design: the GC deletes only keys of the recorded set, the recorded set  *)
(* excludes registrations made before the record (the CAS fails and the   *)
(* GC re-reads them), and a GC that takes over finishes the recorded set   *)
(* before it clears it.                                                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Keys, MaxCrashes,
    NoRecord,   \* the delete set lives only in the GC's memory
    StaleMark   \* the recorded set is the mark as taken, ignoring later
                \* registrations

VARIABLES exists, reg, holds, mark, pend, gcset, net, alive, crashes

vars == <<exists, reg, holds, mark, pend, gcset, net, alive, crashes>>

Init ==
    /\ exists = [k \in Keys |-> TRUE]
    /\ reg = {}
    /\ holds = {}
    /\ mark = {}
    /\ pend = {}
    /\ gcset = {}
    /\ net = {}
    /\ alive = TRUE
    /\ crashes = 0

Register(k) ==
    /\ k \notin reg
    /\ reg' = reg \cup {k}
    /\ UNCHANGED <<exists, holds, mark, pend, gcset, net, alive, crashes>>

Verify(k) ==
    /\ k \in reg
    /\ k \notin holds
    /\ k \notin pend
    /\ exists[k]
    /\ holds' = holds \cup {k}
    /\ UNCHANGED <<exists, reg, mark, pend, gcset, net, alive, crashes>>

Unregister(k) ==
    /\ k \in reg
    /\ reg' = reg \ {k}
    /\ holds' = holds \ {k}
    /\ UNCHANGED <<exists, mark, pend, gcset, net, alive, crashes>>

Mark ==
    /\ alive
    /\ pend = {}
    /\ gcset = {}
    /\ mark' = {k \in Keys : exists[k] /\ k \notin reg}
    /\ UNCHANGED <<exists, reg, holds, pend, gcset, net, alive, crashes>>

Record ==
    /\ alive
    /\ pend = {}
    /\ gcset = {}
    /\ LET set == IF StaleMark THEN mark ELSE mark \ reg IN
       /\ set /= {}
       /\ gcset' = set
       /\ pend' = IF NoRecord THEN {} ELSE set
    /\ mark' = {}
    /\ UNCHANGED <<exists, reg, holds, net, alive, crashes>>

Send(k) ==
    /\ alive
    /\ k \in gcset
    /\ net' = net \cup {k}
    /\ UNCHANGED <<exists, reg, holds, mark, pend, gcset, alive, crashes>>

Deliver(k) ==
    /\ k \in net
    /\ exists' = [exists EXCEPT ![k] = FALSE]
    /\ net' = net \ {k}
    /\ UNCHANGED <<reg, holds, mark, pend, gcset, alive, crashes>>

\* Every key of the set is confirmed gone: clear the set.
Complete ==
    /\ alive
    /\ gcset /= {}
    /\ \A k \in gcset : ~exists[k]
    /\ gcset' = {}
    /\ pend' = {}
    /\ UNCHANGED <<exists, reg, holds, mark, net, alive, crashes>>

\* The GC dies; its DELETEs already in flight still arrive.
Crash ==
    /\ alive
    /\ crashes < MaxCrashes
    /\ alive' = FALSE
    /\ crashes' = crashes + 1
    /\ gcset' = {}
    /\ mark' = {}
    /\ UNCHANGED <<exists, reg, holds, pend, net>>

\* Another GC takes over (the CAS raises the generation, so the dead GC can
\* no longer record a set) and resumes the recorded set.
Takeover ==
    /\ ~alive
    /\ alive' = TRUE
    /\ gcset' = pend
    /\ UNCHANGED <<exists, reg, holds, mark, pend, net, crashes>>

Next ==
    \/ Mark \/ Record \/ Complete \/ Crash \/ Takeover
    \/ \E k \in Keys : Register(k) \/ Verify(k) \/ Unregister(k)
                       \/ Send(k) \/ Deliver(k)

Spec == Init /\ [][Next]_vars

\* An object a reader verified and still holds exists.
HeldExists == \A k \in holds : exists[k]
\* The sweep does delete something (the rules are not vacuous).
SomeDeleted == \A k \in Keys : exists[k]
=============================================================================
