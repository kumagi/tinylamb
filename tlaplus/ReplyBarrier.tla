---------------------------- MODULE ReplyBarrier ----------------------------
(***************************************************************************)
(* When may a reply leave the server?  (docs/primary-s3.md, reply barrier) *)
(*                                                                         *)
(* Commits toggle one key (insert, delete, insert, ...).  Write intents    *)
(* serialize them, so LSN order is publication order.  A commit becomes    *)
(* visible (MVCC publish) before its WAL is durable (S3 durable LSN in S3  *)
(* mode, local DurableLSN in local mode).  A reader takes a snapshot,      *)
(* observes the key through the heap or through an index, computes a      *)
(* target LSN by one of the rules below, and replies once durable >=       *)
(* target.  Safety: a reply never reveals a commit that is not durable.    *)
(*                                                                         *)
(* Rules                                                                   *)
(*   "version"  : the existing D4 rule.  Heap reads of a retained version  *)
(*                chain record that version's commit LSN.  An index lookup *)
(*                that finds nothing reads no heap row and records nothing.*)
(*   "tree"     : "version" plus the published-modification LSN of the     *)
(*                object read (heap or index tree), read at snapshot time. *)
(*   "snapshot" : the highest published commit LSN at snapshot time.       *)
(*   "buffered" : the WAL buffered end when the reply is ready (old draft).*)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    NumCommits, Readers, Paths,
    Rule,               \* "version" | "tree" | "snapshot" | "buffered"
    ModBeforeVisible,   \* raise mod/published LSNs before the commit is visible
    GcEarly             \* drop a version chain before its commit is durable

VARIABLES
    nlog,     \* commit records appended (LSN = position)
    raised,   \* commits whose mod LSN / published LSN were raised
    vis,      \* commits visible to new snapshots (prefix)
    durable,  \* durable prefix of the WAL
    chains,   \* LSNs whose heap version chain is still retained
    rd,       \* reader state
    sent      \* replies sent: [rel, dur]

vars == <<nlog, raised, vis, durable, chains, rd, sent>>

Max(a, b) == IF a > b THEN a ELSE b
Present(m) == m % 2 = 1          \* after m toggles starting from absent

Init ==
    /\ nlog = 0 /\ raised = 0 /\ vis = 0 /\ durable = 0
    /\ chains = {}
    /\ rd = [r \in Readers |-> [st |-> "idle", rel |-> 0, target |-> 0]]
    /\ sent = {}

(* A transaction commits (kCommit appended).  The previous commit on the *)
(* key must have been published: it held the write intent.              *)
AppendCommit ==
    /\ nlog < NumCommits
    /\ vis = nlog /\ raised = nlog
    /\ nlog' = nlog + 1
    /\ UNCHANGED <<raised, vis, durable, chains, rd, sent>>

Raise ==
    /\ raised < nlog
    /\ IF ModBeforeVisible THEN vis = raised ELSE vis = raised + 1
    /\ raised' = raised + 1
    /\ UNCHANGED <<nlog, vis, durable, chains, rd, sent>>

Publish ==
    /\ vis < nlog
    /\ IF ModBeforeVisible THEN raised = vis + 1 ELSE raised = vis
    /\ vis' = vis + 1
    /\ chains' = chains \cup {vis + 1}
    /\ UNCHANGED <<nlog, raised, durable, rd, sent>>

Flush ==
    /\ durable < nlog
    /\ durable' = durable + 1
    /\ UNCHANGED <<nlog, raised, vis, chains, rd, sent>>

Gc(i) ==
    /\ i \in chains
    /\ GcEarly \/ durable >= i
    /\ chains' = chains \ {i}
    /\ UNCHANGED <<nlog, raised, vis, durable, rd, sent>>

(* Dependency recorded by the existing per-version rule. *)
VersionDep(snap, path) ==
    IF snap = 0 THEN 0
    ELSE IF path = "index" /\ ~Present(snap) THEN 0   \* nothing found, no heap read
    ELSE IF snap \in chains THEN snap                  \* version chain read
    ELSE 0                                             \* physical image read

Read(r, path) ==
    LET snap == vis
        dep  == CASE Rule = "version"  -> VersionDep(snap, path)
                  [] Rule = "tree"     -> Max(VersionDep(snap, path), raised)
                  [] Rule = "snapshot" -> raised
                  [] Rule = "buffered" -> 0
    IN
    /\ rd[r].st = "idle"
    /\ path \in Paths
    /\ rd' = [rd EXCEPT ![r] = [st |-> "ready", rel |-> snap, target |-> dep]]
    /\ UNCHANGED <<nlog, raised, vis, durable, chains, sent>>

Reply(r) ==
    LET target == IF Rule = "buffered" THEN nlog ELSE rd[r].target IN
    /\ rd[r].st = "ready"
    /\ durable >= target
    /\ sent' = sent \cup {[rel |-> rd[r].rel, dur |-> durable]}
    /\ rd' = [rd EXCEPT ![r].st = "done"]
    /\ UNCHANGED <<nlog, raised, vis, durable, chains>>

Next ==
    \/ AppendCommit \/ Raise \/ Publish \/ Flush
    \/ \E i \in 1..NumCommits : Gc(i)
    \/ \E r \in Readers, p \in Paths : Read(r, p)
    \/ \E r \in Readers : Reply(r)

Spec == Init /\ [][Next]_vars

\* The latest commit the reply observed was durable when the reply left.
ObservedDurable == \A s \in sent : s.rel <= s.dur
=============================================================================
