------------------------------ MODULE TxSpill ------------------------------
(***************************************************************************)
(* Uncommitted-page private spill (ADR 0007) + commit-only redo recovery. *)
(*                                                                         *)
(* Abstraction of:                                                         *)
(*   page/page_manager.cpp   EvictBatch, EnsureEvictedPageResident,        *)
(*                           ApplyTransactionCommitRecord,                 *)
(*                           ApplyTransactionAbortRecord,                  *)
(*                           WriteFuzzyCheckpoint                          *)
(*   transaction/transaction_manager.cpp  PreCommit, Abort,                *)
(*                           ForgetTransaction                             *)
(*   recovery/checkpoint_manager.cpp      replay anchor selection          *)
(*   recovery/recovery_manager.cpp        committed-only replay            *)
(* See README.md for the mapping and what is abstracted away.             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    NumPages, NumSlots, NumTxns,
    MaxCrashes,       \* crash/restart cycles to explore
    MaxCheckpoints,   \* fuzzy checkpoints to explore
    \* What a blocks-file write (EvictBatch / WriteFuzzyCheckpoint) waits for:
    \*   "record" : DurableLSN >= max committed slot LSN (= update-record LSN).
    \*              Historical barrier before commit reflection was tracked.
    \*   "commit" : the kCommit of every txn contributing to the image is durable.
    \*   "none"   : no wait (bug injection).
    WalBeforeDataMode,
    \* Knobs: TRUE = the implementation's current behaviour.  Flipping one to
    \* FALSE injects a known bug; the model must then fail.
    CheckStaleOnPublish,          \* EvictBatch re-checks live_seq_/committed_seq_
    CheckGenOnPublish,            \* EvictBatch refuses a descriptor into a dead block-file gen
    KeepRegisteredUntilPublished  \* ForgetTransaction runs after commit reflection

ASSUME NumPages \in Nat \ {0} /\ NumSlots \in Nat \ {0} /\ NumTxns \in Nat \ {0}
ASSUME MaxCrashes \in Nat \ {0} /\ MaxCheckpoints \in Nat \ {0}
ASSUME WalBeforeDataMode \in {"record", "commit", "none"}

Pages == 1..NumPages
Slots == 1..NumSlots
Txns  == 1..NumTxns
Keys  == Pages \X Slots
NoTxn == 0
NoKey == <<0, 0>>
\* A slot value is 0 (initial) or the id of the transaction that wrote it.

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

InitImg == [s \in Slots |-> [v |-> 0, l |-> 0]]   \* committed slot value + slot LSN
NoSpill == [has |-> FALSE, ov |-> [s \in Slots |-> 0]]
EvNone  == [phase |-> "none", hasUnc |-> FALSE, ov |-> [s \in Slots |-> 0],
            needIO |-> FALSE, img |-> InitImg, idx |-> 0, stale |-> FALSE]
CpNone  == [phase |-> "none", snap |-> 0, anchor |-> 0,
            descs |-> [p \in Pages |-> 0], done |-> {}]

VARIABLES
    \* ---- WAL ----
    wal,        \* Seq of records; LSN = index
    durable,    \* DurableLSN: wal[1..durable] is fsynced
    \* ---- transactions ----
    tstate,     \* idle|active|committing|published|forgotten|aborting|aborted|dead
    registered, \* member of active_transactions_ (pins the checkpoint anchor)
    firstLsn,   \* Transaction::FirstLSN()
    writes,     \* LSNs of the txn's update records, in order
    commitLsn,  \* LSN of the txn's kCommit (0 = none)
    cursor,     \* progress of commit reflection / abort rollback
    acked,      \* commit acknowledged to the client (after WaitForDurable)
    lockOwner,  \* row write intent
    \* ---- volatile page state (PageManager) ----
    resident,   \* page is in blocks_
    live,       \* live_packed_      (valid only while resident)
    cval, clsn, \* committed_packed_ + committed_lsns_ (valid only while resident)
    diskDirty,  \* rp.disk_dirty_
    desc,       \* disk_offset_ / evicted_pages_ descriptor: index into blocks
    spill,      \* uncommitted_spills_: 'TUPO' overlay in the temp SpillStore
    \* ---- persistent state ----
    blocks,     \* <db>.blocks.*: append-only images [page, gen, img]
    bgen,       \* live block-file generation (older gens are deleted)
    ckpt,       \* <db>.db snapshot: [desc, anchor]
    \* ---- in-flight background work ----
    ev,         \* per-page EvictBatch progress (one evictor: eviction_mu_)
    cp,         \* fuzzy checkpoint in progress
    \* ---- bounds ----
    crashes, nckpt,
    \* ---- ghosts: the abstract database the implementation must represent ----
    gLive, gCommitted

vars == <<wal, durable, tstate, registered, firstLsn, writes, commitLsn, cursor,
          acked, lockOwner, resident, live, cval, clsn, diskDirty, desc, spill,
          blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive, gCommitted>>

-----------------------------------------------------------------------------
PageImg(p)  == [s \in Slots |-> [v |-> cval[<<p, s>>], l |-> clsn[<<p, s>>]]]
LiveOf(p)   == [s \in Slots |-> live[<<p, s>>]]
LiveEqCommitted(p) == \A s \in Slots : live[<<p, s>>] = cval[<<p, s>>]
MaxLsn(img) == Max({img[s].l : s \in Slots})
CleanEvictable(p) == LiveEqCommitted(p) /\ ~diskDirty[p] /\ desc[p] # 0
NoEvictInFlight == \A q \in Pages : ev[q].phase = "none"
\* Any change to a page's live or committed state invalidates an in-flight
\* eviction capture (live_seq_ / committed_seq_ bump).
MarkStale(p) == IF ev[p].phase = "none" THEN ev ELSE [ev EXCEPT ![p].stale = TRUE]

CommitDurable(t) == commitLsn[t] # 0 /\ commitLsn[t] <= durable

\* Guard of every blocks-file write of a committed image (WAL-before-data).
DataWriteAllowed(img) ==
    CASE WalBeforeDataMode = "none"   -> TRUE
      [] WalBeforeDataMode = "record" -> durable >= MaxLsn(img)
      [] WalBeforeDataMode = "commit" ->
            \A s \in Slots : LET l == img[s].l IN
                IF l = 0 THEN TRUE
                ELSE IF l \in DOMAIN wal THEN CommitDurable(wal[l].txn)
                ELSE FALSE

UpdRec(t, k, new, old) == [kind |-> "upd", txn |-> t, key |-> k, new |-> new, old |-> old]
CtlRec(kind, t)        == [kind |-> kind, txn |-> t, key |-> NoKey, new |-> 0, old |-> 0]

\* Redo of committed records with slot-LSN idempotence (ReplayCommittedRecord).
RECURSIVE Replay(_, _, _, _)
Replay(W, st, i, comm) ==
    IF i > Len(W) THEN st
    ELSE LET r   == W[i]
             st2 == IF r.kind = "upd" /\ r.txn \in comm /\ i > st[r.key].l
                    THEN [st EXCEPT ![r.key] = [v |-> r.new, l |-> i]]
                    ELSE st
         IN Replay(W, st2, i + 1, comm)

CommittedIn(W, from) == {W[i].txn : i \in {j \in from..Len(W) : W[j].kind = "commit"}}

-----------------------------------------------------------------------------
Init ==
    /\ wal = <<>>
    /\ durable = 0
    /\ tstate = [t \in Txns |-> "idle"]
    /\ registered = [t \in Txns |-> FALSE]
    /\ firstLsn = [t \in Txns |-> 0]
    /\ writes = [t \in Txns |-> <<>>]
    /\ commitLsn = [t \in Txns |-> 0]
    /\ cursor = [t \in Txns |-> 0]
    /\ acked = [t \in Txns |-> FALSE]
    /\ lockOwner = [k \in Keys |-> NoTxn]
    /\ resident = [p \in Pages |-> TRUE]
    /\ live = [k \in Keys |-> 0]
    /\ cval = [k \in Keys |-> 0]
    /\ clsn = [k \in Keys |-> 0]
    /\ diskDirty = [p \in Pages |-> FALSE]
    /\ desc = [p \in Pages |-> p]
    /\ spill = [p \in Pages |-> NoSpill]
    /\ blocks = [i \in Pages |-> [page |-> i, gen |-> 1, img |-> InitImg]]
    /\ bgen = 1
    /\ ckpt = [desc |-> [p \in Pages |-> p], anchor |-> 1]
    /\ ev = [p \in Pages |-> EvNone]
    /\ cp = CpNone
    /\ crashes = 0
    /\ nckpt = 0
    /\ gLive = [k \in Keys |-> 0]
    /\ gCommitted = [k \in Keys |-> 0]

-----------------------------------------------------------------------------
(* Transactions *)

Begin(t) ==
    /\ tstate[t] = "idle"
    /\ tstate' = [tstate EXCEPT ![t] = "active"]
    /\ registered' = [registered EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<wal, durable, firstLsn, writes, commitLsn, cursor, acked,
                   lockOwner, resident, live, cval, clsn, diskDirty, desc, spill,
                   blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive, gCommitted>>

\* Row write under the page latch: AddLog, then modify live_packed_.
Write(t, k) ==
    LET p == k[1] IN
    /\ tstate[t] = "active"
    /\ resident[p]
    /\ lockOwner[k] \in {NoTxn, t}
    /\ \A i \in 1..Len(writes[t]) : wal[writes[t][i]].key # k
    /\ wal' = Append(wal, UpdRec(t, k, t, live[k]))
    /\ writes' = [writes EXCEPT ![t] = Append(@, Len(wal) + 1)]
    /\ firstLsn' = IF firstLsn[t] = 0
                   THEN [firstLsn EXCEPT ![t] = Len(wal) + 1] ELSE firstLsn
    /\ live' = [live EXCEPT ![k] = t]
    /\ lockOwner' = [lockOwner EXCEPT ![k] = t]
    /\ gLive' = [gLive EXCEPT ![k] = t]
    /\ ev' = MarkStale(p)
    /\ UNCHANGED <<durable, tstate, registered, commitLsn, cursor, acked,
                   resident, cval, clsn, diskDirty, desc, spill, blocks, bgen,
                   ckpt, cp, crashes, nckpt, gCommitted>>

\* PreCommit: kCommit AddLog + status flip under chain_mu_.
LogCommit(t) ==
    /\ tstate[t] = "active"
    /\ Len(writes[t]) > 0
    /\ wal' = Append(wal, CtlRec("commit", t))
    /\ commitLsn' = [commitLsn EXCEPT ![t] = Len(wal) + 1]
    /\ tstate' = [tstate EXCEPT ![t] = "committing"]
    /\ cursor' = [cursor EXCEPT ![t] = 1]
    /\ registered' = IF KeepRegisteredUntilPublished THEN registered
                     ELSE [registered EXCEPT ![t] = FALSE]
    /\ UNCHANGED <<durable, firstLsn, writes, acked, lockOwner, resident, live,
                   cval, clsn, diskDirty, desc, spill, blocks, bgen, ckpt, ev,
                   cp, crashes, nckpt, gLive, gCommitted>>

\* ApplyTransactionCommitRecord: one record at a time, BEFORE the commit is
\* durable.  The slot LSN is the update record's LSN (record_lsn).  An
\* evicted page must be faulted in first (FaultIn), as the implementation
\* loops on EnsureEvictedPageResident.
ApplyCommit(t) ==
    /\ tstate[t] = "committing"
    /\ cursor[t] <= Len(writes[t])
    /\ LET l == writes[t][cursor[t]]
           r == wal[l]
           p == r.key[1]
       IN /\ resident[p]
          /\ cval' = [cval EXCEPT ![r.key] = r.new]
          /\ clsn' = [clsn EXCEPT ![r.key] = l]
          /\ diskDirty' = [diskDirty EXCEPT ![p] = TRUE]
          /\ gCommitted' = [gCommitted EXCEPT ![r.key] = r.new]
          /\ ev' = MarkStale(p)
    /\ cursor' = [cursor EXCEPT ![t] = @ + 1]
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   acked, lockOwner, resident, live, desc, spill, blocks, bgen,
                   ckpt, cp, crashes, nckpt, gLive>>

\* CommitVersions: publish and release write intents.
FinishPublish(t) ==
    /\ tstate[t] = "committing"
    /\ cursor[t] > Len(writes[t])
    /\ tstate' = [tstate EXCEPT ![t] = "published"]
    /\ lockOwner' = [k \in Keys |-> IF lockOwner[k] = t THEN NoTxn ELSE lockOwner[k]]
    /\ UNCHANGED <<wal, durable, registered, firstLsn, writes, commitLsn, cursor,
                   acked, resident, live, cval, clsn, diskDirty, desc, spill,
                   blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive, gCommitted>>

\* ForgetTransaction: leave active_transactions_.
Forget(t) ==
    /\ tstate[t] = "published"
    /\ tstate' = [tstate EXCEPT ![t] = "forgotten"]
    /\ registered' = [registered EXCEPT ![t] = FALSE]
    /\ UNCHANGED <<wal, durable, firstLsn, writes, commitLsn, cursor, acked,
                   lockOwner, resident, live, cval, clsn, diskDirty, desc, spill,
                   blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive, gCommitted>>

\* Reply to the client after WaitForDurable(commit end).
Ack(t) ==
    /\ tstate[t] \in {"published", "forgotten"}
    /\ ~acked[t]
    /\ durable >= commitLsn[t]
    /\ acked' = [acked EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, lockOwner, resident, live, cval, clsn, diskDirty, desc,
                   spill, blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive,
                   gCommitted>>

StartAbort(t) ==
    /\ tstate[t] = "active"
    /\ tstate' = [tstate EXCEPT ![t] = "aborting"]
    /\ cursor' = [cursor EXCEPT ![t] = Len(writes[t])]
    /\ UNCHANGED <<wal, durable, registered, firstLsn, writes, commitLsn, acked,
                   lockOwner, resident, live, cval, clsn, diskDirty, desc, spill,
                   blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive, gCommitted>>

\* ApplyTransactionAbortRecord: restore the live slot, newest record first
\* (staged copy or WAL re-read via prev_lsn: same effect in this model).
UndoStep(t) ==
    /\ tstate[t] = "aborting"
    /\ cursor[t] >= 1
    /\ LET r == wal[writes[t][cursor[t]]]
           p == r.key[1]
       IN /\ resident[p]
          /\ live' = [live EXCEPT ![r.key] = r.old]
          /\ gLive' = [gLive EXCEPT ![r.key] = r.old]
          /\ ev' = MarkStale(p)
    /\ cursor' = [cursor EXCEPT ![t] = @ - 1]
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   acked, lockOwner, resident, cval, clsn, diskDirty, desc, spill,
                   blocks, bgen, ckpt, cp, crashes, nckpt, gCommitted>>

\* Append kAbort (not waited for), release intents, ForgetTransaction.
FinishAbort(t) ==
    /\ tstate[t] = "aborting"
    /\ cursor[t] = 0
    /\ wal' = IF Len(writes[t]) > 0 THEN Append(wal, CtlRec("abort", t)) ELSE wal
    /\ lockOwner' = [k \in Keys |-> IF lockOwner[k] = t THEN NoTxn ELSE lockOwner[k]]
    /\ tstate' = [tstate EXCEPT ![t] = "aborted"]
    /\ registered' = [registered EXCEPT ![t] = FALSE]
    /\ UNCHANGED <<durable, firstLsn, writes, commitLsn, cursor, acked, resident,
                   live, cval, clsn, diskDirty, desc, spill, blocks, bgen, ckpt,
                   ev, cp, crashes, nckpt, gLive, gCommitted>>

\* Group commit / periodic fdatasync.
Flush ==
    /\ durable < Len(wal)
    /\ durable' = durable + 1
    /\ UNCHANGED <<wal, tstate, registered, firstLsn, writes, commitLsn, cursor,
                   acked, lockOwner, resident, live, cval, clsn, diskDirty, desc,
                   spill, blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive,
                   gCommitted>>

-----------------------------------------------------------------------------
(* Buffer pool *)

\* EnsureEvictedPageResident: committed image from the blocks file, then the
\* 'TUPO' overlay (if any) on top; the overlay segment is dropped.
FaultIn(p) ==
    /\ ~resident[p]
    /\ LET e == blocks[desc[p]] IN
       /\ cval' = [k \in Keys |-> IF k[1] = p THEN e.img[k[2]].v ELSE cval[k]]
       /\ clsn' = [k \in Keys |-> IF k[1] = p THEN e.img[k[2]].l ELSE clsn[k]]
       /\ live' = [k \in Keys |->
                    IF k[1] # p THEN live[k]
                    ELSE IF spill[p].has THEN spill[p].ov[k[2]] ELSE e.img[k[2]].v]
    /\ resident' = [resident EXCEPT ![p] = TRUE]
    /\ spill' = [spill EXCEPT ![p] = NoSpill]
    /\ diskDirty' = [diskDirty EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, desc, blocks, bgen, ckpt, ev, cp,
                   crashes, nckpt, gLive, gCommitted>>

\* EvictBatch, clean_match && !disk_dirty_ path: drop the payload.
EvictClean(p) ==
    /\ resident[p]
    /\ NoEvictInFlight
    /\ CleanEvictable(p)
    /\ resident' = [resident EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, live, cval, clsn, diskDirty, desc,
                   spill, blocks, bgen, ckpt, ev, cp, crashes, nckpt, gLive,
                   gCommitted>>

\* EvictBatch phase 1 (page latch held): build the committed block record if
\* disk-dirty and EncodeUncommittedOverlay if live != committed.
EvictCapture(p) ==
    /\ resident[p]
    /\ NoEvictInFlight
    /\ ~CleanEvictable(p)
    /\ ev' = [ev EXCEPT ![p] =
                [phase  |-> "captured",
                 hasUnc |-> ~LiveEqCommitted(p),
                 ov     |-> LiveOf(p),
                 needIO |-> diskDirty[p] \/ desc[p] = 0,
                 img    |-> PageImg(p),
                 idx    |-> 0,
                 stale  |-> FALSE]]
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, resident, live, cval, clsn,
                   diskDirty, desc, spill, blocks, bgen, ckpt, cp, crashes,
                   nckpt, gLive, gCommitted>>

\* EvictBatch phase 2 (no page latch): WaitForDurableLSN(max_batch_lsn),
\* pwrite + fdatasync the blocks file, write the overlay to the SpillStore.
EvictWrite(p) ==
    /\ ev[p].phase = "captured"
    /\ IF ev[p].needIO
       THEN /\ DataWriteAllowed(ev[p].img)
            /\ blocks' = Append(blocks, [page |-> p, gen |-> bgen, img |-> ev[p].img])
            /\ ev' = [ev EXCEPT ![p].phase = "written", ![p].idx = Len(blocks) + 1]
       ELSE /\ ev' = [ev EXCEPT ![p].phase = "written"]
            /\ UNCHANGED blocks
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, resident, live, cval, clsn,
                   diskDirty, desc, spill, bgen, ckpt, cp, crashes, nckpt,
                   gLive, gCommitted>>

\* EvictBatch phase 3 (page latch re-taken): re-validate, then publish the
\* descriptor and the overlay, or give up and keep the page resident.
EvictPublish(p) ==
    /\ ev[p].phase = "written"
    /\ LET e  == ev[p]
           ok == /\ (CheckStaleOnPublish => ~e.stale)
                 /\ ((e.needIO /\ CheckGenOnPublish) => blocks[e.idx].gen = bgen)
       IN IF ok
          THEN /\ resident' = [resident EXCEPT ![p] = FALSE]
               /\ desc' = IF e.needIO THEN [desc EXCEPT ![p] = e.idx] ELSE desc
               /\ diskDirty' = [diskDirty EXCEPT ![p] = FALSE]
               /\ spill' = [spill EXCEPT ![p] =
                              IF e.hasUnc THEN [has |-> TRUE, ov |-> e.ov] ELSE NoSpill]
          ELSE UNCHANGED <<resident, desc, diskDirty, spill>>
    /\ ev' = [ev EXCEPT ![p] = EvNone]
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, live, cval, clsn, blocks, bgen, ckpt,
                   cp, crashes, nckpt, gLive, gCommitted>>

-----------------------------------------------------------------------------
(* Fuzzy checkpoint *)

\* CheckpointManager: under transaction_table_lock, snap = BufferedLSN and
\* anchor = min(snap, FirstLSN of every registered txn that has logged).
CkptBegin ==
    /\ cp.phase = "none"
    /\ nckpt < MaxCheckpoints
    /\ LET snap == Len(wal) + 1
           pins == {firstLsn[t] : t \in {u \in Txns : registered[u] /\ firstLsn[u] > 0}}
       IN cp' = [phase |-> "pages", snap |-> snap, anchor |-> Min(pins \cup {snap}),
                 descs |-> [p \in Pages |-> 0], done |-> {}]
    /\ nckpt' = nckpt + 1
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, resident, live, cval, clsn,
                   diskDirty, desc, spill, blocks, bgen, ckpt, ev, crashes,
                   gLive, gCommitted>>

\* WriteFuzzyCheckpoint, one page at a time, concurrently with everything else.
CkptPage(p) ==
    /\ cp.phase = "pages"
    /\ p \notin cp.done
    /\ durable >= cp.snap - 1                     \* WaitForDurable(snap_lsn)
    /\ IF resident[p] /\ (diskDirty[p] \/ desc[p] = 0)
       THEN LET img == PageImg(p) IN
            /\ DataWriteAllowed(img)              \* WaitForDurableLSN(max_dirty_lsn)
            /\ blocks' = Append(blocks, [page |-> p, gen |-> bgen, img |-> img])
            /\ desc' = [desc EXCEPT ![p] = Len(blocks) + 1]
            /\ diskDirty' = [diskDirty EXCEPT ![p] = FALSE]
            /\ cp' = [cp EXCEPT !.descs[p] = Len(blocks) + 1, !.done = @ \cup {p}]
       ELSE /\ cp' = [cp EXCEPT !.descs[p] = desc[p], !.done = @ \cup {p}]
            /\ UNCHANGED <<blocks, desc, diskDirty>>
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, resident, live, cval, clsn, spill,
                   bgen, ckpt, ev, crashes, nckpt, gLive, gCommitted>>

\* Publish <db>.db atomically (tmp + rename).  With compact = TRUE, every
\* referenced image is copied into a new block-file generation, existing
\* descriptors are fixed up to copied images in the new generation. Old
\* images remain in blocks; generation checks reject stale descriptors. An
\* eviction that already wrote into the old generation is NOT fixed up.
CkptFinish(compact) ==
    /\ cp.phase = "pages"
    /\ cp.done = Pages
    /\ IF compact
       THEN LET R == {desc[p] : p \in Pages} \cup {cp.descs[p] : p \in Pages}
                NewIdx(i) == Len(blocks) + Cardinality({j \in R : j <= i})
                Copied == [n \in 1..Cardinality(R) |->
                             LET i == CHOOSE x \in R : Cardinality({j \in R : j <= x}) = n
                             IN [blocks[i] EXCEPT !.gen = bgen + 1]]
            IN /\ blocks' = blocks \o Copied
               /\ bgen' = bgen + 1
               /\ desc' = [p \in Pages |-> NewIdx(desc[p])]
               /\ ckpt' = [desc |-> [p \in Pages |-> NewIdx(cp.descs[p])],
                           anchor |-> cp.anchor]
       ELSE /\ ckpt' = [desc |-> cp.descs, anchor |-> cp.anchor]
            /\ UNCHANGED <<blocks, bgen, desc>>
    /\ cp' = CpNone
    /\ UNCHANGED <<wal, durable, tstate, registered, firstLsn, writes, commitLsn,
                   cursor, acked, lockOwner, resident, live, cval, clsn,
                   diskDirty, spill, ev, crashes, nckpt, gLive, gCommitted>>

-----------------------------------------------------------------------------
(* Crash + restart *)

\* Everything volatile is lost: RAM pages, the SpillStore (unlinked temp file),
\* the un-fsynced WAL tail, in-flight eviction/checkpoint, the txn registry.
\* Restart = load snapshot + redo of committed owners from the anchor.
\* The ghost is reset to the "true" committed state: every update of a txn
\* whose kCommit is durable, replayed from LSN 1 over the initial database.
Crash ==
    /\ crashes < MaxCrashes
    /\ LET W    == SubSeq(wal, 1, durable)
           base == [k \in Keys |-> blocks[ckpt.desc[k[1]]].img[k[2]]]
           rec  == Replay(W, base, ckpt.anchor, CommittedIn(W, ckpt.anchor))
           exp  == Replay(W, [k \in Keys |-> [v |-> 0, l |-> 0]], 1, CommittedIn(W, 1))
       IN /\ wal' = W
          /\ cval' = [k \in Keys |-> rec[k].v]
          /\ clsn' = [k \in Keys |-> rec[k].l]
          /\ live' = [k \in Keys |-> rec[k].v]
          /\ diskDirty' = [p \in Pages |-> \E s \in Slots : rec[<<p, s>>] # base[<<p, s>>]]
          /\ gCommitted' = [k \in Keys |-> exp[k].v]
          /\ gLive' = [k \in Keys |-> exp[k].v]
    /\ resident' = [p \in Pages |-> TRUE]
    /\ desc' = ckpt.desc
    /\ spill' = [p \in Pages |-> NoSpill]
    /\ ev' = [p \in Pages |-> EvNone]
    /\ cp' = CpNone
    /\ tstate' = [t \in Txns |-> IF tstate[t] = "idle" THEN "idle" ELSE "dead"]
    /\ registered' = [t \in Txns |-> FALSE]
    /\ lockOwner' = [k \in Keys |-> NoTxn]
    /\ commitLsn' = [t \in Txns |-> IF commitLsn[t] > durable THEN 0 ELSE commitLsn[t]]
    \* Dead txns can never act again: drop their WAL bookkeeping too, or
    \* writes/cursor/acked keep pointing past the truncated wal'.
    /\ firstLsn' = [t \in Txns |-> IF tstate[t] = "idle" THEN firstLsn[t] ELSE 0]
    /\ writes' = [t \in Txns |-> IF tstate[t] = "idle" THEN writes[t] ELSE <<>>]
    /\ cursor' = [t \in Txns |-> IF tstate[t] = "idle" THEN cursor[t] ELSE 0]
    /\ acked' = [t \in Txns |-> IF tstate[t] = "idle" THEN acked[t] ELSE FALSE]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<durable, blocks, bgen, ckpt, nckpt>>

-----------------------------------------------------------------------------
Next ==
    \/ \E t \in Txns :
         \/ Begin(t) \/ LogCommit(t) \/ ApplyCommit(t) \/ FinishPublish(t)
         \/ Forget(t) \/ Ack(t) \/ StartAbort(t) \/ UndoStep(t) \/ FinishAbort(t)
    \/ \E t \in Txns, k \in Keys : Write(t, k)
    \/ Flush
    \/ \E p \in Pages :
         \/ FaultIn(p) \/ EvictClean(p) \/ EvictCapture(p) \/ EvictWrite(p)
         \/ EvictPublish(p) \/ CkptPage(p)
    \/ CkptBegin
    \/ \E c \in BOOLEAN : CkptFinish(c)
    \/ Crash

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* What a reader would see for key k right now, wherever the page lives.
ViewCommitted(k) ==
    IF resident[k[1]] THEN cval[k] ELSE blocks[desc[k[1]]].img[k[2]].v
ViewLive(k) ==
    IF resident[k[1]] THEN live[k]
    ELSE IF spill[k[1]].has THEN spill[k[1]].ov[k[2]] ELSE ViewCommitted(k)

\* Eviction + fault-in (with or without an overlay) never changes the
\* committed state; after a crash, restart reproduces exactly the durable
\* committed history (the Crash action resets the ghost to that history).
CommittedStateCorrect == \A k \in Keys : ViewCommitted(k) = gCommitted[k]

\* Uncommitted changes survive eviction via the overlay, and abort restores them.
LiveStateCorrect == \A k \in Keys : ViewLive(k) = gLive[k]

\* An evicted page's descriptor points into a block file that still exists.
EvictedPageReadable == \A p \in Pages : ~resident[p] => blocks[desc[p]].gen = bgen

\* The published snapshot references only existing block-file images.
CheckpointReadable == \A p \in Pages : blocks[ckpt.desc[p]].gen = bgen

\* A blocks image may expose a reflected update only once its update and
\* contributing commit have reached the durable WAL frontier.
WalBeforeData ==
    \A i \in 1..Len(blocks) : \A s \in Slots :
        LET l == blocks[i].img[s].l IN
        \* IF/THEN/ELSE is lazy: wal[l] is only evaluated when l is inside
        \* the (possibly crash-truncated) WAL domain.  A slot LSN beyond
        \* Len(wal) is a violation (FALSE), not a TLC domain error.
        IF l = 0 THEN TRUE
        ELSE IF l <= Len(wal) THEN (l <= durable /\ CommitDurable(wal[l].txn))
        ELSE FALSE

\* A commit acknowledged to the client is durable.
AckedIsDurable == \A t \in Txns : acked[t] => CommitDurable(t)

\* Overlays exist only for evicted pages.
SpillOnlyForEvicted == \A p \in Pages : spill[p].has => ~resident[p]

=============================================================================
