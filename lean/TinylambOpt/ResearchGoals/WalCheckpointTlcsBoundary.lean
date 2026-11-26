import Std

/-!
These claims describe the write-ahead ordering between checkpoint state and encoded
column data. SQL recovery depends on reading data and redo records from a consistent
modeled boundary.

Modeled claims

- `Goal` — WAL checkpoint vs persistent TLCS boundary
  (`recovery/checkpoint_manager.cpp`, `recovery/recovery_manager.cpp`): under
  writes-before-commit and MVCC write-intent commit serialization, TLCS snapshot at
  `cpLsn` plus WAL replay from `minActiveWriteLsn` agrees with full WAL replay from
  LSN 0.
-/

namespace TinylambOpt.ResearchGoals.WalCheckpointTlcsBoundary

inductive Record where
  | write (tx : Nat) (key : Nat) (val : Int)
  | commit (tx : Nat)
  | abort (tx : Nat)
  deriving DecidableEq

abbrev State := Nat → Option Int

def applyWrite (st : State) (key : Nat) (val : Int) : State :=
  fun k => if k == key then some val else st k

def commitsIn (rs : List Record) : List Nat :=
  rs.filterMap fun
    | .commit tx => some tx
    | _ => none

def foldWritesFor (txs : List Nat) (rs : List Record) (st0 : State) : State :=
  rs.foldl
    (fun st rec =>
      match rec with
      | .write tx k v => if txs.contains tx then applyWrite st k v else st
      | _ => st)
    st0

def refState (log : List Record) : State :=
  foldWritesFor (commitsIn log) log (fun _ => none)

def minActiveWriteLsn (postCommitted : List Nat) (cpLsn : Nat) (log : List Record) : Nat :=
  log.zipIdx.foldl
    (fun acc (rec, idx) =>
      match rec with
      | .write tx _ _ => if postCommitted.contains tx then Nat.min acc idx else acc
      | _ => acc)
    cpLsn

def recover (log : List Record) (cpLsn : Nat) : State :=
  let cpCommitted := commitsIn (log.take cpLsn)
  let allCommitted := commitsIn log
  let postCommitted := allCommitted.filter (fun tx => !cpCommitted.contains tx)
  let tlcsSnap := foldWritesFor cpCommitted (log.take cpLsn) (fun _ => none)
  let startLsn := minActiveWriteLsn postCommitted cpLsn log
  foldWritesFor postCommitted (log.drop startLsn) tlcsSnap

def WritesBeforeCommit (log : List Record) : Prop :=
  ∀ (i j tx k : Nat) (v : Int),
    i < j → log[i]? = some (.commit tx) → log[j]? ≠ some (.write tx k v)

def SameKeyIntentOrder (log : List Record) (cpLsn : Nat) : Prop :=
  let cpCommitted := commitsIn (log.take cpLsn)
  let allCommitted := commitsIn log
  let postCommitted := allCommitted.filter (fun tx => !cpCommitted.contains tx)
  ∀ (i j tx1 tx2 k : Nat) (v1 v2 : Int),
    i < j →
    log[i]? = some (.write tx2 k v2) →
    log[j]? = some (.write tx1 k v1) →
    tx2 ∈ postCommitted →
    tx1 ∈ cpCommitted →
    False

/-- WAL checkpoint vs persistent TLCS boundary (`recovery/checkpoint_manager.cpp`,
    `recovery/recovery_manager.cpp`): under writes-before-commit and MVCC
    write-intent commit serialization, TLCS snapshot at `cpLsn` plus WAL replay
    from `minActiveWriteLsn` agrees with full WAL replay from LSN 0. -/
def Goal : Prop :=
  ∀ (log : List Record) (cpLsn : Nat) (k : Nat),
    cpLsn ≤ log.length →
    WritesBeforeCommit log →
    SameKeyIntentOrder log cpLsn →
    recover log cpLsn k = refState log k

def naiveRecoverFromCp (log : List Record) (cpLsn : Nat) : State :=
  let cpCommitted := commitsIn (log.take cpLsn)
  let allCommitted := commitsIn log
  let postCommitted := allCommitted.filter (fun tx => !cpCommitted.contains tx)
  let tlcsSnap := foldWritesFor cpCommitted (log.take cpLsn) (fun _ => none)
  foldWritesFor postCommitted (log.drop cpLsn) tlcsSnap

private def walStraddle : List Record :=
  [ .write 1 10 100
  , .commit 1
  , .write 2 20 200
  , .write 3 30 300
  , .commit 2
  ]

example :
    recover walStraddle 3 10 = refState walStraddle 10 ∧
    recover walStraddle 3 20 = refState walStraddle 20 ∧
    recover walStraddle 3 30 = refState walStraddle 30 := by
  decide

example :
    naiveRecoverFromCp walStraddle 3 20 = none ∧
    refState walStraddle 20 = some 200 := by
  decide

private def walBadIntent : List Record :=
  [ .write 2 10 111
  , .write 1 10 222
  , .commit 1
  , .commit 2
  ]

example :
    recover walBadIntent 3 10 = some 111 ∧
    refState walBadIntent 10 = some 222 := by
  decide

end TinylambOpt.ResearchGoals.WalCheckpointTlcsBoundary
