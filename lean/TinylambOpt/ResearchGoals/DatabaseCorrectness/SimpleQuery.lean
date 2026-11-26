import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-! Simple Query message boundaries. DML is modeled as a uniquely observable
row emission; COMMIT or ROLLBACK closes the current block. BEGIN adopts prior
implicit work in the same message. The wire model retains bytes belonging to
earlier messages when discarding a withheld reply after a durability failure. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow (Frames remaining)

inductive Mode where | idle | explicitTxn | failedTxn
  deriving Repr, DecidableEq
inductive Cmd where | write (row : Nat) | beginTxn | commit | rollback | fail
  deriving Repr, DecidableEq
structure Client where
  committedRows : List Nat
  staged : List Nat
  mode : Mode
  deriving DecidableEq, Repr
def clear (c : Client) := {c with staged := [], mode := .idle}
def flush (c : Client) := {clear c with committedRows := c.committedRows ++ c.staged}
def message (c : Client) : List Cmd → Client
  | [] => if c.mode == .idle then flush c else c
  | cmd::rest => match cmd with
    | .commit => message (if c.mode == .failedTxn then clear c else flush c) rest
    | .rollback => message (clear c) rest
    | .fail => if c.mode == .idle then clear c else {c with mode := .failedTxn}
    | .beginTxn => if c.mode == .failedTxn then c else message {c with mode := .explicitTxn} rest
    | .write r => if c.mode == .failedTxn then c else message {c with staged := c.staged++[r]} rest

def empty (rows : List Nat) : Client := ⟨rows,[],.idle⟩
def writes (xs : List Nat) := xs.map Cmd.write

/-- A late statement error discards the current implicit block, stops the message, and leaves a prior explicit COMMIT intact. -/
def ErrorAfterCommitRollsBackOnlyNewBlock.Goal : Prop := ∀ prior a b tail,
  message (empty prior) (writes a ++ [.commit] ++ writes b ++ .fail::tail) = empty (prior++a)

/-- BEGIN after an implicit write adopts it, allowing a later message to roll it back with the explicit block. -/
def BeginAdoptsImplicitMessagePrefix.Goal : Prop := ∀ prior a b,
  let c := message (empty prior) (writes a ++ [.beginTxn] ++ writes b)
  c = ⟨prior,a++b,.explicitTxn⟩ ∧ message c [.rollback] = empty prior

/-- End of a successful Simple Query message commits its implicit block independently of an error in the next message. -/
def ReadyBoundarySeparatesImplicitTransactions.Goal : Prop := ∀ prior a b,
  message (message (empty prior) (writes a)) (writes b ++ [.fail]) = empty (prior++a)

/-- An error inside an explicit transaction suppresses later commands of this message and cannot be turned into a commit by the next one. -/
def FailedExplicitCommitDiscardsStaging.Goal : Prop := ∀ prior a tail,
  let c := message ⟨prior,[],.explicitTxn⟩ (writes a ++ .fail::tail)
  c.mode = .failedTxn ∧ message c [.commit] = empty prior

/-- A rollback embedded in a multi-statement message permits a fresh implicit block for following writes. -/
def RollbackStartsFreshImplicitBlock.Goal : Prop := ∀ prior a b,
  message (empty prior) (writes a ++ [.rollback] ++ writes b) = empty (prior++b)

def discard (buf : List Nat) (sent earlierPending : Nat) :=
  let offset := min sent buf.length
  buf.take (offset + min (buf.length-offset) earlierPending)

/-- A durability failure removes only the withheld message even if earlier DataRows are partly sent. -/
def DiscardUnconfirmedReplyAtWireCut.Goal : Prop := ∀ (old hidden : Frames) n, n ≤ old.flatten.length →
  old.flatten.take n ++ (discard ((old++hidden).flatten) n (remaining n old).flatten.length).drop n = old.flatten

/-- Buffer compaction and cancellation of a withheld reply commute at any valid sent cursor. -/
def WireCompactionPreservesEarlierPending.Goal : Prop := ∀ buf n earlier, n ≤ buf.length →
  (discard buf n earlier).drop n = discard (buf.drop n) 0 earlier

#guard message (empty []) [.write 4,.commit,.write 5,.fail,.write 6] == empty [4]

end TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
