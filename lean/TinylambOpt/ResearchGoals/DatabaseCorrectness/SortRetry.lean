import Std

/-! A spill run contains ordered (key, row-id) pairs. Merge ties favor the older
run. An in-progress merge pass replaces a completed prefix but keeps an unmerged
suffix. A separate cursor model distinguishes row commit from chunk publication. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

abbrev Row := Nat × Nat
abbrev Run := List Row
def sorted (rs : Run) := rs.mergeSort (fun a b => decide (a.1 ≤ b.1))
def published (groups : List (List Run)) (rest : List Run) :=
  groups.map (fun g => sorted g.flatten) ++ rest

/-- A failed intermediate merge can restart from its already published prefix without losing or duplicating input occurrences. -/
def RetriedMergePassRetainsStableStream.Goal : Prop := ∀ groups rest,
  sorted (published groups rest).flatten = sorted (groups.flatten ++ rest).flatten

/-- The final stream preserves input order among equal keys across run boundaries, including duplicate row ids. -/
def SpillTiesPreserveInputOccurrenceOrder.Goal : Prop := ∀ groups rest k,
  ((sorted (published groups rest).flatten).filter (fun r => r.1 == k)) =
  ((groups.flatten++rest).flatten.filter (fun r => r.1 == k))

/-- LIMIT may discard every unread run only after the emitted prefix equals the corresponding in-memory top rows. -/
def MergeRetryLimitPrefix.Goal : Prop := ∀ groups rest already n,
  ((sorted (published groups rest).flatten).drop already).take n =
    ((sorted (groups.flatten++rest).flatten).drop already).take n

inductive Step where | ready | refillError | undoAllocationError
  deriving DecidableEq, Repr
structure Cursor where
  rows : Run
  emitted : Nat
  deriving DecidableEq, Repr
def next (s : Cursor) (step : Step) : Except Step (Option Row) × Cursor :=
  match s.rows, step with
  | [], _ => (.ok none,s)
  | r::rs, .ready => (.ok (some r),⟨rs,s.emitted+1⟩)
  | _::_, .refillError => (.error .refillError,s)
  | _::rs, .undoAllocationError => (.error .undoAllocationError,{s with rows := rs})

/-- A failed refill followed by a successful prepend leaves the next successful row and emitted count unchanged. -/
def RestoredWinnerRowRetry.Goal : Prop := ∀ s, next (next s .refillError).2 .ready = next s .ready

/-- Strong retry hypothesis: even failure to allocate the undo buffer leaves the next row recoverable. -/
def FailedUndoAllocationStillRetryable.Goal : Prop := ∀ s, next (next s .undoAllocationError).2 .ready = next s .ready

/-- A successful NextRow advances both the output count and the logical row sequence exactly once. -/
def SingleRowCommitConservation.Goal : Prop := ∀ s r after, next s .ready = (.ok (some r),after) →
  r::after.rows = s.rows ∧ after.emitted = s.emitted+1

/-- A chunk builder has already advanced k successful rows when a later NextRow fails.
Can a caller discard its partial output and retry from the returned cursor? -/
def DiscardPartialChunkThenRetry.Goal : Prop := ∀ (rs : Run) k n, k < n → k < rs.length →
  ((rs.drop k).take n) = rs.take n

/-- Preserving the partial chunk, and requesting only its remaining capacity, must exactly recover the original full chunk. -/
def RetainPartialChunkAcrossRowFault.Goal : Prop := ∀ (rs : Run) k n, k ≤ n →
  rs.take k ++ (rs.drop k).take (n-k) = rs.take n

#guard (next (next ⟨[(1,5),(1,6)],0⟩ .undoAllocationError).2 .ready).1.toOption == some (some (1,6))

end TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry
