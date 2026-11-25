import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_OverlapDebt

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.IntervalDeletes

/-! `live n (a ++ b)` deletes the union of two interval logs.  Since `blocked` on the
concatenation is the disjunction of the two blockers, the surviving rows are exactly
the rows surviving `a` that are additionally not blocked by `b`; splitting a filtered
list into the `q` and `!q` parts gives the counting identity. -/

private theorem filter_and_not_count (xs : List Nat) (p q : Nat → Bool) :
    (xs.filter (fun x => p x && !q x)).length + (xs.filter p).countP q =
      (xs.filter p).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases hp : p x <;> cases hq : q x <;> simp [hp, hq, ih] <;> omega

private theorem blocked_append (a b : List Span) (i : Nat) :
    blocked (a ++ b) i = (blocked a i || blocked b i) := by
  simp [blocked]

theorem sound_IntervalDeletes_OverlapDebt : OverlapDebt.Goal := by
  intro n a b
  unfold live
  have hba : ∀ i, blocked (a ++ b) i = (blocked a i || blocked b i) := blocked_append a b
  simpa [hba] using
    (filter_and_not_count (List.range n) (fun i => !blocked a i) (fun i => blocked b i))

end TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_OverlapDebt
