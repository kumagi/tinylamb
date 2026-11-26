import TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_NullSafeIncrement

open TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics
open TinylambOpt.ResearchGoals.SearchBounds (fanout)

private theorem count_filterMap_some (b : List (Option Nat)) (k : Nat) :
    (b.filterMap id).count k = b.count (some k) := by
  induction b with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simp [ih]
      | some v =>
          by_cases hv : v = k
          · subst v
            simp [ih]
          · have hbe : (v == k) = false := beq_eq_false_iff_ne.mpr hv
            simp [List.count_cons, ih, hbe, hv]

theorem sound : NullSafeIncrement.Goal := by
  intro a b
  induction a with
  | nil => simp [fanout]
  | cons x xs ih =>
      cases x with
      | none =>
          simp [fanout, ih, Nat.add_mul, Nat.one_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      | some k =>
          simp [fanout, ih, count_filterMap_some, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_NullSafeIncrement
