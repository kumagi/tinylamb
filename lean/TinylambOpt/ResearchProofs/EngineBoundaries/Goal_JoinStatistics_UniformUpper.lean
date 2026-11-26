import TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics

/-!
Join summaries and statistics must retain the multiplicity or bounds that the SQL
planner derives from them.

Refuted by counterexample

- `UniformUpper` — A proposed NDV-based estimate used as an upper bound for an inner
  join.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_UniformUpper

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

theorem refute : ¬ UniformUpper.Goal := by
  intro h
  have hh := h [0] [0,0,1]
  have hbad : ¬ fanout [0] [0,0,1] * max [0].eraseDups.length [0,0,1].eraseDups.length ≤
      [0].length * [0,0,1].length := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_UniformUpper
