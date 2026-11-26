import TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics

/-!
Join summaries and statistics must retain the multiplicity or bounds that the SQL
planner derives from them.

Proved

- `OuterCorrection` — For a LEFT JOIN, add unmatched left rows once, including
  duplicate preserved keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_OuterCorrection

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

theorem sound : OuterCorrection.Goal := by
  intro a b
  induction a with
  | nil => simp [outerSize, fanout]
  | cons k xs ih =>
      unfold outerSize at ih ⊢
      simp only [List.map_cons, List.sum_cons] at ih ⊢
      by_cases hmem : k ∈ b
      · have hbpos : b.count k ≠ 0 := by
          intro hz
          exact (List.count_eq_zero.mp hz) hmem
        have hmax : max 1 (b.count k) = b.count k := by omega
        simp [fanout, hmem, hmax, ih]
        omega
      · have hb : b.count k = 0 := List.count_eq_zero.mpr hmem
        simp [fanout, hmem, hb, ih]
        omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_JoinStatistics_OuterCorrection
