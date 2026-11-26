import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# CorrelatedPositiveCountAsMembership

A positive correlated count exists exactly for keys present among qualifying inner rows.

The executor can use a membership structure for eligible manufacturers instead of
counting inner matches per outer row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CorrelatedPositiveCountAsMembership
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites

theorem countWhere_pos {α : Type} (xs : List α) (p : α → Bool) :
  0 < countWhere xs p ↔ ∃ x ∈ xs, p x = true := by
  unfold countWhere
  induction xs with
  | nil => simp
  | cons x xs ih =>
    by_cases hx : p x = true
    · simp [hx]
    · simp [hx, ih]

theorem count_hit_pos (k : Option String) (items : List (Option String × Bool)) :
  k.any ((items.filter Prod.snd).filterMap Prod.fst).eraseDups.contains = true ↔
    0 < countWhere items (fun r => hit k r.1 && r.2) := by
  cases k with
  | none =>
      simp [countWhere, hit]
  | some s =>
      rw [Option.any_some]
      simp only [List.contains_eq_mem, decide_eq_true_eq, List.mem_eraseDups,
        List.mem_filterMap, List.mem_filter]
      rw [countWhere_pos]
      simp [hit, beq_iff_eq, and_left_comm, and_assoc, and_comm]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.CorrelatedPositiveCountAsMembership.Goal := by
  intro outerKeys items
  dsimp
  apply List.filter_congr
  intro k hk
  exact Bool.eq_iff_iff.mpr (by simpa [decide_eq_true_eq] using count_hit_pos k items)
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CorrelatedPositiveCountAsMembership
