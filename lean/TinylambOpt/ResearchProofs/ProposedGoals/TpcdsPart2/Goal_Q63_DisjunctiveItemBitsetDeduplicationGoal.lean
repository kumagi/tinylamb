import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Disjunctive item bitset partition (TPC-DS Q63)

The `p1 || p2` branch and the disjoint remainder `!p1 && p2` partition the qualifying
rows, hence their exact widened sums add to the unpartitioned sum. The executor may test
one dense bitset per row and split overlapping branch contributions without duplication.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q63_DisjunctiveItemBitsetDeduplicationGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

/-- A disjunctive sum is the first branch plus the part of the second branch outside it. -/
theorem total_or_partition {α : Type} (rows : List α) (p q : α → Bool) (f : α → Int) :
    total (rows.filter (fun r => p r || q r)) f =
      total (rows.filter p) f + total (rows.filter (fun r => (!p r) && q r)) f := by
  induction rows with
  | nil => simp [total]
  | cons r rows ih =>
    simp only [List.filter_cons]
    by_cases hp : p r = true <;> by_cases hq : q r = true <;>
      simp [total, hp, hq] at ih ⊢ <;> omega

theorem sound : Q63_DisjunctiveItemBitsetDeduplicationGoal := by
  intro sales p1 p2
  exact total_or_partition sales (fun r => p1 r.1) (fun r => p2 r.1) Prod.snd

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q63_DisjunctiveItemBitsetDeduplicationGoal
