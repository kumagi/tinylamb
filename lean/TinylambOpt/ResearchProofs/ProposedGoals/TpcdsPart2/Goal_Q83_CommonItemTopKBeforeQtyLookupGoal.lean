import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Common-item top-k selection before quantity lookup (TPC-DS Q83)

The ordered top-k key set contains the requested item, so restricting store rows to that
key set first does not drop any row contributing to that item's quantity sum. The executor
may determine the top-k keys before loading return quantities.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q83_CommonItemTopKBeforeQtyLookupGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q83_CommonItemTopKBeforeQtyLookupGoal := by
  intro sRows cItems wItems k item
  dsimp only
  intro hmem
  let top := best k (members (members (sRows.map Prod.fst) cItems) wItems)
  have hmemTop : item ∈ top := by simpa [top] using hmem
  change total (sRows.filter (fun r => r.1 == item)) Prod.snd =
    total ((sRows.filter (fun r => top.contains r.1)).filter (fun r => r.1 == item)) Prod.snd
  rw [List.filter_filter]
  have hfilters : sRows.filter (fun r => r.1 == item) =
      sRows.filter (fun r => r.1 == item && top.contains r.1) := by
    apply List.filter_congr
    intro r hr
    by_cases hri : (r.1 == item) = true
    · have hri_eq : r.1 = item := eq_of_beq hri
      have hc : top.contains item = true := by
        simpa [List.contains_iff_mem] using hmemTop
      rw [hri_eq]
      simp [hri, hc, hmemTop]
    · simp [hri]
  rw [hfilters]

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q83_CommonItemTopKBeforeQtyLookupGoal
