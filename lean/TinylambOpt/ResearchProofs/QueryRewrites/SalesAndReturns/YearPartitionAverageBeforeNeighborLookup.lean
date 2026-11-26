import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# YearPartitionAverageBeforeNeighborLookup

Selecting the target-year window inputs separately preserves the average for that
partition.

The executor can aggregate the target year's rows and fetch neighboring months
separately for the subsequent lookup.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.YearPartitionAverageBeforeNeighborLookup
open TinylambOpt.ResearchGoals.QueryRewrites

theorem filter_of_implies {α : Type} (p q : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = true → q x = true) :
    (l.filter q).filter p = l.filter p := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro x hx
  by_cases hp : p x = true
  · have hq : q x = true := h x hx hp
    simp [hp, hq]
  · simp [hp]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.YearPartitionAverageBeforeNeighborLookup.Goal := by
  intro months y
  dsimp
  rw [filter_of_implies (fun r : (Int × Nat) × Option Int => r.1.1 == y)
    (fun r => r.1.1 == y || (r.1.1 == y-1 && r.1.2 == 12) || (r.1.1 == y+1 && r.1.2 == 1))
    months]
  intro r hr h
  simp [h]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.YearPartitionAverageBeforeNeighborLookup
