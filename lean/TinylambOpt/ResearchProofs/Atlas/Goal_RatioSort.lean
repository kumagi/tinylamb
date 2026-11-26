import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_RatioSort

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `PredicateOrder.RatioSort` — `∀ d ts alt, 0 < d → valid d ts → (∀ t ∈ ts, 0 < t.price
  ∨ t.pass < d) → ts.Perm alt → bill d (ts.mergeSort (byRatio d)) ≤ bill d alt`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RatioSort

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder

theorem sound_RatioSort : PredicateOrder.RatioSort.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_RatioSort.sound_RatioSort

end TinylambOpt.ResearchProofs.Atlas.Goal_RatioSort
