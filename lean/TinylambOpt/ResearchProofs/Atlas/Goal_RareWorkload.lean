import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_RareWorkload

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `RiskEnvelope.RareWorkload` — `∀ xs extra z a b, (∀ y, loss xs z a b ≤ loss xs y a b)
  → ∀ y, loss (xs++extra) z a b ≤ loss (xs++extra) y a b + max a b * (z-y).natAbs *
  mass extra (fun _ => true)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RareWorkload

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

theorem sound_RareWorkload : RiskEnvelope.RareWorkload.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_RareWorkload.sound_RareWorkload

end TinylambOpt.ResearchProofs.Atlas.Goal_RareWorkload
