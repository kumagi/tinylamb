import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_OptimalTree

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.OptimalTree` — `∀ ws, ws ≠ [] → (∃ t, weights t = ws ∧ work t = best
  ws) ∧ ∀ t, weights t = ws → best ws ≤ work t`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_OptimalTree

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

theorem sound_OptimalTree : AlphabeticMerge.OptimalTree.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_OptimalTree.sound_OptimalTree

end TinylambOpt.ResearchProofs.Atlas.Goal_OptimalTree
