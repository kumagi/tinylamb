import TinylambOpt.ResearchGoals.Atlas.PlanSearch
import TinylambOpt.ResearchProofs.Atlas.PlanSearch_Feasible

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `ParallelCuts.Feasible` — `∀ cap xs k, (∀ w ∈ xs, w ≤ cap) → (required cap xs ≤ k ↔ ∃
  blocks : List (List Nat), blocks.flatten = xs ∧ blocks.length ≤ k ∧ peak blocks ≤
  cap)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Feasible

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

theorem sound_Feasible : ParallelCuts.Feasible.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.PlanSearch_Feasible.sound_Feasible

end TinylambOpt.ResearchProofs.Atlas.Goal_Feasible
