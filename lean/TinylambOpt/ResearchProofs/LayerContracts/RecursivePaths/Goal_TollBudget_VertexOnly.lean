import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_VertexOnly

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.TollBudget

theorem refute : ¬ TollBudget.VertexOnly.Goal := by
  intro h
  let g : List Edge := [⟨0,1,1⟩, ⟨0,1,0⟩, ⟨1,2,3⟩]
  have hnot : ¬ (2 ∈ query TollBudget.model g 0 2 (fun p => (p.1,0)) ↔
                 2 ∈ query TollBudget.model g 0 2 id) := by
    native_decide
  exact hnot (h g 0 2 2)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_VertexOnly
