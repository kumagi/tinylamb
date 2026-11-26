import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_VertexOnly

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.AtLeastOneRed

theorem refute : ¬ AtLeastOneRed.VertexOnly.Goal := by
  intro h
  let g : List Edge := [⟨0,0,0⟩, ⟨0,0,1⟩]
  have hnot : ¬ (0 ∈ query AtLeastOneRed.model g 0 1 (fun p => (p.1,0)) ↔
                 0 ∈ query AtLeastOneRed.model g 0 1 id) := by
    native_decide
  exact hnot (h g 0 1 0)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_VertexOnly
