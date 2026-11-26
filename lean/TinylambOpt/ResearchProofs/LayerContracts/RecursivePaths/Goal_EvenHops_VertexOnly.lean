import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_EvenHops_VertexOnly

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.EvenHops

theorem refute : ¬ EvenHops.VertexOnly.Goal := by
  intro h
  let g : List Edge := [⟨0,0,0⟩, ⟨0,1,0⟩]
  have hnot : ¬ (1 ∈ query EvenHops.model g 0 2 (fun p => (p.1,0)) ↔
                 1 ∈ query EvenHops.model g 0 2 id) := by
    native_decide
  exact hnot (h g 0 2 1)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_EvenHops_VertexOnly
