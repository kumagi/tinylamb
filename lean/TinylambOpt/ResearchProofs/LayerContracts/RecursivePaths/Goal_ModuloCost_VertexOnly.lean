import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_ModuloCost_VertexOnly

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.ModuloCost

theorem refute : ¬ ModuloCost.VertexOnly.Goal := by
  intro h
  let g : List Edge := [⟨0,0,1⟩, ⟨0,1,1⟩]
  have hnot : ¬ (1 ∈ query ModuloCost.model g 0 3 (fun p => (p.1,0)) ↔
                 1 ∈ query ModuloCost.model g 0 3 id) := by
    native_decide
  exact hnot (h g 0 3 1)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_ModuloCost_VertexOnly
