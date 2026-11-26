import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_ModuloCost_ShardUnion

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.ModuloCost

theorem refute : ¬ ModuloCost.ShardUnion.Goal := by
  intro h
  let a : List Edge := [⟨0,0,1⟩]
  let b : List Edge := [⟨0,1,1⟩]
  have hi := h a b 0 3 1
  have hl : 1 ∈ query ModuloCost.model (a++b) 0 3 id := by native_decide
  have hr : ¬ (1 ∈ query ModuloCost.model a 0 3 id ∨ 1 ∈ query ModuloCost.model b 0 3 id) := by
    native_decide
  exact hr (hi.mp hl)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_ModuloCost_ShardUnion
