import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_ShardUnion

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.AtLeastOneRed

theorem refute : ¬ AtLeastOneRed.ShardUnion.Goal := by
  intro h
  let a : List Edge := [⟨0,0,1⟩]
  let b : List Edge := [⟨0,1,0⟩]
  have hi := h a b 0 2 1
  have hl : 1 ∈ query AtLeastOneRed.model (a++b) 0 2 id := by native_decide
  have hr : ¬ (1 ∈ query AtLeastOneRed.model a 0 2 id ∨ 1 ∈ query AtLeastOneRed.model b 0 2 id) := by
    native_decide
  exact hr (hi.mp hl)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_ShardUnion
