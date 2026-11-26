import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_ShardUnion

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.PatternAB

theorem refute : ¬ PatternAB.ShardUnion.Goal := by
  intro h
  let a : List Edge := [⟨0,0,1⟩]
  let b : List Edge := [⟨0,0,2⟩]
  have hi := h a b 0 2 0
  have hl : 0 ∈ query PatternAB.model (a++b) 0 2 id := by native_decide
  have hr : ¬ (0 ∈ query PatternAB.model a 0 2 id ∨ 0 ∈ query PatternAB.model b 0 2 id) := by
    native_decide
  exact hr (hi.mp hl)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_ShardUnion
