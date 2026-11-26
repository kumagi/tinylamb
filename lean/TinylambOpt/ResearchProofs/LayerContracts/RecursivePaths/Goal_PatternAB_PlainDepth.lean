import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_PlainDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.PatternAB

theorem refute : ¬ PatternAB.PlainDepth.Goal := by
  intro h
  let g : List Edge := [⟨0,0,1⟩, ⟨0,0,2⟩]
  have hi := h g 0 2 0
  have ha : 0 ∈ query PatternAB.model g 0 2 id := by native_decide
  have hb : ¬ (0 ∈ query PatternAB.model g 0 (1 + 0) id) := by native_decide
  have hb' : ¬ (0 ∈ query PatternAB.model g 0 1 id) := by native_decide
  exact hb' (hi ha)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_PatternAB_PlainDepth
