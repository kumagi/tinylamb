import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_SingleRed_PlainDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.SingleRed

theorem refute : ¬ SingleRed.PlainDepth.Goal := by
  intro h
  let g : List Edge := [⟨0,1,0⟩, ⟨1,0,1⟩]
  have hi := h g 0 3 1
  have ha : 1 ∈ query SingleRed.model g 0 3 id := by native_decide
  have hb : ¬ (1 ∈ query SingleRed.model g 0 (2 + 0) id) := by native_decide
  have hb' : ¬ (1 ∈ query SingleRed.model g 0 2 id) := by native_decide
  exact hb' (hi ha)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_SingleRed_PlainDepth
