import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_TwoEvictions

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem refute : ¬ FloorMean.TwoEvictions.Goal := by
  intro h
  have hh := h [0, 1, 1] 0 1 (by simp) (by simp)
  have hbad : repaired FloorMean.model ([0, 1, 1].erase 0) 1 ≠
      repaired FloorMean.model ([0, 1, 1].erase 1) 0 := by decide
  exact hbad hh

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_TwoEvictions
