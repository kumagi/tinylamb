import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

-- Means 0 (from [0]) and 0 (from [0,3]) combine to 0, but [0,0,3] has mean 1.
theorem refute : ¬ FloorMean.CombineFrames.Goal := by
  intro h
  have hh := h [0] [0, 3] (some 0) (by decide)
  have hbad : value FloorMean.model ([0] ++ [0, 3]) ≠ some 0 := by decide
  exact hbad hh

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_CombineFrames
