import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

-- Adding the distinct counts of two overlapping frames double-counts [0].
theorem refute : ¬ DistinctCount.CombineFrames.Goal := by
  intro h
  have hh := h [0] [0] (some 2) (by decide)
  have hbad : value DistinctCount.model ([0] ++ [0]) ≠ some 2 := by decide
  exact hbad hh

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_DistinctCount_CombineFrames
