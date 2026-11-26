import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_CombineFrames

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

-- [0,0] and [1,1] both have width 0, but their concatenation has width 1.
theorem refute : ¬ RangeWidth.CombineFrames.Goal := by
  intro h
  have hh := h [0, 0] [1, 1] (some 0) (by decide)
  have hbad : value RangeWidth.model ([0, 0] ++ [1, 1]) ≠ some 0 := by decide
  exact hbad hh

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_CombineFrames
