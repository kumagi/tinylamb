import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_LowerMedian

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_DuplicateFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.CommonLowerMedian
open List

theorem sound : LowerMedian.DuplicateFrame.Goal := by
  intro xs
  by_cases h : xs = []
  · subst xs
    simp [value, LowerMedian.model]
  · have hmed := lowerMedian_double h
    have hnonempty : xs ++ xs ≠ [] := by simp [h]
    simp [value, LowerMedian.model, h, hnonempty, hmed]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_LowerMedian_DuplicateFrame
