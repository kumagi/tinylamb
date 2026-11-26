import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `RangeWidth.AlwaysInvertible` — Does a finalized scalar and non-NULL count determine
  every single-row retraction?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_AlwaysInvertible

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

-- Both frames have width 1, but erasing 0 leaves widths 1 and 0.
theorem refute : ¬ RangeWidth.AlwaysInvertible.Goal := by
  apply alwaysInvertible_refute [0, 0, 1] [0, 1, 1] 0
  · simp
  · decide
  · simp
  · simp
  · decide

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_AlwaysInvertible
