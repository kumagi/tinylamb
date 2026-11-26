import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `FloorMean.FastRetraction` — Every accepted constant-state deletion agrees with
  recomputing the remaining bag.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

-- [0,1,1] has mean 0; retracting 0 via (3*0-0)/2 = 0 loses the remainder,
-- while the remaining frame [1,1] has mean 1.
theorem refute : ¬ FloorMean.FastRetraction.Goal := by
  intro h
  have hh := h [0, 1, 1] 0 (some 0) (by simp) (by decide)
  have hbad : value FloorMean.model ([0, 1, 1].erase 0) ≠ some 0 := by decide
  exact hbad hh

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_FastRetraction
