import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Sum.FastRetraction` — Every accepted constant-state deletion agrees with recomputing
  the remaining bag.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_FastRetraction

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Sum.FastRetraction.Goal := by
  apply fastRetraction_of_remove
  intro xs x hx hgt
  simp [Sum.model, sum_erase_of_mem hx]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_FastRetraction
