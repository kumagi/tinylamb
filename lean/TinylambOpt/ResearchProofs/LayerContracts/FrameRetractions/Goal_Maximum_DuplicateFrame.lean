import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Maximum.DuplicateFrame` — Can an executor collapse a duplicated whole peer frame
  before finalizing?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_DuplicateFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : Maximum.DuplicateFrame.Goal := by
  intro xs
  by_cases h : xs = []
  · subst xs
    simp [value]
  · have hp := (List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD h)
    have happ : xs ++ xs ≠ [] := by simp [h]
    have hm : (xs ++ xs).max? = some (xs.max?.getD 0) := by
      apply (List.max?_eq_some_iff).2
      constructor
      · simp [hp.1]
      · intro z hz
        simp at hz
        exact hp.2 z (by simpa using hz)
    simp [value, Maximum.model, h, happ, hm]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_DuplicateFrame
