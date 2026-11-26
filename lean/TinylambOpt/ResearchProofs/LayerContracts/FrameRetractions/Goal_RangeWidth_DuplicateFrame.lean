import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `RangeWidth.DuplicateFrame` — Can an executor collapse a duplicated whole peer frame
  before finalizing?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_DuplicateFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : RangeWidth.DuplicateFrame.Goal := by
  intro xs
  by_cases h : xs = []
  · subst xs
    simp [value]
  have hp := (List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD h)
  have hq := (List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD h)
  have happ : xs ++ xs ≠ [] := by simp [h]
  have hmin : (xs ++ xs).min? = some (xs.min?.getD 0) := (List.min?_eq_some_iff).2 ⟨by
    simp [hp.1], by
    intro z hz
    simp at hz
    exact hp.2 z (by simpa using hz)⟩
  have hmax : (xs ++ xs).max? = some (xs.max?.getD 0) := (List.max?_eq_some_iff).2 ⟨by
    simp [hq.1], by
    intro z hz
    simp at hz
    exact hq.2 z (by simpa using hz)⟩
  simp [value, RangeWidth.model, h, happ, hmin, hmax]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_DuplicateFrame
