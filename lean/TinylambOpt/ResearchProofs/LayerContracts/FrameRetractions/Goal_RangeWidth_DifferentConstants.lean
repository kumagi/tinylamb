import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_DifferentConstants

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : RangeWidth.DifferentConstants.Goal := by
  intro x y hxy
  have hmin : [x, y].min?.getD 0 = x := by
    have hm : [x, y].min? = some x := (List.min?_eq_some_iff).2 ⟨by simp, by
      intro b hb
      simp at hb
      rcases hb with rfl | rfl <;> omega⟩
    simpa [hm]
  have hmax : [x, y].max?.getD 0 = y := by
    have hm : [x, y].max? = some y := (List.max?_eq_some_iff).2 ⟨by simp, by
      intro b hb
      simp at hb
      rcases hb with rfl | rfl <;> omega⟩
    simpa [hm]
  have hmin2 : min x y = x := Nat.min_eq_left (Nat.le_of_lt hxy)
  have hmax2 : max x y = y := Nat.max_eq_right (Nat.le_of_lt hxy)
  simp [value, RangeWidth.model, hmin, hmax, hmin2, hmax2]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_DifferentConstants
