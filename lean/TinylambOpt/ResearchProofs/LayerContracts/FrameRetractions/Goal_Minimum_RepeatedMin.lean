import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_RepeatedMin

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : Minimum.RepeatedMin.Goal := by
  intro x y hxy
  have hminxy : [x, y].min?.getD 0 = x := by
    have hm : [x, y].min? = some x := (List.min?_eq_some_iff).2 ⟨by simp, by
      intro b hb
      simp at hb
      rcases hb with rfl | rfl <;> omega⟩
    simpa [hm]
  have hnot : ¬ min x y < x := by have hle := Nat.min_le_left x y; omega
  have hmin2 : min x y = x := Nat.min_eq_left hxy
  simp [repaired, dropOne, value, Minimum.model, hminxy, hnot, hmin2]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Minimum_RepeatedMin
