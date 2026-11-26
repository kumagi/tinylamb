import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Maximum.DropUniqueMax` — `∀ x y, x < y → repaired model [x,y] y = some x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_DropUniqueMax

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : Maximum.DropUniqueMax.Goal := by
  intro x y hxy
  have hmaxxy : [x, y].max?.getD 0 = y := by
    have hm : [x, y].max? = some y := (List.max?_eq_some_iff).2 ⟨by simp, by
      intro b hb
      simp at hb
      rcases hb with rfl | rfl <;> omega⟩
    simpa [hm]
  have hmax2 : max x y = y := Nat.max_eq_right (Nat.le_of_lt hxy)
  have hcond : ¬ y < max x y := by rw [hmax2]; omega
  have herase : ([x, y].erase y).max?.getD 0 = x := by
    have hne : x ≠ y := by omega
    have he : [x, y].erase y = [x] := by simp [hne]
    rw [he]
    simp
  simp [repaired, dropOne, value, Maximum.model, hmaxxy, hcond, herase]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_DropUniqueMax
