import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Maximum.RepeatedMax` — `∀ x y, x ≤ y → repaired model [x,y,y] y = some y`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_RepeatedMax

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : Maximum.RepeatedMax.Goal := by
  intro x y hxy
  have hmax : [y, y].max?.getD 0 = y := by
    have hm : [y, y].max? = some y := (List.max?_eq_some_iff).2 ⟨by simp, by
      intro b hb
      simp at hb
      omega⟩
    simpa [hm]
  have hcond : ¬ y < max x y := by
    have hm : max x y = y := Nat.max_eq_right hxy
    rw [hm]
    omega
  have herase : ([x, y, y].erase y).max?.getD 0 = y := by
    by_cases hxy' : x == y
    · have hxyeq : x = y := by simpa using hxy'
      subst x
      simp
    · have hxyne : x ≠ y := by intro h; subst y; simp at hxy'
      have he : [x, y, y].erase y = [x, y] := by simp [hxyne]
      rw [he]
      have hm : [x, y].max? = some y := (List.max?_eq_some_iff).2 ⟨by simp, by
        intro b hb
        simp at hb
        rcases hb with rfl | rfl <;> omega⟩
      simpa [hm]
  simp [repaired, dropOne, value, Maximum.model, hmax, hcond, herase]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_RepeatedMax
