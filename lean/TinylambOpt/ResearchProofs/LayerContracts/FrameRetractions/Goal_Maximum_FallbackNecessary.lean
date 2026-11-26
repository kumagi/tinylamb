import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `Maximum.FallbackNecessary` — A rejected deletion has indistinguishable old summaries
  requiring different results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_FallbackNecessary

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

-- The frame [0,0] has maximum 0; the inverse is rejected, but every
-- length-two frame with maximum 0 is [0,0], so no distinguishable peer exists.
theorem refute : ¬ Maximum.FallbackNecessary.Goal := by
  intro h
  obtain ⟨ys, hysmem, hyslen, hysval, hdiff⟩ := h [0, 0] 0 (by simp) (by decide)
  have hys : ys = [0, 0] := by
    cases ys with
    | nil => simp at hyslen
    | cons a ys1 =>
        cases ys1 with
        | nil => simp at hyslen
        | cons b ys2 =>
            cases ys2 with
            | nil =>
                have hm : max a b = 0 := by simpa [value, Maximum.model] using hysval
                have ha : a = 0 := by have := Nat.le_max_left a b; omega
                have hb : b = 0 := by have := Nat.le_max_right a b; omega
                subst a; subst b
                rfl
            | cons c ys3 =>
                simp at hyslen
  simp [hys] at hdiff

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Maximum_FallbackNecessary
