import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `RangeWidth.FallbackNecessary` — A rejected deletion has indistinguishable old
  summaries requiring different results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_FallbackNecessary

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

-- With xs = [0,1] and outgoing 0, the inverse is rejected, but every
-- length-two frame with width 1 is a pair of distinct constants, so erasing
-- the 0 always leaves a singleton (width 0).
theorem refute : ¬ RangeWidth.FallbackNecessary.Goal := by
  intro h
  obtain ⟨ys, hysmem, hyslen, hysval, hdiff⟩ := h [0, 1] 0 (by simp) (by decide)
  have hys : ys = [0, 1] ∨ ys = [1, 0] := by
    cases ys with
    | nil => simp at hyslen
    | cons a ys1 =>
        cases ys1 with
        | nil => simp at hyslen
        | cons b ys2 =>
            cases ys2 with
            | nil =>
                have hw : (max a b - min a b) = 1 := by
                  simpa [value, RangeWidth.model] using hysval
                have hcont : (a = 0 ∧ b = 1) ∨ (a = 1 ∧ b = 0) := by
                  have ha0b0 : a = 0 ∨ b = 0 := by
                    simpa [eq_comm] using hysmem
                  rcases ha0b0 with rfl | rfl
                  · left
                    constructor
                    · rfl
                    · omega
                  · right
                    constructor
                    · omega
                    · rfl
                rcases hcont with ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩
                · exact Or.inl rfl
                · exact Or.inr rfl
            | cons c ys3 => simp at hyslen
  rcases hys with rfl | rfl <;> simp at hdiff

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_RangeWidth_FallbackNecessary
