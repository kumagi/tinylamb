import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-! Independent proof record for the InductionLowering.FinalIterationStrengthOverflow research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
Range checks on emitted quadratic values do not bound the value one iteration
past the last emitted row. The identity induction `f(x) = x` on `x = 0 .. 7`
fits a 3-bit signed range, but the next emitted value `8` does not.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_InductionLowering_FinalIterationStrengthOverflow
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering.FinalIterationStrengthOverflow
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ Goal := by
  intro h
  have hfit : ∀ x ∈ differences 0 1 0 0 1 8, signedFits 3 x := by
    intro x hx
    have hdiff : differences 0 1 0 0 1 8 = ([0, 1, 2, 3, 4, 5, 6, 7] : List Int) := by
      decide
    rw [hdiff] at hx
    simp at hx
    simp [signedFits]
    omega
  have t := h 0 1 0 3 0 1 8 (by omega) hfit
  have hbad : ¬ signedFits 3 (poly 0 1 0 (0 + 1 * (8 : Nat))) := by
    simp [signedFits, poly]
  exact hbad t

end Nov26.Proof_InductionLowering_FinalIterationStrengthOverflow
