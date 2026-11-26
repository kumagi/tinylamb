import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-!
For a nonempty digit string the negative branch of the parser accepts exactly
the magnitudes at most `2^w`: with `v = -magnitude s` the bounds
`-(2^w) ≤ v ∧ v < 2^w` reduce (the upper bound is automatic) to the unsigned
test `magnitude s ≤ 2^w`. Comparing the unsigned magnitude directly against
the signed negative limit therefore needs no extra endpoint check:
`(parse w true s).isSome = decide (magnitude s ≤ 2^w)`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_NegativeAccumulatorThreshold
open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem sound : NegativeAccumulatorThreshold.Goal := by
  intro w s h hs
  have hpow : ((2^w : Nat) : Int) = (2 : Int)^w := by simp
  have h1 : (1 : Int) ≤ (2 : Int)^w := by
    have hp' : (1 : Nat) ≤ 2^w := Nat.one_le_pow _ _ (by decide)
    have h2 : (1 : Int) ≤ ((2^w : Nat) : Int) := by omega
    rwa [hpow] at h2
  unfold parse
  have hi : List.isEmpty s = false := by
    cases s with
    | nil => exact absurd rfl hs
    | cons a t => simp
  by_cases hm : magnitude s ≤ 2^w
  · have hp1 : (-(2^w : Int) ≤ (-(magnitude s : Int))) := by
      have hmid : (magnitude s : Int) ≤ (2 : Int)^w := by rw [← hpow]; omega
      omega
    have hp2 : ((-(magnitude s : Int)) < (2^w : Int)) := by omega
    simp [hp1, hp2, hm, hi, h, hs]
  · have hp1 : ¬ (-(2^w : Int) ≤ (-(magnitude s : Int))) := by
      intro hh
      have hmid : (magnitude s : Int) ≤ (2 : Int)^w := by omega
      rw [← hpow] at hmid
      omega
    have hp2 : ((-(magnitude s : Int)) < (2^w : Int)) := by omega
    simp [hp1, hp2, hm, hi, h, hs]

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks_NegativeAccumulatorThreshold
