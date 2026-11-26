import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Refuted by counterexample

- `SignedSum.FinalCheck` — Aggressive proposal: replace every prefix guard by one check
  on the final state.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_FinalCheck

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels.SignedSum

/-- Counterexample at width 8: the incoming state (127:Int) and the final state both
fit, but an interior prefix state overflows for [some 1, some (-1)], so one final check
cannot certify the block. -/
theorem refute : ¬ FinalCheck.Goal := by
  intro h
  have hk : (fits (w := (8:Nat)) (127:Int)) = true := by native_decide
  have hf : (fits (w := (8:Nat)) (apply (whole update [some 1, some (-1)]) (127:Int))) = true := by native_decide
  have h2 : (cert update (8:Nat) (127:Int) [some 1, some (-1)]) = false := by native_decide
  have hc := h 8 (127:Int) [some 1, some (-1)] hk hf
  rw [h2] at hc
  exact absurd hc Bool.false_ne_true

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedSum_FinalCheck
