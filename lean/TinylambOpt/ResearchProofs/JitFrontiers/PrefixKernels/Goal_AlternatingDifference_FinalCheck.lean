import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_FinalCheck

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels.AlternatingDifference

/-- Counterexample at width 8: the incoming state (0:Int) and the final state both
fit, but an interior prefix state overflows for [some 127, some 255, some 128], so one final check
cannot certify the block. -/
theorem refute : ¬ FinalCheck.Goal := by
  intro h
  have hk : (fits (w := (8:Nat)) (0:Int)) = true := by native_decide
  have hf : (fits (w := (8:Nat)) (apply (whole update [some 127, some 255, some 128]) (0:Int))) = true := by native_decide
  have h2 : (cert update (8:Nat) (0:Int) [some 127, some 255, some 128]) = false := by native_decide
  have hc := h 8 (0:Int) [some 127, some 255, some 128] hk hf
  rw [h2] at hc
  exact absurd hc Bool.false_ne_true

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_FinalCheck
