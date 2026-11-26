import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_FinalCheck

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels.DifferenceFromBase

/-- Counterexample at width 8: the incoming state (30:Int) and the final state both
fit, but an interior prefix state overflows for [some 0, some 0, some 255], so one final check
cannot certify the block. -/
theorem refute : ¬ FinalCheck.Goal := by
  intro h
  have hk : (fits (w := (8:Nat)) (30:Int)) = true := by native_decide
  have hf : (fits (w := (8:Nat)) (apply (whole update [some 0, some 0, some 255]) (30:Int))) = true := by native_decide
  have h2 : (cert update (8:Nat) (30:Int) [some 0, some 0, some 255]) = false := by native_decide
  have hc := h 8 (30:Int) [some 0, some 0, some 255] hk hf
  rw [h2] at hc
  exact absurd hc Bool.false_ne_true

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_FinalCheck
