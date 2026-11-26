import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.SameSignReassociation.Goal`. -/

/-! Positive operands do not make addition associative: adding `2^-53` to `1`
rounds back to `1`, but adding the two tiny addends first gives `2^-52`. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_FloatingReduction_SameSignReassociation

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.SameSignReassociation

theorem refute : ¬ Goal := by
  intro h
  let t : Float := 1 / 9007199254740992
  have ht : 0 < t := by native_decide
  have h' := h (1 : Float) t t (by native_decide) (by native_decide) (by native_decide)
    (by native_decide) ht ht
  have hbad : ¬ same (((1 : Float) + t) + t) ((1 : Float) + (t + t)) := by
    unfold same bits
    native_decide
  exact hbad h'

end Nov26.Proof_FloatingReduction_SameSignReassociation
