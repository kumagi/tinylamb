import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_SameSignReassociation

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! Two positive tiny addends sum to a representable value, but each one is
rounded away when added alone to the large accumulator. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.SameSignReassociation.Goal := by
  rw [SameSignReassociation.Goal]
  intro h
  let t : Float := 1 / 9007199254740992
  have ht : 0 < t := by native_decide
  have hh := h (1 : Float) t t (by native_decide) (by native_decide) (by native_decide)
    (by native_decide) ht ht
  have hbad : ¬ same (((1 : Float) + t) + t) ((1 : Float) + (t + t)) := by
    unfold same bits
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_SameSignReassociation
