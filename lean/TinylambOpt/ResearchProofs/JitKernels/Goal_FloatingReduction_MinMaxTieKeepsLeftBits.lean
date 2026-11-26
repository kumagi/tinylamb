import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! IEEE `≤` treats `+0.0` and `-0.0` as equal but `<` does not, so the
strict-select variant returns the right-hand (negative zero) bit pattern. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MinMaxTieKeepsLeftBits

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ MinMaxTieKeepsLeftBits.Goal := by
  intro h
  have h' := h (0 : Float) (Float.ofBits 0x8000000000000000)
    (by native_decide) (by native_decide)
  have hbad : ¬ same
      (if (0 : Float) ≤ Float.ofBits 0x8000000000000000 then 0 else Float.ofBits 0x8000000000000000)
      (if (0 : Float) < Float.ofBits 0x8000000000000000 then 0 else Float.ofBits 0x8000000000000000) := by
    unfold same bits
    native_decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MinMaxTieKeepsLeftBits
