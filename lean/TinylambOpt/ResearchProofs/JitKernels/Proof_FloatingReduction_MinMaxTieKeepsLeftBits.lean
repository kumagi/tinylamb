import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.MinMaxTieKeepsLeftBits.Goal`. -/

/-! IEEE `≤` treats `+0.0` and `-0.0` as equal but `<` does not, so the
strict-select variant returns the right-hand (negative zero) bit pattern. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_FloatingReduction_MinMaxTieKeepsLeftBits

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.MinMaxTieKeepsLeftBits

theorem refute : ¬ Goal := by
  intro h
  have h' := h (0 : Float) (Float.ofBits 0x8000000000000000)
    (by native_decide) (by native_decide)
  have hbad : ¬ same
      (if (0 : Float) ≤ Float.ofBits 0x8000000000000000 then 0 else Float.ofBits 0x8000000000000000)
      (if (0 : Float) < Float.ofBits 0x8000000000000000 then 0 else Float.ofBits 0x8000000000000000) := by
    unfold same bits
    native_decide
  exact hbad h'

end Nov26.Proof_FloatingReduction_MinMaxTieKeepsLeftBits
