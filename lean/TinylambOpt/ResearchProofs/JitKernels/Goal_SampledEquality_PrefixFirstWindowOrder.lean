import TinylambOpt.ResearchGoals.JitKernels.SampledEquality
set_option linter.unusedSimpArgs false

/-!
The sampled suffix is not the lexicographic suffix: unsampled bytes can sit
between the windows. A difference there decides the full-string order before
the appended sample windows are ever inspected.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_PrefixFirstWindowOrder

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality
open TinylambOpt.SimdGroupHash (byteSamples)

theorem refute : ¬ PrefixFirstWindowOrder.Goal := by
  intro h
  let a : List UInt8 := List.replicate 9 (0 : UInt8) ++ [2] ++ List.replicate 3 (0 : UInt8)
  let b : List UInt8 := List.replicate 8 (0 : UInt8) ++ [1, 1] ++ List.replicate 3 (0 : UInt8)
  have got := h a b
  simp [a, b, sig, byteSamples, win] at got
  have contra : ¬ ([0, 2, 0, 0, 0] < ([1, 1, 0, 0, 0] : List UInt8) ↔
      [2, 0, 0, 0] < ([1, 0, 0, 0] : List UInt8)) := by
    native_decide
  exact contra got

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_PrefixFirstWindowOrder
