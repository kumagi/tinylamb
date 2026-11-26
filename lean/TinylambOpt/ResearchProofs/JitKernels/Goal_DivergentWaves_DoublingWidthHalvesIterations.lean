import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_DoublingWidthHalvesIterations

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.DoublingWidthHalvesIterations.Goal := by
  intro h
  let steps : List Nat := [1, 100]
  have hbad : ticks (2 * 1) steps ≠ (ticks 1 steps + 1) / 2 := by
    native_decide
  exact hbad (h steps 1 (by decide))

end TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_DoublingWidthHalvesIterations
