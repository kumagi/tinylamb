import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-!
Sorting work lengths by value does not minimize the sum of per-wave maxima when
a short packet can be paired with a long one. The permutation in this
counterexample finishes in one wave, while the sorted schedule takes two.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_SortedLengthWavesMinimizeTicks

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.SortedLengthWavesMinimizeTicks.Goal := by
  intro h
  have h' := h [0, 1, 1] [1, 1, 0] 2 (by omega) (by native_decide)
  have hne : ¬ (ticks 2 [0, 1, 1].mergeSort ≤ ticks 2 [1, 1, 0]) := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_SortedLengthWavesMinimizeTicks
