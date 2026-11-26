import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! REFUTED: Stable-sorting by actual work length does not minimize wave ticks over all
permutations. Sorting can place the medium items early and strand the long outlier in a
tail wave of its own, while an order that pairs the outlier with medium work in one wave
finishes earlier overall. Counterexample: `steps = [0, 1, 1]` at width `2`: the sorted
order gives waves `[1, 1]` (2 ticks), but `[1, 1, 0]` gives waves `[1, 0]` (1 tick). -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_SortedLengthWavesMinimizeTicks
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-- The sorted order needs 2 ticks while the permutation `[1, 1, 0]` needs 1. -/
theorem refute : ¬ SortedLengthWavesMinimizeTicks.Goal := by
  intro h
  have hms : List.mergeSort ([0, 1, 1] : List Nat) = [0, 1, 1] := by simp [List.mergeSort]
  have hc := h [0, 1, 1] [1, 1, 0] 2 (by decide) (by decide)
  rw [hms] at hc
  exact absurd hc (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_SortedLengthWavesMinimizeTicks
