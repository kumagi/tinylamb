import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: sorting lanes by work length does not always minimize vector
iterations. A short trailing lane occupies its own partial packet; moving it to
the front pushes a longer lane into the tail packet, increasing total ticks. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_SortedLengthWavesMinimizeTicks

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

theorem refute : ¬ SortedLengthWavesMinimizeTicks.Goal := by
  intro h
  have perm : List.Perm [0, 1, 1] [1, 1, 0] := by decide
  have hbad := h [0, 1, 1] [1, 1, 0] 2 (by omega) perm
  have hc : ¬ ticks 2 ([0, 1, 1].mergeSort) ≤ ticks 2 [1, 1, 0] := by native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_SortedLengthWavesMinimizeTicks
