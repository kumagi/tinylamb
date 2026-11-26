import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ChunkLocalBaseThenConcatenate

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-! Sorting two chunks separately and concatenating need not be sorted: a
later chunk can contain a key smaller than the last key of the first chunk. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.RadixCursors.ChunkLocalBaseThenConcatenate.Goal := by
  rw [ChunkLocalBaseThenConcatenate.Goal]
  intro h
  have hh := h [0,0,1] [0]
  have hbad : ¬ ([0,0,1].mergeSort ++ [0].mergeSort = ([0,0,1] ++ [0]).mergeSort) := by
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ChunkLocalBaseThenConcatenate
