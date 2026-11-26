import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Sorting two chunks independently and concatenating them does not produce a
globally sorted stream: a larger key in the first chunk precedes a smaller key
from the second. A merge pass remains necessary for ordered SQL output.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ChunkLocalBaseThenConcatenate

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem refute : ¬ ChunkLocalBaseThenConcatenate.Goal := by
  intro h
  have got := h [2] [1]
  simp at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ChunkLocalBaseThenConcatenate
