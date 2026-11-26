import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Sorting two chunks independently and concatenating them does not produce a
globally sorted stream: a larger key in the first chunk precedes a smaller key
from the second. A merge pass remains necessary for ordered SQL output.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace Nov26.Proof_RadixCursors_ChunkLocalBaseThenConcatenate

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors
open TinylambOpt.ResearchGoals.JitKernels.RadixCursors.ChunkLocalBaseThenConcatenate

theorem refute : ¬ Goal := by
  intro h
  have got := h [2] [1]
  simp at got

end Nov26.Proof_RadixCursors_ChunkLocalBaseThenConcatenate
