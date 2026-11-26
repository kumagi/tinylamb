import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Independent byte-sized histograms can overflow when combined: two lanes each
with 200 rows of one bucket produce 400 rows, which does not fit in a byte.
The SQL kernel must therefore keep the carry instead of masking it away.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ByteHistogramCarryFree

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem refute : ¬ ByteHistogramCarryFree.Goal := by
  intro h
  have got := h (List.replicate 200 (some 0)) (List.replicate 200 (some 0)) 0
  simp [active] at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ByteHistogramCarryFree
