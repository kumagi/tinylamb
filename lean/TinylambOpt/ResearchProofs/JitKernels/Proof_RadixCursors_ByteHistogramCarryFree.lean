import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Independent byte-sized histograms can overflow when combined: two lanes each
with 200 rows of one bucket produce 400 rows, which does not fit in a byte.
The SQL kernel must therefore keep the carry instead of masking it away.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace Nov26.Proof_RadixCursors_ByteHistogramCarryFree

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors
open TinylambOpt.ResearchGoals.JitKernels.RadixCursors.ByteHistogramCarryFree

theorem refute : ¬ Goal := by
  intro h
  have got := h (List.replicate 200 (some 0)) (List.replicate 200 (some 0)) 0
  simp [active] at got

end Nov26.Proof_RadixCursors_ByteHistogramCarryFree
