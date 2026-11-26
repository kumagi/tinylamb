import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
A monotone radix stream may contain several distinct buckets. A live lane in a
later bucket has fewer same-bucket predecessors than total live predecessors, so
a single popcount of all preceding live lanes overestimates its destination rank.
This counterexample keeps the SQL scatter kernel from dropping the bucket filter.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace Nov26.Proof_RadixCursors_PrefixMaskAsBucketRank

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors
open TinylambOpt.ResearchGoals.JitKernels.RadixCursors.PrefixMaskAsBucketRank

theorem refute : ¬ Goal := by
  intro h
  have got := h [some 0, some 1] 1 1
  simp [active, rank] at got

end Nov26.Proof_RadixCursors_PrefixMaskAsBucketRank
