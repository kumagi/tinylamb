import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
A monotone radix stream may contain several distinct buckets. A live lane in a
later bucket has fewer same-bucket predecessors than total live predecessors, so
a single popcount of all preceding live lanes overestimates its destination rank.
This counterexample keeps the SQL scatter kernel from dropping the bucket filter.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_PrefixMaskAsBucketRank

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem refute : ¬ PrefixMaskAsBucketRank.Goal := by
  intro h
  have got := h [some 0, some 1] 1 1
  simp [active, rank] at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_PrefixMaskAsBucketRank
