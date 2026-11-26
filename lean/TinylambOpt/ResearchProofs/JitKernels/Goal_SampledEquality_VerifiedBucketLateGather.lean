import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
Exact verification after a sampled bucket hit changes only the filter inside
that bucket: every exact hit necessarily carries the probe's signature. This
justifies a late gather of verified rows for SQL grouping and joins.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_VerifiedBucketLateGather

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

theorem sound : VerifiedBucketLateGather.Goal := by
  intro pool probe
  rw [List.filter_filter]
  apply List.filter_congr
  intro s hs
  by_cases same : s = probe
  · simp [same]
  · simp [same]

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_VerifiedBucketLateGather
