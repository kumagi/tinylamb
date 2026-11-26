import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_VerifiedBucketLateGather

theorem sound : TinylambOpt.ResearchGoals.JitKernels.SampledEquality.VerifiedBucketLateGather.Goal := by
  intro pool probe
  rw [List.filter_filter]
  apply List.filter_congr
  intro s hs
  by_cases h : s == probe
  · have hsp : s = probe := beq_iff_eq.mp h
    subst s
    simp
  · simp [h]

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_VerifiedBucketLateGather
