import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CheckpointRetention_WholeSegmentReplayCoverage
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
theorem sound : WholeSegmentReplayCoverage.Goal := by
  intro segs cp pins r hfloor
  simp only [retained, List.filter_filter]
  congr 1
  funext s
  simp only [intersects, Bool.decide_and, Bool.decide_eq_true, Bool.and_eq_true, decide_eq_true_eq]
  by_cases hi : r.lo < s.hi ∧ s.lo < r.hi
  · simp only [hi, and_true, true_and]
    exact decide_eq_true_eq.mpr (Nat.lt_of_le_of_lt hfloor hi.1)
  · simp [hi]
end Nov26.Proof_CheckpointRetention_WholeSegmentReplayCoverage
