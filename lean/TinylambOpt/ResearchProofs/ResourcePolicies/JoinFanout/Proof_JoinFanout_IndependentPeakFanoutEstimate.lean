import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_JoinFanout_IndependentPeakFanoutEstimate
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.IndependentPeakFanoutEstimate
theorem refute : ¬ Goal := by
  intro h
  let a : Keys := [some 1, some 1, some 2]
  let b : Keys := [some 1, some 3, some 3]
  have hpos : 0 < product a b := by native_decide
  have := h a b hpos
  revert this
  native_decide
end Nov26.Proof_JoinFanout_IndependentPeakFanoutEstimate
