import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_JoinFanout_SymmetricHotKeySalting
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.SymmetricHotKeySalting
theorem refute : ¬ Goal := by
  intro h
  have := h 1 2 2 (by decide)
  revert this
  native_decide
end Nov26.Proof_JoinFanout_SymmetricHotKeySalting
