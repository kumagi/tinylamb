import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_EvictionWriteback_FlushRacingMutation
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem sound : FlushRacingMutation.Goal := by
  intro s lsn data durable
  unfold reclaimable finish mutate
  simp only
  split
  · simp
  · simp_all
    intro _ heq
    have := congrArg Image.revision heq
    simp at this
end Nov26.Proof_EvictionWriteback_FlushRacingMutation
