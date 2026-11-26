import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_PinDuringWriteback
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem sound : PinDuringWriteback.Goal := by
  intro s t wal h
  simp only [reclaimable, finish]
  split <;> simp <;> omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_PinDuringWriteback
