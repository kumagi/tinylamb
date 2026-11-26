import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_EvictionWriteback_RecycledSlotCompletionFence
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem sound : RecycledSlotCompletionFence.Goal := by
  intro s t wal h
  unfold finish
  have hne : (t.incarnation != s.live.incarnation) = true := by
    rw [bne_iff_ne]
    exact Ne.symm h
  simp [hne]
end Nov26.Proof_EvictionWriteback_RecycledSlotCompletionFence
