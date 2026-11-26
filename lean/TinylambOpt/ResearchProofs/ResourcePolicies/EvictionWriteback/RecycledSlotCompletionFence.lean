import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.ResourcePolicies.EvictionWriteback

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem recycledSlotCompletionFence : RecycledSlotCompletionFence.Goal := by
  intro s t wal h
  unfold finish
  have : (t.incarnation != s.live.incarnation) = true := by
    simp [bne_iff_ne, Ne.symm h]
  simp [this]
