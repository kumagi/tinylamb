import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.ResourcePolicies.EvictionWriteback.Goal_RecycledSlotCompletionFence

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
A ticket from a previous incarnation of a physical slot is fenced out by the
incarnation check: its completion leaves the slot untouched. Thus recycling a
page slot cannot be corrupted by a stale in-flight writeback.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.RecycledSlotCompletionFence.Goal := by
  intro s t wal h
  unfold finish
  have h' : t.incarnation ≠ s.live.incarnation := fun hEq => h hEq.symm
  simp [h']
end TinylambOpt.ResearchProofs.ResourcePolicies.EvictionWriteback.Goal_RecycledSlotCompletionFence
