import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_CrossChannelItemWitnessBeforeFactExpansion

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Translating an intersection of label classes back to physical item IDs
preserves sales for every historical version of each class. `members` removes
duplicates, but the outer `ids.filter` only needs membership, so the erase-dups
step does not change the selected rows. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

/-- Membership in a chain of `members` intersections is a conjunction of
memberships in the three original label lists. -/
theorem mem_members_chain (a b c : List Nat) (x : Nat) :
    x ∈ members (members a b) c ↔ x ∈ a ∧ x ∈ b ∧ x ∈ c := by
  simp [members, and_assoc]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.CrossChannelItemWitnessBeforeFactExpansion.Goal := by
  intro ids label a b c
  apply List.filter_congr
  intro id hid
  have hidErase : id ∈ ids.eraseDups := List.mem_eraseDups.mpr hid
  apply Bool.eq_iff_iff.mpr
  simp [hidErase, mem_members_chain, and_assoc]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_CrossChannelItemWitnessBeforeFactExpansion
