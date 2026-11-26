import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_CrossChannelItemWitnessBeforeFactExpansion

Registers the proof of the CrossChannelItemWitnessBeforeFactExpansion goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_CrossChannelItemWitnessBeforeFactExpansion
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.CrossChannelItemWitnessBeforeFactExpansion
open TinylambOpt.ResearchGoals.QueryRewrites (members)

theorem sound : Goal := by
  intro ids label a b c
  dsimp [Goal]
  apply List.filter_congr
  intro id hid
  rw [Bool.eq_iff_iff]
  simp only [members, List.contains_iff_mem, List.mem_filter, List.mem_eraseDups, Bool.and_eq_true, beq_iff_eq]
  constructor
  · intro h
    rcases h with ⟨_, hcommon⟩
    exact hcommon
  · intro h
    exact ⟨hid, h⟩
end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_CrossChannelItemWitnessBeforeFactExpansion
