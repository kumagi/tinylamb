import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# CrossChannelItemWitnessBeforeFactExpansion
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.CrossChannelItemWitnessBeforeFactExpansion
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.CrossChannelItemWitnessBeforeFactExpansion.Goal := by
  intro ids label a b c
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.CrossChannelItemWitnessBeforeFactExpansion.Goal, members]
  apply List.filter_congr
  intro id hid
  simp [List.mem_filter, List.mem_eraseDups, List.contains_iff_mem, hid]

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.CrossChannelItemWitnessBeforeFactExpansion.sound
