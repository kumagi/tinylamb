import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_InventoryRatioFeasibilityEnvelope

Registers the proof of the InventoryRatioFeasibilityEnvelope goal in PartitionedAggregateStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/

/-!
# InventoryRatioFeasibilityEnvelope

Positive before totals and certified bounds on both sums can reject an
infeasible two-thirds to three-halves ratio early.
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_InventoryRatioFeasibilityEnvelope

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.InventoryRatioFeasibilityEnvelope.Goal := by
  intro before after bLo bHi aLo aHi _ hbLo hbHi haLo haHi hbound hratio
  rcases hbound with hlt | hlt
  · -- 3*aHi < 2*bLo
    have h1 : 3 * after ≤ 3 * aHi := by omega
    have h2 : 2 * bLo ≤ 2 * before := by omega
    omega
  · -- 3*bHi < 2*aLo
    have h1 : 3 * before ≤ 3 * bHi := by omega
    have h2 : 2 * aLo ≤ 2 * after := by omega
    omega

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_InventoryRatioFeasibilityEnvelope
