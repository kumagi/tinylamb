import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-!
# Goal_PartitionedAggregateStates_DominantChannelRatioTournament

Refutation. The goal statement parses as

    (0 < dc → 0 < ds → 0 < dw → (cat*ds > store*dc ∧ cat*dw > web*dc)) ↔
      cat*ds*dw > max (store*dc*dw) (web*dc*ds)

because `→` binds tighter than `↔`. With a non-positive `dc` the left-hand
implication is vacuously true, while the right-hand inequality can still fail.
The intended dominance equivalence holds only with the positivity premises
outside the equivalence; as written the proposal is unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_DominantChannelRatioTournament

theorem refute :
    ¬ TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.DominantChannelRatioTournament.Goal := by
  intro h
  have hiff := h 0 1 1 (-5) 1 (-5)
  have hante : (0 < (-5 : Int) → 0 < (1 : Int) → 0 < (-5 : Int) →
      (0 * 1 > 1 * (-5) ∧ 0 * (-5) > 1 * (-5))) := by
    intro hdc
    omega
  have hbad : ¬ (0 * 1 * (-5) > max (1 * (-5) * (-5)) (1 * (-5) * 1)) := by
    omega
  exact hbad (hiff.mp hante)

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_PartitionedAggregateStates_DominantChannelRatioTournament
