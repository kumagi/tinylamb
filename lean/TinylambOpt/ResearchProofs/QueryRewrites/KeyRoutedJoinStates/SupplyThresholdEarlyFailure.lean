import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# SupplyThresholdEarlyFailure

Once a nonempty prefix reaches twice available stock, a nonnegative remainder
cannot restore the strict supply comparison.
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.SupplyThresholdEarlyFailure

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.SupplyThresholdEarlyFailure.Goal := by
  intro scanned pending available _ hsum hlt
  have happend : (scanned ++ pending).sum = scanned.sum + pending.sum := by
    simp [List.sum_append]
  omega

end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.SupplyThresholdEarlyFailure

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.SupplyThresholdEarlyFailure.sound
