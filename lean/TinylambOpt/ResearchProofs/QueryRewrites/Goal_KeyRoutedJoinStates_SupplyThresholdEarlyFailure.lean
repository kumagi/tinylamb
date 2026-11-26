import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_SupplyThresholdEarlyFailure

Registers the proof of the SupplyThresholdEarlyFailure goal in KeyRoutedJoinStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_SupplyThresholdEarlyFailure
open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.SupplyThresholdEarlyFailure

theorem sound : Goal := by
  intro scanned pending available _ h2
  intro hlt
  have hsum : (scanned ++ pending).sum = scanned.sum + pending.sum := by
    simp [List.sum_append]
  have hnp : 0 ≤ pending.sum := by
    exact Nat.zero_le _
  omega

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_SupplyThresholdEarlyFailure
