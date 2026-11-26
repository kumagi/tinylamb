import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_SupplyThresholdEarlyFailure

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! The early threshold test is sound: appending a nonnegative pending sum cannot
    lower a scanned sum that already reached twice the available stock. -/

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.SupplyThresholdEarlyFailure.Goal := by
  intro scanned pending available hnonempty hthreshold hlt
  simp only [List.sum_append] at hlt
  omega
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_SupplyThresholdEarlyFailure
