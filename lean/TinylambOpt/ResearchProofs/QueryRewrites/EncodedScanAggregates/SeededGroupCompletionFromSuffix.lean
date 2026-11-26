import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# SeededGroupCompletionFromSuffix

For each group chosen from the prefix of the dictionary, its final count is its
prefix count plus the number of suffix rows whose key is in the chosen set.
The executor may complete only the seeded groups from a suffix scan and avoid
materializing counts for keys that will not be emitted.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.SeededGroupCompletionFromSuffix

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates



set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.SeededGroupCompletionFromSuffix.Goal := by
  intro a b
  dsimp
  apply List.map_congr_left
  intro k hk
  have hkeep : (a.eraseDups.take 10).contains k = true := by
    simpa using hk
  rw [List.count_append]
  rw [List.count_filter hkeep]
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.SeededGroupCompletionFromSuffix
