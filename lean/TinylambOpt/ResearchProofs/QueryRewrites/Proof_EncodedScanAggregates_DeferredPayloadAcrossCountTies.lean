import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DeferredPayloadAcrossCountTies

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Deferred payload aggregation across count ties: filtering by a shortlist that
contains the probed key leaves that key's payload stream unchanged. This lets
payload aggregation happen after key qualification.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- Filtering rows by a shortlist that contains the probed key cannot change
that key's payload stream, so payload aggregation may be deferred. -/
theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.DeferredPayloadAcrossCountTies.Goal := by
  intro rs keep k hk
  unfold TinylambOpt.ResearchGoals.QueryRewrites.forKey
  congr 1
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases h : r.1 == k
  · have hrk : r.1 = k := by simpa using h
    simp [hrk, hk]
  · simp [h]
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DeferredPayloadAcrossCountTies
