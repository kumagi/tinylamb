import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ConditionalReferrerKeyInterning

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Conditional referrer interning: a reserved zero code represents an empty
referrer and distinct nonempty referrers receive distinct codes. This keeps
CASE grouping keys equivalent while allowing dictionary-encoded scans.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- A reserved zero code for the empty referrer and an injective interning for
nonempty referrers preserve CASE-key equality in a fixed source partition. -/
theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ConditionalReferrerKeyInterning.Goal := by
  intro s a x y intern hinj
  by_cases hmode : s == 0 && a == 0
  · simp [hmode]
    constructor
    · intro h
      by_cases hx : x = ""
      · by_cases hy : y = ""
        · simp [hx, hy]
        · simp [hx, hy] at h
      · by_cases hy : y = ""
        · simp [hx, hy] at h
        · have hxy : intern x = intern y := by simpa [hx, hy] using h
          exact hinj x y hxy
    · intro h
      subst h
      rfl
  · simp [hmode]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ConditionalReferrerKeyInterning
