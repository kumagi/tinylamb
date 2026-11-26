import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DefaultLaneSumWithPatches

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Default-lane sum with patches: a constant base plus signed corrections
reproduces the exact integer sum. This justifies accumulating a compact
base-and-residual representation while preserving the SQL SUM result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- A constant base plus signed corrections at exceptional lanes reproduces an
exact non-null integer sum; the expansion is independent of evaluation order. -/
theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.DefaultLaneSumWithPatches.Goal := by
  intro ids base
  induction ids with
  | nil => simp [TinylambOpt.ResearchGoals.QueryRewrites.total]
  | cons x xs ih =>
      by_cases hx : x == base
      · have hx' : x = base := by simpa using hx
        simp [TinylambOpt.ResearchGoals.QueryRewrites.total, hx', ih]
        rw [Int.add_mul]
        omega
      · have hne : (x != base) = true := by simpa using hx
        simp [TinylambOpt.ResearchGoals.QueryRewrites.total, hne, ih]
        rw [Int.add_mul]
        omega
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DefaultLaneSumWithPatches
