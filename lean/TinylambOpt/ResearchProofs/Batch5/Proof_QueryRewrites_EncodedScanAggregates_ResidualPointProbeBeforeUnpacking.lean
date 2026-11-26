import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ResidualPointProbeBeforeUnpacking

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# ResidualPointProbeBeforeUnpacking

Equality to an absolute signed key can be checked on residuals: for every
`base`, `base + d == key` is equivalent to `d == key - base`. The SQL executor
may therefore filter compressed residual lanes and still preserve the modeled
point-probe multiplicity; it need not reconstruct absolute values first.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ResidualPointProbeBeforeUnpacking.Goal := by
  intro ds base key
  induction ds with
  | nil => simp
  | cons d ds ih =>
      have hcond : ((base + d) == key) = (d == (key - base)) := by
        cases h1 : (base + d == key) <;> cases h2 : (d == (key - base)) <;> simp at h1 h2 ⊢
        · omega
        · omega
      by_cases h : d = key - base
      · subst d
        simp at hcond
        simp [hcond, ih]
      · simp [hcond, h, ih]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ResidualPointProbeBeforeUnpacking
