import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# ResidualPointProbeBeforeUnpacking

Equality to an absolute signed key can be tested in the residual stream
without materializing the base-adjusted values.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_ResidualPointProbeBeforeUnpacking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ResidualPointProbeBeforeUnpacking.Goal := by
  intro ds base key
  induction ds with
  | nil => simp
  | cons d ds ih =>
      have hcond : ((base + d) == key) = (d == (key - base)) := by
        rw [Bool.eq_iff_iff]
        simp only [beq_iff_eq]
        constructor <;> intro h <;> omega
      simp only [List.map_cons, List.filter_cons, List.count_cons, hcond]
      split <;> simp [ih]


end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_ResidualPointProbeBeforeUnpacking
