import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# ConditionalReferrerKeyInterning

A reserved code for the empty referrer and injective codes for nonempty
strings preserve CASE-key equality in a fixed source partition.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_ConditionalReferrerKeyInterning

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ConditionalReferrerKeyInterning.Goal := by
  intro s a x y intern hinj
  dsimp
  by_cases hsa : s == 0 && a == 0
  · simp only [hsa, ite_true]
    by_cases hx : x = ""
    · by_cases hy : y = ""
      · simp [hx, hy]
      · simp [hx, hy]
    · by_cases hy : y = ""
      · simp [hx, hy]
      · simp [hx, hy]
        constructor
        · intro h
          have hxy : intern x = intern y := by omega
          exact hinj x y hxy
        · intro h; subst h; rfl
  · simp [hsa]


end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_ConditionalReferrerKeyInterning
