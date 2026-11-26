import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# PlaceGroupedCountryAgeTotals

Grouping by place before applying the country lookup preserves the count of rows,
nullable age sum, and count of non-NULL ages.

The executor can resolve the country once per place and merge the three aggregate
states.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.PlaceGroupedCountryAgeTotals

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.PlaceGroupedCountryAgeTotals.Goal := by
  intro edgeAges country k
  let p : Nat → Bool := fun place => country place == k
  have hValues := KeyGrouping.forKey_filter_perm edgeAges p
  dsimp only
  apply Prod.ext
  · simpa only [List.length_flatMap] using hValues.length_eq
  · apply Prod.ext
    · exact (NullableSum.nsum_group_by_key p edgeAges).symm
    · simpa only [List.filterMap_flatMap, List.length_flatMap] using
        (hValues.filterMap id).length_eq

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.PlaceGroupedCountryAgeTotals
