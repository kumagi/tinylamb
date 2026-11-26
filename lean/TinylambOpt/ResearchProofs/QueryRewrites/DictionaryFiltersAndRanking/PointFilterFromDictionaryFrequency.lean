import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# PointFilterFromDictionaryFrequency

The target ID's encoded frequency determines the size of the matching constant output.
The executor can generate those rows without loading their original payloads.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PointFilterFromDictionaryFrequency
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.PointFilterFromDictionaryFrequency.Goal := by
  intro xs k
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x == k
      · have hxk : x = k := beq_iff_eq.mp h
        subst hxk
        simp [List.count_cons, List.replicate_succ, ih]
      · simp [List.count_cons, h, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PointFilterFromDictionaryFrequency
