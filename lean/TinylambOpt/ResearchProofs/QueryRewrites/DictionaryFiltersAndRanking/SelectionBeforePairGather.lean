import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# SelectionBeforePairGather

Gathering WatchID and IP at positions passing the nonempty phrase predicate preserves
the selected key rows. The executor can defer reading these keys until after filtering.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SelectionBeforePairGather
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.SelectionBeforePairGather.Goal := by
  intro ids keep pair
  induction ids with
  | nil => simp
  | cons i is ih =>
      by_cases h : keep i
      · simp [h, ih]
      · simp [h, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SelectionBeforePairGather
