import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# ConditionalReferrerGather

Only the branch with both search and ad IDs equal to zero uses the original referrer.
The executor can defer retrieving Referer to that branch.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.ConditionalReferrerGather
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.ConditionalReferrerGather.Goal := by
  intro rows referrer
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨s, a, i⟩
      simp [ih]

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.ConditionalReferrerGather
