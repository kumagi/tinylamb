import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# DictionaryPairPredicateMemoization

With stable dictionaries, evaluating a predicate on a pair of codes through a cached
lookup gives the same result as decoding and evaluating that pair.

The filter can reuse the predicate result across rows with the same title and URL codes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.DictionaryPairPredicateMemoization
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.DictionaryPairPredicateMemoization.Goal := by
  intro pairs title url accepts allowed
  apply List.filter_congr
  intro x hx
  rw [Bool.eq_iff_iff]
  rw [List.elem_iff]
  rw [List.mem_filter]
  rw [List.mem_eraseDups]
  simp [hx]
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.DictionaryPairPredicateMemoization
