import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# MergeTitleFrequencyBeforeRanking

Counts for identical titles in separate dictionaries must be added before ranking. The
executor can feed per-dictionary subtotals to the global aggregation.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.MergeTitleFrequencyBeforeRanking
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.MergeTitleFrequencyBeforeRanking.Goal := by
  intro pages title
  induction pages with
  | nil => simp
  | cons p ps ih =>
      simp [List.count_append, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.MergeTitleFrequencyBeforeRanking
