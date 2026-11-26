import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# SplitTitleAndUrlFilter

Conjoining the title and nonempty-phrase tests with the negative URL predicate preserves
the modeled filter. The executor can evaluate the URL predicate only on rows surviving
the first tests.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SplitTitleAndUrlFilter
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.SplitTitleAndUrlFilter.Goal := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rcases x with ⟨p, t, u⟩
      simp only [List.filter_cons]
      by_cases hp : (p != "") = true
      · by_cases ht : t.contains "Google" = true
        · by_cases hu : (!u.contains ".google.") = true
          · simp [hp, ht, hu, ih]
          · simp [hp, ht, hu, ih]
        · simp [hp, ht, ih]
      · simp [hp, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SplitTitleAndUrlFilter
