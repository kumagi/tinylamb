import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# PairCountAfterLeadingKeyPartition

Partitioning by SearchEngineID and then grouping by ClientIP preserves the compound
group keys. An inner hash table can use just the IP component.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PairCountAfterLeadingKeyPartition
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.PairCountAfterLeadingKeyPartition.Goal := by
  intro pairs e ip
  induction pairs with
  | nil => simp
  | cons p ps ih =>
      by_cases h1 : p.1 == e
      · by_cases h2 : p.2 == ip
        · have hp : p = (e, ip) := by
            ext
            · simpa [beq_iff_eq] using h1
            · simpa [beq_iff_eq] using h2
          simp [List.count_cons, h1, h2, hp, ih]
        · have h2f : (p.2 == ip) = false := by simpa using h2
          have hp : p ≠ (e, ip) := by
            intro heq
            exact h2 (by simp [heq])
          simp [List.count_cons, h1, h2f, hp, ih]
      · have h1f : (p.1 == e) = false := by simpa using h1
        have hp : p ≠ (e, ip) := by
          intro heq
          exact h1 (by simp [heq])
        simp [List.count_cons, h1f, hp, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PairCountAfterLeadingKeyPartition
