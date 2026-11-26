import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# DistinctCardinalityMakesOffsetEmpty

If the number of distinct qualifying resolutions is no greater than the OFFSET, the
modeled result is empty. The executor can avoid sorting the groups in that case.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.DistinctCardinalityMakesOffsetEmpty
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.DistinctCardinalityMakesOffsetEmpty.Goal := by
  intro dims rank h
  have hdrop : ((dims.eraseDups.map rank).mergeSort (fun a b => decide (a ≤ b))).drop 10000 = [] := by
    rw [List.drop_eq_nil_iff]
    simpa using h
  simp only [TinylambOpt.ResearchGoals.QueryRewrites.ranges]
  rw [hdrop]
  simp

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.DistinctCardinalityMakesOffsetEmpty

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
