import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# NonZeroLengthSumAndCount

For the modeled URL layout, endpoint differences and a nonempty count give the length
aggregate for an eligible CounterID run. The executor can use metadata instead of
decoding URL bodies.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.NonZeroLengthSumAndCount
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem len_nonzero (lengths : List Nat) :
    (lengths.filter (fun x => x != 0)).length = lengths.length - lengths.count 0 := by
  induction lengths with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x == 0
      · have hx : x = 0 := beq_iff_eq.mp h
        subst x
        simp [ih]
      · have hx0 : (x != 0) = true := by simpa using h
        simp [hx0, h, ih, List.count_cons]
        exact (Nat.sub_add_comm (List.count_le_length (a := 0) (l := xs))).symm

theorem sum_nonzero (lengths : List Nat) :
    (lengths.filter (fun x => x != 0)).sum = lengths.sum := by
  induction lengths with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x == 0
      · have hx : x = 0 := beq_iff_eq.mp h
        subst x
        simp [ih]
      · have hx0 : (x != 0) = true := by simpa using h
        simp [hx0, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.NonZeroLengthSumAndCount.Goal := by
  intro lengths
  apply Prod.ext
  · exact sum_nonzero lengths
  · exact len_nonzero lengths
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.NonZeroLengthSumAndCount

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
