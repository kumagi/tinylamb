import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_MonthCaseZeroAndNullState

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! A monthly nullable conditional sum can append a single zero witness if any
other month occurred: rows outside the month contribute a typed zero, which
changes neither the SUM nor its NULL-ness when another zero witness exists. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-- The default of a nullable sum is the sum of its non-null values. -/
theorem nsum_getD_sum (xs : List (Option Int)) : (nsum xs).getD 0 = (xs.filterMap id).sum := by
  cases h : xs.filterMap id <;> simp [nsum, h]

/-- Appending a zero witness makes the nullable sum depend only on the sum of
the non-null values of the input. -/
theorem nsum_append_zero_of_sum_eq {xs ys : List (Option Int)}
    (h : (xs.filterMap id).sum = (ys.filterMap id).sum) :
    nsum (xs ++ [some 0]) = nsum (ys ++ [some 0]) := by
  simp [nsum, List.filterMap_append, h]

/-- Prepending a zero witness is the same as appending one. -/
theorem nsum_cons_zero (xs : List (Option Int)) : nsum (some 0 :: xs) = nsum (xs ++ [some 0]) := by
  simp [nsum, List.filterMap_append, Int.zero_add]

/-- Appending a second zero witness leaves a nullable sum unchanged. -/
theorem nsum_append_zero_twice (xs : List (Option Int)) :
    nsum (xs ++ [some 0, some 0]) = nsum (xs ++ [some 0]) := by
  simp [nsum, List.filterMap_append]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.MonthCaseZeroAndNullState.Goal := by
  intro rs month
  induction rs with
  | nil => simp [nsum, forKey]
  | cons r rs ih =>
      have ih' : nsum (rs.map (fun x => if x.1 == month then x.2 else some 0)) =
          nsum (forKey rs month ++ if rs.any (fun x => x.1 != month) then [some 0] else []) := by
        simpa using ih
      by_cases hm : r.1 == month
      · have hne : (r.1 != month) = false := by
          by_cases hEq : r.1 = month
          · simp [hEq]
          · simp [hEq] at hm
        simp only [List.map_cons, hm, ↓reduceIte, forKey, List.filter_cons]
        rw [nsum_cons]
        rw [List.cons_append]
        rw [nsum_cons]
        simp only [List.any_cons, hne, Bool.false_or]
        rw [ih']
        simp [forKey]
      · have hne : (r.1 != month) = true := by simpa using hm
        simp only [List.map_cons, hm, ↓reduceIte, forKey, List.filter_cons]
        simp only [hm, Bool.false_eq_true, ↓reduceIte]
        simp only [List.any_cons, hne, Bool.true_or, ↓reduceIte]
        rw [nsum_cons_zero]
        have hsum := congrArg (fun o : Option Int => o.getD 0) ih'
        rw [nsum_getD_sum, nsum_getD_sum] at hsum
        simp only [List.filterMap_append] at hsum
        by_cases hany : rs.any (fun x => x.1 != month)
        · simp [hany] at hsum
          apply nsum_append_zero_of_sum_eq
          simpa [forKey] using hsum
        · simp [hany] at hsum
          apply nsum_append_zero_of_sum_eq
          simpa [forKey] using hsum
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_MonthCaseZeroAndNullState
