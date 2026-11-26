import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
Combining a hot bitset partition and a cold residual partition with SQL
three-valued OR preserves the result of testing their concatenation. The proof
reduces `test` to membership of the probe and NULL in each partition and then
checks the nine Kleene cases; WHERE execution may split an IN list in two as
long as the two results are combined with three-valued OR.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_InSetChunkedThreeValueOr

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem test_some (xs : Keys) (x : Int) :
    test xs (some x) =
      if some x ∈ xs then some true else if none ∈ xs then none else some false := by
  unfold test
  by_cases h : xs = []
  · simp [h]
  · simp [h]

theorem test_none (xs : Keys) :
    test xs none = if xs = [] then some false else none := by
  unfold test
  by_cases h : xs = []
  · simp [h]
  · simp [h]

theorem sound : InSetChunkedThreeValueOr.Goal := by
  intro xs ys v
  cases v with
  | none =>
      rw [test_none, test_none, test_none]
      by_cases hx : xs = [] <;> by_cases hy : ys = [] <;> simp [disj, hx, hy]
  | some x =>
      rw [test_some, test_some, test_some]
      by_cases hxA : some x ∈ xs <;> by_cases hxB : some x ∈ ys <;>
        by_cases hxN : none ∈ xs <;> by_cases hyN : none ∈ ys <;>
        simp [disj, hxA, hxB, hxN, hyN]

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_InSetChunkedThreeValueOr
