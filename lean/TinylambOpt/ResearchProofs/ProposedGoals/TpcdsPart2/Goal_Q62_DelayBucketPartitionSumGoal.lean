import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Exact five-bucket delay histogram (TPC-DS Q62)

For any row, `bucket [30,60,90,120]` returns a value in `0..4`, so the five
`CASE` buckets partition the input and their counts sum to its length. The executor
may classify each delay once in a single pass instead of evaluating five aggregates
over joined wide strings.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q62_DelayBucketPartitionSumGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

/-- Five disjoint predicates whose index is bounded by four partition a list. -/
theorem count_partition_five {α : Type} (xs : List α) (f : α → Nat) (hf : ∀ x, f x ≤ 4) :
    countWhere xs (fun x => f x == 0) + countWhere xs (fun x => f x == 1) +
    countWhere xs (fun x => f x == 2) + countWhere xs (fun x => f x == 3) +
    countWhere xs (fun x => f x == 4) = xs.length := by
  induction xs with
  | nil => simp [countWhere]
  | cons a xs ih =>
    have hhead : countWhere [a] (fun x => f x == 0) + countWhere [a] (fun x => f x == 1) +
        countWhere [a] (fun x => f x == 2) + countWhere [a] (fun x => f x == 3) +
        countWhere [a] (fun x => f x == 4) = 1 := by
      have hle := hf a
      have hcases : f a = 0 ∨ f a = 1 ∨ f a = 2 ∨ f a = 3 ∨ f a = 4 := by omega
      rcases hcases with h | h | h | h | h <;> simp [countWhere, h]
    have hcons : ∀ j : Nat, countWhere (a :: xs) (fun x => f x == j) =
        countWhere [a] (fun x => f x == j) + countWhere xs (fun x => f x == j) := by
      intro j
      by_cases hp : (f a == j) = true <;> simp [countWhere, hp, Nat.add_comm]
    rw [hcons 0, hcons 1, hcons 2, hcons 3, hcons 4]
    simp only [countWhere, List.length_cons] at ih hhead ⊢
    omega

theorem sound : Q62_DelayBucketPartitionSumGoal := by
  intro delays
  let bs : List Int := [30, 60, 90, 120]
  have hf : ∀ d : Int, bucket bs d ≤ 4 := by
    intro d
    simpa [bs, bucket] using List.countP_le_length (p := fun b : Int => b < d) (l := bs)
  simpa [countWhere, bs] using count_partition_five delays (bucket bs) hf

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q62_DelayBucketPartitionSumGoal
