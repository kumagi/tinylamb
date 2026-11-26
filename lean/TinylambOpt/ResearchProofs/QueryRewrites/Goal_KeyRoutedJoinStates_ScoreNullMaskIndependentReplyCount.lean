import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_ScoreNullMaskIndependentReplyCount

On a qualified reply group, null score runs contribute to the reply count but
not to either component of the average state. Expanding runs with `replicate`
therefore preserves count, non-null count and non-null sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_ScoreNullMaskIndependentReplyCount

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem sum_replicate (n : Nat) (x : Int) : (List.replicate n x).sum = (n : Int) * x := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.sum_cons, ih]
      rw [show ((n + 1 : Nat) : Int) = (n : Int) + 1 by simp]
      rw [Int.add_mul]
      omega

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.ScoreNullMaskIndependentReplyCount.Goal := by
  intro runs
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.ScoreNullMaskIndependentReplyCount.Goal, total]
  induction runs with
  | nil => simp
  | cons r rs ih =>
      have ih1 := congrArg Prod.fst ih
      have ih2 := congrArg (fun p : Nat × Nat × Int => p.2.1) ih
      have ih3 := congrArg (fun p : Nat × Nat × Int => p.2.2) ih
      simp at ih1 ih2 ih3
      rcases r with ⟨v, n⟩
      cases v with
      | none => simp [ih1, ih2, ih3]
      | some x =>
          simp [List.filterMap_replicate, sum_replicate, ih1, ih2, ih3, Int.mul_comm]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_ScoreNullMaskIndependentReplyCount
