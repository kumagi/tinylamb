import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_ScoreNullMaskIndependentReplyCount

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! NULL score runs still contribute to reply count, but not to the non-null
    count or the score sum used by the average. The reply-count component can
    therefore be kept while the two average components mask NULL scores. -/

open TinylambOpt.ResearchGoals.QueryRewrites

theorem filterMap_id_replicate_some (x : Int) (n : Nat) :
    (List.replicate n (some x)).filterMap id = List.replicate n x := by
  induction n with
  | zero => rfl
  | succ n ih => simp [ih]

theorem filterMap_id_replicate_none (n : Nat) :
    (List.replicate n (none : Option Int)).filterMap id = [] := by
  induction n with
  | zero => rfl
  | succ n ih => simp [ih]

theorem sum_replicate_int (x : Int) (n : Nat) : (List.replicate n x).sum = (n : Int) * x := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih]
      simp [Nat.cast]
      rw [Int.add_comm]
      simp [Int.add_mul]

theorem allScores_length (runs : List (Option Int × Nat)) :
    (runs.flatMap (fun (v, n) => List.replicate n v)).length = (runs.map Prod.snd).sum := by
  induction runs with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨v, n⟩
      simp [ih]

theorem allScores_nonNullCount (runs : List (Option Int × Nat)) :
    ((runs.flatMap (fun (v, n) => List.replicate n v)).filterMap id).length =
      ((runs.filter (fun r => r.1.isSome)).map Prod.snd).sum := by
  induction runs with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨v, n⟩
      cases v with
      | none => simp [filterMap_id_replicate_none, ih]
      | some x => simp [filterMap_id_replicate_some, ih]

theorem allScores_sum (runs : List (Option Int × Nat)) :
    ((runs.flatMap (fun (v, n) => List.replicate n v)).filterMap id).sum =
      total runs (fun (v, n) => v.getD 0 * (n : Int)) := by
  induction runs with
  | nil => simp [total]
  | cons r rs ih =>
      rcases r with ⟨v, n⟩
      cases v with
      | none => simp [filterMap_id_replicate_none, ih, total]
      | some x =>
          simp [filterMap_id_replicate_some, sum_replicate_int, ih, total]
          rw [Int.mul_comm]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.ScoreNullMaskIndependentReplyCount.Goal := by
  intro runs
  dsimp only
  apply Prod.ext
  · exact allScores_length runs
  · apply Prod.ext
    · exact allScores_nonNullCount runs
    · exact allScores_sum runs
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_ScoreNullMaskIndependentReplyCount
