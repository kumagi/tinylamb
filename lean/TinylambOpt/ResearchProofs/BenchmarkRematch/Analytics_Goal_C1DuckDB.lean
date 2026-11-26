import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C1DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem isEmpty_append {α : Type} (a b : List α) : (a ++ b).isEmpty = (a.isEmpty && b.isEmpty) := by
  cases a <;> simp
theorem sum_eq_zero_of_isEmpty (l : List Int) (h : l.isEmpty = true) : l.sum = 0 := by
  cases l <;> simp at h ⊢
theorem nsum_append (a b : List (Option Int)) : nsum (a ++ b) =
    match nsum a, nsum b with
    | none, none => none
    | some x, none => some x
    | none, some y => some y
    | some x, some y => some (x + y) := by
  unfold nsum
  simp only [List.filterMap_append, isEmpty_append, List.sum_append]
  by_cases ha : (List.filterMap id a).isEmpty = true <;>
    by_cases hb : (List.filterMap id b).isEmpty = true <;>
    simp [ha, hb, sum_eq_zero_of_isEmpty]
theorem nsum_none_cons (xs : List (Option Int)) : nsum (none :: xs) = nsum xs := by
  simp [nsum]
theorem nsum_some_cons (v : Int) (xs : List (Option Int)) : nsum (some v :: xs) = some (v + (nsum xs).getD 0) := by
  unfold nsum
  simp only [List.filterMap_cons]
  by_cases h : (List.filterMap id xs).isEmpty = true <;> simp [h, sum_eq_zero_of_isEmpty]
theorem nsum_replicate (n : Nat) (o : Option Int) : nsum (List.replicate n o) =
    if n = 0 then none else o.map (fun v => (n : Int) * v) := by
  cases o with
  | none => simp [nsum]
  | some v =>
      induction n with
      | zero => simp [nsum]
      | succ n ih =>
          simp only [List.replicate_succ, nsum_some_cons, ih]
          by_cases hn : n = 0
          · simp [hn, nsum]
          · simp [hn]
            rw [Int.add_mul]
            simp [Int.add_comm]

theorem nsum_flatMap_replicate (votes : Nat) : ∀ scores : List (Option Int),
    nsum (scores.flatMap (List.replicate votes)) =
      if votes == 0 then none else (nsum scores).map (fun x => (votes : Int) * x)
  | [] => by simp [nsum]
  | a :: t => by
      rw [List.flatMap_cons, nsum_append, nsum_replicate, nsum_flatMap_replicate votes t]
      by_cases hv : votes = 0
      · simp [hv, nsum]
      · cases a with
        | none =>
            rw [nsum_none_cons]
            cases h : nsum t <;> simp [hv, h]
        | some v =>
            cases h : nsum t with
            | none =>
                simp [hv, nsum_some_cons, h]
            | some s =>
                simp [hv, nsum_some_cons, h, Int.mul_add]

theorem sum_const_nat {α : Type} (n : Nat) (l : List α) : (l.map (fun _ => n)).sum = l.length * n := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp [ih, Nat.add_mul, Nat.add_comm]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.C1DuckDB.Goal := by
  intro scores votes
  dsimp only
  apply Prod.ext
  · induction scores with
    | nil => simp
    | cons a t ih => simp [ih, sum_const_nat, Nat.add_mul, Nat.add_comm]
  · exact nsum_flatMap_replicate votes scores

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C1DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C1DuckDB.sound
