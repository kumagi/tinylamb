import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B4DuckDB

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
theorem nsum_getD_append (a b : List (Option Int)) :
    (nsum (a ++ b)).getD 0 = (nsum a).getD 0 + (nsum b).getD 0 := by
  rw [nsum_append]
  cases nsum a <;> cases nsum b <;> simp
theorem nsum_cons_getD (o : Option Int) (xs : List (Option Int)) :
    (nsum (o :: xs)).getD 0 = o.getD 0 + (nsum xs).getD 0 := by
  cases o with
  | none => simp [nsum]
  | some v =>
      unfold nsum
      simp only [List.filterMap_cons]
      by_cases h : (List.filterMap id xs).isEmpty = true <;> simp [h, sum_eq_zero_of_isEmpty]

theorem len_parts : ∀ (rs : List (Bool × Bool × Option Int)),
    (rs.filter (fun r => r.1 || r.2.1)).length =
      (rs.filter Prod.fst).length + (rs.filter (fun r => !r.1 && r.2.1)).length := by
  intro rs
  induction rs with
  | nil => simp
  | cons r t ih =>
      rcases r with ⟨u, v, o⟩
      cases u <;> cases v <;> simp [ih] <;> omega

theorem nsum_parts : ∀ (rs : List (Bool × Bool × Option Int)),
    (nsum ((rs.filter (fun r => r.1 || r.2.1)).map (fun r => r.2.2))).getD 0 =
      (nsum ((rs.filter Prod.fst).map (fun r => r.2.2))).getD 0 +
        (nsum ((rs.filter (fun r => !r.1 && r.2.1)).map (fun r => r.2.2))).getD 0 := by
  intro rs
  induction rs with
  | nil => simp [nsum]
  | cons r t ih =>
      rcases r with ⟨u, v, o⟩
      cases u <;> cases v <;> simp [ih, nsum_cons_getD, nsum_getD_append] <;> omega

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.B4DuckDB.Goal := by
  intro rs
  dsimp only
  apply Prod.ext
  · exact len_parts rs
  · exact nsum_parts rs

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B4DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B4DuckDB.sound
