import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_H5DuckDB

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
theorem nsum_none_cons (xs : List (Option Int)) : nsum (none :: xs) = nsum xs := by
  simp [nsum]
theorem nsum_some_cons (v : Int) (xs : List (Option Int)) : nsum (some v :: xs) = some (v + (nsum xs).getD 0) := by
  unfold nsum
  simp only [List.filterMap_cons]
  by_cases h : (List.filterMap id xs).isEmpty = true <;> simp [h, sum_eq_zero_of_isEmpty]
theorem nsum_cons_getD (o : Option Int) (xs : List (Option Int)) :
    (nsum (o :: xs)).getD 0 = o.getD 0 + (nsum xs).getD 0 := by
  cases o with
  | none => simp [nsum]
  | some v => rw [nsum_some_cons]; simp

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
            | none => simp [hv, nsum_some_cons, h]
            | some s => simp [hv, nsum_some_cons, h, Int.mul_add]

theorem map_const_eq_replicate {α β : Type} (a : β) : ∀ l : List α,
    l.map (fun _ => a) = List.replicate l.length a
  | [] => by simp
  | b :: t => by
      simp only [List.map_cons, map_const_eq_replicate a t, List.length_cons]
      rw [List.replicate_succ]

theorem fst_pairs_eq (x y : List (Option Int)) :
    (x.flatMap (fun a => y.map (fun b => (a, b)))).map Prod.fst =
      x.flatMap (fun a => List.replicate y.length a) := by
  induction x with
  | nil => simp
  | cons a t ih =>
      simp only [List.flatMap_cons, List.map_append]
      rw [ih]
      congr 1
      have hmap : (y.map (fun b => (a, b))).map Prod.fst = y.map (fun _ => a) := by
        induction y with
        | nil => simp
        | cons b ys hy => simp [hy]
      rw [hmap, map_const_eq_replicate]

theorem snd_pairs_eq (x y : List (Option Int)) :
    (x.flatMap (fun a => y.map (fun b => (a, b)))).map Prod.snd =
      x.flatMap (fun _ => y) := by
  induction x with
  | nil => simp
  | cons a t ih =>
      simp only [List.flatMap_cons, List.map_append]
      rw [ih]
      congr 1
      have hmap : (y.map (fun b => (a, b))).map Prod.snd = y := by
        induction y with
        | nil => simp
        | cons b ys hy => simp [hy, Function.comp_def]
      rw [hmap]

theorem nsum_flatMap_const (xs ys : List (Option Int)) :
    nsum (xs.flatMap (fun _ => ys)) =
      if xs.isEmpty then none else (nsum ys).map (fun v => (xs.length : Int) * v) := by
  induction xs with
  | nil => simp [nsum]
  | cons a t ih =>
      simp only [List.flatMap_cons]
      rw [nsum_append, ih]
      cases hny : nsum ys with
      | none => simp [hny, nsum]
      | some v =>
          cases t with
          | nil => simp [hny]
          | cons b t' =>
              simp [ih, hny, Int.natCast_add, Int.add_mul, Int.add_comm, Int.add_left_comm]
              have htwo : (2 : Int) * v = v + v := by
                rw [show (2 : Int) = 1 + 1 by decide, Int.add_mul]
                simp
              rw [htwo]
              omega

theorem bool_len_zero_eq_isEmpty {α : Type} (l : List α) : (l.length == 0) = l.isEmpty := by
  cases l <;> simp

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.H5DuckDB.Goal := by
  intro x y
  dsimp only
  apply Prod.ext
  · rw [fst_pairs_eq, nsum_flatMap_replicate, bool_len_zero_eq_isEmpty]
    by_cases hy : y.isEmpty <;> simp [hy, Int.mul_comm]
  · rw [snd_pairs_eq, nsum_flatMap_const]
    by_cases hx : x.isEmpty <;> simp [hx, Int.mul_comm]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_H5DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_H5DuckDB.sound
