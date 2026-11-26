import TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_IceBaseDuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sum_map_zero_nat {α : Type} (xs : List α) : (xs.map fun _ => 0).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa using ih

theorem sum_map_zero_int {α : Type} (xs : List α) : (xs.map fun _ => (0 : Int)).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa using ih

def addOpt (a b : Option Int) : Option Int :=
  match a, b with
  | none, b => b
  | a, none => a
  | some x, some y => some (x + y)

theorem nsum_eq_of_length_sum (xs ys : List Int) (hl : xs.length = ys.length) (hs : xs.sum = ys.sum) :
    nsum (xs.map some) = nsum (ys.map some) := by
  by_cases h : xs.length = 0
  · have hx : xs = [] := List.length_eq_zero_iff.mp h
    have hy : ys = [] := List.length_eq_zero_iff.mp (by omega)
    simp [hx, hy, nsum]
  · have hx : xs ≠ [] := by intro hx; subst hx; simp at h
    have hy : ys ≠ [] := by intro hy; subst hy; simp [hl] at h
    simp [nsum, hx, hy, hs]

theorem nsum_append (xs ys : List Int) :
    nsum ((xs ++ ys).map some) = addOpt (nsum (xs.map some)) (nsum (ys.map some)) := by
  cases xs with
  | nil => simp [nsum, addOpt]
  | cons x xs =>
      cases ys with
      | nil => simp [nsum, addOpt]
      | cons y ys => simp [nsum, addOpt, List.cons_append, Int.add_assoc]

theorem nsum_cons (o : Option Int) (xs : List (Option Int)) :
    nsum (o :: xs) = addOpt o (nsum xs) := by
  cases o with
  | none => simp [nsum, addOpt]
  | some a =>
      cases hx : xs.filterMap id with
      | nil => simp [nsum, addOpt, hx]
      | cons b bs => simp [nsum, addOpt, hx, Int.add_assoc]


theorem nsum_eq_filterMap (xs : List (Option Int)) :
    nsum xs = nsum ((xs.filterMap id).map some) := by
  cases xs <;> simp [nsum]

theorem nsum_append_opt (xs ys : List (Option Int)) :
    nsum (xs ++ ys) = addOpt (nsum xs) (nsum ys) := by
  rw [nsum_eq_filterMap, nsum_eq_filterMap (xs := xs), nsum_eq_filterMap (xs := ys)]
  rw [List.filterMap_append, nsum_append]

theorem nsum_map_nsum (xs : List (List Int)) :
    nsum (xs.map fun l => nsum (l.map some)) = nsum (xs.flatten.map some) := by
  induction xs with
  | nil => simp [nsum]
  | cons l ls ih =>
      rw [List.map_cons, List.flatten_cons, nsum_append]
      rw [nsum_cons, ih]

theorem sum_replicate_int (n : Nat) (r : Int) : (List.replicate n r).sum = (n : Int) * r := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih, Int.natCast_add, Int.add_mul]
      change r + (n : Int) * r = (n : Int) * r + 1 * r
      rw [Int.one_mul]
      exact Int.add_comm _ _

theorem sum_flatMap_replicate (rows : List (Nat × Int)) (fs : List Nat) :
    (rows.flatMap (fun (x,r) => List.replicate (fs.count x) r)).sum =
      (rows.map (fun (x,r) => (fs.count x : Int) * r)).sum := by
  induction rows with
  | nil => simp
  | cons r rows ih =>
      obtain ⟨x, v⟩ := r
      simp [sum_replicate_int, ih]

theorem length_cons_decomp (rows : List (Nat × Int)) (f : Nat) (fs : List Nat) :
    (rows.flatMap (fun (x,r) => List.replicate ((f::fs).count x) r)).length =
      (rows.flatMap (fun (x,r) => List.replicate (fs.count x) r)).length +
      (rows.filter (fun r => r.1 == f)).length := by
  rw [List.length_flatMap, List.length_flatMap]
  simp only [List.length_replicate]
  induction rows with
  | nil => simp
  | cons r rows ih =>
      obtain ⟨x, v⟩ := r
      simp only [List.map_cons, List.sum_cons, List.filter_cons, ih]
      by_cases h : x == f
      · have hxf : x = f := by simpa using h
        have hfx : f = x := hxf.symm
        simp [List.count_cons, hfx]
        omega
      · have hfx : f ≠ x := by intro hfx; exact h (by simp [hfx.symm])
        have hxf : x ≠ f := by intro hxf; exact hfx hxf.symm
        simp [List.count_cons, hfx, hxf]
        omega

theorem sum_cons_decomp (rows : List (Nat × Int)) (f : Nat) (fs : List Nat) :
    (rows.flatMap (fun (x,r) => List.replicate ((f::fs).count x) r)).sum =
      (rows.flatMap (fun (x,r) => List.replicate (fs.count x) r)).sum +
      ((rows.filter (fun r => r.1 == f)).map Prod.snd).sum := by
  rw [sum_flatMap_replicate, sum_flatMap_replicate]
  induction rows with
  | nil => simp
  | cons r rows ih =>
      obtain ⟨x, v⟩ := r
      simp only [List.map_cons, List.sum_cons, List.filter_cons, List.map_cons, ih]
      by_cases h : x == f
      · have hxf : x = f := by simpa using h
        have hfx : f = x := hxf.symm
        simp [List.count_cons, hfx, Int.natCast_add, Int.add_mul, Int.add_comm, Int.add_left_comm, Int.add_assoc]
      · have hfx : f ≠ x := by intro hfx; exact h (by simp [hfx.symm])
        have hxf : x ≠ f := by intro hxf; exact hfx hxf.symm
        simp [List.count_cons, hfx, hxf, Int.add_assoc]

theorem length_eq (files : List Nat) (rows : List (Nat × Int)) :
    (rows.flatMap (fun (f,r) => List.replicate (files.count f) r)).length =
      (files.flatMap (fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd)).length := by
  induction files with
  | nil => simp [sum_map_zero_nat]
  | cons f fs ih =>
      simp only [List.flatMap_cons, List.length_append]
      rw [length_cons_decomp rows f fs, ih]
      simp [List.length_map]
      omega

theorem sum_eq (files : List Nat) (rows : List (Nat × Int)) :
    (rows.flatMap (fun (f,r) => List.replicate (files.count f) r)).sum =
      (files.flatMap (fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd)).sum := by
  induction files with
  | nil =>
      induction rows with
      | nil => simp
      | cons r rows ihr => simpa using ihr
  | cons f fs ih =>
      simp only [List.flatMap_cons, List.sum_append]
      rw [sum_cons_decomp rows f fs, ih]
      exact Int.add_comm _ _

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud.IceBaseDuckDB.Goal := by
  intro files rows
  dsimp only
  apply Prod.ext
  · rw [← List.length_flatMap]
    exact length_eq files rows
  · simp only [Prod.snd]
    have hright :
        nsum (files.map (fun f =>
          nsum (((rows.filter (fun r => r.1 == f)).map Prod.snd).map some))) =
        nsum ((files.flatMap (fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd)).map some) := by
      induction files with
      | nil => simp [nsum]
      | cons f fs ih =>
          simp only [List.map_cons, List.flatMap_cons, List.map_append]
          rw [nsum_cons, nsum_append_opt, ih]
    rw [hright]
    exact nsum_eq_of_length_sum _ _ (length_eq files rows) (sum_eq files rows)

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_IceBaseDuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Cloud_IceBaseDuckDB.sound
