import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q03DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

abbrev leNat : Nat → Nat → Bool := fun a b => decide (a ≤ b)

theorem leNat_trans : ∀ a b c, leNat a b = true → leNat b c = true → leNat a c = true := by
  intro a b c hab hbc
  simp [leNat] at hab hbc ⊢
  exact Nat.le_trans hab hbc

theorem leNat_total : ∀ a b, (leNat a b || leNat b a) = true := by
  intro a b
  by_cases h : a ≤ b
  · simp [leNat, h]
  · have h' : b ≤ a := by omega
    simp [leNat, h, h']

theorem pairwise_sort (xs : List Nat) : (xs.mergeSort leNat).Pairwise (fun a b => a ≤ b) := by
  simpa [leNat] using (List.pairwise_mergeSort (le := leNat) leNat_trans leNat_total xs)

theorem sorted_prefix (xs : List Nat) (n : Nat) : ((xs.mergeSort leNat).take n).Pairwise (fun a b => a ≤ b) :=
  (pairwise_sort xs).take

theorem sort_eq_of_perm {xs ys : List Nat} (h : xs.Perm ys) : xs.mergeSort leNat = ys.mergeSort leNat := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
  · intro a b _ _ hab hba
    exact Nat.le_antisymm hab hba
  · exact pairwise_sort xs
  · exact pairwise_sort ys
  · exact (List.mergeSort_perm xs leNat).trans (h.trans (List.mergeSort_perm ys leNat).symm)

theorem best_eq_of_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys) : best n xs = best n ys := by
  simp [best, sort_eq_of_perm h]

theorem sorted_append_best (n : Nat) {s t : List Nat}
    (hs : s.Pairwise (fun a b => a ≤ b)) (ht : t.Pairwise (fun a b => a ≤ b)) :
    best n (s ++ t) = (s.merge t leNat).take n := by
  have hs' : s.Pairwise (fun a b => leNat a b = true) := by simpa [leNat] using hs
  have ht' : t.Pairwise (fun a b => leNat a b = true) := by simpa [leNat] using ht
  have hp : (s.merge t leNat).Pairwise (fun a b => leNat a b = true) :=
    List.pairwise_merge leNat_trans leNat_total s t hs' ht'
  have hperm : (s.merge t leNat).Perm (s ++ t) := List.merge_perm_append leNat
  have heq : (s ++ t).mergeSort leNat = s.merge t leNat := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat => leNat a b = true)
    · intro a b _ _ hab hba
      simpa [leNat] using Nat.le_antisymm (by simpa [leNat] using hab) (by simpa [leNat] using hba)
    · simpa [leNat] using pairwise_sort (s ++ t)
    · exact hp
    · exact (List.mergeSort_perm (s ++ t) leNat).trans hperm.symm
  simp [best, heq]

theorem merge_take_left (n : Nat) : ∀ s t : List Nat,
    (s.merge t leNat).take n = ((s.take n).merge t leNat).take n := by
  induction n with
  | zero => intro s t; simp
  | succ n ih =>
    intro s t
    cases s with
    | nil => simp
    | cons a s =>
      cases t with
      | nil => simp [List.take_take]
      | cons b t =>
        rw [List.cons_merge_cons]
        by_cases h : leNat a b = true
        · simp [h]
          exact ih s (b :: t)
        · simp [h]
          calc
            ((a :: s).merge t leNat).take n = (((a :: s).take n).merge t leNat).take n := ih (a :: s) t
            _ = (((a :: s.take n).take n).merge t leNat).take n := by
              cases n <;> simp [List.take_take]
            _ = ((a :: s.take n).merge t leNat).take n := (ih (a :: s.take n) t).symm

theorem merge_take_right (n : Nat) : ∀ s t : List Nat,
    (s.merge t leNat).take n = (s.merge (t.take n) leNat).take n := by
  induction n with
  | zero => intro s t; simp
  | succ n ih =>
    intro s t
    cases t with
    | nil => simp
    | cons b t =>
      cases s with
      | nil => simp [List.take_take]
      | cons a s =>
        rw [List.cons_merge_cons]
        by_cases h : leNat a b = true
        · simp [h]
          calc
            (s.merge (b :: t) leNat).take n = (s.merge ((b :: t).take n) leNat).take n := ih s (b :: t)
            _ = (s.merge ((b :: t.take n).take n) leNat).take n := by
              cases n <;> simp [List.take_take]
            _ = (s.merge (b :: t.take n) leNat).take n := (ih s (b :: t.take n)).symm
        · simp [h]
          exact ih (a :: s) t

theorem merge_take (n : Nat) (s t : List Nat) :
    (s.merge t leNat).take n = ((s.take n).merge (t.take n) leNat).take n := by
  calc
    (s.merge t leNat).take n = ((s.take n).merge t leNat).take n := merge_take_left n s t
    _ = ((s.take n).merge (t.take n) leNat).take n := merge_take_right n (s.take n) t

theorem best_best (n : Nat) (xs : List Nat) : best n (best n xs) = best n xs := by
  have hp : ((xs.mergeSort leNat).take n).Pairwise (fun a b => leNat a b = true) := by
    simpa [leNat] using sorted_prefix xs n
  change List.take n ((List.take n (xs.mergeSort leNat)).mergeSort leNat) =
    List.take n (xs.mergeSort leNat)
  rw [List.mergeSort_of_pairwise hp]
  simp [List.take_take]

theorem best_append_best (n : Nat) (xs ys : List Nat) :
    best n (xs ++ ys) = best n (best n xs ++ best n ys) := by
  have h1 : best n (xs ++ ys) = ((xs.mergeSort leNat).merge (ys.mergeSort leNat) leNat).take n := by
    calc
      best n (xs ++ ys) = best n (xs.mergeSort leNat ++ ys.mergeSort leNat) := by
        apply best_eq_of_perm
        exact ((List.mergeSort_perm xs leNat).append (List.mergeSort_perm ys leNat)).symm
      _ = ((xs.mergeSort leNat).merge (ys.mergeSort leNat) leNat).take n :=
        sorted_append_best n (pairwise_sort xs) (pairwise_sort ys)
  have h2 : best n (best n xs ++ best n ys) =
      ((best n xs).merge (best n ys) leNat).take n :=
    sorted_append_best n (sorted_prefix xs n) (sorted_prefix ys n)
  have h2' : best n (best n xs ++ best n ys) =
      (((xs.mergeSort leNat).take n).merge ((ys.mergeSort leNat).take n) leNat).take n := by
    simpa only [best, leNat] using h2
  rw [h1, h2']
  exact merge_take n (xs.mergeSort leNat) (ys.mergeSort leNat)

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch.Q03DuckDB.Goal := by
  intro shards
  induction shards with
  | nil => simp [best]
  | cons sh shards ih =>
    simp only [List.flatMap_cons, List.flatten_cons]
    rw [best_append_best 10 (best 10 sh) (List.flatMap (best 10) shards)]
    rw [best_best]
    rw [ih]
    rw [← best_append_best 10 sh shards.flatten]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q03DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpch.Goal_Q03DuckDB.sound
