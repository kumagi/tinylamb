import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q39DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

abbrev leNat (a b : Nat) : Bool := decide (a ≤ b)

theorem leNat_trans (a b c : Nat) : (leNat a b) = true → (leNat b c) = true → (leNat a c) = true := by
  intro hab hbc
  simp [leNat] at hab hbc ⊢
  exact Nat.le_trans hab hbc

theorem leNat_total (a b : Nat) : (leNat a b || leNat b a) = true := by
  by_cases h : a ≤ b
  · simp [leNat, h]
  · have hba : b ≤ a := by omega
    simp [leNat, h, hba]

theorem merge_take_left : ∀ (A B : List Nat) (n m : Nat),
    (A.merge B leNat).take n = ((A.take (n + m)).merge B leNat).take n := by
  intro A B n
  induction n generalizing A B with
  | zero => simp
  | succ n ih =>
      intro m
      cases A with
      | nil => simp
      | cons a A' =>
          cases B with
          | nil => simp [List.take_take]
          | cons b B' =>
              have hNM : n + 1 + m = (n + m) + 1 := by omega
              by_cases hle : leNat a b = true
              · have hih := ih A' (b :: B') m
                simpa [hNM, hle] using congrArg (fun xs => a :: xs) hih
              · have hlef : leNat a b = false := by simpa using hle
                have hih := ih (a :: A') B' (m + 1)
                have hNM2 : n + (m + 1) = (n + m) + 1 := by omega
                simpa [hNM, hNM2, hlef] using congrArg (fun xs => b :: xs) hih

theorem merge_take_right : ∀ (A B : List Nat) (n m : Nat),
    (A.merge B leNat).take n = (A.merge (B.take (n + m)) leNat).take n := by
  intro A B n
  induction n generalizing A B with
  | zero => simp
  | succ n ih =>
      intro m
      cases A with
      | nil => simp [List.take_take]
      | cons a A' =>
          cases B with
          | nil => simp
          | cons b B' =>
              have hNM : n + 1 + m = (n + m) + 1 := by omega
              by_cases hle : leNat a b = true
              · have hih := ih A' (b :: B') (m + 1)
                have hNM3 : n + (m + 1) = n + m + 1 := by omega
                simpa [hNM, hNM3, hle] using congrArg (fun xs => a :: xs) hih
              · have hlef : leNat a b = false := by simpa using hle
                have hih := ih (a :: A') B' m
                simpa [hNM, hlef] using congrArg (fun xs => b :: xs) hih

theorem mergeSort_append_full (xs ys : List Nat) :
    (xs ++ ys).mergeSort leNat = (xs.mergeSort leNat).merge (ys.mergeSort leNat) leNat := by
  apply List.Perm.eq_of_pairwise
  · intro a b ha hb hab hba
    apply Nat.le_antisymm
    · simpa [leNat] using hab
    · simpa [leNat] using hba
  · exact (List.pairwise_mergeSort leNat_trans leNat_total (xs ++ ys)).imp (by
      intro a b h
      simpa [leNat] using h)
  · exact (List.pairwise_merge leNat_trans leNat_total _ _
      (List.pairwise_mergeSort leNat_trans leNat_total xs)
      (List.pairwise_mergeSort leNat_trans leNat_total ys)).imp (by
      intro a b h
      simpa [leNat] using h)
  · have p1 := List.mergeSort_perm (xs ++ ys) leNat
    have p2 := List.merge_perm_append leNat (xs := xs.mergeSort leNat) (ys := ys.mergeSort leNat)
    have p3 := (List.mergeSort_perm xs leNat).append (List.mergeSort_perm ys leNat)
    exact p1.trans (p2.trans p3).symm

theorem pref_append_right (xs ys : List Nat) (n : Nat) :
    ((xs ++ ys).mergeSort leNat).take n = (((xs.mergeSort leNat).take n ++ ys).mergeSort leNat).take n := by
  rw [mergeSort_append_full, mergeSort_append_full]
  have hsorted : (xs.mergeSort leNat).Pairwise (fun a b => leNat a b = true) :=
    List.pairwise_mergeSort leNat_trans leNat_total xs
  have htake : ((xs.mergeSort leNat).take n).mergeSort leNat = (xs.mergeSort leNat).take n :=
    List.mergeSort_of_pairwise hsorted.take
  rw [htake]
  exact merge_take_left (xs.mergeSort leNat) (ys.mergeSort leNat) n 0

theorem pref_cons_eq_of_pref_eq (x : Nat) (ys zs : List Nat) (n : Nat)
    (h : (ys.mergeSort leNat).take n = (zs.mergeSort leNat).take n) :
    ((x :: ys).mergeSort leNat).take n = ((x :: zs).mergeSort leNat).take n := by
  rw [show x :: ys = [x] ++ ys from rfl, show x :: zs = [x] ++ zs from rfl]
  rw [mergeSort_append_full, mergeSort_append_full]
  simp only [List.mergeSort_singleton]
  have h1 : ([x].merge (ys.mergeSort leNat) leNat).take n =
      ([x].merge ((ys.mergeSort leNat).take n) leNat).take n := by
    simpa using merge_take_right [x] (ys.mergeSort leNat) n 0
  have h2 : ([x].merge (zs.mergeSort leNat) leNat).take n =
      ([x].merge ((zs.mergeSort leNat).take n) leNat).take n := by
    simpa using merge_take_right [x] (zs.mergeSort leNat) n 0
  have h3 := congrArg (fun us : List Nat => ([x].merge us leNat).take n) h
  exact h1.trans (h3.trans h2.symm)

theorem pref_prefix_eq (as ys zs : List Nat) (n : Nat)
    (h : (ys.mergeSort leNat).take n = (zs.mergeSort leNat).take n) :
    ((as ++ ys).mergeSort leNat).take n = ((as ++ zs).mergeSort leNat).take n := by
  induction as with
  | nil => simpa using h
  | cons a as ih =>
      simpa using pref_cons_eq_of_pref_eq a (as ++ ys) (as ++ zs) n ih

theorem pref_flatMap_best (shards : List (List Nat)) (n : Nat) :
    ((shards.flatMap (best n)).mergeSort leNat).take n = (shards.flatten.mergeSort leNat).take n := by
  induction shards with
  | nil => simp [best]
  | cons s shards ih =>
      rw [List.flatMap_cons, List.flatten_cons]
      have h1 := pref_append_right s shards.flatten n
      rw [h1]
      exact pref_prefix_eq (best n s) (shards.flatMap (best n)) shards.flatten n ih

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q39DuckDB.Goal := by
  intro shards
  simp only [TinylambOpt.ResearchGoals.BenchmarkRematch.ranges]
  rw [List.take_drop, List.take_drop]
  have hpref := pref_flatMap_best shards 1010
  have hnum : 1000 + 10 = 1010 := by omega
  rw [hnum]
  have hpref' : ((List.flatMap (best 1010) shards).mergeSort (fun a b => decide (a ≤ b))).take 1010 =
      (shards.flatten.mergeSort (fun a b => decide (a ≤ b))).take 1010 := by
    simpa [leNat] using hpref
  rw [hpref']

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q39DuckDB
