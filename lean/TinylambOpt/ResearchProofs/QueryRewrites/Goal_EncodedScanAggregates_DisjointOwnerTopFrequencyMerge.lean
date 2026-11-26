import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
Disjoint owner top-frequency merge: shard-local descending top-ten frequency
lists suffice for the global top ten when each owner belongs to at most one
shard. This justifies replacing a global scan over all owners with a merge of
already-completed per-shard aggregates, preserving the modeled result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DisjointOwnerTopFrequencyMerge

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


open TinylambOpt.ResearchGoals.QueryRewrites

private theorem removeAll_eq_self_of_disjoint {a b : List Nat}
    (h : ∀ k ∈ a, k ∉ b) : b.removeAll a = b := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      have hxnot : x ∉ b := h x (by simp)
      have ih' : b.removeAll xs = b := ih (fun k hk => h k (by simp [hk]))
      rw [List.removeAll_cons]
      have hfilt : (b.filter fun y => ! y == x) = b := by
        apply (List.filter_eq_self).2
        intro k hk
        have hkne : k ≠ x := by
          intro hkx
          subst hkx
          exact hxnot hk
        simp [hkne]
      rw [hfilt, ih']

private theorem frequency_append_disjoint {a b : List Nat}
    (h : ∀ k ∈ a, k ∉ b) : frequency (a ++ b) = frequency a ++ frequency b := by
  unfold frequency
  rw [List.eraseDups_append, removeAll_eq_self_of_disjoint h, List.map_append]
  congr 1
  · apply List.map_congr_left
    intro x hx
    have hxa : x ∈ a := List.mem_eraseDups.mp hx
    have hxb : b.count x = 0 := (List.count_eq_zero).2 (h x hxa)
    simp [List.count_append, hxb]
  · apply List.map_congr_left
    intro x hx
    have hxb : x ∈ b := List.mem_eraseDups.mp hx
    have hxa : x ∉ a := by
      intro hxain
      exact h x hxain hxb
    have hxa0 : a.count x = 0 := (List.count_eq_zero).2 hxa
    simp [List.count_append, hxa0]

private theorem counts_flatten_perm : ∀ (shards : List (List Nat)),
    shards.Pairwise (fun a b => ∀ k ∈ a, ¬ k ∈ b) →
    ((frequency shards.flatten).map Prod.snd).Perm
      (shards.flatMap (fun s => (frequency s).map Prod.snd)) := by
  intro shards hpair
  induction shards with
  | nil => simp [frequency]
  | cons sh rest ih =>
      have hhead : ∀ t ∈ rest, ∀ k ∈ sh, k ∉ t := (List.pairwise_cons.mp hpair).1
      have htail : rest.Pairwise (fun a b => ∀ k ∈ a, ¬ k ∈ b) := (List.pairwise_cons.mp hpair).2
      have hdisj : ∀ k ∈ sh, k ∉ rest.flatten := by
        intro k hk hkflat
        rcases List.mem_flatten.mp hkflat with ⟨t, ht, hkt⟩
        exact hhead t ht k hk hkt
      simp only [List.flatten_cons, List.flatMap_cons]
      rw [frequency_append_disjoint hdisj, List.map_append]
      exact (ih htail).append_left ((frequency sh).map Prod.snd)

private abbrev leNat : Nat → Nat → Bool := fun a b => decide (a ≤ b)
private abbrev geNat : Nat → Nat → Bool := fun a b => decide (b ≤ a)

private theorem leNat_trans : ∀ a b c, leNat a b = true → leNat b c = true → leNat a c = true := by
  intro a b c hab hbc
  simp [leNat] at hab hbc ⊢
  exact Nat.le_trans hab hbc

private theorem leNat_total : ∀ a b, (leNat a b || leNat b a) = true := by
  intro a b
  by_cases h : a ≤ b
  · simp [leNat, h]
  · have h' : b ≤ a := by omega
    simp [leNat, h, h']

private theorem geNat_trans : ∀ a b c, geNat a b = true → geNat b c = true → geNat a c = true := by
  intro a b c hab hbc
  simp [geNat] at hab hbc ⊢
  exact Nat.le_trans hbc hab

private theorem geNat_total : ∀ a b, (geNat a b || geNat b a) = true := by
  intro a b
  by_cases h : a ≤ b
  · simp [geNat, h]
  · have h' : b ≤ a := by omega
    simp [geNat, h, h']

private theorem pairwise_sort_le (xs : List Nat) : (xs.mergeSort leNat).Pairwise (fun a b => a ≤ b) := by
  simpa [leNat] using (List.pairwise_mergeSort (le := leNat) leNat_trans leNat_total xs)

private theorem pairwise_sort_ge (xs : List Nat) : (xs.mergeSort geNat).Pairwise (fun a b => b ≤ a) := by
  simpa [geNat] using (List.pairwise_mergeSort (le := geNat) geNat_trans geNat_total xs)

private theorem reverse_mergeSort_eq (xs : List Nat) :
    (xs.mergeSort leNat).reverse = xs.mergeSort geNat := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => b ≤ a)
  · intro a b _ _ hab hba
    exact Nat.le_antisymm hba hab
  · rw [List.pairwise_reverse]
    exact pairwise_sort_le xs
  · exact pairwise_sort_ge xs
  · exact (List.reverse_perm (xs.mergeSort leNat)).trans
      ((List.mergeSort_perm xs leNat).trans (List.mergeSort_perm xs geNat).symm)

private abbrev bestD (n : Nat) (xs : List Nat) := (xs.mergeSort geNat).take n

private theorem top_eq_bestD (n : Nat) (xs : List Nat) :
    (xs.mergeSort leNat).reverse.take n = bestD n xs := by
  unfold bestD
  rw [reverse_mergeSort_eq]

private theorem sorted_prefix_ge (xs : List Nat) (n : Nat) : (bestD n xs).Pairwise (fun a b => b ≤ a) :=
  (pairwise_sort_ge xs).take

private theorem bestD_eq_of_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys) : bestD n xs = bestD n ys := by
  unfold bestD
  have hx : xs.mergeSort geNat = ys.mergeSort geNat := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat => b ≤ a)
    · intro a b _ _ hab hba
      exact Nat.le_antisymm hba hab
    · exact pairwise_sort_ge xs
    · exact pairwise_sort_ge ys
    · exact (List.mergeSort_perm xs geNat).trans (h.trans (List.mergeSort_perm ys geNat).symm)
  rw [hx]

private theorem sorted_append_bestD (n : Nat) {s t : List Nat}
    (hs : s.Pairwise (fun a b => b ≤ a)) (ht : t.Pairwise (fun a b => b ≤ a)) :
    bestD n (s ++ t) = (s.merge t geNat).take n := by
  have hs' : s.Pairwise (fun a b => geNat a b = true) := by simpa [geNat] using hs
  have ht' : t.Pairwise (fun a b => geNat a b = true) := by simpa [geNat] using ht
  have hp : (s.merge t geNat).Pairwise (fun a b => geNat a b = true) :=
    List.pairwise_merge geNat_trans geNat_total s t hs' ht'
  have hperm : (s.merge t geNat).Perm (s ++ t) := List.merge_perm_append geNat
  have heq : (s ++ t).mergeSort geNat = s.merge t geNat := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat => geNat a b = true)
    · intro a b _ _ hab hba
      simpa [geNat] using Nat.le_antisymm (by simpa [geNat] using hba) (by simpa [geNat] using hab)
    · simpa [geNat] using pairwise_sort_ge (s ++ t)
    · exact hp
    · exact (List.mergeSort_perm (s ++ t) geNat).trans hperm.symm
  unfold bestD
  rw [heq]

private theorem merge_take_left (n : Nat) : ∀ s t : List Nat,
    (s.merge t geNat).take n = ((s.take n).merge t geNat).take n := by
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
        by_cases h : geNat a b = true
        · simp [h]
          exact ih s (b :: t)
        · simp [h]
          calc
            ((a :: s).merge t geNat).take n = (((a :: s).take n).merge t geNat).take n := ih (a :: s) t
            _ = (((a :: s.take n).take n).merge t geNat).take n := by
              cases n <;> simp [List.take_take]
            _ = ((a :: s.take n).merge t geNat).take n := (ih (a :: s.take n) t).symm

private theorem merge_take_right (n : Nat) : ∀ s t : List Nat,
    (s.merge t geNat).take n = (s.merge (t.take n) geNat).take n := by
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
        by_cases h : geNat a b = true
        · simp [h]
          calc
            (s.merge (b :: t) geNat).take n = (s.merge ((b :: t).take n) geNat).take n := ih s (b :: t)
            _ = (s.merge ((b :: t.take n).take n) geNat).take n := by
              cases n <;> simp [List.take_take]
            _ = (s.merge (b :: t.take n) geNat).take n := (ih s (b :: t.take n)).symm
        · simp [h]
          exact ih (a :: s) t

private theorem merge_take (n : Nat) (s t : List Nat) :
    (s.merge t geNat).take n = ((s.take n).merge (t.take n) geNat).take n := by
  calc
    (s.merge t geNat).take n = ((s.take n).merge t geNat).take n := merge_take_left n s t
    _ = ((s.take n).merge (t.take n) geNat).take n := merge_take_right n (s.take n) t

private theorem bestD_bestD (n : Nat) (xs : List Nat) : bestD n (bestD n xs) = bestD n xs := by
  have hp : (bestD n xs).Pairwise (fun a b => geNat a b = true) := by
    simpa [geNat] using sorted_prefix_ge xs n
  unfold bestD
  rw [List.mergeSort_of_pairwise hp]
  simp [List.take_take]

private theorem bestD_append_bestD (n : Nat) (xs ys : List Nat) :
    bestD n (xs ++ ys) = bestD n (bestD n xs ++ bestD n ys) := by
  have h1 : bestD n (xs ++ ys) = ((xs.mergeSort geNat).merge (ys.mergeSort geNat) geNat).take n := by
    calc
      bestD n (xs ++ ys) = bestD n (xs.mergeSort geNat ++ ys.mergeSort geNat) := by
        apply bestD_eq_of_perm
        exact ((List.mergeSort_perm xs geNat).append (List.mergeSort_perm ys geNat)).symm
      _ = ((xs.mergeSort geNat).merge (ys.mergeSort geNat) geNat).take n :=
        sorted_append_bestD n (pairwise_sort_ge xs) (pairwise_sort_ge ys)
  have h2 : bestD n (bestD n xs ++ bestD n ys) =
      ((bestD n xs).merge (bestD n ys) geNat).take n :=
    sorted_append_bestD n (sorted_prefix_ge xs n) (sorted_prefix_ge ys n)
  have h2' : bestD n (bestD n xs ++ bestD n ys) =
      (((xs.mergeSort geNat).take n).merge ((ys.mergeSort geNat).take n) geNat).take n := by
    simpa only [bestD] using h2
  rw [h1, h2']
  exact merge_take n (xs.mergeSort geNat) (ys.mergeSort geNat)

private theorem bestD_flatMap_bestD : ∀ (shards : List (List Nat)),
    bestD 10 (shards.flatMap (fun s => bestD 10 ((frequency s).map Prod.snd))) =
      bestD 10 (shards.flatMap (fun s => (frequency s).map Prod.snd)) := by
  intro shards
  induction shards with
  | nil => simp
  | cons sh rest ih =>
      simp only [List.flatMap_cons]
      rw [bestD_append_bestD 10 (bestD 10 ((frequency sh).map Prod.snd))
          (List.flatMap (fun s => bestD 10 ((frequency s).map Prod.snd)) rest)]
      rw [bestD_bestD]
      rw [ih]
      rw [← bestD_append_bestD 10 ((frequency sh).map Prod.snd)
          (List.flatMap (fun s => (frequency s).map Prod.snd) rest)]

theorem sound : DisjointOwnerTopFrequencyMerge.Goal := by
  intro shards hpair
  let counts := fun (xs : List Nat) => (frequency xs).map Prod.snd
  change (((shards.flatMap (fun s => ((counts s).mergeSort leNat).reverse.take 10)).mergeSort leNat).reverse.take 10) =
      (((counts shards.flatten).mergeSort leNat).reverse.take 10)
  rw [top_eq_bestD 10 (shards.flatMap (fun s => ((counts s).mergeSort leNat).reverse.take 10))]
  rw [top_eq_bestD 10 (counts shards.flatten)]
  have hfun : (fun s => ((counts s).mergeSort leNat).reverse.take 10) =
      (fun s => bestD 10 (counts s)) := by
    funext s
    exact top_eq_bestD 10 (counts s)
  rw [hfun]
  rw [bestD_flatMap_bestD]
  apply bestD_eq_of_perm 10
  exact (counts_flatten_perm shards hpair).symm

end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DisjointOwnerTopFrequencyMerge
