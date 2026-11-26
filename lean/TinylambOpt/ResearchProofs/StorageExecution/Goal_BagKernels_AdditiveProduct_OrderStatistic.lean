import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

private theorem nodup_eraseDups_nat : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: t => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        rw [List.mem_eraseDups, List.mem_filter] at h
        simp at h
      · exact nodup_eraseDups_nat (t.filter fun b : Nat => !b == a)
termination_by l => l.length
decreasing_by
  have hh := List.length_filter_le (fun b : Nat => !b == a) t
  rw [List.length_cons]
  omega

private theorem sortedNat (l : List Nat) : l.mergeSort.Pairwise (fun a b : Nat => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c hab hbc; by_cases h1 : a ≤ b <;> by_cases h2 : b ≤ c <;> simp_all; omega)
    (by intro a b; by_cases h : a ≤ b <;> simp [h] <;> omega) l
  exact h.imp (by intro a b hab; simpa using hab)

private theorem sorted_find (s u : List Nat)
    (hu_sorted : u.Pairwise (fun a b : Nat => a ≤ b))
    (hu_nodup : u.Nodup) (hu_perm : u.Perm s.eraseDups)
    (hs_sorted : s.Pairwise (fun a b : Nat => a ≤ b)) (i : Nat) :
    u.find? (fun x => i < (s.filter (fun y => decide (y ≤ x))).length) = s[i]? := by
  by_cases hi : i < s.length
  · rw [List.getElem?_eq_getElem hi]
    have hv_mem : s[i] ∈ u :=
      hu_perm.mem_iff.mpr (List.mem_eraseDups.mpr (List.getElem_mem hi))
    obtain ⟨as, bs, habs⟩ := List.append_of_mem hv_mem
    rw [habs] at hu_sorted hu_nodup ⊢
    apply (List.find?_eq_some_iff_append).mpr
    constructor
    · have htake_le : ∀ x ∈ s.take (i + 1), x ≤ s[i] := by
        intro x hx
        rw [List.mem_take_iff_getElem] at hx
        obtain ⟨j, hj, rfl⟩ := hx
        have hjle : j ≤ i := by omega
        rcases Nat.lt_or_eq_of_le hjle with hjlt | rfl
        · exact hs_sorted.rel_getElem_of_lt (by omega) hi hjlt
        · exact Nat.le_refl _
      have hfilter_take : (s.take (i + 1)).filter (fun y : Nat => decide (y ≤ s[i])) = s.take (i + 1) := by
        apply List.filter_eq_self.mpr
        intro x hx
        simpa using htake_le x hx
      have hsub : ((s.take (i + 1)).filter (fun y : Nat => decide (y ≤ s[i]))).Sublist
          (s.filter (fun y : Nat => decide (y ≤ s[i]))) :=
        List.Sublist.filter _ (List.take_sublist (i + 1) s)
      have hlen_take : (s.take (i + 1)).length = i + 1 := by
        rw [List.length_take]
        omega
      have hle : i + 1 ≤ (s.filter (fun y : Nat => decide (y ≤ s[i]))).length := by
        have hh := hsub.length_le
        rw [hfilter_take, hlen_take] at hh
        exact hh
      simpa using (show i < (s.filter (fun y : Nat => decide (y ≤ s[i]))).length by omega)
    · refine ⟨as, bs, rfl, ?_⟩
      intro a ha
      have hle_av : a ≤ s[i] := hu_sorted.rel_of_mem_append ha (by simp)
      have hne_av : a ≠ s[i] := hu_nodup.rel_of_mem_append ha (by simp)
      have hlt_av : a < s[i] := by omega
      have hv_take : s[i] ∈ s.take (i + 1) := by
        rw [List.mem_take_iff_getElem]
        refine ⟨i, ?_, rfl⟩
        rw [Nat.min_eq_left]
        · omega
        · omega
      have hdrop_no : (s.drop i).filter (fun y : Nat => decide (y ≤ a)) = [] := by
        apply (List.filter_eq_nil_iff).mpr
        intro y hy
        rw [List.mem_iff_getElem] at hy
        obtain ⟨j, hj, hjy⟩ := hy
        have hij : i + j < s.length := by
          rw [List.length_drop] at hj
          omega
        have hjy' : s[i + j]'hij = y := by simpa using hjy
        have hvy : s[i] ≤ y := by
          cases j with
          | zero =>
              have hjy'' : s[i] = y := by simpa using hjy'
              omega
          | succ j =>
              have hidx : i < i + Nat.succ j := by omega
              have hltlen : i + Nat.succ j < s.length := by omega
              have hrel := hs_sorted.rel_getElem_of_lt hi hltlen hidx
              have hsame : s[i + Nat.succ j]'hltlen = s[i + Nat.succ j]'hij := by congr
              rw [hsame, hjy'] at hrel
              exact hrel
        by_cases hya : y ≤ a
        · omega
        · simp [hya]
      have hfilter_eq : s.filter (fun y : Nat => decide (y ≤ a)) =
          (s.take i).filter (fun y : Nat => decide (y ≤ a)) := by
        conv =>
          lhs
          rw [← List.take_append_drop i s]
        rw [List.filter_append, hdrop_no, List.append_nil]
      have hcount_le : (s.filter (fun y : Nat => decide (y ≤ a))).length ≤ i := by
        rw [hfilter_eq]
        have hh := List.length_filter_le (fun y : Nat => decide (y ≤ a)) (s.take i)
        have htk : (s.take i).length = i := by
          rw [List.length_take]
          omega
        omega
      have hnot : ¬ i < (s.filter (fun y : Nat => decide (y ≤ a))).length := by omega
      simp [hnot]
  · have hnone : s[i]? = none := List.getElem?_eq_none (Nat.le_of_not_gt hi)
    rw [hnone]
    apply (List.find?_eq_none).mpr
    intro x hx
    have hh := List.length_filter_le (fun y : Nat => decide (y ≤ x)) s
    have hnot : ¬ i < (s.filter (fun y : Nat => decide (y ≤ x))).length := by omega
    simp [hnot]

private theorem count_add_map (x k : Nat) (b : List Nat) :
    (b.map (fun y => x + y)).count k = if x ≤ k then b.count (k - x) else 0 := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.count_cons, ih]
      by_cases hxy : x + y = k
      · have hxle : x ≤ k := by omega
        have hykx : k - x = y := by omega
        simp [hxle, hykx, hxy]
      · have hbeq : (x + y == k) = false := by simp [hxy]
        by_cases hxle : x ≤ k
        · have hykx : y ≠ k - x := by omega
          simp [hbeq, hxle, hykx]
        · simp [hbeq, hxle]

private theorem sum_filter_partition (l : List Nat) (p : Nat → Bool) (f : Nat → Nat) :
    (l.map f).sum = ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum := by
  induction l with
  | nil => simp
  | cons a t ih =>
      by_cases h : p a
      · simp [h, ih]
        omega
      · simp [h, ih]
        omega

private theorem sum_filter_eq_count (l : List Nat) (x : Nat) (f : Nat → Nat) :
    ((l.filter (fun y => y == x)).map f).sum = l.count x * f x := by
  induction l with
  | nil => simp
  | cons a t ih =>
      by_cases h : a == x
      · have hax : a = x := by simpa using h
        simp [hax, ih]
        rw [Nat.succ_mul]
        omega
      · rw [List.count_cons]
        have hb : (a == x) = false := by simpa using h
        simp [hb, ih]

private theorem sum_map_eraseDups_count (l : List Nat) (f : Nat → Nat) :
    (l.map f).sum = (l.eraseDups.map (fun x => l.count x * f x)).sum := by
  induction hl : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons x xs =>
          let u : List Nat := xs.filter (fun y : Nat => !y == x)
          have hulen : u.length < n := by
            rw [← hl]
            have hh := List.length_filter_le (fun y : Nat => !y == x) xs
            dsimp [u]
            change (xs.filter (fun y : Nat => !y == x)).length < xs.length + 1
            omega
          have ihu := ih u.length hulen u rfl
          have hpart := sum_filter_partition xs (fun y : Nat => y == x) f
          have hcount_part := sum_filter_eq_count xs x f
          have hxs : (xs.map f).sum = xs.count x * f x + (u.map f).sum := by
            rw [hpart, hcount_part]
          have hxcount : (x :: xs).count x = xs.count x + 1 := by
            rw [List.count_cons]
            simp
          have hycount : ∀ y ∈ u.eraseDups, (x :: xs).count y = u.count y := by
            intro y hy
            have hyu : y ∈ u := List.mem_eraseDups.mp hy
            have hbad : (!y == x) = true := (List.mem_filter.mp hyu).2
            have hxne : (x == y) = false := by
              by_cases h : x == y
              · have : y == x := by simpa [Bool.beq_comm] using h
                simp [this] at hbad
              · simpa using h
            have hu_count : u.count y = xs.count y := by
              dsimp [u]
              exact List.count_filter hbad
            rw [List.count_cons]
            simp [hxne, hu_count]
          have hsumy : (u.eraseDups.map (fun y => (x :: xs).count y * f y)).sum =
              (u.eraseDups.map (fun y => u.count y * f y)).sum := by
            congr 1
            apply List.map_congr_left
            intro y hy
            rw [hycount y hy]
          simp only [List.map_cons, List.sum_cons, List.eraseDups_cons]
          rw [hxs, hxcount, hsumy, ← ihu]
          rw [Nat.succ_mul]
          omega

private theorem mass_count (a b : List Nat) (k : Nat) :
    bag.mass (a, b) k = (bag.expand (a, b)).count k := by
  simp only [bag, List.count_flatMap]
  calc
    (a.eraseDups.map (fun x => if x ≤ k then a.count x * b.count (k - x) else 0)).sum
        = (a.eraseDups.map (fun x => a.count x * (if x ≤ k then b.count (k - x) else 0))).sum := by
            congr 1
            apply List.map_congr_left
            intro x hx
            by_cases h : x ≤ k <;> simp [h]
    _ = (a.map (fun x => if x ≤ k then b.count (k - x) else 0)).sum := by
            exact (sum_map_eraseDups_count a (fun x => if x ≤ k then b.count (k - x) else 0)).symm
    _ = (a.map (fun x => (b.map (fun y => x + y)).count k)).sum := by
            congr 1
            apply List.map_congr_left
            intro x hx
            rw [count_add_map]

private theorem mem_keys_iff_mem_expand (a b : List Nat) (k : Nat) :
    k ∈ bag.keys (a, b) ↔ k ∈ bag.expand (a, b) := by
  simp only [bag, List.mem_flatMap, List.mem_map, List.mem_eraseDups]

private theorem keys_perm (a b : List Nat) :
    (bag.keys (a, b)).eraseDups.Perm (bag.expand (a, b)).eraseDups := by
  rw [List.perm_iff_count]
  intro k
  rw [(nodup_eraseDups_nat (bag.keys (a, b))).count,
      (nodup_eraseDups_nat (bag.expand (a, b))).count]
  simp only [List.mem_eraseDups]
  by_cases hk : k ∈ bag.keys (a, b)
  · have hke : k ∈ bag.expand (a, b) := (mem_keys_iff_mem_expand a b k).mp hk
    simp [hk, hke]
  · have hke : k ∉ bag.expand (a, b) := fun h => hk ((mem_keys_iff_mem_expand a b k).mpr h)
    simp [hk, hke]

private theorem live_perm_keys (a b : List Nat) :
    (live bag (a, b)).Perm (bag.keys (a, b)).eraseDups := by
  unfold live
  apply List.Perm.of_eq
  apply List.filter_eq_self.mpr
  intro k hk
  have hk' : k ∈ bag.keys (a, b) := List.mem_eraseDups.mp hk
  have hkexp : k ∈ bag.expand (a, b) := (mem_keys_iff_mem_expand a b k).mp hk'
  have hpos : 0 < (bag.expand (a, b)).count k := List.count_pos_iff.mpr hkexp
  rw [mass_count a b k]
  simpa using hpos

private theorem eraseDups_mergeSort_perm (l : List Nat) :
    l.eraseDups.Perm (l.mergeSort.eraseDups) := by
  rw [List.perm_iff_count]
  intro k
  have h1 := (nodup_eraseDups_nat l).count (a := k)
  have h2 := (nodup_eraseDups_nat l.mergeSort).count (a := k)
  rw [h1, h2]
  simp only [List.mem_eraseDups, List.mem_mergeSort]

private theorem sum_map_decide_le (l : List Nat) (v : Nat) :
    (l.map (fun k => (decide (k ≤ v)).toNat)).sum = (l.filter (fun k => decide (k ≤ v))).length := by
  induction l with
  | nil => simp
  | cons a t ih =>
      by_cases h : a ≤ v
      · simp [h, ih]
        omega
      · simp [h, ih]

private theorem below_eq (a b : List Nat) (v : Nat) :
    below bag (a, b) v = ((bag.expand (a, b)).mergeSort.filter (fun y => decide (y ≤ v))).length := by
  unfold below weighted
  simp only [mass_count a b]
  have hp := (keys_perm a b).map (fun k => (bag.expand (a, b)).count k * (decide (k ≤ v)).toNat)
  rw [hp.sum_nat]
  rw [← sum_map_eraseDups_count (bag.expand (a, b)) (fun k => (decide (k ≤ v)).toNat)]
  rw [sum_map_decide_le]
  have hperm := List.mergeSort_perm (bag.expand (a, b)) (fun x y : Nat => decide (x ≤ y))
  exact (hperm.filter (fun y => decide (y ≤ v))).length_eq.symm

theorem sound_OrderStatistic : AdditiveProduct.OrderStatistic.Goal := by
  intro p i
  rcases p with ⟨a, b⟩
  unfold quantile
  let s : List Nat := (bag.expand (a, b)).mergeSort
  let u : List Nat := (live bag (a, b)).mergeSort
  have hs_sorted : s.Pairwise (fun x y : Nat => x ≤ y) := by
    simpa [s] using sortedNat (bag.expand (a, b))
  have hu_sorted : u.Pairwise (fun x y : Nat => x ≤ y) := by
    simpa [u] using sortedNat (live bag (a, b))
  have hu_perm : u.Perm s.eraseDups := by
    have h1 : u.Perm (live bag (a, b)) := by
      simpa [u] using List.mergeSort_perm (live bag (a, b)) (fun x y : Nat => decide (x ≤ y))
    have h2 := live_perm_keys a b
    have h3 := keys_perm a b
    have h4 : (bag.expand (a, b)).eraseDups.Perm s.eraseDups := by
      simpa [s] using eraseDups_mergeSort_perm (bag.expand (a, b))
    exact h1.trans (h2.trans (h3.trans h4))
  have hu_nodup : u.Nodup := hu_perm.symm.nodup (nodup_eraseDups_nat s)
  change u.find? (fun x => i < below bag (a, b) x) = s[i]?
  rw [show (fun x => decide (i < below bag (a, b) x)) =
      (fun x => decide (i < (s.filter (fun y => decide (y ≤ x))).length)) by
        funext x
        rw [below_eq a b x]]
  exact sorted_find s u hu_sorted hu_nodup hu_perm hs_sorted i

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_OrderStatistic
