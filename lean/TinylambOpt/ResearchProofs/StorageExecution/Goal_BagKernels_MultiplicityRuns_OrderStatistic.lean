import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.OrderStatistic` — Find an order statistic with ties by cumulative
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

#eval quantile bag [(3,2),(7,3)] 0
#eval ((bag.expand [(3,2),(7,3)]).mergeSort)[0]?
#eval quantile bag [(0,0),(0,2),(1,1)] 2
#eval ((bag.expand [(0,0),(0,2),(1,1)]).mergeSort)[2]?

private abbrev B := bag

private theorem sum_replicate (n c : Nat) : (List.replicate n c).sum = n * c := by
  induction n with
  | zero => simp
  | succ n ih => simp [Nat.succ_mul]

private theorem count_expand_eq_mass (rs : List (Nat × Nat)) (k : Nat) :
    (B.expand rs).count k = B.mass rs k := by
  unfold B bag
  simp only
  induction rs with
  | nil => simp
  | cons r rs ih =>
    obtain ⟨x, n⟩ := r
    simp only [List.flatMap_cons, List.count_append, List.count_replicate]
    rw [ih]
    simp only [List.filter_cons]
    by_cases hx : x == k <;> simp [hx]

private theorem countP_eq_sum_toNat (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
    cases h : p a <;> simp [h, ih, Nat.add_comm]

private theorem mass_cons (x n : Nat) (rs : List (Nat × Nat)) (k : Nat) :
    B.mass ((x, n) :: rs) k = (if x == k then n else 0) + B.mass rs k := by
  unfold B bag
  simp only
  simp only [List.filter_cons]
  by_cases h : x == k <;> simp [h]

private theorem mass_cons_of_ne (x n k : Nat) (rs : List (Nat × Nat))
    (h : !(k == x) = true) : B.mass ((x, n) :: rs) k = B.mass rs k := by
  rw [mass_cons]
  have hk : k ≠ x := by
    intro heq
    subst heq
    simp at h
  have hxk' : x ≠ k := by intro heq; exact hk heq.symm
  have hxk : (x == k) = false := by simp [hxk']
  simp [hxk]


private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  induction hl : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
    cases l with
    | nil => simp
    | cons a as =>
      have hlen : as.length + 1 = n := by simpa using hl
      rw [List.eraseDups_cons]
      apply List.Pairwise.cons
      · intro b hb
        have hbmem : b ∈ as.filter (fun c => ! c == a) := by
          simpa using hb
        have hne : !(b == a) = true := (List.mem_filter.mp hbmem).2
        have hba : b ≠ a := by
          intro heq
          subst heq
          simp at hne
        exact hba.symm
      · apply ih (as.filter (fun c => ! c == a)).length
        · have hle := List.length_filter_le (fun c => ! c == a) as
          omega
        · rfl

private theorem count_eraseDups (l : List Nat) (k : Nat) :
    l.eraseDups.count k = if k ∈ l then 1 else 0 := by
  rw [List.nodup_iff_count_eq_ite.mp (nodup_eraseDups l)]
  simp [List.mem_eraseDups]

private theorem eraseDups_perm_of_same_mem {l₁ l₂ : List Nat}
    (h : ∀ k, k ∈ l₁ ↔ k ∈ l₂) : l₁.eraseDups.Perm l₂.eraseDups := by
  rw [List.perm_iff_count]
  intro k
  rw [count_eraseDups, count_eraseDups]
  simp [h k]

private theorem perm_sum {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simpa using ih
  | swap x y l => simp [Nat.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem mass_eq_zero_of_not_mem_keys (rs : List (Nat × Nat)) (k : Nat)
    (h : k ∉ rs.map Prod.fst) : B.mass rs k = 0 := by
  unfold B bag
  simp only
  have hf : (rs.filter (fun r => r.1 == k)) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro r hr
    have hrk : (r.1 == k) = true := (List.mem_filter.mp hr).2
    have hkmap : k ∈ rs.map Prod.fst := List.mem_map.mpr ⟨r, (List.mem_filter.mp hr).1, eq_of_beq hrk⟩
    exact h hkmap
  simp [hf]

private theorem below_eq_countP (rs : List (Nat × Nat)) (z : Nat) :
    below B rs z = (B.expand rs).countP (fun y => decide (y ≤ z)) := by
  have hmain : ((rs.map Prod.fst).eraseDups.map
        (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum =
      ((B.expand rs).map (fun y => (decide (y ≤ z)).toNat)).sum := by
    induction rs with
    | nil => simp [B, bag]
    | cons r rs ih =>
      obtain ⟨x, n⟩ := r
      change (((x :: rs.map Prod.fst).eraseDups).map
        (fun k => B.mass ((x, n) :: rs) k * (decide (k ≤ z)).toNat)).sum =
      ((List.replicate n x ++ B.expand rs).map (fun y => (decide (y ≤ z)).toNat)).sum
      simp only [List.eraseDups_cons, List.map_cons, List.sum_cons, List.map_append,
        List.sum_append, List.map_replicate]
      rw [sum_replicate]
      have hsum_filter :
          (((rs.map Prod.fst).filter (fun k => ! k == x)).eraseDups.map
            (fun k => B.mass ((x, n) :: rs) k * (decide (k ≤ z)).toNat)).sum =
          (((rs.map Prod.fst).filter (fun k => ! k == x)).eraseDups.map
            (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        have hk' : k ∈ (rs.map Prod.fst).filter (fun y => ! y == x) := by
          simpa using hk
        have hne : !(k == x) = true := (List.mem_filter.mp hk').2
        rw [mass_cons_of_ne x n k rs hne]
      rw [hsum_filter]
      have hmass_x : B.mass ((x, n) :: rs) x = n + B.mass rs x := by
        rw [mass_cons]
        simp
      rw [hmass_x, Nat.add_mul]
      have hsum_all : ((rs.map Prod.fst).eraseDups.map
          (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum =
          B.mass rs x * (decide (x ≤ z)).toNat +
          (((rs.map Prod.fst).filter (fun k => ! k == x)).eraseDups.map
            (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum := by
        by_cases hxmem : x ∈ rs.map Prod.fst
        · have hperm : (rs.map Prod.fst).eraseDups.Perm
              ((x :: rs.map Prod.fst).eraseDups) := by
            apply eraseDups_perm_of_same_mem
            intro k
            constructor
            · intro hk; exact List.mem_cons_of_mem x hk
            · intro hk
              simp only [List.mem_cons] at hk
              cases hk with
              | inl hk => simpa [hk] using hxmem
              | inr hk => exact hk
          calc
            ((rs.map Prod.fst).eraseDups.map
                (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum =
                (((x :: rs.map Prod.fst).eraseDups).map
                (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum := by
                  apply perm_sum
                  exact hperm.map (fun k => B.mass rs k * (decide (k ≤ z)).toNat)
            _ = B.mass rs x * (decide (x ≤ z)).toNat +
                (((rs.map Prod.fst).filter (fun k => ! k == x)).eraseDups.map
                (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum := by
                  rw [List.eraseDups_cons, List.map_cons, List.sum_cons]
        · have hfilter : (rs.map Prod.fst).filter (fun k => ! k == x) = rs.map Prod.fst := by
            apply List.filter_eq_self.mpr
            intro k hk
            have hkne : k ≠ x := by
              intro heq
              subst heq
              exact hxmem hk
            simp [hkne]
          rw [hfilter]
          have hmass0 : B.mass rs x = 0 := mass_eq_zero_of_not_mem_keys rs x hxmem
          simp [hmass0]
      have htail : B.mass rs x * (decide (x ≤ z)).toNat +
          (((rs.map Prod.fst).filter (fun k => ! k == x)).eraseDups.map
            (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum =
          ((B.expand rs).map (fun y => (decide (y ≤ z)).toNat)).sum := by
        rw [← hsum_all, ih]
      rw [Nat.add_assoc, htail]
  change ((rs.map Prod.fst).eraseDups.map
        (fun k => B.mass rs k * (decide (k ≤ z)).toNat)).sum =
      (B.expand rs).countP (fun y => decide (y ≤ z))
  rw [hmain]
  rw [← countP_eq_sum_toNat]


private theorem sorted_mergeSort (l : List Nat) : l.mergeSort.Pairwise (fun a b => a ≤ b) := by
  simpa using (List.pairwise_mergeSort
    (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (by intro a b; simp [Nat.le_total]) l)

private theorem mem_expand_mem_keys (rs : List (Nat × Nat)) (k : Nat)
    (h : k ∈ B.expand rs) : k ∈ B.keys rs := by
  unfold B bag at h ⊢
  simp only at h ⊢
  induction rs with
  | nil => simp at h
  | cons r rs ih =>
    obtain ⟨x, n⟩ := r
    simp only [List.flatMap_cons, List.mem_append, List.mem_replicate] at h
    simp only [List.map_cons, List.mem_cons]
    cases h with
    | inl h => exact Or.inl h.2
    | inr h => exact Or.inr (ih h)

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro rs i
  let E := B.expand rs
  let L := E.mergeSort
  let K := (live B rs).mergeSort
  let p : Nat → Bool := fun x => i < below B rs x
  change K.find? p = L[i]?
  have hpermLE : L.Perm E := by
    dsimp [L]
    exact List.mergeSort_perm E _
  have hbelow (x : Nat) : below B rs x = L.countP (fun y => decide (y ≤ x)) := by
    rw [below_eq_countP]
    exact (hpermLE.countP_eq (fun y => decide (y ≤ x))).symm
  by_cases hi : i < L.length
  · have hget : L[i]? = some (L[i]) := List.getElem?_eq_getElem hi
    rw [hget]
    let v := L[i]
    have hvL : v ∈ L := by
      rw [List.mem_iff_getElem]
      exact ⟨i, hi, rfl⟩
    have hvE : v ∈ E := by
      simpa [L] using (List.mem_mergeSort.mp hvL)
    have hvmass : 0 < B.mass rs v := by
      rw [← count_expand_eq_mass]
      exact List.count_pos_iff.mpr hvE
    have hvkeys : v ∈ B.keys rs := mem_expand_mem_keys rs v hvE
    have hvlive : v ∈ live B rs := by
      simp [live, List.mem_eraseDups, hvmass, hvkeys]
    have hvK : v ∈ K := by
      dsimp [K]
      exact List.mem_mergeSort.mpr hvlive
    let j := K.findIdx (fun y => y == v)
    have hjlt : j < K.length := by
      dsimp [j]
      apply List.findIdx_lt_length.mpr
      exact ⟨v, hvK, by simp⟩
    have hvKj : K[j] = v := by
      have hpj : (K[j] == v) = true := by
        simpa [j] using (List.findIdx_getElem (xs := K) (p := fun y => y == v)
          (w := by simpa [j] using hjlt))
      exact eq_of_beq hpj
    have hv_drop : v ∈ K.drop j := by
      rw [List.drop_eq_getElem_cons hjlt]
      simp [hvKj]
    have hv_drop_eq : L.drop i = v :: L.drop (i + 1) := by
      rw [List.drop_eq_getElem_cons hi]
    have hv_drop_L : v ∈ L.drop i := by
      rw [hv_drop_eq]
      simp
    have hsK : K.Pairwise (fun a b : Nat => a ≤ b) := by
      dsimp [K]
      exact sorted_mergeSort (live B rs)
    have hnone : (K.take j).find? p = none := by
      rw [List.find?_eq_none]
      intro a ha
      simp only [Bool.not_eq_true, p]
      have hale : a ≤ v := hsK.rel_of_mem_take_of_mem_drop ha hv_drop
      have hane : a ≠ v := by
        rcases List.mem_iff_getElem?.mp ha with ⟨t, ht⟩
        have htj : t < j := by
          have htlen : t < (K.take j).length := (List.getElem?_eq_some_iff.mp ht).1
          rw [List.length_take] at htlen
          omega
        have hta : K[t] = a := by
          have ht' : K[t]? = some a := by
            rwa [List.getElem?_take_of_lt htj] at ht
          exact (List.getElem?_eq_some_iff.mp ht').choose_spec
        have htf : (K[t] == v) = false := List.not_of_lt_findIdx htj
        intro heq
        subst heq
        rw [hta] at htf
        simp at htf
      have halt : a < v := by omega
      have hv_take : v ∈ L.take (i + 1) := by
        apply List.mem_iff_getElem?.mpr
        refine ⟨i, ?_⟩
        rw [List.getElem?_take_of_succ]
        change L[i]? = some L[i]
        exact List.getElem?_eq_getElem hi
      have hdrop_all : ∀ y ∈ L.drop i, ¬ y ≤ a := by
        intro y hy
        rw [hv_drop_eq] at hy
        simp only [List.mem_cons] at hy
        cases hy with
        | inl hy =>
          subst hy
          omega
        | inr hy =>
          have hvle : v ≤ y := (sorted_mergeSort E).rel_of_mem_take_of_mem_drop hv_take hy
          omega
      have hdrop_count : (L.drop i).countP (fun y => decide (y ≤ a)) = 0 := by
        rw [List.countP_eq_zero.mpr]
        intro y hy
        simpa using hdrop_all y hy
      rw [hbelow a]
      rw [decide_eq_false_iff_not]
      have hsplit : L.countP (fun y => decide (y ≤ a)) =
          (L.take i).countP (fun y => decide (y ≤ a)) := by
        calc
          L.countP (fun y => decide (y ≤ a)) =
              ((L.take i ++ L.drop i).countP (fun y => decide (y ≤ a))) := by
                rw [List.take_append_drop]
          _ = (L.take i).countP (fun y => decide (y ≤ a)) +
              (L.drop i).countP (fun y => decide (y ≤ a)) := by
                rw [List.countP_append]
          _ = (L.take i).countP (fun y => decide (y ≤ a)) := by
                rw [hdrop_count, Nat.add_zero]
      rw [hsplit]
      have hlen_take : (L.take i).length = i := by
        rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hi)]
      have hc := List.countP_le_length (p := fun y => decide (y ≤ a)) (l := L.take i)
      omega
    have hpv : p v = true := by
      simp only [p, hbelow, decide_eq_true_eq]
      have hsL := sorted_mergeSort E
      have hprefix : ∀ x ∈ L.take i, x ≤ v := by
        intro x hx
        exact hsL.rel_of_mem_take_of_mem_drop hx hv_drop_L
      have hcnt_take : (L.take i).countP (fun y => decide (y ≤ v)) = i := by
        rw [List.countP_eq_length.mpr]
        · rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hi)]
        · intro x hx
          simpa using hprefix x hx
      have htake_succ : L.take (i + 1) = L.take i ++ [v] := by
        rw [List.take_succ_eq_append_getElem hi]
      have hcnt_succ : (L.take (i + 1)).countP (fun y => decide (y ≤ v)) = i + 1 := by
        rw [htake_succ, List.countP_append, List.countP_singleton]
        simp [hcnt_take]
      have hle := (List.take_sublist (i + 1) L).countP_le
        (p := fun y => decide (y ≤ v))
      omega
    apply (List.find?_eq_some_iff_append).2
    refine ⟨hpv, K.take j, K.drop (j + 1), ?_, ?_⟩
    · calc
        K = K.take j ++ K.drop j := (List.take_append_drop j K).symm
        _ = K.take j ++ (K[j] :: K.drop (j + 1)) := by
          rw [List.drop_eq_getElem_cons hjlt]
        _ = K.take j ++ v :: K.drop (j + 1) := by rw [hvKj]
    · intro a ha
      have hna : ¬ p a = true := (List.find?_eq_none.mp hnone) a ha
      simpa [Bool.not_eq_true] using hna
  · rw [List.getElem?_eq_none (Nat.le_of_not_gt hi)]
    rw [List.find?_eq_none]
    intro x hx
    simp only [Bool.not_eq_true, p]
    rw [hbelow x]
    rw [decide_eq_false_iff_not]
    have hle := List.countP_le_length (p := fun y => decide (y ≤ x)) (l := L)
    have hlen : L.length ≤ i := Nat.le_of_not_gt hi
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_OrderStatistic
