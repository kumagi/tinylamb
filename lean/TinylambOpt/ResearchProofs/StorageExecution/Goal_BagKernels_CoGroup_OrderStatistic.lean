import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `CoGroup.OrderStatistic` — Find an order statistic with ties by cumulative
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

#eval quantile bag ([3, 1, 3, 2], [2, 3, 3, 3]) 0
#eval ((bag.expand ([3, 1, 3, 2], [2, 3, 3, 3])).mergeSort)[0]?
#eval quantile bag ([3, 1, 3, 2], [2, 3, 3, 3]) 3
#eval ((bag.expand ([3, 1, 3, 2], [2, 3, 3, 3])).mergeSort)[3]?
#eval quantile bag ([3, 1, 3, 2], [2, 3, 3, 3]) 6
#eval ((bag.expand ([3, 1, 3, 2], [2, 3, 3, 3])).mergeSort)[6]?
#eval quantile bag ([], [1, 2]) 0

private abbrev B : Bag (List Nat × List Nat) := bag

private theorem sum_filter_split (l : List Nat) (f : Nat → Nat) (p : Nat → Bool) :
    (l.map f).sum = ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
    simp only [List.map_cons, List.sum_cons, List.filter_cons]
    by_cases hp : p a = true
    · simp [hp, ih]; omega
    · have hp' : p a = false := by revert hp; cases p a <;> simp
      simp [hp', ih]; omega

private theorem count_eq_count_filter_ne (a : Nat) (as : List Nat) :
    ∀ k, k ≠ a → (List.filter (fun b => !b == a) as).count k = as.count k := by
  intro k hne
  induction as with
  | nil => simp
  | cons b bs ih =>
    simp only [List.filter, List.count_cons]
    by_cases hb : !b == a = true
    · simp [hb, List.count_cons, ih]
    · have hb' : (!b == a) = false := by revert hb; cases (!b == a) <;> simp
      simp [hb']
      have hba : b = a := by simpa using hb'
      rw [hba]
      have hak : a ≠ k := Ne.symm hne
      simp [hak, ih]

private theorem sum_map_const_nat (l : List Nat) (c : Nat) :
    (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_mul, Nat.one_mul]; omega

private theorem sum_filter_eq_count_mul (as : List Nat) (a : Nat) (f : Nat → Nat) :
    ((as.filter (fun b => b == a)).map f).sum = as.count a * f a := by
  have h1 : ((as.filter (fun b => b == a)).map f).sum =
      ((as.filter (fun b => b == a)).map (fun _ => f a)).sum := by
    congr 1
    apply List.map_congr_left
    intro b hb
    rw [List.mem_filter] at hb
    have : b = a := eq_of_beq hb.2
    simp [this]
  rw [h1, sum_map_const_nat]
  have h2 : (List.filter (fun b => b == a) as).length = as.count a := by
    rw [List.count_eq_length_filter]
  rw [h2, Nat.mul_comm]

private theorem sum_eraseDups_count_mul (xs : List Nat) (f : Nat → Nat) :
    ((xs.eraseDups.map (fun k => xs.count k * f k)).sum) = (xs.map f).sum := by
  suffices H : ∀ n (xs : List Nat), xs.length ≤ n →
      ((xs.eraseDups.map (fun k => xs.count k * f k)).sum) = (xs.map f).sum from
    H xs.length xs (Nat.le_refl _)
  intro n
  induction n with
  | zero =>
    intro xs hle
    match xs with
    | [] => simp
    | _ :: _ => simp at hle
  | succ n ih =>
    intro xs hle
    match xs with
    | [] => simp
    | a :: as =>
      have hle' : as.length ≤ n := by simp at hle; omega
      rw [List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons, List.count_cons, beq_self_eq_true, ite_true]
      have hhead : (as.count a + 1) * f a = f a + as.count a * f a := by
        rw [Nat.add_mul, Nat.one_mul, Nat.add_comm]
      rw [hhead]
      have hcount_k : ∀ k, k ∈ (List.filter (fun b => !b == a) as).eraseDups →
          (if (a == k) = true then 1 else 0) + as.count k = as.count k := by
        intro k hk
        rw [List.mem_eraseDups, List.mem_filter] at hk
        have hne : (a == k) = false := by
          have : (!k == a) = true := hk.2
          simp at this
          rw [beq_eq_false_iff_ne]
          intro heq; subst heq; simp at this
        simp [hne]
      have hmap_eq : ((List.filter (fun b => !b == a) as).eraseDups.map
          (fun k => (as.count k + if (a == k) = true then 1 else 0) * f k)).sum =
          ((List.filter (fun b => !b == a) as).eraseDups.map
          (fun k => as.count k * f k)).sum := by
        congr 1; apply List.map_congr_left; intro k hk
        rw [Nat.add_comm (List.count k as) (if (a == k) = true then 1 else 0), hcount_k k hk]
      rw [hmap_eq]
      have hcount_filter : ∀ k, k ∈ (List.filter (fun b => !b == a) as).eraseDups →
          as.count k = (List.filter (fun b => !b == a) as).count k := by
        intro k hk
        rw [List.mem_eraseDups, List.mem_filter] at hk
        have hne : k ≠ a := by
          have : (!k == a) = true := hk.2
          simp at this
          intro heq; subst heq; simp at this
        exact (count_eq_count_filter_ne a as k hne).symm
      have hmap_eq2 : ((List.filter (fun b => !b == a) as).eraseDups.map
          (fun k => as.count k * f k)).sum =
          ((List.filter (fun b => !b == a) as).eraseDups.map
          (fun k => (List.filter (fun b => !b == a) as).count k * f k)).sum := by
        congr 1; apply List.map_congr_left; intro k hk
        rw [hcount_filter k hk]
      rw [hmap_eq2]
      have hlen_f : (List.filter (fun b => !b == a) as).length ≤ n :=
        Nat.le_trans (List.length_filter_le _ _) hle'
      have ihf := ih _ hlen_f
      rw [ihf]
      have hsplit := sum_filter_split as f (fun b => b == a)
      have hfeq := sum_filter_eq_count_mul as a f
      rw [hsplit, hfeq]
      omega

private theorem count_filter_eq (ys : List Nat) (x k : Nat) :
    (ys.filter (fun y => y == x)).count k = if x == k then ys.count k else 0 := by
  rw [List.count_eq_length_filter]
  rw [List.filter_filter]
  by_cases hxk : x == k
  · have hx_eq_k : x = k := eq_of_beq hxk
    subst k
    simp [List.count_eq_length_filter]
  · have hp : (fun y : Nat => y == k && y == x) = (fun _ => false) := by
      funext y
      by_cases hyx : y == x
      · have hy_eq_x : y = x := eq_of_beq hyx
        subst y
        simp [hxk]
      · simp [hyx]
    rw [hp]
    simp [hxk]

private theorem count_expand_eq_mass (xs ys : List Nat) (k : Nat) :
    (B.expand (xs, ys)).count k = B.mass (xs, ys) k := by
  simp only [B, bag]
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp only [List.flatMap_cons, List.count_append, List.count_cons]
    rw [count_filter_eq, ih]
    by_cases hx : x == k
    · simp [hx]
      rw [Nat.add_mul, Nat.one_mul]
      omega
    · simp [hx]

private theorem sum_filter_toNat (ys : List Nat) (x z : Nat) :
    ((ys.filter (fun y => y == x)).map (fun y => (decide (y ≤ z)).toNat)).sum =
      ys.count x * (decide (x ≤ z)).toNat := by
  exact sum_filter_eq_count_mul ys x (fun y => (decide (y ≤ z)).toNat)

private theorem map_sum_expand (xs ys : List Nat) (z : Nat) :
    (xs.map (fun x => ys.count x * (decide (x ≤ z)).toNat)).sum =
      ((B.expand (xs, ys)).map (fun y => (decide (y ≤ z)).toNat)).sum := by
  induction xs with
  | nil => simp [B, bag]
  | cons x xs ih =>
    simp only [B, bag, List.flatMap_cons, List.map_cons, List.sum_cons,
      List.map_append, List.sum_append]
    rw [sum_filter_toNat, ih]
    rfl

private theorem countP_eq_sum_toNat (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
    cases h : p a <;> simp [h, ih, Nat.add_comm]

private theorem below_eq_countP (xs ys : List Nat) (z : Nat) :
    below B (xs, ys) z = (B.expand (xs, ys)).countP (fun y => decide (y ≤ z)) := by
  have hmain : ((xs.eraseDups.map
        (fun k => (xs.count k * ys.count k) * (decide (k ≤ z)).toNat)).sum) =
      ((B.expand (xs, ys)).map (fun y => (decide (y ≤ z)).toNat)).sum := by
    calc
      ((xs.eraseDups.map
        (fun k => (xs.count k * ys.count k) * (decide (k ≤ z)).toNat)).sum) =
          ((xs.eraseDups.map
        (fun k => xs.count k * (ys.count k * (decide (k ≤ z)).toNat))).sum) := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro k hk
          rw [Nat.mul_assoc]
      _ = (xs.map (fun k => ys.count k * (decide (k ≤ z)).toNat)).sum := by
          exact sum_eraseDups_count_mul xs (fun k => ys.count k * (decide (k ≤ z)).toNat)
      _ = ((B.expand (xs, ys)).map (fun y => (decide (y ≤ z)).toNat)).sum := by
          exact map_sum_expand xs ys z
  change ((xs.eraseDups.map
        (fun k => (xs.count k * ys.count k) * (decide (k ≤ z)).toNat)).sum) =
      (B.expand (xs, ys)).countP (fun y => decide (y ≤ z))
  rw [hmain]
  rw [← countP_eq_sum_toNat]

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

private theorem sorted_mergeSort (l : List Nat) : l.mergeSort.Pairwise (fun a b => a ≤ b) := by
  simpa using (List.pairwise_mergeSort
    (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (by intro a b; simp [Nat.le_total]) l)

private theorem mem_expand_mem_keys (xs ys : List Nat) (k : Nat)
    (h : k ∈ B.expand (xs, ys)) : k ∈ B.keys (xs, ys) := by
  simp only [B, bag] at h ⊢
  induction xs with
  | nil => simp at h
  | cons x xs ih =>
    simp only [List.flatMap_cons, List.mem_append, List.mem_filter] at h
    simp only [List.mem_cons]
    cases h with
    | inl h => exact Or.inl (eq_of_beq h.2)
    | inr h => exact Or.inr (ih h)

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro p i
  obtain ⟨xs, ys⟩ := p
  let E := B.expand (xs, ys)
  let L := E.mergeSort
  let K := (live B (xs, ys)).mergeSort
  let q : Nat → Bool := fun x => i < below B (xs, ys) x
  change K.find? q = L[i]?
  have hpermLE : L.Perm E := by
    dsimp [L]
    exact List.mergeSort_perm E _
  have hbelow (x : Nat) : below B (xs, ys) x = L.countP (fun y => decide (y ≤ x)) := by
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
    have hvmass : 0 < B.mass (xs, ys) v := by
      rw [← count_expand_eq_mass]
      exact List.count_pos_iff.mpr hvE
    have hvkeys : v ∈ B.keys (xs, ys) := mem_expand_mem_keys xs ys v hvE
    have hvlive : v ∈ live B (xs, ys) := by
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
      exact sorted_mergeSort (live B (xs, ys))
    have hnone : (K.take j).find? q = none := by
      rw [List.find?_eq_none]
      intro a ha
      simp only [Bool.not_eq_true, q]
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
    have hqv : q v = true := by
      simp only [q, hbelow, decide_eq_true_eq]
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
    refine ⟨hqv, K.take j, K.drop (j + 1), ?_, ?_⟩
    · calc
        K = K.take j ++ K.drop j := (List.take_append_drop j K).symm
        _ = K.take j ++ (K[j] :: K.drop (j + 1)) := by
          rw [List.drop_eq_getElem_cons hjlt]
        _ = K.take j ++ v :: K.drop (j + 1) := by rw [hvKj]
    · intro a ha
      have hna : ¬ q a = true := (List.find?_eq_none.mp hnone) a ha
      simpa [Bool.not_eq_true] using hna
  · rw [List.getElem?_eq_none (Nat.le_of_not_gt hi)]
    rw [List.find?_eq_none]
    intro x hx
    simp only [Bool.not_eq_true, q]
    rw [hbelow x]
    rw [decide_eq_false_iff_not]
    have hle := List.countP_le_length (p := fun y => decide (y ≤ x)) (l := L)
    have hlen : L.length ≤ i := Nat.le_of_not_gt hi
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_OrderStatistic
