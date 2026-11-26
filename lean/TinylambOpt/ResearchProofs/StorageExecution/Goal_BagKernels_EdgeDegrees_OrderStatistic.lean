import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.mem_cons] at ha
      rw [List.count_cons]
      by_cases hba : b = a
      · subst a
        simp [List.count_eq_zero_of_not_mem hn.1]
      · have ha' : a ∈ bs := by
          cases ha with
          | inl h => exact (hba h.symm).elim
          | inr h => exact h
        have hbeq : (b == a) = false := by simp [hba]
        simp [hbeq, ih hn.2 ha']

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem perm_sum {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simpa using ih
  | swap x y l => simp [Nat.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_replicate (n c : Nat) : (List.replicate n c).sum = n * c := by
  induction n with
  | zero => simp
  | succ n ih => simp [Nat.succ_mul]

private theorem sum_flatMap (l : List Nat) (g : Nat → List Nat) :
    (l.flatMap g).sum = (l.map (fun x => (g x).sum)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih, List.sum_append]

private theorem sum_map_ite_of_nodup (d : List Nat) (x : Nat) (c : Nat → Nat) (hd : d.Nodup) :
    (d.map (fun k => if k == x then c k else 0)).sum = if x ∈ d then c x else 0 := by
  induction d with
  | nil => simp
  | cons k ks ih =>
      rw [List.nodup_cons] at hd
      simp only [List.map_cons, List.sum_cons]
      by_cases hkx : k == x
      · have hxks : x ∉ ks := by
          intro hxks
          have hkks : k ∉ ks := hd.1
          have hkx' : k = x := eq_of_beq hkx
          subst x
          exact hkks hxks
        rw [ih hd.2, ite_eq_left hkx, ite_eq_right hxks]
        have hmem : x ∈ k :: ks := by
          have hkx' : x = k := (eq_of_beq hkx).symm
          simp [hkx']
        rw [ite_eq_left hmem]
        have hkx' : x = k := (eq_of_beq hkx).symm
        simp [hkx']
      · have hxk : x ≠ k := by
          intro h
          subst h
          simp at hkx
        rw [ih hd.2]
        simp [hkx, hxk]

/-- Summing `count k l * f k` over the distinct elements of `l` is the same as
    summing `f` over all elements of `l`. -/
private theorem sum_count_eraseDups (l : List Nat) (f : Nat → Nat) :
    (l.eraseDups.map (fun k => l.count k * f k)).sum = (l.map f).sum := by
  let R := l.eraseDups.flatMap (fun k => List.replicate (l.count k) k)
  have hperm : l.Perm R := by
    rw [List.perm_iff_count]
    intro x
    symm
    dsimp [R]
    rw [List.count_flatMap]
    have hnodup : l.eraseDups.Nodup := nodup_eraseDups l
    calc
      (l.eraseDups.map (fun k => (List.replicate (l.count k) k).count x)).sum =
          (l.eraseDups.map (fun k => if k == x then l.count k else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        rw [List.count_replicate]
      _ = if x ∈ l.eraseDups then l.count x else 0 := by
        exact sum_map_ite_of_nodup l.eraseDups x l.count hnodup
      _ = l.count x := by
        by_cases hx : x ∈ l
        · simp [List.mem_eraseDups.mpr hx]
        · have hx' : x ∉ l.eraseDups := by simpa using hx
          simp [hx', List.count_eq_zero_of_not_mem hx]
  have hsum : (R.map f).sum = (l.map f).sum := by
    exact (perm_sum (hperm.map f)).symm
  rw [← hsum]
  dsimp [R]
  rw [List.map_flatMap]
  rw [sum_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  simp

private theorem countP_eq_sum_toNat (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases h : p a <;> simp [h, ih, Nat.add_comm]

private theorem count_expand_eq_mass (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand es).count k = bag.mass es k := by
  induction es with
  | nil => simp [bag]
  | cons e es ih =>
      obtain ⟨a, b⟩ := e
      change (List.count k ([a, b] ++ bag.expand es)) =
        List.count k (a :: es.map Prod.fst) + List.count k (b :: es.map Prod.snd)
      rw [List.count_append, List.count_cons, List.count_cons, ih]
      simp [bag, List.count_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem mass_eq_count_keys (es : List (Nat × Nat)) (k : Nat) :
    bag.mass es k = (bag.keys es).count k := by
  simp [bag, List.count_append]

private theorem keys_perm_expand (es : List (Nat × Nat)) : (bag.keys es).Perm (bag.expand es) := by
  rw [List.perm_iff_count]
  intro k
  rw [← mass_eq_count_keys es k, ← count_expand_eq_mass es k]

private theorem below_eq_countP (es : List (Nat × Nat)) (z : Nat) :
    below bag es z = (bag.expand es).countP (fun y => decide (y ≤ z)) := by
  unfold below weighted
  change ((bag.keys es).eraseDups.map
      (fun k => bag.mass es k * (decide (k ≤ z)).toNat)).sum =
    (bag.expand es).countP (fun y => decide (y ≤ z))
  have hmass : ((bag.keys es).eraseDups.map
      (fun k => bag.mass es k * (decide (k ≤ z)).toNat)).sum =
      ((bag.keys es).eraseDups.map
      (fun k => (bag.keys es).count k * (decide (k ≤ z)).toNat)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count_keys]
  rw [hmass, sum_count_eraseDups]
  rw [← countP_eq_sum_toNat]
  exact (keys_perm_expand es).countP_eq (fun y => decide (y ≤ z))

private theorem sorted_mergeSort (l : List Nat) : l.mergeSort.Pairwise (fun a b => a ≤ b) := by
  simpa using (List.pairwise_mergeSort
    (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (by intro a b; simp [Nat.le_total]) l)

private theorem mem_expand_mem_keys (es : List (Nat × Nat)) (k : Nat)
    (h : k ∈ bag.expand es) : k ∈ bag.keys es := by
  exact (keys_perm_expand es).symm.subset h

theorem sound_OrderStatistic : EdgeDegrees.OrderStatistic.Goal := by
  intro es i
  let E := bag.expand es
  let L := E.mergeSort
  let K := (live bag es).mergeSort
  let p : Nat → Bool := fun x => i < below bag es x
  change K.find? p = L[i]?
  have hpermLE : L.Perm E := by
    dsimp [L]
    exact List.mergeSort_perm E _
  have hbelow (x : Nat) : below bag es x = L.countP (fun y => decide (y ≤ x)) := by
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
    have hvmass : 0 < bag.mass es v := by
      rw [← count_expand_eq_mass]
      exact List.count_pos_iff.mpr hvE
    have hvkeys : v ∈ bag.keys es := mem_expand_mem_keys es v hvE
    have hvlive : v ∈ live bag es := by
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
      exact sorted_mergeSort (live bag es)
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_OrderStatistic
