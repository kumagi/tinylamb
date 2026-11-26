import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DeletedOrdinals.JoinFanout` — Evaluate band join count from the two weighted
  supports.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

/-! ## Generic list lemmas -/

private theorem sum_map_eq_count_head {α : Type} [BEq α] [LawfulBEq α]
    (k : α) (ks : List α) (f : α → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · have hkx : (k == x) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hx (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem dedup_weight_sum (f : Nat → Nat) (keys : List Nat) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Nat, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => !(b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
            (Nat.lt_succ_self _)
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        have hhead : (List.count k (k :: ks)) * f k = f k + ks.count k * f k := by
          rw [List.count_cons]
          simp [Nat.add_mul, Nat.add_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            apply Bool.eq_false_of_ne_true
            intro h
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b : Nat => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simp [h] at hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hkane : k ≠ a := by
            intro h
            subst k
            simp at hane
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            exact hkane (beq_iff_eq.mp h))
          rw [List.count_cons, hka, hrestc]
          simp
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a as =>
        let rest := as.filter (fun b => !(b == a))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : α => !(b == a)) as)
            (Nat.lt_succ_self _)
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro h
          have hm : a ∈ rest.eraseDups := h
          rw [List.mem_eraseDups] at hm
          have hm' := (List.mem_filter.mp hm).2
          have hself : (a == a) = true := beq_iff_eq.mpr rfl
          simp [hself] at hm'
        · exact ihrest
  exact H l rfl

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  have hle : l.count a ≤ 1 := List.nodup_iff_count.mp hn a
  have hpos : 0 < l.count a := List.count_pos_iff.mpr ha
  omega

private theorem sum_eq_of_nodup_mem (l₁ l₂ : List Nat) (f : Nat → Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  have hperm : l₁.Perm l₂ := List.perm_iff_count.mpr (by
    intro d
    by_cases hd : d ∈ l₁
    · have hd2 : d ∈ l₂ := (hmem d).mp hd
      rw [count_eq_one_of_mem_nodup hnd₁ hd, count_eq_one_of_mem_nodup hnd₂ hd2]
    · have hd2 : d ∉ l₂ := by intro h; exact hd ((hmem d).mpr h)
      rw [List.count_eq_zero.mpr hd, List.count_eq_zero.mpr hd2])
  exact List.Perm.sum_nat (hperm.map f)

private theorem sum_map_filter_eq {α : Type} (l : List α) (f : α → Nat) (p : α → Bool)
    (hz : ∀ x ∈ l, p x = false → f x = 0) :
    (l.map f).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases h : p x = true
      · simp [h]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        have hx := hz x (by simp) hf
        simp [h, hx]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)

private theorem sum_eq_of_mem_or_zero (l₁ l₂ : List Nat) (f : Nat → Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (h₁ : ∀ d ∈ l₁, d ∉ l₂ → f d = 0)
    (h₂ : ∀ d ∈ l₂, d ∉ l₁ → f d = 0) :
    (l₁.map f).sum = (l₂.map f).sum := by
  let p : Nat → Bool := fun d => f d ≠ 0
  have hz₁ : ∀ d ∈ l₁, p d = false → f d = 0 := by
    intro d hd hp
    by_cases hne : f d = 0
    · exact hne
    · have ht : p d = true := by simp [p, hne]
      simp [ht] at hp
  have hz₂ : ∀ d ∈ l₂, p d = false → f d = 0 := by
    intro d hd hp
    by_cases hne : f d = 0
    · exact hne
    · have ht : p d = true := by simp [p, hne]
      simp [ht] at hp
  have e₁ : (l₁.map f).sum = ((l₁.filter p).map f).sum := sum_map_filter_eq l₁ f p hz₁
  have e₂ : (l₂.map f).sum = ((l₂.filter p).map f).sum := sum_map_filter_eq l₂ f p hz₂
  have hmem : ∀ d, d ∈ l₁.filter p ↔ d ∈ l₂.filter p := by
    intro d
    simp only [List.mem_filter]
    constructor
    · rintro ⟨hd1, hp1⟩
      by_cases hd2 : d ∈ l₂
      · exact ⟨hd2, hp1⟩
      · have hf0 := h₁ d hd1 hd2
        simp [p, hf0] at hp1
    · rintro ⟨hd2, hp2⟩
      by_cases hd1 : d ∈ l₁
      · exact ⟨hd1, hp2⟩
      · have hf0 := h₂ d hd2 hd1
        simp [p, hf0] at hp2
  have hnd₁' : (l₁.filter p).Nodup := List.Sublist.nodup List.filter_sublist hnd₁
  have hnd₂' : (l₂.filter p).Nodup := List.Sublist.nodup List.filter_sublist hnd₂
  have heq : ((l₁.filter p).map f).sum = ((l₂.filter p).map f).sum :=
    sum_eq_of_nodup_mem _ _ f hnd₁' hnd₂' hmem
  omega

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem decide_le_toNat (x y slack : Nat) :
    (decide (x ≤ y ∧ y ≤ x + slack)).toNat =
      (if x ≤ y && y ≤ x + slack then 1 else 0) := by
  by_cases hxy : x ≤ y <;> by_cases hyx : y ≤ x + slack <;> simp [hxy, hyx]

private theorem sum_ite_bool_eq_filter_length (l : List Nat) (p : Nat → Bool) :
    (l.map fun y => if p y then 1 else 0).sum = (l.filter p).length := by
  induction l with
  | nil => simp
  | cons y ys ih => by_cases h : p y <;> simp [h, ih, Nat.add_comm]

private theorem sum_indicator_eq_filter_length (l : List Nat) (x slack : Nat) :
    (l.map (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat)).sum =
      (l.filter (fun y => x ≤ y && y ≤ x + slack)).length := by
  let p : Nat → Bool := fun y => x ≤ y && y ≤ x + slack
  have hmap : (l.map (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat)) =
      (l.map fun y => if p y then 1 else 0) := by
    apply List.map_congr_left
    intro y hy
    simpa [p] using decide_le_toNat x y slack
  rw [hmap]
  exact sum_ite_bool_eq_filter_length l p

private theorem pairs_length_eq_sum (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = (xs.map fun x => (ys.filter (pred x)).length).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons x xs ih => simp [pairs, ih]

/-! ## DeletedOrdinals bag facts -/

private theorem countP_as_sum (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun i => (p i).toNat)).sum := by
  induction l with
  | nil => simp
  | cons i is ih => rw [List.countP_cons, List.map_cons, List.sum_cons, ih]; by_cases h : p i <;> simp [h, Nat.add_comm]

private theorem sum_split_nodup (l : List Nat) (hl : l.Nodup) (a : Nat) (p q : Nat → Nat)
    (h : ∀ i, i ∈ l → p i = q i + (if i == a then 1 else 0)) :
    (l.map p).sum = (l.map q).sum + (if a ∈ l then 1 else 0) := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hparts := List.nodup_cons.mp hl
      have hbs : bs.Nodup := hparts.2
      have hbne : b ∉ bs := hparts.1
      rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons,
        ih hbs (by intro i hi; exact h i (by simp [hi]))]
      simp only [List.mem_cons]
      by_cases hba : b = a
      · subst b
        have hnot : a ∉ bs := hbne
        have hself : (a == a) = true := beq_iff_eq.mpr rfl
        have hpb := h a (by simp)
        simp [hself, hnot] at hpb ⊢
        omega
      · have hba' : (b == a) = false := Bool.eq_false_of_ne_true (by
          intro h'
          exact hba (beq_iff_eq.mp h'))
        have hab : a ≠ b := by intro h'; exact hba h'.symm
        have hpb := h b (by simp)
        simp [hba', hab] at hpb ⊢
        omega

private theorem deleted_pred_step_eq (x : Nat) (xs : List Nat) (k n i : Nat) (hx : (x == k) = true) :
    (decide (n ≤ i) && (x :: xs)[i - n]? == some k).toNat =
      (decide (n + 1 ≤ i) && xs[i - (n + 1)]? == some k).toNat + (if i == n then 1 else 0) := by
  by_cases hin : i < n
  · have hnle : ¬ n ≤ i := Nat.not_le.mpr hin
    have hnle1 : ¬ n + 1 ≤ i := by omega
    have hine : (i == n) = false := Bool.eq_false_of_ne_true (by
      intro h
      have h' : i = n := beq_iff_eq.mp h
      omega)
    simp [hnle, hnle1, hine]
  · have hni : n ≤ i := Nat.le_of_not_gt hin
    by_cases heq : i = n
    · subst i
      have hnle1 : ¬ n + 1 ≤ n := by omega
      have hself : (n == n) = true := beq_iff_eq.mpr rfl
      simp [hnle1, hself, hx]
    · have hlt : n < i := by omega
      let j := i - (n + 1)
      have hdecomp : i = n + 1 + j := by omega
      have hsub1 : n + 1 + j - n = j + 1 := by omega
      have hsub2 : n + 1 + j - (n + 1) = j := by omega
      have hine : (n + 1 + j == n) = false := by
        apply Bool.eq_false_of_ne_true
        intro h
        have := beq_iff_eq.mp h
        omega
      rw [hdecomp, hsub1, hsub2]
      simp only [List.getElem?_cons_succ]
      have hnle1 : n + 1 ≤ n + 1 + j := by omega
      have hnle0 : n ≤ n + 1 + j := by omega
      simp [hnle1, hnle0, hine]

private theorem deleted_count_step (x : Nat) (xs ds : List Nat) (k n : Nat) :
    ds.eraseDups.countP (fun i => decide (n ≤ i) && (x :: xs)[i - n]? == some k) =
      ds.eraseDups.countP (fun i => decide (n + 1 ≤ i) && xs[i - (n + 1)]? == some k) +
        (if ds.contains n && x == k then 1 else 0) := by
  rw [countP_as_sum, countP_as_sum]
  let p : Nat → Nat := fun i => (decide (n ≤ i) && (x :: xs)[i - n]? == some k).toNat
  let q : Nat → Nat := fun i => (decide (n + 1 ≤ i) && xs[i - (n + 1)]? == some k).toNat
  change (ds.eraseDups.map p).sum = (ds.eraseDups.map q).sum +
    (if (ds.contains n && x == k) = true then 1 else 0)
  by_cases hx : (x == k) = true
  · have hpq : ∀ i, i ∈ ds.eraseDups → p i = q i + (if i == n then 1 else 0) := by
      intro i hi
      dsimp [p, q]
      exact deleted_pred_step_eq x xs k n i hx
    rw [sum_split_nodup ds.eraseDups (eraseDups_nodup ds) n p q hpq]
    by_cases hn : n ∈ ds
    · have hn' : n ∈ ds.eraseDups := List.mem_eraseDups.mpr hn
      have hc : ds.contains n = true := by
        change List.elem n ds = true
        rw [List.elem_eq_mem]
        simp [hn]
      simp [hn', hn, hc, hx]
    · have hn' : n ∉ ds.eraseDups := by intro h; exact hn (List.mem_eraseDups.mp h)
      have hc : ds.contains n = false := by
        change List.elem n ds = false
        rw [List.elem_eq_mem]
        simp [hn]
      simp [hn', hn, hc]
  · have hxf : (x == k) = false := Bool.eq_false_of_ne_true hx
    have hpq : ∀ i, i ∈ ds.eraseDups → p i = q i := by
      intro i hi
      -- same pointwise equality, with no head contribution
      by_cases hin : i < n
      · have hnle : ¬ n ≤ i := Nat.not_le.mpr hin
        have hnle1 : ¬ n + 1 ≤ i := by omega
        dsimp [p, q]
        simp [hnle, hnle1]
      · have hni : n ≤ i := Nat.le_of_not_gt hin
        by_cases heq : i = n
        · subst i
          have hnle1 : ¬ n + 1 ≤ n := by omega
          dsimp [p, q]
          simp [hnle1, hxf]
        · have hlt : n < i := by omega
          let j := i - (n + 1)
          have hi_sub : i - n = j + 1 := by omega
          have hi_sub2 : i - (n + 1) = j := rfl
          dsimp [p, q]
          rw [hi_sub, hi_sub2]
          simp only [List.getElem?_cons_succ]
          have hnle1 : n + 1 ≤ i := by omega
          have hnle0 : n ≤ i := by omega
          simp [hnle1, hnle0]
    have hsum : (ds.eraseDups.map p).sum = (ds.eraseDups.map q).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro i hi
      exact hpq i hi
    rw [hsum]
    simp [hxf]

private theorem expand_count_add_deleted_count (xs ds : List Nat) (k n : Nat) :
    (((xs.zipIdx n).filter (fun e => !ds.contains e.2)).map Prod.fst).count k +
      ds.eraseDups.countP (fun i => decide (n ≤ i) && xs[i - n]? == some k) = xs.count k := by
  induction xs generalizing n ds k with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons]
      by_cases hdn : n ∈ ds
      · have hp : (!(ds.contains n)) = false := by
          change (!(List.elem n ds)) = false
          rw [List.elem_eq_mem]
          simp [hdn]
        rw [List.filter_cons]
        rw [hp]
        simp
        rw [deleted_count_step x xs ds k n]
        rw [List.count_cons]
        have htail := ih (n := n + 1) (ds := ds) (k := k)
        have hc : ds.contains n = true := by
          change List.elem n ds = true
          rw [List.elem_eq_mem]
          simp [hdn]
        rw [hc]
        simp
        by_cases hx : (x == k) = true
        · simp [hx] at htail ⊢
          omega
        · have hxf : (x == k) = false := Bool.eq_false_of_ne_true hx
          simp [hxf] at htail ⊢
          omega
      · have hp : (!(ds.contains n)) = true := by
          change (!(List.elem n ds)) = true
          rw [List.elem_eq_mem]
          simp [hdn]
        rw [List.filter_cons]
        rw [hp]
        simp only [ite_true, List.map_cons]
        rw [deleted_count_step x xs ds k n]
        rw [List.count_cons]
        have htail := ih (n := n + 1) (ds := ds) (k := k)
        have hc : ds.contains n = false := by
          change List.elem n ds = false
          rw [List.elem_eq_mem]
          simp [hdn]
        rw [hc]
        simp
        rw [List.count_cons]
        by_cases hx : (x == k) = true
        · have hx' : x = k := beq_iff_eq.mp hx
          simp [hx'] at htail ⊢
          omega
        · have hxf : (x == k) = false := Bool.eq_false_of_ne_true hx
          have hxf' : x ≠ k := by
            intro h
            have hb : (x == k) = true := beq_iff_eq.mpr h
            simp [hb] at hxf
          simp [hxf'] at htail ⊢
          omega

private theorem mass_eq_count (xs ds : List Nat) (k : Nat) :
    bag.mass (xs, ds) k = (bag.expand (xs, ds)).count k := by
  change xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k) =
    (((xs.zipIdx).filter (fun e => !ds.contains e.2)).map Prod.fst).count k
  have H := expand_count_add_deleted_count xs ds k 0
  change (((xs.zipIdx).filter (fun e => !ds.contains e.2)).map Prod.fst).count k +
      ds.eraseDups.countP (fun i => xs[i]? == some k) = xs.count k at H
  omega

private theorem mem_keys_of_mem_expand (xs ds : List Nat) (k : Nat) :
    k ∈ bag.expand (xs, ds) → k ∈ bag.keys (xs, ds) := by
  intro h
  change k ∈ xs
  change k ∈ (xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst at h
  rw [List.mem_map] at h
  rcases h with ⟨e, he_f, heq⟩
  have he : e ∈ xs.zipIdx := (List.mem_filter.mp he_f).1
  have hz := List.mem_zipIdx he
  rw [← heq, hz.2.2]
  have hlt : e.snd - 0 < xs.length := by omega
  exact List.getElem_mem hlt

private theorem mass_zero_of_not_mem_expand (xs ds : List Nat) (k : Nat)
    (h : k ∉ bag.expand (xs, ds)) : bag.mass (xs, ds) k = 0 := by
  rw [mass_eq_count]
  exact List.count_eq_zero.mpr h

private theorem weighted_eq_expand_sum (p : List Nat × List Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨xs, ds⟩
  let m : Nat → Nat := fun k => bag.mass (xs, ds) k
  change ((bag.keys (xs, ds)).eraseDups.map fun k => m k * f k).sum =
    ((bag.expand (xs, ds)).map f).sum
  have hL : ((bag.keys (xs, ds)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (xs, ds)).eraseDups.map fun k => m k * f k).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro x hx hxnot
      have hxnot' : x ∉ bag.expand (xs, ds) := by
        intro hxE
        apply hxnot
        rw [List.mem_eraseDups]
        exact hxE
      simp [m, mass_zero_of_not_mem_expand xs ds x hxnot']
    · intro x hx hxnot
      exfalso
      apply hxnot
      rw [List.mem_eraseDups]
      rw [List.mem_eraseDups] at hx
      exact mem_keys_of_mem_expand xs ds x hx
  have hR : ((bag.expand (xs, ds)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (xs, ds)).map f).sum := by
    rw [← dedup_weight_sum f (bag.expand (xs, ds))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    dsimp [m]
    rw [← mass_eq_count]
  exact hL.trans hR

/-! ## Goal -/

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  calc
    weighted bag a (fun x => weighted bag c
        (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat))
        = ((bag.expand a).map (fun x => weighted bag c
            (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat))).sum := by
              rw [weighted_eq_expand_sum]
    _ = ((bag.expand a).map (fun x =>
          ((bag.expand c).filter (fun y => x ≤ y && y ≤ x + slack)).length)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro x hx
            rw [weighted_eq_expand_sum]
            exact sum_indicator_eq_filter_length (bag.expand c) x slack
    _ = (pairs (bag.expand a) (bag.expand c) (fun x y => x ≤ y && y ≤ x + slack)).length := by
            rw [pairs_length_eq_sum]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_JoinFanout
