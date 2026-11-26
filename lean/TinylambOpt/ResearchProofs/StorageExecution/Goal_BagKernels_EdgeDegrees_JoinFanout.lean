import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `EdgeDegrees.JoinFanout` — Evaluate band join count from the two weighted supports.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

-- Sanity check of both sides on a small instance.
#eval (weighted bag [(1, 2), (3, 1)] (fun x =>
        weighted bag [(2, 4), (1, 1)] (fun y => (decide (x ≤ y ∧ y ≤ x + 2)).toNat)),
      (pairs (bag.expand [(1, 2), (3, 1)]) (bag.expand [(2, 4), (1, 1)])
        (fun x y => x ≤ y && y ≤ x + 2)).length)

/-! ## Generic list lemmas -/

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
    by_cases hxy : x ≤ y <;> by_cases hyx : y ≤ x + slack <;> simp [p, hxy, hyx]
  rw [hmap]
  exact sum_ite_bool_eq_filter_length l p

private theorem pairs_length_eq_sum (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = (xs.map fun x => (ys.filter (pred x)).length).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons x xs ih => simp [pairs, ih]

/-! ## EdgeDegrees bag facts -/

private theorem count_expand_eq_mass (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand es).count k = bag.mass es k := by
  induction es with
  | nil => simp [bag]
  | cons e es ih =>
      obtain ⟨a, b⟩ := e
      have ih' : (bag.expand es).count k =
          (es.map Prod.fst).count k + (es.map Prod.snd).count k := by
        simpa [bag] using ih
      by_cases hka : k = a
      · subst k
        by_cases hkb : a = b
        · subst b
          simp [bag] at ih' ⊢
          omega
        · have hbne : b ≠ a := fun h => hkb h.symm
          simp [bag, hbne] at ih' ⊢
          omega
      · by_cases hkb : k = b
        · subst k
          have hane : a ≠ b := fun h => hka h.symm
          simp [bag, hane, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] at ih' ⊢
          omega
        · have hane : a ≠ k := fun h => hka h.symm
          have hbne : b ≠ k := fun h => hkb h.symm
          simpa [bag, hane, hbne] using ih'

private theorem mem_keys_iff_mem_expand (es : List (Nat × Nat)) (k : Nat) :
    k ∈ bag.keys es ↔ k ∈ bag.expand es := by
  induction es with
  | nil => simp [bag]
  | cons e es ih =>
      obtain ⟨a, b⟩ := e
      simp [bag, ih]
      constructor
      · intro h
        rcases h with h | h | h | h
        · exact Or.inl h
        · rcases h with ⟨x, hx⟩
          exact Or.inr (Or.inr ⟨k, x, hx, Or.inl rfl⟩)
        · exact Or.inr (Or.inl h)
        · rcases h with ⟨x, hx⟩
          exact Or.inr (Or.inr ⟨x, k, hx, Or.inr rfl⟩)
      · intro h
        rcases h with h | h | h
        · exact Or.inl h
        · exact Or.inr (Or.inr (Or.inl h))
        · rcases h with ⟨x, y, hxy, hk⟩
          rcases hk with hk | hk
          · subst k
            exact Or.inr (Or.inl ⟨y, hxy⟩)
          · subst k
            exact Or.inr (Or.inr (Or.inr ⟨x, hxy⟩))

private theorem weighted_eq_expand_sum (es : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag es f = ((bag.expand es).map f).sum := by
  unfold weighted
  change ((bag.keys es).eraseDups.map (fun k => bag.mass es k * f k)).sum =
    ((bag.expand es).map f).sum
  have hL : ((bag.keys es).eraseDups.map (fun k => bag.mass es k * f k)).sum =
      ((bag.expand es).eraseDups.map (fun k => bag.mass es k * f k)).sum := by
    apply sum_eq_of_nodup_mem
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d
      simp [List.mem_eraseDups, mem_keys_iff_mem_expand]
  have hR : ((bag.expand es).eraseDups.map (fun k => bag.mass es k * f k)).sum =
      ((bag.expand es).map f).sum := by
    calc
      ((bag.expand es).eraseDups.map (fun k => bag.mass es k * f k)).sum =
          ((bag.expand es).eraseDups.map (fun k => (bag.expand es).count k * f k)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro k hk
            rw [count_expand_eq_mass]
      _ = ((bag.expand es).map f).sum := dedup_weight_sum f (bag.expand es)
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_JoinFanout
