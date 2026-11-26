import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

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
          simp at hm'
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

private theorem count_flatMap {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (g : α → List α) (a : α) :
    (xs.flatMap g).count a = (xs.map fun x => (g x).count a).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.count_append, ih]

private theorem count_add_left (x : Nat) : ∀ (b : List Nat) (k : Nat),
    (b.map fun y => x + y).count k = if x ≤ k then b.count (k - x) else 0 := by
  intro b
  induction b with
  | nil => intro k; simp
  | cons y ys ih =>
      intro k
      by_cases hle : x ≤ k
      · by_cases hy : y = k - x
        · have heq : x + y = k := by omega
          have hbeq1 : (x + y == k) = true := beq_iff_eq.mpr heq
          have hbeq2 : (y == k - x) = true := beq_iff_eq.mpr hy
          simp [List.count_cons, hbeq1, hbeq2, ih, hle]
        · have hybeq : (y == k - x) = false := by simpa [beq_iff_eq] using hy
          have hxyne : x + y ≠ k := by
            intro he
            apply hy
            omega
          have hbeq1 : (x + y == k) = false := by simpa [beq_iff_eq] using hxyne
          simp [List.count_cons, hbeq1, hybeq, ih, hle]
      · have heqfalse : x + y ≠ k := by omega
        have hbeq1 : (x + y == k) = false := by simpa [beq_iff_eq] using heqfalse
        simp [List.count_cons, hbeq1, ih, hle]

/-! ## AdditiveProduct bag facts -/

private theorem mass_eq_sum (a b : List Nat) (k : Nat) :
    bag.mass (a, b) k = (a.map fun x => if x ≤ k then b.count (k - x) else 0).sum := by
  change (a.eraseDups.map fun x => if x ≤ k then a.count x * b.count (k - x) else 0).sum =
    (a.map fun x => if x ≤ k then b.count (k - x) else 0).sum
  rw [show (a.eraseDups.map fun x => if x ≤ k then a.count x * b.count (k - x) else 0) =
       (a.eraseDups.map fun x => a.count x * (if x ≤ k then b.count (k - x) else 0)) by
       apply List.map_congr_left
       intro x hx
       by_cases hle : x ≤ k <;> simp [hle]]
  rw [dedup_weight_sum]

private theorem count_expand_eq_mass (a b : List Nat) (k : Nat) :
    (bag.expand (a, b)).count k = bag.mass (a, b) k := by
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.map (fun y => x + y)) by rfl]
  rw [count_flatMap]
  rw [mass_eq_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  rw [count_add_left]

private theorem mem_keys_of_mem_expand (a b : List Nat) (k : Nat) :
    k ∈ bag.expand (a, b) → k ∈ bag.keys (a, b) := by
  intro h
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.map (fun y => x + y)) by rfl] at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hx, hrest⟩
  rw [List.mem_map] at hrest
  rcases hrest with ⟨y, hy, hxy⟩
  rw [show bag.keys (a, b) = a.eraseDups.flatMap (fun x => b.eraseDups.map (fun y => x + y)) by rfl]
  rw [List.mem_flatMap]
  refine ⟨x, ?_, ?_⟩
  · rw [List.mem_eraseDups]
    exact hx
  · rw [List.mem_map]
    refine ⟨y, ?_, ?_⟩
    · rw [List.mem_eraseDups]
      exact hy
    · exact hxy

private theorem mass_zero_of_not_mem_expand (a b : List Nat) (k : Nat)
    (h : k ∉ bag.expand (a, b)) : bag.mass (a, b) k = 0 := by
  rw [← count_expand_eq_mass]
  exact List.count_eq_zero.mpr h

private theorem weighted_eq_expand_sum (p : List Nat × List Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨a, b⟩
  let m : Nat → Nat := fun k => bag.mass (a, b) k
  change ((bag.keys (a, b)).eraseDups.map fun k => m k * f k).sum =
    ((bag.expand (a, b)).map f).sum
  have hL : ((bag.keys (a, b)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (a, b)).eraseDups.map fun k => m k * f k).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdnot' : d ∉ bag.expand (a, b) := by
        intro hdE
        apply hdnot
        rw [List.mem_eraseDups]
        exact hdE
      simp [m, mass_zero_of_not_mem_expand a b d hdnot']
    · intro d hd hdnot
      exfalso
      apply hdnot
      rw [List.mem_eraseDups]
      rw [List.mem_eraseDups] at hd
      exact mem_keys_of_mem_expand a b d hd
  have hR : ((bag.expand (a, b)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (a, b)).map f).sum := by
    rw [← dedup_weight_sum f (bag.expand (a, b))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    dsimp [m]
    rw [count_expand_eq_mass]
  exact hL.trans hR

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
  | cons x xs ih => simp [pairs]

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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_JoinFanout
