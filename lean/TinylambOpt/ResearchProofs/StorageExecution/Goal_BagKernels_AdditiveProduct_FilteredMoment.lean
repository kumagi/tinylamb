import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_FilteredMoment

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

private abbrev B := bag

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
            have hamem' : a ∈ ks.filter (fun b => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simp [h] at hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            have hkaeq : k = a := beq_iff_eq.mp h
            have haeq : (a == k) = true := beq_iff_eq.mpr hkaeq.symm
            simp [haeq] at hane)
          simp [List.count_cons, hka, hrestc]
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

private theorem sum_filter_eq_sum_ite {α : Type} (L : List α) (p : α → Bool) (g : α → Nat) :
    ((L.filter p).map g).sum = (L.map fun x => if p x = true then g x else 0).sum := by
  induction L with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

private theorem count_eraseDups_le_one {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.eraseDups.count a ≤ 1 := by
  let motive : Nat → Prop := fun n => ∀ (l : List α), l.length = n → ∀ a, l.eraseDups.count a ≤ 1
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen a
    cases l with
    | nil => simp
    | cons b bs =>
        rw [List.eraseDups_cons, List.count_cons]
        let rest := bs.filter (fun c => !(c == b))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun c : α => !(c == b)) bs)
            (Nat.lt_succ_self _)
        have ihrest : ∀ c, rest.eraseDups.count c ≤ 1 := by
          intro c
          exact ih rest.length hrestlen rest rfl c
        by_cases hba : b == a
        · have hzero : rest.eraseDups.count a = 0 := by
            rw [List.count_eq_zero]
            intro hm
            rw [List.mem_eraseDups] at hm
            have hf := (List.mem_filter.mp hm).2
            have hab : (a == b) = true := beq_iff_eq.mpr (beq_iff_eq.mp hba).symm
            simp [hab] at hf
          have hz' : (List.filter (fun c : α => !(c == b)) bs).eraseDups.count a = 0 := by
            simpa [rest] using hzero
          simp [hba, hz']
        · have hbfalse : (b == a) = false := Bool.eq_false_of_ne_true hba
          simp [hbfalse]
          exact ihrest a
  exact H l rfl a

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup :=
  List.nodup_iff_count.mpr (fun a => count_eraseDups_le_one l a)

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

private theorem sum_nat_eq_zero_of_mem_eq_zero {l : List Nat} (h : ∀ x ∈ l, x = 0) : l.sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : x = 0 := h x (by simp)
      have hxs : xs.sum = 0 := ih (by intro y hy; exact h y (by simp [hy]))
      simp [hx, hxs]

/-! ## AdditiveProduct bag facts -/

private theorem count_add_map (x k : Nat) (b : List Nat) :
    (b.map (fun y => x + y)).count k = if x ≤ k then b.count (k - x) else 0 := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.count_cons, ih]
      by_cases hx : x ≤ k
      · by_cases hxy : x + y = k
        · have hy : y = k - x := by omega
          simp [hx, hy, Nat.add_sub_of_le hx]
        · have hy : y ≠ k - x := by intro hy'; apply hxy; omega
          simp [hx, hxy, hy]
      · have hne : x + y ≠ k := by omega
        simp [hx, hne]

private theorem count_expand_eq_mass (a b : List Nat) (k : Nat) :
    (B.expand (a, b)).count k = B.mass (a, b) k := by
  unfold B bag
  simp only
  rw [List.count_flatMap]
  rw [← dedup_weight_sum (List.count k ∘ fun x => b.map (fun y => x + y)) a]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  rw [count_add_map]
  by_cases hxk : x ≤ k <;> simp [hxk]

private theorem mem_keys_of_mem_expand (a b : List Nat) (k : Nat) :
    k ∈ B.expand (a, b) → k ∈ B.keys (a, b) := by
  intro h
  change k ∈ a.flatMap (fun x => b.map (fun y => x + y)) at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hxa, hkx⟩
  rw [List.mem_map] at hkx
  rcases hkx with ⟨y, hyb, hxy⟩
  change k ∈ a.eraseDups.flatMap (fun x => b.eraseDups.map (fun y => x + y))
  rw [List.mem_flatMap]
  refine ⟨x, ?_, ?_⟩
  · rw [List.mem_eraseDups]
    exact hxa
  · rw [List.mem_map]
    refine ⟨y, ?_, ?_⟩
    · rw [List.mem_eraseDups]
      exact hyb
    · omega

private theorem mass_zero_of_not_mem_expand (a b : List Nat) (k : Nat)
    (h : k ∉ B.expand (a, b)) : B.mass (a, b) k = 0 := by
  rw [← count_expand_eq_mass a b k]
  exact List.count_eq_zero.mpr h

/-! ## Goal -/

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rcases p with ⟨a, b⟩
  let f : Nat → Nat := fun x => if lo ≤ x && x < hi then x * x else 0
  let hsq : Nat → Nat := fun x => x * x
  let p : Nat → Bool := fun x => lo ≤ x && x < hi
  change ((B.keys (a, b)).eraseDups.map (fun k => B.mass (a, b) k * f k)).sum =
    (((B.expand (a, b)).filter p).map hsq).sum

  have hR1 : (((B.expand (a, b)).filter p).map hsq).sum = ((B.expand (a, b)).map f).sum := by
    rw [sum_filter_eq_sum_ite (B.expand (a, b)) p hsq]

  have hR2 : ((B.expand (a, b)).map f).sum =
      (((B.expand (a, b)).eraseDups.map fun k => B.mass (a, b) k * f k).sum) := by
    rw [← dedup_weight_sum f (B.expand (a, b))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]

  have hL : ((B.keys (a, b)).eraseDups.map (fun k => B.mass (a, b) k * f k)).sum =
      ((B.expand (a, b)).eraseDups.map (fun k => B.mass (a, b) k * f k)).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdnot' : d ∉ B.expand (a, b) := by
        intro hde
        exact hdnot (by rw [List.mem_eraseDups]; exact hde)
      have hm := mass_zero_of_not_mem_expand a b d hdnot'
      simp [hm]
    · intro d hd hdnot
      have hde : d ∈ B.expand (a, b) := by
        rw [List.mem_eraseDups] at hd
        exact hd
      exfalso
      apply hdnot
      rw [List.mem_eraseDups]
      exact mem_keys_of_mem_expand a b d hde

  calc
    ((B.keys (a, b)).eraseDups.map (fun k => B.mass (a, b) k * f k)).sum
        = ((B.expand (a, b)).eraseDups.map (fun k => B.mass (a, b) k * f k)).sum := hL
    _ = ((B.expand (a, b)).map f).sum := hR2.symm
    _ = (((B.expand (a, b)).filter p).map hsq).sum := hR1.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_FilteredMoment
