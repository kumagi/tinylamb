import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_AntiRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

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

/-! ## CoGroup bag facts -/

private theorem count_flatMap {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (g : α → List α) (a : α) :
    (xs.flatMap g).count a = (xs.map fun x => (g x).count a).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.count_append, ih]

private theorem count_filter_eq (b : List Nat) (x k : Nat) :
    (b.filter (fun y => y == x)).count k = if x == k then b.count k else 0 := by
  by_cases h : x == k
  · have hxk : x = k := beq_iff_eq.mp h
    subst x
    simp
  · have hz : (b.filter (fun y => y == x)).count k = 0 := by
      rw [List.count_eq_zero]
      intro hm
      have hmem := (List.mem_filter.mp hm).2
      have hxk : x = k := (beq_iff_eq.mp hmem).symm
      exact h (beq_iff_eq.mpr hxk)
    rw [hz]
    simp [h]

private theorem sum_count_constant (a : List Nat) (b : List Nat) (k : Nat) :
    (a.map fun x => if x == k then b.count k else 0).sum = a.count k * b.count k := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxk : x = k := beq_iff_eq.mp hx
        subst x
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        simp
        rw [Nat.succ_mul]
        omega
      · simp only [List.map_cons, List.sum_cons]
        rw [ih]
        simp [List.count_cons, hx]

private theorem count_expand_eq_mass (a b : List Nat) (k : Nat) :
    (bag.expand (a, b)).count k = bag.mass (a, b) k := by
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.filter (fun y => y == x)) by rfl]
  rw [count_flatMap]
  change (a.map fun x => (b.filter (fun y => y == x)).count k).sum = a.count k * b.count k
  rw [show (a.map fun x => (b.filter (fun y => y == x)).count k) =
       (a.map fun x => if x == k then b.count k else 0) by
       apply List.map_congr_left
       intro x hx
       exact count_filter_eq b x k]
  exact sum_count_constant a b k

private theorem mem_keys_of_mem_expand (a b : List Nat) (k : Nat) :
    k ∈ bag.expand (a, b) → k ∈ bag.keys (a, b) := by
  intro h
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.filter (fun y => y == x)) by rfl] at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hx, hrest⟩
  have hxk : x = k := (beq_iff_eq.mp (List.mem_filter.mp hrest).2).symm
  subst x
  rw [show bag.keys (a, b) = a by rfl]
  exact hx

private theorem mass_zero_of_not_mem_expand (a b : List Nat) (k : Nat)
    (h : k ∉ bag.expand (a, b)) : bag.mass (a, b) k = 0 := by
  rw [← count_expand_eq_mass a b k]
  exact List.count_eq_zero.mpr h

/-! ## Goal -/

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rcases a with ⟨a₁, a₂⟩
  rcases c with ⟨c₁, c₂⟩
  let f : Nat → Nat := fun k => if bag.mass (c₁, c₂) k == 0 then k else 0
  change ((bag.keys (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
    ((bag.expand (a₁, a₂)).filter (fun x => !(bag.expand (c₁, c₂)).contains x)).sum

  -- The two distinct-key supports agree wherever the multiplicity is nonzero.
  have hL : ((bag.keys (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
      ((bag.expand (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdnotE : d ∉ bag.expand (a₁, a₂) := by
        intro hdE
        exact hdnot (List.mem_eraseDups.mpr hdE)
      simp [mass_zero_of_not_mem_expand a₁ a₂ d hdnotE]
    · intro d hd hdnot
      exfalso
      exact hdnot (List.mem_eraseDups.mpr (mem_keys_of_mem_expand a₁ a₂ d (List.mem_eraseDups.mp hd)))
  rw [hL]

  -- Regroup by multiplicity: multiplicity equals the count in the expansion.
  have hD0 := dedup_weight_sum f (bag.expand (a₁, a₂))
  have hD : ((bag.expand (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
      ((bag.expand (a₁, a₂)).map f).sum := by
    rw [← hD0]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]
  rw [hD]

  -- The multiplicity of a key on the right is zero exactly when it is absent
  -- from the right expansion, so the `if` guards coincide pointwise.
  have hfilter : ((bag.expand (a₁, a₂)).filter
      (fun x => !(bag.expand (c₁, c₂)).contains x)).sum =
      ((bag.expand (a₁, a₂)).map
        (fun x => if (!(bag.expand (c₁, c₂)).contains x) = true then x else 0)).sum := by
    simpa using (sum_filter_eq_sum_ite (bag.expand (a₁, a₂))
      (fun x => !(bag.expand (c₁, c₂)).contains x) (fun x : Nat => x))
  rw [hfilter]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  have hmass_eq : (bag.mass (c₁, c₂) k = 0) ↔ k ∉ bag.expand (c₁, c₂) := by
    rw [← count_expand_eq_mass c₁ c₂ k]
    exact List.count_eq_zero
  by_cases hzero : bag.mass (c₁, c₂) k = 0
  · have hnotmem : k ∉ bag.expand (c₁, c₂) := hmass_eq.mp hzero
    simp [f, hzero, hnotmem]
  · by_cases hmem : k ∈ bag.expand (c₁, c₂)
    · simp [f, hzero, hmem]
    · exact (hzero (hmass_eq.mpr hmem)).elim

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_AntiRevenue
