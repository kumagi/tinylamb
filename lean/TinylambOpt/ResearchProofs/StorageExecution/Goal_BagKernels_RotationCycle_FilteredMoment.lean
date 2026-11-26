import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_FilteredMoment

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private theorem mod_add_eq_key_iff (s0 a k q : Nat) (hq : 0 < q) (hs0 : s0 < q) (ha : a < q) (hk : k < q) :
    (s0 + a) % q = k ↔ a = (k + q - s0) % q := by
  constructor
  · intro h
    have hlt2 : s0 + a < q + q := by omega
    have hcases : s0 + a = k ∨ s0 + a = k + q := by
      by_cases hltq : s0 + a < q
      · left
        rw [Nat.mod_eq_of_lt hltq] at h
        omega
      · have hsub := Nat.mod_eq_sub_mod (by omega : s0 + a ≥ q)
        rw [hsub] at h
        have hltq2 : s0 + a - q < q := by omega
        rw [Nat.mod_eq_of_lt hltq2] at h
        right
        omega
    rcases hcases with hlt | hge
    · have hs0k : s0 ≤ k := by omega
      have hr : (k + q - s0) % q = k - s0 := by
        rw [show k + q - s0 = q + (k - s0) by omega]
        rw [Nat.add_mod, Nat.mod_self, Nat.zero_add, Nat.mod_mod]
        exact Nat.mod_eq_of_lt (by omega)
      rw [hr]
      omega
    · by_cases hs0k : s0 ≤ k
      · exfalso
        omega
      · have hr : (k + q - s0) % q = k + q - s0 := Nat.mod_eq_of_lt (by omega)
        rw [hr]
        omega
  · intro har
    by_cases hs0k : s0 ≤ k
    · have hr : (k + q - s0) % q = k - s0 := by
        rw [show k + q - s0 = q + (k - s0) by omega]
        rw [Nat.add_mod, Nat.mod_self, Nat.zero_add, Nat.mod_mod]
        exact Nat.mod_eq_of_lt (by omega)
      rw [har, hr]
      have : s0 + (k - s0) = k := by omega
      rw [this, Nat.mod_eq_of_lt hk]
    · have hr : (k + q - s0) % q = k + q - s0 := Nat.mod_eq_of_lt (by omega)
      rw [har, hr]
      have : s0 + (k + q - s0) = k + q := by omega
      rw [this]
      rw [Nat.mod_eq_sub_mod (by omega : k + q ≥ q)]
      rw [show k + q - q = k by omega, Nat.mod_eq_of_lt hk]

private theorem succ_mod (n q : Nat) (hq : 0 < q) :
    (n+1) % q = if n % q + 1 < q then n % q + 1 else 0 := by
  have hmod : (n+1) % q = (n % q + 1) % q := by
    rw [show n + 1 = 1 + n by omega, ← Nat.add_mod_mod, show 1 + n % q = n % q + 1 by omega]
  rw [hmod]
  by_cases h : n % q + 1 < q
  · simp [h, Nat.mod_eq_of_lt h]
  · have heq : n % q + 1 = q := by
      have hnq : n % q < q := Nat.mod_lt n hq
      omega
    simp [heq, Nat.mod_self]

private theorem succ_div_eq (n q : Nat) (_hq : 0 < q) :
    (n+1) / q = n / q + if (n+1) % q = 0 then 1 else 0 := by
  rw [Nat.succ_div]
  by_cases h : q ∣ n + 1
  · have hm : (n+1) % q = 0 := Nat.mod_eq_zero_of_dvd h
    simp [h, hm]
  · have hm : (n+1) % q ≠ 0 := by intro h'; exact h (Nat.dvd_of_mod_eq_zero h')
    simp [h, hm]

private theorem key_iff (n m s k : Nat) (hk : k ≤ m) :
    (s + n) % (m+1) = k ↔ n % (m+1) = (k + (m+1) - s % (m+1)) % (m+1) := by
  have hq : 0 < m+1 := by omega
  rw [Nat.add_mod]
  exact mod_add_eq_key_iff (s % (m+1)) (n % (m+1)) k (m+1) hq
    (Nat.mod_lt s hq) (Nat.mod_lt n hq) (by omega)

private theorem mass_succ (n m s k : Nat) :
    bag.mass (n+1, m, s) k = bag.mass (n, m, s) k + (decide ((s+n) % (m+1) == k)).toNat := by
  unfold bag
  simp only
  by_cases hk : k ≤ m
  · have hq : 0 < m+1 := by omega
    let q := m+1
    let r := (k + q - s % q) % q
    have hrq : r < q := Nat.mod_lt _ hq
    have haq : n % q < q := Nat.mod_lt n hq
    have hkey := key_iff n m s k hk
    have hsmod := succ_mod n q hq
    have hsdiv := succ_div_eq n q hq
    change (if k ≤ m then (n+1)/q + (decide (r < (n+1)%q)).toNat else 0) =
      (if k ≤ m then n/q + (decide (r < n%q)).toNat else 0) +
        (decide ((s+n)%q == k)).toNat
    simp [hk]
    have hbool : (decide ((s+n)%q = k)).toNat = (decide (n%q = r)).toNat := by
      by_cases hsn : (s+n)%q = k
      · have hnr : n%q = r := hkey.mp hsn
        simp [hsn, hnr]
      · have hnr : n%q ≠ r := by intro h; exact hsn (hkey.mpr h)
        simp [hsn, hnr]
    rw [hbool]
    by_cases hlt : n % q + 1 < q
    · have hsm : (n+1)%q = n%q+1 := by simpa [hlt] using hsmod
      have hsd : (n+1)/q = n/q := by
        have hz : (n+1)%q ≠ 0 := by rw [hsm]; omega
        simpa [hz] using hsdiv
      rw [hsm, hsd]
      by_cases hra : r < n%q
      · have hralt : r < n%q+1 := by omega
        have hneq : n%q ≠ r := by omega
        simp [hra, hralt, hneq]
      · by_cases hreq : r = n%q
        · have hreq' : n%q = r := hreq.symm
          simp [hreq']
        · have hrnlt : ¬ r < n%q+1 := by
            intro hrlt
            have : r ≤ n%q := by omega
            omega
          have hreq' : n%q ≠ r := by omega
          simp [hra, hrnlt, hreq']
    · have hnq1 : n % q + 1 = q := by
        have hnq : n%q < q := Nat.mod_lt n hq
        omega
      have hsm : (n+1)%q = 0 := by simpa [hnq1] using hsmod
      have hsd : (n+1)/q = n/q+1 := by
        simpa [hsm] using hsdiv
      rw [hsm, hsd]
      by_cases hra : r < n%q
      · have hneq : n%q ≠ r := by omega
        simp [hra, hneq]
      · have hreq : n%q = r := by
          have hnq : n%q < q := Nat.mod_lt n hq
          omega
        simp [hreq]
  · have hkm : m < k := by omega
    have hqle : m+1 ≤ k := by omega
    have hlt : (s+n) % (m+1) < m+1 := Nat.mod_lt _ (by omega)
    have hne : (s+n) % (m+1) ≠ k := by omega
    have hbeq : ((s+n) % (m+1) == k) = false :=
      Bool.eq_false_of_ne_true (by intro h; exact hne (beq_iff_eq.mp h))
    simp [hk, hbeq]

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

private theorem sum_map_ite_filter {α : Type} (p : α → Bool) (f : α → Nat) (l : List α) :
    (l.map (fun x => if p x then f x else 0)).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a <;> simp [h, ih]

private theorem prop_toNat_decide (P : Prop) [Decidable P] : (if P then 1 else 0) = (decide P).toNat := by
  by_cases h : P <;> simp [h]

private theorem count_expand_eq_mass (n m s k : Nat) :
    (bag.expand (n,m,s)).count k = bag.mass (n,m,s) k := by
  induction n with
  | zero => simp [bag]
  | succ n ih =>
      have hexp : bag.expand (n+1, m, s) = bag.expand (n, m, s) ++ [(s+n) % (m+1)] := by
        simp [bag, List.range_succ]
      rw [hexp]
      simp only [List.count_append, List.count_cons, List.count_nil]
      rw [ih, mass_succ]
      simp [prop_toNat_decide]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rcases p with ⟨n, m, s⟩
  let f : Nat → Nat := fun x => if lo ≤ x && x < hi then x * x else 0
  let hsq : Nat → Nat := fun x => x * x
  let pr : Nat → Bool := fun x => lo ≤ x && x < hi
  unfold weighted
  change ((bag.keys (n, m, s)).eraseDups.map (fun k => bag.mass (n, m, s) k * f k)).sum =
    (((bag.expand (n, m, s)).filter pr).map hsq).sum

  have hR1 : (((bag.expand (n, m, s)).filter pr).map hsq).sum =
      ((bag.expand (n, m, s)).map f).sum := by
    rw [sum_map_ite_filter pr hsq]

  have hR2 : ((bag.expand (n, m, s)).map f).sum =
      (((bag.expand (n, m, s)).eraseDups.map fun k => bag.mass (n, m, s) k * f k).sum) := by
    rw [← dedup_weight_sum f]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]

  have hL : ((bag.keys (n, m, s)).eraseDups.map (fun k => bag.mass (n, m, s) k * f k)).sum =
      ((bag.expand (n, m, s)).eraseDups.map (fun k => bag.mass (n, m, s) k * f k)).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdnot' : d ∉ bag.expand (n, m, s) := by
        intro hde
        exact hdnot (by rw [List.mem_eraseDups]; exact hde)
      have hm : bag.mass (n, m, s) d = 0 := by
        rw [← count_expand_eq_mass]
        exact List.count_eq_zero.mpr hdnot'
      simp [hm]
    · intro d hd hdnot
      have hde : d ∈ bag.expand (n, m, s) := by
        rw [List.mem_eraseDups] at hd
        exact hd
      exfalso
      apply hdnot
      rw [List.mem_eraseDups]
      change d ∈ List.range (m+1)
      rw [List.mem_range]
      change d ∈ (List.range n).map (fun i => (s+i) % (m+1)) at hde
      rw [List.mem_map] at hde
      rcases hde with ⟨i, hi, hval⟩
      rw [← hval]
      exact Nat.mod_lt _ (by omega)

  calc
    ((bag.keys (n, m, s)).eraseDups.map (fun k => bag.mass (n, m, s) k * f k)).sum
        = ((bag.expand (n, m, s)).eraseDups.map (fun k => bag.mass (n, m, s) k * f k)).sum := hL
    _ = ((bag.expand (n, m, s)).map f).sum := hR2.symm
    _ = (((bag.expand (n, m, s)).filter pr).map hsq).sum := hR1.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_FilteredMoment
