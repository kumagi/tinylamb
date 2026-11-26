import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_AntiRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

private theorem count_zipIdx_index (d : List Nat) (s k i : Nat) :
    (((d.zipIdx s).filter (fun e => e.1 == k)).map (fun e => (decide (e.2 == i)).toNat)).sum =
      if s ≤ i then (decide (d[i - s]? == some k)).toNat else 0 := by
  induction d generalizing s i with
  | nil =>
      simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.filter_cons]
      by_cases hxk : x == k
      · rw [if_pos hxk]
        simp only [List.map_cons, List.sum_cons]
        simp only [beq_iff_eq]
        by_cases hs : s ≤ i
        · by_cases his : i = s
          · subst i
            have ht :
                (List.map (fun e => (decide (e.2 = s)).toNat)
                  (List.filter (fun e => e.1 == k) (xs.zipIdx (s + 1)))).sum = 0 := by
              have ht' :
                  (List.map (fun e => (decide (e.2 = s)).toNat)
                    (List.filter (fun e => e.1 == k) (xs.zipIdx (s + 1)))).sum =
                    if s + 1 ≤ s then (decide (xs[s - (s + 1)]? == some k)).toNat else 0 := by
                simpa [beq_iff_eq] using (ih (s + 1) s)
              rw [ht']
              simp [show ¬ s + 1 ≤ s by omega]
            rw [ht]
            have hxkeq : x = k := beq_iff_eq.mp hxk
            simp [hxkeq]
          · have hsi : s + 1 ≤ i := by omega
            have hsne : s ≠ i := by omega
            have hIdx : i - s = (i - (s + 1)) + 1 := by omega
            rw [hIdx, List.getElem?_cons_succ]
            rw [show (List.map (fun e => (decide (e.2 = i)).toNat)
                  (List.filter (fun e => e.1 == k) (xs.zipIdx (s + 1)))).sum =
                (if s + 1 ≤ i then (decide (xs[i - (s + 1)]? == some k)).toNat else 0) by
                  simpa [beq_iff_eq] using (ih (s + 1) i)]
            simp [hs, hsi, hsne]
        · have hsi : ¬ s + 1 ≤ i := by omega
          have hsne : s ≠ i := by omega
          rw [show (List.map (fun e => (decide (e.2 = i)).toNat)
                (List.filter (fun e => e.1 == k) (xs.zipIdx (s + 1)))).sum =
              (if s + 1 ≤ i then (decide (xs[i - (s + 1)]? == some k)).toNat else 0) by
                simpa [beq_iff_eq] using (ih (s + 1) i)]
          simp [hs, hsi, hsne]
      · rw [if_neg hxk]
        by_cases hs : s ≤ i
        · by_cases his : i = s
          · subst i
            have ht :
                (List.map (fun e => (decide (e.2 = s)).toNat)
                  (List.filter (fun e => e.1 == k) (xs.zipIdx (s + 1)))).sum = 0 := by
              have ht' :
                  (List.map (fun e => (decide (e.2 = s)).toNat)
                    (List.filter (fun e => e.1 == k) (xs.zipIdx (s + 1)))).sum =
                    if s + 1 ≤ s then (decide (xs[s - (s + 1)]? == some k)).toNat else 0 := by
                simpa [beq_iff_eq] using (ih (s + 1) s)
              rw [ht']
              simp [show ¬ s + 1 ≤ s by omega]
            simp only [beq_iff_eq]
            rw [ht]
            have hxkne : x ≠ k := by intro h; apply hxk; simpa [h]
            simp [hxkne]
          · have hsi : s + 1 ≤ i := by omega
            have hIdx : i - s = (i - (s + 1)) + 1 := by omega
            rw [hIdx, List.getElem?_cons_succ]
            rw [ih (s + 1) i]
            simp [hs, hsi]
        · have hsi : ¬ s + 1 ≤ i := by omega
          rw [ih (s + 1) i]
          simp [hs, hsi]

private theorem countP_eq_sum_toNat {α : Type} (p : α → Bool) (l : List α) :
    l.countP p = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [ih, h, Nat.add_comm]

private theorem count_eq_sum_decide_eq (ids : List Nat) (x : Nat) :
    ids.count x = (ids.map (fun i => (decide (x == i)).toNat)).sum := by
  induction ids with
  | nil => simp
  | cons i is ih =>
      rw [List.count_cons, List.map_cons, List.sum_cons, ih]
      by_cases h : i == x
      · have heq : i = x := beq_iff_eq.mp h
        have h' : x == i := beq_iff_eq.mpr heq.symm
        simp [h, h', Nat.add_comm]
      · have hne : i ≠ x := by
          intro he
          apply h
          simpa [he]
        have hne' : x ≠ i := by
          intro hh
          exact hne hh.symm
        have h' : (x == i) = false := by simpa [beq_iff_eq] using hne'
        simp [h, h']

private theorem sum_map_add {α : Type} (l : List α) (g h : α → Nat) :
    (l.map (fun x => g x + h x)).sum = (l.map g).sum + (l.map h).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem sum_map_sum {α β : Type} (xs : List α) (ys : List β) (f : α → β → Nat) :
    (xs.map (fun a => (ys.map (fun b => f a b)).sum)).sum =
      (ys.map (fun b => (xs.map (fun a => f a b)).sum)).sum := by
  induction xs with
  | nil => simp [sum_map_zero]
  | cons a xs ih =>
      simp [sum_map_add, ih]

private theorem count_expand_eq_mass (d ids : List Nat) (k : Nat) :
    (bag.expand (d, ids)).count k = bag.mass (d, ids) k := by
  rw [show bag.expand (d, ids) = ids.filterMap (fun i => d[i]?) by rfl]
  rw [List.count_filterMap]
  rw [countP_eq_sum_toNat (fun i => d[i]? == some k) ids]
  unfold bag
  simp only
  rw [show ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => ids.count e.2)).sum =
      ((d.zipIdx.filter (fun e => e.1 == k)).map
        (fun e => (ids.map (fun i => (decide (e.2 == i)).toNat)).sum)).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro e he
    exact count_eq_sum_decide_eq ids e.2]
  rw [sum_map_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro i hi
  have hdec : (decide (d[i]? = some k)).toNat = (d[i]? == some k).toNat := by
    by_cases hh : d[i]? == some k
    · have hheq : d[i]? = some k := by simpa using hh
      simp [hh, hheq]
    · have hhne : d[i]? ≠ some k := by
        intro he
        apply hh
        simpa [he]
      simp [hh, hhne]
  simpa [hdec] using (count_zipIdx_index d 0 k i).symm

private theorem mem_keys_of_mem_expand (d ids : List Nat) {k : Nat}
    (hk : k ∈ bag.expand (d, ids)) : k ∈ bag.keys (d, ids) := by
  rw [show bag.expand (d, ids) = ids.filterMap (fun i => d[i]?) by rfl] at hk
  rw [List.mem_filterMap] at hk
  rcases hk with ⟨i, hi, hval⟩
  rw [show bag.keys (d, ids) = d by rfl]
  rcases List.getElem?_eq_some_iff.mp hval with ⟨hidx, heq⟩
  rw [← heq]
  exact List.getElem_mem hidx

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
        simp [List.count_cons, hkx, hx, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

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
          simp [Nat.add_mul, Nat.add_comm, Nat.add_left_comm]
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
          simpa [rest] using (sum_map_eq_count_head k ks f)
        rw [hks]
        omega
  exact H keys rfl

private theorem mass_zero_of_not_mem_expand (d ids : List Nat) (k : Nat)
    (h : k ∉ bag.expand (d, ids)) : bag.mass (d, ids) k = 0 := by
  rw [← count_expand_eq_mass]
  exact List.count_eq_zero_of_not_mem h

private theorem mass_zero_of_not_mem_keys (d ids : List Nat) (k : Nat)
    (h : k ∉ bag.keys (d, ids)) : bag.mass (d, ids) k = 0 := by
  rw [← count_expand_eq_mass]
  apply List.count_eq_zero_of_not_mem
  intro hk
  exact h (mem_keys_of_mem_expand d ids hk)

private theorem mass_eq_zero_bool_iff_not_mem_expand (d ids : List Nat) (k : Nat) :
    (bag.mass (d, ids) k == 0) = true ↔ k ∉ bag.expand (d, ids) := by
  rw [← count_expand_eq_mass d ids k]
  constructor
  · intro h
    have hz : (bag.expand (d, ids)).count k = 0 := by simpa using h
    exact List.count_eq_zero.mp hz
  · intro h
    have hz : (bag.expand (d, ids)).count k = 0 := List.count_eq_zero.mpr h
    simp [hz]

private theorem contains_true_iff_mem (l : List Nat) (k : Nat) :
    l.contains k = true ↔ k ∈ l := by
  induction l with
  | nil => simp
  | cons x xs ih => simp

private theorem not_contains_true_iff_not_mem (l : List Nat) (k : Nat) :
    (!l.contains k) = true ↔ k ∉ l := by
  have hc := contains_true_iff_mem l k
  constructor
  · intro h hm
    have ht : l.contains k = true := hc.mpr hm
    rw [ht] at h
    simp at h
  · intro h
    cases heq : l.contains k <;> simp_all

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rcases a with ⟨a₁, a₂⟩
  rcases c with ⟨c₁, c₂⟩
  let f : Nat → Nat := fun k => if bag.mass (c₁, c₂) k == 0 then k else 0
  change ((bag.keys (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
    ((bag.expand (a₁, a₂)).filter (fun x => !(bag.expand (c₁, c₂)).contains x)).sum

  have hL : ((bag.keys (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
      ((bag.expand (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdnotE : d ∉ bag.expand (a₁, a₂) := by
        intro hdE
        apply hdnot
        rw [List.mem_eraseDups]
        exact hdE
      simp [mass_zero_of_not_mem_expand a₁ a₂ d hdnotE]
    · intro d hd hdnot
      have hdnotK : d ∉ bag.keys (a₁, a₂) := by
        intro hdk
        apply hdnot
        rw [List.mem_eraseDups]
        exact hdk
      simp [mass_zero_of_not_mem_keys a₁ a₂ d hdnotK]
  rw [hL]

  have hD0 := dedup_weight_sum f (bag.expand (a₁, a₂))
  have hD : ((bag.expand (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
      ((bag.expand (a₁, a₂)).map f).sum := by
    rw [← hD0]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]
  rw [hD]

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
  by_cases hzero : (bag.mass (c₁, c₂) k == 0) = true
  · have hnotmem : k ∉ bag.expand (c₁, c₂) :=
      (mass_eq_zero_bool_iff_not_mem_expand c₁ c₂ k).mp hzero
    have hnotcontains : (!(bag.expand (c₁, c₂)).contains k) = true :=
      (not_contains_true_iff_not_mem (bag.expand (c₁, c₂)) k).mpr hnotmem
    have hmassz : bag.mass (c₁, c₂) k = 0 := beq_iff_eq.mp hzero
    simp [f, hmassz, hnotmem]
  · have hnotcontains : ¬ ((!(bag.expand (c₁, c₂)).contains k) = true) := by
      intro hnc
      have hnotmem : k ∉ bag.expand (c₁, c₂) :=
        (not_contains_true_iff_not_mem (bag.expand (c₁, c₂)) k).mp hnc
      exact hzero ((mass_eq_zero_bool_iff_not_mem_expand c₁ c₂ k).mpr hnotmem)
    have hmem : k ∈ bag.expand (c₁, c₂) := by
      by_cases hm : k ∈ bag.expand (c₁, c₂)
      · exact hm
      · have hnc : (!(bag.expand (c₁, c₂)).contains k) = true :=
          (not_contains_true_iff_not_mem (bag.expand (c₁, c₂)) k).mpr hm
        exact (hnotcontains hnc).elim
    have hmassnz : bag.mass (c₁, c₂) k ≠ 0 := by
      intro hm0
      apply hzero
      simp [hm0]
    simp [f, hzero, hnotcontains, hmem, hmassnz]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_AntiRevenue
