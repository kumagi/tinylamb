import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_FilteredMoment

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

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

/-! ## DictionaryAliases bag facts -/

private theorem count_filterMap_get_eq (d ids : List Nat) (k : Nat) :
    (ids.filterMap (fun i => d[i]?)).count k = (ids.filter (fun i => d[i]? == some k)).length := by
  rw [List.count_filterMap, List.countP_eq_length_filter]

private theorem mem_indices_iff (d : List Nat) (k i : Nat) :
    i ∈ (d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd ↔ d[i]? == some k := by
  constructor
  · intro h
    rw [List.mem_map] at h
    rcases h with ⟨e, hef, heq⟩
    have hezip : e ∈ d.zipIdx := (List.mem_filter.mp hef).1
    have hek : e.1 == k := (List.mem_filter.mp hef).2
    have hget : d[e.2]? = some e.1 := List.mk_mem_zipIdx_iff_getElem?.mp hezip
    have hval : e.1 = k := beq_iff_eq.mp hek
    change d[i]? == some k
    rw [← heq, hget, hval]
    simp
  · intro h
    rw [List.mem_map]
    refine ⟨(k, i), ?_, rfl⟩
    rw [List.mem_filter]
    constructor
    · exact List.mk_mem_zipIdx_iff_getElem?.mpr (beq_iff_eq.mp h)
    · simp

private theorem nodup_indices (d : List Nat) (k : Nat) :
    ((d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd).Nodup := by
  have hzip : (d.zipIdx.map Prod.snd).Nodup := by
    rw [List.zipIdx_map_snd]
    exact List.nodup_range' 1 (by simp)
  have hsub : ((d.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map Prod.snd).Sublist
      (d.zipIdx.map Prod.snd) := by
    exact List.Sublist.map Prod.snd List.filter_sublist
  exact List.Sublist.nodup hsub hzip

private theorem sum_map_add_indicator (indices : List Nat) (g : Nat → Nat) (i : Nat) :
    (indices.map (fun j => g j + (if (i == j) = true then 1 else 0))).sum =
      (indices.map g).sum + (indices.filter (fun j => i == j)).length := by
  induction indices with
  | nil => simp
  | cons j js ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      rw [ih]
      by_cases hij : i == j
      · have hji : (j == i) = true := beq_iff_eq.mpr (beq_iff_eq.mp hij).symm
        simp [hij]
        omega
      · have hji : (j == i) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hij (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [hij]
        omega

private theorem filter_beq_length_eq_count (indices : List Nat) (i : Nat) :
    (indices.filter (fun j => i == j)).length = indices.count i := by
  induction indices with
  | nil => simp
  | cons j js ih =>
      simp only [List.filter_cons, List.count_cons]
      by_cases hji : j == i
      · have hij : i == j := beq_iff_eq.mpr (beq_iff_eq.mp hji).symm
        simp [hji, hij, ih]
      · have hij : (i == j) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hji (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [hji, hij, ih]

private theorem filter_mem_length_eq_sum_count (ids indices : List Nat) (hnd : indices.Nodup) :
    (ids.filter (fun i => i ∈ indices)).length = (indices.map (fun i => ids.count i)).sum := by
  induction ids with
  | nil => simp [sum_nat_eq_zero_of_mem_eq_zero]
  | cons i ids ih =>
      simp only [List.filter_cons, List.count_cons]
      rw [sum_map_add_indicator indices (fun j => ids.count j) i, ← ih,
        filter_beq_length_eq_count indices i]
      by_cases hi : i ∈ indices
      · rw [hnd.count_of_mem hi]
        simp [hi]
      · rw [List.count_eq_zero.mpr hi]
        simp [hi]

private theorem count_expand_eq_mass (d ids : List Nat) (k : Nat) :
    (B.expand (d, ids)).count k = B.mass (d, ids) k := by
  change (ids.filterMap (fun i => d[i]?)).count k =
    ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => ids.count e.2)).sum
  rw [count_filterMap_get_eq]
  let indices := (d.zipIdx.filter (fun e => e.1 == k)).map Prod.snd
  have hfl := filter_mem_length_eq_sum_count ids indices (nodup_indices d k)
  have hsum : ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => ids.count e.2)).sum =
      (indices.map (fun i => ids.count i)).sum := by
    simp [indices, List.map_map, Function.comp_def]
  rw [hsum, ← hfl]
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  apply Bool.eq_iff_iff.mpr
  simpa [indices] using (mem_indices_iff d k i).symm

private theorem mem_keys_of_mem_expand (d ids : List Nat) (k : Nat) :
    k ∈ B.expand (d, ids) → k ∈ B.keys (d, ids) := by
  intro h
  change k ∈ ids.filterMap (fun i => d[i]?) at h
  rw [List.mem_filterMap] at h
  rcases h with ⟨i, hi, hval⟩
  change k ∈ d
  have hget : d[i]? = some k := hval
  have hmem : (k, i) ∈ d.zipIdx := List.mk_mem_zipIdx_iff_getElem?.mpr hget
  rw [← List.zipIdx_map_fst 0 d]
  exact List.mem_map.mpr ⟨(k, i), hmem, rfl⟩

private theorem mass_zero_of_not_mem_expand (d ids : List Nat) (k : Nat)
    (h : k ∉ B.expand (d, ids)) : B.mass (d, ids) k = 0 := by
  rw [← count_expand_eq_mass d ids k]
  exact List.count_eq_zero.mpr h

/-! ## Goal -/

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rcases p with ⟨d, ids⟩
  let f : Nat → Nat := fun x => if lo ≤ x && x < hi then x * x else 0
  let hsq : Nat → Nat := fun x => x * x
  let p : Nat → Bool := fun x => lo ≤ x && x < hi
  change ((B.keys (d, ids)).eraseDups.map (fun k => B.mass (d, ids) k * f k)).sum =
    (((B.expand (d, ids)).filter p).map hsq).sum

  have hR1 : (((B.expand (d, ids)).filter p).map hsq).sum = ((B.expand (d, ids)).map f).sum := by
    simpa [f, hsq, p] using (sum_filter_eq_sum_ite (B.expand (d, ids)) p hsq)

  have hR2 : ((B.expand (d, ids)).map f).sum =
      (((B.expand (d, ids)).eraseDups.map fun k => B.mass (d, ids) k * f k).sum) := by
    rw [← dedup_weight_sum f (B.expand (d, ids))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]

  have hL : ((B.keys (d, ids)).eraseDups.map (fun k => B.mass (d, ids) k * f k)).sum =
      ((B.expand (d, ids)).eraseDups.map (fun k => B.mass (d, ids) k * f k)).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro a ha hanot
      have hanot' : a ∉ B.expand (d, ids) := by
        intro hae
        exact hanot (by rw [List.mem_eraseDups]; exact hae)
      have hm := mass_zero_of_not_mem_expand d ids a hanot'
      simp [hm]
    · intro a ha hanot
      have hae : a ∈ B.expand (d, ids) := by
        rw [List.mem_eraseDups] at ha
        exact ha
      exfalso
      apply hanot
      rw [List.mem_eraseDups]
      exact mem_keys_of_mem_expand d ids a hae

  calc
    ((B.keys (d, ids)).eraseDups.map (fun k => B.mass (d, ids) k * f k)).sum
        = ((B.expand (d, ids)).eraseDups.map (fun k => B.mass (d, ids) k * f k)).sum := hL
    _ = ((B.expand (d, ids)).map f).sum := hR2.symm
    _ = (((B.expand (d, ids)).filter p).map hsq).sum := hR1.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_FilteredMoment
