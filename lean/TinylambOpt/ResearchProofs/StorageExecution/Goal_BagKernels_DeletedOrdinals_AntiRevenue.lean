import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DeletedOrdinals.AntiRevenue` — Compute revenue of all left rows whose key is absent
  on the right.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_AntiRevenue

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

private theorem sum_eq_of_nodup_mem (l₁ l₂ : List Nat) (f : Nat → Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  have hperm : l₁.Perm l₂ := List.perm_iff_count.mpr (by
    intro d
    by_cases hd : d ∈ l₁
    · have hd2 : d ∈ l₂ := (hmem d).1 hd
      rw [count_eq_one_of_mem_nodup hnd₁ hd, count_eq_one_of_mem_nodup hnd₂ hd2]
    · have hd2 : d ∉ l₂ := by intro h; exact hd ((hmem d).2 h)
      rw [List.count_eq_zero_of_not_mem hd, List.count_eq_zero_of_not_mem hd2])
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

/-! ## DeletedOrdinals bag facts -/

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

private theorem perm_of_nodup_mem_iff {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hm : ∀ a, a ∈ l₁ ↔ a ∈ l₂) : l₁.Perm l₂ := by
  rw [List.perm_iff_count]
  intro a
  by_cases h : a ∈ l₁
  · rw [count_eq_one_of_mem_nodup h₁ h,
        count_eq_one_of_mem_nodup h₂ ((hm a).1 h)]
  · rw [List.count_eq_zero_of_not_mem h,
        List.count_eq_zero_of_not_mem (fun ha => h ((hm a).2 ha))]

private theorem contains_true_iff_mem (l : List Nat) (a : Nat) : l.contains a = true ↔ a ∈ l := by
  induction l with
  | nil => simp
  | cons b bs ih => by_cases h : b = a <;> simp [h]

private theorem zipIdx_snd_nodup {α : Type} (l : List α) (n : Nat) :
    ((l.zipIdx n).map Prod.snd).Nodup := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨e, he, heq⟩
        obtain ⟨y, i⟩ := e
        simp only at heq
        subst i
        obtain ⟨hlo, hi, hval⟩ := List.mem_zipIdx (k := n + 1) he
        omega
      · simpa using ih (n + 1)

private def idxs (xs : List Nat) (k : Nat) : List Nat :=
  ((xs.zipIdx.filter (fun e => e.1 == k)).map Prod.snd)

private theorem idxs_nodup (xs : List Nat) (k : Nat) : (idxs xs k).Nodup := by
  have hsub : (idxs xs k).Sublist ((xs.zipIdx).map Prod.snd) :=
    List.Sublist.map Prod.snd List.filter_sublist
  exact hsub.nodup (zipIdx_snd_nodup xs 0)

private theorem mem_idxs_iff_getElem? (xs : List Nat) (k a : Nat) :
    a ∈ idxs xs k ↔ xs[a]? = some k := by
  unfold idxs
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨e, he, heq⟩
    obtain ⟨y, i⟩ := e
    simp only at heq
    subst i
    have hf := List.mem_filter.mp he
    have hpred : (y == k) = true := hf.2
    have hzip := List.mem_zipIdx (xs := xs) (k := 0) hf.1
    have ha : a < xs.length := by simpa using hzip.2.1
    have hy : y = xs[a] := by simpa using hzip.2.2
    have hyk : y = k := eq_of_beq hpred
    rw [List.getElem?_eq_getElem ha]
    rw [← hy, hyk]
  · intro hget
    obtain ⟨ha, hval⟩ := List.getElem?_eq_some_iff.mp hget
    apply List.mem_map.mpr
    refine ⟨(k, a), ?_, rfl⟩
    apply List.mem_filter.mpr
    constructor
    · have hzget : (xs.zipIdx)[a]? = some (k, a) := by
        rw [List.getElem?_zipIdx]
        simp [List.getElem?_eq_getElem ha, hval]
      obtain ⟨hidx, hidxval⟩ := List.getElem?_eq_some_iff.mp hzget
      rw [← hidxval]
      exact List.getElem_mem hidx
    · simp

private theorem zipIdx_filter_fst (xs : List Nat) (n k : Nat) :
    ((xs.zipIdx n).filter (fun e => e.1 == k)).map Prod.fst = xs.filter (fun x => x == k) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons]
      by_cases h : a == k <;> simp [h, ih]

private theorem idxs_length (xs : List Nat) (k : Nat) : (idxs xs k).length = xs.count k := by
  have h := congrArg List.length (zipIdx_filter_fst xs 0 k)
  simpa [idxs, List.count_eq_length_filter] using h

private theorem mem_zipIdx_filter_snd_iff (xs ds : List Nat) (k j : Nat) :
    j ∈ ((xs.zipIdx.filter (fun e => !ds.contains e.2 && e.1 == k)).map Prod.snd) ↔
      j ∈ (idxs xs k).filter (fun i => !ds.contains i) := by
  constructor
  · intro hj
    rcases List.mem_map.mp hj with ⟨e, he, heq⟩
    obtain ⟨v, i⟩ := e
    simp only at heq
    subst i
    have hf := List.mem_filter.mp he
    have hp : (!ds.contains j && (v == k)) = true := by simpa only using hf.2
    have hjnotds : j ∉ ds := by
      have hn : (!ds.contains j) = true := (Bool.and_eq_true_iff.mp hp).1
      simpa using hn
    have hvk : v = k := eq_of_beq (Bool.and_eq_true_iff.mp hp).2
    have hzip := List.mem_zipIdx (xs := xs) (k := 0) hf.1
    have hjlen : j < xs.length := by simpa using hzip.2.1
    have hvget : v = xs[j] := by simpa using hzip.2.2
    exact List.mem_filter.mpr ⟨(mem_idxs_iff_getElem? xs k j).2 (by
      rw [List.getElem?_eq_getElem hjlen, ← hvget, hvk]), by simpa using hjnotds⟩
  · intro hj
    have hf := List.mem_filter.mp hj
    have hidx := (mem_idxs_iff_getElem? xs k j).1 hf.1
    obtain ⟨hjlen, hjval⟩ := List.getElem?_eq_some_iff.mp hidx
    have hjnotds : j ∉ ds := by simpa using hf.2
    have hnot : ds.contains j = false := by simpa using hjnotds
    apply List.mem_map.mpr
    refine ⟨(k, j), ?_, rfl⟩
    apply List.mem_filter.mpr
    constructor
    · have hzget : (xs.zipIdx)[j]? = some (k, j) := by
        rw [List.getElem?_zipIdx]
        simp [List.getElem?_eq_getElem hjlen, hjval]
      obtain ⟨hidx', hidxval⟩ := List.getElem?_eq_some_iff.mp hzget
      rw [← hidxval]
      exact List.getElem_mem hidx'
    · simpa [hnot]

private theorem mem_ds_filter_iff (xs ds : List Nat) (k j : Nat) :
    j ∈ (ds.eraseDups.filter (fun i => xs[i]? == some k)) ↔
      j ∈ (idxs xs k).filter (fun i => ds.contains i) := by
  constructor
  · intro hj
    have hf := List.mem_filter.mp hj
    have hjds : j ∈ ds := (List.mem_eraseDups).1 hf.1
    apply List.mem_filter.mpr
    constructor
    · exact (mem_idxs_iff_getElem? xs k j).2 (by simpa using hf.2)
    · exact (contains_true_iff_mem ds j).2 hjds
  · intro hj
    have hf := List.mem_filter.mp hj
    have hidx := (mem_idxs_iff_getElem? xs k j).1 hf.1
    have hjds : j ∈ ds := (contains_true_iff_mem ds j).1 hf.2
    exact List.mem_filter.mpr ⟨(List.mem_eraseDups).2 hjds, by simp [hidx]⟩

private theorem count_map_fst_filter (l : List (Nat × Nat)) (P : Nat × Nat → Bool) (k : Nat) :
    ((l.filter P).map Prod.fst).count k = (l.filter (fun e => P e && e.1 == k)).length := by
  induction l with
  | nil => simp
  | cons e es ih =>
      by_cases hp : P e = true
      · have hfilter : (e :: es).filter P = e :: es.filter P := by simp [hp]
        rw [hfilter, List.map_cons, List.count_cons, ih]
        by_cases hk : (e.1 == k) = true <;> simp [hp, hk]
      · have hfilter : (e :: es).filter P = es.filter P := by simp [hp]
        rw [hfilter, ih]
        simp [hp]

private theorem countP_not_add (l : List Nat) (p : Nat → Bool) :
    l.countP (fun x => !p x) + l.countP p = l.length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases h : p a = true <;> simp [h] <;> omega

private theorem count_expand_eq_idxs_filter (xs ds : List Nat) (k : Nat) :
    (bag.expand (xs, ds)).count k = ((idxs xs k).filter (fun i => !ds.contains i)).length := by
  change (((xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst).count k) = _
  rw [count_map_fst_filter]
  have hperm : ((xs.zipIdx.filter (fun e => !ds.contains e.2 && e.1 == k)).map Prod.snd).Perm
      ((idxs xs k).filter (fun i => !ds.contains i)) := by
    apply perm_of_nodup_mem_iff
    · have hsub : ((xs.zipIdx.filter (fun e => !ds.contains e.2 && e.1 == k)).map Prod.snd).Sublist
          ((xs.zipIdx).map Prod.snd) :=
        List.Sublist.map Prod.snd List.filter_sublist
      exact hsub.nodup (zipIdx_snd_nodup xs 0)
    · exact (idxs_nodup xs k).filter _
    · exact mem_zipIdx_filter_snd_iff xs ds k
  rw [← hperm.length_eq]
  simp

private theorem count_del_eq_idxs_filter (xs ds : List Nat) (k : Nat) :
    ds.eraseDups.countP (fun i => xs[i]? == some k) =
      ((idxs xs k).filter (fun i => ds.contains i)).length := by
  have hperm : (ds.eraseDups.filter (fun i => xs[i]? == some k)).Perm
      ((idxs xs k).filter (fun i => ds.contains i)) := by
    apply perm_of_nodup_mem_iff
    · exact (nodup_eraseDups ds).filter _
    · exact (idxs_nodup xs k).filter _
    · exact mem_ds_filter_iff xs ds k
  rw [List.countP_eq_length_filter]
  exact hperm.length_eq

private theorem count_expand_eq_mass (xs ds : List Nat) (k : Nat) :
    (bag.expand (xs, ds)).count k = bag.mass (xs, ds) k := by
  rw [count_expand_eq_idxs_filter]
  change ((idxs xs k).filter (fun i => !ds.contains i)).length =
    xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k)
  rw [← idxs_length, count_del_eq_idxs_filter]
  have hcnt := (List.countP_eq_length_filter (p := fun i => !ds.contains i) (l := idxs xs k))
  have hcnt' := (List.countP_eq_length_filter (p := fun i => ds.contains i) (l := idxs xs k))
  have h := countP_not_add (idxs xs k) (fun i => ds.contains i)
  omega

private theorem mem_keys_of_mass_pos (xs ds : List Nat) (k : Nat)
    (h : 0 < bag.mass (xs, ds) k) : k ∈ bag.keys (xs, ds) := by
  change k ∈ xs
  have hpos : 0 < (bag.expand (xs, ds)).count k := by
    simpa [count_expand_eq_mass] using h
  have hmem : k ∈ bag.expand (xs, ds) := List.count_pos_iff.mp hpos
  change k ∈ (xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst at hmem
  rcases List.mem_map.mp hmem with ⟨e, he, heq⟩
  obtain ⟨v, i⟩ := e
  simp only at heq
  have hzip := List.mem_zipIdx (xs := xs) (k := 0) (List.mem_filter.mp he).1
  have hvi : v = xs[i] := by simpa using hzip.2.2
  have hki : k = xs[i] := by rw [← heq]; exact hvi
  rw [hki]
  exact List.getElem_mem (by simpa using hzip.2.1)

private theorem mem_keys_of_mem_expand (xs ds : List Nat) (k : Nat)
    (h : k ∈ bag.expand (xs, ds)) : k ∈ bag.keys (xs, ds) := by
  apply mem_keys_of_mass_pos
  rw [← count_expand_eq_mass]
  exact List.count_pos_iff.mpr h

private theorem mass_zero_of_not_mem_expand (xs ds : List Nat) (k : Nat)
    (h : k ∉ bag.expand (xs, ds)) : bag.mass (xs, ds) k = 0 := by
  rw [← count_expand_eq_mass]
  exact List.count_eq_zero.mpr h

/-! ## Goal -/

theorem sound_AntiRevenue : DeletedOrdinals.AntiRevenue.Goal := by
  intro a c
  rcases a with ⟨a₁, a₂⟩
  rcases c with ⟨c₁, c₂⟩
  let f : Nat → Nat := fun k => if bag.mass (c₁, c₂) k == 0 then k else 0
  change ((bag.keys (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
    ((bag.expand (a₁, a₂)).filter (fun x => !(bag.expand (c₁, c₂)).contains x)).sum

  -- Both supports describe the same distinct live keys; dead keys carry zero mass.
  have hL : ((bag.keys (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
      ((bag.expand (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum := by
    apply sum_eq_of_mem_or_zero
    · exact nodup_eraseDups _
    · exact nodup_eraseDups _
    · intro d hd hdnot
      have hdnotE : d ∉ bag.expand (a₁, a₂) := by
        intro hdE
        exact hdnot (List.mem_eraseDups.mpr hdE)
      simp [mass_zero_of_not_mem_expand a₁ a₂ d hdnotE]
    · intro d hd hdnot
      exfalso
      exact hdnot (List.mem_eraseDups.mpr
        (mem_keys_of_mem_expand a₁ a₂ d (List.mem_eraseDups.mp hd)))
  rw [hL]

  -- Regroup by multiplicity: mass equals the count in the expansion.
  have hD0 := dedup_weight_sum f (bag.expand (a₁, a₂))
  have hD : ((bag.expand (a₁, a₂)).eraseDups.map fun k => bag.mass (a₁, a₂) k * f k).sum =
      ((bag.expand (a₁, a₂)).map f).sum := by
    rw [← hD0]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]
  rw [hD]

  -- A right-side key has zero multiplicity exactly when it is absent from the
  -- right expansion, so the `if` guards coincide pointwise.
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_AntiRevenue
