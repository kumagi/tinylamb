import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DeletedOrdinals.OrderStatistic` — Find an order statistic with ties by cumulative
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

private abbrev B := bag

/-! ## Generic list lemmas -/

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

/-! ## Generic order-statistic lemmas -/

private theorem countP_eq_sum_toNat (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
    cases h : p a <;> simp [h, ih, Nat.add_comm]

private theorem sorted_mergeSort (l : List Nat) : l.mergeSort.Pairwise (fun a b => a ≤ b) := by
  simpa using (List.pairwise_mergeSort
    (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c hab hbc; simp at hab hbc ⊢; omega)
    (by intro a b; simp [Nat.le_total]) l)

/-! ## DeletedOrdinals bag facts -/

private theorem contains_eraseDups_eq (ds : List Nat) (i : Nat) :
    ds.contains i = ds.eraseDups.contains i := by
  apply Bool.eq_iff_iff.mpr
  simp [List.mem_eraseDups]

private theorem count_map_fst_eq_countP_fst (L : List (Nat × Nat)) (k : Nat) :
    (L.map Prod.fst).count k = L.countP (fun e => e.1 == k) := by
  induction L with
  | nil => simp
  | cons e L ih =>
      rcases e with ⟨a, i⟩
      simp [List.count_cons, List.countP_cons, ih]

private theorem countP_and_not {α : Type} (l : List α) (p q : α → Bool) :
    l.countP (fun a => p a && !q a) + l.countP (fun a => p a && q a) = l.countP p := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hq : q a = true
      · by_cases hp : p a = true <;> simp [hq, hp, ih] <;> omega
      · by_cases hp : p a = true <;> simp [hq, hp, ih] <;> omega

private theorem countP_or_disjoint {α : Type} (l : List α) (r p q : α → Bool)
    (hd : ∀ a ∈ l, ¬ (p a = true ∧ q a = true)) :
    l.countP (fun a => r a && (p a || q a)) =
      l.countP (fun a => r a && p a) + l.countP (fun a => r a && q a) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have hd' : ∀ b ∈ l, ¬ (p b = true ∧ q b = true) := by
        intro b hb
        exact hd b (by simp [hb])
      rw [List.countP_cons, List.countP_cons, List.countP_cons, ih hd']
      have hda : ¬ (p a = true ∧ q a = true) := hd a (by simp)
      by_cases hp : p a = true
      · have hqf : q a = false := Bool.eq_false_of_ne_true (by
          intro hq
          exact hda ⟨hp, hq⟩)
        by_cases hr : r a = true <;> simp [hp, hqf, hr] <;> omega
      · have hpf : p a = false := Bool.eq_false_of_ne_true hp
        by_cases hq : q a = true <;> by_cases hr : r a = true <;>
          simp [hpf, hq, hr] <;> omega

private theorem countP_zipIdx_single (xs : List Nat) (s i k : Nat) :
    (xs.zipIdx s).countP (fun e => (e.1 == k) && decide (e.2 = i)) =
      (if s ≤ i && xs[i - s]? == some k then 1 else 0) := by
  induction xs generalizing s with
  | nil => simp
  | cons a xs ih =>
      rw [List.zipIdx_cons, List.countP_cons, ih (s + 1)]
      by_cases hi : i < s
      · have hnot : ¬ s ≤ i := Nat.not_le.mpr hi
        have hnot' : ¬ s + 1 ≤ i := by omega
        have hne : s ≠ i := by omega
        simp [hnot, hnot', hne]
      · have hle : s ≤ i := Nat.le_of_not_gt hi
        by_cases heq : i = s
        · subst i
          simp <;> omega
        · have hlt : s < i := by omega
          have hs1 : s + 1 ≤ i := hlt
          have hne : s ≠ i := by omega
          have hsub : i - s = (i - (s + 1)) + 1 := by omega
          rw [hsub, List.getElem?_cons_succ]
          simp [hne, hs1, hle]

private theorem countP_zipIdx_memD (xs D : List Nat) (k : Nat) (hD : D.Nodup) :
    xs.zipIdx.countP (fun e => (e.1 == k) && D.contains e.2) =
      D.countP (fun i => xs[i]? == some k) := by
  induction D with
  | nil => simp
  | cons i D ih =>
      obtain ⟨hi_not, hDtail⟩ := List.nodup_cons.mp hD
      rw [List.countP_cons]
      rw [show xs.zipIdx.countP (fun e => (e.1 == k) && (i :: D).contains e.2) =
             xs.zipIdx.countP (fun e => (e.1 == k) && (decide (e.2 = i) || D.contains e.2)) by
             apply congrArg (List.countP · xs.zipIdx)
             funext e
             simp]
      rw [countP_or_disjoint xs.zipIdx (fun e => e.1 == k)
          (fun e => decide (e.2 = i)) (fun e => D.contains e.2) (by
            intro e he hboth
            rcases hboth with ⟨h1, h2⟩
            have heqi : e.2 = i := of_decide_eq_true h1
            apply hi_not
            have : e.2 ∈ D := by simpa using h2
            simpa [heqi] using this)]
      rw [countP_zipIdx_single]
      rw [ih hDtail]
      simp at *
      omega

private theorem count_expand_eq_mass (xs ds : List Nat) (k : Nat) :
    (B.expand (xs, ds)).count k = B.mass (xs, ds) k := by
  change List.count k ((xs.zipIdx.filter (fun e => !(ds.contains e.2))).map Prod.fst) =
    xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k)
  have hcontains : (xs.zipIdx.filter (fun e => !(ds.contains e.2))) =
      (xs.zipIdx.filter (fun e => !(ds.eraseDups.contains e.2))) := by
    apply List.filter_congr
    intro e he
    rw [contains_eraseDups_eq]
  rw [hcontains]
  rw [count_map_fst_eq_countP_fst]
  rw [List.countP_filter]
  have htotal : xs.count k = xs.zipIdx.countP (fun e => e.1 == k) := by
    calc
      xs.count k = (xs.zipIdx.map Prod.fst).count k := by rw [List.zipIdx_map_fst 0 xs]
      _ = xs.zipIdx.countP (fun e => e.1 == k) := count_map_fst_eq_countP_fst xs.zipIdx k
  have hpart := countP_and_not xs.zipIdx (fun e => e.1 == k)
      (fun e => ds.eraseDups.contains e.2)
  have hB : xs.zipIdx.countP (fun e => (e.1 == k) && ds.eraseDups.contains e.2) =
      ds.eraseDups.countP (fun i => xs[i]? == some k) :=
    countP_zipIdx_memD xs ds.eraseDups k (eraseDups_nodup ds)
  rw [hB, ← htotal] at hpart
  omega

private theorem mem_keys_of_mem_expand (xs ds : List Nat) (k : Nat) :
    k ∈ B.expand (xs, ds) → k ∈ B.keys (xs, ds) := by
  intro h
  change k ∈ (xs.zipIdx.filter (fun e => !(ds.contains e.2))).map Prod.fst at h
  rw [List.mem_map] at h
  rcases h with ⟨e, he, rfl⟩
  change e.1 ∈ xs
  have hezip : e ∈ xs.zipIdx := (List.mem_filter.mp he).1
  rw [← List.zipIdx_map_fst 0 xs]
  exact List.mem_map.mpr ⟨e, hezip, rfl⟩

private theorem mass_zero_of_not_mem_expand (xs ds : List Nat) (k : Nat)
    (h : k ∉ B.expand (xs, ds)) : B.mass (xs, ds) k = 0 := by
  rw [← count_expand_eq_mass xs ds k]
  exact List.count_eq_zero.mpr h

private theorem below_eq_countP (xs ds : List Nat) (z : Nat) :
    below B (xs, ds) z = (B.expand (xs, ds)).countP (fun y => decide (y ≤ z)) := by
  let f : Nat → Nat := fun k => (decide (k ≤ z)).toNat
  change ((B.keys (xs, ds)).eraseDups.map (fun k => B.mass (xs, ds) k * f k)).sum = _
  have hL : ((B.keys (xs, ds)).eraseDups.map (fun k => B.mass (xs, ds) k * f k)).sum =
      ((B.expand (xs, ds)).eraseDups.map (fun k => B.mass (xs, ds) k * f k)).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdnot' : d ∉ B.expand (xs, ds) := by
        intro hde
        exact hdnot (by rw [List.mem_eraseDups]; exact hde)
      have hm := mass_zero_of_not_mem_expand xs ds d hdnot'
      simp [hm]
    · intro d hd hdnot
      have hde : d ∈ B.expand (xs, ds) := by
        rw [List.mem_eraseDups] at hd
        exact hd
      exfalso
      apply hdnot
      rw [List.mem_eraseDups]
      exact mem_keys_of_mem_expand xs ds d hde
  have hR : ((B.expand (xs, ds)).eraseDups.map (fun k => B.mass (xs, ds) k * f k)).sum =
      ((B.expand (xs, ds)).map f).sum := by
    rw [← dedup_weight_sum f (B.expand (xs, ds))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]
  rw [hL, hR]
  exact (countP_eq_sum_toNat (B.expand (xs, ds)) (fun y => decide (y ≤ z))).symm

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro bp i
  rcases bp with ⟨xs, ds⟩
  let E := B.expand (xs, ds)
  let L := E.mergeSort
  let K := (live B (xs, ds)).mergeSort
  let p : Nat → Bool := fun x => i < below B (xs, ds) x
  change K.find? p = L[i]?
  have hpermLE : L.Perm E := by
    dsimp [L]
    exact List.mergeSort_perm E _
  have hbelow (x : Nat) : below B (xs, ds) x = L.countP (fun y => decide (y ≤ x)) := by
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
    have hvmass : 0 < B.mass (xs, ds) v := by
      rw [← count_expand_eq_mass]
      exact List.count_pos_iff.mpr hvE
    have hvkeys : v ∈ B.keys (xs, ds) := mem_keys_of_mem_expand xs ds v hvE
    have hvlive : v ∈ live B (xs, ds) := by
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
      exact sorted_mergeSort (live B (xs, ds))
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_OrderStatistic

