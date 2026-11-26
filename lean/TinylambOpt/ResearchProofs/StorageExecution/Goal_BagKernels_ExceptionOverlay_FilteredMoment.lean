import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_FilteredMoment

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private abbrev B := bag

/-! ## Generic list lemmas -/

private theorem countP_add_neg {α : Type} (p : α → Bool) (l : List α) :
    l.countP p + l.countP (fun x => !p x) = l.length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h]
        omega
      · simp [h]
        omega

private theorem countP_or_disjoint {α : Type} (l : List α) (p q r : α → Bool)
    (h : ∀ x ∈ l, p x = true ↔ (q x = true ∨ r x = true))
    (hd : ∀ x ∈ l, ¬ (q x = true ∧ r x = true)) :
    l.countP p = l.countP q + l.countP r := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxs : ∀ y ∈ xs, p y = true ↔ (q y = true ∨ r y = true) := by
        intro y hy; exact h y (by simp [hy])
      have hdxs : ∀ y ∈ xs, ¬ (q y = true ∧ r y = true) := by
        intro y hy; exact hd y (by simp [hy])
      have ih' := ih hxs hdxs
      have hx := h x (by simp)
      have hdx' := hd x (by simp)
      by_cases hqx : q x = true
      · have hrxf : r x = false := by
          by_cases hr : r x = true
          · exact False.elim (hdx' ⟨hqx, hr⟩)
          · exact Bool.eq_false_of_ne_true hr
        have hpx : p x = true := hx.mpr (Or.inl hqx)
        simp [hpx, hqx, hrxf, ih']
        omega
      · by_cases hrx : r x = true
        · have hpx : p x = true := hx.mpr (Or.inr hrx)
          have hqxf : q x = false := Bool.eq_false_of_ne_true hqx
          simp [hpx, hqxf, hrx, ih']
          omega
        · have hpx : p x = false := by
            cases hpv : p x with
            | true =>
                have hpt : p x = true := hpv
                rcases hx.mp hpt with hqq | hrr
                · exact False.elim (hqx hqq)
                · exact False.elim (hrx hrr)
            | false => rfl
          have hqxf : q x = false := Bool.eq_false_of_ne_true hqx
          have hrxf : r x = false := Bool.eq_false_of_ne_true hrx
          simp [hpx, hqxf, hrxf, ih']

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

private theorem count_mem_range (a n : Nat) (ha : a < n) : (List.range n).count a = 1 := by
  exact count_eq_one_of_mem_nodup List.nodup_range (List.mem_range.mpr ha)

private theorem countP_eq_count {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.countP (fun x => x == a) = l.count a := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.countP_cons, List.count_cons, ih]

private theorem count_mem_range_filter (l : List Nat) (hn : l.Nodup) (n : Nat) :
    (List.range n).countP (fun i => decide (i ∈ l)) = (l.filter (fun i => i < n)).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hn
      have haas : a ∉ as := hn.1
      have hnas : as.Nodup := hn.2
      have ih' := ih hnas
      rw [List.filter_cons]
      by_cases han : a < n
      · have hdis : ∀ x ∈ List.range n, ¬ ((x == a) = true ∧ (decide (x ∈ as)) = true) := by
          intro x hx hboth
          have hxa : x = a := beq_iff_eq.mp hboth.1
          exact haas (hxa ▸ (of_decide_eq_true hboth.2))
        have hor : ∀ x ∈ List.range n,
            (decide (x ∈ a :: as)) = true ↔ ((x == a) = true ∨ (decide (x ∈ as)) = true) := by
          intro x hx
          simp only [List.mem_cons, decide_eq_true_eq]
          constructor
          · intro h
            rcases h with hxa | hxas
            · exact Or.inl (beq_iff_eq.mpr hxa)
            · exact Or.inr hxas
          · intro h
            rcases h with hxa | hxas
            · exact Or.inl (beq_iff_eq.mp hxa)
            · exact Or.inr hxas
        have hcount := countP_or_disjoint (List.range n) (fun x => decide (x ∈ a :: as))
          (fun x => x == a) (fun x => decide (x ∈ as)) hor hdis
        rw [hcount, countP_eq_count, count_mem_range a n han, ih']
        simp [han]
        omega
      · have hnage : n ≤ a := Nat.le_of_not_gt han
        have hcongr : (List.range n).countP (fun i => decide (i ∈ a :: as)) =
            (List.range n).countP (fun i => decide (i ∈ as)) := by
          apply List.countP_congr
          intro i hi
          have hin : i < n := List.mem_range.mp hi
          simp only [List.mem_cons, decide_eq_true_eq]
          constructor
          · intro h
            rcases h with hia | hias
            · subst i
              exact False.elim (Nat.not_lt_of_ge hnage hin)
            · exact hias
          · intro hias
            exact Or.inr hias
        rw [hcongr, ih']
        simp [han]

/-! ## Lookup facts -/

private theorem lookup_none_iff_not_mem_fst (es : List (Nat × Nat)) (i : Nat) :
    es.lookup i = none ↔ i ∉ es.map Prod.fst := by
  induction es with
  | nil => simp
  | cons r es ih =>
      obtain ⟨j, v⟩ := r
      by_cases hij : i == j
      · have hij' : i = j := beq_iff_eq.mp hij
        subst j
        simp
      · have hne : i ≠ j := by
          intro h
          exact hij (beq_iff_eq.mpr h)
        simp [List.lookup_cons, hij, hne, ih]

private theorem lookup_some_mem_fst (es : List (Nat × Nat)) (i k : Nat) :
    es.lookup i = some k → i ∈ es.map Prod.fst := by
  induction es with
  | nil => simp
  | cons r es ih =>
      obtain ⟨j, v⟩ := r
      by_cases hij : i == j
      · intro h
        have hij' : i = j := beq_iff_eq.mp hij
        subst j
        simp
      · intro h
        have htail : es.lookup i = some k := by
          simpa [List.lookup_cons, hij] using h
        have hmem : i ∈ es.map Prod.fst := ih htail
        simp [hmem]

/-! ## ExceptionOverlay bag facts -/

private abbrev pos (es : List (Nat × Nat)) : List Nat := (es.map Prod.fst).eraseDups
private abbrev ps (n : Nat) (es : List (Nat × Nat)) : List Nat := (pos es).filter (fun i => i < n)

private theorem pos_nodup (es : List (Nat × Nat)) : (pos es).Nodup := by
  simpa [pos] using eraseDups_nodup (es.map Prod.fst)

private theorem count_lookup_none (es : List (Nat × Nat)) (n : Nat) :
    (List.range n).countP (fun i => es.lookup i == none) = n - (ps n es).length := by
  have hcongr : (List.range n).countP (fun i => es.lookup i == none) =
      (List.range n).countP (fun i => !(decide (i ∈ pos es))) := by
    apply List.countP_congr
    intro i hi
    have hn : (es.lookup i == none) = true ↔ es.lookup i = none := by
      cases es.lookup i <;> simp
    have hnot : es.lookup i = none ↔ i ∉ pos es := by
      constructor
      · intro hnone
        have hnf := (lookup_none_iff_not_mem_fst es i).mp hnone
        intro hip
        exact hnf (List.mem_eraseDups.mp (by simpa [pos] using hip))
      · intro hip
        apply (lookup_none_iff_not_mem_fst es i).mpr
        intro hf
        exact hip (by simpa [pos] using (List.mem_eraseDups.mpr hf))
    constructor
    · intro h
      have hnone := hn.mp h
      have hni := hnot.mp hnone
      simp [hni]
    · intro h
      have hni : i ∉ pos es := by simpa using h
      exact hn.mpr (hnot.mpr hni)
  have hc := count_mem_range_filter (pos es) (pos_nodup es) n
  have hneg := countP_add_neg (fun i => decide (i ∈ pos es)) (List.range n)
  simp only [List.length_range] at hneg
  rw [hcongr]
  change (List.range n).countP (fun i => !(decide (i ∈ pos es))) = n - ((pos es).filter (fun i => i < n)).length
  rw [← hc]
  omega

private theorem count_lookup_some (es : List (Nat × Nat)) (n k : Nat) :
    (List.range n).countP (fun i => es.lookup i == some k) =
      (ps n es).countP (fun i => es.lookup i == some k) := by
  let p : Nat → Bool := fun i => es.lookup i == some k
  have hndF : ((List.range n).filter p).Nodup :=
    List.Sublist.nodup List.filter_sublist List.nodup_range
  have hndPs : (ps n es).Nodup := by
    dsimp [ps]
    exact List.Sublist.nodup List.filter_sublist (pos_nodup es)
  have hndPsF : ((ps n es).filter p).Nodup :=
    List.Sublist.nodup List.filter_sublist hndPs
  have hmem : ∀ i, i ∈ (List.range n).filter p ↔ i ∈ (ps n es).filter p := by
    intro i
    simp only [List.mem_filter]
    constructor
    · rintro ⟨hir, hp⟩
      have hlt : i < n := List.mem_range.mp hir
      have hp' : (es.lookup i == some k) = true := by simpa [p] using hp
      have hsome : es.lookup i = some k := by
        cases hli : es.lookup i with
        | none => simp [hli] at hp'
        | some v =>
            have hvk : (v == k) = true := by simpa [hli] using hp'
            have hvke : v = k := beq_iff_eq.mp hvk
            simp [hvke]
      have hf : i ∈ es.map Prod.fst := lookup_some_mem_fst es i k hsome
      have hipos : i ∈ pos es := by simpa [pos] using (List.mem_eraseDups.mpr hf)
      have hips : i ∈ ps n es := by
        change i ∈ (pos es).filter (fun j => j < n)
        exact List.mem_filter.mpr ⟨hipos, by simp [hlt]⟩
      have hips' : i ∈ pos es ∧ decide (i < n) = true := by
        simpa only [ps, List.mem_filter] using hips
      exact ⟨hips', hp⟩
    · rintro ⟨hips, hp⟩
      have hfil : i ∈ pos es ∧ decide (i < n) = true := by
        simpa only [ps, List.mem_filter] using hips
      have hipos : i ∈ pos es := hfil.1
      have hlt : i < n := of_decide_eq_true hfil.2
      exact ⟨List.mem_range.mpr hlt, hp⟩
  have hperm : ((List.range n).filter p).Perm ((ps n es).filter p) := by
    rw [List.perm_iff_count]
    intro i
    by_cases hi : i ∈ (List.range n).filter p
    · have hi2 : i ∈ (ps n es).filter p := (hmem i).mp hi
      rw [count_eq_one_of_mem_nodup hndF hi, count_eq_one_of_mem_nodup hndPsF hi2]
    · have hi2 : i ∉ (ps n es).filter p := by
        intro h
        exact hi ((hmem i).mpr h)
      rw [List.count_eq_zero.mpr hi, List.count_eq_zero.mpr hi2]
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  exact hperm.length_eq

private theorem count_map_eq_countP (l : List Nat) (g : Nat → Nat) (k : Nat) :
    (l.map g).count k = l.countP (fun i => g i == k) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons]
      rw [List.count_cons, List.countP_cons, ih]

private theorem countP_val_split (es : List (Nat × Nat)) (n d k : Nat) :
    (List.range n).countP (fun i => (es.lookup i).getD d == k) =
      (List.range n).countP (fun i => (es.lookup i == none) && (d == k)) +
      (List.range n).countP (fun i => es.lookup i == some k) := by
  apply countP_or_disjoint
  · intro i hi
    cases es.lookup i <;> simp
  · intro i hi hboth
    cases hli : es.lookup i <;> simp [hli] at hboth

private theorem count_expand_eq_mass (n d k : Nat) (es : List (Nat × Nat)) :
    (B.expand (n, d, es)).count k = B.mass (n, d, es) k := by
  change ((List.range n).map (fun i => (es.lookup i).getD d)).count k =
    (if k == d then n - (ps n es).length else 0) +
      (ps n es).countP (fun i => es.lookup i == some k)
  rw [count_map_eq_countP]
  rw [countP_val_split]
  have hsplit : (List.range n).countP (fun i => (es.lookup i == none) && (d == k)) =
      if d == k then (List.range n).countP (fun i => es.lookup i == none) else 0 := by
    by_cases hdk : d == k
    · simp [hdk]
    · simp [hdk]
  rw [hsplit]
  rw [count_lookup_none]
  rw [count_lookup_some]
  by_cases hdk : d == k
  · have hkd : (k == d) = true := beq_iff_eq.mpr (beq_iff_eq.mp hdk).symm
    simp [hdk, hkd]
  · have hkd : (k == d) = false := by
      apply Bool.eq_false_of_ne_true
      intro h
      exact hdk (beq_iff_eq.mpr (beq_iff_eq.mp h).symm)
    simp [hdk, hkd]

private theorem lookup_some_mem_snd (es : List (Nat × Nat)) (i v : Nat) :
    es.lookup i = some v → v ∈ es.map Prod.snd := by
  induction es with
  | nil => simp
  | cons r es ih =>
      obtain ⟨j, w⟩ := r
      by_cases hij : i == j
      · intro h
        have hwv : w = v := by simpa [List.lookup_cons, hij] using h
        subst v
        simp
      · intro h
        have ht : es.lookup i = some v := by simpa [List.lookup_cons, hij] using h
        simp
        exact Or.inr (by simpa using (ih ht))

private theorem mem_keys_of_mem_expand (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    k ∈ B.expand (n, d, es) → k ∈ B.keys (n, d, es) := by
  intro h
  change k ∈ (List.range n).map (fun i => (es.lookup i).getD d) at h
  rcases List.mem_map.mp h with ⟨i, hi, hval⟩
  cases hli : es.lookup i with
  | none =>
      rw [hli] at hval
      simp at hval
      subst k
      simp [B, bag]
  | some v =>
      rw [hli] at hval
      simp at hval
      subst k
      have hvsnd : v ∈ es.map Prod.snd := lookup_some_mem_snd es i v hli
      change v ∈ d :: es.map Prod.snd
      simp [hvsnd]

private theorem mass_zero_of_not_mem_expand (n d k : Nat) (es : List (Nat × Nat)) :
    k ∉ B.expand (n, d, es) → B.mass (n, d, es) k = 0 := by
  intro h
  have hc : (B.expand (n, d, es)).count k = 0 := List.count_eq_zero.mpr h
  rw [count_expand_eq_mass] at hc
  exact hc

/-! ## Generic sum lemmas -/

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

/-! ## Goal -/

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  obtain ⟨n, d, es⟩ := p
  let f : Nat → Nat := fun x => if lo ≤ x && x < hi then x * x else 0
  let hsq : Nat → Nat := fun x => x * x
  let pred : Nat → Bool := fun x => lo ≤ x && x < hi
  change ((B.keys (n, d, es)).eraseDups.map (fun k => B.mass (n, d, es) k * f k)).sum =
    (((B.expand (n, d, es)).filter pred).map hsq).sum
  have hR1 : (((B.expand (n, d, es)).filter pred).map hsq).sum =
      ((B.expand (n, d, es)).map f).sum := by
    simpa [f, hsq, pred] using (sum_filter_eq_sum_ite (B.expand (n, d, es)) pred hsq)
  have hR2 : ((B.expand (n, d, es)).map f).sum =
      (((B.expand (n, d, es)).eraseDups.map fun k => B.mass (n, d, es) k * f k).sum) := by
    rw [← dedup_weight_sum f (B.expand (n, d, es))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [count_expand_eq_mass]
  have hL : ((B.keys (n, d, es)).eraseDups.map (fun k => B.mass (n, d, es) k * f k)).sum =
      ((B.expand (n, d, es)).eraseDups.map (fun k => B.mass (n, d, es) k * f k)).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro z hz hznot
      have hznot' : z ∉ B.expand (n, d, es) := by
        intro hze
        exact hznot (by rw [List.mem_eraseDups]; exact hze)
      have hm := mass_zero_of_not_mem_expand n d z es hznot'
      simp [hm]
    · intro z hz hznot
      have hze : z ∈ B.expand (n, d, es) := by
        rw [List.mem_eraseDups] at hz
        exact hz
      exfalso
      apply hznot
      rw [List.mem_eraseDups]
      exact mem_keys_of_mem_expand n d es z hze
  calc
    ((B.keys (n, d, es)).eraseDups.map (fun k => B.mass (n, d, es) k * f k)).sum
        = ((B.expand (n, d, es)).eraseDups.map (fun k => B.mass (n, d, es) k * f k)).sum := hL
    _ = ((B.expand (n, d, es)).map f).sum := hR2.symm
    _ = (((B.expand (n, d, es)).filter pred).map hsq).sum := hR1.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_FilteredMoment
