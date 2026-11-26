import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Two independent ways of describing key multiplicities agree, and every physically
produced key is among the declared candidates.

Named results include `beq_tt`, `beq_ff`, `beq_comm`.
-/

set_option maxHeartbeats 0
set_option linter.unusedVariables false

/-! Shared machinery for the StorageExecution.BagKernels research batch. -/

namespace TinylambOpt.ResearchProofs.StorageExecution.BagKernels

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open List

section Helpers

theorem beq_tt {a b : Nat} (h : a = b) : (a == b) = true :=
  (Nat.beq_eq_true_eq a b).mpr h

theorem beq_ff {a b : Nat} (h : ¬ a = b) : (a == b) = false :=
  Bool.eq_false_of_ne_true (fun he => h ((Nat.beq_eq_true_eq a b).mp he))

theorem beq_comm (a b : Nat) : (a == b) = (b == a) := by
  induction a generalizing b with
  | zero =>
    cases b with
    | zero => rfl
    | succ b' => rfl
  | succ a' ih =>
    cases b with
    | zero => rfl
    | succ b' => exact ih b'

theorem toNat_eq_one (h : c = true) : c.toNat = 1 := by cases c <;> simp_all
theorem toNat_eq_zero (h : c = false) : c.toNat = 0 := by cases c <;> simp_all
theorem decide_bool (b : Bool) : decide (b = true) = b := by cases b <;> rfl

theorem le_total_bool (a b : Nat) : (a ≤ b || b ≤ a) = true := by
  by_cases h : a ≤ b
  · rw [decide_eq_true h]
    simp
  · rw [decide_eq_false h, Bool.false_or]
    exact decide_eq_true (Nat.le_of_not_le h)

theorem andb_decide (p q : Bool) : (p && q) = decide (p ∧ q) := by
  cases p <;> cases q <;> simp

theorem max1_eq (n : Nat) : max 1 n = n + (decide (n == 0)).toNat := by
  cases n with
  | zero => rfl
  | succ m =>
    rw [Nat.max_eq_right (by omega : 1 ≤ m + 1)]
    simp [beq_ff (by omega : ¬(m + 1 = 0))]

theorem comb_0 : comb 0 = 0 := rfl

theorem comb_succ (n : Nat) : comb (n + 1) = comb n + n := by
  show (n + 1) * n / 2 = n * (n - 1) / 2 + n
  cases n with
  | zero => rfl
  | succ m =>
    have h1 : (m + 2) * (m + 1) = (m + 1) * m + 2 * (m + 1) := by
      simp only [Nat.mul_add, Nat.add_mul, Nat.mul_one, Nat.mul_comm, Nat.add_assoc]
    rw [h1, Nat.add_mul_div_left _ _ (by omega : (0 : Nat) < 2)]
    simp only [Nat.add_sub_cancel_right]

theorem tri_succ (t : Nat) : (t + 1) * (t + 2) / 2 = t * (t + 1) / 2 + (t + 1) := by
  have h := comb_succ (t + 1)
  simp only [comb, Nat.add_sub_cancel] at h
  rw [Nat.mul_comm (t + 2) (t + 1), Nat.mul_comm (t + 1) t] at h
  exact h

theorem sum_map_congr_of_mem (l : List Nat) (f g : Nat → Nat)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [map_cons, sum_cons, map_cons, sum_cons, h a (List.Mem.head _),
      ih (fun x hx => h x (List.Mem.tail _ hx))]

theorem sum_map_perm (f : Nat → Nat) : ∀ {l₁ l₂ : List Nat}, l₁ ~ l₂ →
    (l₁.map f).sum = (l₂.map f).sum := by
  intro l₁ l₂ h
  induction h with
  | nil => rfl
  | @cons a x y _ ih => rw [map_cons, sum_cons, map_cons, sum_cons, ih]
  | @swap a b l =>
    simp only [map_cons, sum_cons]
    omega
  | @trans _ _ _ _ _ ih₁ ih₂ => exact Eq.trans ih₁ ih₂

theorem sum_map_const (l : List Nat) (c : Nat) : (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons a as ih =>
    rw [map_cons, sum_cons, length_cons, ih, Nat.succ_mul]
    omega

theorem sum_map_replicate (f : Nat → Nat) :
    ∀ (n : Nat) (a : Nat), ((List.replicate n a).map f).sum = n * f a := by
  intro n
  induction n with
  | zero => intro a; simp
  | succ m ih =>
    intro a
    rw [replicate_succ, map_cons, sum_cons, ih, Nat.succ_mul]
    omega

theorem sum_add_map (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [map_cons, sum_cons, map_cons, sum_cons, map_cons, sum_cons, ih]
    omega

theorem sum_length_map {β : Type} (l : List Nat) (g : Nat → List β) :
    (l.map (fun x => (g x).length)).sum = (l.flatMap g).length := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [map_cons, sum_cons, flatMap_cons, length_append, ih]

theorem filter_congr_helper {p q : Nat → Bool} : ∀ (l : List Nat),
    (∀ x ∈ l, p x = q x) → l.filter p = l.filter q := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a as ih =>
    intro h
    have hq : p a = q a := h a (List.Mem.head _)
    cases hpa : p a with
    | true =>
      rw [filter_cons_of_pos hpa,
        filter_cons_of_pos (show q a = true from by rw [← hq]; exact hpa),
        ih (fun x hx => h x (List.Mem.tail _ hx))]
    | false =>
      rw [filter_cons_of_neg (by simpa using hpa),
        filter_cons_of_neg (by simpa using (hq.symm.trans hpa)),
        ih (fun x hx => h x (List.Mem.tail _ hx))]

theorem sum_filter_eq (g : Nat → Nat) (p : Nat → Bool) : ∀ (l : List Nat),
    (∀ x ∈ l, ¬ p x → g x = 0) → (l.map g).sum = ((l.filter p).map g).sum := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a as ih =>
    intro hz
    rw [map_cons]
    cases hpa : p a with
    | true =>
      rw [filter_cons_of_pos hpa, map_cons, sum_cons,
        ih (fun x hx hnp => hz x (List.Mem.tail _ hx) hnp), sum_cons]
    | false =>
      rw [filter_cons_of_neg (by simpa using hpa), sum_cons,
        ih (fun x hx hnp => hz x (List.Mem.tail _ hx) hnp),
        hz a (List.Mem.head _) (by simpa using hpa)]
      omega

theorem map_idem (l : List Nat) : l.map (fun x => x) = l := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [map_cons, ih]

theorem find?_congr (l : List Nat) (p q : Nat → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.find? p = l.find? q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    cases hpa : p a with
    | true =>
      have hqa : q a = true := by rw [← h a (List.Mem.head _)]; exact hpa
      rw [find?_cons_of_pos hpa, find?_cons_of_pos hqa]
    | false =>
      have hqa : q a = false := by rw [← h a (List.Mem.head _)]; exact hpa
      rw [find?_cons_of_neg (by simpa using hpa), find?_cons_of_neg (by simpa using hqa),
        ih (fun x hx => h x (List.Mem.tail _ hx))]

theorem filter_true (l : List Nat) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [filter_cons_of_pos (rfl : (fun _ => (true : Bool)) a = true), ih]

theorem nodup_filter {p : Nat → Bool} : ∀ (l : List Nat), l.Nodup → (l.filter p).Nodup := by
  intro l
  induction l with
  | nil => intro _; exact nodup_nil
  | cons a as ih =>
    intro h
    have hpc := nodup_cons.mp h
    cases hpa : p a with
    | true =>
      rw [filter_cons_of_pos hpa]
      refine nodup_cons.2 ⟨fun m => absurd (mem_filter.mp m).1 hpc.1, ih hpc.2⟩
    | false =>
      rw [filter_cons_of_neg (by simpa using hpa)]
      exact ih hpc.2

theorem nodup_filter_eraseDups : ∀ (l : List Nat), ∀ (p : Nat → Bool),
    ((l.filter p).eraseDups).Nodup := by
  intro l
  induction l with
  | nil => intro p; rw [filter_nil, eraseDups_nil]; exact nodup_nil
  | cons a as ih =>
    intro p
    cases hpa : p a with
    | false => rw [filter_cons_of_neg (by simpa using hpa)]; exact ih _
    | true =>
      rw [filter_cons_of_pos hpa, eraseDups_cons]
      refine nodup_cons.2 ⟨fun m => ?_, ?_⟩
      · obtain ⟨_, h1⟩ := mem_filter.mp (mem_eraseDups.mp m)
        exact absurd h1 (by simp)
      · rw [filter_filter]
        exact ih _

theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  have h : l.filter (fun _ => true) = l := filter_true l
  rw [← h]
  exact nodup_filter_eraseDups l (fun _ => true)

theorem filter_beq_replicate (a : Nat) : ∀ (l : List Nat),
    l.filter (· == a) = List.replicate (l.countP (· == a)) a := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
    cases h : x == a with
    | false =>
      rw [filter_cons_of_neg (by simpa using h),
        countP_cons_of_neg (by simpa using h), ih]
    | true =>
      have hxa : x = a := (Nat.beq_eq_true_eq x a).mp h
      subst hxa
      rw [filter_cons_of_pos (p := fun y => y == x) h,
        countP_cons_of_pos (p := fun y => y == x) h, ih, replicate_succ]

theorem filter_beq_ed (a : Nat) : ∀ (l : List Nat),
    (l.filter (· == a)).eraseDups = if a ∈ l then [a] else [] := by
  intro l
  by_cases hmem : a ∈ l
  · obtain ⟨x, xs, heq⟩ : ∃ y ys, l.filter (· == a) = y :: ys := by
      cases h : l.filter (· == a) with
      | nil =>
        have h2 : a ∈ l.filter (fun x => x == a) := mem_filter.mpr ⟨hmem, beq_tt rfl⟩
        rw [h] at h2
        exact absurd h2 List.not_mem_nil
      | cons y ys => exact ⟨y, ys, rfl⟩
    have hall : ∀ y ∈ x :: xs, (y == a) = true := by
      intro y hy
      have hyf : y ∈ l.filter (· == a) := by rw [heq]; exact hy
      exact (mem_filter.mp hyf).2
    have hxa : x = a := (Nat.beq_eq_true_eq x a).mp (hall x (List.Mem.head xs))
    subst hxa
    have hnil : xs.filter (fun b => !(b == x)) = [] := by
      refine filter_eq_nil_iff.mpr (fun y hy => ?_)
      intro h3
      have hya := hall y (List.Mem.tail x hy)
      rw [hya] at h3
      exact absurd h3 (by simp)
    rw [heq, eraseDups_cons, hnil, eraseDups_nil, if_pos hmem]
  · have hnil : l.filter (· == a) = [] := by
      refine filter_eq_nil_iff.mpr (fun y hy => ?_)
      intro h2
      exact hmem (((Nat.beq_eq_true_eq y a).mp h2) ▸ hy)
    rw [hnil, eraseDups_nil, if_neg hmem]

theorem filter_beq_eq_single (l : List Nat) (k : Nat) (h : l.count k = 1) :
    l.filter (· == k) = [k] := by
  have hl : (l.filter (fun x => x == k)).length = 1 := by
    rw [← countP_eq_length_filter, ← count_eq_countP]
    exact h
  cases h2 : l.filter (· == k) with
  | nil => exact absurd hl (by rw [h2]; simp)
  | cons x xs =>
    cases xs with
    | nil =>
      have hm : x ∈ l.filter (· == k) := by rw [h2]; exact List.Mem.head _
      have hx : x = k := (Nat.beq_eq_true_eq x k).mp (mem_filter.mp hm).2
      subst hx
      rfl
    | cons y ys =>
      rw [h2] at hl
      simp only [length_cons] at hl
      omega

theorem countP_map_toNat (q : Nat → Bool) : ∀ (l : List Nat),
    (l.map (fun x => (q x).toNat)).sum = (l.filter q).length := by
  intro l
  induction l with
  | nil => rfl
  | cons a as ih =>
    cases hq : q a with
    | true =>
      rw [map_cons, sum_cons, filter_cons_of_pos hq, toNat_eq_one hq, ih, length_cons]
      omega
    | false =>
      rw [map_cons, sum_cons, filter_cons_of_neg (by simpa using hq), toNat_eq_zero hq, ih]
      omega

theorem sum_map_ite (p : Nat → Bool) (f : Nat → Nat) : ∀ (l : List Nat),
    (l.map (fun x => if p x then f x else 0)).sum = ((l.filter p).map f).sum := by
  intro l
  induction l with
  | nil => rfl
  | cons a as ih =>
    cases hpa : p a with
    | true =>
      rw [map_cons, sum_cons, if_pos hpa, filter_cons_of_pos (p := p) hpa, map_cons, sum_cons, ih]
    | false =>
      rw [map_cons, sum_cons, if_neg (by simpa using hpa),
        filter_cons_of_neg (by simpa using hpa), ih]
      omega

theorem contains_decide (l : List Nat) (a : Nat) : l.contains a = decide (a ∈ l) := by
  by_cases h : a ∈ l
  · have h1 : l.contains a = true := by simpa [contains_iff_mem] using h
    have h2 : decide (a ∈ l) = true := by simpa [decide_eq_true_eq] using h
    rw [h1, h2]
  · have h1 : l.contains a = false := by simpa [contains_iff_mem] using h
    have h2 : decide (a ∈ l) = false := by simpa [decide_eq_false_iff_not] using h
    rw [h1, h2]

theorem flatMap_congr_of_mem (l : List Nat) (f g : Nat → List Nat)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [flatMap_cons, h a (List.Mem.head _), flatMap_cons,
      ih (fun x hx => h x (List.Mem.tail _ hx))]

theorem count_eq_length_filter (l : List Nat) (k : Nat) :
    l.count k = (l.filter (fun x => k == x)).length := by
  rw [count_eq_countP, countP_eq_length_filter]
  rw [filter_congr_helper l (by intro x _; rw [beq_comm])]

theorem sum_flatMap_replicate (c : Nat → Nat) (f : Nat → Nat) : ∀ (s : List Nat),
    ((s.flatMap (fun x => List.replicate (c x) x)).map f).sum =
      (s.map (fun x => c x * f x)).sum := by
  intro s
  induction s with
  | nil => rfl
  | cons a as ih =>
    rw [flatMap_cons, map_append, sum_append, sum_map_replicate, map_cons, sum_cons, ih]

theorem count_flatReplicate (c : Nat → Nat) : ∀ (s : List Nat), s.Nodup →
    ∀ k, (s.flatMap (fun x => List.replicate (c x) x)).count k =
      if k ∈ s then c k else 0 := by
  intro s
  induction s with
  | nil => intro _ k; simp
  | cons a as ih =>
    intro hnd k
    have hpc := pairwise_cons.mp hnd
    by_cases hk : k = a
    · subst hk
      rw [flatMap_cons, count_append, count_replicate, if_pos (beq_tt rfl), ih hpc.2]
      have h1 : (if k ∈ (k :: as) then c k else 0) = c k := if_pos (List.Mem.head as)
      have h2 : (if k ∈ as then c k else 0) = 0 :=
        if_neg (fun hm => absurd rfl (hpc.1 k hm))
      rw [h1, h2]
      omega
    · rw [flatMap_cons, count_append, count_replicate,
        if_neg (fun h2 => hk ((Nat.beq_eq_true_eq a k).mp h2).symm), ih hpc.2]
      by_cases hin : k ∈ as
      · rw [if_pos hin]
        have h1 : (if k ∈ (a :: as) then c k else 0) = c k := if_pos (List.Mem.tail a hin)
        rw [h1]
        omega
      · rw [if_neg hin]
        have hnotin : ¬ (k ∈ a :: as) := by
          rw [mem_cons]
          rintro (h2 | h2)
          · exact hk h2
          · exact hin h2
        rw [if_neg hnotin]

theorem sum_count_mul (l : List Nat) (f : Nat → Nat) :
    ((l.eraseDups).map (fun k => l.count k * f k)).sum = (l.map f).sum := by
  have hperm : (l.eraseDups.flatMap (fun k => List.replicate (l.count k) k)) ~ l := by
    rw [perm_iff_count]
    intro k
    rw [count_flatReplicate (fun m => l.count m) l.eraseDups (nodup_eraseDups l) k]
    by_cases hk : k ∈ l
    · rw [if_pos (mem_eraseDups.mpr hk)]
    · rw [if_neg (fun he => hk (mem_eraseDups.mp he)), count_eq_zero.mpr hk]
  rw [(sum_flatMap_replicate (fun m => l.count m) f l.eraseDups).symm]
  exact sum_map_perm f hperm

theorem zipIdx_add (l : List Nat) (n m : Nat) :
    l.zipIdx (n + m) = (l.zipIdx n).map (fun xi => (xi.1, xi.2 + m)) := by
  induction l generalizing n with
  | nil => rfl
  | cons a as ih =>
    rw [zipIdx_cons, zipIdx_cons, map_cons,
      show (n + m + 1) = (n + 1 + m) from by omega, ih (n + 1)]

theorem eraseDups_partition_perm (p : Nat → Bool) : ∀ (l : List Nat),
    l.eraseDups ~ (l.filter p).eraseDups ++ (l.filter (fun x => !p x)).eraseDups := by
  intro l
  refine (perm_ext_iff_of_nodup (nodup_eraseDups l) ?_).mpr (fun x => ?_)
  · refine nodup_append.2 ⟨nodup_eraseDups _, nodup_eraseDups _, ?_⟩
    intro y hy₁ w hw he
    obtain ⟨_, hpy⟩ := mem_filter.mp (mem_eraseDups.mp hy₁)
    obtain ⟨_, hnp⟩ := mem_filter.mp (mem_eraseDups.mp hw)
    rw [← he, hpy] at hnp
    exact absurd hnp (by simp)
  · rw [mem_append, mem_eraseDups, mem_eraseDups, mem_eraseDups, mem_filter, mem_filter]
    by_cases hpx : p x <;> by_cases hx : x ∈ l <;> simp_all

end Helpers

/-- Two independent ways of describing key multiplicities agree, and every
physically produced key is among the declared candidates. -/
structure WellFormed (b : Bag α) : Prop where
  mass_eq : ∀ p k, (b.expand p).count k = b.mass p k
  cover : ∀ p k, k ∈ b.expand p → k ∈ b.keys p

theorem live_nodup (b : Bag α) (p : α) : (live b p).Nodup := by
  unfold live
  exact nodup_filter _ (nodup_eraseDups _)

theorem live_perm (b : Bag α) (h : WellFormed b) (p : α) :
    live b p ~ (b.expand p).eraseDups := by
  unfold live
  refine (perm_ext_iff_of_nodup (nodup_filter _ (nodup_eraseDups _)) (nodup_eraseDups _)).mpr ?_
  intro x
  rw [mem_filter, mem_eraseDups, mem_eraseDups]
  refine ⟨fun hx => ?_, fun hx => ⟨h.cover _ _ hx, ?_⟩⟩
  · have hc : 0 < (b.expand p).count x := by rw [h.mass_eq p x]; exact of_decide_eq_true hx.2
    exact count_pos_iff.mp hc
  · have hc : 0 < (b.expand p).count x := count_pos_iff.mpr hx
    rw [← h.mass_eq p x]
    exact decide_eq_true hc

theorem weighted_eq (b : Bag α) (h : WellFormed b) (p : α) (f : Nat → Nat) :
    weighted b p f = ((b.expand p).map f).sum := by
  rw [weighted]
  rw [sum_map_congr_of_mem (b.keys p).eraseDups (fun k => b.mass p k * f k)
    (fun k => (b.expand p).count k * f k) (by intro x _; rw [h.mass_eq p x])]
  have hsupp : ∀ k ∈ (b.keys p).eraseDups,
      ¬ (decide (0 < (b.expand p).count k) = true) →
        (b.expand p).count k * f k = 0 := by
    intro k _ hnp
    have h0 : (b.expand p).count k = 0 := by
      have hlt : ¬ (0 < (b.expand p).count k) :=
        fun hpos => absurd (decide_eq_true hpos) hnp
      omega
    rw [h0]
    exact Nat.zero_mul _
  rw [sum_filter_eq (fun k => (b.expand p).count k * f k)
    (fun k => decide (0 < (b.expand p).count k)) _ hsupp]
  have hfilt : (b.keys p).eraseDups.filter (fun k => decide (0 < (b.expand p).count k))
      ~ (b.expand p).eraseDups := by
    refine (perm_ext_iff_of_nodup (nodup_filter _ (nodup_eraseDups _)) (nodup_eraseDups _)).mpr ?_
    intro x
    rw [mem_filter, mem_eraseDups, mem_eraseDups]
    refine ⟨fun hx => count_pos_iff.mp (of_decide_eq_true hx.2),
      fun hx => ⟨h.cover _ _ hx, decide_eq_true (count_pos_iff.mpr hx)⟩⟩
  rw [sum_map_perm _ hfilt]
  exact sum_count_mul (b.expand p) f

theorem below_eq (b : Bag α) (h : WellFormed b) (p : α) (v : Nat) :
    below b p v = ((b.expand p).filter (fun y => decide (y ≤ v))).length := by
  rw [below, weighted_eq b h p, countP_map_toNat (fun y => decide (y ≤ v))]

theorem quantile_scan : ∀ (s : List Nat), Pairwise (fun a b => decide (a ≤ b) = true) s →
    s.Nodup →
    ∀ (t : List Nat), (∀ x, x ∈ t ↔ x ∈ s) → ∀ i,
      s.find? (fun x => i < (t.filter (fun y => decide (y ≤ x))).length) = t.mergeSort[i]? := by
  intro s
  induction s with
  | nil =>
    intro _ _ t ht i
    have ht0 : t = [] := by
      cases t with
      | nil => rfl
      | cons a as => exact absurd ((ht a).mp (List.Mem.head _)) (by simp)
    subst ht0
    rw [find?_nil, mergeSort_nil, getElem?_nil]
  | cons a as ih =>
    intro hp hnd t ht i
    have hp₁ := (pairwise_cons.mp hp).1
    let n := t.countP (fun x => x == a)
    have h₁ : t.filter (· == a) = List.replicate n a := filter_beq_replicate a t
    let t₂ := t.filter (fun x => !(x == a))
    have hperm : List.replicate n a ++ t₂ ~ t := by
      rw [← h₁]
      exact filter_append_perm (fun x => x == a) t
    have hmin : ∀ x ∈ t₂, decide (a ≤ x) = true ∧ ¬ (x = a) := by
      intro x hx
      rw [mem_filter] at hx
      have hne : ¬ (x = a) := by
        intro he
        exact absurd (beq_tt he) (by simpa using hx.2)
      have hxs := (ht x).mp hx.1
      rw [mem_cons] at hxs
      rcases hxs with rfl | hxs
      · exact absurd rfl hne
      · exact ⟨hp₁ x hxs, fun he => (nodup_cons.mp hnd).1 (by rw [← he]; exact hxs)⟩
    have hset₂ : ∀ x, x ∈ t₂ ↔ x ∈ as := by
      intro x
      rw [mem_filter]
      constructor
      · rintro ⟨hx, hn⟩
        have hne : ¬ (x = a) := by
          intro he
          rw [he] at hn
          simp at hn
        have hmem := (ht x).mp hx
        rw [mem_cons] at hmem
        rcases hmem with he | hx'
        · exact absurd he hne
        · exact hx'
      · intro hx
        refine ⟨(ht x).mpr (mem_cons.mpr (Or.inr hx)), ?_⟩
        cases hxa : x == a with
        | true =>
          exact absurd ((Nat.beq_eq_true_eq x a).mp hxa)
            (fun he => (nodup_cons.mp hnd).1 (by rw [← he]; exact hx))
        | false => rfl
    have hcount : ∀ x ∈ as,
        (t.filter (fun y => decide (y ≤ x))).length = n + (t₂.filter (fun y => decide (y ≤ x))).length := by
      intro x hx
      have hle : decide (a ≤ x) = true := hp₁ x hx
      calc (t.filter (fun y => decide (y ≤ x))).length
          = countP (fun y => decide (y ≤ x)) t := (countP_eq_length_filter).symm
        _ = countP (fun y => decide (y ≤ x)) (List.replicate n a ++ t₂) :=
            (Perm.countP_eq _ hperm).symm
        _ = countP (fun y => decide (y ≤ x)) (List.replicate n a)
              + countP (fun y => decide (y ≤ x)) t₂ := countP_append
        _ = n + countP (fun y => decide (y ≤ x)) t₂ := by
          rw [countP_replicate, if_pos hle]
        _ = n + (t₂.filter (fun y => decide (y ≤ x))).length := by rw [countP_eq_length_filter]
    have hlena : (t.filter (fun y => decide (y ≤ a))).length = n := by
      have hz : countP (fun y => decide (y ≤ a)) t₂ = 0 := by
        refine countP_eq_zero.mpr (fun x hx h => ?_)
        obtain ⟨hle, hne⟩ := hmin x hx
        have hax : a ≤ x := of_decide_eq_true hle
        have hxa : x ≤ a := by simpa using h
        exact absurd (Nat.le_antisymm hxa hax) hne
      calc (t.filter (fun y => decide (y ≤ a))).length
          = countP (fun y => decide (y ≤ a)) t := (countP_eq_length_filter).symm
        _ = countP (fun y => decide (y ≤ a)) (List.replicate n a ++ t₂) :=
            (Perm.countP_eq _ hperm).symm
        _ = countP (fun y => decide (y ≤ a)) (List.replicate n a)
              + countP (fun y => decide (y ≤ a)) t₂ := countP_append
        _ = n + 0 := by
          rw [countP_replicate, if_pos (decide_eq_true (Nat.le_refl a)), hz]
        _ = n := by omega
    have hM : t.mergeSort = List.replicate n a ++ t₂.mergeSort := by
      have pw : Pairwise (fun a b => decide (a ≤ b)) (t.mergeSort) :=
        pairwise_mergeSort (le := fun x y => decide (x ≤ y))
          (fun x y z hxy hyz =>
            decide_eq_true (Nat.le_trans (of_decide_eq_true hxy) (of_decide_eq_true hyz)))
          le_total_bool t
      have pw₂ : Pairwise (fun a b => decide (a ≤ b))
          (List.replicate n a ++ t₂.mergeSort) :=
        pairwise_append.mpr ⟨pairwise_replicate.mpr (Or.inr (decide_eq_true (Nat.le_refl a))),
          pairwise_mergeSort (le := fun x y => decide (x ≤ y))
            (fun x y z hxy hyz =>
              decide_eq_true (Nat.le_trans (of_decide_eq_true hxy) (of_decide_eq_true hyz)))
            le_total_bool t₂,
          fun x hx y hym => by
            have hx' : x = a := by
              rw [mem_replicate] at hx
              exact hx.2
            obtain ⟨hle, _⟩ := hmin y (mem_mergeSort.mp hym)
            rw [hx']
            exact hle⟩
      have hperm' : t.mergeSort ~ List.replicate n a ++ t₂.mergeSort :=
        Perm.trans (mergeSort_perm t (fun a b => decide (a ≤ b)))
          (Perm.trans hperm.symm
            (Perm.append (Perm.refl _)
              ((mergeSort_perm t₂ (fun a b => decide (a ≤ b))).symm)))
      refine Perm.eq_of_pairwise (fun x y _ _ hxy hyx =>
        Nat.le_antisymm (of_decide_eq_true hxy) (of_decide_eq_true hyx)) pw pw₂ hperm'
    rw [hM]
    by_cases hi : i < n
    · rw [find?_cons_of_pos (by simpa [hlena] using hi),
        getElem?_append_left (by simpa [length_replicate] using hi),
        getElem?_replicate_of_lt hi]
    · rw [find?_cons_of_neg (by simpa [hlena] using hi),
        getElem?_append_right (by
          simp only [length_replicate]
          exact Nat.le_of_not_lt hi)]
      have hconv : as.find? (fun x => i < (t.filter (fun y => decide (y ≤ x))).length)
          = as.find? (fun x => i - n < (t₂.filter (fun y => decide (y ≤ x))).length) :=
        find?_congr as (fun x => decide (i < (t.filter (fun y => decide (y ≤ x))).length))
          (fun x => decide (i - n < (t₂.filter (fun y => decide (y ≤ x))).length))
          (by intro x hx
              rw [hcount x hx]
              by_cases hlt : i < n + (t₂.filter (fun y => decide (y ≤ x))).length
              · have h2 : i - n < (t₂.filter (fun y => decide (y ≤ x))).length := by omega
                rw [decide_eq_true hlt, decide_eq_true h2]
              · have h2 : ¬ i - n < (t₂.filter (fun y => decide (y ≤ x))).length := by omega
                rw [decide_eq_false hlt, decide_eq_false h2])
      rw [hconv, ih (Pairwise.tail hp) (nodup_cons.mp hnd).2 t₂ hset₂ (i - n),
        length_replicate]

theorem selfPairs_list : ∀ (l : List Nat),
    ((l.eraseDups).map (fun k => comb (l.count k))).sum =
      (l.zipIdx.map (fun xi => ((l.drop (xi.2 + 1)).count xi.1))).sum := by
  intro l
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [eraseDups_cons, map_cons, sum_cons, count_cons_self, comb_succ,
      zipIdx_cons, map_cons, sum_cons]
    rw [show ((a :: as).drop ((0 : Nat) + 1)).count a = as.count a from rfl]
    rw [zipIdx_add as 0 1]
    have hfun : (fun xi : Nat × Nat => ((a :: as).drop (xi.2 + 1 + 1)).count xi.1)
        = (fun xi => (as.drop (xi.2 + 1)).count xi.1) := by
      funext xi
      show ((a :: as).drop (xi.2 + 1 + 1)).count xi.1 = (as.drop (xi.2 + 1)).count xi.1
      rw [show xi.2 + 1 + 1 = (xi.2 + 1) + 1 from by omega, drop_succ_cons]
    have htail : (map (fun xi => ((a :: as).drop (xi.2 + 1)).count xi.1)
        (map (fun xi => (xi.1, xi.2 + 1)) (as.zipIdx 0)))
        = (map (fun xi => (as.drop (xi.2 + 1)).count xi.1) (as.zipIdx 0)) := by
      rw [map_map]
      refine congrArg (fun F => List.map F (as.zipIdx 0)) (funext fun xi => ?_)
      show ((a :: as).drop (xi.2 + 1 + 1)).count xi.1 = (as.drop (xi.2 + 1)).count xi.1
      rw [show xi.2 + 1 + 1 = (xi.2 + 1) + 1 from by omega, drop_succ_cons]
    rw [htail]
    rw [← ih]
    rw [sum_map_congr_of_mem ((as.filter (fun b : Nat => !(b == a))).eraseDups)
      (fun k => comb ((a :: as).count k)) (fun k => comb (as.count k))
      (by intro k hk
          have hne : ¬ (a = k) := by
            intro he
            have hka : !(k == a) = true := (mem_filter.mp (mem_eraseDups.mp hk)).2
            rw [he] at hka
            simp at hka
          exact congrArg comb (count_cons_of_ne hne))]
    have part := eraseDups_partition_perm (fun x => x == a) as
    have hsum : (((as.filter (· == a)).eraseDups ++
          (as.filter (fun x => !(x == a))).eraseDups).map (fun k => comb (as.count k))).sum
        = (((as.filter (· == a)).eraseDups).map (fun k => comb (as.count k))).sum
          + (((as.filter (fun x => !(x == a))).eraseDups).map (fun k => comb (as.count k))).sum :=
      by rw [map_append, sum_append]
    have hX : ((as.eraseDups).map (fun k => comb (as.count k))).sum
        = (((as.filter (· == a)).eraseDups).map (fun k => comb (as.count k))).sum
          + (((as.filter (fun x => !(x == a))).eraseDups).map (fun k => comb (as.count k))).sum := by
      rw [← hsum]
      exact sum_map_perm (fun k => comb (as.count k)) part
    have hbeq : (((as.filter (· == a)).eraseDups).map (fun k => comb (as.count k))).sum
        = comb (as.count a) := by
      rw [filter_beq_ed a as]
      by_cases hmem : a ∈ as
      · rw [if_pos hmem]
        rfl
      · rw [if_neg hmem, count_eq_zero.mpr hmem]
        simp [comb]
    omega

theorem mass_beq_zero_not_contains (b : Bag α) (h : WellFormed b) (c : α) (x : Nat) :
    (b.mass c x == 0) = !(b.expand c).contains x := by
  by_cases hz : (b.expand c).count x = 0
  · have h1 : (b.mass c x == 0) = true := beq_tt (by rw [← h.mass_eq]; exact hz)
    have h2 : (b.expand c).contains x = false := by
      rw [contains_decide, decide_eq_false]
      exact fun hm => by
        have hc := count_pos_iff.mpr hm
        omega
    rw [h1, h2]
    rfl
  · have hpos : 0 < (b.expand c).count x := by omega
    have h1 : (b.mass c x == 0) = false :=
      beq_ff (fun he => hz (by rw [h.mass_eq]; exact he))
    have h2 : (b.expand c).contains x = true := by
      rw [contains_decide, decide_eq_true]
      exact count_pos_iff.mp hpos
    rw [h1, h2]
    rfl

theorem wf_distinctOutput (b : Bag α) (h : WellFormed b) : distinctOutput b :=
  fun p => live_perm b h p

theorem wf_filteredMoment (b : Bag α) (h : WellFormed b) : filteredMoment b := by
  intro p lo hi
  rw [weighted_eq b h p]
  exact sum_map_ite (fun x => lo ≤ x && x < hi) (fun x => x * x) (b.expand p)

theorem wf_orderStatistic (b : Bag α) (h : WellFormed b) : orderStatistic b := by
  intro p i
  unfold quantile
  rw [show (fun x => decide (i < below b p x))
      = (fun x => decide (i < ((b.expand p).filter (fun y => decide (y ≤ x))).length)) from
    funext (fun x => congrArg (fun v => decide (i < v)) (below_eq b h p x))]
  refine quantile_scan ((live b p).mergeSort) ?_ ?_ (b.expand p) ?_ i
  · exact pairwise_mergeSort (le := fun a b => decide (a ≤ b))
      (fun x y z hxy hyz =>
        decide_eq_true (Nat.le_trans (of_decide_eq_true hxy) (of_decide_eq_true hyz)))
      le_total_bool (live b p)
  · exact Perm.nodup (mergeSort_perm (live b p) (fun a b => decide (a ≤ b))).symm
      (live_nodup b p)
  · intro x
    rw [mem_mergeSort,
      (show live b p = (b.keys p).eraseDups.filter (fun k => 0 < b.mass p k) from rfl),
      mem_filter, mem_eraseDups, ← h.mass_eq p x]
    refine ⟨fun hx => ⟨h.cover _ _ hx, decide_eq_true (count_pos_iff.mpr hx)⟩,
      fun ⟨_, hx⟩ => count_pos_iff.mp (of_decide_eq_true hx)⟩

theorem wf_joinFanout (b : Bag α) (h : WellFormed b) : joinFanout b := by
  intro a c slack
  have key : ∀ (x : Nat),
      weighted b c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
        (((b.expand c).filter (fun y => x ≤ y && y ≤ x + slack)).map (fun z => (x, z))).length := by
    intro x
    rw [weighted_eq b h c, countP_map_toNat (fun y => decide (x ≤ y ∧ y ≤ x + slack)),
      length_map, congrArg List.length
        (filter_congr_helper (p := fun y => decide (x ≤ y ∧ y ≤ x + slack))
          (q := fun y => x ≤ y && y ≤ x + slack) (b.expand c)
          (by intro y _; rw [Bool.decide_and]))]
  rw [weighted_eq b h a,
    sum_map_congr_of_mem (b.expand a)
      (fun x => weighted b c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat))
      (fun x => (((b.expand c).filter (fun y => x ≤ y && y ≤ x + slack)).map (fun z => (x, z))).length)
      (by intro x _; exact key x)]
  refine (sum_length_map (b.expand a)
    (fun x => ((b.expand c).filter (fun y => x ≤ y && y ≤ x + slack)).map (fun z => (x, z)))).trans ?_
  simp only [pairs]

theorem wf_antiRevenue (b : Bag α) (h : WellFormed b) : antiRevenue b := by
  intro a c
  rw [weighted_eq b h a, sum_map_ite (fun x => b.mass c x == 0) (fun x => x) (b.expand a),
    map_idem]
  refine congrArg List.sum
    (filter_congr_helper (p := fun x => b.mass c x == 0)
      (q := fun x => !(b.expand c).contains x) (b.expand a)
      (by intro x _; exact mass_beq_zero_not_contains b h c x))

theorem wf_selfPairs (b : Bag α) (h : WellFormed b) : selfPairs b := by
  intro p
  have h1 : ((live b p).map (fun k => comb (b.mass p k))).sum
      = (((b.expand p).eraseDups).map (fun k => comb ((b.expand p).count k))).sum := by
    refine (sum_map_congr_of_mem (live b p) (fun k => comb (b.mass p k))
        (fun k => comb ((b.expand p).count k)) (by intro x _; rw [← h.mass_eq p x])).trans
      (sum_map_perm (fun k => comb ((b.expand p).count k)) (live_perm b h p))
  rw [h1]
  exact selfPairs_list (b.expand p)

theorem wf_intersectAll (b : Bag α) (h : WellFormed b) : intersectAll b := by
  intro a c k
  have key : ∀ x ∈ live b a,
      min (b.mass a x) (b.mass c x) =
        min ((b.expand a).count x) ((b.expand c).count x) := by
    intro x _
    rw [← h.mass_eq a x, ← h.mass_eq c x]
  have hf : (live b a).flatMap (fun x => List.replicate (min (b.mass a x) (b.mass c x)) x)
      = (live b a).flatMap (fun x => List.replicate (min ((b.expand a).count x) ((b.expand c).count x)) x) :=
    flatMap_congr_of_mem (live b a) _ _
      (by intro x hx; exact congrArg (fun m => List.replicate m x) (key x hx))
  rw [hf]
  rw [count_flatReplicate (fun x => min ((b.expand a).count x) ((b.expand c).count x))
    (live b a) (live_nodup b a) k]
  by_cases hk : k ∈ live b a
  · rw [if_pos hk]
  · rw [if_neg hk]
    have h0 : ¬ (0 < (b.expand a).count k) := by
      intro hpos
      exact hk (mem_filter.mpr ⟨mem_eraseDups.mpr (h.cover _ _ (count_pos_iff.mp hpos)),
        decide_eq_true (by rw [← h.mass_eq a k]; exact hpos)⟩)
    have hc0 : (b.expand a).count k = 0 := by omega
    rw [hc0]
    simp

theorem wf_outerWeight (b : Bag α) (h : WellFormed b) : outerWeight b := by
  intro a c
  have key : ∀ x, (b.mass c x == 0) = !(b.expand c).contains x :=
    mass_beq_zero_not_contains b h c
  have h1 : ((b.expand a).map (fun x => b.mass c x)).sum
      = ((b.expand a).map (fun x => (b.expand c).count x)).sum :=
    sum_map_congr_of_mem (b.expand a) (fun x => b.mass c x) (fun x => (b.expand c).count x)
      (by intro x _; rw [← h.mass_eq c x])
  have h2 : ((b.expand a).map (fun x => (b.expand c).count x)).sum
      = (pairs (b.expand a) (b.expand c) (· == ·)).length := by
    have key2 : (fun x => (b.expand c).count x)
        = (fun x => (((b.expand c).filter (fun y => x == y)).map (fun z => (x, z))).length) := by
      funext x
      rw [length_map, count_eq_length_filter (b.expand c) x]
    rw [key2]
    refine (sum_length_map (b.expand a)
      (fun x => ((b.expand c).filter (fun y => x == y)).map (fun z => (x, z)))).trans ?_
    simp only [pairs]
  have h3 : ((b.expand a).map (fun x => (decide (b.mass c x == 0)).toNat)).sum
      = ((b.expand a).filter (fun x => !(b.expand c).contains x)).length := by
    rw [countP_map_toNat (fun x => decide (b.mass c x == 0))]
    refine congrArg List.length
      (filter_congr_helper (p := fun x => decide (b.mass c x == 0))
        (q := fun x => !(b.expand c).contains x) (b.expand a)
        (by intro x _; rw [decide_bool, key x]))
  rw [weighted_eq b h a,
    sum_map_congr_of_mem (b.expand a) (fun x => max 1 (b.mass c x))
      (fun x => b.mass c x + (decide (b.mass c x == 0)).toNat)
      (by intro x _; exact max1_eq _),
    sum_add_map, h1, h2, h3]

end TinylambOpt.ResearchProofs.StorageExecution.BagKernels
