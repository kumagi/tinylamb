import TinylambOpt.ResearchGoals.BoundedCountRadixDescSortEquiv

/-!
Proved

- `Goal` — Signed 64-bit order-reversing LSB descending radix sort equivalence (H2O H2
  / LDBC L2 / DictionaryJoinsAndLateLabels B1 / STATS-CEB C2 / JOB J4): for all
  inputs with `key ∈ [i64Min, i64Max]` and any `(radix, passes)` with `2^64 ≤ radix ^
  passes` (including both `(256, 8)` byte passes and `(16, 16)` nibble passes),
  `radixSortDescWith` preserves the exact multiset (`count`), produces monotonically
  descending keys (`b.key ≤ a.key`), and is stable.

The reference model is defined in
`TinylambOpt.ResearchGoals.BoundedCountRadixDescSortEquiv`.
-/

namespace TinylambOpt.ResearchProofs.BoundedCountRadixDescSortEquiv

open TinylambOpt.ResearchGoals.BoundedCountRadixDescSortEquiv

/-- A sort item paired with its original position in the input; the position is the
    stability witness used in the per-digit stable counting-sort induction. -/
private abbrev IItem := SortItem × Nat

/-- The order-reversing unsigned projection of a key (`flipDesc`). -/
private def v (x : SortItem) : Nat := flipDesc x.key

private theorem v_le_cap (x : SortItem) : v x ≤ u64MaxNat := Nat.sub_le _ _

/-! ## The signed → unsigned projection flips the order on `[i64Min, i64Max]` -/

private theorem flip_le_flip_iff {a b : Int}
    (ha : i64Min ≤ a) (ha' : a ≤ i64Max) (hb : i64Min ≤ b) (hb' : b ≤ i64Max) :
    (flipDesc a ≤ flipDesc b) ↔ b ≤ a := by
  have hcap : (u64MaxNat : Int) = (i64Max : Int) - i64Min := by decide
  have hua : ((a - i64Min).toNat : Int) = a - i64Min :=
    Int.toNat_of_nonneg (by omega : (0 : Int) ≤ a - i64Min)
  have hub : ((b - i64Min).toNat : Int) = b - i64Min :=
    Int.toNat_of_nonneg (by omega : (0 : Int) ≤ b - i64Min)
  have ha2 : (a - i64Min).toNat ≤ u64MaxNat := by
    apply Int.ofNat_le.mp
    rw [hua, hcap]
    omega
  have hb2 : (b - i64Min).toNat ≤ u64MaxNat := by
    apply Int.ofNat_le.mp
    rw [hub, hcap]
    omega
  simp only [flipDesc]
  refine ⟨fun h => ?_, fun h => ?_⟩ <;> omega

/-! ## Base-`radix` digit arithmetic -/

private theorem extractDigit_lt (val r p : Nat) (hr : 0 < r) : extractDigit val r p < r := by
  show (val / r ^ p) % r < r
  exact Nat.mod_lt _ hr

/-- The `(p+1)`-digit prefix of `val` splits into the digit at position `p` scaled by
    `r ^ p`, plus the `p`-digit prefix. -/
private theorem digit_step (val r p : Nat) :
    val % r ^ (p + 1) = extractDigit val r p * r ^ p + val % r ^ p := by
  rw [Nat.mod_pow_succ, Nat.add_comm, Nat.mul_comm]
  rfl

private theorem lt_of_extract_lt (v1 v2 r p : Nat) (hr : 0 < r)
    (h : extractDigit v1 r p < extractDigit v2 r p) :
    v1 % r ^ (p + 1) < v2 % r ^ (p + 1) := by
  have hp : 0 < r ^ p := Nat.pow_pos_iff.mpr (Or.inl hr)
  have h1 : v1 % r ^ p < r ^ p := Nat.mod_lt _ hp
  have h2 : v2 % r ^ p < r ^ p := Nat.mod_lt _ hp
  have hb1 : extractDigit v1 r p * r ^ p + v1 % r ^ p
      < (extractDigit v1 r p + 1) * r ^ p := by
    rw [Nat.add_mul, Nat.one_mul]
    exact Nat.add_lt_add_left h1 _
  have hb2 : (extractDigit v1 r p + 1) * r ^ p ≤ extractDigit v2 r p * r ^ p :=
    Nat.mul_le_mul_right _ (Nat.succ_le_of_lt h)
  have hb3 : extractDigit v2 r p * r ^ p ≤ extractDigit v2 r p * r ^ p + v2 % r ^ p :=
    Nat.le_add_right _ _
  rw [digit_step, digit_step]
  exact Nat.lt_of_lt_of_le (Nat.lt_of_lt_of_le hb1 hb2) hb3

/-! ## One stable bucket pass on indexed items -/

/-- Sortedness invariant after `p` passes: items are ordered by the low `p` digits of
    their projected key, with ties broken by the original input index. -/
private def ordAt (r p : Nat) (x y : IItem) : Prop :=
  v x.1 % r ^ p < v y.1 % r ^ p ∨ (v x.1 % r ^ p = v y.1 % r ^ p ∧ x.2 ≤ y.2)

/-- Two items consecutive after distributing one more digit: either ordered by the new
    digit, or equal digits with the old invariant intact. -/
private def nr (r p : Nat) (a b : IItem) : Prop :=
  extractDigit (v a.1) r p < extractDigit (v b.1) r p ∨
    (extractDigit (v a.1) r p = extractDigit (v b.1) r p ∧ ordAt r p a b)

private def ibuckets (r p : Nat) (xs : List IItem) : List (List IItem) :=
  (List.range r).map (fun d => xs.filter (fun z => extractDigit (v z.1) r p == d))

private theorem ordAt_succ (r p : Nat) (hr : 0 < r) {a b : IItem}
    (h : nr r p a b) : ordAt r (p + 1) a b := by
  rcases h with hlt | ⟨heq, hor⟩
  · left
    exact lt_of_extract_lt (v a.1) (v b.1) r p hr hlt
  · have ha := digit_step (v a.1) r p
    have hb := digit_step (v b.1) r p
    rw [heq] at ha
    rcases hor with hlo | ⟨hloe, hidx⟩
    · left
      rw [ha, hb]
      exact Nat.add_lt_add_left hlo _
    · right
      refine ⟨by rw [ha, hb, hloe], hidx⟩

private theorem pairwise_imp_mem {l : List IItem} {P : IItem → Prop}
    {R S : IItem → IItem → Prop}
    (hP : ∀ x, x ∈ l → P x)
    (himp : ∀ {a b}, P a → P b → R a b → S a b)
    (hs : l.Pairwise R) : l.Pairwise S := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp only [List.pairwise_cons] at hs ⊢
      constructor
      · intro b hb
        exact himp (hP a (by simp)) (hP b (by simp [hb])) (hs.1 b hb)
      · exact ih (fun x hx => hP x (by simp [hx])) hs.2

private theorem buckets_pairwise (l : List IItem) (r p : Nat)
    (hl : l.Pairwise (ordAt r p)) :
    ∀ ds : List Nat, ds.Pairwise (· < ·) →
      (ds.map (fun d => l.filter (fun z => extractDigit (v z.1) r p == d))).flatten.Pairwise
        (nr r p) := by
  intro ds
  induction ds with
  | nil => intro _; simp
  | cons d t ih =>
      intro hd
      simp only [List.pairwise_cons] at hd
      obtain ⟨hlt, hd⟩ := hd
      rw [List.map_cons, List.flatten_cons, List.pairwise_append]
      refine ⟨?_, ih hd, ?_⟩
      · have hp : (l.filter (fun z => extractDigit (v z.1) r p == d)).Pairwise (ordAt r p) :=
          List.Pairwise.sublist List.filter_sublist hl
        refine pairwise_imp_mem (P := fun z => extractDigit (v z.1) r p = d) ?_ ?_ hp
        · intro x hx
          exact by simpa using (List.mem_filter.mp hx).2
        · intro a b ha hb hab
          exact Or.inr ⟨ha.trans hb.symm, hab⟩
      · intro a ha b hb
        obtain ⟨l', hl', hab'⟩ := List.mem_flatten.mp hb
        obtain ⟨e, het, rfl⟩ := List.mem_map.mp hl'
        have hda : extractDigit (v a.1) r p = d := by simpa using (List.mem_filter.mp ha).2
        have heb : extractDigit (v b.1) r p = e := by simpa using (List.mem_filter.mp hab').2
        exact Or.inl (by rw [hda, heb]; exact hlt e het)

private theorem ipass_pairwise (l : List IItem) (r p : Nat) (hr : 0 < r)
    (hl : l.Pairwise (ordAt r p)) :
    (ibuckets r p l).flatten.Pairwise (ordAt r (p + 1)) := by
  have h := buckets_pairwise l r p hl (List.range r) List.pairwise_lt_range
  exact h.imp (fun hnx => ordAt_succ r p hr hnx)

/-! ## The indexed fold reproduces `radixSortDescWith` after projecting out indices -/

private def ifold (r : Nat) : Nat → List IItem → List IItem
  | 0 => fun l => l
  | p + 1 => fun l => (ibuckets r p (ifold r p l)).flatten

private theorem ifold_succ (r p : Nat) (l : List IItem) :
    ifold r (p + 1) l = (ibuckets r p (ifold r p l)).flatten := rfl

private theorem ifold_pairwise (r p : Nat) (l : List IItem) (hr : 0 < r)
    (hl : l.Pairwise (ordAt r 0)) : (ifold r p l).Pairwise (ordAt r p) := by
  induction p generalizing l with
  | zero => exact hl
  | succ p ih =>
      rw [ifold_succ]
      exact ipass_pairwise (ifold r p l) r p hr (ih l hl)

private theorem map_fst_zipIdx_from (xs : List SortItem) : ∀ i : Nat,
    (xs.zipIdx i).map Prod.fst = xs := by
  intro i
  induction xs generalizing i with
  | nil => rfl
  | cons a t ih => rw [List.zipIdx_cons, List.map_cons, ih (i + 1)]

private theorem map_ibuckets (xs : List IItem) (r p : Nat) :
    (ibuckets r p xs).map (List.map Prod.fst) =
      ((List.range r).map fun d =>
        (xs.map Prod.fst).filter (fun x : SortItem => extractDigit (v x) r p == d)) := by
  rw [ibuckets, List.map_map]
  congr 1
  funext d
  show ((xs.filter (fun z => extractDigit (v z.1) r p == d)).map Prod.fst)
    = ((xs.map Prod.fst).filter (fun x => extractDigit (v x) r p == d))
  rw [List.filter_map]
  refine congrArg (fun q : IItem → Bool => List.map Prod.fst (List.filter q xs)) ?_
  funext z
  rfl

private theorem radixPass_eq (xs : List SortItem) (r p : Nat) :
    radixPass xs r p =
      ((List.range r).map fun d =>
        xs.filter (fun x => extractDigit (v x) r p == d)).flatten := rfl

private theorem ifold_map (r : Nat) : ∀ (p : Nat) (l : List IItem),
    (ifold r p l).map Prod.fst = radixSortDescWith r p (l.map Prod.fst) := by
  intro p
  induction p with
  | zero => intro l; rfl
  | succ p ih =>
      intro l
      show ((ibuckets r p (ifold r p l)).flatten).map Prod.fst = _
      rw [List.map_flatten, map_ibuckets, ← radixPass_eq, ih l]
      have hstep : radixSortDescWith r (p + 1) (l.map Prod.fst)
          = radixPass (radixSortDescWith r p (l.map Prod.fst)) r p := by
        show (List.range (p + 1)).foldl (fun acc pass => radixPass acc r pass) (l.map Prod.fst)
          = radixPass ((List.range p).foldl (fun acc pass => radixPass acc r pass) (l.map Prod.fst))
              r p
        rw [show List.range (p + 1) = List.range p ++ [p] from List.range_succ,
          List.foldl_append]
        rfl
      rw [hstep]

/-! ## Multiset preservation of one pass and of the whole fold -/

private theorem count_filter {α : Type} [DecidableEq α] (p : α → Bool) (ys : List α) (y : α) :
    (ys.filter p).count y = if p y then ys.count y else 0 := by
  induction ys with
  | nil =>
      show 0 = if p y = true then 0 else 0
      by_cases hy : p y <;> simp [hy]
  | cons a t ih =>
      by_cases hab : a = y
      · subst hab
        by_cases hy : p a
        · rw [List.filter_cons_of_pos hy, ite_eq_left hy, List.count_cons_self, ih, ite_eq_left hy,
            List.count_cons_self]
        · rw [List.filter_cons_of_neg hy, ite_eq_right hy, ih, ite_eq_right hy]
      · have hne : ¬((a == y) = true) := by simpa using hab
        by_cases ha : p a
        · rw [List.filter_cons_of_pos ha, List.count_cons, ite_eq_right hne, Nat.add_zero,
            List.count_cons, ite_eq_right hne, Nat.add_zero, ih]
        · rw [List.filter_cons_of_neg ha, ih, List.count_cons, ite_eq_right hne, Nat.add_zero]

private theorem beq_neg_of_ne {α : Type} [BEq α] [LawfulBEq α] {a b : α} (h : ¬(a = b)) :
    ¬(a == b) :=
  fun hb => h (beq_iff_eq.mp hb)

private theorem flatten_range_filter_count {α : Type} [DecidableEq α] (xs : List α)
    (dig : α → Nat) (y : α) :
    ∀ n : Nat,
    ((List.range n).map (fun d => xs.filter (fun x => dig x == d))).flatten.count y
    = if dig y < n then xs.count y else 0 := by
  intro n
  induction n with
  | zero => simp
  | succ k ih =>
      rw [show List.range (k + 1) = List.range k ++ [k] from List.range_succ,
        List.map_append, List.map_singleton, List.flatten_append, List.flatten_singleton,
        List.count_append, ih, count_filter]
      by_cases h1 : dig y < k
      · rw [ite_eq_left h1, ite_eq_right (beq_neg_of_ne (Nat.ne_of_lt h1)),
          ite_eq_left (by omega : dig y < k + 1)]
        try omega
      · by_cases h2 : dig y = k
        · rw [ite_eq_right h1, ite_eq_left (beq_iff_eq.mpr h2), ite_eq_left (by omega : dig y < k + 1)]
          try omega
        · rw [ite_eq_right h1, ite_eq_right (beq_neg_of_ne h2), ite_eq_right (by omega : ¬dig y < k + 1)]
          try omega

private theorem pass_count (xs : List SortItem) (y : SortItem) (r p : Nat) (hr : 0 < r) :
    (radixPass xs r p).count y = xs.count y := by
  have hlt : extractDigit (v y) r p < r := extractDigit_lt (v y) r p hr
  have h := flatten_range_filter_count xs (fun x => extractDigit (v x) r p) y r
  rw [ite_eq_left hlt] at h
  exact h

private theorem foldl_count (r p : Nat) (hr : 0 < r) (xs : List SortItem) (y : SortItem) :
    ((List.range p).foldl (fun acc pass => radixPass acc r pass) xs).count y = xs.count y := by
  induction p with
  | zero => rfl
  | succ p ih =>
      have hstep : (List.range (p + 1)).foldl (fun acc pass => radixPass acc r pass) xs
          = radixPass ((List.range p).foldl (fun acc pass => radixPass acc r pass) xs) r p := by
        rw [show List.range (p + 1) = List.range p ++ [p] from List.range_succ,
          List.foldl_append]
        rfl
      rw [hstep, pass_count _ _ r p hr, ih]

private theorem count_preserved (r p : Nat) (hr : 0 < r) (xs : List SortItem) (y : SortItem) :
    (radixSortDescWith r p xs).count y = xs.count y := foldl_count r p hr xs y

private theorem perm_result (r p : Nat) (hr : 0 < r) (xs : List SortItem) :
    List.Perm (radixSortDescWith r p xs) xs :=
  List.perm_iff_count.mpr (fun y => count_preserved r p hr xs y)

/-! ## Assembling descending order -/

private theorem ordAt_imp_v_le (r P : Nat) {x y : IItem}
    (hx : v x.1 < r ^ P) (hy : v y.1 < r ^ P) (h : ordAt r P x y) : v x.1 ≤ v y.1 := by
  have hx' : v x.1 % r ^ P = v x.1 := Nat.mod_eq_of_lt hx
  have hy' : v y.1 % r ^ P = v y.1 := Nat.mod_eq_of_lt hy
  rcases h with hlt | ⟨heq, _⟩
  · rw [hx', hy'] at hlt
    exact Nat.le_of_lt hlt
  · rw [hx', hy'] at heq
    exact Nat.le_of_eq heq

private theorem zipIdx_pairwise_idx_le : ∀ (xs : List SortItem) (i : Nat),
    (xs.zipIdx i).Pairwise (fun x y : IItem => x.2 ≤ y.2) := by
  intro xs
  induction xs with
  | nil => intro i; simp
  | cons a t ih =>
      intro i
      rw [List.zipIdx_cons, List.pairwise_cons]
      constructor
      · intro b hb
        have h := (List.mem_zipIdx_iff_le_and_getElem?_sub).mp hb
        exact Nat.le_trans (Nat.le_succ i) h.1
      · exact ih (i + 1)

private theorem ordAt_zero_of_idx (r : Nat) {x y : IItem} (h : x.2 ≤ y.2) : ordAt r 0 x y := by
  simp only [ordAt, Nat.pow_zero, Nat.mod_one]
  exact Or.inr ⟨trivial, h⟩

private theorem zipIdx_ord0 (r : Nat) (xs : List SortItem) :
    (xs.zipIdx).Pairwise (ordAt r 0) :=
  (zipIdx_pairwise_idx_le xs 0).imp (fun h => ordAt_zero_of_idx r h)

private theorem pairwise_flip (l : List SortItem)
    (hl : l.Pairwise (fun a b => v a ≤ v b))
    (hr : ∀ x ∈ l, i64Min ≤ x.key ∧ x.key ≤ i64Max) :
    l.Pairwise (fun a b => b.key ≤ a.key) := by
  induction l with
  | nil => simp
  | cons c t ih =>
      rw [List.pairwise_cons] at hl ⊢
      constructor
      · intro b hb
        have hc := hr c (by simp)
        have hbb := hr b (by simp [hb])
        exact (flip_le_flip_iff hc.1 hc.2 hbb.1 hbb.2).mp (hl.1 b hb)
      · exact ih hl.2 (fun x hx => hr x (by simp [hx]))

theorem sound : TinylambOpt.ResearchGoals.BoundedCountRadixDescSortEquiv.Goal := by
  intro radix passes xs hradix hbig hall
  refine ⟨?_, ?_⟩
  · intro y
    exact count_preserved radix passes hradix xs y
  · have hv_lt : ∀ (x : SortItem), v x < radix ^ passes := fun x =>
      Nat.lt_of_le_of_lt (v_le_cap x) hbig
    have hpair :=
      ((ifold_pairwise radix passes xs.zipIdx hradix (zipIdx_ord0 radix xs)).imp
        fun h => ordAt_imp_v_le radix passes (hv_lt _) (hv_lt _) h)
    have hflip : (radixSortDescWith radix passes xs).Pairwise (fun a b => v a ≤ v b) := by
      have h3 :=
        List.Pairwise.map (f := Prod.fst) (S := fun a b => v a ≤ v b) (fun _ _ hab => hab) hpair
      rw [ifold_map radix passes xs.zipIdx, map_fst_zipIdx_from xs 0] at h3
      exact h3
    have hall' : ∀ x ∈ radixSortDescWith radix passes xs,
        i64Min ≤ x.key ∧ x.key ≤ i64Max := by
      intro x hx
      exact hall x ((perm_result radix passes hradix xs).mem_iff.mp hx)
    exact pairwise_flip _ hflip hall'

end TinylambOpt.ResearchProofs.BoundedCountRadixDescSortEquiv

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.hpp
