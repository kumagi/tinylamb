import TinylambOpt.ResearchGoals.BoundedHeapTopNMerge

/-!
Proved

- `Goal` — Two-stage worker-local Top-(offset + limit) merge (`executor/topn.cpp`,
  `executor/detail/relational_sort_limit.cpp`): pruning each worker to `offset +
  limit` rows and taking `(sorted.drop offset).take limit` on the merged union equals
  full global sorting across arbitrary chunk splits.

The reference model is defined in `TinylambOpt.ResearchGoals.BoundedHeapTopNMerge`.
-/

-- # Soundness of bounded-heap Top-N merge
--
-- `refTopN off lim chunks` sorts the flattened chunks once and cuts the
-- window `[off, off + lim)` out of it. `parTopN off lim chunks` instead
-- lets every worker sort its own chunk and keep only its own prefix of
-- length `off + lim` (the bounded-heap bound), flattens the truncated
-- local results, and re-sorts. A row outside a worker's own `off + lim`
-- prefix cannot appear inside the global window `[off, off + lim)`, so the
-- truncation is invisible after the window cut.
--
-- The proof runs through the total order on rows (transported from the
-- Boolean `rowLt`), sorted-insertion bookkeeping for `sortRows`, and one
-- truncation-substitutivity principle for sorted prefixes
-- (`take_sortRows_merge_trunc`), applied chunk by chunk under `flatten`.

open List

namespace TinylambOpt.ResearchProofs.BoundedHeapTopNMerge

open TinylambOpt.ResearchGoals.BoundedHeapTopNMerge

/-- `rowLe` is the order transported from the Boolean comparator:
`rowLe a b` holds exactly when `rowLt b a` is `false`. -/
private def rowLe (a b : Row) : Prop := ¬ rowLt b a = true

/-- Exactly when the comparator answers `true`. -/
private theorem rowLt_char {a b : Row} :
    rowLt a b = true ↔ a.1 < b.1 ∨ a.1 = b.1 ∧ a.2 < b.2 := by
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  simp [rowLt, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq]

private theorem rowLt_asym {a b : Row} (h1 : rowLt a b = true) (h2 : rowLt b a = true) : False := by
  rcases rowLt_char.mp h1 with h | ⟨e, h⟩
  · rcases rowLt_char.mp h2 with h' | ⟨e', h'⟩
    · omega
    · omega
  · rcases rowLt_char.mp h2 with h' | ⟨e', h'⟩
    · omega
    · omega

private theorem rowLe_total (a b : Row) : rowLe a b ∨ rowLe b a := by
  by_cases h : rowLt b a = true
  · exact Or.inr (fun hab => rowLt_asym hab h)
  · exact Or.inl h

private theorem lt_of_not_rowLe {a b : Row} (h : ¬ rowLe a b) : rowLt b a = true :=
  Classical.byContradiction (fun hn => h hn)

private theorem rowLe_antisymm {a b : Row} (h1 : rowLe a b) (h2 : rowLe b a) : a = b := by
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  simp only [rowLe] at h1 h2
  rw [rowLt_char] at h1 h2
  have h1a : ¬ b1 < a1 := fun h => h1 (Or.inl h)
  have h2a : ¬ a1 < b1 := fun h => h2 (Or.inl h)
  have heq : a1 = b1 := by omega
  have h1b : ¬ b2 < a2 := fun h => h1 (Or.inr ⟨heq.symm, h⟩)
  have h2b : ¬ a2 < b2 := fun h => h2 (Or.inr ⟨heq, h⟩)
  exact Prod.ext heq (by omega)

private theorem rowLe_refl (a : Row) : rowLe a a := by
  intro heq
  rcases rowLt_char (a := a) (b := a) |>.mp heq with hlt | ⟨_, hlt⟩
  · exact absurd hlt (Int.lt_irrefl _)
  · exact absurd hlt (Nat.lt_irrefl _)

private theorem rowLe_trans {a b c : Row} (h1 : rowLe a b) (h2 : rowLe b c) : rowLe a c := by
  rw [rowLe] at h1 h2 ⊢
  intro hca
  obtain step : ∀ (x : Row), rowLt c x = true ∨ rowLt x a = true := by
    rcases rowLt_char (a := c) (b := a) |>.mp hca with hlt | ⟨heq, hlt⟩
    · intro x
      rcases Int.lt_trichotomy c.1 x.1 with hxx | hxx | hxx
      · exact Or.inl (rowLt_char.mpr (Or.inl hxx))
      · exact Or.inr (rowLt_char.mpr (Or.inl (by omega : x.1 < a.1)))
      · exact Or.inr (rowLt_char.mpr (Or.inl (by omega : x.1 < a.1)))
    · intro x
      rcases Int.lt_trichotomy c.1 x.1 with hxx | hxx | hxx
      · exact Or.inl (rowLt_char.mpr (Or.inl hxx))
      · rcases Nat.lt_trichotomy c.2 x.2 with hxy | hxy | hxy
        · exact Or.inl (rowLt_char.mpr (Or.inr ⟨hxx, hxy⟩))
        · exact Or.inr (rowLt_char.mpr (Or.inr
          ⟨(by omega : x.1 = a.1), (by omega : x.2 < a.2)⟩))
        · exact Or.inr (rowLt_char.mpr (Or.inr
          ⟨(by omega : x.1 = a.1), (by omega : x.2 < a.2)⟩))
      · exact Or.inr (rowLt_char.mpr (Or.inl (by omega : x.1 < a.1)))
  rcases step b with hx | hx
  · exact absurd hx h2
  · exact absurd hx h1

private theorem mem_insertRow {r y : Row} : ∀ l : List Row, y ∈ insertRow r l ↔ y = r ∨ y ∈ l := by
  intro l
  induction l with
  | nil => simp [insertRow]
  | cons x xs ih =>
      by_cases h : rowLt r x = true
      · simp [insertRow, h]
      · simp [insertRow, h, ih, or_left_comm]

private theorem insertRow_cons_pos (r x : Row) (l : List Row) (h : rowLt r x = true) :
    insertRow r (x :: l) = r :: x :: l := by
  simp [insertRow, h]

private theorem insertRow_cons_neg (r x : Row) (l : List Row) (h : ¬ rowLt r x = true) :
    insertRow r (x :: l) = x :: insertRow r l := by
  simp [insertRow, h]

private theorem insertRow_perm (r : Row) : ∀ l : List Row, insertRow r l ~ r :: l := by
  intro l
  induction l with
  | nil => exact Perm.refl _
  | cons x xs ih =>
      by_cases h : rowLt r x = true
      · rw [insertRow_cons_pos _ _ _ h]
      · rw [insertRow_cons_neg _ _ _ h]
        exact (Perm.cons x ih).trans (Perm.swap x r xs).symm

private theorem insertRow_pairwise (r : Row) :
    ∀ l : List Row, l.Pairwise rowLe → (insertRow r l).Pairwise rowLe := by
  intro l
  induction l with
  | nil => intro _; simp [insertRow]
  | cons x xs ih =>
      intro h
      obtain ⟨hx, hxs⟩ := pairwise_cons.mp h
      by_cases hc : rowLt r x = true
      · rw [insertRow_cons_pos _ _ _ hc]
        refine pairwise_cons.mpr ⟨fun y hy => ?_, pairwise_cons.mpr ⟨hx, hxs⟩⟩
        rcases mem_cons.mp hy with rfl | hy
        · exact fun hb => rowLt_asym hc hb
        · exact rowLe_trans (fun hb => rowLt_asym hc hb) (hx y hy)
      · rw [insertRow_cons_neg _ _ _ hc]
        refine pairwise_cons.mpr ⟨fun y hy => ?_, ih hxs⟩
        rw [mem_insertRow] at hy
        rcases hy with rfl | hy
        · exact hc
        · exact hx y hy

private theorem sortRows_perm : ∀ l : List Row, sortRows l ~ l := by
  intro l
  induction l with
  | nil => exact Perm.refl _
  | cons r rs ih =>
      calc sortRows (r :: rs) = insertRow r (sortRows rs) := rfl
        _ ~ r :: sortRows rs := insertRow_perm r _
        _ ~ r :: rs := Perm.cons r ih

private theorem sortRows_pairwise : ∀ l : List Row, (sortRows l).Pairwise rowLe := by
  intro l
  induction l with
  | nil => simp [sortRows]
  | cons r rs ih => exact insertRow_pairwise r _ ih

private theorem sortRows_eq_of_perm_sorted {l₁ l₂ : List Row}
    (h : l₁ ~ l₂) (h₁ : l₁.Pairwise rowLe) (h₂ : l₂.Pairwise rowLe) : l₁ = l₂ :=
  Perm.eq_of_pairwise (fun _ _ _ _ hab hba => rowLe_antisymm hab hba) h₁ h₂ h

private theorem sortRows_idempotent (l : List Row) : sortRows (sortRows l) = sortRows l :=
  sortRows_eq_of_perm_sorted (sortRows_perm (sortRows l))
    (sortRows_pairwise _) (sortRows_pairwise _)

private theorem sortRows_cong {l₁ l₂ : List Row} (h : l₁ ~ l₂) : sortRows l₁ = sortRows l₂ :=
  sortRows_eq_of_perm_sorted ((sortRows_perm l₁).trans (h.trans (sortRows_perm l₂).symm))
    (sortRows_pairwise _) (sortRows_pairwise _)

private theorem pairwise_take (l : List Row) (h : l.Pairwise rowLe) (i : Nat) :
    (l.take i).Pairwise rowLe :=
  Pairwise.sublist (take_sublist i l) h

private theorem take_app_self (Y : List Row) : ∀ C : List Row, (C ++ Y).take C.length = C := by
  intro C
  induction C with
  | nil => rfl
  | cons x xs ih => rw [cons_append, length_cons, take_succ_cons, ih]

private theorem take_drop_bound : ∀ (l : List Row) (i : Nat), l.Pairwise rowLe →
    ∀ c ∈ l.take i, ∀ d ∈ l.drop i, rowLe c d := by
  intro l
  induction l with
  | nil => intro i _ c hc; rw [take_nil] at hc; exact False.elim (not_mem_nil hc)
  | cons x xs ih =>
      intro i h
      cases i with
      | zero => intro c hc; rw [take_zero] at hc; exact False.elim (not_mem_nil hc)
      | succ m =>
          intro c hc d hd
          rw [take_succ_cons] at hc
          rw [drop_succ_cons] at hd
          rcases mem_cons.mp hc with rfl | hc
          · exact rel_of_pairwise_cons h (Sublist.mem hd (drop_sublist m xs))
          · exact ih m (pairwise_cons.mp h).2 c hc d hd

private theorem take_cons_take (x : Row) (xs : List Row) (m : Nat) :
    (x :: xs).take m = (x :: xs.take m).take m := by
  cases m with
  | zero => rfl
  | succ j =>
      rw [take_succ_cons, take_succ_cons, take_take, Nat.min_eq_left (Nat.le_succ j)]

/-- Truncating after inserting equals inserting into the truncation. -/
private theorem take_insertRow (r : Row) : ∀ (l : List Row) (k : Nat),
    (insertRow r l).take k = (insertRow r (l.take k)).take k := by
  intro l
  induction l with
  | nil => intro k; rw [take_nil]
  | cons x xs ih =>
      intro k
      cases k with
      | zero => rfl
      | succ m =>
          by_cases h : rowLt r x = true
          · rw [insertRow_cons_pos _ _ _ h,
              show ((x :: xs).take (m + 1)) = x :: xs.take m from take_succ_cons,
              insertRow_cons_pos _ _ _ h, take_succ_cons, take_succ_cons]
            exact congrArg (fun t => r :: t) (take_cons_take x xs m)
          · rw [take_succ_cons, insertRow_cons_neg _ _ _ h, insertRow_cons_neg _ _ _ h,
            take_succ_cons, take_succ_cons, ih]

/--
`φ x a = (insertRow x a).take k` folds a left append segment; the fold
absorbs `sortRows` of the segment because each insertion commutes past
the outer truncation.
-/
private theorem take_sortRows_append_foldr (k : Nat) : ∀ K L : List Row,
    (sortRows (K ++ L)).take k =
      K.foldr (fun x a => (insertRow x a).take k) ((sortRows L).take k) := by
  intro K
  induction K with
  | nil => intro L; rfl
  | cons x K ih =>
      intro L
      show (insertRow x (sortRows (K ++ L))).take k =
        (x :: K).foldr (fun y a => (insertRow y a).take k) ((sortRows L).take k)
      rw [take_insertRow x (sortRows (K ++ L)) k, ih L]
      exact (foldr_cons (f := fun y a => (insertRow y a).take k) (a := x) (l := K)
        (b := (sortRows L).take k)).symm

private theorem exists_decomp_of_mem (b : Row) :
    ∀ {C : List Row}, b ∈ C → ∃ t r : List Row, C = t ++ b :: r := by
  intro C h
  induction C with
  | nil => exact False.elim (List.not_mem_nil h)
  | cons x xs ih =>
      rcases mem_cons.mp h with rfl | h
      · exact ⟨[], xs, rfl⟩
      · rcases ih h with ⟨t, r, heq⟩
        exact ⟨x :: t, r, by rw [heq, ← cons_append]⟩

private theorem rowLt_of_rowLe_of_lt {a p b : Row} (h1 : rowLe a p) (h2 : rowLt p b = true) :
    rowLt a b = true := by
  apply Classical.byContradiction
  intro hn
  exact absurd h2 (rowLe_trans hn h1)

private theorem ne_of_rowLt {a b : Row} (h : rowLt a b = true) : a ≠ b := by
  intro heq
  subst heq
  rcases rowLt_char (a := a) (b := a) |>.mp h with hlt | ⟨_, hlt⟩
  · exact absurd hlt (Int.lt_irrefl _)
  · exact absurd hlt (Nat.lt_irrefl _)

private theorem length_le_of_count_le :
    ∀ (P Q : List Row), (∀ v : Row, count v P ≤ count v Q) → P.length ≤ Q.length := by
  intro P
  induction P with
  | nil => intro Q _; exact Nat.zero_le _
  | cons a P ih =>
      intro Q h
      have h0 : 0 < count a (a :: P) := by
        rw [count_cons_self]
        omega
      have h1 := h a
      have haQ : a ∈ Q := count_pos_iff.mp (by omega : 0 < count a Q)
      rcases exists_decomp_of_mem a haQ with ⟨Q₁, Q₂, hQ⟩
      have hlen := ih (Q₁ ++ Q₂) (by
        intro v
        specialize h v
        rw [hQ, count_append, count_cons, count_cons] at h
        rw [count_append]
        omega)
      simp only [hQ, length_append, length_cons] at hlen ⊢
      omega

/-- Every row of the `k`-prefix of `sortRows (P₁ ++ rest ++ L)` is `≤ p`
whenever `P₁` has length `k` and is pointwise `≤ p`. -/
private theorem prefix_small : ∀ (P₁ rest L : List Row) (k : Nat) (p : Row),
    P₁.length = k → (∀ a ∈ P₁, rowLe a p) →
    ∀ b ∈ (sortRows (P₁ ++ rest ++ L)).take k, rowLe b p := by
  intro P₁ rest L k p hlen hle b hb hpb
  let S := sortRows (P₁ ++ rest ++ L)
  have hS : S = sortRows (P₁ ++ rest ++ L) := rfl
  have hbS : b ∈ S.take k := hb
  have hpair : S.Pairwise rowLe := sortRows_pairwise _
  rcases exists_decomp_of_mem b hbS with ⟨front, r, hC⟩
  have h1 : S = (front ++ b :: r) ++ S.drop k := by
    rw [← hC, take_append_drop]
  have hBdeq : S = front ++ (b :: r ++ S.drop k) := h1.trans (append_assoc _ _ _)
  rw [hBdeq] at hpair
  obtain ⟨_, htail, _⟩ := pairwise_append.mp hpair
  have hb_le : ∀ x ∈ r ++ S.drop k, rowLe b x := by
    intro x hx
    exact rel_of_pairwise_cons htail hx
  have hcount : ∀ v : Row, count v P₁ ≤ count v front := by
    intro v
    by_cases hv : rowLt v b = true
    · have hzero : count v (b :: r ++ S.drop k) = 0 := by
        apply count_eq_zero_of_not_mem
        intro hx
        rcases mem_cons.mp hx with rfl | hx'
        · exact absurd hv (rowLe_refl _)
        · rcases mem_append.mp hx' with hxr | hxd
          · exact absurd hv (hb_le v (mem_append.mpr (Or.inl hxr)))
          · exact absurd hv (hb_le v (mem_append.mpr (Or.inr hxd)))
      calc count v P₁
          _ ≤ count v (P₁ ++ rest) := by
              rw [count_append]
              exact Nat.le_add_right _ _
          _ ≤ count v (P₁ ++ rest) + count v L := Nat.le_add_right _ _
          _ = count v (P₁ ++ rest ++ L) := count_append.symm
          _ = count v S := ((perm_iff_count (α := Row)).mp (sortRows_perm (P₁ ++ rest ++ L)) v).symm
          _ = count v front := by
              rw [hBdeq, count_append, hzero, Nat.add_zero]
    · have hzero : count v P₁ = 0 :=
          count_eq_zero_of_not_mem
            (fun hmem => absurd (rowLt_of_rowLe_of_lt (hle v hmem) hpb) hv)
      rw [hzero]
      exact Nat.zero_le _
  have hcard1 : P₁.length ≤ front.length := length_le_of_count_le P₁ front hcount
  have hcard2 : front.length + 1 ≤ k := by
    calc front.length + 1
        _ ≤ (front ++ b :: r).length := by
              rw [length_append, length_cons]
              omega
        _ = (S.take k).length := by rw [← hC]
        _ = min k S.length := length_take
        _ ≤ k := Nat.min_le_left _ _
  have hlen2 : k ≤ front.length := by
    rw [← hlen]
    exact hcard1
  omega

private theorem ins_cut : ∀ (B : List Row) (p : Row) (k : Nat), B.Pairwise rowLe →
    k ≤ B.length → (∀ b ∈ B.take k, rowLe b p) → (insertRow p B).take k = B.take k := by
  intro B
  induction B with
  | nil => intro p k _ hlen _; have hk : k = 0 := Nat.eq_zero_of_le_zero hlen; subst hk; rfl
  | cons b B ih =>
      intro p k hB hlen hle
      cases k with
      | zero => rfl
      | succ m =>
          by_cases hc : rowLt p b = true
          · exfalso
            have hb : b ∈ (b :: B).take (m + 1) := by
              rw [take_succ_cons]
              exact mem_cons_self
            exact absurd hc (hle b hb)
          · rw [insertRow_cons_neg _ _ _ hc, take_succ_cons, take_succ_cons]
            refine congrArg (fun t => b :: t)
              (ih p m (pairwise_cons.mp hB).2
                (by simpa [length_cons] using hlen)
                (fun c hc' => hle c ?_))
            rw [take_succ_cons]
            exact mem_cons.mpr (Or.inr hc')

private theorem perm_cons_append (p : Row) : ∀ s : List Row, p :: s ~ s ++ [p] := by
  intro s
  induction s with
  | nil => rfl
  | cons x xs ih =>
      calc p :: x :: xs ~ x :: p :: xs := Perm.swap x p xs
        _ ~ x :: (xs ++ [p]) := Perm.cons x ih

private theorem perm_shift : ∀ (p : Row) (Q s : List Row), Q ++ (p :: s) ~ (Q ++ s) ++ [p] := by
  intro p Q
  induction Q with
  | nil => intro s; exact perm_cons_append p s
  | cons x xs ih =>
      intro s
      calc (x :: xs) ++ (p :: s) = x :: (xs ++ p :: s) := rfl
        _ ~ x :: ((xs ++ s) ++ [p]) := Perm.cons x (ih s)
        _ = (x :: xs ++ s) ++ [p] := rfl

private theorem perm_rearrange (A C M : List Row) : A ++ (C ++ M) ~ C ++ (A ++ M) := by
  induction A with
  | nil => simp
  | cons x xs ih =>
      calc (x :: xs) ++ (C ++ M) = x :: (xs ++ (C ++ M)) := rfl
        _ ~ x :: (C ++ (xs ++ M)) := Perm.cons x ih
        _ = (x :: C) ++ (xs ++ M) := rfl
        _ ~ (C ++ [x]) ++ (xs ++ M) := Perm.append (perm_cons_append x C) (Perm.refl (xs ++ M))
        _ = C ++ ((x :: xs) ++ M) := by rw [append_assoc, cons_append, nil_append, ← cons_append]

private theorem length_insertRow : ∀ (r : Row) (l : List Row), (insertRow r l).length = l.length + 1 := by
  intro r l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : rowLt r x = true
      · rw [insertRow_cons_pos r x xs h]
        simp only [length_cons]
      · rw [insertRow_cons_neg r x xs h]
        simp only [length_cons, ih]

private theorem length_sortRows : ∀ l : List Row, (sortRows l).length = l.length := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      show (insertRow x (sortRows xs)).length = (x :: xs).length
      rw [length_insertRow, ih, length_cons]

private theorem sortRows_append_singleton : ∀ (U : List Row) (p : Row),
    sortRows (U ++ [p]) = insertRow p (sortRows U) := by
  intro U p
  induction U with
  | nil => rfl
  | cons x xs ih =>
      have hp : (sortRows xs).Pairwise rowLe := sortRows_pairwise _
      have e1 := (insertRow_perm x (insertRow p (sortRows xs))).trans
        (Perm.cons x (insertRow_perm p (sortRows xs)))
      have e2 : x :: (p :: sortRows xs) ~ p :: (x :: sortRows xs) :=
        (Perm.swap x p (sortRows xs)).symm
      have e3 := (Perm.cons p (insertRow_perm x (sortRows xs))).symm
      have e4 := (insertRow_perm p (insertRow x (sortRows xs))).symm
      have key : insertRow x (insertRow p (sortRows xs))
          = insertRow p (insertRow x (sortRows xs)) :=
        sortRows_eq_of_perm_sorted (((e1.trans e2).trans e3).trans e4)
          (insertRow_pairwise x _ (insertRow_pairwise p _ hp))
          (insertRow_pairwise p _ (insertRow_pairwise x _ hp))
      calc sortRows ((x :: xs) ++ [p])
          = insertRow x (sortRows (xs ++ [p])) := rfl
        _ = insertRow x (insertRow p (sortRows xs)) := congrArg (insertRow x) ih
        _ = insertRow p (insertRow x (sortRows xs)) := key
        _ = insertRow p (sortRows (x :: xs)) := rfl

private theorem take_sortRows_merge_trunc : ∀ (k : Nat) (P₁ P₂ L : List Row),
    (P₁.length = k ∨ P₂ = []) → (P₁ ++ P₂).Pairwise rowLe →
    (sortRows (P₁ ++ (P₂ ++ L))).take k = (sortRows (P₁ ++ L)).take k := by
  intro k P₁ P₂
  induction P₂ with
  | nil => intro L _ _; rfl
  | cons p P₂ ih =>
      intro L hcase hpair
      obtain hP₁ | hnil := hcase
      · obtain ⟨hP₁pair, hrest, hcros⟩ := pairwise_append.mp hpair
        have hP₂pair : (P₁ ++ P₂).Pairwise rowLe :=
          pairwise_append.mpr ⟨hP₁pair, (pairwise_cons.mp hrest).2,
            fun a ha b hb => hcros a ha b (mem_cons.mpr (Or.inr hb))⟩
        calc (sortRows (P₁ ++ (p :: P₂ ++ L))).take k
            _ = (sortRows ((P₁ ++ (P₂ ++ L)) ++ [p])).take k :=
              congrArg (fun t : List Row => t.take k)
                (sortRows_cong (perm_shift p P₁ (P₂ ++ L)))
            _ = (insertRow p (sortRows (P₁ ++ (P₂ ++ L)))).take k :=
              congrArg (fun t : List Row => t.take k) (sortRows_append_singleton _ p)
            _ = (sortRows (P₁ ++ (P₂ ++ L))).take k :=
              ins_cut (sortRows (P₁ ++ (P₂ ++ L))) p k (sortRows_pairwise _)
                (by rw [length_sortRows, length_append, ← hP₁]; exact Nat.le_add_right _ _)
                (fun b hb => prefix_small P₁ P₂ L k p hP₁
                  (fun a ha => hcros a ha p (mem_cons.mpr (Or.inl rfl)))
                  b (by rw [append_assoc]; exact hb))
            _ = (sortRows (P₁ ++ L)).take k := ih L (Or.inl hP₁) hP₂pair
      · exfalso
        exact List.cons_ne_nil p P₂ hnil

/-- Chunk-wise truncation is invisible to the global `take n`: flattening the
`n`-truncated sorted chunks and sorting agrees with the full union. -/
private theorem take_sortRows_flatten : ∀ (n : Nat) (chunks : List (List Row)) (A : List Row),
    (sortRows (A ++ (chunks.map (fun c => (sortRows c).take n)).flatten)).take n
      = (sortRows (A ++ chunks.flatten)).take n := by
  intro n chunks
  induction chunks with
  | nil => intro A; rfl
  | cons c chunks ih =>
      intro A
      have hflat : ((c :: chunks).map (fun y => (sortRows y).take n)).flatten
          = (sortRows c).take n ++ (chunks.map (fun y => (sortRows y).take n)).flatten := by
            simp [List.map]
      have hcase : ((sortRows c).take n).length = n ∨ (sortRows c).drop n = [] := by
        rcases Nat.lt_or_ge n (sortRows c).length with hlt | hge
        · left
          rw [length_take, Nat.min_eq_left (Nat.le_of_lt hlt)]
        · right
          refine List.eq_nil_of_length_eq_zero ?_
          rw [length_drop, length_sortRows]
          rw [length_sortRows] at hge
          omega
      have hp : (sortRows c).Pairwise rowLe := sortRows_pairwise c
      have hpair : ((sortRows c).take n ++ (sortRows c).drop n).Pairwise rowLe :=
        pairwise_append.mpr ⟨pairwise_take (sortRows c) hp n,
          Pairwise.sublist (drop_sublist n (sortRows c)) hp,
          fun a ha b hb => take_drop_bound (sortRows c) n hp a ha b hb⟩
      calc (sortRows (A ++ ((c :: chunks).map (fun y => (sortRows y).take n)).flatten)).take n
          _ = (sortRows (A ++ ((sortRows c).take n ++ (chunks.map (fun y => (sortRows y).take n)).flatten))).take n :=
            congrArg (fun t : List Row => (sortRows (A ++ t)).take n) hflat
          _ = (sortRows ((sortRows c).take n ++ (A ++ (chunks.map (fun y => (sortRows y).take n)).flatten))).take n :=
            congrArg (fun t : List Row => t.take n)
              (sortRows_cong (perm_rearrange A ((sortRows c).take n)
                (chunks.map (fun y => (sortRows y).take n)).flatten))
          _ = (sortRows ((sortRows c).take n ++ ((sortRows c).drop n ++
              (A ++ (chunks.map (fun y => (sortRows y).take n)).flatten)))).take n :=
            (take_sortRows_merge_trunc n ((sortRows c).take n) ((sortRows c).drop n)
              (A ++ (chunks.map (fun y => (sortRows y).take n)).flatten) hcase hpair).symm
          _ = (sortRows (sortRows c ++ (A ++ (chunks.map (fun y => (sortRows y).take n)).flatten))).take n :=
            congrArg (fun t : List Row => (sortRows t).take n)
              (by rw [← append_assoc, take_append_drop])
          _ = (sortRows (c ++ (A ++ (chunks.map (fun y => (sortRows y).take n)).flatten))).take n :=
            congrArg (fun t : List Row => t.take n)
              (sortRows_cong (Perm.append (sortRows_perm c)
                (Perm.refl (A ++ (chunks.map (fun y => (sortRows y).take n)).flatten))))
          _ = (sortRows ((c ++ A) ++ (chunks.map (fun y => (sortRows y).take n)).flatten)).take n :=
            congrArg (fun t : List Row => (sortRows t).take n)
              (append_assoc c A (chunks.map (fun y => (sortRows y).take n)).flatten).symm
          _ = (sortRows ((c ++ A) ++ chunks.flatten)).take n :=
            ih (c ++ A)
          _ = (sortRows (A ++ (c :: chunks).flatten)).take n := by
            rw [append_assoc c A chunks.flatten]
            exact congrArg (fun t : List Row => t.take n)
              (sortRows_cong (perm_rearrange A c chunks.flatten).symm)

theorem sound : TinylambOpt.ResearchGoals.BoundedHeapTopNMerge.Goal := by
  intro off lim chunks
  show ((sortRows ((chunks.map (fun c => (sortRows c).take (off + lim))).flatten)).drop off).take lim
      = ((sortRows chunks.flatten).drop off).take lim
  rw [List.take_drop, List.take_drop]
  exact congrArg (fun t : List Row => t.drop off)
    (by show (sortRows ((chunks.map (fun c => (sortRows c).take (off + lim))).flatten)).take (off + lim)
          = (sortRows chunks.flatten).take (off + lim)
        exact take_sortRows_flatten (off + lim) chunks [])

end TinylambOpt.ResearchProofs.BoundedHeapTopNMerge
