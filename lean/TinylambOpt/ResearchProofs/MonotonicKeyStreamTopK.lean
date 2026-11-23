import TinylambOpt.ResearchGoals.MonotonicKeyStreamTopK

-- Soundness of monotonic-key streaming Top-K.
--
-- The proof transports the Boolean comparator `betterEntry` to a strict total
-- order on entries (keys are distinct inside the grouped stream), proves the
-- usual sorted-insertion bookkeeping for `sortBy`, and then relates the
-- bounded streaming pipeline to an unbounded "online" grouping process whose
-- final accumulator is exactly `foldl addKeyVal [] chunks.flatten`.  Finally
-- the truncation is shown invisible to `take k`.

open List

namespace TinylambOpt.ResearchProofs.MonotonicKeyStreamTopK

open TinylambOpt.ResearchGoals.MonotonicKeyStreamTopK

/-- The order transported from the Boolean comparator. -/
private def entryLe (a b : Entry) : Prop := ¬ betterEntry b a = true

private theorem better_char {a b : Entry} :
    betterEntry a b = true ↔ b.2 < a.2 ∨ (a.2 = b.2 ∧ a.1 < b.1) := by
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  by_cases h : a2 = b2
  · simp [betterEntry, h, decide_eq_true_eq]
  · simp [betterEntry, h, decide_eq_true_eq]

private theorem better_asym {a b : Entry} (h1 : betterEntry a b = true)
    (h2 : betterEntry b a = true) : False := by
  rcases better_char.mp h1 with h | ⟨_, h⟩
  · rcases better_char.mp h2 with h' | ⟨e', h'⟩
    · omega
    · omega
  · rcases better_char.mp h2 with h' | ⟨e', h'⟩
    · omega
    · omega

private theorem better_trans {a b c : Entry} (h1 : betterEntry a b = true)
    (h2 : betterEntry b c = true) : betterEntry a c = true := by
  rw [better_char] at h1 h2 ⊢
  rcases h1 with h1 | ⟨e1, h1⟩
  · rcases h2 with h2 | ⟨e2, h2⟩
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
  · rcases h2 with h2 | ⟨e2, h2⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, by omega⟩

private theorem better_trichotomy (a b : Entry) :
    betterEntry a b = true ∨ a = b ∨ betterEntry b a = true := by
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  rw [better_char, better_char]
  simp only [Prod.mk.injEq]
  omega

private theorem entryLe_refl (a : Entry) : entryLe a a := by
  rw [entryLe]
  intro h
  rw [better_char] at h
  rcases h with h | ⟨_, h⟩ <;> omega

private theorem entryLe_total (a b : Entry) : entryLe a b ∨ entryLe b a := by
  rcases better_trichotomy a b with h | h | h
  · exact Or.inl (by rw [entryLe]; intro hb; exact better_asym h hb)
  · subst h
    exact Or.inl (entryLe_refl _)
  · exact Or.inr (by rw [entryLe]; intro hb; exact better_asym h hb)

private theorem entryLe_antisymm {a b : Entry} (h1 : entryLe a b) (h2 : entryLe b a) : a = b := by
  rw [entryLe] at h1 h2
  rcases better_trichotomy a b with h | h | h
  · exact False.elim (h2 h)
  · exact h
  · exact False.elim (h1 h)

private theorem entryLe_trans {a b c : Entry} (h1 : entryLe a b) (h2 : entryLe b c) : entryLe a c := by
  rw [entryLe] at h1 h2 ⊢
  intro hca
  rcases better_trichotomy b c with hbc | hbc | hcb
  · exact absurd (better_trans hbc hca) h1
  · subst hbc
    exact h1 hca
  · exact h2 hcb

private theorem better_of_not_entryLe {a b : Entry} (h : ¬ entryLe a b) : betterEntry b a = true :=
  Classical.byContradiction (fun hn => h hn)

private theorem entryLe_of_better {a b : Entry} (h : betterEntry a b = true) : entryLe a b := by
  rw [entryLe]
  intro hb
  exact better_asym h hb

private theorem better_of_entryLe_not {a b : Entry} (h : entryLe a b) (hne : a ≠ b) :
    betterEntry a b = true := by
  rcases better_trichotomy a b with hlt | heq | hgt
  · exact hlt
  · exact False.elim (hne heq)
  · exact False.elim (h hgt)

-- Sorted insertion bookkeeping.

private theorem mem_insertSortedBy {x y : Entry} : ∀ l : List Entry,
    y ∈ insertSortedBy betterEntry x l ↔ y = x ∨ y ∈ l := by
  intro l
  induction l with
  | nil => simp [insertSortedBy]
  | cons z zs ih =>
      by_cases h : betterEntry x z = true
      · simp [insertSortedBy, h]
      · simp [insertSortedBy, h, ih, or_left_comm]

private theorem insertSortedBy_cons_pos (x z : Entry) (l : List Entry)
    (h : betterEntry x z = true) :
    insertSortedBy betterEntry x (z :: l) = x :: z :: l := by
  simp [insertSortedBy, h]

private theorem insertSortedBy_cons_neg (x z : Entry) (l : List Entry)
    (h : ¬ betterEntry x z = true) :
    insertSortedBy betterEntry x (z :: l) = z :: insertSortedBy betterEntry x l := by
  simp [insertSortedBy, h]

private theorem insertSortedBy_perm (x : Entry) : ∀ l : List Entry,
    insertSortedBy betterEntry x l ~ x :: l := by
  intro l
  induction l with
  | nil => exact Perm.refl _
  | cons z zs ih =>
      by_cases h : betterEntry x z = true
      · rw [insertSortedBy_cons_pos _ _ _ h]
      · rw [insertSortedBy_cons_neg _ _ _ h]
        exact (Perm.cons z ih).trans (Perm.swap z x zs).symm

private theorem insertSortedBy_pairwise (x : Entry) : ∀ l : List Entry,
    l.Pairwise entryLe → (insertSortedBy betterEntry x l).Pairwise entryLe := by
  intro l
  induction l with
  | nil => intro _; simp [insertSortedBy]
  | cons z zs ih =>
      intro h
      obtain ⟨hz, hzs⟩ := pairwise_cons.mp h
      by_cases hc : betterEntry x z = true
      · rw [insertSortedBy_cons_pos _ _ _ hc]
        refine pairwise_cons.mpr ⟨fun y hy => ?_, pairwise_cons.mpr ⟨hz, hzs⟩⟩
        rcases mem_cons.mp hy with rfl | hy
        · exact entryLe_of_better hc
        · exact entryLe_trans (entryLe_of_better hc) (hz y hy)
      · rw [insertSortedBy_cons_neg _ _ _ hc]
        refine pairwise_cons.mpr ⟨fun y hy => ?_, ih hzs⟩
        rw [mem_insertSortedBy] at hy
        rcases hy with rfl | hy
        · exact hc
        · exact hz y hy

private theorem sortBy_perm : ∀ l : List Entry, sortBy betterEntry l ~ l := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      calc sortBy betterEntry (x :: xs) = insertSortedBy betterEntry x (sortBy betterEntry xs) := rfl
        _ ~ x :: sortBy betterEntry xs := insertSortedBy_perm x _
        _ ~ x :: xs := Perm.cons x ih

private theorem sortBy_pairwise : ∀ l : List Entry, (sortBy betterEntry l).Pairwise entryLe := by
  intro l
  induction l with
  | nil => simp [sortBy]
  | cons x xs ih => exact insertSortedBy_pairwise x _ ih

private theorem sortBy_eq_of_perm_sorted {l₁ l₂ : List Entry}
    (h : l₁ ~ l₂) (h₁ : l₁.Pairwise entryLe) (h₂ : l₂.Pairwise entryLe) : l₁ = l₂ :=
  Perm.eq_of_pairwise (fun _ _ _ _ hab hba => entryLe_antisymm hab hba) h₁ h₂ h

private theorem sortBy_idempotent (l : List Entry) : sortBy betterEntry (sortBy betterEntry l) = sortBy betterEntry l :=
  sortBy_eq_of_perm_sorted (sortBy_perm (sortBy betterEntry l))
    (sortBy_pairwise _) (sortBy_pairwise _)

private theorem sortBy_cong {l₁ l₂ : List Entry} (h : l₁ ~ l₂) : sortBy betterEntry l₁ = sortBy betterEntry l₂ :=
  sortBy_eq_of_perm_sorted ((sortBy_perm l₁).trans (h.trans (sortBy_perm l₂).symm))
    (sortBy_pairwise _) (sortBy_pairwise _)

private theorem pairwise_take (l : List Entry) (h : l.Pairwise entryLe) (i : Nat) :
    (l.take i).Pairwise entryLe :=
  Pairwise.sublist (take_sublist i l) h

private theorem take_append_self (Y : List Entry) : ∀ C : List Entry, (C ++ Y).take C.length = C := by
  intro C
  induction C with
  | nil => rfl
  | cons x xs ih => rw [cons_append, length_cons, take_succ_cons, ih]

private theorem take_drop_bound : ∀ (l : List Entry) (i : Nat), l.Pairwise entryLe →
    ∀ c ∈ l.take i, ∀ d ∈ l.drop i, entryLe c d := by
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

private theorem take_cons_take (x : Entry) (xs : List Entry) (m : Nat) :
    (x :: xs).take m = (x :: xs.take m).take m := by
  cases m with
  | zero => rfl
  | succ j =>
      rw [take_succ_cons, take_succ_cons, take_take, Nat.min_eq_left (Nat.le_succ j)]

/-- Truncating after a sorted insertion equals inserting into the truncation. -/
private theorem take_insertSorted (x : Entry) : ∀ (l : List Entry) (k : Nat),
    (insertSortedBy betterEntry x l).take k = (insertSortedBy betterEntry x (l.take k)).take k := by
  intro l
  induction l with
  | nil => intro k; rw [take_nil]
  | cons z zs ih =>
      intro k
      cases k with
      | zero => rfl
      | succ m =>
          by_cases h : betterEntry x z = true
          · rw [insertSortedBy_cons_pos _ _ _ h,
                show ((z :: zs).take (m + 1)) = z :: zs.take m from take_succ_cons,
                insertSortedBy_cons_pos _ _ _ h, take_succ_cons, take_succ_cons]
            exact congrArg (fun t => x :: t) (take_cons_take z zs m)
          · rw [take_succ_cons, insertSortedBy_cons_neg _ _ _ h, insertSortedBy_cons_neg _ _ _ h,
              take_succ_cons, take_succ_cons, ih]

-- Top-k as a canonical sorted prefix.

private def topk (k : Nat) (l : List Entry) : List Entry := (sortBy betterEntry l).take k

private theorem topk_cong {l₁ l₂ : List Entry} (h : l₁ ~ l₂) (k : Nat) : topk k l₁ = topk k l₂ := by
  unfold topk
  rw [sortBy_cong h]

private theorem sortBy_eq_self_of_pairwise {l : List Entry} (h : l.Pairwise entryLe) :
    sortBy betterEntry l = l :=
  sortBy_eq_of_perm_sorted (sortBy_perm l) (sortBy_pairwise l) h

private theorem topk_insert (k : Nat) (e : Entry) (l : List Entry) :
    topk k (e :: l) = insertTopK k e (topk k l) := by
  unfold topk insertTopK
  rw [sortBy, take_insertSorted]
  rw [sortBy_eq_self_of_pairwise (pairwise_take (sortBy betterEntry l) (sortBy_pairwise l) k)]

private theorem exists_decomp_of_mem (b : Entry) :
    ∀ {C : List Entry}, b ∈ C → ∃ t r : List Entry, C = t ++ b :: r := by
  intro C h
  induction C with
  | nil => exact False.elim (List.not_mem_nil h)
  | cons x xs ih =>
      rcases mem_cons.mp h with rfl | h
      · exact ⟨[], xs, rfl⟩
      · rcases ih h with ⟨t, r, heq⟩
        exact ⟨x :: t, r, by rw [heq, ← cons_append]⟩

private theorem better_of_entryLe_of_better {a p b : Entry} (h1 : entryLe a p)
    (h2 : betterEntry p b = true) : betterEntry a b = true := by
  apply Classical.byContradiction
  intro hn
  have hba : entryLe b a := by rw [entryLe]; exact hn
  exact absurd h2 (entryLe_trans hba h1)

private theorem ne_of_better {a b : Entry} (h : betterEntry a b = true) : a ≠ b := by
  intro heq
  subst heq
  rw [better_char] at h
  rcases h with h | ⟨_, h⟩ <;> omega

private theorem length_le_of_count_le :
    ∀ (P Q : List Entry), (∀ v : Entry, count v P ≤ count v Q) → P.length ≤ Q.length := by
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

/-- Every row of the `k`-prefix of `sortBy (P₁ ++ rest ++ L)` is `≤ p`
whenever `P₁` has length `k` and is pointwise `≤ p`. -/
private theorem prefix_small : ∀ (P₁ rest L : List Entry) (k : Nat) (p : Entry),
    P₁.length = k → (∀ a ∈ P₁, entryLe a p) →
    ∀ b ∈ (sortBy betterEntry (P₁ ++ rest ++ L)).take k, entryLe b p := by
  intro P₁ rest L k p hlen hle b hb hpb
  let S := sortBy betterEntry (P₁ ++ rest ++ L)
  have hbS : b ∈ S.take k := hb
  have hpair : S.Pairwise entryLe := sortBy_pairwise _
  rcases exists_decomp_of_mem b hbS with ⟨front, r, hC⟩
  have h1 : S = (front ++ b :: r) ++ S.drop k := by
    rw [← hC, take_append_drop]
  have hBdeq : S = front ++ (b :: r ++ S.drop k) := h1.trans (append_assoc _ _ _)
  rw [hBdeq] at hpair
  obtain ⟨_, htail, _⟩ := pairwise_append.mp hpair
  have hb_le : ∀ x ∈ r ++ S.drop k, entryLe b x := by
    intro x hx
    exact rel_of_pairwise_cons htail hx
  have hcount : ∀ v : Entry, count v P₁ ≤ count v front := by
    intro v
    by_cases hv : betterEntry v b = true
    · have hzero : count v (b :: r ++ S.drop k) = 0 := by
        apply count_eq_zero_of_not_mem
        intro hx
        rcases mem_cons.mp hx with rfl | hx'
        · exact absurd hv (entryLe_refl _)
        · rcases mem_append.mp hx' with hxr | hxd
          · exact absurd hv (hb_le v (mem_append.mpr (Or.inl hxr)))
          · exact absurd hv (hb_le v (mem_append.mpr (Or.inr hxd)))
      calc count v P₁
          _ ≤ count v (P₁ ++ rest) := by
              rw [count_append]
              exact Nat.le_add_right _ _
          _ ≤ count v (P₁ ++ rest) + count v L := Nat.le_add_right _ _
          _ = count v (P₁ ++ rest ++ L) := count_append.symm
          _ = count v S := ((perm_iff_count (α := Entry)).mp (sortBy_perm (P₁ ++ rest ++ L)) v).symm
          _ = count v front := by
              rw [hBdeq, count_append, hzero, Nat.add_zero]
    · have hzero : count v P₁ = 0 :=
          count_eq_zero_of_not_mem
            (fun hmem => absurd (better_of_entryLe_of_better (hle v hmem) hpb) hv)
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

private theorem ins_cut : ∀ (B : List Entry) (p : Entry) (k : Nat), B.Pairwise entryLe →
    k ≤ B.length → (∀ b ∈ B.take k, entryLe b p) → (insertSortedBy betterEntry p B).take k = B.take k := by
  intro B
  induction B with
  | nil => intro p k _ hlen _; have hk : k = 0 := Nat.eq_zero_of_le_zero hlen; subst hk; rfl
  | cons b B ih =>
      intro p k hB hlen hle
      cases k with
      | zero => rfl
      | succ m =>
          by_cases hc : betterEntry p b = true
          · exfalso
            have hb : b ∈ (b :: B).take (m + 1) := by
              rw [take_succ_cons]
              exact mem_cons_self
            exact absurd hc (hle b hb)
          · rw [insertSortedBy_cons_neg _ _ _ hc, take_succ_cons, take_succ_cons]
            refine congrArg (fun t => b :: t)
              (ih p m (pairwise_cons.mp hB).2
                (by simpa [length_cons] using hlen)
                (fun c hc' => hle c ?_))
            rw [take_succ_cons]
            exact mem_cons.mpr (Or.inr hc')

private theorem perm_cons_append (p : Entry) : ∀ s : List Entry, p :: s ~ s ++ [p] := by
  intro s
  induction s with
  | nil => rfl
  | cons x xs ih =>
      calc p :: x :: xs ~ x :: p :: xs := Perm.swap x p xs
        _ ~ x :: (xs ++ [p]) := Perm.cons x ih
        _ = (x :: xs) ++ [p] := rfl

private theorem perm_shift : ∀ (p : Entry) (Q s : List Entry), Q ++ (p :: s) ~ (Q ++ s) ++ [p] := by
  intro p Q
  induction Q with
  | nil => intro s; exact perm_cons_append p s
  | cons x xs ih =>
      intro s
      calc (x :: xs) ++ (p :: s) = x :: (xs ++ p :: s) := rfl
        _ ~ x :: ((xs ++ s) ++ [p]) := Perm.cons x (ih s)
        _ = (x :: xs ++ s) ++ [p] := rfl

private theorem perm_rearrange (A C M : List Entry) : A ++ (C ++ M) ~ C ++ (A ++ M) := by
  induction A with
  | nil => simp
  | cons x xs ih =>
      calc (x :: xs) ++ (C ++ M) = x :: (xs ++ (C ++ M)) := rfl
        _ ~ x :: (C ++ (xs ++ M)) := Perm.cons x ih
        _ = (x :: C) ++ (xs ++ M) := rfl
        _ ~ (C ++ [x]) ++ (xs ++ M) := Perm.append (perm_cons_append x C) (Perm.refl (xs ++ M))
        _ = C ++ ((x :: xs) ++ M) := by rw [append_assoc, cons_append, nil_append, ← cons_append]

private theorem length_insertSortedBy : ∀ (r : Entry) (l : List Entry),
    (insertSortedBy betterEntry r l).length = l.length + 1 := by
  intro r l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : betterEntry r x = true
      · rw [insertSortedBy_cons_pos r x xs h]
        simp only [length_cons]
      · rw [insertSortedBy_cons_neg r x xs h]
        simp only [length_cons, ih]

private theorem length_sortBy : ∀ l : List Entry, (sortBy betterEntry l).length = l.length := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      show (insertSortedBy betterEntry x (sortBy betterEntry xs)).length = (x :: xs).length
      rw [length_insertSortedBy, ih, length_cons]

private theorem sortBy_append_singleton : ∀ (U : List Entry) (p : Entry),
    sortBy betterEntry (U ++ [p]) = insertSortedBy betterEntry p (sortBy betterEntry U) := by
  intro U p
  induction U with
  | nil => rfl
  | cons x xs ih =>
      have hp : (sortBy betterEntry xs).Pairwise entryLe := sortBy_pairwise _
      have e1 := (insertSortedBy_perm x (insertSortedBy betterEntry p (sortBy betterEntry xs))).trans
        (Perm.cons x (insertSortedBy_perm p (sortBy betterEntry xs)))
      have e2 : x :: (p :: sortBy betterEntry xs) ~ p :: (x :: sortBy betterEntry xs) :=
        (Perm.swap x p (sortBy betterEntry xs)).symm
      have e3 := (Perm.cons p (insertSortedBy_perm x (sortBy betterEntry xs))).symm
      have e4 := (insertSortedBy_perm p (insertSortedBy betterEntry x (sortBy betterEntry xs))).symm
      have key : insertSortedBy betterEntry x (insertSortedBy betterEntry p (sortBy betterEntry xs))
          = insertSortedBy betterEntry p (insertSortedBy betterEntry x (sortBy betterEntry xs)) :=
        sortBy_eq_of_perm_sorted (((e1.trans e2).trans e3).trans e4)
          (insertSortedBy_pairwise x _ (insertSortedBy_pairwise p _ hp))
          (insertSortedBy_pairwise p _ (insertSortedBy_pairwise x _ hp))
      calc sortBy betterEntry ((x :: xs) ++ [p])
          = insertSortedBy betterEntry x (sortBy betterEntry (xs ++ [p])) := rfl
        _ = insertSortedBy betterEntry x (insertSortedBy betterEntry p (sortBy betterEntry xs)) :=
          congrArg (insertSortedBy betterEntry x) ih
        _ = insertSortedBy betterEntry p (insertSortedBy betterEntry x (sortBy betterEntry xs)) := key
        _ = insertSortedBy betterEntry p (sortBy betterEntry (x :: xs)) := rfl

private theorem take_sortBy_merge_trunc : ∀ (k : Nat) (P₁ P₂ L : List Entry),
    (P₁.length = k ∨ P₂ = []) → (P₁ ++ P₂).Pairwise entryLe →
    (sortBy betterEntry (P₁ ++ (P₂ ++ L))).take k = (sortBy betterEntry (P₁ ++ L)).take k := by
  intro k P₁ P₂
  induction P₂ with
  | nil => intro L _ _; rfl
  | cons p P₂ ih =>
      intro L hcase hpair
      obtain hP₁ | hnil := hcase
      · obtain ⟨hP₁pair, hrest, hcros⟩ := pairwise_append.mp hpair
        have hP₂pair : (P₁ ++ P₂).Pairwise entryLe :=
          pairwise_append.mpr ⟨hP₁pair, (pairwise_cons.mp hrest).2,
            fun a ha b hb => hcros a ha b (mem_cons.mpr (Or.inr hb))⟩
        calc (sortBy betterEntry (P₁ ++ (p :: P₂ ++ L))).take k
            _ = (sortBy betterEntry ((P₁ ++ (P₂ ++ L)) ++ [p])).take k :=
              congrArg (fun t : List Entry => t.take k)
                (sortBy_cong (perm_shift p P₁ (P₂ ++ L)))
            _ = (insertSortedBy betterEntry p (sortBy betterEntry (P₁ ++ (P₂ ++ L)))).take k :=
              congrArg (fun t : List Entry => t.take k) (sortBy_append_singleton _ p)
            _ = (sortBy betterEntry (P₁ ++ (P₂ ++ L))).take k :=
              ins_cut (sortBy betterEntry (P₁ ++ (P₂ ++ L))) p k (sortBy_pairwise _)
                (by rw [length_sortBy, length_append, ← hP₁]; exact Nat.le_add_right _ _)
                (fun b hb => prefix_small P₁ P₂ L k p hP₁
                  (fun a ha => hcros a ha p (mem_cons.mpr (Or.inl rfl)))
                  b (by rw [append_assoc]; exact hb))
            _ = (sortBy betterEntry (P₁ ++ L)).take k := ih L (Or.inl hP₁) hP₂pair
      · exfalso
        exact List.cons_ne_nil p P₂ hnil

/-- Chunk-wise truncation is invisible to the global `take k`. -/
private theorem take_sortBy_flatten : ∀ (n : Nat) (chunks : List (List Entry)) (A : List Entry),
    (sortBy betterEntry (A ++ (chunks.map (fun c => (sortBy betterEntry c).take n)).flatten)).take n
      = (sortBy betterEntry (A ++ chunks.flatten)).take n := by
  intro n chunks
  induction chunks with
  | nil => intro A; rfl
  | cons c chunks ih =>
      intro A
      have hflat : ((c :: chunks).map (fun y => (sortBy betterEntry y).take n)).flatten
          = (sortBy betterEntry c).take n ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten := by
            simp [List.map]
      have hcase : ((sortBy betterEntry c).take n).length = n ∨ (sortBy betterEntry c).drop n = [] := by
        rcases Nat.lt_or_ge n (sortBy betterEntry c).length with hlt | hge
        · left
          rw [length_take, Nat.min_eq_left (Nat.le_of_lt hlt)]
        · right
          refine List.eq_nil_of_length_eq_zero ?_
          rw [length_drop, length_sortBy]
          rw [length_sortBy] at hge
          omega
      have hp : (sortBy betterEntry c).Pairwise entryLe := sortBy_pairwise c
      have hpair : ((sortBy betterEntry c).take n ++ (sortBy betterEntry c).drop n).Pairwise entryLe :=
        pairwise_append.mpr ⟨pairwise_take (sortBy betterEntry c) hp n,
          Pairwise.sublist (drop_sublist n (sortBy betterEntry c)) hp,
          fun a ha b hb => take_drop_bound (sortBy betterEntry c) n hp a ha b hb⟩
      calc (sortBy betterEntry (A ++ ((c :: chunks).map (fun y => (sortBy betterEntry y).take n)).flatten)).take n
          _ = (sortBy betterEntry (A ++ ((sortBy betterEntry c).take n ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten))).take n :=
            congrArg (fun t : List Entry => (sortBy betterEntry (A ++ t)).take n) hflat
          _ = (sortBy betterEntry ((sortBy betterEntry c).take n ++ (A ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten))).take n :=
            congrArg (fun t : List Entry => t.take n)
              (sortBy_cong (perm_rearrange A ((sortBy betterEntry c).take n)
                (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten))
          _ = (sortBy betterEntry ((sortBy betterEntry c).take n ++ ((sortBy betterEntry c).drop n ++
              (A ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten)))).take n :=
            (take_sortBy_merge_trunc n ((sortBy betterEntry c).take n) ((sortBy betterEntry c).drop n)
              (A ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten) hcase hpair).symm
          _ = (sortBy betterEntry (sortBy betterEntry c ++ (A ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten))).take n :=
            congrArg (fun t : List Entry => (sortBy betterEntry t).take n)
              (by rw [← append_assoc, take_append_drop])
          _ = (sortBy betterEntry (c ++ (A ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten))).take n :=
            congrArg (fun t : List Entry => t.take n)
              (sortBy_cong (Perm.append (sortBy_perm c)
                (Perm.refl (A ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten))))
          _ = (sortBy betterEntry ((c ++ A) ++ (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten)).take n :=
            congrArg (fun t : List Entry => (sortBy betterEntry t).take n)
              (append_assoc c A (chunks.map (fun y => (sortBy betterEntry y).take n)).flatten).symm
          _ = (sortBy betterEntry ((c ++ A) ++ chunks.flatten)).take n :=
            ih (c ++ A)
          _ = (sortBy betterEntry (A ++ (c :: chunks).flatten)).take n := by
            rw [append_assoc c A chunks.flatten]
            exact congrArg (fun t : List Entry => t.take n)
              (sortBy_cong (perm_rearrange A c chunks.flatten).symm)

private theorem topk_append_topk (k : Nat) (A B : List Entry) :
    topk k (topk k A ++ topk k B) = topk k (A ++ B) := by
  unfold topk
  let sA := sortBy betterEntry A
  let sB := sortBy betterEntry B
  have hcase : (sA.take k).length = k ∨ sA.drop k = [] := by
    rcases Nat.lt_or_ge k sA.length with hlt | hge
    · left
      rw [length_take, Nat.min_eq_left (Nat.le_of_lt hlt)]
    · right
      refine List.eq_nil_of_length_eq_zero ?_
      rw [length_drop]
      omega
  have hpair : (sA.take k ++ sA.drop k).Pairwise entryLe :=
    pairwise_append.mpr ⟨pairwise_take sA (sortBy_pairwise A) k,
      Pairwise.sublist (drop_sublist k sA) (sortBy_pairwise A),
      fun a ha b hb => take_drop_bound sA k (sortBy_pairwise A) a ha b hb⟩
  have h1 : (sortBy betterEntry (sA.take k ++ (sA.drop k ++ sB.take k))).take k =
      (sortBy betterEntry (sA.take k ++ sB.take k)).take k :=
    take_sortBy_merge_trunc k (sA.take k) (sA.drop k) (sB.take k) hcase hpair
  have h1' : (sortBy betterEntry (sA ++ sB.take k)).take k =
      (sortBy betterEntry (sA.take k ++ sB.take k)).take k := by
    rw [← h1]
    congr 2
    rw [← append_assoc, take_append_drop]
  have h2 : (sortBy betterEntry (sA ++ sB.take k)).take k =
      (sortBy betterEntry (sA ++ B)).take k := by
    simpa [sB] using take_sortBy_flatten k [B] sA
  have h3 : (sortBy betterEntry (sA ++ B)).take k = (sortBy betterEntry (A ++ B)).take k := by
    apply congrArg (fun t : List Entry => t.take k)
    exact sortBy_cong (Perm.append (sortBy_perm A) (Perm.refl B))
  rw [← h1', h2, h3]

-- Grouping primitives.

private def sumVals (l : List Entry) : Int := (l.map Prod.snd).sum

private theorem addKeyVal_self (acc : List Entry) (k v : Int) :
    ∃ f ∈ addKeyVal acc (k, v), f.1 = k := by
  induction acc with
  | nil => exact ⟨(k, v), by simp [addKeyVal], rfl⟩
  | cons e es ih =>
      by_cases h : e.1 == k
      · refine ⟨(e.1, e.2 + v), ?_, ?_⟩
        · simp [addKeyVal, h]
        · exact of_decide_eq_true h
      · obtain ⟨f, hf, hfk⟩ := ih
        refine ⟨f, ?_, hfk⟩
        simp [addKeyVal, h, hf]

private theorem addKeyVal_preserve (acc : List Entry) (k v : Int) (f : Entry)
    (hf : f ∈ acc) (hne : f.1 ≠ k) :
    ∃ f' ∈ addKeyVal acc (k, v), f'.1 = f.1 := by
  induction acc with
  | nil => exact False.elim (not_mem_nil hf)
  | cons e es ih =>
      rcases mem_cons.mp hf with hEq | hf
      · by_cases h : e.1 == k
        · refine ⟨(e.1, e.2 + v), ?_, ?_⟩
          · simp [addKeyVal, h]
          · rw [hEq]
        · refine ⟨e, ?_, ?_⟩
          · simp [addKeyVal, h]
          · rw [hEq]
      · by_cases h : e.1 == k
        · refine ⟨f, ?_, rfl⟩
          simp [addKeyVal, h, hf]
        · obtain ⟨f', hf', hkey⟩ := ih hf
          refine ⟨f', ?_, hkey⟩
          have heq : addKeyVal (e :: es) (k, v) = e :: addKeyVal es (k, v) := by
            simp [addKeyVal, h]
          rw [heq]
          exact mem_cons.mpr (Or.inr hf')

private theorem key_survives (acc l : List Entry) (k : Int)
    (hk : ∃ f ∈ acc, f.1 = k) : ∃ f ∈ l.foldl addKeyVal acc, f.1 = k := by
  induction l generalizing acc with
  | nil => exact hk
  | cons x xs ih =>
      apply ih (acc := addKeyVal acc x)
      rcases hk with ⟨f, hf, hfk⟩
      by_cases hx : x.1 = k
      · obtain ⟨g, hg, hgk⟩ := addKeyVal_self acc x.1 x.2
        exact ⟨g, by simpa using hg, hgk.trans hx⟩
      · obtain ⟨g, hg, hgk⟩ := addKeyVal_preserve acc x.1 x.2 f hf (by
            intro hf1
            apply hx
            omega)
        exact ⟨g, hg, hgk.trans hfk⟩

private theorem key_survives_mem : ∀ (l : List Entry) (e : Entry), e ∈ l →
    ∀ (acc : List Entry), ∃ f ∈ l.foldl addKeyVal acc, e.1 = f.1 := by
  intro l
  induction l with
  | nil => intro e h; exact False.elim (not_mem_nil h)
  | cons x xs ih =>
      intro e h acc
      rcases mem_cons.mp h with hEq | h
      · subst e
        simpa using key_survives (addKeyVal acc x) xs x.1 (addKeyVal_self acc x.1 x.2)
      · exact ih e h (addKeyVal acc x)

private theorem addKeyVal_append_new (m : List Entry) (k v : Int)
    (h : ∀ e ∈ m, e.1 ≠ k) : addKeyVal m (k, v) = m ++ [(k, v)] := by
  induction m with
  | nil => rfl
  | cons e es ih =>
      have he : e.1 ≠ k := h e List.mem_cons_self
      have hes : ∀ e' ∈ es, e'.1 ≠ k := fun e' he' => h e' (mem_cons.mpr (Or.inr he'))
      simp [addKeyVal, he, ih hes]

private theorem addKeyVal_update_last (m' : List Entry) (k s v : Int)
    (h : ∀ e ∈ m', e.1 ≠ k) : addKeyVal (m' ++ [(k, s)]) (k, v) = m' ++ [(k, s + v)] := by
  induction m' with
  | nil => simp [addKeyVal]
  | cons e es ih =>
      have he : e.1 ≠ k := h e List.mem_cons_self
      have hes : ∀ e' ∈ es, e'.1 ≠ k := fun e' he' => h e' (mem_cons.mpr (Or.inr he'))
      simp [addKeyVal, he, ih hes]

private theorem addKeyVal_twice (acc : List Entry) (k v w : Int) :
    addKeyVal (addKeyVal acc (k, v)) (k, w) = addKeyVal acc (k, v + w) := by
  induction acc with
  | nil => simp [addKeyVal, Int.add_assoc]
  | cons e es ih =>
      by_cases h : e.1 == k
      · simp [addKeyVal, h, Int.add_assoc]
      · simp [addKeyVal, h, ih]

private theorem foldl_addKeyVal_key : ∀ (es : List Entry) (acc : List Entry) (k : Int),
    es ≠ [] → (∀ e ∈ es, e.1 = k) →
    es.foldl addKeyVal acc = addKeyVal acc (k, sumVals es) := by
  intro es
  induction es with
  | nil => intro acc k hne _; exact False.elim (hne rfl)
  | cons e es ih =>
      intro acc k hne hall
      have heq : e.1 = k := hall e List.mem_cons_self
      have hacc : addKeyVal acc e = addKeyVal acc (k, e.2) := by
        cases e with
        | mk e1 e2 =>
            simp at heq
            subst e1
            rfl
      cases es with
      | nil =>
          change addKeyVal acc e = addKeyVal acc (k, sumVals [e])
          rw [hacc]
          simp [sumVals]
      | cons y ys =>
          have hnon : y :: ys ≠ [] := by simp
          have hall' : ∀ z ∈ y :: ys, z.1 = k := by
            intro z hz
            exact hall z (mem_cons.mpr (Or.inr hz))
          rw [foldl_cons, ih (addKeyVal acc e) k hnon hall', hacc, addKeyVal_twice]
          simp [sumVals]

private def runSplit (k : Int) : List Entry → List Entry × List Entry
  | [] => ([], [])
  | (k', v) :: xs =>
      if k' == k then
        let (r, s) := runSplit k xs
        ((k', v) :: r, s)
      else ([], (k', v) :: xs)

private theorem runSplit_append (k : Int) : ∀ l : List Entry,
    let (r, s) := runSplit k l; r ++ s = l := by
  intro l
  induction l with
  | nil => rfl
  | cons e es ih =>
      by_cases h : e.1 == k
      · simp [runSplit, h, ih]
      · simp [runSplit, h]

private theorem runSplit_all_key (k : Int) : ∀ l : List Entry,
    let (r, s) := runSplit k l; ∀ e ∈ r, e.1 = k := by
  intro l
  induction l with
  | nil => simp [runSplit]
  | cons e es ih =>
      by_cases h : e.1 == k
      · simp only [runSplit, h]
        have ih' : ∀ z ∈ (runSplit k es).1, z.1 = k := by simpa using ih
        intro x hx
        rcases mem_cons.mp hx with hEq | hx
        · rw [hEq]
          exact of_decide_eq_true h
        · exact ih' x hx
      · simp [runSplit, h]

private theorem runSplit_rest (k : Int) : ∀ l : List Entry,
    match (runSplit k l).2 with
    | [] => True
    | e :: _ => e.1 ≠ k := by
  intro l
  induction l with
  | nil => simp [runSplit]
  | cons e es ih =>
      by_cases h : e.1 == k
      · simp [runSplit, h, ih]
      · simp [runSplit, h]
        intro heq
        apply h
        simp [heq]

private theorem runSplit_nonempty_of_same (x : Entry) (xs : List Entry) :
    (runSplit x.1 (x :: xs)).1 ≠ [] := by
  simp [runSplit]

private theorem keys_addKeyVal (acc : List Entry) (k v : Int) :
    ∀ f ∈ addKeyVal acc (k, v), f.1 = k ∨ ∃ a ∈ acc, f.1 = a.1 := by
  induction acc with
  | nil => simp [addKeyVal]
  | cons e es ih =>
      intro f hf
      by_cases h : e.1 == k
      · simp [addKeyVal, h] at hf
        rcases hf with rfl | hf
        · exact Or.inl (of_decide_eq_true h)
        · exact Or.inr ⟨f, mem_cons.mpr (Or.inr hf), rfl⟩
      · simp [addKeyVal, h] at hf
        rcases hf with rfl | hf
        · exact Or.inr ⟨e, List.mem_cons_self, rfl⟩
        · rcases ih f hf with hk | ⟨a, ha, hkey⟩
          · exact Or.inl hk
          · exact Or.inr ⟨a, mem_cons.mpr (Or.inr ha), hkey⟩

private theorem foldl_addKeyVal_eq_append_groups : ∀ (n : Nat) (acc l : List Entry),
    l.length = n →
    (l.map Prod.fst).Pairwise (fun a b => a ≤ b) →
    (∀ a ∈ acc, ∀ e ∈ l, a.1 < e.1) →
    l.foldl addKeyVal acc = acc ++ l.foldl addKeyVal [] := by
  intro n
  refine Nat.strongRecOn n ?_
  intro n ih acc l hlen hmono hsep
  cases l with
  | nil => simp
  | cons x xs =>
      let k := x.1
      rcases hrunSplit : runSplit k (x :: xs) with ⟨r, s⟩
      have hsplit : r ++ s = x :: xs := by simpa [hrunSplit] using runSplit_append k (x :: xs)
      have hrun : r ≠ [] := by simpa [hrunSplit, k] using runSplit_nonempty_of_same x xs
      have hrkey : ∀ e ∈ r, e.1 = k := by simpa [hrunSplit] using runSplit_all_key k (x :: xs)
      have hslt : s.length < n := by
        have hrl : 0 < r.length := length_pos_iff.mpr hrun
        rw [← hsplit, length_append] at hlen
        omega
      have hfold : (x :: xs).foldl addKeyVal acc =
          s.foldl addKeyVal (addKeyVal acc (k, sumVals r)) := by
        rw [← hsplit, foldl_append, foldl_addKeyVal_key r acc k hrun hrkey]
      have hmono_s : (s.map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        have hm : ((r ++ s).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
          simpa [← hsplit] using hmono
        obtain ⟨_, hsmono, _⟩ := pairwise_append.mp (by simpa using hm)
        exact hsmono
      have hskeys : ∀ e ∈ s, k < e.1 := by
        cases s with
        | nil => intro e he; exact False.elim (not_mem_nil he)
        | cons e0 s' =>
            have hne0 : e0.1 ≠ k := by
              have hrest := runSplit_rest k (x :: xs)
              rw [hrunSplit] at hrest
              simpa using hrest
            have he0xs : e0 ∈ xs := by
              have he0l : e0 ∈ x :: xs := by
                rw [← hsplit]
                exact mem_append.mpr (Or.inr List.mem_cons_self)
              rcases mem_cons.mp he0l with hEq | hmem
              · exact False.elim (hne0 (by simpa [k] using congrArg Prod.fst hEq))
              · exact hmem
            have hk_le : k ≤ e0.1 :=
              (pairwise_cons.mp hmono).1 e0.1 (List.mem_map.mpr ⟨e0, he0xs, rfl⟩)
            have hklt : k < e0.1 := by omega
            intro e he
            rcases mem_cons.mp he with rfl | he
            · exact hklt
            · have hle : e0.1 ≤ e.1 :=
                rel_of_pairwise_cons hmono_s (List.mem_map.mpr ⟨e, he, rfl⟩)
              omega
      have hsep' : ∀ a ∈ addKeyVal acc (k, sumVals r), ∀ e ∈ s, a.1 < e.1 := by
        intro a ha e he
        rcases keys_addKeyVal acc k (sumVals r) a ha with hak | ⟨f, hf, hfa⟩
        · rw [hak]
          exact hskeys e he
        · have hfl : e ∈ x :: xs := by
            rw [← hsplit]
            exact mem_append.mpr (Or.inr he)
          have hf_lt := hsep f hf e hfl
          omega
      have ih0 := ih s.length hslt [(k, sumVals r)] s rfl hmono_s (by
        intro a ha e he
        simp at ha
        subst a
        exact hskeys e he)
      have hfold0 : (x :: xs).foldl addKeyVal [] = (k, sumVals r) :: s.foldl addKeyVal [] := by
        rw [← hsplit, foldl_append, foldl_addKeyVal_key r [] k hrun hrkey]
        simpa [addKeyVal] using ih0
      have ih' := ih s.length hslt (addKeyVal acc (k, sumVals r)) s rfl hmono_s hsep'
      have hknew : ∀ a ∈ acc, a.1 ≠ k := by
        intro a ha hak
        have hlt := hsep a ha x List.mem_cons_self
        rw [hak] at hlt
        omega
      rw [hfold, hfold0, ih']
      rw [addKeyVal_append_new acc k (sumVals r) hknew]
      simp [append_assoc]

-- Online grouping: the closed groups plus the open group are exactly the
-- `addKeyVal` grouping of the prefix consumed so far.

private def OnlineGood (p : List Entry) (s : List Entry × Option Entry) : Prop :=
  let (acc, cur) := s
  acc ++ Option.toList cur = p.foldl addKeyVal [] ∧
  (acc.map Prod.fst).Pairwise (fun a b => a < b) ∧
  (∀ e ∈ acc, ∀ c ∈ Option.toList cur, e.1 < c.1) ∧
  (match cur with
   | none => p = []
   | some (ck, _) => (∀ e ∈ p, e.1 ≤ ck) ∧ ∃ e ∈ p, e.1 = ck)

private def combineAll (acc : List Entry) (cur : Option Entry) (e : Entry) : List Entry × Option Entry :=
  match cur with
  | none => (acc, some e)
  | some (ck, cv) =>
      if e.1 == ck then (acc, some (ck, cv + e.2))
      else (acc ++ [(ck, cv)], some e)

private def combineStep (s : List Entry × Option Entry) (e : Entry) : List Entry × Option Entry :=
  combineAll s.1 s.2 e

/-- Adding one (already grouped) entry whose key is not smaller than any
key of the processed prefix preserves the online invariant. -/
private theorem combineGroup_good {p q : List Entry} {s : List Entry × Option Entry}
    (hs : OnlineGood p s) (g : Entry)
    (hmono : (q.map Prod.fst).Pairwise (fun a b => a ≤ b))
    (hsum : q.foldl addKeyVal [] = addKeyVal (p.foldl addKeyVal []) g)
    (hcp : ∀ e ∈ p, e.1 ≤ g.1)
    (hkeys : ∀ e ∈ q, e.1 ≤ g.1) (hwit : ∃ e ∈ q, e.1 = g.1) :
    OnlineGood q (combineAll s.1 s.2 g) := by
  rcases g with ⟨g1, g2⟩
  rcases s with ⟨acc, cur⟩
  dsimp [OnlineGood] at hs
  rcases hs with ⟨h_eq, hpair, hcross, hcur⟩
  cases cur with
  | none =>
      simp at h_eq hcur
      subst p
      subst acc
      simp [addKeyVal] at hsum
      simp [combineAll]
      dsimp [OnlineGood]
      refine ⟨?_, by simp, by simp, ?_⟩
      · simpa using hsum.symm
      · exact ⟨hkeys, hwit⟩
  | some ckcv =>
      rcases ckcv with ⟨ck, cv⟩
      have h_eq' : acc ++ [(ck, cv)] = p.foldl addKeyVal [] := by simpa using h_eq
      have hcur' : (∀ e ∈ p, e.1 ≤ ck) ∧ ∃ e ∈ p, e.1 = ck := by simpa using hcur
      have hsum' : q.foldl addKeyVal [] = addKeyVal (acc ++ [(ck, cv)]) (g1, g2) := by
        rw [hsum, ← h_eq']
      have hcp_ck : ck ≤ g1 := by
        obtain ⟨e, he, heq⟩ := hcur'.2
        rw [← heq]
        exact hcp e he
      by_cases heqkey : g1 == ck
      · have hgck : g1 = ck := of_decide_eq_true heqkey
        have hnot : ∀ a ∈ acc, a.1 ≠ ck := by
          intro a ha hac
          have hlt := hcross a ha (ck, cv) (by simp)
          omega
        have hadd : addKeyVal (acc ++ [(ck, cv)]) (g1, g2) = acc ++ [(ck, cv + g2)] := by
          rw [hgck]
          exact addKeyVal_update_last acc ck cv g2 hnot
        have hsum2 : q.foldl addKeyVal [] = acc ++ [(ck, cv + g2)] := by
          rw [hsum']
          simpa using hadd
        simp [combineAll, heqkey]
        dsimp [OnlineGood]
        refine ⟨?_, hpair, ?_, ?_⟩
        · simpa using hsum2.symm
        · simpa using hcross
        · refine ⟨?_, ?_⟩
          · intro e he
            have := hkeys e he
            omega
          · obtain ⟨e, he, heq⟩ := hwit
            exact ⟨e, he, by omega⟩
      · have hne : g1 ≠ ck := by
          intro h
          apply heqkey
          simp [h]
        have hcklt : ck < g1 := by omega
        have hnot : ∀ a ∈ acc ++ [(ck, cv)], a.1 ≠ g1 := by
          intro a ha haeq
          simp at ha
          rcases ha with ha | rfl
          · have hlt := hcross a ha (ck, cv) (by simp)
            omega
          · exact hne haeq.symm
        have hadd := addKeyVal_append_new (acc ++ [(ck, cv)]) g1 g2 hnot
        have hsum2 : q.foldl addKeyVal [] = (acc ++ [(ck, cv)]) ++ [(g1, g2)] := by
          rw [hsum']
          simpa using hadd
        simp [combineAll, heqkey]
        dsimp [OnlineGood]
        refine ⟨?_, ?_, ?_, ?_⟩
        · simpa [append_assoc] using hsum2.symm
        · have hp : ((acc.map Prod.fst) ++ [ck]).Pairwise (fun a b => a < b) := by
            apply pairwise_append.mpr
            refine ⟨hpair, by simp, ?_⟩
            intro a ha b hb
            simp at hb
            subst b
            obtain ⟨e, heacc, heq⟩ := List.mem_map.mp ha
            rw [← heq]
            exact hcross e heacc (ck, cv) (by simp)
          simpa using hp
        · intro e he c hc
          simp at hc
          subst c
          simp at he
          rcases he with he | he
          · have hlt := hcross e he (ck, cv) (by simp)
            omega
          · rcases he with rfl
            exact hcklt
        · exact ⟨hkeys, hwit⟩

private theorem onlineFold_good_gen : ∀ (es : List Entry) (p : List Entry)
    (s0 : List Entry × Option Entry),
    OnlineGood p s0 → ((p ++ es).map Prod.fst).Pairwise (fun a b => a ≤ b) →
    OnlineGood (p ++ es) (es.foldl combineStep s0) := by
  intro es
  induction es with
  | nil =>
      intro p s0 hs hmono
      simpa using hs
  | cons e rest ih =>
      intro p s0 hs hmono
      rw [foldl_cons]
      have hmono1 : ((p ++ [e]).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        have hm : (((p ++ [e]) ++ rest).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
          simpa [append_assoc] using hmono
        exact (pairwise_append.mp (by simpa using hm)).1
      obtain ⟨_, _, hcross1⟩ := pairwise_append.mp (by simpa using hmono1)
      have hcp1 : ∀ x ∈ p, x.1 ≤ e.1 := by
        intro x hx
        exact hcross1 x.1 (List.mem_map.mpr ⟨x, hx, rfl⟩) e.1 (by simp)
      have hkeys1 : ∀ x ∈ p ++ [e], x.1 ≤ e.1 := by
        intro x hx
        rcases mem_append.mp hx with hx | hx
        · exact hcp1 x hx
        · simp at hx
          subst x
          omega
      have hwit1 : ∃ x ∈ p ++ [e], x.1 = e.1 := ⟨e, mem_append.mpr (Or.inr (by simp)), rfl⟩
      have hs1 : OnlineGood (p ++ [e]) (combineStep s0 e) := by
        dsimp [combineStep]
        apply combineGroup_good hs e hmono1
        · rw [foldl_append]
          rfl
        · exact hcp1
        · exact hkeys1
        · exact hwit1
      have hrestmono : ((p ++ [e] ++ rest).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        simpa [append_assoc] using hmono
      simpa [append_assoc] using ih (p ++ [e]) (combineStep s0 e) hs1 hrestmono

private theorem online_group (chunks : List (List Entry))
    (hmono : (chunks.flatten.map Prod.fst).Pairwise (fun a b => a ≤ b)) :
    OnlineGood chunks.flatten (chunks.flatten.foldl combineStep ([], none)) := by
  exact onlineFold_good_gen chunks.flatten [] ([], none) (by
    dsimp [OnlineGood]
    simp) (by simpa using hmono)

-- Worker-local streaming: first group, current group, closed groups.

private def workerStep (s : Option Entry × Option Entry × List Entry) (e : Entry) :
    Option Entry × Option Entry × List Entry :=
  let (first, cur, closed) := s
  match cur with
  | none => (none, some e, closed)
  | some (ck, cv) =>
      if e.1 == ck then (first, some (ck, cv + e.2), closed)
      else match first with
           | none => (some (ck, cv), some e, closed)
           | some _ => (first, some e, closed ++ [(ck, cv)])

private def WorkerGood' (p : List Entry) (first : Option Entry) (cur : Option Entry)
    (closed : List Entry) : Prop :=
  Option.toList first ++ closed ++ Option.toList cur = p.foldl addKeyVal [] ∧
  (Option.toList first ++ closed ++ Option.toList cur).Pairwise (fun a b => a.1 < b.1) ∧
  (first = none → closed = []) ∧
  (match cur with
   | none => p = []
   | some (ck, _) => (∀ e ∈ p, e.1 ≤ ck) ∧ ∃ e ∈ p, e.1 = ck)

private def WorkerGood (p : List Entry) (s : Option Entry × Option Entry × List Entry) : Prop :=
  WorkerGood' p s.1 s.2.1 s.2.2

private theorem pairwise_append_gt (l : List Entry) (hl : l.Pairwise (fun a b => a.1 < b.1))
    (e : Entry) (hgt : ∀ x ∈ l, x.1 < e.1) :
    (l ++ [e]).Pairwise (fun a b => a.1 < b.1) := by
  apply pairwise_append.mpr
  refine ⟨hl, by simp, ?_⟩
  intro a ha b hb
  simp at hb
  subst b
  exact hgt a ha

private theorem workerStep_good (first cur : Option Entry) (closed : List Entry) (e : Entry)
    {p : List Entry} (hs : WorkerGood' p first cur closed)
    (hmono : ((p ++ [e]).map Prod.fst).Pairwise (fun a b => a ≤ b)) :
    WorkerGood' (p ++ [e]) (workerStep (first, cur, closed) e).1
      (workerStep (first, cur, closed) e).2.1 (workerStep (first, cur, closed) e).2.2 := by
  rcases hs with ⟨h_eq, hpair, hfirst, hcur⟩
  cases cur with
  | none =>
      subst p
      have hfn : first = none := by
        cases first with
        | none => rfl
        | some x => simp at h_eq
      subst first
      simp at h_eq
      subst closed
      simp [workerStep]
      refine ⟨?_, ?_, ?_, ?_⟩
      · simp [addKeyVal]
      · simp
      · intro h; rfl
      · refine ⟨?_, ?_⟩
        · intro x hx; simp at hx; subst x; omega
        · exact ⟨e, by simp, rfl⟩
  | some ckcv =>
      rcases ckcv with ⟨ck, cv⟩
      have h_eq' : Option.toList first ++ closed ++ [(ck, cv)] = p.foldl addKeyVal [] := h_eq
      obtain ⟨_, _, hcross_mono⟩ := pairwise_append.mp (by simpa using hmono)
      have hcp_e : ck ≤ e.1 := by
        obtain ⟨x, hx, hxe⟩ := hcur.2
        rw [← hxe]
        exact hcross_mono x.1 (List.mem_map.mpr ⟨x, hx, rfl⟩) e.1 (by simp)
      have hkeys_mono : ∀ x ∈ p ++ [e], x.1 ≤ e.1 := by
        intro x hx
        rcases mem_append.mp hx with hx | hx
        · exact hcross_mono x.1 (List.mem_map.mpr ⟨x, hx, rfl⟩) e.1 (by simp)
        · simp at hx; subst x; omega
      have hwit_e : ∃ x ∈ p ++ [e], x.1 = e.1 := ⟨e, mem_append.mpr (Or.inr (by simp)), rfl⟩
      have hcross_keys : ∀ x ∈ Option.toList first ++ closed, x.1 < ck := by
        have hp : ((Option.toList first ++ closed) ++ [(ck, cv)]).Pairwise (fun a b => a.1 < b.1) := by
          simpa [append_assoc] using hpair
        obtain ⟨_, _, hc⟩ := pairwise_append.mp hp
        intro x hx
        exact hc x hx (ck, cv) (by simp)
      by_cases heqkey : e.1 == ck
      · have heqck : e.1 = ck := of_decide_eq_true heqkey
        have hnot : ∀ a ∈ Option.toList first ++ closed, a.1 ≠ ck := by
          intro a ha hac
          have hlt := hcross_keys a ha
          omega
        have hadd : addKeyVal (Option.toList first ++ closed ++ [(ck, cv)]) e =
            Option.toList first ++ closed ++ [(ck, cv + e.2)] := by
          rw [show e = (e.1, e.2) by cases e; rfl, heqck]
          simpa [append_assoc] using addKeyVal_update_last (Option.toList first ++ closed) ck cv e.2 hnot
        have hsum2 : first.toList ++ closed ++ [(ck, cv + e.2)] = (p ++ [e]).foldl addKeyVal [] := by
          rw [foldl_append]
          change _ = addKeyVal (p.foldl addKeyVal []) e
          rw [← h_eq']
          simpa using hadd.symm
        simp [workerStep, heqkey]
        change WorkerGood' (p ++ [e]) first (some (ck, cv + e.2)) closed
        dsimp [WorkerGood']
        refine ⟨?_, ?_, hfirst, ?_⟩
        · simpa using hsum2
        · have hpold : ((first.toList ++ closed) ++ [(ck, cv)]).Pairwise (fun a b => a.1 < b.1) := by
            simpa [append_assoc] using hpair
          obtain ⟨hbase, _, hc⟩ := pairwise_append.mp hpold
          have hc' : ∀ a ∈ first.toList ++ closed, ∀ b ∈ [(ck, cv + e.2)], a.1 < b.1 := by
            intro a ha b hb
            simp at hb
            subst b
            exact hc a ha (ck, cv) (by simp)
          have hpnew : ((first.toList ++ closed) ++ [(ck, cv + e.2)]).Pairwise (fun a b => a.1 < b.1) :=
            pairwise_append.mpr ⟨hbase, by simp, hc'⟩
          simpa [append_assoc] using hpnew
        · refine ⟨?_, ?_⟩
          · intro x hx
            rcases mem_append.mp hx with hxp | hxe
            · have hle := hcur.1 x hxp
              omega
            · simp at hxe
              subst x
              omega
          · obtain ⟨x, hx, hxe⟩ := hwit_e
            exact ⟨x, hx, by omega⟩
      · have hne : e.1 ≠ ck := by
          intro h; apply heqkey; simp [h]
        have hcklt : ck < e.1 := by omega
        have hnot : ∀ a ∈ Option.toList first ++ closed ++ [(ck, cv)], a.1 ≠ e.1 := by
          intro a ha haeq
          have ha' : a ∈ (Option.toList first ++ closed) ++ [(ck, cv)] := by
            simpa [append_assoc] using ha
          rcases mem_append.mp ha' with ha'' | hsing
          · have hlt := hcross_keys a ha''
            omega
          · have haeq' : a = (ck, cv) := by simpa using hsing
            rw [haeq'] at haeq
            exact hne haeq.symm
        have hadd : addKeyVal (Option.toList first ++ closed ++ [(ck, cv)]) e =
            (Option.toList first ++ closed ++ [(ck, cv)]) ++ [e] := by
          rw [show e = (e.1, e.2) by cases e; rfl]
          exact addKeyVal_append_new _ e.1 e.2 hnot
        have hsum2 : (Option.toList first ++ closed ++ [(ck, cv)]) ++ [e] =
            (p ++ [e]).foldl addKeyVal [] := by
          rw [foldl_append]
          change _ = addKeyVal (p.foldl addKeyVal []) e
          rw [← h_eq']
          simpa using hadd.symm
        cases first with
        | none =>
            have hcl : closed = [] := hfirst rfl
            subst closed
            simp [workerStep, heqkey]
            change WorkerGood' (p ++ [e]) (some (ck, cv)) (some e) []
            dsimp [WorkerGood']
            refine ⟨?_, ?_, ?_, ?_⟩
            · simpa using hsum2
            · simpa using pairwise_append_gt [(ck, cv)] (by simp) e (by
                intro x hx; simp at hx; subst x; exact hcklt)
            · intro h; rfl
            · exact ⟨hkeys_mono, hwit_e⟩
        | some f =>
            simp [workerStep, heqkey]
            change WorkerGood' (p ++ [e]) (some f) (some e) (closed ++ [(ck, cv)])
            dsimp [WorkerGood']
            refine ⟨?_, ?_, ?_, ?_⟩
            · simpa [append_assoc] using hsum2
            · have hbase : (Option.toList (some f) ++ closed ++ [(ck, cv)]).Pairwise (fun a b => a.1 < b.1) := hpair
              apply pairwise_append_gt _ hbase e
              intro x hx
              have hx' : x ∈ (Option.toList (some f) ++ closed) ++ [(ck, cv)] := by
                simpa [append_assoc] using hx
              obtain ⟨_, _, hc⟩ := pairwise_append.mp (by
                simpa [append_assoc] using hbase :
                  ((Option.toList (some f) ++ closed) ++ [(ck, cv)]).Pairwise (fun a b => a.1 < b.1))
              rcases mem_append.mp hx' with hxm | hxs
              · have hlt := hc x hxm (ck, cv) (by simp)
                omega
              · have hxs' : x = (ck, cv) := by simpa using hxs
                rw [hxs']
                exact hcklt
            · intro h; exact False.elim (by cases h)
            · exact ⟨hkeys_mono, hwit_e⟩

private theorem workerFold_good_gen : ∀ (es : List Entry) (p : List Entry)
    (s0 : Option Entry × Option Entry × List Entry),
    WorkerGood p s0 → ((p ++ es).map Prod.fst).Pairwise (fun a b => a ≤ b) →
    WorkerGood (p ++ es) (es.foldl workerStep s0) := by
  intro es
  induction es with
  | nil =>
      intro p s0 hs hmono
      simpa using hs
  | cons e rest ih =>
      intro p s0 hs hmono
      rw [foldl_cons]
      have hmono1 : ((p ++ [e]).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        have hm : (((p ++ [e]) ++ rest).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
          simpa [append_assoc] using hmono
        exact (pairwise_append.mp (by simpa using hm)).1
      have hs1 : WorkerGood (p ++ [e]) (workerStep s0 e) := by
        rcases s0 with ⟨f, c, cl⟩
        exact (show WorkerGood' (p ++ [e]) (workerStep (f, c, cl) e).1
          (workerStep (f, c, cl) e).2.1 (workerStep (f, c, cl) e).2.2 from
          workerStep_good f c cl e hs hmono1)
      have hrestmono : ((p ++ [e] ++ rest).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        simpa [append_assoc] using hmono
      simpa [append_assoc] using ih (p ++ [e]) (workerStep s0 e) hs1 hrestmono
-- Bounded worker: its closed list is the top-k of the unbounded one.

private theorem topk_idempotent (k : Nat) (l : List Entry) : topk k (topk k l) = topk k l := by
  unfold topk
  rw [sortBy_eq_self_of_pairwise (pairwise_take (sortBy betterEntry l) (sortBy_pairwise l) k)]
  simp [take_take]

private theorem perm_append_comm (l₁ l₂ : List Entry) : l₁ ++ l₂ ~ l₂ ++ l₁ := by
  induction l₁ with
  | nil => simp
  | cons x xs ih =>
      calc (x :: xs) ++ l₂ = x :: (xs ++ l₂) := rfl
        _ ~ x :: (l₂ ++ xs) := Perm.cons x ih
        _ ~ (l₂ ++ xs) ++ [x] := perm_cons_append x (l₂ ++ xs)
        _ = l₂ ++ (xs ++ [x]) := append_assoc _ _ _
        _ ~ l₂ ++ (x :: xs) := Perm.append (Perm.refl l₂) (perm_cons_append x xs).symm

private theorem topk_append_trunc (k : Nat) (l M : List Entry) :
    topk k (topk k l ++ M) = topk k (l ++ M) := by
  calc topk k (topk k l ++ M) = topk k (M ++ topk k l) := topk_cong (perm_append_comm _ _) k
    _ = topk k (M ++ l) := by simpa [topk] using take_sortBy_flatten k [l] M
    _ = topk k (l ++ M) := topk_cong (perm_append_comm _ _) k

private theorem insertTopK_topk_append (k : Nat) (e : Entry) (l : List Entry) :
    insertTopK k e (topk k l) = topk k (topk k l ++ [e]) := by
  calc insertTopK k e (topk k l) = insertTopK k e (topk k (topk k l)) := by rw [topk_idempotent]
    _ = topk k (e :: topk k l) := (topk_insert k e (topk k l)).symm
    _ = topk k (topk k l ++ [e]) := topk_cong (perm_cons_append e (topk k l)) k

private def workerStepTop (k : Nat) (s : Option Entry × Option Entry × List Entry) (e : Entry) :
    Option Entry × Option Entry × List Entry :=
  let (first, cur, closed) := s
  match cur with
  | none => (none, some e, closed)
  | some (ck, cv) =>
      if e.1 == ck then (first, some (ck, cv + e.2), closed)
      else match first with
           | none => (some (ck, cv), some e, closed)
           | some _ => (first, some e, insertTopK k (ck, cv) closed)

private def TopRelated (k : Nat) (sB sU : Option Entry × Option Entry × List Entry) : Prop :=
  sB.1 = sU.1 ∧ sB.2.1 = sU.2.1 ∧ sB.2.2 = topk k sU.2.2

private theorem workerTopStep (k : Nat) (sB sU : Option Entry × Option Entry × List Entry) (e : Entry)
    (hrel : TopRelated k sB sU) : TopRelated k (workerStepTop k sB e) (workerStep sU e) := by
  rcases sB with ⟨fB, cB, clB⟩
  rcases sU with ⟨fU, cU, clU⟩
  dsimp [TopRelated] at hrel ⊢
  rcases hrel with ⟨hf, hc, hcl⟩
  subst fB
  subst cB
  cases cU with
  | none =>
      simp [TopRelated, workerStepTop, workerStep, hcl]
  | some ckcv =>
      rcases ckcv with ⟨ck, cv⟩
      by_cases heq : e.1 == ck
      · simp [TopRelated, workerStepTop, workerStep, heq, hcl]
      · cases fU with
        | none =>
            simp [TopRelated, workerStepTop, workerStep, heq, hcl]
        | some f =>
            simp [TopRelated, workerStepTop, workerStep, heq]
            rw [hcl, insertTopK_topk_append, topk_append_trunc]

private theorem workerTop_fold_gen (k : Nat) : ∀ (es : List Entry)
    (sB sU : Option Entry × Option Entry × List Entry), TopRelated k sB sU →
    TopRelated k (es.foldl (workerStepTop k) sB) (es.foldl workerStep sU) := by
  intro es
  induction es with
  | nil => intro sB sU h; simpa using h
  | cons e es ih =>
      intro sB sU h
      rw [foldl_cons, foldl_cons]
      apply ih
      exact workerTopStep k sB sU e h

private theorem workerTop_spec (k : Nat) (chunk : List Entry) :
    let sB := chunk.foldl (workerStepTop k) (none, none, [])
    let sU := chunk.foldl workerStep (none, none, [])
    TopRelated k sB sU := by
  apply workerTop_fold_gen
  dsimp [TopRelated, topk, sortBy]
  simp

private def seamsOf (f c : Option Entry) : List Entry :=
  match f, c with
  | none, none => []
  | none, some x => [x]
  | some x, none => [x]
  | some x, some y => [x, y]

private def workerGoTop (k : Nat) (first cur : Option Entry) (closed : List Entry) :
    List Entry → WorkerSummary
  | [] => { seams := seamsOf first cur, interiorTop := closed }
  | (key, val) :: rest =>
      match cur with
      | none => workerGoTop k none (some (key, val)) closed rest
      | some (ck, cv) =>
          if key == ck then workerGoTop k first (some (ck, cv + val)) closed rest
          else match first with
               | none => workerGoTop k (some (ck, cv)) (some (key, val)) closed rest
               | some _ => workerGoTop k first (some (key, val)) (insertTopK k (ck, cv) closed) rest

private theorem streamWorker_go_eq_workerGoTop (k : Nat) : ∀ (f c : Option Entry) (cl rest : List Entry),
    streamWorker.go k f c cl rest = workerGoTop k f c cl rest := by
  intro f c cl rest
  induction rest generalizing f c cl with
  | nil =>
      cases f <;> cases c <;> rfl
  | cons e es ih =>
      rcases e with ⟨key, val⟩
      cases c with
      | none => simp [streamWorker.go, workerGoTop, ih]
      | some ckcv =>
          rcases ckcv with ⟨ck, cv⟩
          by_cases h : key == ck
          · simp [streamWorker.go, workerGoTop, h, ih]
          · cases f with
            | none => simp [streamWorker.go, workerGoTop, h, ih]
            | some fv => simp [streamWorker.go, workerGoTop, h, ih]

private theorem streamWorker_eq_workerGoTop (k : Nat) (chunk : List Entry) :
    streamWorker k chunk = workerGoTop k none none [] chunk := by
  exact streamWorker_go_eq_workerGoTop k none none [] chunk

private theorem workerGoTop_fold (k : Nat) : ∀ (f c : Option Entry) (cl : List Entry)
    (rest : List Entry),
    workerGoTop k f c cl rest =
      let s := rest.foldl (workerStepTop k) (f, c, cl)
      { seams := seamsOf s.1 s.2.1, interiorTop := s.2.2 } := by
  intro f c cl rest
  induction rest generalizing f c cl with
  | nil => rfl
  | cons x xs ih =>
      rcases x with ⟨key, val⟩
      cases c with
      | none => simp [workerGoTop, workerStepTop, ih]
      | some ckcv =>
          rcases ckcv with ⟨ck, cv⟩
          by_cases h : key == ck
          · simp [workerGoTop, workerStepTop, h, ih]
          · cases f with
            | none => simp [workerGoTop, workerStepTop, h, ih]
            | some fv => simp [workerGoTop, workerStepTop, h, ih]

private theorem worker_summary_spec (k : Nat) (chunk : List Entry)
    (hmono : (chunk.map Prod.fst).Pairwise (fun a b => a ≤ b)) :
    let sU := chunk.foldl workerStep (none, none, [])
    let sB := chunk.foldl (workerStepTop k) (none, none, [])
    streamWorker k chunk =
      { seams := seamsOf sU.1 sU.2.1, interiorTop := sB.2.2 } ∧
    WorkerGood' chunk sU.1 sU.2.1 sU.2.2 ∧ sB.2.2 = topk k sU.2.2 := by
  have hrel := workerTop_spec k chunk
  have hgood : WorkerGood chunk (chunk.foldl workerStep (none, none, [])) := by
    exact workerFold_good_gen chunk [] (none, none, []) (by simp [WorkerGood, WorkerGood'])
      (by simpa using hmono)
  dsimp at hrel hgood
  refine ⟨?_, hgood, ?_⟩
  · rw [streamWorker_eq_workerGoTop, workerGoTop_fold]
    dsimp
    rw [hrel.1, hrel.2.1]
  · exact hrel.2.2

private theorem foldl_addKeyVal_of_groups : ∀ (n : Nat) (acc c : List Entry),
    c.length = n →
    (c.map Prod.fst).Pairwise (fun a b => a ≤ b) →
    (∀ a ∈ acc, ∀ e ∈ c, a.1 ≤ e.1) →
    c.foldl addKeyVal acc = (c.foldl addKeyVal []).foldl addKeyVal acc := by
  intro n
  refine Nat.strongRecOn n ?_
  intro n ih acc c hlen hmono hsep
  cases c with
  | nil => simp
  | cons x xs =>
      let k := x.1
      rcases hrunSplit : runSplit k (x :: xs) with ⟨r, s⟩
      have hsplit : r ++ s = x :: xs := by simpa [hrunSplit] using runSplit_append k (x :: xs)
      have hrun : r ≠ [] := by simpa [hrunSplit, k] using runSplit_nonempty_of_same x xs
      have hrkey : ∀ e ∈ r, e.1 = k := by simpa [hrunSplit] using runSplit_all_key k (x :: xs)
      have hslt : s.length < n := by
        have hrl : 0 < r.length := length_pos_iff.mpr hrun
        rw [← hsplit, length_append] at hlen
        omega
      have hmono_s : (s.map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        have hm : ((r ++ s).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
          simpa [← hsplit] using hmono
        obtain ⟨_, hs, _⟩ := pairwise_append.mp (by simpa using hm)
        exact hs
      have hskeys : ∀ e ∈ s, k < e.1 := by
        cases s with
        | nil => intro e he; exact False.elim (not_mem_nil he)
        | cons e0 s' =>
            have hne0 : e0.1 ≠ k := by
              have hrest := runSplit_rest k (x :: xs)
              rw [hrunSplit] at hrest
              simpa using hrest
            have he0xs : e0 ∈ xs := by
              have he0l : e0 ∈ x :: xs := by
                rw [← hsplit]
                exact mem_append.mpr (Or.inr List.mem_cons_self)
              rcases mem_cons.mp he0l with hEq | hmem
              · exact False.elim (hne0 (by simpa [k] using congrArg Prod.fst hEq))
              · exact hmem
            have hk_le : k ≤ e0.1 :=
              (pairwise_cons.mp hmono).1 e0.1 (List.mem_map.mpr ⟨e0, he0xs, rfl⟩)
            have hklt : k < e0.1 := by omega
            intro e he
            rcases mem_cons.mp he with rfl | he
            · exact hklt
            · have hle : e0.1 ≤ e.1 :=
                rel_of_pairwise_cons hmono_s (List.mem_map.mpr ⟨e, he, rfl⟩)
              omega
      have hfold : (x :: xs).foldl addKeyVal acc =
          s.foldl addKeyVal (addKeyVal acc (k, sumVals r)) := by
        rw [← hsplit, foldl_append, foldl_addKeyVal_key r acc k hrun hrkey]
      have hsep' : ∀ a ∈ addKeyVal acc (k, sumVals r), ∀ e ∈ s, a.1 ≤ e.1 := by
        intro a ha e he
        rcases keys_addKeyVal acc k (sumVals r) a ha with hak | ⟨f, hf, hfa⟩
        · rw [hak]
          have := hskeys e he
          omega
        · rw [hfa]
          exact hsep f hf e (by
            rw [← hsplit]
            exact mem_append.mpr (Or.inr he))
      have ih0 := foldl_addKeyVal_eq_append_groups s.length [(k, sumVals r)] s rfl hmono_s (by
        intro a ha e he
        simp at ha
        subst a
        have := hskeys e he
        omega)
      have hfold0 : (x :: xs).foldl addKeyVal [] = (k, sumVals r) :: s.foldl addKeyVal [] := by
        rw [← hsplit, foldl_append, foldl_addKeyVal_key r [] k hrun hrkey]
        simpa [addKeyVal] using ih0
      have ih' := ih s.length hslt (addKeyVal acc (k, sumVals r)) s rfl hmono_s hsep'
      rw [hfold, hfold0, foldl_cons, ih']

private theorem group_keys_in_acc : ∀ (acc l : List Entry) (g : Entry),
    g ∈ l.foldl addKeyVal acc → (∃ e ∈ l, e.1 = g.1) ∨ ∃ a ∈ acc, a.1 = g.1 := by
  intro acc l
  induction l generalizing acc with
  | nil => intro g hg; exact Or.inr ⟨g, hg, rfl⟩
  | cons e es ih =>
      intro g hg
      rcases ih (addKeyVal acc e) g hg with ⟨x, hx, hxk⟩ | ⟨a, ha, hag⟩
      · exact Or.inl ⟨x, mem_cons.mpr (Or.inr hx), hxk⟩
      · rcases keys_addKeyVal acc e.1 e.2 a (by simpa using ha) with hae | ⟨a', ha', ha'k⟩
        · exact Or.inl ⟨e, List.mem_cons_self, by omega⟩
        · exact Or.inr ⟨a', ha', by omega⟩

private theorem group_keys_in_list (l : List Entry) (g : Entry) (hg : g ∈ l.foldl addKeyVal []) :
    ∃ e ∈ l, e.1 = g.1 := by
  rcases group_keys_in_acc [] l g hg with h | ⟨a, ha, _⟩
  · exact h
  · exact False.elim (by simpa using ha)

private theorem onlineGood_of_eq_groups {q r : List Entry} {s : List Entry × Option Entry}
    (hgr : q.foldl addKeyVal [] = r.foldl addKeyVal [])
    (hs : OnlineGood q s)
    (hwit : match s.2 with | none => r = [] | some (ck, _) => ∃ e ∈ r, e.1 = ck) :
    OnlineGood r s := by
  rcases s with ⟨acc, cur⟩
  dsimp [OnlineGood] at hs ⊢
  refine ⟨?_, hs.2.1, hs.2.2.1, ?_⟩
  · rw [← hgr]
    exact hs.1
  · cases cur with
    | none => exact hwit
    | some ckcv =>
        rcases ckcv with ⟨ck, cv⟩
        refine ⟨?_, hwit⟩
        intro e he
        obtain ⟨g, hg, hge⟩ := key_survives_mem r e he []
        obtain ⟨x, hxq, hxg⟩ := group_keys_in_list q g (by rw [hgr]; exact hg)
        have hle := hs.2.2.2.1 x hxq
        omega

private theorem processGroups_good : ∀ (G : List Entry) (p : List Entry) (s : List Entry × Option Entry),
    OnlineGood p s → ((p ++ G).map Prod.fst).Pairwise (fun a b => a ≤ b) →
    G.Pairwise (fun a b => a.1 < b.1) →
    (∀ a ∈ p.foldl addKeyVal [], ∀ g ∈ G, a.1 ≤ g.1) →
    OnlineGood (p ++ G) (G.foldl combineStep s) := by
  intro G
  induction G with
  | nil =>
      intro p s hs hmono hpair hsep
      simpa using hs
  | cons g gs ih =>
      intro p s hs hmono hpair hsep
      rw [foldl_cons]
      have hpG : (((p ++ [g]) ++ gs).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        simpa [append_assoc] using hmono
      have hmono1 : ((p ++ [g]).map Prod.fst).Pairwise (fun a b => a ≤ b) :=
        (pairwise_append.mp (by simpa using hpG)).1
      obtain ⟨_, _, hcross1⟩ := pairwise_append.mp (by simpa using hmono1)
      have hcp : ∀ x ∈ p, x.1 ≤ g.1 := by
        intro x hx
        exact hcross1 x.1 (List.mem_map.mpr ⟨x, hx, rfl⟩) g.1 (by simp)
      have hkeys1 : ∀ x ∈ p ++ [g], x.1 ≤ g.1 := by
        intro x hx
        rcases mem_append.mp hx with hx | hx
        · exact hcp x hx
        · simp at hx; subst x; omega
      have hwit1 : ∃ x ∈ p ++ [g], x.1 = g.1 := ⟨g, mem_append.mpr (Or.inr (by simp)), rfl⟩
      have hs1 : OnlineGood (p ++ [g]) (combineStep s g) := by
        dsimp [combineStep]
        apply combineGroup_good hs g hmono1
        · rw [foldl_append]
          rfl
        · exact hcp
        · exact hkeys1
        · exact hwit1
      have hsep2 : ∀ a ∈ (p ++ [g]).foldl addKeyVal [], ∀ x ∈ gs, a.1 ≤ x.1 := by
        intro a ha x hx
        rw [foldl_append] at ha
        change a ∈ [g].foldl addKeyVal (p.foldl addKeyVal []) at ha
        change a ∈ addKeyVal (p.foldl addKeyVal []) g at ha
        rcases keys_addKeyVal (p.foldl addKeyVal []) g.1 g.2 a ha with hak | ⟨b, hb, hba⟩
        · -- a.1 = g.1
          have hgltx : g.1 < x.1 := (pairwise_cons.mp hpair).1 x hx
          omega
        · have hle := hsep b hb x (mem_cons.mpr (Or.inr hx))
          omega
      have hpairgs : gs.Pairwise (fun a b => a.1 < b.1) := (pairwise_cons.mp hpair).2
      have hmonogs : (((p ++ [g]) ++ gs).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        simpa [append_assoc] using hmono
      simpa [append_assoc] using ih (p ++ [g]) (combineStep s g) hs1 hmonogs hpairgs hsep2

private theorem addKeyVal_ne_nil (m : List Entry) (kv : Entry) (h : m ≠ []) : addKeyVal m kv ≠ [] := by
  cases m with
  | nil => contradiction
  | cons e es =>
      by_cases h : e.1 == kv.1 <;> simp [addKeyVal, h]

private theorem foldl_addKeyVal_ne_nil : ∀ (es : List Entry) (acc : List Entry), acc ≠ [] →
    es.foldl addKeyVal acc ≠ [] := by
  intro es
  induction es with
  | nil => intro acc h; exact h
  | cons e es ih =>
      intro acc h
      apply ih
      exact addKeyVal_ne_nil acc e h

-- Coordinator.

private def workerSum (c : List Entry) : Option Entry × Option Entry × List Entry :=
  let s := c.foldl workerStep (none, none, [])
  (s.1, s.2.1, s.2.2)

private def coordIdealStep (s : List Entry × Option Entry)
    (w : Option Entry × Option Entry × List Entry) : List Entry × Option Entry :=
  (Option.toList w.1 ++ w.2.2 ++ Option.toList w.2.1).foldl combineStep s

private theorem coordIdealStep_good {p c : List Entry} {s : List Entry × Option Entry}
    (hs : OnlineGood p s) (hmono_pc : ((p ++ c).map Prod.fst).Pairwise (fun a b => a ≤ b))
    (hmono_c : (c.map Prod.fst).Pairwise (fun a b => a ≤ b))
    (hgood : WorkerGood' c (workerSum c).1 (workerSum c).2.1 (workerSum c).2.2) :
    OnlineGood (p ++ c) (coordIdealStep s (workerSum c)) := by
  let f := (workerSum c).1
  let l := (workerSum c).2.1
  let mid := (workerSum c).2.2
  let G := Option.toList f ++ mid ++ Option.toList l
  have hgr_c : G = c.foldl addKeyVal [] := by
    dsimp [G, f, l, mid, workerSum]
    exact hgood.1
  have hsep_acc : ∀ a ∈ p.foldl addKeyVal [], ∀ g ∈ G, a.1 ≤ g.1 := by
    intro a ha g hg
    obtain ⟨x, hxp, hxa⟩ := group_keys_in_list p a ha
    obtain ⟨y, hyc, hyg⟩ := group_keys_in_list c g (by rw [← hgr_c]; exact hg)
    rw [← hxa, ← hyg]
    obtain ⟨_, _, hcross⟩ := pairwise_append.mp (by simpa using hmono_pc)
    exact hcross x.1 (List.mem_map.mpr ⟨x, hxp, rfl⟩) y.1 (List.mem_map.mpr ⟨y, hyc, rfl⟩)
  have hpairG : G.Pairwise (fun a b => a.1 < b.1) := by
    dsimp [G, f, l, mid]
    simpa using hgood.2.1
  have hpg : ((p ++ G).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
    have hp_p : (p.map Prod.fst).Pairwise (fun a b => a ≤ b) :=
      (pairwise_append.mp (by simpa using hmono_pc)).1
    have hgmap : (G.map Prod.fst).Pairwise (fun a b => a ≤ b) :=
      hpairG.map Prod.fst (fun a b h => by omega)
    have hcrossPG : ∀ x ∈ p.map Prod.fst, ∀ y ∈ G.map Prod.fst, x ≤ y := by
      intro x hx y hy
      obtain ⟨g, hgG, hgy⟩ := List.mem_map.mp hy
      obtain ⟨z, hzc, hzg⟩ := group_keys_in_list c g (by rw [← hgr_c]; exact hgG)
      obtain ⟨e, hep, hex⟩ := List.mem_map.mp hx
      rw [← hex, ← hgy]
      obtain ⟨_, _, hcross⟩ := pairwise_append.mp (by simpa using hmono_pc)
      have hle := hcross e.1 (List.mem_map.mpr ⟨e, hep, rfl⟩) z.1 (List.mem_map.mpr ⟨z, hzc, rfl⟩)
      omega
    have hpAppend := pairwise_append.mpr ⟨hp_p, hgmap, hcrossPG⟩
    simpa using hpAppend
  have hsG : OnlineGood (p ++ G) (G.foldl combineStep s) :=
    processGroups_good G p s hs hpg hpairG hsep_acc
  have hgr_pc : (p ++ c).foldl addKeyVal [] = G.foldl addKeyVal (p.foldl addKeyVal []) := by
    rw [foldl_append]
    rw [foldl_addKeyVal_of_groups c.length (p.foldl addKeyVal []) c rfl hmono_c]
    · rw [← hgr_c]
    · intro a ha e he
      obtain ⟨x, hxp, hxa⟩ := group_keys_in_list p a ha
      rw [← hxa]
      obtain ⟨_, _, hcross⟩ := pairwise_append.mp (by simpa using hmono_pc)
      exact hcross x.1 (List.mem_map.mpr ⟨x, hxp, rfl⟩) e.1 (List.mem_map.mpr ⟨e, he, rfl⟩)
  have hgr_q : (p ++ G).foldl addKeyVal [] = G.foldl addKeyVal (p.foldl addKeyVal []) := by
    rw [foldl_append]
  have hwit : match (G.foldl combineStep s).2 with
    | none => p ++ c = []
    | some (ck, _) => ∃ e ∈ p ++ c, e.1 = ck := by
    have hgrs : (G.foldl combineStep s).1 ++ Option.toList (G.foldl combineStep s).2 =
        (p ++ c).foldl addKeyVal [] := by
      have h1 := hsG.1
      rw [hgr_q, ← hgr_pc] at h1
      exact h1
    cases hX : (G.foldl combineStep s).2 with
    | none =>
        simp [hX]
        have hpGnil : p ++ G = [] := by simpa [hX] using hsG.2.2.2
        have hp_nil : p = [] := by
          have hlen : p.length + G.length = 0 := by
            simpa only [length_append, length_nil] using congrArg List.length hpGnil
          apply List.eq_nil_of_length_eq_zero
          omega
        have hG_nil : G = [] := by
          have hlen : p.length + G.length = 0 := by
            simpa only [length_append, length_nil] using congrArg List.length hpGnil
          apply List.eq_nil_of_length_eq_zero
          omega
        have hc_nil : c = [] := by
          cases c with
          | nil => rfl
          | cons y ys =>
              exfalso
              have hcempty : (y :: ys).foldl addKeyVal [] = [] := by
                rw [← hgr_c, hG_nil]
              obtain ⟨g, hg, hgy⟩ := key_survives_mem (y :: ys) y (by simp) []
              rw [hcempty] at hg
              exact False.elim (not_mem_nil hg)
        simp [hp_nil, hc_nil]
    | some ckcv =>
        rcases ckcv with ⟨ck, cv⟩
        change ∃ e ∈ p ++ c, e.1 = ck
        have hmem : (ck, cv) ∈ (p ++ c).foldl addKeyVal [] := by
          rw [← hgrs, hX]
          simp
        obtain ⟨e, he, heq⟩ := group_keys_in_list (p ++ c) (ck, cv) hmem
        exact ⟨e, he, by omega⟩
  have hconv := onlineGood_of_eq_groups (q := p ++ G) (r := p ++ c)
    (by rw [hgr_q, hgr_pc]) hsG hwit
  change OnlineGood (p ++ c)
    ((Option.toList (workerSum c).1 ++ (workerSum c).2.2 ++ Option.toList (workerSum c).2.1).foldl combineStep s)
  simpa [G, f, l, mid] using hconv

private def idealFold (s : List Entry × Option Entry) (c : List Entry) : List Entry × Option Entry :=
  coordIdealStep s (workerSum c)

private theorem chunk_mono_of_flatten : ∀ (chunks : List (List Entry)),
    (chunks.flatten.map Prod.fst).Pairwise (fun a b => a ≤ b) →
    ∀ c ∈ chunks, (c.map Prod.fst).Pairwise (fun a b => a ≤ b) := by
  intro chunks
  induction chunks with
  | nil => intro h c hc; exact False.elim (not_mem_nil hc)
  | cons x xs ih =>
      intro h c hc
      have hxflat : ((x ++ xs.flatten).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        simpa using h
      obtain ⟨hx, hxs, _⟩ := pairwise_append.mp (by simpa using hxflat)
      rcases mem_cons.mp hc with rfl | hc
      · exact hx
      · exact ih (by simpa using hxs) c hc

private theorem coordIdeal_chunks_good : ∀ (chunks : List (List Entry)) (p : List Entry)
    (s0 : List Entry × Option Entry),
    OnlineGood p s0 → ((p ++ chunks.flatten).map Prod.fst).Pairwise (fun a b => a ≤ b) →
    OnlineGood (p ++ chunks.flatten) (chunks.foldl idealFold s0) := by
  intro chunks
  induction chunks with
  | nil =>
      intro p s0 hs hmono
      simpa using hs
  | cons c cs ih =>
      intro p s0 hs hmono
      rw [foldl_cons]
      have hcsmono : ((c ++ cs.flatten).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        have hpflat : (p.map Prod.fst ++ (c ++ cs.flatten).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
          simpa using hmono
        exact (pairwise_append.mp (by simpa using hpflat)).2.1
      have hcmono : (c.map Prod.fst).Pairwise (fun a b => a ≤ b) :=
        (pairwise_append.mp (by simpa using hcsmono)).1
      have hmono_pc : ((p ++ c).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        have hall : ((p ++ (c ++ cs.flatten)).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
          simpa [append_assoc] using hmono
        exact (pairwise_append.mp (by simpa using hall)).1
      have hgoodW : WorkerGood' c (workerSum c).1 (workerSum c).2.1 (workerSum c).2.2 := by
        have htmp := workerFold_good_gen c [] (none, none, [])
          (by simp [WorkerGood, WorkerGood']) (by simpa using hcmono)
        simpa [WorkerGood, workerSum] using htmp
      have hs1 := coordIdealStep_good hs hmono_pc hcmono hgoodW
      have hrest : (((p ++ c) ++ cs.flatten).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        simpa [append_assoc] using hmono
      simpa [append_assoc, idealFold] using ih (p ++ c) (idealFold s0 c) hs1 hrest

-- Top-k bookkeeping for the coordinator's bounded operations.

private theorem topk_cons_topk_append (k : Nat) (e : Entry) (l : List Entry) :
    topk k (e :: topk k l) = topk k (l ++ [e]) := by
  calc topk k (e :: topk k l) = insertTopK k e (topk k (topk k l)) := by rw [topk_insert]
    _ = insertTopK k e (topk k l) := by rw [topk_idempotent]
    _ = topk k (topk k l ++ [e]) := insertTopK_topk_append k e l
    _ = topk k (l ++ [e]) := topk_append_trunc k l [e]

private theorem perm_middle (A B : List Entry) (e : Entry) : (A ++ B) ++ [e] ~ A ++ [e] ++ B := by
  calc (A ++ B) ++ [e] = A ++ (B ++ [e]) := append_assoc _ _ _
    _ ~ A ++ ([e] ++ B) := Perm.append (Perm.refl A) (perm_append_comm B [e])
    _ = A ++ [e] ++ B := (append_assoc _ _ _).symm

private theorem topk_append_topk' (k : Nat) (A B : List Entry) :
    topk k (A ++ topk k B) = topk k (A ++ B) := by
  calc topk k (A ++ topk k B) = topk k (topk k B ++ A) := topk_cong (perm_append_comm _ _) k
    _ = topk k (B ++ A) := topk_append_trunc k B A
    _ = topk k (A ++ B) := topk_cong (perm_append_comm _ _) k

private theorem insertTopK_topk_mid (k : Nat) (e : Entry) (A B : List Entry) :
    insertTopK k e (topk k (A ++ B)) = topk k (A ++ [e] ++ B) := by
  rw [insertTopK_topk_append, topk_append_trunc]
  exact topk_cong (perm_middle A B e) k

private theorem combineAll_key (acc : List Entry) (cur : Option Entry) (g : Entry) :
    match (combineAll acc cur g).2 with | none => False | some c => c.1 = g.1 := by
  cases cur with
  | none => simp [combineAll]
  | some c =>
      by_cases h : g.1 == c.1
      · simp [combineAll, h, of_decide_eq_true h]
      · simp [combineAll, h]

private theorem combineTwo_topk' (k : Nat) (acc : List Entry) (cur : Option Entry)
    (e : Entry) (mid : List Entry) :
    (combineTwo k cur (topk k (acc ++ mid)) e).2 =
      topk k ((combineAll acc cur e).1 ++ mid) ∧
    (combineTwo k cur (topk k (acc ++ mid)) e).1 = (combineAll acc cur e).2 := by
  rcases cur with _ | ⟨ck, cv⟩
  · simp [combineTwo, combineAll]
  · rcases e with ⟨ek, ev⟩
    constructor
    · by_cases h : ek == ck
      · have heq : ck = ek := (of_decide_eq_true h).symm
        simp [combineTwo, combineAll, h, heq]
      · have hne : ek ≠ ck := by intro he; apply h; simp [he]
        have hne' : ck ≠ ek := fun hx => hne hx.symm
        simp [combineTwo, combineAll, h, hne']
        rw [insertTopK_topk_append, topk_append_trunc]
        simpa using topk_cong (perm_middle acc mid (ck, cv)) k
    · by_cases h : ek == ck
      · have heq : ck = ek := (of_decide_eq_true h).symm
        simp [combineTwo, combineAll, h, heq]
      · have hne : ek ≠ ck := by intro he; apply h; simp [he]
        have hne' : ck ≠ ek := fun hx => hne hx.symm
        simp [combineTwo, combineAll, h, hne']

private theorem combineStep_fold_mid_last (acc : List Entry) (cur : Option Entry)
    (mid : List Entry) (lg : Entry)
    (hG : (mid ++ [lg]).Pairwise (fun a b => a.1 < b.1))
    (hgt : ∀ g ∈ mid ++ [lg], ∀ cc, cur = some cc → cc.1 < g.1) :
    let s' := (mid ++ [lg]).foldl combineStep (acc, cur)
    s'.1 = acc ++ Option.toList cur ++ mid ∧ s'.2 = some lg := by
  induction mid generalizing acc cur with
  | nil =>
      dsimp
      cases cur with
      | none => simp [combineStep, combineAll]
      | some c =>
          have hlt := hgt lg (by simp) c rfl
          have hkey : lg.1 ≠ c.1 := by omega
          by_cases h : lg.1 == c.1
          · exact False.elim (hkey (of_decide_eq_true h))
          · simp [combineStep, combineAll, h, append_assoc]
  | cons m ms ih =>
      dsimp
      have hGtail : (ms ++ [lg]).Pairwise (fun a b => a.1 < b.1) := (pairwise_cons.mp hG).2
      have hfirstlt : ∀ z ∈ ms ++ [lg], m.1 < z.1 := by
        intro z hz
        exact (pairwise_cons.mp hG).1 z hz
      cases hcur : cur with
      | none =>
          have hm : combineStep (acc, none) m = (acc, some m) := by simp [combineStep, combineAll]
          rw [hm]
          have hgt' : ∀ z ∈ ms ++ [lg], ∀ cc, (some m : Option Entry) = some cc → cc.1 < z.1 := by
            intro z hz cc hcc
            cases hcc
            exact hfirstlt z hz
          have ih' := ih acc (some m) hGtail hgt'
          dsimp at ih'
          simpa [combineStep, combineAll, append_assoc] using ih'
      | some c =>
          have hltc := hgt m (by simp) c hcur
          have hkey : m.1 ≠ c.1 := by omega
          by_cases heq : m.1 == c.1
          · exact False.elim (hkey (of_decide_eq_true heq))
          · have hm : combineStep (acc, some c) m = (acc ++ [c], some m) := by
              simp [combineStep, combineAll, heq]
            rw [hm]
            have hgt' : ∀ z ∈ ms ++ [lg], ∀ cc, (some m : Option Entry) = some cc → cc.1 < z.1 := by
              intro z hz cc hcc
              cases hcc
              exact hfirstlt z hz
            have ih' := ih (acc ++ [c]) (some m) hGtail hgt'
            dsimp at ih'
            simpa [combineStep, combineAll, append_assoc] using ih'
private def coordBoundStep (k : Nat) (s : Option Entry × List Entry) (w : WorkerSummary) :
    Option Entry × List Entry :=
  let (curOpt, topAcc) := s
  let top1 := mergeTopK k topAcc w.interiorTop
  match w.seams with
  | [] => (curOpt, top1)
  | [single] => combineTwo k curOpt top1 single
  | f :: l :: _ =>
      let (midOpt, top2) := combineTwo k curOpt top1 f
      let top3 := match midOpt with | none => top2 | some m => insertTopK k m top2
      (some l, top3)

private def boundFold (k : Nat) (s : Option Entry × List Entry) (c : List Entry) : Option Entry × List Entry :=
  coordBoundStep k s (streamWorker k c)

private theorem coordMerge_eq_fold (k : Nat) (workers : List WorkerSummary) :
    coordMerge k workers =
      let (tail, top) := workers.foldl (coordBoundStep k) (none, [])
      match tail with | none => top | some t => insertTopK k t top := by
  rfl

private theorem boundChunk_step (k : Nat) (p c : List Entry)
    (sIdeal : List Entry × Option Entry) (sBound : Option Entry × List Entry)
    (hsIdeal : OnlineGood p sIdeal)
    (hb : sBound.2 = topk k sIdeal.1 ∧ sBound.1 = sIdeal.2)
    (hcmono : (c.map Prod.fst).Pairwise (fun a b => a ≤ b)) :
    let si' := idealFold sIdeal c
    let sb' := boundFold k sBound c
    sb'.2 = topk k si'.1 ∧ sb'.1 = si'.2 := by
  let ws := workerSum c
  have hws : workerSum c = ws := rfl
  have hspec0 := worker_summary_spec k c hcmono
  dsimp at hspec0
  have hspec' : streamWorker k c =
      { seams := seamsOf ws.1 ws.2.1, interiorTop := topk k ws.2.2 } ∧
      WorkerGood' c ws.1 ws.2.1 ws.2.2 := by
    constructor
    · rw [hspec0.1, hspec0.2.2]
      rfl
    · simpa [ws, workerSum] using hspec0.2.1
  dsimp [idealFold, coordIdealStep, boundFold]
  rw [hws]
  rw [hspec'.1]
  dsimp [seamsOf]
  cases hwf : ws.1 with
  | none =>
      cases hwl : ws.2.1 with
      | none =>
          have hmid : ws.2.2 = [] := hspec'.2.2.2.1 hwf
          simp [hwf, hwl, hmid, coordBoundStep, mergeTopK, hb]
          change topk k (topk k sIdeal.1 ++ topk k ([] : List Entry)) = topk k sIdeal.1
          rw [topk_append_topk', append_nil]
          exact topk_idempotent k sIdeal.1
      | some lg =>
          have hmid : ws.2.2 = [] := hspec'.2.2.2.1 hwf
          simp [hwf, hwl, hmid, coordBoundStep, mergeTopK, hb]
          change (combineTwo k sIdeal.2 (topk k (topk k sIdeal.1 ++ topk k ([] : List Entry))) lg).2 =
              topk k (combineStep sIdeal lg).1 ∧
            (combineTwo k sIdeal.2 (topk k (topk k sIdeal.1 ++ topk k ([] : List Entry))) lg).1 =
              (combineStep sIdeal lg).2
          rw [show topk k (topk k sIdeal.1 ++ topk k ([] : List Entry)) = topk k sIdeal.1 by
            rw [topk_append_topk', append_nil]
            exact topk_idempotent k sIdeal.1]
          have hct := combineTwo_topk' k sIdeal.1 sIdeal.2 lg []
          simpa [combineStep, combineAll] using hct
      -- `none` has only two alternatives
  | some f0 =>
      cases hwl : ws.2.1 with
      | none =>
          have hcempty : c = [] := by simpa [hwl] using hspec'.2.2.2.2
          have hfnone : ws.1 = none := by
            dsimp [ws, workerSum]
            rw [hcempty]
            simp [workerStep]
          rw [hwf] at hfnone
          cases hfnone
      | some lg =>
          have hGpair : ([f0] ++ ws.2.2 ++ [lg]).Pairwise (fun a b => a.1 < b.1) := by
            simpa [hwf, hwl] using hspec'.2.2.1
          let hfirst : List Entry × Option Entry := combineAll sIdeal.1 sIdeal.2 f0
          let hcurA : Option Entry := hfirst.2
          let haccA : List Entry := hfirst.1
          have hmidpair : (ws.2.2 ++ [lg]).Pairwise (fun a b => a.1 < b.1) := (pairwise_cons.mp hGpair).2
          have hkeyA : match hcurA with | none => False | some cc => cc.1 = f0.1 := by
            dsimp [hcurA, hfirst]
            exact combineAll_key sIdeal.1 sIdeal.2 f0
          have hgtmid' : ∀ g ∈ ws.2.2 ++ [lg], ∀ cc, hcurA = some cc → cc.1 < g.1 := by
            intro g hg cc hcc
            have hk := hkeyA
            rw [hcc] at hk
            have hlt_f0 : f0.1 < g.1 := (pairwise_cons.mp hGpair).1 g hg
            omega
          have hgtmid := hgtmid'
          have hfold := combineStep_fold_mid_last haccA hcurA ws.2.2 lg hmidpair hgtmid
          dsimp at hfold
          -- bounded side
          simp [hwf, hwl, coordBoundStep, mergeTopK]
          rw [hb.1]
          have htop1 : take k (sortBy betterEntry (topk k sIdeal.1 ++ topk k ws.2.2)) =
              topk k (sIdeal.1 ++ ws.2.2) := by
            change topk k (topk k sIdeal.1 ++ topk k ws.2.2) = topk k (sIdeal.1 ++ ws.2.2)
            rw [topk_append_trunc k sIdeal.1 (topk k ws.2.2)]
            exact topk_append_topk' k sIdeal.1 ws.2.2
          rw [htop1]
          rw [hb.2]
          have hct := combineTwo_topk' k sIdeal.1 sIdeal.2 f0 ws.2.2
          rw [hct.1]
          obtain ⟨m, hm⟩ : ∃ m, (combineTwo k sIdeal.2 (topk k (sIdeal.1 ++ ws.2.2)) f0).1 = some m := by
            cases sIdeal.2 with
            | none => exact ⟨f0, by simp [combineTwo]⟩
            | some c =>
                by_cases hkey : f0.1 == c.1
                · have heq : c.1 = f0.1 := (of_decide_eq_true hkey).symm
                  exact ⟨(c.1, c.2 + f0.2), by simp [combineTwo, hkey, heq]⟩
                · have hne : c.1 ≠ f0.1 := by intro he; apply hkey; simp [he]
                  exact ⟨f0, by simp [combineTwo, hkey, hne]⟩
          rw [hm]
          simp
          rw [insertTopK_topk_mid]
          have hideal : (combineStep (foldl combineStep (combineStep sIdeal f0) ws.2.2) lg).1 =
              haccA ++ Option.toList hcurA ++ ws.2.2 := by
            change (foldl combineStep (foldl combineStep (combineStep sIdeal f0) ws.2.2) [lg]).1 =
              haccA ++ Option.toList hcurA ++ ws.2.2
            rw [← foldl_append]
            rw [show combineStep sIdeal f0 = (hfirst.1, hfirst.2) by rfl]
            simpa [haccA, hcurA] using hfold.1
          have hideal2 : (combineStep (foldl combineStep (combineStep sIdeal f0) ws.2.2) lg).2 = some lg := by
            change (foldl combineStep (foldl combineStep (combineStep sIdeal f0) ws.2.2) [lg]).2 = some lg
            rw [← foldl_append]
            rw [show combineStep sIdeal f0 = (hfirst.1, hfirst.2) by rfl]
            simpa [haccA, hcurA] using hfold.2
          rw [hideal, hideal2]
          have hcurA_def : (combineAll sIdeal.1 sIdeal.2 f0).2 = hcurA := rfl
          rcases hcurA with _ | cc
          · exact False.elim hkeyA
          · have hmcc : m = cc := by
              have h' : some m = some cc := by rw [← hm, hct.2, hcurA_def]
              cases h'
              rfl
            subst m
            have haccA_def : haccA = (combineAll sIdeal.1 sIdeal.2 f0).1 := rfl
            rw [← haccA_def]
            simp
private theorem boundFold_topk : ∀ (chunks : List (List Entry)) (p : List Entry)
    (sIdeal : List Entry × Option Entry) (sBound : Option Entry × List Entry),
    OnlineGood p sIdeal →
    sBound.2 = topk k sIdeal.1 ∧ sBound.1 = sIdeal.2 →
    ((p ++ chunks.flatten).map Prod.fst).Pairwise (fun a b => a ≤ b) →
    (∀ c ∈ chunks, (c.map Prod.fst).Pairwise (fun a b => a ≤ b)) →
    (chunks.foldl (boundFold k) sBound).2 = topk k (chunks.foldl idealFold sIdeal).1 ∧
    (chunks.foldl (boundFold k) sBound).1 = (chunks.foldl idealFold sIdeal).2 := by
  intro chunks
  induction chunks with
  | nil =>
      intro p si sb hs hb hmono hcs
      simpa using hb
  | cons c cs ih =>
      intro p si sb hs hb hmono hcs
      rw [foldl_cons, foldl_cons]
      have hcmono : (c.map Prod.fst).Pairwise (fun a b => a ≤ b) := hcs c (by simp)
      have hmono_pc : ((p ++ c).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        have hall : ((p ++ (c ++ cs.flatten)).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
          simpa [append_assoc] using hmono
        exact (pairwise_append.mp (by simpa using hall)).1
      have hstep := boundChunk_step k p c si sb hs hb hcmono
      dsimp at hstep
      have hsI := coordIdealStep_good hs hmono_pc hcmono (by
        have ht := workerFold_good_gen c [] (none, none, [])
          (by simp [WorkerGood, WorkerGood']) (by simpa using hcmono)
        simpa [WorkerGood, workerSum] using ht)
      have hrestmono : (((p ++ c) ++ cs.flatten).map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        simpa [append_assoc] using hmono
      have hcs' : ∀ x ∈ cs, (x.map Prod.fst).Pairwise (fun a b => a ≤ b) := by
        intro x hx
        exact hcs x (mem_cons.mpr (Or.inr hx))
      simpa [idealFold, boundFold] using ih (p ++ c) (idealFold si c) (boundFold k sb c)
        hsI hstep hrestmono hcs'

private theorem topk_length_le (k : Nat) (l : List Entry) : (topk k l).length ≤ k := by
  unfold topk
  rw [length_take]
  exact Nat.min_le_left _ _

theorem sound : TinylambOpt.ResearchGoals.MonotonicKeyStreamTopK.Goal := by
  intro k chunks hmono
  have hmono_chunks : ∀ c ∈ chunks, (c.map Prod.fst).Pairwise (fun a b => a ≤ b) :=
    chunk_mono_of_flatten chunks hmono
  constructor
  · have hgoodIdeal := coordIdeal_chunks_good chunks [] ([], none) (by
      dsimp [OnlineGood]
      simp) (by simpa using hmono)
    have hbound := boundFold_topk (k := k) chunks [] ([], none) (none, ([] : List Entry)) (by
      dsimp [OnlineGood]
      simp) (by simp [topk, sortBy]) (by simpa using hmono) hmono_chunks
    rw [optStreamTopK]
    -- coordMerge over streamWorker summaries equals fold boundFold
    have hcm : coordMerge k (chunks.map (streamWorker k)) =
        let (tail, top) := chunks.foldl (boundFold k) (none, [])
        match tail with | none => top | some t => insertTopK k t top := by
      rw [coordMerge_eq_fold]
      have hf_all : ∀ (cs : List (List Entry)) (s0 : Option Entry × List Entry),
          (cs.map (streamWorker k)).foldl (coordBoundStep k) s0 =
          cs.foldl (boundFold k) s0 := by
        intro cs
        induction cs with
        | nil => intro s0; rfl
        | cons c cs ih => intro s0; simp [boundFold, ih]
      rw [hf_all chunks (none, [])]
    rw [hcm]
    cases htail : (chunks.foldl (boundFold k) (none, [])).1 with
    | none =>
        simp [htail]
        rw [hbound.1]
        have hiCur : (chunks.foldl idealFold ([], none)).2 = none := by rw [← hbound.2, htail]
        have hacc : (chunks.foldl idealFold ([], none)).1 = chunks.flatten.foldl addKeyVal [] := by
          have hf := hgoodIdeal.1
          simpa [hiCur] using hf
        unfold topk refTopK
        rw [hacc]
    | some t =>
        simp [htail]
        rw [hbound.1]
        have hiCur : (chunks.foldl idealFold ([], none)).2 = some t := by rw [← hbound.2, htail]
        rw [insertTopK_topk_append]
        rw [topk_append_trunc]
        have hacc : (chunks.foldl idealFold ([], none)).1 ++ [t] =
            chunks.flatten.foldl addKeyVal [] := by
          have hf := hgoodIdeal.1
          simpa [hiCur] using hf
        rw [hacc]
        unfold topk refTopK
        rfl
  · intro chunk hc
    have hmc := hmono_chunks chunk hc
    have hspec := worker_summary_spec k chunk hmc
    dsimp at hspec
    rw [hspec.1]
    simp [seamsOf]
    -- interiorTop = topk k mid
    have htop := hspec.2.2
    rw [htop]
    exact topk_length_le k _

end TinylambOpt.ResearchProofs.MonotonicKeyStreamTopK
