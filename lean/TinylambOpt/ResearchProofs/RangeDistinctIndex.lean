import TinylambOpt.ResearchGoals.RangeDistinctIndex

/-!
Proved

- `Goal` — `∀ xs lo hi, query (build xs) lo hi = reference xs lo hi`

The reference model is defined in `TinylambOpt.ResearchGoals.RangeDistinctIndex`.
-/

namespace TinylambOpt.ResearchProofs.RangeDistinctIndex

open TinylambOpt.ResearchGoals.RangeDistinctIndex

/-! ## Generic helpers for `List` / `Nat` -/

private theorem mem_filterMap_id {xs : List (Option Int)} {y : Int} :
    y ∈ xs.filterMap id ↔ some y ∈ xs := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      cases a with
      | none => simp [ih]
      | some z =>
          rw [show ((some z :: as).filterMap id) = z :: as.filterMap id from rfl]
          rw [List.mem_cons, List.mem_cons]
          constructor
          · rintro (rfl | h)
            · exact Or.inl rfl
            · exact Or.inr (ih.mp h)
          · rintro (h | h)
            · exact Or.inl (Option.some.inj h)
            · exact Or.inr (ih.mpr h)

private theorem filter_eq_self_of_forall {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ a ∈ l, p a = true) : l.filter p = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : p a = true := h a List.mem_cons_self
      have has : ∀ b ∈ as, p b = true := fun b hb => h b (List.mem_cons_of_mem a hb)
      rw [List.filter_cons_of_pos ha, ih has]

private theorem filter_eq_nil_of_forall_not {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ a ∈ l, p a = false) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : ¬ p a := by
        have h' : p a = false := h a List.mem_cons_self
        simp [h']
      have has : ∀ b ∈ as, p b = false := fun b hb => h b (List.mem_cons_of_mem a hb)
      rw [List.filter_cons_of_neg ha, ih has]

private theorem filter_map_commute {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).filter p = (l.filter fun x => p (f x)).map f := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      by_cases hp : p (f a) = true
      · rw [List.map_cons, List.filter_cons_of_pos hp,
          show ((a :: as).filter fun x => p (f x)) = a :: as.filter (fun x => p (f x)) from
            List.filter_cons_of_pos hp,
          List.map_cons, ih]
      · rw [List.map_cons, List.filter_cons_of_neg hp,
          show ((a :: as).filter fun x => p (f x)) = as.filter (fun x => p (f x)) from
            List.filter_cons_of_neg hp,
          ih]

private theorem nodup_map_add {l : List Nat} (h : l.Nodup) : (l.map fun p => p + 1).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      obtain ⟨hna, hd⟩ := List.nodup_cons.mp h
      rw [List.map_cons]
      refine List.nodup_cons.mpr ⟨?_, ih hd⟩
      intro hmem
      obtain ⟨q, hq, heq⟩ := List.mem_map.mp hmem
      have haq : a = q := by omega
      rw [← haq] at hq
      exact hna hq

private theorem nodup_filter {l : List Nat} (h : l.Nodup) (p : Nat → Bool) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      obtain ⟨hna, hd⟩ := List.nodup_cons.mp h
      by_cases hp : p a = true
      · rw [List.filter_cons_of_pos hp]
        refine List.nodup_cons.mpr ⟨?_, ih hd⟩
        intro hmem
        exact hna (List.mem_filter.mp hmem |>.1)
      · rw [List.filter_cons_of_neg hp]
        exact ih hd

private theorem length_eq_of_mem {l₁ : List Nat} (d₁ : l₁.Nodup) :
    ∀ (l₂ : List Nat), l₂.Nodup → (∀ a, a ∈ l₁ ↔ a ∈ l₂) → l₁.length = l₂.length := by
  induction l₁ with
  | nil =>
      intro l₂ _ h
      cases l₂ with
      | nil => rfl
      | cons b bs =>
          exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ d₂ h
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp (@List.mem_cons_self _ a r)
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact absurd rfl hca
          · exact hr
      have hpos : 0 < l₂.length := by
        cases l₂ with
        | nil => exact absurd ha List.not_mem_nil
        | cons b bs => exact Nat.succ_pos _
      have hlen : (l₂.erase a).length + 1 = l₂.length := by
        rw [List.length_erase_of_mem ha]
        exact Nat.sub_add_cancel (Nat.succ_le_of_lt hpos)
      rw [List.length_cons, ih hnd (l₂.erase a) hd hiff, hlen]

private theorem mem_of_drop {α : Type} {l : List α} {n : Nat} {a : α}
    (h : a ∈ l.drop n) : a ∈ l := by
  induction n generalizing l with
  | zero => simpa using h
  | succ k ih =>
      cases l with
      | nil => exact absurd h List.not_mem_nil
      | cons b bs =>
          rw [List.drop_cons (show 0 < k + 1 by omega)] at h
          exact List.mem_cons_of_mem b (ih h)

private theorem drop_append_one {α : Type} (l : List α) (c : α) {n : Nat}
    (h : n ≤ l.length) : (l ++ [c]).drop n = l.drop n ++ [c] := by
  induction l generalizing n with
  | nil =>
      have hn : n = 0 := by simpa using h
      subst hn
      simp
  | cons a as ih =>
      cases n with
      | zero => simp
      | succ k =>
          have hlen : (a :: as).length = as.length + 1 := rfl
          rw [List.cons_append, List.drop_succ_cons, List.drop_succ_cons,
            ih (by omega)]

private theorem drop_append_length {α : Type} (l₁ : List α) (l₂ : List α) :
    (l₁ ++ l₂).drop l₁.length = l₂ := by
  induction l₁ generalizing l₂ with
  | nil => rfl
  | cons a as ih =>
      rw [List.cons_append, show ((a :: as).length) = as.length + 1 from rfl,
        List.drop_succ_cons]
      exact ih l₂

private theorem drop_append_length_add {α : Type} (l₁ l₂ : List α) (m : Nat) :
    (l₁ ++ l₂).drop (l₁.length + m) = l₂.drop m := by
  rw [← List.drop_drop, drop_append_length]

private theorem getElem?_some_lt_length {α : Type} {l : List α} {i : Nat} {v : α}
    (h : l[i]? = some v) : i < l.length := by
  rcases Nat.lt_or_ge i l.length with hlt | hge
  · exact hlt
  · exfalso
    rw [List.getElem?_eq_none hge] at h
    exact (Option.some_ne_none v) h.symm

/-! ## `isLast` and `lastList` -/

/-- Position `p` is the last occurrence of its (non-`none`) key in `l`. -/
private def isLast (l : List (Option Int)) (p : Nat) : Prop :=
  ∃ x, l[p]? = some (some x) ∧ ¬ x ∈ (l.drop (p + 1)).filterMap id

/-- The increasing list of last-occurrence positions of distinct non-`none` keys. -/
private def lastList : List (Option Int) → List Nat
  | [] => []
  | none :: as => (lastList as).map fun p => p + 1
  | some x :: as =>
      let t := (lastList as).map fun p => p + 1
      if (some x : Option Int) ∈ as then t else 0 :: t

private theorem lastList_mem (l : List (Option Int)) :
    ∀ p, p ∈ lastList l ↔ isLast l p := by
  induction l with
  | nil =>
      intro p
      constructor
      · intro h; exact absurd h List.not_mem_nil
      · rintro ⟨x, hx, _⟩
        exact absurd (getElem?_some_lt_length hx) (by simp)
  | cons a as ih =>
      intro p
      cases a with
      | none =>
          show p ∈ (lastList as).map (fun q => q + 1) ↔ isLast (none :: as) p
          constructor
          · intro h
            obtain ⟨q, hq, heq⟩ := List.mem_map.mp h
            obtain ⟨x, hx1, hx2⟩ := (ih q).mp hq
            subst heq
            exact ⟨x, by simpa using hx1, by simpa using hx2⟩
          · rintro ⟨x, hx1, hx2⟩
            have hgt : p ≠ 0 := by
              intro h0
              subst h0
              simpa using hx1
            obtain ⟨q, rfl⟩ : ∃ q, p = q + 1 := ⟨p - 1, by omega⟩
            exact List.mem_map.mpr ⟨q, (ih q).mpr ⟨x, by simpa using hx1, by simpa using hx2⟩, rfl⟩
      | some x =>
          show p ∈ (if (some x : Option Int) ∈ as then (lastList as).map (fun q => q + 1)
            else 0 :: (lastList as).map (fun q => q + 1)) ↔ isLast (some x :: as) p
          by_cases hc : (some x : Option Int) ∈ as
          · rw [if_pos hc]
            constructor
            · intro h
              obtain ⟨q, hq, heq⟩ := List.mem_map.mp h
              obtain ⟨y, hy1, hy2⟩ := (ih q).mp hq
              subst heq
              exact ⟨y, by simpa using hy1, by simpa using hy2⟩
            · rintro ⟨y, hy1, hy2⟩
              have hp0 : p ≠ 0 := by
                intro h0
                subst h0
                have hxy : x = y := by simpa using hy1
                have hy2' : ¬ y ∈ (as : List (Option Int)).filterMap id := by simpa using hy2
                exact hy2' (mem_filterMap_id.mpr (hxy ▸ hc))
              obtain ⟨q, rfl⟩ : ∃ q, p = q + 1 := ⟨p - 1, by omega⟩
              exact List.mem_map.mpr ⟨q, (ih q).mpr ⟨y, by simpa using hy1, by simpa using hy2⟩, rfl⟩
          · rw [if_neg hc]
            simp only [List.mem_cons]
            constructor
            · rintro (rfl | h)
              · exact ⟨x, by simpa, fun hmem => hc (mem_filterMap_id.mp hmem)⟩
              · obtain ⟨q, hq, heq⟩ := List.mem_map.mp h
                obtain ⟨y, hy1, hy2⟩ := (ih q).mp hq
                subst heq
                exact ⟨y, by simpa using hy1, by simpa using hy2⟩
            · rintro ⟨y, hy1, hy2⟩
              rcases Nat.eq_zero_or_pos p with rfl | hgt
              · exact Or.inl rfl
              · obtain ⟨q, rfl⟩ : ∃ q, p = q + 1 := ⟨p - 1, by omega⟩
                exact Or.inr (List.mem_map.mpr
                  ⟨q, (ih q).mpr ⟨y, by simpa using hy1, by simpa using hy2⟩, rfl⟩)

private theorem nodup_lastList (l : List (Option Int)) : (lastList l).Nodup := by
  induction l with
  | nil => simp [lastList]
  | cons a as ih =>
      cases a with
      | none => exact nodup_map_add ih
      | some x =>
          show (if (some x : Option Int) ∈ as then (lastList as).map (fun p => p + 1)
            else 0 :: (lastList as).map (fun p => p + 1)).Nodup
          by_cases hc : (some x : Option Int) ∈ as
          · rw [if_pos hc]
            exact nodup_map_add ih
          · rw [if_neg hc]
            refine List.nodup_cons.mpr ⟨?_, nodup_map_add ih⟩
            intro hmem
            obtain ⟨q, _, hq⟩ := List.mem_map.mp hmem
            omega

private theorem lastList_lt_length {l : List (Option Int)} {q : Nat}
    (h : q ∈ lastList l) : q < l.length := by
  rw [lastList_mem] at h
  obtain ⟨x, hx, _⟩ := h
  exact getElem?_some_lt_length hx

private theorem lastList_append_none (l : List (Option Int)) :
    lastList (l ++ [none]) = lastList l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      cases a with
      | none => simpa [lastList, ih]
      | some x =>
          by_cases hc : (some x : Option Int) ∈ as
          · have hc' : (some x : Option Int) ∈ as ++ [none] :=
              List.mem_append.mpr (Or.inl hc)
            rw [show lastList (some x :: as ++ [none]) =
                  (lastList (as ++ [none])).map (fun p => p + 1) from if_pos hc', ih,
              show lastList (some x :: as) = (lastList as).map (fun p => p + 1) from if_pos hc]
          · have hc' : ¬ (some x : Option Int) ∈ as ++ [none] := by simpa using hc
            rw [show lastList (some x :: as ++ [none]) =
                  0 :: (lastList (as ++ [none])).map (fun p => p + 1) from if_neg hc', ih,
              show lastList (some x :: as) = 0 :: (lastList as).map (fun p => p + 1) from
                if_neg hc]

private theorem isLast_append_last (l : List (Option Int)) (x : Int) :
    isLast (l ++ [some x]) l.length := by
  refine ⟨x, ?_, ?_⟩
  · rw [List.getElem?_append]
    simp
  · rw [drop_append_length_add]
    show ¬ x ∈ (([] : List (Option Int))).filterMap id
    simp

private theorem isLast_append_some_lt (l : List (Option Int)) (x : Int) {p : Nat}
    (hp : p < l.length) :
    isLast (l ++ [some x]) p ↔ (isLast l p ∧ ¬ (l[p]? = some (some x))) := by
  have getp : (l ++ [some x])[p]? = l[p]? := by
    rw [List.getElem?_append, if_pos hp]
  have droph : ∀ y : Int,
      (y ∈ ((l ++ [some x]).drop (p + 1)).filterMap id) ↔
        (y ∈ (l.drop (p + 1)).filterMap id ∨ y = x) := by
    intro y
    rw [mem_filterMap_id, drop_append_one l (some x) (Nat.succ_le_of_lt hp),
      List.mem_append, List.mem_singleton]
    constructor
    · rintro (h | h)
      · exact Or.inl (mem_filterMap_id.mpr h)
      · exact Or.inr (Option.some.inj h)
    · rintro (h | h)
      · exact Or.inl (mem_filterMap_id.mp h)
      · exact Or.inr (congrArg some h)
  constructor
  · rintro ⟨y, hy1, hy2⟩
    have hget : l[p]? = some (some y) := by
      rw [← getp]
      exact hy1
    refine ⟨⟨y, hget, fun h => hy2 ((droph y).mpr (Or.inl h))⟩, ?_⟩
    intro hcon
    have hxy : y = x := Option.some.inj (Option.some.inj (hget.symm.trans hcon))
    subst hxy
    exact hy2 ((droph y).mpr (Or.inr rfl))
  · rintro ⟨⟨y, hy1, hy2⟩, hne⟩
    refine ⟨y, ?_, fun h => by
      rw [droph] at h
      cases h with
      | inl h => exact hy2 h
      | inr h => exact hne (by rw [← h]; exact hy1)⟩
    rw [getp]
    exact hy1

private theorem isLast_append_none (l : List (Option Int)) {p : Nat} :
    isLast (l ++ [none]) p ↔ isLast l p := by
  have dropfm : ∀ y : Int,
      (y ∈ ((l ++ [none]).drop (p + 1)).filterMap id) ↔ (y ∈ (l.drop (p + 1)).filterMap id) := by
    intro y
    rw [mem_filterMap_id]
    rcases Nat.lt_or_ge (p + 1) l.length with hlt | hge
    · rw [drop_append_one l none (Nat.le_of_lt hlt), List.mem_append, List.mem_singleton]
      rw [← mem_filterMap_id]
      simp
    · have hsplit : l.length + (p + 1 - l.length) = p + 1 := Nat.add_sub_of_le hge
      rw [show l.drop (p + 1) = ([] : List (Option Int)) from List.drop_eq_nil_of_le hge,
        ← hsplit, drop_append_length_add]
      rcases Nat.eq_zero_or_pos (p + 1 - l.length) with hz | hpos
      · rw [hz]
        simp
      · rw [show ([none] : List (Option Int)).drop (p + 1 - l.length) = [] from
            List.drop_eq_nil_of_le hpos]
        simp
  constructor
  · rintro ⟨x, hx1, hx2⟩
    have hlt : p < l.length := by
      rcases Nat.lt_or_ge p l.length with h | h
      · exact h
      · exfalso
        rw [List.getElem?_append, if_neg (by omega : ¬ p < l.length)] at hx1
        rcases Nat.eq_or_lt_of_le h with rfl | hgt
        · simp at hx1
        · rw [List.getElem?_eq_none (by omega : (1 : Nat) ≤ p - l.length)] at hx1
          simp at hx1
    exact ⟨x, by
      rw [← (show (l ++ [none])[p]? = l[p]? from by
        rw [List.getElem?_append, if_pos hlt])];
      exact hx1,
      fun h => hx2 ((dropfm x).mpr h)⟩
  · rintro ⟨x, hx1, hx2⟩
    refine ⟨x, ?_, fun h => hx2 ((dropfm x).mp h)⟩
    rw [List.getElem?_append, if_pos (getElem?_some_lt_length hx1)]
    exact hx1

private theorem lastList_append_some (l : List (Option Int)) (x : Int) (q : Nat) :
    q ∈ lastList (l ++ [some x]) ↔
      q = l.length ∨ (q ∈ lastList l ∧ ¬ (l[q]? = some (some x))) := by
  rw [lastList_mem, lastList_mem]
  rcases Nat.lt_trichotomy q l.length with hlt | heq | hgt
  · rw [isLast_append_some_lt l x hlt]
    constructor
    · intro ⟨h, hne⟩
      exact Or.inr ⟨h, hne⟩
    · rintro (h' | ⟨h, hne⟩)
      · exact absurd h' (by omega)
      · exact ⟨h, hne⟩
  · subst heq
    constructor
    · intro _
      exact Or.inl rfl
    · intro _
      exact isLast_append_last l x
  · have hge : l.length ≤ q := by omega
    have ho : (l ++ [some x])[q]? = none :=
      List.getElem?_eq_none (by simp [List.length_append]; omega)
    have hlq : l[q]? = none := List.getElem?_eq_none hge
    constructor
    · rintro ⟨y, hy, _⟩
      rw [ho] at hy
      exact absurd hy (by simp)
    · rintro (h' | ⟨⟨y, hy1, _⟩, _⟩)
      · exact absurd h' (by omega)
      · rw [hlq] at hy1
        exact absurd hy1 (by simp)

/-! ## Marked trees: well-formedness and the behaviour of `mark` / `suffix` -/

/-- `WF t d ps` states that `ps` is (as a list) exactly the marked positions of `t`
    viewed at depth `d`, with the stored counts consistent with `ps.length`. -/
private inductive WF : Marks → Nat → List Nat → Prop where
  | empty (d : Nat) : WF Marks.empty d []
  | leaf : WF (.branch 1 .empty .empty) 0 [0]
  | node (d : Nat) (l r : Marks) (ps qs : List Nat) :
      WF l d ps → WF r d qs →
      WF (.branch (ps.length + qs.length) l r) (d + 1) (ps ++ qs.map fun p => p + 2 ^ d)

private theorem WF.bounded {t : Marks} {d : Nat} {ps : List Nat} (hwf : WF t d ps) :
    ∀ p, p ∈ ps → p < 2 ^ d := by
  induction hwf with
  | empty d => intro p hp; exact absurd hp List.not_mem_nil
  | leaf => intro p hp; rw [List.mem_singleton] at hp; subst hp; exact Nat.one_pos
  | node d l r ps qs hl hr ihl ihr =>
      intro p hp
      rw [List.mem_append, List.mem_map] at hp
      rcases hp with hps | ⟨q, hq, rfl⟩
      · have h2 : 2 ^ (d + 1) = 2 ^ d + 2 ^ d := by
          rw [Nat.pow_succ, Nat.mul_two]
        rw [h2]
        exact Nat.lt_of_lt_of_le (ihl p hps) (Nat.le_add_right _ _)
      · have h2 : 2 ^ (d + 1) = 2 ^ d + 2 ^ d := by
          rw [Nat.pow_succ, Nat.mul_two]
        rw [h2]
        exact Nat.add_lt_add_right (ihr q hq) _

private theorem WF.total_eq_length {t : Marks} {d : Nat} {ps : List Nat} (hwf : WF t d ps) :
    total t = ps.length := by
  induction hwf with
  | empty d => rfl
  | leaf => rfl
  | node d l r ps qs hl hr ihl ihr =>
      show ps.length + qs.length = (ps ++ qs.map fun p => p + 2 ^ d).length
      rw [List.length_append, List.length_map]

private theorem mark_succ_branch_left (on : Bool) (c : Nat) (l r : Marks) (pos d : Nat)
    (h : pos < 2 ^ d) :
    mark on (.branch c l r) pos (d + 1)
      = .branch (total (mark on l pos d) + total r) (mark on l pos d) r := by
  conv => lhs; unfold mark; simp only [children]; rw [if_pos h]

private theorem mark_succ_branch_right (on : Bool) (c : Nat) (l r : Marks) (pos d : Nat)
    (h : ¬ pos < 2 ^ d) :
    mark on (.branch c l r) pos (d + 1)
      = .branch (total l + total (mark on r (pos - 2 ^ d) d)) l (mark on r (pos - 2 ^ d) d) := by
  conv => lhs; unfold mark; simp only [children]; rw [if_neg h]

private theorem mark_succ_empty_left (on : Bool) (pos d : Nat) (h : pos < 2 ^ d) :
    mark on Marks.empty pos (d + 1)
      = .branch (total (mark on Marks.empty pos d) + total Marks.empty) (mark on Marks.empty pos d)
          Marks.empty := by
  conv => lhs; unfold mark; simp only [children]; rw [if_pos h]

private theorem mark_succ_empty_right (on : Bool) (pos d : Nat) (h : ¬ pos < 2 ^ d) :
    mark on Marks.empty pos (d + 1)
      = .branch (total Marks.empty + total (mark on Marks.empty (pos - 2 ^ d) d)) Marks.empty
          (mark on Marks.empty (pos - 2 ^ d) d) := by
  conv => lhs; unfold mark; simp only [children]; rw [if_neg h]

private theorem erase_append_left {l r : List Nat} {a : Nat}
    (hl : a ∈ l) : (l ++ r).erase a = l.erase a ++ r := by
  induction l generalizing r with
  | nil => exact absurd hl List.not_mem_nil
  | cons b bs ih =>
      by_cases hb : (b == a) = true
      · rw [List.cons_append, List.erase_cons, if_pos hb, List.erase_cons, if_pos hb]
      · have hbs : a ∈ bs := by
          rcases List.mem_cons.mp hl with rfl | h
          · exact absurd (by simp) hb
          · exact h
        rw [List.cons_append, List.erase_cons, if_neg hb, List.erase_cons, if_neg hb,
          ih hbs, List.cons_append]

private theorem erase_append_right {l r : List Nat} {a : Nat}
    (hl : ∀ x ∈ l, x ≠ a) : (l ++ r).erase a = l ++ r.erase a := by
  induction l generalizing r with
  | nil => rfl
  | cons b bs ih =>
      by_cases hb : (b == a) = true
      · have hb' : b = a := by simpa using hb
        exact absurd hb' (hl b List.mem_cons_self)
      · rw [List.cons_append, List.erase_cons, if_neg hb,
          ih (fun x hx => hl x (List.mem_cons_of_mem b hx)), List.cons_append]

private theorem erase_map_add {xs : List Nat} {a h : Nat} (hh : h ≤ a) :
    (xs.map fun x => x + h).erase a = (xs.erase (a - h)).map fun x => x + h := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : (x + h == a) = true
      · have he : x + h = a := by simpa using hx
        have h2 : (x == a - h) = true := by
          have hxe : x = a - h := by omega
          simpa using hxe
        rw [List.map_cons, List.erase_cons, if_pos hx, List.erase_cons, if_pos h2]
      · have hne : x + h ≠ a := by simpa using hx
        have h2 : ¬ ((x == a - h) = true) := by
          intro hc
          have hxe : x = a - h := by simpa using hc
          omega
        rw [List.map_cons, List.erase_cons, if_neg hx, List.erase_cons, if_neg h2, ih,
          List.map_cons]

private theorem WF_mark_false : ∀ {t : Marks} {d : Nat} {ps : List Nat}, WF t d ps →
    ∀ pos, pos ∈ ps → WF (mark false t pos d) d (ps.erase pos) := by
  intro t d ps hwf
  induction hwf with
  | empty d =>
      intro pos hmem
      exact absurd hmem List.not_mem_nil
  | leaf =>
      intro pos hmem
      have hpos : pos = 0 := by simpa using hmem
      subst hpos
      exact WF.empty 0
  | node d l r ps qs hl hr ihl ihr =>
      intro pos hmem
      rw [List.mem_append] at hmem
      rcases hmem with hps | hq
      · have hb : pos < 2 ^ d := hl.bounded pos hps
        have h1 := ihl pos hps
        rw [mark_succ_branch_left false _ _ _ _ _ hb, erase_append_left hps,
          WF.total_eq_length h1, WF.total_eq_length hr]
        exact WF.node d (mark false l pos d) r (ps.erase pos) qs h1 hr
      · rcases List.mem_map.mp hq with ⟨q, hqs, rfl⟩
        have hbq : q < 2 ^ d := hr.bounded q hqs
        have h1 := ihr q hqs
        have hnl : ∀ x ∈ ps, x ≠ q + 2 ^ d := by
          intro x hx hxeq
          have := hl.bounded x hx
          omega
        rw [mark_succ_branch_right false _ _ _ _ _ (by omega : ¬ q + 2 ^ d < 2 ^ d),
          erase_append_right hnl, erase_map_add (by omega : 2 ^ d ≤ q + 2 ^ d),
          Nat.add_sub_cancel, WF.total_eq_length hl, WF.total_eq_length h1]
        exact WF.node d l (mark false r q d) ps (qs.erase q) hl h1

private theorem WF_mark_true : ∀ (d : Nat) (t : Marks) (ps : List Nat), WF t d ps →
    ∀ pos, pos < 2 ^ d → pos ∉ ps →
      ∃ ps', WF (mark true t pos d) d ps' ∧ ∀ q, q ∈ ps' ↔ (q = pos ∨ q ∈ ps) := by
  intro d
  induction d with
  | zero =>
      intro t ps hwf pos hlt hnmem
      cases hwf with
      | empty d =>
          have hpos : pos = 0 := by omega
          subst hpos
          exact ⟨[0], WF.leaf, fun q => by simp⟩
      | leaf =>
          have h0 : pos ∈ ([0] : List Nat) := by
            have : pos = 0 := by omega
            simpa using this
          exact absurd h0 hnmem
  | succ d ih =>
      intro t ps hwf pos hlt hnmem
      cases hwf with
      | empty d' =>
          by_cases hb : pos < 2 ^ d
          · obtain ⟨ps', h2, hm⟩ := ih _ _ (WF.empty d) pos hb (by simpa using hnmem)
            rw [mark_succ_empty_left true pos d hb, WF.total_eq_length h2,
              WF.total_eq_length (WF.empty d)]
            exact ⟨ps' ++ [].map (fun p => p + 2 ^ d),
              WF.node d (mark true Marks.empty pos d) Marks.empty ps' [] h2 (WF.empty d),
              fun q => by rw [List.mem_append, List.mem_map, hm]; simp⟩
          · obtain ⟨ps', h2, hm⟩ := ih _ _ (WF.empty d) (pos - 2 ^ d) (by omega)
              (by simpa using hnmem)
            rw [mark_succ_empty_right true pos d hb, WF.total_eq_length (WF.empty d),
              WF.total_eq_length h2]
            refine ⟨[] ++ ps'.map (fun p => p + 2 ^ d),
              WF.node d Marks.empty (mark true Marks.empty (pos - 2 ^ d) d) [] ps' (WF.empty d) h2,
              fun q => by
                constructor
                · intro h
                  rcases List.mem_append.mp h with hnil | hm1
                  · exact absurd hnil List.not_mem_nil
                  · obtain ⟨b, hb, rfl⟩ := List.mem_map.mp hm1
                    rcases (hm b).mp hb with rfl | hb0
                    · exact Or.inl (by omega)
                    · exact absurd hb0 List.not_mem_nil
                · intro h
                  rcases h with (hqe | hnil)
                  · rw [hqe]
                    exact List.mem_append.mpr (Or.inr
                      (List.mem_map.mpr ⟨pos - 2 ^ d,
                        (hm (pos - 2 ^ d)).mpr (Or.inl rfl), by omega⟩))
                  · exact absurd hnil List.not_mem_nil⟩
      | node d' l r ps qs hl hr =>
          rw [List.mem_append] at hnmem
          by_cases hb : pos < 2 ^ d
          · obtain ⟨ps2, h2, hm2⟩ :=
              ih l ps hl pos hb (fun h => hnmem (Or.inl h))
            rw [mark_succ_branch_left true _ _ _ _ _ hb, WF.total_eq_length h2,
              WF.total_eq_length hr]
            exact ⟨ps2 ++ qs.map (fun p => p + 2 ^ d),
              WF.node d (mark true l pos d) r ps2 qs h2 hr,
              fun q => by
                constructor
                · intro h
                  rcases List.mem_append.mp h with h2' | hq
                  · rcases (hm2 q).mp h2' with hqe | hps
                    · exact Or.inl hqe
                    · exact Or.inr (List.mem_append.mpr (Or.inl hps))
                  · exact Or.inr (List.mem_append.mpr (Or.inr hq))
                · rw [List.mem_append]
                  intro h
                  rcases h with hqe | hps | hq
                  · rw [hqe]
                    exact List.mem_append.mpr (Or.inl ((hm2 pos).mpr (Or.inl rfl)))
                  · exact List.mem_append.mpr (Or.inl ((hm2 q).mpr (Or.inr hps)))
                  · exact List.mem_append.mpr (Or.inr hq)⟩
          · have hge : 2 ^ d ≤ pos := by omega
            obtain ⟨ps2, h2, hm2⟩ :=
              ih r qs hr (pos - 2 ^ d) (by omega)
                (fun h => hnmem (Or.inr (List.mem_map.mpr ⟨pos - 2 ^ d, h, by omega⟩)))
            rw [mark_succ_branch_right true _ _ _ _ _ (by omega : ¬ pos < 2 ^ d),
              WF.total_eq_length hl, WF.total_eq_length h2]
            refine ⟨ps ++ ps2.map (fun p => p + 2 ^ d),
              WF.node d l (mark true r (pos - 2 ^ d) d) ps ps2 hl h2, fun q => by
                constructor
                · intro h
                  rcases List.mem_append.mp h with hps | hm2'
                  · exact Or.inr (List.mem_append.mpr (Or.inl hps))
                  · obtain ⟨b, hb, rfl⟩ := List.mem_map.mp hm2'
                    rcases (hm2 b).mp hb with hqe | hq
                    · rw [hqe]
                      exact Or.inl (by omega)
                    · exact Or.inr (List.mem_append.mpr
                        (Or.inr (List.mem_map.mpr ⟨b, hq, rfl⟩)))
                · rw [List.mem_append]
                  intro h
                  rcases h with hqe | hps | hq
                  · rw [hqe]
                    exact List.mem_append.mpr (Or.inr (List.mem_map.mpr
                      ⟨pos - 2 ^ d, (hm2 (pos - 2 ^ d)).mpr (Or.inl rfl), by omega⟩))
                  · exact List.mem_append.mpr (Or.inl hps)
                  · obtain ⟨c, hc, rfl⟩ := List.mem_map.mp hq
                    exact List.mem_append.mpr (Or.inr (List.mem_map.mpr
                      ⟨c, (hm2 c).mpr (Or.inr hc), rfl⟩))⟩

private theorem suffix_zero (t : Marks) (d : Nat) : suffix t 0 d = total t := by
  cases d with
  | zero => rfl
  | succ d => rfl

private theorem suffix_succ_empty_lt (d k : Nat) (h : k + 1 < 2 ^ d) :
    suffix Marks.empty (k + 1) (d + 1) =
      suffix Marks.empty (k + 1) d + total Marks.empty := by
  conv =>
    lhs
    unfold suffix
    rw [if_neg (by omega : ¬ (k + 1 = 0))]
    simp only [children]
    rw [if_pos h]

private theorem suffix_succ_empty_ge (d k : Nat) (h : ¬ k + 1 < 2 ^ d) :
    suffix Marks.empty (k + 1) (d + 1) = suffix Marks.empty (k + 1 - 2 ^ d) d := by
  conv =>
    lhs
    unfold suffix
    rw [if_neg (by omega : ¬ (k + 1 = 0))]
    simp only [children]
    rw [if_neg h]

private theorem suffix_succ_branch_lt (c : Nat) (l r : Marks) (d k : Nat) (h : k + 1 < 2 ^ d) :
    suffix (.branch c l r) (k + 1) (d + 1) = suffix l (k + 1) d + total r := by
  conv =>
    lhs
    unfold suffix
    rw [if_neg (by omega : ¬ (k + 1 = 0))]
    simp only [children]
    rw [if_pos h]

private theorem suffix_succ_branch_ge (c : Nat) (l r : Marks) (d k : Nat) (h : ¬ k + 1 < 2 ^ d) :
    suffix (.branch c l r) (k + 1) (d + 1) = suffix r (k + 1 - 2 ^ d) d := by
  conv =>
    lhs
    unfold suffix
    rw [if_neg (by omega : ¬ (k + 1 = 0))]
    simp only [children]
    rw [if_neg h]

private theorem suffix_correct : ∀ (d : Nat) (t : Marks) (ps : List Nat), WF t d ps →
    ∀ lo : Nat, suffix t lo d = (ps.filter fun p => lo ≤ p).length := by
  intro d
  induction d with
  | zero =>
      intro t ps hwf lo
      cases hwf with
      | empty d => cases lo with
          | zero => rfl
          | succ k => rfl
      | leaf => cases lo with
          | zero => rfl
          | succ k => rfl
  | succ d ih =>
      intro t ps hwf lo
      cases hwf with
      | empty d' =>
          cases lo with
          | zero => rfl
          | succ k =>
              by_cases h : k + 1 < 2 ^ d
              · rw [suffix_succ_empty_lt _ _ h, ih Marks.empty [] (WF.empty d) (k + 1)]
                exact rfl
              · rw [suffix_succ_empty_ge _ _ h, ih Marks.empty [] (WF.empty d) (k + 1 - 2 ^ d)]
                exact rfl
      | node d' l r ps qs hl hr =>
          cases lo with
          | zero =>
              rw [suffix_zero,
                show total (.branch (ps.length + qs.length) l r) = ps.length + qs.length from rfl,
                filter_eq_self_of_forall _ _ (fun a _ => by simp),
                List.length_append, List.length_map]
          | succ k =>
              by_cases h : k + 1 < 2 ^ d
              · rw [suffix_succ_branch_lt _ _ _ _ _ h, ih l ps hl (k + 1), WF.total_eq_length hr,
                  List.filter_append, List.length_append, filter_map_commute _ _ _,
                  List.length_map,
                  filter_eq_self_of_forall qs (fun q => k + 1 ≤ q + 2 ^ d)
                    (fun q hq => by
                      have hbq : q < 2 ^ d := hr.bounded q hq
                      exact decide_eq_true (by omega : (k + 1 ≤ q + 2 ^ d)))]
              · have hge : 2 ^ d ≤ k + 1 := by omega
                have hfnp : ((fun q => k + 1 ≤ q + 2 ^ d) : Nat → Bool)
                    = (fun q => k + 1 - 2 ^ d ≤ q) := by
                  funext q
                  by_cases h2 : k + 1 ≤ q + 2 ^ d
                  · rw [decide_eq_true h2, decide_eq_true (by omega : k + 1 - 2 ^ d ≤ q)]
                  · rw [decide_eq_false h2,
                      decide_eq_false (by omega : ¬ k + 1 - 2 ^ d ≤ q)]
                rw [suffix_succ_branch_ge _ _ _ _ _ h, ih r qs hr (k + 1 - 2 ^ d),
                  List.filter_append,
                  filter_eq_nil_of_forall_not ps (fun p => (k + 1 ≤ p))
                    (fun p hp => by
                      have hbp : p < 2 ^ d := hl.bounded p hp
                      exact decide_eq_false (by omega : ¬ (k + 1 ≤ p)))]
                simp only [List.filter_nil, List.length_nil, List.nil_append, Nat.zero_add]
                rw [filter_map_commute _ _ _, List.length_map, hfnp]

/-! ## Arithmetic bounds -/

private theorem length_lt_two_pow_log2_add_one (n : Nat) : n < 2 ^ (n.log2 + 1) := by
  cases n with
  | zero => simp
  | succ k =>
      exact (Nat.log2_lt (Nat.succ_ne_zero k)).mp (Nat.lt_succ_self _)

/-! ## More generic list helpers -/

private theorem nodup_eraseDups (l : List Int) : l.eraseDups.Nodup := by
  let P : Nat → Prop := fun n => ∀ l : List Int, l.length = n → l.eraseDups.Nodup
  have h : ∀ n, P n := fun n => Nat.strongRecOn n (fun n ih l hl => by
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        refine List.nodup_cons.mpr ⟨?_, ?_⟩
        · intro hm
          have hma : a ∈ as.filter (fun b => ! b == a) := (List.mem_eraseDups).mp hm
          have hf := (List.mem_filter.mp hma).2
          simp at hf
        · apply ih (as.filter (fun b => ! b == a)).length
          · have hl' : as.length + 1 = n := by simpa using hl
            have : as.length < n := by omega
            exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) this
          · rfl)
  exact h l.length l rfl

private theorem nodup_length_eq_of_mem_iff {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l₁ : List α}, l₁.Nodup → ∀ l₂ : List α, l₂.Nodup →
      (∀ a, a ∈ l₁ ↔ a ∈ l₂) → l₁.length = l₂.length := by
  intro l₁ d₁
  induction l₁ with
  | nil =>
      intro l₂ d₂ h
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ d₂ h
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp (@List.mem_cons_self _ a r)
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact absurd rfl hca
          · exact hr
      have hpos : 0 < l₂.length := by
        cases l₂ with
        | nil => exact absurd ha List.not_mem_nil
        | cons b bs => exact Nat.succ_pos _
      have hlen : (l₂.erase a).length + 1 = l₂.length := by
        rw [List.length_erase_of_mem ha]
        exact Nat.sub_add_cancel (Nat.succ_le_of_lt hpos)
      rw [List.length_cons, ih hnd (l₂.erase a) hd hiff, hlen]

private theorem increasing_eq_of_mem_iff {l₁ l₂ : List Nat}
    (h₁ : l₁.Pairwise (· < ·)) (h₂ : l₂.Pairwise (· < ·))
    (hmem : ∀ q, q ∈ l₁ ↔ q ∈ l₂) : l₁ = l₂ := by
  revert l₂
  induction l₁ with
  | nil =>
      intro l₂ h₂ hmem
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((hmem b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ h₂ hmem
      obtain ⟨haft, hrinc⟩ := List.pairwise_cons.mp h₁
      cases l₂ with
      | nil => exact absurd ((hmem a).mp List.mem_cons_self) List.not_mem_nil
      | cons b bs =>
          obtain ⟨hbft, hbinc⟩ := List.pairwise_cons.mp h₂
          have hba : b = a := by
            by_cases hba' : b = a
            · exact hba'
            · exfalso
              rcases Nat.lt_trichotomy b a with hlt | heq | hgt
              · have hbmem : b ∈ a :: r := (hmem b).mpr List.mem_cons_self
                rcases List.mem_cons.mp hbmem with hba'' | hbr
                · exact hba' hba''
                · have hab := haft b hbr
                  omega
              · exact hba' heq
              · have hamem : a ∈ b :: bs := (hmem a).mp List.mem_cons_self
                rcases List.mem_cons.mp hamem with hab' | hbs
                · exact hba' hab'.symm
                · have hbb := hbft a hbs
                  omega
          subst b
          have hmem' : ∀ q, q ∈ r ↔ q ∈ bs := by
            intro q
            have hq : (q = a ∨ q ∈ r) ↔ (q = a ∨ q ∈ bs) := by
              simpa [List.mem_cons] using hmem q
            constructor
            · intro hqr
              have hne : q ≠ a := by
                intro hqa
                subst q
                exact Nat.lt_irrefl a (haft a hqr)
              rcases (hq.mp (Or.inr hqr)) with hqa | hqs
              · exact absurd hqa hne
              · exact hqs
            · intro hqs
              have hne : q ≠ a := by
                intro hqa
                subst q
                exact Nat.lt_irrefl a (hbft a hqs)
              rcases (hq.mpr (Or.inr hqs)) with hqa | hqr
              · exact absurd hqa hne
              · exact hqr
          have htail : r = bs := ih hrinc hbinc hmem'
          rw [htail]

private theorem eraseDups_cons_length (ys : List Int) (x : Int) :
    (x :: ys).eraseDups.length = ys.eraseDups.length + (if x ∈ ys then 0 else 1) := by
  by_cases hxy : x ∈ ys
  · have hlen := nodup_length_eq_of_mem_iff (nodup_eraseDups (x :: ys)) ys.eraseDups
      (nodup_eraseDups ys) (fun z => by
        constructor
        · intro hz
          rcases List.mem_cons.mp ((List.mem_eraseDups).mp hz) with rfl | hzys
          · exact (List.mem_eraseDups).mpr hxy
          · exact (List.mem_eraseDups).mpr hzys
        · intro hz
          apply (List.mem_eraseDups).mpr
          exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hz)))
    rw [hlen, if_pos hxy, Nat.add_zero]
  · have hnody : (x :: ys.eraseDups).Nodup := by
      refine List.nodup_cons.mpr ⟨?_, nodup_eraseDups ys⟩
      intro hm
      exact hxy ((List.mem_eraseDups).mp hm)
    have hlen := nodup_length_eq_of_mem_iff (nodup_eraseDups (x :: ys)) (x :: ys.eraseDups) hnody
      (fun z => by
        constructor
        · intro hz
          have hz' : z = x ∨ z ∈ ys := List.mem_cons.mp ((List.mem_eraseDups).mp hz)
          rcases hz' with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mpr hzys))
        · intro hz
          apply (List.mem_eraseDups).mpr
          rcases List.mem_cons.mp hz with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hzys)))
    rw [hlen, List.length_cons, if_neg hxy]

/-! ## Counting distinct values via last occurrences -/

private def uniqueCount : List (Option Int) → Nat
  | [] => 0
  | none :: as => uniqueCount as
  | some x :: as => uniqueCount as + (if (some x : Option Int) ∈ as then 0 else 1)

private theorem lastList_length_uniqueCount (l : List (Option Int)) :
    (lastList l).length = uniqueCount l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      cases a with
      | none =>
          rw [show lastList (none :: as) = (lastList as).map (fun p => p + 1) from rfl,
            List.length_map, ih]
          rfl
      | some x =>
          by_cases hc : (some x : Option Int) ∈ as
          · rw [show lastList (some x :: as) = (lastList as).map (fun p => p + 1) from if_pos hc,
              List.length_map, ih]
            simp [uniqueCount, hc]
          · rw [show lastList (some x :: as) =
                0 :: (lastList as).map (fun p => p + 1) from if_neg hc,
              List.length_cons, List.length_map, ih]
            simp [uniqueCount, hc]

private theorem filterMap_uniqueCount (l : List (Option Int)) :
    uniqueCount l = ((l.filterMap id).eraseDups).length := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      cases a with
      | none => simpa [uniqueCount] using ih
      | some x =>
          rw [show (some x :: as).filterMap id = x :: as.filterMap id from rfl]
          rw [eraseDups_cons_length, ← ih]
          have hmem : x ∈ as.filterMap id ↔ (some x : Option Int) ∈ as := mem_filterMap_id
          by_cases h : (some x : Option Int) ∈ as
          · simp [uniqueCount, h, hmem.mpr h]
          · have hn : ¬ x ∈ as.filterMap id := (not_congr hmem).mpr h
            simp [uniqueCount, h, hn]

private theorem length_lastList_eq_reference (l : List (Option Int)) :
    (lastList l).length = ((l.filterMap id).eraseDups).length := by
  rw [lastList_length_uniqueCount, filterMap_uniqueCount]

private theorem filter_map_add_one (ps : List Nat) (k : Nat) :
    ((ps.map fun p => p + 1).filter fun p => k + 1 ≤ p).length =
      ((ps.filter fun q => k ≤ q).length) := by
  induction ps with
  | nil => rfl
  | cons a as ih =>
      by_cases h1 : k ≤ a
      · have h2 : k + 1 ≤ a + 1 := by omega
        simp [h1, h2, ih]
      · have h2 : ¬ k + 1 ≤ a + 1 := by omega
        simp [h1, h2, ih]

private theorem filter_lastList_eq_drop (l : List (Option Int)) : ∀ lo : Nat,
    ((lastList l).filter fun p => lo ≤ p).length = (lastList (l.drop lo)).length := by
  induction l with
  | nil => intro lo; simp [lastList]
  | cons a as ih =>
      intro lo
      cases lo with
      | zero => simp [lastList]
      | succ k =>
          cases a with
          | none =>
              rw [show lastList (none :: as) = (lastList as).map (fun p => p + 1) from rfl,
                filter_map_add_one, List.drop_succ_cons]
              exact ih k
          | some x =>
              by_cases hc : (some x : Option Int) ∈ as
              · rw [show lastList (some x :: as) = (lastList as).map (fun p => p + 1) from if_pos hc,
                  filter_map_add_one, List.drop_succ_cons]
                exact ih k
              · rw [show lastList (some x :: as) =
                    0 :: (lastList as).map (fun p => p + 1) from if_neg hc,
                  List.filter_cons_of_neg (by simp), filter_map_add_one, List.drop_succ_cons]
                exact ih k

/-! ## More properties of marked trees -/

private theorem nodup_map_add_const (k : Nat) {l : List Nat} (h : l.Nodup) :
    (l.map fun p => p + k).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      obtain ⟨hna, hd⟩ := List.nodup_cons.mp h
      rw [List.map_cons]
      refine List.nodup_cons.mpr ⟨?_, ih hd⟩
      intro hmem
      obtain ⟨q, hq, heq⟩ := List.mem_map.mp hmem
      have haq : a = q := by omega
      rw [← haq] at hq
      exact hna hq

private theorem WF.nodup {t : Marks} {d : Nat} {ps : List Nat} (hwf : WF t d ps) :
    ps.Nodup := by
  induction hwf with
  | empty d => simp
  | leaf => simp
  | node d l r ps qs hl hr ihl ihr =>
      rw [List.nodup_append]
      refine ⟨ihl, nodup_map_add_const (2 ^ d) ihr, ?_⟩
      intro a ha b hb heq
      obtain ⟨q, hq, rfl⟩ := List.mem_map.mp hb
      have hba := hl.bounded a ha
      have hbq := hr.bounded q hq
      omega

private theorem WF.increasing {t : Marks} {d : Nat} {ps : List Nat} (hwf : WF t d ps) :
    ps.Pairwise (· < ·) := by
  induction hwf with
  | empty d => simp
  | leaf => simp
  | node d l r ps qs hl hr ihl ihr =>
      rw [List.pairwise_append]
      refine ⟨ihl, ?_, ?_⟩
      · exact List.Pairwise.map (fun p => p + 2 ^ d) (by
          intro a b hab
          omega) ihr
      · intro a ha b hb
        obtain ⟨q, hq, rfl⟩ := List.mem_map.mp hb
        have hba := hl.bounded a ha
        omega

private theorem lastList_increasing (l : List (Option Int)) :
    (lastList l).Pairwise (· < ·) := by
  induction l with
  | nil => simp [lastList]
  | cons a as ih =>
      cases a with
      | none =>
          exact List.Pairwise.map (fun p => p + 1) (by intro p q hpq; omega) ih
      | some x =>
          by_cases hc : (some x : Option Int) ∈ as
          · rw [show lastList (some x :: as) = (lastList as).map (fun p => p + 1) from if_pos hc]
            exact List.Pairwise.map (fun p => p + 1) (by intro p q hpq; omega) ih
          · rw [show lastList (some x :: as) =
                0 :: (lastList as).map (fun p => p + 1) from if_neg hc]
            rw [List.pairwise_cons]
            constructor
            · intro q hq
              obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hq
              omega
            · exact List.Pairwise.map (fun p => p + 1) (by intro p q hpq; omega) ih

private theorem WF.congr {t : Marks} {d : Nat} {ps qs : List Nat}
    (hwf : WF t d ps) (h : ps = qs) : WF t d qs := by
  subst h
  exact hwf

private theorem getElem_append_none_lt (pre : List (Option Int)) (q : Nat)
    (hq : q < pre.length) : (pre ++ [none])[q]? = pre[q]? := by
  rw [List.getElem?_append, if_pos hq]

private theorem getElem_append_some_lt (pre : List (Option Int)) (x : Int) (q : Nat)
    (hq : q < pre.length) : (pre ++ [some x])[q]? = pre[q]? := by
  rw [List.getElem?_append, if_pos hq]

private theorem getElem_append_some_self (pre : List (Option Int)) (x : Int) :
    (pre ++ [some x])[pre.length]? = some (some x) := by
  rw [List.getElem?_append]
  simp

/-! ## Replaying `build` as a pure loop -/

private def step (a : Option Int) (i : Nat) (d : Nat) (tree : Marks)
    (prev : Std.HashMap Int Nat) : Marks × Std.HashMap Int Nat :=
  match a with
  | none => (tree, prev)
  | some x =>
    let t1 := match prev[x]? with | some j => mark false tree j d | none => tree
    (mark true t1 i d, prev.insert x i)

private def loop (pairs : List (Option Int × Nat)) (d : Nat) (tree : Marks)
    (prev : Std.HashMap Int Nat) (roots : Array Marks) : Index := Id.run do
  let mut rs := roots
  let mut tr := tree
  let mut previous := prev
  for (cell, i) in pairs do
    if let some x := cell then
      if let some j := previous[x]? then tr := mark false tr j d
      tr := mark true tr i d
      previous := previous.insert x i
    rs := rs.push tr
  return ⟨d, rs⟩

private def treesAfter : List (Option Int) → List (Option Int) → Nat → Marks →
    Std.HashMap Int Nat → List Marks
  | [], _, _, t, _ => [t]
  | a :: as, pre, d, t, p =>
      let (t', p') := step a pre.length d t p
      t :: treesAfter as (pre ++ [a]) d t' p'

private def runCells : List (Option Int) → List (Option Int) → Nat → Marks →
    Std.HashMap Int Nat → Marks × Std.HashMap Int Nat
  | [], _, _, t, p => (t, p)
  | a :: as, pre, d, t, p =>
      let (t', p') := step a pre.length d t p
      runCells as (pre ++ [a]) d t' p'

private theorem build_eq_loop (xs : List (Option Int)) :
    build xs = loop (xs.zipIdx) (xs.length.log2 + 1) Marks.empty {} #[Marks.empty] := by
  rfl

private theorem loop_cons (a : Option Int) (as : List (Option Int)) (i d : Nat)
    (tree : Marks) (prev : Std.HashMap Int Nat) (roots : Array Marks) :
    loop ((a, i) :: as.zipIdx (i + 1)) d tree prev roots =
      let (tree', prev') := step a i d tree prev
      loop (as.zipIdx (i + 1)) d tree' prev' (roots.push tree') := by
  cases a with
  | none => simp [loop, step]
  | some v => cases h : prev[v]? <;> simp [loop, step, h]

private theorem array_push_append (roots : Array Marks) (t : Marks) (l : List Marks) :
    roots.push t ++ l.toArray = roots ++ (t :: l).toArray := by
  apply Array.toList_inj.mp
  simp [Array.toList_append]

private theorem array_singleton_append (t : Marks) (l : List Marks) :
    #[t] ++ l.toArray = (t :: l).toArray := by
  apply Array.toList_inj.mp
  simp

private theorem loop_result (cells pre : List (Option Int)) (d : Nat) (tree : Marks)
    (prev : Std.HashMap Int Nat) (roots : Array Marks) :
    loop (cells.zipIdx pre.length) d tree prev roots =
      ⟨d, roots ++ (treesAfter cells pre d tree prev).tail.toArray⟩ := by
  induction cells generalizing pre tree prev roots with
  | nil =>
      simp [treesAfter, loop]
  | cons a as ih =>
      rw [show (a :: as).zipIdx pre.length = (a, pre.length) :: as.zipIdx (pre.length + 1) by
        simp [List.zipIdx]]
      rw [loop_cons]
      generalize hs : step a pre.length d tree prev = st
      cases st with
      | mk t' p' =>
        have hlen : (pre ++ [a]).length = pre.length + 1 := by simp
        rw [← hlen]
        change loop (as.zipIdx (pre ++ [a]).length) d t' p' (roots.push t') = _
        rw [ih (pre ++ [a]) t' p' (roots.push t')]
        simp only [treesAfter]
        rw [hs]
        rw [array_push_append]
        have ht : treesAfter as (pre ++ [a]) d t' p' =
            t' :: (treesAfter as (pre ++ [a]) d t' p').tail := by
          cases as <;> rfl
        rw [ht]
        simp

private theorem treesAfter_length (cells pre : List (Option Int)) (d : Nat)
    (tree : Marks) (prev : Std.HashMap Int Nat) :
    (treesAfter cells pre d tree prev).length = cells.length + 1 := by
  induction cells generalizing pre tree prev with
  | nil => simp [treesAfter]
  | cons a as ih =>
      simp [treesAfter, ih]

private theorem treesAfter_take (xs pre : List (Option Int)) (d : Nat) (tree : Marks)
    (prev : Std.HashMap Int Nat) (k : Nat) (hk : k ≤ xs.length) :
    (treesAfter xs pre d tree prev)[k]? = some (runCells (xs.take k) pre d tree prev).1 := by
  induction xs generalizing pre tree prev k with
  | nil =>
      have hk0 : k = 0 := by simpa using hk
      subst k
      simp [treesAfter, runCells]
  | cons a as ih =>
      cases k with
      | zero => simp [treesAfter, runCells]
      | succ j =>
          have hj : j ≤ as.length := by simpa using hk
          have htake : (a :: as).take (j + 1) = a :: as.take j := by simp
          rw [htake]
          simp only [runCells]
          dsimp [treesAfter]
          exact ih (pre ++ [a]) (step a pre.length d tree prev).1
            (step a pre.length d tree prev).2 j hj

/-! ## Invariant of the replay loop -/

private theorem runCells_wf (cells pre : List (Option Int)) (d : Nat) (tree : Marks)
    (prev : Std.HashMap Int Nat)
    (hwf : WF tree d (lastList pre))
    (hprev : ∀ v p, prev[v]? = some p ↔ p ∈ lastList pre ∧ pre[p]? = some (some v))
    (hbound : pre.length + cells.length < 2 ^ d) :
    WF (runCells cells pre d tree prev).1 d (lastList (pre ++ cells)) ∧
    (∀ v p, (runCells cells pre d tree prev).2[v]? = some p ↔
      p ∈ lastList (pre ++ cells) ∧ (pre ++ cells)[p]? = some (some v)) := by
  induction cells generalizing pre tree prev with
  | nil =>
      simpa [runCells] using And.intro hwf hprev
  | cons a as ih =>
      cases a with
      | none =>
          have hlen : (pre ++ [none] : List (Option Int)).length = pre.length + 1 := by simp
          have hbound' : (pre ++ [none]).length + as.length < 2 ^ d := by
            have hb' : pre.length + (as.length + 1) < 2 ^ d := by simpa using hbound
            rw [hlen]
            omega
          have hwf' : WF tree d (lastList (pre ++ [none])) := by
            rw [lastList_append_none]
            exact hwf
          have hprev' : ∀ v p, prev[v]? = some p ↔
              p ∈ lastList (pre ++ [none]) ∧ (pre ++ [none])[p]? = some (some v) := by
            intro v p
            rw [lastList_append_none]
            constructor
            · intro h
              rcases (hprev v p).mp h with ⟨hp, hget⟩
              refine ⟨hp, ?_⟩
              rw [getElem_append_none_lt pre p (lastList_lt_length hp)]
              exact hget
            · rintro ⟨hp, hget⟩
              apply (hprev v p).mpr
              refine ⟨hp, ?_⟩
              rw [getElem_append_none_lt pre p (lastList_lt_length hp)] at hget
              exact hget
          have hres := ih (pre ++ [none]) tree prev hwf' hprev' hbound'
          simpa [runCells, step, List.append_assoc] using hres
      | some x =>
          have hblen : pre.length < 2 ^ d := by omega
          rcases hpx : prev[x]? with hnone | j
          · have hnot : pre.length ∉ lastList pre := by
              intro hm
              have hlt := lastList_lt_length hm
              omega
            obtain ⟨ps', hwf1, hmem'⟩ :=
              WF_mark_true d tree (lastList pre) hwf pre.length hblen hnot
            have hsame : ∀ q, q ∈ lastList pre → ¬ pre[q]? = some (some x) := by
              intro q hqm hq
              have hpq : prev[x]? = some q := (hprev x q).mpr ⟨hqm, hq⟩
              rw [hpx] at hpq
              cases hpq
            have hmemeq : ∀ q, q ∈ ps' ↔ q ∈ lastList (pre ++ [some x]) := by
              intro q
              rw [hmem', lastList_append_some]
              constructor
              · rintro (rfl | hq)
                · exact Or.inl rfl
                · exact Or.inr ⟨hq, hsame q hq⟩
              · rintro (rfl | ⟨hq, hneq⟩)
                · exact Or.inl rfl
                · exact Or.inr hq
            have hps : ps' = lastList (pre ++ [some x]) :=
              increasing_eq_of_mem_iff (WF.increasing hwf1)
                (lastList_increasing (pre ++ [some x])) hmemeq
            have hwf2 : WF (mark true tree pre.length d) d (lastList (pre ++ [some x])) :=
              WF.congr hwf1 hps
            have hprev1 : ∀ v p, (prev.insert x pre.length)[v]? = some p ↔
                p ∈ lastList (pre ++ [some x]) ∧ (pre ++ [some x])[p]? = some (some v) := by
              intro v p
              rw [Std.HashMap.getElem?_insert]
              constructor
              · intro h
                by_cases hxv : x = v
                · have hp : p = pre.length := by simpa [hxv] using h.symm
                  subst p
                  subst v
                  constructor
                  · rw [lastList_append_some]
                    exact Or.inl rfl
                  · exact getElem_append_some_self pre x
                · have hpq : prev[v]? = some p := by simpa [hxv] using h
                  rcases (hprev v p).mp hpq with ⟨hp, hget⟩
                  constructor
                  · rw [lastList_append_some]
                    refine Or.inr ⟨hp, ?_⟩
                    intro hcon
                    have hvx : v = x := Option.some.inj (Option.some.inj (hget.symm.trans hcon))
                    exact hxv hvx.symm
                  · rw [getElem_append_some_lt pre x p (lastList_lt_length hp)]
                    exact hget
              · rintro ⟨hpmem, hgetp⟩
                rw [lastList_append_some] at hpmem
                rcases hpmem with hp | ⟨hp, hneq⟩
                · subst p
                  have hvx : v = x := by
                    have hg := getElem_append_some_self pre x
                    rw [hg] at hgetp
                    exact Option.some.inj (Option.some.inj hgetp.symm)
                  subst v
                  simp
                · have hpelem : pre[p]? = some (some v) := by
                    rw [← getElem_append_some_lt pre x p (lastList_lt_length hp)]
                    exact hgetp
                  have hpq : prev[v]? = some p := (hprev v p).mpr ⟨hp, hpelem⟩
                  have hxv : x ≠ v := by
                    intro hx
                    subst v
                    exact hneq hpelem
                  simp [hxv, hpq]
            have hlen : (pre ++ [some x] : List (Option Int)).length = pre.length + 1 := by simp
            have hbound' : (pre ++ [some x]).length + as.length < 2 ^ d := by
              have hb' : pre.length + (as.length + 1) < 2 ^ d := by simpa using hbound
              rw [hlen]
              omega
            have hres := ih (pre ++ [some x]) (mark true tree pre.length d)
              (prev.insert x pre.length) hwf2 hprev1 hbound'
            simpa [runCells, step, hpx, List.append_assoc] using hres
          · have hjmem : j ∈ lastList pre ∧ pre[j]? = some (some x) := (hprev x j).mp hpx
            have hwf0 : WF (mark false tree j d) d ((lastList pre).erase j) :=
              WF_mark_false hwf j hjmem.1
            have hnot : pre.length ∉ (lastList pre).erase j := by
              intro hm
              have hm' := List.mem_of_mem_erase hm
              have hlt := lastList_lt_length hm'
              omega
            obtain ⟨ps', hwf1, hmem'⟩ :=
              WF_mark_true d (mark false tree j d) ((lastList pre).erase j) hwf0
                pre.length hblen hnot
            have hsame : ∀ q, q ∈ lastList pre → (pre[q]? = some (some x) ↔ q = j) := by
              intro q hqm
              constructor
              · intro hq
                have hpq : prev[x]? = some q := (hprev x q).mpr ⟨hqm, hq⟩
                exact (Option.some.inj (hpx.symm.trans hpq)).symm
              · intro hq
                subst q
                exact hjmem.2
            have hmemeq : ∀ q, q ∈ ps' ↔ q ∈ lastList (pre ++ [some x]) := by
              intro q
              rw [hmem', lastList_append_some]
              constructor
              · rintro (rfl | hq)
                · exact Or.inl rfl
                · have hq' := (List.Nodup.mem_erase_iff (nodup_lastList pre)).mp hq
                  refine Or.inr ⟨hq'.2, ?_⟩
                  intro hcon
                  exact hq'.1 ((hsame q hq'.2).mp hcon)
              · rintro (rfl | ⟨hqm, hneq⟩)
                · exact Or.inl rfl
                · refine Or.inr ((List.Nodup.mem_erase_iff (nodup_lastList pre)).mpr ⟨?_, hqm⟩)
                  intro hqj
                  subst q
                  exact hneq hjmem.2
            have hps : ps' = lastList (pre ++ [some x]) :=
              increasing_eq_of_mem_iff (WF.increasing hwf1)
                (lastList_increasing (pre ++ [some x])) hmemeq
            have hwf2 : WF (mark true (mark false tree j d) pre.length d) d
                (lastList (pre ++ [some x])) := WF.congr hwf1 hps
            have hprev1 : ∀ v p, (prev.insert x pre.length)[v]? = some p ↔
                p ∈ lastList (pre ++ [some x]) ∧ (pre ++ [some x])[p]? = some (some v) := by
              intro v p
              rw [Std.HashMap.getElem?_insert]
              constructor
              · intro h
                by_cases hxv : x = v
                · have hp : p = pre.length := by simpa [hxv] using h.symm
                  subst p
                  subst v
                  constructor
                  · rw [lastList_append_some]
                    exact Or.inl rfl
                  · exact getElem_append_some_self pre x
                · have hpq : prev[v]? = some p := by simpa [hxv] using h
                  rcases (hprev v p).mp hpq with ⟨hp, hget⟩
                  constructor
                  · rw [lastList_append_some]
                    refine Or.inr ⟨hp, ?_⟩
                    intro hcon
                    have hvx : v = x := Option.some.inj (Option.some.inj (hget.symm.trans hcon))
                    exact hxv hvx.symm
                  · rw [getElem_append_some_lt pre x p (lastList_lt_length hp)]
                    exact hget
              · rintro ⟨hpmem, hgetp⟩
                rw [lastList_append_some] at hpmem
                rcases hpmem with hp | ⟨hp, hneq⟩
                · subst p
                  have hvx : v = x := by
                    have hg := getElem_append_some_self pre x
                    rw [hg] at hgetp
                    exact Option.some.inj (Option.some.inj hgetp.symm)
                  subst v
                  simp
                · have hpelem : pre[p]? = some (some v) := by
                    rw [← getElem_append_some_lt pre x p (lastList_lt_length hp)]
                    exact hgetp
                  have hpq : prev[v]? = some p := (hprev v p).mpr ⟨hp, hpelem⟩
                  have hxv : x ≠ v := by
                    intro hx
                    subst v
                    exact hneq hpelem
                  simp [hxv, hpq]
            have hlen : (pre ++ [some x] : List (Option Int)).length = pre.length + 1 := by simp
            have hbound' : (pre ++ [some x]).length + as.length < 2 ^ d := by
              have hb' : pre.length + (as.length + 1) < 2 ^ d := by simpa using hbound
              rw [hlen]
              omega
            have hres := ih (pre ++ [some x])
              (mark true (mark false tree j d) pre.length d) (prev.insert x pre.length)
              hwf2 hprev1 hbound'
            simpa [runCells, step, hpx, List.append_assoc] using hres

/-! ## Main result -/

private theorem take_min_length (xs : List (Option Int)) (hi : Nat) :
    xs.take (min hi xs.length) = xs.take hi := by
  by_cases h : hi ≤ xs.length
  · rw [Nat.min_eq_left h]
  · have hlen : xs.length ≤ hi := by omega
    have hdrop : xs.drop hi = [] := List.drop_eq_nil_of_le hlen
    have htake : xs.take hi = xs := by
      have ht := List.take_append_drop hi xs
      rw [hdrop] at ht
      simpa using ht
    have hmin : min hi xs.length = xs.length := Nat.min_eq_right hlen
    rw [hmin, htake]
    have ht := List.take_append_drop xs.length xs
    rw [List.drop_length, List.append_nil] at ht
    simpa using ht

theorem sound : TinylambOpt.ResearchGoals.RangeDistinctIndex.Goal := by
  intro xs lo hi
  let n := xs.length
  let d := n.log2 + 1
  let k := min hi n
  have hnlt : n < 2 ^ d := by
    simpa [n, d] using length_lt_two_pow_log2_add_one xs.length
  have hk_le : k ≤ n := Nat.min_le_right hi n
  have hklt : k < 2 ^ d := Nat.lt_of_le_of_lt hk_le hnlt
  have hbound0 : ([] : List (Option Int)).length + xs.length < 2 ^ d := by
    simpa [n] using hnlt
  have hwf0 : WF Marks.empty d (lastList ([] : List (Option Int))) := by
    simpa [lastList] using (WF.empty d)
  have hprev0 : ∀ v p, ({} : Std.HashMap Int Nat)[v]? = some p ↔
      p ∈ lastList ([] : List (Option Int)) ∧
        ([] : List (Option Int))[p]? = some (some v) := by
    intro v p
    rw [Std.HashMap.getElem?_empty]
    simp [lastList]
  have hboundk : ([] : List (Option Int)).length + (xs.take k).length < 2 ^ d := by
    rw [List.length_take]
    simp only [List.length_nil, Nat.zero_add]
    change min k n < 2 ^ d
    rw [Nat.min_eq_left hk_le]
    exact hklt
  have hrun_k := runCells_wf (xs.take k) [] d Marks.empty {} hwf0 hprev0 hboundk
  have hwf_k : WF (runCells (xs.take k) [] d Marks.empty {}).1 d
      (lastList (xs.take k)) := by
    simpa using hrun_k.1
  have hsize : (build xs).roots.size = xs.length + 1 := by
    rw [build_eq_loop]
    change (loop (xs.zipIdx ([] : List (Option Int)).length) d Marks.empty {} #[Marks.empty]).roots.size = xs.length + 1
    rw [loop_result]
    simp [treesAfter_length]
  have hroots : (build xs).roots[k]? = some (runCells (xs.take k) [] d Marks.empty {}).1 := by
    rw [build_eq_loop]
    change (loop (xs.zipIdx ([] : List (Option Int)).length) d Marks.empty {} #[Marks.empty]).roots[k]? = _
    rw [loop_result]
    rw [array_singleton_append]
    rw [List.getElem?_toArray]
    have hta : treesAfter xs [] d Marks.empty {} =
        Marks.empty :: (treesAfter xs [] d Marks.empty {}).tail := by
      cases xs <;> rfl
    rw [← hta]
    exact treesAfter_take xs [] d Marks.empty {} k hk_le
  have hquery : query (build xs) lo hi =
      ((lastList (xs.take k)).filter fun p => lo ≤ p).length := by
    unfold query
    have hidx : min hi ((build xs).roots.size - 1) = k := by
      rw [hsize]
      simp [k, n]
    rw [hidx]
    rw [hroots]
    simp only [Option.getD_some]
    exact suffix_correct d _ _ hwf_k lo
  have href : reference xs lo hi =
      ((lastList (xs.take k)).filter fun p => lo ≤ p).length := by
    unfold reference
    rw [← take_min_length xs hi]
    change ((((xs.take k).drop lo).filterMap id).eraseDups).length = _
    rw [← length_lastList_eq_reference ((xs.take k).drop lo)]
    rw [← filter_lastList_eq_drop (xs.take k) lo]
  exact hquery.trans href.symm

end TinylambOpt.ResearchProofs.RangeDistinctIndex

-- C++ references to this model or its proof.
-- executor/detail/window_eval.cpp
-- executor/relational_regression_test_part11.cpp
