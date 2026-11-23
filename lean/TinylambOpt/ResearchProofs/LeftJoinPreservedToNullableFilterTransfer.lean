import TinylambOpt.ResearchGoals.LeftJoinPreservedToNullableFilterTransfer

namespace TinylambOpt.ResearchProofs.LeftJoinPreservedToNullableFilterTransfer

open TinylambOpt.ResearchGoals.LeftJoinPreservedToNullableFilterTransfer

/-- `filterMap` after filtering by `g` is unchanged when every element that
    survives `f` also satisfies `g`. -/
private theorem filterMap_filter_eq {α β : Type} (f : α → Option β) (g : α → Bool)
    (h : ∀ x, f x ≠ none → g x = true) :
    ∀ xs : List α, (xs.filter g).filterMap f = xs.filterMap f
  | [] => by simp
  | x :: xs => by
      by_cases hg : g x = true
      · rw [List.filter_cons_of_pos hg]
        cases hfx : f x with
        | none =>
            rw [List.filterMap_cons_none hfx, List.filterMap_cons_none hfx]
            exact filterMap_filter_eq f g h xs
        | some b =>
            rw [List.filterMap_cons_some hfx, List.filterMap_cons_some hfx]
            rw [filterMap_filter_eq f g h xs]
      · rw [List.filter_cons_of_neg hg]
        cases hfx : f x with
        | none =>
            rw [List.filterMap_cons_none hfx]
            exact filterMap_filter_eq f g h xs
        | some b =>
            exfalso
            exact hg (h x (by simp [hfx]))

/-- Congruence of `flatMap` over a list. -/
private theorem flatMap_congr_mem {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      rw [h x List.mem_cons_self,
        ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

private def hitsOf (lk : Int) (rs : List Row) : List Row :=
  rs.filterMap fun r =>
    match r.1 with
    | some rk => if lk == rk then some r else none
    | none => none

private def ljoinOne (l : Row) (rs : List Row) : List (Row × Option Row) :=
  let hits :=
    match l.1 with
    | none => []
    | some lk => hitsOf lk rs
  if hits.isEmpty then [(l, none)] else hits.map (fun r => (l, some r))

private theorem ljoin_eq_flatMap (ls rs : List Row) :
    ljoin ls rs = ls.flatMap (fun l => ljoinOne l rs) := by
  rfl

/-- Filtering the nullable side on the join key does not change the
    contribution of a preserved-side row that itself satisfies the key
    predicate. -/
private theorem ljoinOne_transfer (p : Int → Bool) (l : Row)
    (hl : liftOpt p l.1 = true) (rs : List Row) :
    ljoinOne l (rs.filter fun r => liftOpt p r.1) = ljoinOne l rs := by
  cases hlkey : l.1 with
  | none =>
      simp [hlkey, liftOpt] at hl
  | some lk =>
      have hp_lk : p lk = true := by simpa [hlkey, liftOpt] using hl
      have hfilter :
          hitsOf lk (rs.filter fun r => liftOpt p r.1) = hitsOf lk rs := by
        unfold hitsOf
        apply filterMap_filter_eq
        intro r hr
        cases hrkey : r.1 with
        | none => simp [hrkey] at hr
        | some rk =>
            by_cases hbeq : lk == rk
            · have heq : lk = rk := beq_iff_eq.mp hbeq
              simpa [hrkey, liftOpt, heq] using hp_lk
            · simp [hrkey, hbeq] at hr
      unfold ljoinOne
      rw [hlkey]
      dsimp
      rw [hfilter]

/-- Membership in the `Bool`-valued `contains`. -/
private theorem contains_eq_true_iff_mem {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (y : α) : xs.contains y = true ↔ y ∈ xs := by
  induction xs with
  | nil => simp
  | cons a as ih => simp [beq_iff_eq]

/-- Membership in the filtered list is characterised by membership in the
    original list plus the filter predicate (requires `LawfulBEq`). -/
private theorem contains_filter_iff {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (y : α) (p : α → Bool) :
    (xs.filter p).contains y = true ↔ xs.contains y = true ∧ p y = true := by
  simp [List.mem_filter]

/-- One step of the `foldl insertUniq` / `filter` homomorphism. -/
private theorem filter_insertUniq_step {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (y : α) (p : α → Bool) :
    (if p y = true then insertUniq (xs.filter p) y else xs.filter p) =
      (insertUniq xs y).filter p := by
  unfold insertUniq
  simp only [contains_eq_true_iff_mem]
  by_cases hpy : p y = true
  · simp only [hpy, ↓reduceIte]
    by_cases hyx : y ∈ xs
    · have hyfx : y ∈ xs.filter p := by simp [hyx, hpy]
      simp [hyx, hyfx]
    · have hyfx : y ∉ xs.filter p := by simp [hyx]
      simp [hyx, hyfx, List.filter_append, hpy]
  · by_cases hyx : y ∈ xs
    · simp [hpy, hyx]
    · simp [hpy, hyx, List.filter_append]

/-- Filtering commutes with the `insertUniq` fold. -/
private theorem foldl_insertUniq_filter {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (init : List α) (p : α → Bool) :
    (l.foldl insertUniq init).filter p =
      (l.filter p).foldl insertUniq (init.filter p) := by
  let skip : List α → α → List α := fun xs y => if p y = true then insertUniq xs y else xs
  have hstep : ∀ (xs : List α) (y : α), skip (xs.filter p) y = (insertUniq xs y).filter p := by
    intro xs y
    exact filter_insertUniq_step xs y p
  have hhom : l.foldl skip (init.filter p) = (l.foldl insertUniq init).filter p := by
    simpa [skip] using
      (List.foldl_hom (f := fun xs : List α => xs.filter p)
        (g₁ := insertUniq) (g₂ := skip) (l := l) (init := init) hstep)
  have hfilter : (l.filter p).foldl insertUniq (init.filter p) = l.foldl skip (init.filter p) := by
    simpa [skip] using
      (List.foldl_filter (p := p) (f := insertUniq) (l := l) (init := init.filter p))
  exact hhom.symm.trans hfilter.symm

theorem sound : TinylambOpt.ResearchGoals.LeftJoinPreservedToNullableFilterTransfer.Goal := by
  constructor
  · intro ls rs p
    let lsF := ls.filter (fun l => liftOpt p l.1)
    have hper : ∀ l ∈ lsF, ljoinOne l (rs.filter fun r => liftOpt p r.1) = ljoinOne l rs := by
      intro l hlmem
      have hl : liftOpt p l.1 = true := (List.mem_filter.mp hlmem).2
      exact ljoinOne_transfer p l hl rs
    dsimp
    rw [ljoin_eq_flatMap, ljoin_eq_flatMap]
    exact flatMap_congr_mem lsF _ _ hper
  constructor
  · intro α _ _ a b p
    unfold unionDistinct
    rw [foldl_insertUniq_filter]
    simp [List.filter_append, List.foldl_append]
  · let ls : List Row := [(some 1, 100)]
    let rs : List Row := [(some 1, 50)]
    let q : Int → Bool := fun v => decide (v > 99)
    refine ⟨ls, rs, q, ?_⟩
    decide

end TinylambOpt.ResearchProofs.LeftJoinPreservedToNullableFilterTransfer
