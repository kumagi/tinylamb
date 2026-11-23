import TinylambOpt.ResearchGoals.SkylineAntiJoin

/-!
# Soundness of the one-sort anti-join sweep

`sweep` sorts rows by `key r = r.x.getD 0` and folds `visit`, which keeps the
minimum `y` of the current `x`-group (`groupMin`) separate from the minimum
`y` over all strictly smaller `x` groups (`previousMin`).  The invariant
`Inv P s` says that `s` is the correct sweep state for the sorted prefix `P`.
Rows whose `x` or `y` is `none` (NULL) never dominate and are never dominated:
they are always kept and leave the two minima unchanged.
`sound` assembles `visit_inv` + `go_inv` with `List.mergeSort_perm` and
`List.pairwise_mergeSort`.
-/

namespace TinylambOpt.ResearchProofs.SkylineAntiJoin

open TinylambOpt.ResearchGoals.SkylineAntiJoin

private def key (r : Row) : Int := r.x.getD 0

private def ltX (a b : Option Int) : Bool :=
  match a, b with
  | some x, some y => x < y
  | _, _ => false

private def allSome (P : List Row) : List Row := P.filter (fun r => r.x.isSome)

private def minWith : Option Int → Option Int → Option Int
  | a, none => a
  | none, some y => some y
  | some m, some y => some (min m y)

private theorem bool_eq_of_iff {p q : Bool} (h : (p = true) ↔ (q = true)) : p = q := by
  cases p <;> cases q <;> simp_all

private theorem bool_eq_false_of_not {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b <;> simp_all

/-! ### `min` / `minWith` -/

private theorem int_min_lt {a b z : Int} : min a b < z ↔ a < z ∨ b < z := by
  by_cases h : a ≤ b <;> simp [Int.min_def, h] <;> omega

private theorem int_min_left_comm (a b c : Int) : min a (min b c) = min b (min a c) := by
  rw [← Int.min_assoc, Int.min_comm a b, Int.min_assoc]

private theorem lower_eq_minWith (m : Option Int) (y : Int) : lower m y = minWith m (some y) := by
  cases m <;> simp [lower, minWith]

private theorem minWith_some (m y : Int) : minWith (some m) (some y) = some (min m y) := rfl
private theorem minWith_none_left (a : Option Int) : minWith none a = a := by cases a <;> rfl
private theorem minWith_none_right (a : Option Int) : minWith a none = a := by cases a <;> rfl

private theorem minWith_comm (a b : Option Int) : minWith a b = minWith b a := by
  cases a <;> cases b <;>
    simp only [minWith_some, minWith_none_left, minWith_none_right, Option.some.injEq]
  <;> omega

private theorem minWith_assoc (a b c : Option Int) :
    minWith (minWith a b) c = minWith a (minWith b c) := by
  cases a <;> cases b <;> cases c <;>
    simp only [minWith_some, minWith_none_left, minWith_none_right, Option.some.injEq]
  <;> omega

private theorem minWith_lt2 {a b : Option Int} {z : Int} :
    (minWith a b).any (· < z) ↔ a.any (· < z) ∨ b.any (· < z) := by
  cases a <;> cases b <;> simp [minWith, int_min_lt]

private theorem minWith_elim (g p : Option Int) : g.elim p (lower p) = minWith p g := by
  cases g <;> cases p <;> simp [lower, minWith]

/-! ### `ysMin`: the running minimum over the `some` entries of `y` -/

private def ysMin (rows : List Row) : Option Int :=
  rows.foldl (fun acc r => minWith acc r.y) none

private theorem perm_fold {α : Type} (step : Option Int → α → Option Int)
    (hcomm : ∀ a x y, step (step a x) y = step (step a y) x)
    {l₁ l₂ : List α} (h : l₁.Perm l₂) (a : Option Int) :
    l₁.foldl step a = l₂.foldl step a := by
  induction h generalizing a with
  | nil => rfl
  | cons x h ih => simpa using ih (step a x)
  | swap x y l => simp only [List.foldl_cons]; rw [hcomm]
  | trans h₁ h₂ ih₁ ih₂ => exact (ih₁ a).trans (ih₂ a)

private theorem step_comm (a : Option Int) (r s : Row) :
    minWith (minWith a r.y) s.y = minWith (minWith a s.y) r.y := by
  cases a <;> cases r.y <;> cases s.y <;>
    simp only [minWith_some, minWith_none_left, minWith_none_right, Option.some.injEq]
  <;> omega

private theorem ysMin_perm {l₁ l₂ : List Row} (h : l₁.Perm l₂) : ysMin l₁ = ysMin l₂ := by
  unfold ysMin
  exact perm_fold (fun a r => minWith a r.y) step_comm h none

private theorem ysMin_nil : ysMin [] = none := rfl

private theorem fold_minWith (rows : List Row) (acc : Option Int) :
    rows.foldl (fun a r => minWith a r.y) acc =
      minWith acc (rows.foldl (fun a r => minWith a r.y) none) := by
  induction rows generalizing acc with
  | nil => show acc = minWith acc none; exact (minWith_none_right acc).symm
  | cons r rs ih =>
      rw [List.foldl_cons, List.foldl_cons, minWith_none_left, ih (minWith acc r.y), ih r.y,
        minWith_assoc]

private theorem ysMin_cons (r : Row) (rs : List Row) :
    ysMin (r :: rs) = minWith r.y (ysMin rs) := by
  unfold ysMin
  rw [List.foldl_cons, minWith_none_left, fold_minWith]

private theorem ysMin_append (l₁ l₂ : List Row) :
    ysMin (l₁ ++ l₂) = minWith (ysMin l₁) (ysMin l₂) := by
  unfold ysMin
  rw [List.foldl_append, fold_minWith]

private theorem ysMin_lt_iff {rows : List Row} {y : Int} :
    (ysMin rows).any (· < y) ↔ ∃ r ∈ rows, ∃ v, r.y = some v ∧ v < y := by
  induction rows with
  | nil => simp [ysMin_nil]
  | cons r rs ih =>
      rw [ysMin_cons, minWith_lt2]
      constructor
      · intro h
        rcases h with h | h
        · cases hy : r.y with
          | none => simp [hy] at h
          | some v =>
              simp [hy] at h
              exact ⟨r, List.Mem.head _, v, hy, h⟩
        · rcases ih.mp h with ⟨a, ha, v, hv, hvy⟩
          exact ⟨a, List.Mem.tail _ ha, v, hv, hvy⟩
      · intro h
        rcases h with ⟨a, ha, v, hv, hvy⟩
        rcases List.mem_cons.mp ha with rfl | ha
        · left; simpa [hv] using hvy
        · right; exact ih.mpr ⟨a, ha, v, hv, hvy⟩

private theorem ysMin_of_none {L : List Row} (h : ∀ r ∈ L, r.y = none) : ysMin L = none := by
  induction L with
  | nil => exact ysMin_nil
  | cons r rs ih =>
      rw [ysMin_cons, h r (List.Mem.head _), minWith_none_left,
        ih (fun q hq => h q (List.Mem.tail _ hq))]

private theorem ysMin_singleton (r : Row) : ysMin [r] = r.y := by
  show minWith none r.y = r.y
  cases r.y <;> rfl

private theorem ysMin_yfilter {L : List Row} :
    ysMin L = ysMin (L.filter (fun r => (r.y).isSome)) := by
  induction L with
  | nil => exact ysMin_nil
  | cons r rs ih =>
      rw [ysMin_cons, ih, List.filter_cons]
      cases ry : r.y
      · simp [minWith_none_left]
      · simp [ry, ysMin_cons]

private theorem ysMin_filter_add {p : Row → Bool} {L : List Row} {r : Row}
    (hpr : p r = false) : ysMin ((L ++ [r]).filter p) = ysMin (L.filter p) := by
  have h1 : ([r] : List Row).filter p = [] := by
    rw [List.filter_cons, hpr, List.filter_nil]
    simp
  rw [List.filter_append, h1, List.append_nil]

private theorem ysMin_filter_add_none {p : Row → Bool} {L : List Row} {r : Row}
    (hr : r.y = none) : ysMin ((L ++ [r]).filter p) = ysMin (L.filter p) := by
  cases hpr : p r
  · exact ysMin_filter_add hpr
  · rw [List.filter_append, ysMin_append]
    have h1 : ([r] : List Row).filter p = [r] := by
      rw [List.filter_cons, hpr]
      simp
    rw [h1, ysMin_singleton, hr, minWith_none_right]

/-! ### Dominance facts -/

private theorem dom_false_left {a r : Row} (ha : a.x = none ∨ a.y = none) :
    dominates a r = false := by
  cases a with
  | mk ax ay aid =>
      cases ax with
      | none => rfl
      | some u =>
          cases ay with
          | none => rfl
          | some v =>
              rcases ha with hx | hy
              · exact absurd hx (by simp)
              · exact absurd hy (by simp)

private theorem dom_false_right {a r : Row} (hr : r.x = none ∨ r.y = none) :
    dominates a r = false := by
  cases a with
  | mk ax ay aid =>
      cases ax with
      | none => rfl
      | some u =>
          cases ay with
          | none => rfl
          | some v =>
              cases r with
              | mk rx ry rid =>
                  cases rx with
                  | none => rfl
                  | some w =>
                      cases ry with
                      | none => rfl
                      | some z =>
                          rcases hr with hx | hy
                          · exact absurd hx (by simp)
                          · exact absurd hy (by simp)

private theorem dom_self (x y : Int) (id : Nat) :
    dominates (⟨some x, some y, id⟩ : Row) ⟨some x, some y, id⟩ = false := by
  simp [dominates, Int.lt_irrefl]

private theorem any_dom_iff (P : List Row) (x y : Int) :
    P.any (dominates · ⟨some x, some y, 0⟩) = true ↔
      (ysMin (P.filter (fun r => ltX r.x (some x)))).any (· < y) := by
  rw [List.any_eq_true, ysMin_lt_iff]
  constructor
  · rintro ⟨a, ha, hdom⟩
    cases ax : a.x with
    | none => simp [dominates, ax] at hdom
    | some bx =>
        cases ay : a.y with
        | none => simp [dominates, ax, ay] at hdom
        | some v =>
            simp [dominates, ax, ay] at hdom
            obtain ⟨hbx, hvy⟩ := hdom
            refine ⟨a, ?_, v, ay, hvy⟩
            rw [List.mem_filter]
            exact ⟨ha, by simp [ltX, ax, hbx]⟩
  · rintro ⟨a, ha, v, hv, hvy⟩
    obtain ⟨haP, hlt⟩ := List.mem_filter.mp ha
    cases ax : a.x with
    | none => simp [ltX, ax] at hlt
    | some bx =>
        simp [ltX, ax] at hlt
        exact ⟨a, haP, by simp [dominates, ax, hv, hlt, hvy]⟩

/-! ### Filter bookkeeping for appending one row -/

private theorem perm_cons_append {α : Type} (xs : List α) (x : α) :
    (x :: xs).Perm (xs ++ [x]) := by
  induction xs with
  | nil => exact List.Perm.refl _
  | cons a as ih => exact (List.Perm.swap a x as).trans (List.Perm.cons a ih)

private theorem any_append_last (L : List Row) (r b : Row) :
    (L ++ [r]).any (dominates · b) = (L.any (dominates · b) || dominates r b) := by
  rw [List.any_append, List.any_cons, List.any_nil, Bool.or_false]

private theorem nondom_any_false (L : List Row) {r : Row} (hr : r.x = none ∨ r.y = none) :
    L.any (dominates · r) = false := by
  refine bool_eq_of_iff ?_
  rw [List.any_eq_true]
  constructor
  · intro ⟨b, hb, hdom⟩
    rw [dom_false_right (a := b) hr] at hdom
    exact absurd hdom (by decide)
  · intro h
    exact absurd h (by decide)

private theorem filter_append_nondom (L : List Row) {r : Row} (hr : r.x = none ∨ r.y = none) :
    (L ++ [r]).filter (fun b => !(L ++ [r]).any (dominates · b)) =
      L.filter (fun b => !L.any (dominates · b)) ++ [r] := by
  have hL : ∀ b ∈ L, (!(L ++ [r]).any (dominates · b)) = (!(L.any (dominates · b))) := by
    intro b hb
    rw [any_append_last L r b, dom_false_left hr, Bool.or_false]
  have h1 : ([r] : List Row).filter (fun b => !(L ++ [r]).any (dominates · b)) = [r] := by
    have h : (L ++ [r]).any (dominates · r) = false := by
      rw [any_append_last L r r, dom_false_left hr, nondom_any_false L hr, Bool.or_false]
    simp [List.filter, h]
  have h2 : (L : List Row).filter (fun b => !(L ++ [r]).any (dominates · b))
      = L.filter (fun b => !(L.any (dominates · b))) := List.filter_congr hL
  rw [List.filter_append, h2, h1]

private theorem filter_new_dominates (L : List Row) (r : Row)
    (hnd : ∀ b ∈ L, dominates r b = false) (hr : L.any (dominates · r) = true) :
    (L ++ [r]).filter (fun b => !(L ++ [r]).any (dominates · b)) =
      L.filter (fun b => !L.any (dominates · b)) := by
  have hL : ∀ b ∈ L, (!(L ++ [r]).any (dominates · b)) = (!(L.any (dominates · b))) := by
    intro b hb
    rw [any_append_last L r b, hnd b hb, Bool.or_false]
  have h1 : ([r] : List Row).filter (fun b => !(L ++ [r]).any (dominates · b)) = [] := by
    have h : (L ++ [r]).any (dominates · r) = true := by
      rw [any_append_last L r r, hr, Bool.true_or]
    simp [List.filter, h]
  have h2 : (L : List Row).filter (fun b => !(L ++ [r]).any (dominates · b))
      = L.filter (fun b => !(L.any (dominates · b))) := List.filter_congr hL
  rw [List.filter_append, h2, h1, List.append_nil]

private theorem filter_new_kept (L : List Row) (r : Row)
    (hnd : ∀ b ∈ L, dominates r b = false) (hr : L.any (dominates · r) = false)
    (hrr : dominates r r = false) :
    (L ++ [r]).filter (fun b => !(L ++ [r]).any (dominates · b)) =
      L.filter (fun b => !L.any (dominates · b)) ++ [r] := by
  have hL : ∀ b ∈ L, (!(L ++ [r]).any (dominates · b)) = (!(L.any (dominates · b))) := by
    intro b hb
    rw [any_append_last L r b, hnd b hb, Bool.or_false]
  have h1 : ([r] : List Row).filter (fun b => !(L ++ [r]).any (dominates · b)) = [r] := by
    have h : (L ++ [r]).any (dominates · r) = false := by
      rw [any_append_last L r r, hrr, hr, Bool.or_false]
    simp [List.filter, h]
  have h2 : (L : List Row).filter (fun b => !(L ++ [r]).any (dominates · b))
      = L.filter (fun b => !(L.any (dominates · b))) := List.filter_congr hL
  rw [List.filter_append, h2, h1]

/-! ### The sweep invariant -/

private def Inv (P : List Row) (s : State) : Prop :=
  s.kept.Perm (P.filter fun b => ! P.any (dominates · b)) ∧
  (s.group = none → s.groupMin = none ∧ s.previousMin = none ∧
    (∀ r ∈ P, r.x = none ∨ r.y = none)) ∧
  (∀ gx, s.group = some gx →
    (∀ r ∈ P, r.x = none ∨ r.y = none ∨ key r ≤ gx) ∧
    s.groupMin = ysMin (P.filter (fun r => r.x == some gx)) ∧
    s.previousMin = ysMin (P.filter (fun r => ltX r.x (some gx))) ∧
    ysMin (allSome P) = minWith s.previousMin s.groupMin ∧
    (∃ r ∈ P, r.x = some gx))

private theorem visit_inv_nondom {P : List Row} {s : State} {r : Row}
    (hInv : Inv P s) (hr : r.x = none ∨ r.y = none) :
    Inv (P ++ [r]) (visit s r) := by
  rcases hInv with ⟨hkept, hnone, hsome⟩
  have hvisit : visit s r = { s with kept := r :: s.kept } := by
    cases hx : r.x with
    | none => simp [visit, hx]
    | some x =>
        cases ry : r.y with
        | none => simp [visit, ry]
        | some y =>
            exfalso
            rcases hr with h | h
            · exact absurd h (by simp [hx])
            · exact absurd h (by simp [ry])
  refine ⟨?_, ?_, ?_⟩
  · rw [hvisit, filter_append_nondom P hr]
    exact (hkept.cons r).trans (perm_cons_append _ _)
  · intro hg
    have hg' : s.group = none := by simpa [hvisit] using hg
    obtain ⟨hgm, hpm, hall⟩ := hnone hg'
    rw [hvisit]
    exact ⟨hgm, hpm, fun a ha => by
      rcases List.mem_append.mp ha with ha | h1
      · exact hall a ha
      · obtain rfl : a = (r : Row) := List.mem_singleton.mp h1
        exact hr⟩
  · intro gx hg
    have hg' : s.group = some gx := by simpa [hvisit] using hg
    obtain ⟨hbnd, hgm, hpm, hall, hw⟩ := hsome gx hg'
    have hgm2 : ysMin ((P ++ [r]).filter (fun q => q.x == some gx)) =
        ysMin (P.filter (fun q => q.x == some gx)) := by
      rcases hr with hx | hy
      · exact ysMin_filter_add (by simp [hx])
      · exact ysMin_filter_add_none hy
    have hpm2 : ysMin ((P ++ [r]).filter (fun q => ltX q.x (some gx))) =
        ysMin (P.filter (fun q => ltX q.x (some gx))) := by
      rcases hr with hx | hy
      · exact ysMin_filter_add (by simp [ltX, hx])
      · exact ysMin_filter_add_none hy
    have hall2 : ysMin (allSome (P ++ [r])) = ysMin (allSome P) := by
      rcases hr with hx | hy
      · exact ysMin_filter_add (by simp [hx])
      · exact ysMin_filter_add_none hy
    refine ⟨fun a ha => by
        rcases List.mem_append.mp ha with ha | h1
        · exact hbnd a ha
        · obtain rfl : a = (r : Row) := List.mem_singleton.mp h1
          exact hr.imp_right Or.inl, ?_, ?_, ?_, ?_⟩
    · rw [hvisit, hgm2]; exact hgm
    · rw [hvisit, hpm2]; exact hpm
    · rw [hvisit, hall2]; exact hall
    · obtain ⟨w, hw, hwx⟩ := hw
      exact ⟨w, List.mem_append.mpr (Or.inl hw), hwx⟩

private theorem visit_inv_somesome {P : List Row} {s : State} {x y : Int} {id : Nat}
    (hInv : Inv P s) (hkey : ∀ a ∈ P, key a ≤ x) :
    Inv (P ++ [⟨some x, some y, id⟩]) (visit s ⟨some x, some y, id⟩) := by
  rcases hInv with ⟨hkept, hnone, hsome⟩
  have hdcongr : (fun a => dominates a ⟨some x, some y, id⟩) =
      (fun a => dominates a ⟨some x, some y, 0⟩) := by
    funext a
    cases a with
    | mk ax ay aid => cases ax <;> cases ay <;> simp [dominates]
  by_cases hchg : (s.group == some x) = true
  · -- same group as the previous rows: `previousMin` stands, `groupMin` absorbs `y`
    have hgroup : s.group = some x := by simpa [beq_iff_eq] using hchg
    obtain ⟨hbnd, hgm, hpm, hall, hw⟩ := hsome x hgroup
    have hvisit : visit s ⟨some x, some y, id⟩ =
        ⟨some x, lower s.groupMin y, s.previousMin,
          if (s.previousMin).any (fun m => m < y) then s.kept
          else (⟨some x, some y, id⟩ : Row) :: s.kept⟩ := by
      cases hpv : (s.previousMin).any (fun m => m < y) <;> simp [visit, hgroup, hpv]
    have hdom2 : P.any (dominates · ⟨some x, some y, id⟩) =
        (s.previousMin).any (fun m => m < y) := by
      refine bool_eq_of_iff ?_
      rw [hdcongr, any_dom_iff P x y, ← hpm]
    have hnotdom : ∀ b ∈ P, dominates (⟨some x, some y, id⟩ : Row) b = false := by
      intro b hb
      cases bx : b.x with
      | none => simp [dominates, bx]
      | some v =>
          cases byv : b.y with
          | none => simp [dominates, bx, byv]
          | some w =>
              have hle : v ≤ x := by
                rcases hbnd b hb with h | h | h
                · exact absurd h (by simp [bx])
                · exact absurd h (by simp [byv])
                · simpa [bx, key] using h
              rw [dominates, bx, byv]
              simp [decide_eq_true_eq]
              omega
    refine ⟨?_, ?_, ?_⟩
    · rw [hvisit]
      show (if (s.previousMin).any (fun m => m < y) = true then s.kept
          else (⟨some x, some y, id⟩ : Row) :: s.kept).Perm _
      by_cases hd : P.any (dominates · ⟨some x, some y, id⟩) = true
      · have hpv : (s.previousMin).any (fun m => m < y) = true := by rw [← hdom2]; exact hd
        rw [hpv, ite_eq_left rfl]
        rw [filter_new_dominates P ⟨some x, some y, id⟩ hnotdom hd]
        exact hkept
      · have hd' : P.any (dominates · ⟨some x, some y, id⟩) = false := bool_eq_false_of_not hd
        have hpv : (s.previousMin).any (fun m => m < y) = false := by rw [← hdom2]; exact hd'
        rw [hpv, ite_eq_right (by decide)]
        rw [filter_new_kept P ⟨some x, some y, id⟩ hnotdom hd' (dom_self x y id)]
        exact (hkept.cons ⟨some x, some y, id⟩).trans (perm_cons_append _ _)
    · rw [hvisit]
      simp
    · intro gx2 hg2
      rw [hvisit] at hg2
      simp at hg2
      subst hg2
      have hfr : ([⟨some x, some y, id⟩] : List Row).filter (fun q => q.x == some x) =
          [⟨some x, some y, id⟩] := by simp
      have hbnd' : ∀ a ∈ P ++ [⟨some x, some y, id⟩],
          a.x = none ∨ a.y = none ∨ key a ≤ x := by
        intro a ha
        rcases List.mem_append.mp ha with ha | h1
        · exact hbnd a ha
        · obtain rfl : a = (⟨some x, some y, id⟩ : Row) := List.mem_singleton.mp h1
          exact Or.inr (Or.inr (by simp [key]))
      have hgmS : ysMin ((P ++ ([⟨some x, some y, id⟩] : List Row)).filter (fun q => q.x == some x)) =
          minWith s.groupMin (some y) := by
        rw [List.filter_append, hfr, ysMin_append, ysMin_singleton, ← hgm]
      have hpmA : ysMin ((P ++ ([⟨some x, some y, id⟩] : List Row)).filter (fun q => ltX q.x (some x))) =
          s.previousMin := by
        rw [ysMin_filter_add (by simp only [ltX]; exact decide_eq_false (Int.lt_irrefl _)), hpm]
      have hallA : ysMin (allSome (P ++ [⟨some x, some y, id⟩])) =
          minWith s.previousMin (minWith s.groupMin (some y)) := by
        have hsp : allSome (P ++ [⟨some x, some y, id⟩]) =
            allSome P ++ [⟨some x, some y, id⟩] := by simp [allSome]
        rw [hsp, ysMin_append, hall, ysMin_singleton, minWith_assoc]
      exact ⟨hbnd', by rw [hvisit, hgmS]; simp [lower_eq_minWith],
        by rw [hvisit, hpmA],
        by rw [hvisit, hallA]; simp [lower_eq_minWith],
        ⟨⟨some x, some y, id⟩, List.mem_append.mpr (Or.inr (List.mem_singleton.mpr rfl)), rfl⟩⟩
  · -- switched to a new group: the old `groupMin` merges into `previousMin`
    have hgroupne : (s.group == some x) = false := bool_eq_false_of_not hchg
    by_cases hgn : s.group = none
    · -- first non-NULL group ever seen: every row so far has a NULL x or y
      obtain ⟨hgm0, hpm0, hall0⟩ := hnone hgn
      have hfil : ysMin (P.filter (fun q => ltX q.x (some x))) = none := by
        apply ysMin_of_none
        intro q hq
        obtain ⟨hqP, hlt⟩ := List.mem_filter.mp hq
        rcases hall0 q hqP with h | h
        · rw [h] at hlt
          simp [ltX] at hlt
        · exact h
      have hallP : ysMin (allSome P) = none := by
        apply ysMin_of_none
        intro q hq
        obtain ⟨hqP, hxS⟩ := List.mem_filter.mp hq
        rcases hall0 q hqP with h | h
        · rw [h] at hxS
          simp at hxS
        · exact h
      have hvisit : visit s ⟨some x, some y, id⟩ =
          ⟨some x, some y, none, (⟨some x, some y, id⟩ : Row) :: s.kept⟩ := by
        simp [visit, hgn, hgm0, hpm0, lower]
      have hnotdom : ∀ b ∈ P, dominates (⟨some x, some y, id⟩ : Row) b = false := by
        intro b hb
        rcases hall0 b hb with h | h
        · simp [dominates, h]
        · simp [dominates, h]
      have hdom : P.any (dominates · ⟨some x, some y, id⟩) = false := by
        refine bool_eq_of_iff ?_
        rw [hdcongr, any_dom_iff P x y, hfil]
        simp [Option.any]
      refine ⟨?_, ?_, ?_⟩
      · rw [hvisit, filter_new_kept P ⟨some x, some y, id⟩ hnotdom hdom (dom_self x y id)]
        exact (hkept.cons ⟨some x, some y, id⟩).trans (perm_cons_append _ _)
      · rw [hvisit]
        simp
      · intro gx2 hg2
        rw [hvisit] at hg2
        simp at hg2
        subst hg2
        have hgm0' : ysMin (P.filter (fun q => q.x == some x)) = none := by
          apply ysMin_of_none
          intro q hq
          obtain ⟨hqP, hbeq⟩ := List.mem_filter.mp hq
          rcases hall0 q hqP with h | h
          · rw [h] at hbeq
            simp at hbeq
          · exact h
        have hfr : ([⟨some x, some y, id⟩] : List Row).filter (fun q => q.x == some x) =
            [⟨some x, some y, id⟩] := by simp
        have hbnd' : ∀ a ∈ P ++ [⟨some x, some y, id⟩],
            a.x = none ∨ a.y = none ∨ key a ≤ x := by
          intro a ha
          rcases List.mem_append.mp ha with ha | h1
          · rcases hall0 a ha with h | h
            · exact Or.inl h
            · exact Or.inr (Or.inl h)
          · obtain rfl : a = (⟨some x, some y, id⟩ : Row) := List.mem_singleton.mp h1
            exact Or.inr (Or.inr (by simp [key]))
        exact ⟨hbnd',
          by
            rw [hvisit, List.filter_append, hfr, ysMin_append, hgm0', ysMin_singleton]
            simp [minWith_none_left],
          by rw [hvisit, ysMin_filter_add (by simp only [ltX]; exact decide_eq_false (Int.lt_irrefl _)), hfil],
          by
            rw [hvisit, show allSome (P ++ [⟨some x, some y, id⟩]) =
                allSome P ++ [⟨some x, some y, id⟩] from by simp [allSome],
              ysMin_append, hallP, ysMin_singleton],
          ⟨⟨some x, some y, id⟩, List.mem_append.mpr (Or.inr (List.mem_singleton.mpr rfl)), rfl⟩⟩
    · -- switched from an older group `ggx`
      rcases (show s.group = none ∨ ∃ ggx, s.group = some ggx from by
          cases s.group with
          | none => exact Or.inl rfl
          | some ggx => exact Or.inr ⟨ggx, rfl⟩) with h | ⟨ggx, hgroup⟩
      · exact absurd h hgn
      · obtain ⟨hbnd, hgm, hpm, hall, hw⟩ := hsome ggx hgroup
        have hgxne : ggx ≠ x := by
          intro h
          rw [← h] at hgroupne
          exact absurd hgroup (by simpa using hgroupne)
        obtain ⟨w, hw', hwx⟩ := hw
        have hgxle : ggx ≤ x := by
          have hk := hkey w hw'
          simpa [key, hwx] using hk
        have hglt : ggx < x := by omega
        have hprev : (s.groupMin).elim s.previousMin (lower s.previousMin) =
            minWith s.previousMin s.groupMin := minWith_elim s.groupMin s.previousMin
        have hys : ysMin (P.filter (fun q => ltX q.x (some x))) = ysMin (allSome P) := by
          have hpeq : (P.filter (fun q => ltX q.x (some x))).filter (fun q => (q.y).isSome) =
              (allSome P).filter (fun q => (q.y).isSome) := by
            unfold allSome
            rw [List.filter_filter, List.filter_filter]
            apply List.filter_congr
            intro b hb
            cases bx : b.x with
            | none => simp [ltX]
            | some v =>
                cases byv : b.y with
                | none => simp
                | some w =>
                    have hle : v ≤ ggx := by
                      rcases hbnd b hb with h | h | h
                      · exact absurd h (by simp [bx])
                      · exact absurd h (by simp [byv])
                      · simpa [bx, key] using h
                    have hlt : v < x := by omega
                    simp [ltX, hlt]
          rw [ysMin_yfilter, hpeq, ← ysMin_yfilter]
        have hdom2 : P.any (dominates · ⟨some x, some y, id⟩) =
            ((s.groupMin).elim s.previousMin (lower s.previousMin)).any (fun m => m < y) := by
          refine bool_eq_of_iff ?_
          rw [hdcongr, any_dom_iff P x y, hys, hall, ← hprev]
        have hnotdom : ∀ b ∈ P, dominates (⟨some x, some y, id⟩ : Row) b = false := by
          intro b hb
          cases bx : b.x with
          | none => simp [dominates, bx]
          | some v =>
              cases byv : b.y with
              | none => simp [dominates, bx, byv]
              | some w =>
                  have hle : v ≤ ggx := by
                    rcases hbnd b hb with h | h | h
                    · exact absurd h (by simp [bx])
                    · exact absurd h (by simp [byv])
                    · simpa [bx, key] using h
                  rw [dominates, bx, byv]
                  simp [decide_eq_true_eq]
                  omega
        have hbnd' : ∀ a ∈ P ++ [⟨some x, some y, id⟩],
            a.x = none ∨ a.y = none ∨ key a ≤ x := by
          intro a ha
          rcases List.mem_append.mp ha with ha | h1
          · exact (hbnd a ha).imp_right (fun h => h.imp_right (fun hk => by omega))
          · obtain rfl : a = (⟨some x, some y, id⟩ : Row) := List.mem_singleton.mp h1
            exact Or.inr (Or.inr (by simp [key]))
        have hgmP : ysMin (P.filter (fun q => q.x == some x)) = none := by
          apply ysMin_of_none
          intro q hq
          obtain ⟨hqP, hbeq⟩ := List.mem_filter.mp hq
          rcases hbnd q hqP with h | h | h
          · simp [h] at hbeq
          · exact h
          · exfalso
            have hqeq : q.x = some x := beq_iff_eq.mp hbeq
            have h1 : (q.x).getD 0 ≤ ggx := h
            rw [hqeq] at h1
            have hk : (x : Int) ≤ ggx := h1
            omega
        have hvisit : visit s ⟨some x, some y, id⟩ =
            ⟨some x, lower none y,
              (s.groupMin).elim s.previousMin (lower s.previousMin),
              if ((s.groupMin).elim s.previousMin (lower s.previousMin)).any (fun m => m < y)
                then s.kept else (⟨some x, some y, id⟩ : Row) :: s.kept⟩ := by
          simp [visit, hgroup, hgxne]
          cases hpv : ((s.groupMin).elim s.previousMin (lower s.previousMin)).any
              (fun m => m < y) <;> simp
        refine ⟨?_, ?_, ?_⟩
        · rw [hvisit]
          show (if ((s.groupMin).elim s.previousMin (lower s.previousMin)).any (fun m => m < y) = true
              then s.kept else (⟨some x, some y, id⟩ : Row) :: s.kept).Perm _
          by_cases hd : P.any (dominates · ⟨some x, some y, id⟩) = true
          · have hpv : ((s.groupMin).elim s.previousMin (lower s.previousMin)).any
              (fun m => m < y) = true := by rw [← hdom2]; exact hd
            rw [hpv, ite_eq_left rfl]
            rw [filter_new_dominates P ⟨some x, some y, id⟩ hnotdom hd]
            exact hkept
          · have hd' : P.any (dominates · ⟨some x, some y, id⟩) = false := bool_eq_false_of_not hd
            have hpv : ((s.groupMin).elim s.previousMin (lower s.previousMin)).any
                (fun m => m < y) = false := by rw [← hdom2]; exact hd'
            rw [hpv, ite_eq_right (by decide)]
            rw [filter_new_kept P ⟨some x, some y, id⟩ hnotdom hd' (dom_self x y id)]
            exact (hkept.cons ⟨some x, some y, id⟩).trans (perm_cons_append _ _)
        · rw [hvisit]
          simp
        · intro gx2 hg2
          rw [hvisit] at hg2
          simp at hg2
          subst hg2
          have hfr : ([⟨some x, some y, id⟩] : List Row).filter (fun q => q.x == some x) =
              [⟨some x, some y, id⟩] := by simp
          have hgmS : ysMin ((P ++ ([⟨some x, some y, id⟩] : List Row)).filter (fun q => q.x == some x)) =
              lower none y := by
            rw [List.filter_append, hfr, ysMin_append, hgmP]
            simp only [ysMin_singleton, lower_eq_minWith, minWith_none_left]
          have hpmS : ysMin ((P ++ ([⟨some x, some y, id⟩] : List Row)).filter (fun q => ltX q.x (some x))) =
              minWith s.previousMin s.groupMin := by
            rw [ysMin_filter_add (by simp only [ltX]; exact decide_eq_false (Int.lt_irrefl _)), hys, hall]
          have hallS : ysMin (allSome (P ++ [⟨some x, some y, id⟩])) =
              minWith (minWith s.previousMin s.groupMin) (lower none y) := by
            have hsp : allSome (P ++ [⟨some x, some y, id⟩]) =
                allSome P ++ [⟨some x, some y, id⟩] := by simp [allSome]
            rw [hsp, ysMin_append, hall, ysMin_singleton, lower_eq_minWith,
              minWith_none_left]
          exact ⟨hbnd', by rw [hvisit, hgmS], by rw [hvisit, hpmS, ← hprev],
            by rw [hvisit, hallS, ← hprev],
            ⟨⟨some x, some y, id⟩, List.mem_append.mpr (Or.inr (List.mem_singleton.mpr rfl)), rfl⟩⟩

private theorem visit_inv {P : List Row} {s : State} {r : Row}
    (hInv : Inv P s) (hkey : ∀ a ∈ P, key a ≤ key r) :
    Inv (P ++ [r]) (visit s r) := by
  cases r with
  | mk rx ry rid =>
      cases rx with
      | none => exact visit_inv_nondom hInv (Or.inl rfl)
      | some x =>
          cases ry with
          | none => exact visit_inv_nondom hInv (Or.inr rfl)
          | some y =>
              refine visit_inv_somesome (id := rid) hInv ?_
              intro a ha
              simpa [key] using hkey a ha

private theorem go_inv (P : List Row) (s : State) (L : List Row)
    (hInv : Inv P s) (hsep : ∀ a ∈ P, ∀ b ∈ L, key a ≤ key b)
    (hsorted : L.Pairwise (fun a b => key a ≤ key b)) :
    Inv (P ++ L) (L.foldl visit s) := by
  induction L generalizing P s with
  | nil => simpa using hInv
  | cons r rs ih =>
      have hkey : ∀ a ∈ P, key a ≤ key r := by
        intro a ha; exact hsep a ha r (List.Mem.head _)
      have hsep' : ∀ a ∈ P ++ [r], ∀ b ∈ rs, key a ≤ key b := by
        intro a ha b hb
        rcases List.mem_append.mp ha with ha | h1
        · exact hsep a ha b (List.Mem.tail _ hb)
        · obtain rfl : a = r := List.mem_singleton.mp h1
          rcases List.pairwise_cons.mp hsorted with ⟨hh, _⟩
          exact hh b hb
      have hsorted' : rs.Pairwise (fun a b => key a ≤ key b) := hsorted.tail
      simpa [List.foldl_cons, List.append_assoc] using
        ih (P ++ [r]) (visit s r) (visit_inv hInv hkey) hsep' hsorted'

theorem sound : Goal := by
  intro rows
  let sorted := rows.mergeSort (fun a b => decide (key a ≤ key b))
  have hperm := List.mergeSort_perm rows (fun a b => decide (key a ≤ key b))
  have htrans : ∀ a b c : Row, decide (key a ≤ key b) = true →
      decide (key b ≤ key c) = true → decide (key a ≤ key c) = true := by
    intro a b c hab hbc
    simp only [decide_eq_true_eq] at hab hbc ⊢
    exact Int.le_trans hab hbc
  have htotal : ∀ a b : Row, (decide (key a ≤ key b) || decide (key b ≤ key a)) = true := by
    intro a b
    simp only [Bool.or_eq_true, decide_eq_true_eq]
    exact Int.le_total _ _
  have hsorted : sorted.Pairwise (fun a b => key a ≤ key b) :=
    ((List.Pairwise.iff (S := fun a b => key a ≤ key b)
        (R := fun a b => decide (key a ≤ key b) = true)
          (fun a b => Iff.of_eq (decide_eq_true_eq (p := key a ≤ key b))))).mp
      (List.pairwise_mergeSort htrans htotal rows)
  have hinit : Inv [] {} := by
    refine ⟨List.Perm.refl _, ?_, ?_⟩
    · intro _
      exact ⟨rfl, rfl, fun a ha => absurd ha List.not_mem_nil⟩
    · intro gx h
      exact absurd h (by simp)
  have hinv := go_inv [] {} sorted hinit (by simp) hsorted
  have hperm2 : (sorted.filter fun b => !sorted.any (dominates · b)).Perm
      (rows.filter fun b => !rows.any (dominates · b)) := by
    have hcong : List.filter (fun b => !sorted.any (dominates · b)) rows
        = List.filter (fun b => !rows.any (dominates · b)) rows := by
      refine List.filter_congr (l := rows) ?_
      intro b _
      exact congrArg (fun t => !t) hperm.any_eq
    rw [← hcong]
    exact List.Perm.filter (fun b => !sorted.any (dominates · b)) hperm
  have hres : (sorted.foldl visit {}).kept.Perm
      (sorted.filter fun b => !sorted.any (dominates · b)) := by
    simpa using hinv.1
  exact hres.trans hperm2

end TinylambOpt.ResearchProofs.SkylineAntiJoin
