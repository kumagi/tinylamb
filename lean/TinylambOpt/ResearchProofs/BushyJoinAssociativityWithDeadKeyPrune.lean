import TinylambOpt.ResearchGoals.BushyJoinAssociativityWithDeadKeyPrune

namespace TinylambOpt.ResearchProofs.BushyJoinAssociativityWithDeadKeyPrune

open TinylambOpt.ResearchGoals.BushyJoinAssociativityWithDeadKeyPrune

/-- The `A ⋈_{k1} B` stream, shared by both plans. -/
private def abOf (aRows : List ARow) (bRows : List BRow) : List (Int × Option Int) :=
  aRows.flatMap fun a =>
    bRows.filterMap fun b =>
      match a.k1, b.k1 with
      | some ak1, some bk1 => if ak1 == bk1 then some (a.outA, b.k2) else none
      | _, _ => none

/-- Left-deep continuation from `AB` through `C`. -/
private def refInner (x : Int × Option Int) (cRows : List CRow) : List (Int × Option Int) :=
  cRows.filterMap fun c =>
    match x.2, c.k2 with
    | some k2b, some k2c => if k2b == k2c then some (x.1, c.k3) else none
    | _, _ => none

/-- Left-deep continuation from `ABC` through `D`. -/
private def refFinal (x : Int × Option Int) (dRows : List DRow) : List (Int × Int) :=
  dRows.filterMap fun d =>
    match x.2, d.k3 with
    | some k3c, some k3d => if k3c == k3d then some (x.1, d.outD) else none
    | _, _ => none

/-- The `C ⋈_{k3} D` stream of the bushy plan. -/
private def cdOf (cRows : List CRow) (dRows : List DRow) : List (Option Int × Int) :=
  cRows.flatMap fun c =>
    dRows.filterMap fun d =>
      match c.k3, d.k3 with
      | some k3c, some k3d => if k3c == k3d then some (c.k2, d.outD) else none
      | _, _ => none

/-- Bushy final join on the surviving `k2` key. -/
private def optInner (x : Int × Option Int) (cd : List (Option Int × Int)) : List (Int × Int) :=
  cd.filterMap fun q =>
    match x.2, q.1 with
    | some l2, some r2 => if l2 == r2 then some (x.1, q.2) else none
    | _, _ => none

private theorem ref_eq (aRows : List ARow) (bRows : List BRow)
    (cRows : List CRow) (dRows : List DRow) :
    refLeftDeep aRows bRows cRows dRows =
      ((abOf aRows bRows).flatMap (fun x => refInner x cRows)).flatMap
        (fun y => refFinal y dRows) := by
  rfl

private theorem opt_eq (aRows : List ARow) (bRows : List BRow)
    (cRows : List CRow) (dRows : List DRow) :
    optBushyPruned aRows bRows cRows dRows =
      (abOf aRows bRows).flatMap (fun x => optInner x (cdOf cRows dRows)) := by
  rfl

private theorem flatMap_filterMap {α β γ : Type} (f : α → Option β) (g : β → List γ)
    (xs : List α) :
    (xs.filterMap f).flatMap g = xs.flatMap (fun x => match f x with | some y => g y | none => []) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases h : f x with
      | none => simp [h, ih]
      | some y => simp [h, ih]

private theorem filterMap_flatMap {α β γ : Type} (f : α → List β) (g : β → Option γ)
    (xs : List α) :
    (xs.flatMap f).filterMap g = xs.flatMap (fun x => (f x).filterMap g) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.flatMap_cons, List.filterMap_append, ih]

private theorem flatMap_congr {α β : Type} (f g : α → List β) (xs : List α)
    (h : ∀ x ∈ xs, f x = g x) : xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : f x = g x := h x (by simp)
      have ih' : ∀ y ∈ xs, f y = g y := fun y hy => h y (by simp [hy])
      simpa [List.flatMap_cons, hx] using ih ih'

private theorem filterMap_congr {α β : Type} (f g : α → Option β) (xs : List α)
    (h : ∀ x ∈ xs, f x = g x) : xs.filterMap f = xs.filterMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : f x = g x := h x (by simp)
      have ih' : ∀ y ∈ xs, f y = g y := fun y hy => h y (by simp [hy])
      rw [List.filterMap_cons, List.filterMap_cons]
      cases hgx : g x with
      | none =>
          have hfx : f x = none := by rw [hx, hgx]
          simp [hfx, ih ih']
      | some y =>
          have hfx : f x = some y := by rw [hx, hgx]
          simp [hfx, ih ih']

/-- The key equivalence: for a fixed `AB` tuple, going through `C` then `D`
    emits exactly the same stream as joining `C` and `D` first and pruning `k2`. -/
private theorem inner_eq (x : Int × Option Int) (cRows : List CRow) (dRows : List DRow) :
    (refInner x cRows).flatMap (fun y => refFinal y dRows) =
      optInner x (cdOf cRows dRows) := by
  unfold refInner refFinal optInner cdOf
  rw [flatMap_filterMap]
  rw [filterMap_flatMap]
  apply flatMap_congr
  intro c hc
  rw [List.filterMap_filterMap]
  cases hk2 : x.2 with
  | none => simp
  | some k2 =>
      cases hc2 : c.k2 with
      | none =>
          simp
          intro a ha a2 b h
          cases hk3 : c.k3 with
          | none => simp [hk3] at h
          | some k3 =>
              cases ha3 : a.k3 with
              | none => simp [hk3, ha3] at h
              | some a3 =>
                  by_cases heq : k3 = a3
                  · simp [hk3, ha3, heq] at h
                    rw [← h.1]
                  · simp [hk3, ha3, heq] at h
      | some c2 =>
          by_cases h2 : k2 = c2
          · simp [h2]
            apply filterMap_congr
            intro d hd
            cases hk3 : c.k3 with
            | none => simp
            | some k3 =>
                cases hd3 : d.k3 with
                | none => simp
                | some d3 =>
                    by_cases h3 : k3 = d3 <;> simp [h3]
          · simp [h2]
            intro a ha a2 b h
            cases hk3 : c.k3 with
            | none => simp [hk3] at h
            | some k3 =>
                cases ha3 : a.k3 with
                | none => simp [hk3, ha3] at h
                | some a3 =>
                    by_cases heq : k3 = a3
                    · simp [hk3, ha3, heq] at h
                      rw [← h.1]
                      simp [h2]
                    · simp [hk3, ha3, heq] at h

theorem sound : TinylambOpt.ResearchGoals.BushyJoinAssociativityWithDeadKeyPrune.Goal := by
  intro aRows bRows cRows dRows p
  suffices h : refLeftDeep aRows bRows cRows dRows = optBushyPruned aRows bRows cRows dRows by
    rw [h]
  rw [ref_eq, opt_eq]
  -- Reassociate the left-deep plan so both sides are driven by the same `AB` spine.
  rw [List.flatMap_assoc]
  apply flatMap_congr
  intro x hx
  exact inner_eq x cRows dRows

end TinylambOpt.ResearchProofs.BushyJoinAssociativityWithDeadKeyPrune
