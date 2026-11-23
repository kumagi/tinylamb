import TinylambOpt.ResearchGoals.SelfJoinSharedScanDualFilterSplit

namespace TinylambOpt.ResearchProofs.SelfJoinSharedScanDualFilterSplit

open TinylambOpt.ResearchGoals.SelfJoinSharedScanDualFilterSplit

private abbrev tag (r : BaseRow) : TaggedNode :=
  { id := r.id, payload := r.payload, mask1 := r.cond1, mask2 := r.cond2 }

/-- Collapse two nested conditionals that share their else-branch. -/
private theorem ite_and_prop {α : Type} (c1 c2 : Prop) [Decidable c1] [Decidable c2] (x y : α) :
    (if c1 then (if c2 then x else y) else y) = (if c1 ∧ c2 then x else y) := by
  by_cases h1 : c1 <;> by_cases h2 : c2 <;> simp [h1, h2]

private theorem filter_flatMap {α β : Type} (p : α → Bool) (f : α → List β) (l : List α) :
    (l.filter p).flatMap f = l.flatMap (fun x => if p x then f x else []) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases hpx : p x <;> simp [hpx, ih]

private theorem filter_filterMap {α β : Type} (p : α → Bool) (f : α → Option β) (l : List α) :
    (l.filter p).filterMap f = l.filterMap (fun x => if p x then f x else none) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases hpx : p x <;> cases hfx : f x <;> simp [hpx, hfx, ih]

private theorem map_flatMap {α β γ : Type} (g : α → β) (f : β → List γ) (l : List α) :
    (l.map g).flatMap f = l.flatMap (fun x => f (g x)) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem flatMap_congr {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem filterMap_funext {α β : Type} (l : List α) (f g : α → Option β)
    (h : ∀ x, f x = g x) : l.filterMap f = l.filterMap g := by
  have hfg : f = g := funext h
  rw [hfg]

/-- For a single edge, the reference (two filtered scans) and the optimized
    (one tagged scan with two role bits) produce exactly the same list of
    payload pairs. -/
private theorem perEdge (rows : List BaseRow) (e : Edge) :
    ((rows.filter (fun r => r.cond1)).flatMap fun r1 =>
       if r1.id == e.src then
         (rows.filter (fun r => r.cond2)).filterMap fun r2 =>
           if r2.id == e.dst then some (r1.payload, r2.payload) else none
       else [])
    =
    ((rows.map tag).flatMap fun r1 =>
       if r1.mask1 && r1.id == e.src then
         (rows.map tag).filterMap fun r2 =>
           if r2.mask2 && r2.id == e.dst then some (r1.payload, r2.payload) else none
       else []) := by
  simp [filter_flatMap, filter_filterMap, map_flatMap, ite_and_prop]
  apply flatMap_congr
  intro x hx
  by_cases hcond : x.cond1 = true ∧ x.id = e.src
  · simp [hcond]
    apply filterMap_funext
    intro r2
    simp
  · simp [hcond]

theorem sound : TinylambOpt.ResearchGoals.SelfJoinSharedScanDualFilterSplit.Goal := by
  intro rows edges
  have hout :
      (edges.flatMap fun e =>
        (rows.map tag).flatMap fun r1 =>
          if r1.mask1 && r1.id == e.src then
            (rows.map tag).filterMap fun r2 =>
              if r2.mask2 && r2.id == e.dst then some (r1.payload, r2.payload) else none
          else [])
      =
      (edges.flatMap fun e =>
        (rows.filter (fun r => r.cond1)).flatMap fun r1 =>
          if r1.id == e.src then
            (rows.filter (fun r => r.cond2)).filterMap fun r2 =>
              if r2.id == e.dst then some (r1.payload, r2.payload) else none
          else []) := by
    induction edges with
    | nil => simp
    | cons e es ih =>
        rw [List.flatMap_cons, List.flatMap_cons]
        rw [perEdge rows e, ih]
  constructor
  · intro p
    have hc := congrArg (fun l : List (Int × Int) => l.count p) hout
    simpa [optSharedScanJoin, refDualScanJoin, tag] using hc
  · intro h
    cases rows with
    | nil => contradiction
    | cons r rs =>
        simp

end TinylambOpt.ResearchProofs.SelfJoinSharedScanDualFilterSplit
