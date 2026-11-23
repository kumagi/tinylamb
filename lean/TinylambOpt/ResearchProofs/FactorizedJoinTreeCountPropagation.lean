import TinylambOpt.ResearchGoals.FactorizedJoinTreeCountPropagation

namespace TinylambOpt.ResearchProofs.FactorizedJoinTreeCountPropagation

open TinylambOpt.ResearchGoals.FactorizedJoinTreeCountPropagation

/-!
# Factorized join-tree count propagation (soundness)

`refSubtreeTuples` / `refChildrenCartesian` materialize `Unit` witnesses for
every flat join tuple; `optKeyWeight` / `optChildrenProd` replace the
cartesian products by scalar multiplicities.  The core observation is that
`List.length` turns each reference `flatMap` into exactly the corresponding
`foldl` / product used by the optimized pass.  We prove a single simultaneous
statement with the generated `JoinTree.rec` recursor (whose second motive is
over the *list* of children), then lift it to the root `COUNT(*)` aggregate.
-/

/-! ### Generic list / fold helpers -/

private theorem foldl_add_acc {α : Type} (l : List α) (f : α → Nat) :
    ∀ acc, l.foldl (fun a x => a + f x) acc = acc + l.foldl (fun a x => a + f x) 0 := by
  induction l with
  | nil => intro acc; simp
  | cons x xs ih =>
      intro acc
      simp only [List.foldl_cons, Nat.zero_add]
      rw [ih (acc + f x), ih (f x)]
      omega

private theorem sum_eq_foldl {α : Type} (l : List α) (f : α → Nat) :
    (l.map f).sum = l.foldl (fun a x => a + f x) 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih, List.foldl_cons, Nat.zero_add]
      rw [foldl_add_acc xs f (f x)]

private theorem sum_const {α : Type} (l : List α) (c : Nat) :
    (l.map fun _ => c).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih, List.length_cons]
      rw [Nat.add_mul]
      simp
      omega

private theorem length_if_nil {α : Type} (c : Bool) (l : List α) :
    (if c then l else []).length = if c then l.length else 0 := by
  by_cases h : c <;> simp [h]

private theorem foldl_if_eq {α : Type} (l : List α) (p : α → Bool) (w : α → Nat) (acc : Nat) :
    l.foldl (fun a x => if p x then a + w x else a) acc =
      l.foldl (fun a x => a + (if p x then w x else 0)) acc := by
  have h : (fun a x => if p x then a + w x else a) =
      (fun a x => a + (if p x then w x else 0)) := by
    funext a x
    by_cases hp : p x = true <;> simp [hp]
  rw [h]

private theorem filter_const_map_length (l : List Unit) (v g : Int) :
    ((l.map fun _ => v).filter (fun p => p == g)).length = if v == g then l.length else 0 := by
  induction l with
  | nil => simp
  | cons u us ih =>
      by_cases h : v == g <;> simp [h, ih]

private theorem root_filter_length (rows : List NodeRow) (children : List JoinTree) (g : Int) :
    (List.filter (fun p : Int => p == g)
      (rows.flatMap (fun row : NodeRow =>
        (refChildrenCartesian row.childKeys 0 children).map fun _ : Unit => row.payload))).length =
      rows.foldl
        (fun (acc : Nat) (row : NodeRow) => acc + (if row.payload == g then
          (refChildrenCartesian row.childKeys 0 children).length else 0)) 0 := by
  induction rows with
  | nil => simp
  | cons row rows ih =>
      simp only [List.flatMap_cons, List.filter_append, List.length_append, ih]
      rw [filter_const_map_length]
      simp only [List.foldl_cons, Nat.zero_add]
      have h := foldl_add_acc rows
        (fun (r : NodeRow) => if r.payload == g then
          (refChildrenCartesian r.childKeys 0 children).length else 0)
        (if row.payload == g then (refChildrenCartesian row.childKeys 0 children).length else 0)
      rw [h]

/-! ### Length of the reference expansion equals the factorized weight -/

private def treeChildren : JoinTree → List JoinTree
  | .mk _ cs => cs

private theorem tree_lengths (t : JoinTree) :
    (∀ ep : Option Int, (refSubtreeTuples ep t).length = optKeyWeight ep t) ∧
    (∀ (rowKeys : List (Option Int)) (idx : Nat),
      (refChildrenCartesian rowKeys idx (treeChildren t)).length =
        optChildrenProd rowKeys idx (treeChildren t)) := by
  let motive1 : JoinTree → Prop := fun t =>
    (∀ ep : Option Int, (refSubtreeTuples ep t).length = optKeyWeight ep t) ∧
    (∀ (rowKeys : List (Option Int)) (idx : Nat),
      (refChildrenCartesian rowKeys idx (treeChildren t)).length =
        optChildrenProd rowKeys idx (treeChildren t))
  let motive2 : List JoinTree → Prop := fun cs =>
    ∀ (rowKeys : List (Option Int)) (idx : Nat),
      (refChildrenCartesian rowKeys idx cs).length = optChildrenProd rowKeys idx cs
  exact JoinTree.rec
    (motive_1 := motive1) (motive_2 := motive2)
    (mk := fun rows children ih => by
      dsimp [motive2] at ih
      constructor
      · intro ep
        cases ep with
        | none =>
            simp [refSubtreeTuples, optKeyWeight]
        | some pk =>
            simp only [refSubtreeTuples, optKeyWeight]
            rw [List.length_flatMap]
            rw [sum_eq_foldl]
            simp only [length_if_nil]
            have hmap :
                (fun (a : Nat) (row : NodeRow) => a + (if row.parentKey == some pk then
                    (refChildrenCartesian row.childKeys 0 children).length else 0)) =
                (fun (a : Nat) (row : NodeRow) => a + (if row.parentKey == some pk then
                    optChildrenProd row.childKeys 0 children else 0)) := by
              funext a row
              by_cases hp : row.parentKey == some pk <;> simp [hp, ih]
            rw [hmap]
            rw [← foldl_if_eq]
      · exact ih)
    (nil := by
      intro rowKeys idx
      rfl)
    (cons := fun head tail ih_head ih_tail => by
      intro rowKeys idx
      simp only [refChildrenCartesian, optChildrenProd, List.length_flatMap]
      rw [sum_const]
      rw [ih_head.1, ih_tail])
    t

private theorem subtree_length (ep : Option Int) (t : JoinTree) :
    (refSubtreeTuples ep t).length = optKeyWeight ep t :=
  (tree_lengths t).1 ep

private theorem children_length (rowKeys : List (Option Int)) (idx : Nat) (cs : List JoinTree) :
    (refChildrenCartesian rowKeys idx cs).length = optChildrenProd rowKeys idx cs :=
  (tree_lengths (.mk [] cs)).2 rowKeys idx

/-! ### Main theorem -/

theorem sound : TinylambOpt.ResearchGoals.FactorizedJoinTreeCountPropagation.Goal := by
  intro t g
  cases t with
  | mk rows children =>
      simp only [refCount, refRootTuples, optRootCount]
      rw [root_filter_length]
      have hfun :
          (fun (acc : Nat) (row : NodeRow) => acc + (if row.payload == g then
              (refChildrenCartesian row.childKeys 0 children).length else 0)) =
          (fun (acc : Nat) (row : NodeRow) => acc + (if row.payload == g then
              optChildrenProd row.childKeys 0 children else 0)) := by
        funext acc row
        by_cases hp : row.payload == g <;> simp [hp, children_length]
      rw [hfun]
      rw [← foldl_if_eq]

end TinylambOpt.ResearchProofs.FactorizedJoinTreeCountPropagation
