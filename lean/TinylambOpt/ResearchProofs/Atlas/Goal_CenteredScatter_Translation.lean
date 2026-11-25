import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_Translation

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force truth check: every series of length <= 4 over [-2, 2], shifted
    by every offset in [-3, 3], preserves the total squared pair distance. -/
#eval
  let rec gen : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => (gen n).flatMap (fun xs => [-2, -1, 0, 1, 2].map (fun v => xs ++ [v]))
  let bad := (gen 4).flatMap (fun x =>
    [-3, -2, -1, 0, 1, 2, 3].filterMap (fun b =>
      if CenteredScatter.measure (shifted b x) == CenteredScatter.measure x
      then none else some (x, b)))
  bad.take 5

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β)
    (xs : List α) : (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem pairs_map (f : Int → Int) (xs : Series) :
    pairs (xs.map f) = (pairs xs).map (fun t => (f t.1, f t.2)) := by
  induction xs with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.map_cons, pairs_cons, pairs_cons, ih]
      simp only [List.map_append, List.map_map, Function.comp_def]

theorem sound_CenteredScatter_Translation : CenteredScatter.Translation.Goal := by
  intro b x
  simp only [CenteredScatter.measure, shifted]
  rw [pairs_map]
  simp only [List.map_map, Function.comp_def]
  apply congrArg List.sum
  apply List.map_congr_left
  intro t _
  grind

end TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_Translation
