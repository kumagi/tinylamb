import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `CenteredScatter.PairMerge` — `∀ x y, measure (x++y)-measure x-measure y = size
  y*(x.map (·^2)).sum + size x*(y.map (·^2)).sum-2*x.sum*y.sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_PairMerge

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.CenteredScatter

/-! Self-contained list algebra: `pairs` enumerates every ordered pair of
physical positions; `cart` is the cross product of two series. -/

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

private theorem cart_cons (a : Int) (xs ys : Series) :
    cart (a :: xs) ys = ys.map (fun b => (a, b)) ++ cart xs ys := by
  simp [cart]

private theorem pairs_sum_append (f : Int × Int → Int) (xs ys : Series) :
    ((pairs (xs ++ ys)).map f).sum =
      ((pairs xs).map f).sum + ((pairs ys).map f).sum + ((cart xs ys).map f).sum := by
  induction xs with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.map_append, List.map_map, List.sum_append]
      rw [ih]
      simp only [Function.comp_def]
      grind

private theorem sum_map_sub_sq (a : Int) (xs : Series) :
    (xs.map fun b => (a - b) ^ 2).sum =
      (xs.length : Int) * a ^ 2 - 2 * a * xs.sum + (xs.map fun b => b ^ 2).sum := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, ih]
      grind

private theorem cart_sq_sum (xs ys : Series) :
    ((cart xs ys).map fun t => (t.1 - t.2) ^ 2).sum =
      (ys.length : Int) * (xs.map fun v => v ^ 2).sum +
        (xs.length : Int) * (ys.map fun v => v ^ 2).sum - 2 * xs.sum * ys.sum := by
  induction xs with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [cart_cons]
      simp only [List.map_append, List.map_map, List.sum_append, List.length_cons,
        List.map_cons, List.sum_cons, Function.comp_def] at ih ⊢
      rw [sum_map_sub_sq, ih]
      grind

theorem sound_CenteredScatter_PairMerge : CenteredScatter.PairMerge.Goal := by
  intro x y
  simp only [CenteredScatter.measure]
  rw [pairs_sum_append, cart_sq_sum]
  simp only [size]
  grind

end TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_PairMerge
