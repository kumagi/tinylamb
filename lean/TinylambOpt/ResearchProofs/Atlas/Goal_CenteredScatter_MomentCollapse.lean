import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `CenteredScatter.MomentCollapse` — `∀ x, measure x = size x*(x.map
  (·^2)).sum-x.sum^2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_MomentCollapse

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
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

private theorem measure_cons (a : Int) (xs : Series) :
    CenteredScatter.measure (a :: xs) =
      (xs.map (fun b => (a - b)^2)).sum + CenteredScatter.measure xs := by
  simp [CenteredScatter.measure, pairs_cons, List.map_append, List.sum_append,
    List.map_map, Function.comp_def]

private theorem sum_map_sq_sub (a : Int) (xs : Series) :
    (xs.map (fun b => (a - b)^2)).sum =
      (xs.length : Int) * a^2 - 2 * a * xs.sum + (xs.map (fun v => v^2)).sum := by
  induction xs with
  | nil => simp
  | cons v xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
      rw [ih]
      grind

theorem sound_CenteredScatter_MomentCollapse : CenteredScatter.MomentCollapse.Goal := by
  intro x
  induction x with
  | nil => simp [CenteredScatter.measure, pairs, size]
  | cons a xs ih =>
      rw [measure_cons, sum_map_sq_sub, ih]
      simp [size]
      grind

end TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_MomentCollapse
