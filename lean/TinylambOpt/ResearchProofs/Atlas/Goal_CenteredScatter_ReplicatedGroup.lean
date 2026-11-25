import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_ReplicatedGroup

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

private theorem measure_eq_moment (x : Series) :
    CenteredScatter.measure x = (x.length : Int) * (x.map (fun v => v^2)).sum - x.sum^2 := by
  induction x with
  | nil => simp [CenteredScatter.measure, pairs]
  | cons a xs ih =>
      rw [measure_cons, sum_map_sq_sub, ih]
      simp
      grind

private theorem flatten_replicate_length (n : Nat) (x : Series) :
    (((List.replicate n x).flatten).length : Int) = (n : Int) * (x.length : Int) := by
  induction n with
  | zero => simp
  | succ n => simp

private theorem flatten_replicate_sum (n : Nat) (x : Series) :
    ((List.replicate n x).flatten).sum = (n : Int) * x.sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons, List.sum_append, ih]
      grind

private theorem flatten_replicate_sumsq (n : Nat) (x : Series) :
    ((List.replicate n x).flatten.map (fun v => v^2)).sum =
      (n : Int) * (x.map (fun v => v^2)).sum := by
  simpa [List.map_flatten] using flatten_replicate_sum n (x.map (fun v => v^2))

theorem sound_CenteredScatter_ReplicatedGroup : CenteredScatter.ReplicatedGroup.Goal := by
  intro n x
  rw [measure_eq_moment, measure_eq_moment]
  rw [flatten_replicate_length, flatten_replicate_sum, flatten_replicate_sumsq]
  grind

end TinylambOpt.ResearchProofs.Atlas.Goal_CenteredScatter_ReplicatedGroup
