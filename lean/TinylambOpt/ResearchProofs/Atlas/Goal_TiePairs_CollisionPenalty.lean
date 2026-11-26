import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `TiePairs.CollisionPenalty` — `∀ x (f : Int → Int), measure (x.map f) - measure x =
  (pairs x).countP (fun (a,b) => a != b && f a == f b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_CollisionPenalty

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TiePairs

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

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private def penaltyCount (x : Series) (f : Int → Int) : Nat :=
  (pairs x).countP (fun p => p.1 != p.2 && f p.1 == f p.2)

private theorem measure_cons (a : Int) (xs : Series) :
    TiePairs.measure (a :: xs) = (xs.countP (fun b => a == b) : Int) + TiePairs.measure xs := by
  unfold TiePairs.measure
  rw [pairs_cons, List.countP_append, countP_map_comp]
  simp

private theorem mapped_measure_cons (a : Int) (xs : Series) (f : Int → Int) :
    TiePairs.measure (f a :: xs.map f) =
      (xs.countP (fun b => f a == f b) : Int) + TiePairs.measure (xs.map f) := by
  rw [measure_cons]
  rw [countP_map_comp]

private theorem penalty_cons (a : Int) (xs : Series) (f : Int → Int) :
    penaltyCount (a :: xs) f = xs.countP (fun b => a != b && f a == f b) + penaltyCount xs f := by
  unfold penaltyCount
  rw [pairs_cons, List.countP_append, countP_map_comp]

private theorem countP_congruence_split (xs : List Int) (a : Int) (f : Int → Int) :
    xs.countP (fun b => f a == f b) =
      xs.countP (fun b => a == b) + xs.countP (fun b => a != b && f a == f b) := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      simp only [List.countP_cons]
      rw [ih]
      by_cases hab : a = b
      · subst b
        simp
        omega
      · by_cases hfab : f a = f b
        · simp [hab, hfab, beq_iff_eq]
          omega
        · simp [hab, hfab, beq_iff_eq]

theorem sound_TiePairs_CollisionPenalty : TiePairs.CollisionPenalty.Goal := by
  intro x f
  change TiePairs.measure (x.map f) - TiePairs.measure x = (penaltyCount x f : Int)
  induction x with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons]
      rw [mapped_measure_cons, measure_cons, penalty_cons]
      change ((xs.countP (fun b => f a == f b) : Int) + TiePairs.measure (List.map f xs)) -
          ((xs.countP (fun b => a == b) : Int) + TiePairs.measure xs) =
        ((xs.countP (fun b => a != b && f a == f b) + penaltyCount xs f : Nat) : Int)
      have hsplit := countP_congruence_split xs a f
      have hsplit' : (xs.countP (fun b => f a == f b) : Int) =
          (xs.countP (fun b => a == b) : Int) +
            (xs.countP (fun b => a != b && f a == f b) : Int) := by
        exact_mod_cast hsplit
      change (xs.countP (fun b => f a == f b) : Int) + TiePairs.measure (List.map f xs) -
          ((xs.countP (fun b => a == b) : Int) + TiePairs.measure xs) =
        (xs.countP (fun b => a != b && f a == f b) : Int) + (penaltyCount xs f : Int)
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_CollisionPenalty
