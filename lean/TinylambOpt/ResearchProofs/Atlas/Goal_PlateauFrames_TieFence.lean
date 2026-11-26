import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PlateauFrames.TieFence` — `∀ x, measure x ≤ size x + TiePairs.measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_TieFence

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames

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

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

private theorem windows_cons_prefix (a : Int) (xs : Series) :
    windows (a :: xs) =
      [[a]] ++ ((List.range xs.length).map (fun i => a :: xs.take (i + 1))) ++ windows xs := by
  unfold windows
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.flatMap_append, List.take_succ_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  congr 2
  · simp [List.range_succ_eq_map, List.drop_zero, List.take_succ_cons,
      List.take_zero, Nat.add_assoc]
  · funext i
    simp [List.drop_drop, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private def leadCount (a : Int) (x : Series) : Nat := (x.takeWhile (fun b => b == a)).length

private theorem count_range_takeWhile (a : Int) (xs : Series) :
    (List.range xs.length).countP (fun i => (xs.take (i + 1)).all (fun b => b == a)) =
      leadCount a xs := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      unfold leadCount at ih ⊢
      by_cases hba : b == a
      · rw [List.length_cons, List.range_succ_eq_map, List.countP_cons, countP_map_comp]
        simp [List.take_succ_cons, hba, ih]
      · simp [hba, List.length_cons, List.range_succ_eq_map, countP_map_comp,
          List.take_succ_cons]

private theorem measure_cons (a : Int) (xs : Series) :
    measure (a :: xs) = (1 : Int) + (leadCount a xs : Int) + measure xs := by
  unfold TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.measure
  rw [windows_cons_prefix]
  rw [List.countP_append, List.countP_append]
  have hsingle :
      ([[a]] : List (List Int)).countP (fun w => w.all (fun c => some c == w.head?)) = 1 := by
    simp
  rw [hsingle]
  have hmid :
      ((List.range xs.length).map (fun i => a :: xs.take (i + 1))).countP
          (fun w => w.all (fun c => some c == w.head?)) =
        (List.range xs.length).countP (fun i => (xs.take (i + 1)).all (fun b => b == a)) := by
    rw [countP_map_comp]
    congr 1
    funext i
    simp
  rw [hmid, count_range_takeWhile]
  simp

private theorem measure_nil :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PlateauFrames.measure ([] : Series) = 0 := rfl

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using (eq_comm : a = b ↔ b = a)

private def tieCount (x : Series) : Nat := (pairs x).countP (fun p => p.1 == p.2)

private theorem tieCount_cons (a : Int) (xs : Series) :
    tieCount (a :: xs) = tieCount xs + xs.countP (fun b => b == a) := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  have hbeq : (fun x : Int => a == x) = (fun x : Int => x == a) := by
    funext x
    exact beq_comm_int a x
  rw [hbeq]
  omega

private theorem leadCount_le_count (a : Int) (xs : Series) :
    leadCount a xs ≤ xs.countP (fun b => b == a) := by
  induction xs with
  | nil => simp [leadCount]
  | cons b bs ih =>
      unfold leadCount at ih ⊢
      by_cases hba : b == a
      · simp [hba]
        omega
      · simp [hba]

theorem sound_PlateauFrames_TieFence : PlateauFrames.TieFence.Goal := by
  intro x
  induction x with
  | nil => simp [measure_nil, size, TiePairs.measure]
  | cons a xs ih =>
      rw [measure_cons]
      dsimp [size, TiePairs.measure] at ih ⊢
      change measure xs ≤ (xs.length : Int) + (tieCount xs : Int) at ih
      change (1 : Int) + (leadCount a xs : Int) + measure xs ≤
        (xs.length : Int) + 1 + (tieCount (a :: xs) : Int)
      rw [tieCount_cons]
      have hlead : (leadCount a xs : Int) ≤ (xs.countP (fun b => b == a) : Int) := by
        exact_mod_cast leadCount_le_count a xs
      push_cast
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_TieFence
