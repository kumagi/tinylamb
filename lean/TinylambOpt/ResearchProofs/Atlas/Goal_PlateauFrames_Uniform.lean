import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PlateauFrames.Uniform` — `∀ n a, measure (repeated n a) = (choose2 (n+1):Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_Uniform

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

private theorem range_succ_cons (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun i => i + 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.range (Nat.succ n + 1) = List.range (n + 1) ++ [n + 1] := by
          simp [Nat.succ_eq_add_one, List.range_succ]
        _ = (0 :: (List.range n).map (fun i => i + 1)) ++ [n + 1] := by rw [ih]
        _ = 0 :: ((List.range n).map (fun i => i + 1) ++ [n + 1]) := by simp
        _ = 0 :: (List.range (n + 1)).map (fun i => i + 1) := by simp [List.range_succ]

private theorem range_flatMap_succ {α : Type} (n : Nat) (f : Nat → List α) :
    (List.range (n + 1)).flatMap f = f 0 ++ (List.range n).flatMap (fun i => f (i + 1)) := by
  rw [range_succ_cons n]
  simp [List.flatMap_map]

private def prefixWindows (x : Series) := (List.range x.length).map (fun j => x.take (j + 1))

private theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) = prefixWindows (a :: xs) ++ windows xs := by
  unfold windows prefixWindows
  simp only [List.length_cons]
  rw [range_flatMap_succ]
  simp [List.drop_succ_cons]

private theorem matchesFirst_cons (a : Int) (xs : Series) :
    matchesFirst (a :: xs) = (xs.takeWhile (fun c => c == a)).length + 1 := by
  simp [matchesFirst]

private theorem take_all_ne (a b : Int) (bs : List Int) (j : Nat) (h : b ≠ a) :
    ((b :: bs).take (j + 1)).all (fun c => c == a) = false := by
  rw [List.take_succ_cons]
  simp [h]

private theorem count_take_eq (a : Int) (xs : Series) :
    (List.range (xs.length + 1)).countP
        (fun j => ((a :: xs).take (j + 1)).all (fun c => c == a)) =
      (xs.takeWhile (fun c => c == a)).length + 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.length_cons]
      rw [range_succ_cons (bs.length + 1)]
      change List.countP (fun j => ((a :: b :: bs).take (j + 1)).all (fun c => c == a))
          (0 :: (List.range (bs.length + 1)).map (fun i => i + 1)) =
        (List.takeWhile (fun c => c == a) (b :: bs)).length + 1
      rw [List.countP_cons, List.countP_map]
      change List.countP (fun i => ((a :: b :: bs).take ((i + 1) + 1)).all (fun c => c == a))
          (List.range (bs.length + 1)) +
          (if ((a :: b :: bs).take (0 + 1)).all (fun c => c == a) = true then 1 else 0) =
        (List.takeWhile (fun c => c == a) (b :: bs)).length + 1
      have hp0 : ((a :: b :: bs).take (0 + 1)).all (fun c => c == a) = true := by simp
      rw [ite_eq_left hp0]
      have hpred : (fun i => ((a :: b :: bs).take ((i + 1) + 1)).all (fun c => c == a)) =
          (fun i => ((b :: bs).take (i + 1)).all (fun c => c == a)) := by
        funext i
        simp only [Nat.add_assoc, List.take_succ_cons, List.all_cons]
        simp
      rw [hpred]
      by_cases hba : b = a
      · subst b
        rw [ih]
        simp
      · have hshift : (List.range (bs.length + 1)).countP
              (fun i => ((b :: bs).take (i + 1)).all (fun c => c == a)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro i hi
            rw [take_all_ne a b bs i hba]
            simp
        rw [hshift]
        simp [hba]

private theorem countP_prefixWindows (a : Int) (xs : Series) :
    (prefixWindows (a :: xs)).countP (fun w => w.all (fun c => some c == w.head?)) =
      matchesFirst (a :: xs) := by
  unfold prefixWindows
  rw [List.countP_map]
  simp only [List.length_cons]
  change List.countP
      (fun j => ((a :: xs).take (j + 1)).all (fun c => some c == ((a :: xs).take (j + 1)).head?))
      (List.range (xs.length + 1)) = matchesFirst (a :: xs)
  have hhead : ∀ j, ((a :: xs).take (j + 1)).head? = some a := by
    intro j
    simp
  simp only [hhead]
  rw [matchesFirst_cons]
  exact count_take_eq a xs

private theorem measure_cons (a : Int) (xs : Series) :
    PlateauFrames.measure (a :: xs) = matchesFirst (a :: xs) + PlateauFrames.measure xs := by
  unfold PlateauFrames.measure
  rw [windows_cons, List.countP_append, countP_prefixWindows]
  omega

private theorem choose2_step (n : Nat) :
    choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  simp only [show n + 2 - 1 = n + 1 by omega, show n + 1 - 1 = n by omega]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind,
    Nat.mul_comm 2 (n + 1), Nat.add_mul_div_right _ _ (by decide)]

private theorem matchesFirst_replicate_succ (n : Nat) (a : Int) :
    matchesFirst (a :: List.replicate n a) = n + 1 := by
  simp [matchesFirst]

theorem sound_PlateauFrames_Uniform : PlateauFrames.Uniform.Goal := by
  intro n a
  induction n with
  | zero =>
      simp [PlateauFrames.measure, windows, repeated, choose2]
  | succ n ih =>
      change PlateauFrames.measure (a :: List.replicate n a) = (choose2 (n + 2) : Int)
      change PlateauFrames.measure (List.replicate n a) = (choose2 (n + 1) : Int) at ih
      rw [measure_cons, matchesFirst_replicate_succ, ih]
      rw [show choose2 (n + 2) = choose2 (n + 1) + (n + 1) by exact choose2_step n]
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_Uniform
