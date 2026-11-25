import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_ExpandRuns

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

private theorem leading_replicate_append (k : Nat) (a : Int) (Z : Series) :
    ((List.replicate k a ++ Z).takeWhile (fun c => c == a)).length =
      k + (Z.takeWhile (fun c => c == a)).length := by
  induction k with
  | zero => simp
  | succ k ih =>
      simp [List.replicate_succ]
      omega

private theorem measure_replicate_append (k : Nat) (a : Int) (Z : Series) :
    PlateauFrames.measure (List.replicate k a ++ Z) =
      (choose2 (k + 1) : Int) + (k : Int) * ((Z.takeWhile (fun c => c == a)).length : Int) +
        PlateauFrames.measure Z := by
  induction k with
  | zero => simp [choose2]
  | succ k ih =>
      rw [List.replicate_succ, List.cons_append, measure_cons]
      rw [matchesFirst_cons, ih, leading_replicate_append]
      rw [show (choose2 (k + 2) : Int) = (choose2 (k + 1) : Int) + (k + 1 : Nat) by
        exact_mod_cast choose2_step k]
      have hk1 : ((k + 1 : Nat) : Int) = (k : Int) + 1 := by omega
      rw [hk1, Int.add_mul]
      simp only [Int.one_mul]
      omega

private theorem leading_replicate_succ_append_ne (k : Nat) (b a : Int) (Z : Series) (h : b ≠ a) :
    ((List.replicate (k + 1) b ++ Z).takeWhile (fun c => c == a)).length = 0 := by
  have hbeq : (b == a) = false := by simpa [beq_iff_eq] using h
  have htw : (List.replicate (k + 1) b).takeWhile (fun c => c == a) = [] := by
    cases k with
    | zero => simp [hbeq]
    | succ k => simp [List.replicate_succ, hbeq]
  rw [List.takeWhile_append, htw]
  simp

private theorem flatMap_repeated_zero (x : Series) : x.flatMap (repeated 0) = [] := by
  induction x with
  | nil => simp
  | cons a xs ih => simp [repeated, ih]

private theorem leading_flatMap (k : Nat) (a : Int) (x : Series) :
    ((x.flatMap (repeated k)).takeWhile (fun c => c == a)).length =
      k * (x.takeWhile (fun c => c == a)).length := by
  induction x with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · have hbaeq : b = a := by simpa using hba
        subst b
        simp [repeated, ih]
        rw [Nat.mul_add, Nat.mul_one]
        omega
      · by_cases hk : k = 0
        · subst k
          simp [repeated, flatMap_repeated_zero, hba]
        · obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hk
          rw [show (b :: bs).flatMap (repeated (j + 1)) =
              List.replicate (j + 1) b ++ bs.flatMap (repeated (j + 1)) by simp [repeated]]
          rw [leading_replicate_succ_append_ne j b a _ (by simpa using hba)]
          simp [hba]

private theorem expand_alg (s L M B c : Int) :
    B + c + c * (c * L) + (s * (B + c) + c * c * (M - s)) =
      (s + 1) * (B + c) + c * c * (L + 1 + M - (s + 1)) := by
  simp only [Int.mul_add, Int.add_mul, Int.mul_sub, Int.mul_assoc, Int.add_assoc, Int.add_comm,
    Int.add_left_comm]
  omega

theorem sound_PlateauFrames_ExpandRuns : PlateauFrames.ExpandRuns.Goal := by
  intro x n
  induction x generalizing n with
  | nil =>
      simp [PlateauFrames.measure, windows, size, choose2]
  | cons a xs ih =>
      by_cases hn : n = 0
      · subst n
        rw [flatMap_repeated_zero]
        simp [PlateauFrames.measure, windows, size, choose2]
      · obtain ⟨k, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hn
        change PlateauFrames.measure (List.replicate (k + 1) a ++ xs.flatMap (repeated (k + 1))) =
          size (a :: xs) * choose2 (k + 2) + ((k + 1 : Nat) : Int)^2 *
            (PlateauFrames.measure (a :: xs) - size (a :: xs))
        rw [measure_replicate_append, ih (k + 1), leading_flatMap]
        rw [measure_cons, matchesFirst_cons]
        simp only [size, List.length_cons]
        have hc : (choose2 (k + 2) : Int) = (choose2 (k + 1) : Int) + (k + 1 : Nat) := by
          exact_mod_cast choose2_step k
        rw [hc]
        push_cast
        simp only [Int.pow_succ, Int.pow_zero, Int.one_mul]
        exact expand_alg (xs.length : Int)
          ((xs.takeWhile (fun c => c == a)).length : Int) (PlateauFrames.measure xs)
          (choose2 (k + 1) : Int) ((k : Int) + 1)

end TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_ExpandRuns
