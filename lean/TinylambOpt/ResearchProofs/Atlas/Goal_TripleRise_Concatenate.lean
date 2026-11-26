import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `TripleRise.Concatenate` — `∀ x y, measure (x++y) = measure x + measure y + ((pairs
  x).filter rising |>.map (fun t => y.countP (t.2 < ·))).sum + ((pairs y).filter
  rising |>.map (fun t => x.countP (· < t.1))).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_Concatenate

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise

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

private theorem triples_cons (a : Int) (xs : Series) :
    triples (a :: xs) = (pairs xs).map (fun t => (a, t.1, t.2)) ++ triples xs := by
  unfold triples
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private def belowPairs (a : Int) (z : Series) : Int :=
  ((pairs z).countP (fun t : Int × Int => decide (a < t.1 && t.1 < t.2)) : Int)

private theorem strict3_cons_pred (a : Int) (t : Int × Int) :
    strict3 (a, t.1, t.2) = decide (a < t.1 && t.1 < t.2) := by
  simp [strict3]

private theorem measure_cons (a : Int) (z : Series) :
    TripleRise.measure (a :: z) = TripleRise.measure z + belowPairs a z := by
  unfold TripleRise.measure
  rw [triples_cons, List.countP_append]
  push_cast
  simp only [List.countP_map]
  rw [show (strict3 ∘ fun t : Int × Int => (a, t.1, t.2)) =
        (fun t : Int × Int => decide (a < t.1 && t.1 < t.2)) by
      funext t
      exact strict3_cons_pred a t]
  unfold belowPairs
  omega

private def cntLt (b : Int) (z : Series) : Int := (z.countP (fun c => decide (b < c)) : Int)

private theorem cntLt_append (b : Int) (u v : Series) :
    cntLt b (u ++ v) = cntLt b u + cntLt b v := by
  simp [cntLt, List.countP_append]

private theorem map_pair_count (a b : Int) (z : Series) :
    ((z.map (fun c => (b, c))).countP (fun t => decide (a < t.1 && t.1 < t.2)) : Int) =
      if a < b then cntLt b z else 0 := by
  induction z with
  | nil => simp [cntLt]
  | cons c cs ih =>
      rw [List.countP_map] at ih ⊢
      simp only [List.countP_cons, Function.comp_apply]
      push_cast
      by_cases h : a < b
      · rw [ih]
        simp [h, cntLt, List.countP_cons]
      · simp [h]

private theorem belowPairs_cons (a b : Int) (xs : Series) :
    belowPairs a (b :: xs) = (if a < b then cntLt b xs else 0) + belowPairs a xs := by
  unfold belowPairs
  rw [pairs_cons, List.countP_append]
  push_cast
  rw [map_pair_count]

private def crossBelow (a : Int) (xs y : Series) : Int :=
  (((xs.filter (fun b => decide (a < b))).map (fun b => y.countP (fun c => decide (b < c)))).sum : Int)

private theorem crossBelow_cons (a b : Int) (xs y : Series) :
    crossBelow a (b :: xs) y = (if a < b then cntLt b y else 0) + crossBelow a xs y := by
  unfold crossBelow cntLt
  by_cases h : a < b <;> simp [h]

private theorem belowPairs_append (a : Int) (xs y : Series) :
    belowPairs a (xs ++ y) = belowPairs a xs + belowPairs a y + crossBelow a xs y := by
  induction xs with
  | nil => simp [belowPairs, crossBelow, pairs]
  | cons b bs ih =>
      unfold belowPairs at ih ⊢
      rw [show pairs ((b :: bs) ++ y) =
        (bs ++ y).map (fun c => (b, c)) ++ pairs (bs ++ y) by
          rw [List.cons_append, pairs_cons]]
      rw [List.countP_append]
      push_cast
      rw [map_pair_count]
      rw [show pairs (b :: bs) = bs.map (fun c => (b, c)) ++ pairs bs by rw [pairs_cons]]
      rw [List.countP_append]
      push_cast
      rw [map_pair_count]
      rw [ih, crossBelow_cons, cntLt_append]
      by_cases h : a < b <;> simp [h] <;> omega

private theorem countP_filter_rising_eq_below (a : Int) (y : Series) :
    (((pairs y).filter (fun t : Int × Int => decide (t.1 < t.2))).countP
        (fun t : Int × Int => decide (a < t.1)) : Int) = belowPairs a y := by
  rw [List.countP_filter]
  simp [belowPairs]

private theorem sum_countP_cons (a : Int) (xs : Series) (ts : List (Int × Int)) :
    ((ts.map (fun t => (a :: xs).countP (fun v => decide (v < t.1)))).sum : Int) =
      ((ts.map (fun t => xs.countP (fun v => decide (v < t.1)))).sum : Int) +
        (ts.countP (fun t : Int × Int => decide (a < t.1)) : Int) := by
  induction ts with
  | nil => simp
  | cons t ts ih =>
      simp only [List.map_cons, List.sum_cons]
      push_cast
      rw [ih]
      simp [List.countP_cons]
      omega

private def crossAbove (xs y : Series) : Int :=
  (((pairs y).filter (fun t : Int × Int => decide (t.1 < t.2))).map (fun t => xs.countP (fun v => decide (v < t.1)))).sum

private theorem crossAbove_cons (a : Int) (xs y : Series) :
    crossAbove (a :: xs) y = crossAbove xs y + belowPairs a y := by
  unfold crossAbove
  rw [sum_countP_cons]
  rw [countP_filter_rising_eq_below]

private theorem crossBelow_as_filtered_map (a : Int) (xs y : Series) :
    crossBelow a xs y =
      (((xs.map (fun b => (a, b))).filter (fun t : Int × Int => decide (t.1 < t.2))).map (fun t => y.countP (fun c => decide (t.2 < c)))).sum := by
  unfold crossBelow
  induction xs with
  | nil => simp
  | cons b bs ih =>
      by_cases h : a < b <;> simp [h, ih]

private def leftSum (xs y : Series) : Int :=
  (((pairs xs).filter (fun t : Int × Int => decide (t.1 < t.2))).map (fun t => y.countP (fun c => decide (t.2 < c)))).sum

private def rightSum (xs y : Series) : Int :=
  (((pairs y).filter (fun t : Int × Int => decide (t.1 < t.2))).map (fun t => xs.countP (fun v => decide (v < t.1)))).sum

private theorem leftSum_cons (a : Int) (xs y : Series) :
    leftSum (a :: xs) y = leftSum xs y + crossBelow a xs y := by
  unfold leftSum
  rw [pairs_cons, List.filter_append, List.map_append, List.sum_append]
  simp [crossBelow_as_filtered_map]
  omega

private theorem rightSum_cons (a : Int) (xs y : Series) :
    rightSum (a :: xs) y = rightSum xs y + belowPairs a y := by
  unfold rightSum
  exact crossAbove_cons a xs y

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih]

theorem sound_TripleRise_Concatenate : TripleRise.Concatenate.Goal := by
  intro x y
  change TripleRise.measure (x ++ y) =
    TripleRise.measure x + TripleRise.measure y + leftSum x y + rightSum x y
  induction x with
  | nil =>
      simp [TripleRise.measure, triples, pairs, leftSum, rightSum, sum_map_zero]
  | cons a xs ih =>
      rw [List.cons_append, measure_cons, ih, belowPairs_append,
        measure_cons, leftSum_cons, rightSum_cons]
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_Concatenate
