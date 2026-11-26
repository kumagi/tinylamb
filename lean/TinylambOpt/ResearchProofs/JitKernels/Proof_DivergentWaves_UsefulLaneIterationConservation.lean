import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! SOUND: The area under the active-mask population curve equals exactly the scalar lane
work. For each iteration `t < peak xs`, the mask counts lanes with more than `t` steps
left; summing over all iterations charges each lane `x` exactly once per `t < x`, i.e.
`x` times. Formally, for every bound `m` the curve area up to `m` equals
`(xs.map (min · m)).sum`; instantiating `m = peak xs` (so `min x (peak xs) = x` for all
lanes) gives the scalar total. This validates using the mask-popcount integral as a
cost model for masked divergent vector loops. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_UsefulLaneIterationConservation
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-- `count true` of a consed boolean list peels one element. -/
private theorem count_true_cons (b : Bool) (l : List Bool) :
    (b :: l).count true = (if b then 1 else 0) + l.count true := by
  have key : ∀ (m : List Bool) (acc : Nat),
      List.countP.go (fun x => x == true) m acc = m.count true + acc := by
    intro m
    induction m with
    | nil =>
        intro acc
        rw [show List.count true [] = 0 from rfl, Nat.zero_add]
        rfl
    | cons x xs ih =>
        intro acc
        cases x with
        | true =>
          show List.countP.go (fun y => y == true) xs (acc + 1)
              = List.countP.go (fun y => y == true) xs 1 + acc
          rw [ih (acc + 1), ih 1]; omega
        | false =>
          show List.countP.go (fun y => y == true) xs acc
              = List.countP.go (fun y => y == true) xs 0 + acc
          rw [ih acc, ih 0]; omega
  cases b with
  | true =>
    show List.countP.go (fun y => y == true) l 1 = 1 + l.count true
    rw [key l 1]; omega
  | false =>
    show List.countP.go (fun y => y == true) l 0 = 0 + l.count true
    rw [key l 0]; omega

/-- A `0`-valued map sums to zero. -/
private theorem sum_map_zero (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [List.map_cons]; simp [ih]

/-- Sums of pointwise additions of two maps split. -/
private theorem sum_map_add (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun t => f t + g t)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [List.map_cons, List.sum_cons, ih, List.map_cons, List.map_cons,
      List.sum_cons, List.sum_cons]
    omega

/-- The number of iterations below `a` inside the first `m` iterations is `min a m`. -/
private theorem sum_ite_range : ∀ a m : Nat,
    ((List.range m).map (fun t => if t < a then 1 else 0)).sum = Nat.min a m := by
  intro a m
  induction m with
  | zero =>
      rw [List.range_zero, List.map_nil, List.sum_nil]
      have h0 : Nat.min a 0 = 0 := Nat.min_eq_right (Nat.zero_le a)
      rw [h0]
  | succ k ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih,
        show (List.map (fun t => if t < a then 1 else 0) [k]).sum
            = (if k < a then 1 else 0) from rfl]
      by_cases h : k < a
      · have h1 : Nat.min a k = k := Nat.min_eq_right (by omega)
        have h2 : Nat.min a (k + 1) = k + 1 := Nat.min_eq_right (by omega)
        rw [if_pos h, h1, h2]
      · have h1 : Nat.min a k = a := Nat.min_eq_left (by omega)
        have h2 : Nat.min a (k + 1) = a := Nat.min_eq_left (by omega)
        rw [if_neg h, h1, h2]
        omega

/-- The curve area below any bound `m` equals the clamped lane work. -/
private theorem F_main : ∀ xs : List Nat, ∀ m : Nat,
    ((List.range m).map (fun t => (maskAt xs t).count true)).sum
      = (xs.map (fun x => Nat.min x m)).sum := by
  intro xs
  induction xs with
  | nil =>
      intro m
      rw [show (fun t => (maskAt [] t).count true) = (fun _ => 0) from rfl,
        sum_map_zero, List.map_nil, List.sum_nil]
  | cons a as ih =>
      intro m
      have hp : ∀ t : Nat, (maskAt (a :: as) t).count true
          = (if t < a then 1 else 0) + (maskAt as t).count true := by
        intro t
        show (decide (t < a) :: maskAt as t).count true
            = (if t < a then 1 else 0) + (maskAt as t).count true
        rw [count_true_cons]
        by_cases h : t < a <;> simp [h]
      rw [congrArg List.sum (List.map_congr_left fun t _ => hp t), sum_map_add, sum_ite_range,
        ih]
      rw [List.map_cons, List.sum_cons]

/-- The fold accumulator never exceeds the running maximum. -/
private theorem le_foldl_max :
    ∀ (as : List Nat) (init : Nat), init ≤ as.foldl max init := by
  intro as
  induction as with
  | nil => intro init; exact Nat.le_refl init
  | cons a as ih =>
      intro init
      show init ≤ as.foldl max (max init a)
      exact Nat.le_trans (Nat.le_max_left init a) (ih (max init a))

/-- Every element never exceeds the running maximum. -/
private theorem mem_foldl_max :
    ∀ (as : List Nat) (init x : Nat), x ∈ as → x ≤ as.foldl max init := by
  intro as
  induction as with
  | nil => intro init x h; cases h
  | cons a as ih =>
      intro init x h
      show x ≤ as.foldl max (max init a)
      rw [List.mem_cons] at h
      cases h with
      | inl he =>
          rw [he]
          exact Nat.le_trans (Nat.le_max_right init a) (le_foldl_max as (max init a))
      | inr hx => exact ih (max init a) x hx

/-- Every lane's work is bounded by the peak. -/
private theorem peak_le_mem : ∀ (x : Nat) (xs : List Nat), x ∈ xs → x ≤ peak xs := by
  intro x xs h
  cases xs with
  | nil => cases h
  | cons a as =>
      rw [List.mem_cons] at h
      cases h with
      | inl he => rw [he]; exact le_foldl_max as a
      | inr hx => exact mem_foldl_max as a x hx

/-- The mask-popcount integral equals the total scalar lane work. -/
theorem sound : UsefulLaneIterationConservation.Goal := by
  intro xs
  show ((List.range (peak xs)).map (fun t => (maskAt xs t).count true)).sum = xs.sum
  rw [F_main]
  have hmin : xs.map (fun x => Nat.min x (peak xs)) = xs.map (fun x => x) :=
    List.map_congr_left (fun x hx => Nat.min_eq_left (peak_le_mem x xs hx))
  rw [hmin]
  show (xs.map (fun x => x)).sum = xs.sum
  exact congrArg List.sum (List.map_id xs)

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_UsefulLaneIterationConservation
