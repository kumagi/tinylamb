import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `DoubleBilling.SquaredDelta` — `∀ n db delta b, (∀ r x y, r ≠ 0 → delta r x y = 0) →
  diff 2 (fun u => reference pattern n (slope db delta u) b) 0 = 2*reference pattern
  n (fun r => if r == 0 then delta 0 else db r) b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DoubleBilling_SquaredDelta

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

/-! Finite-range integer sums and second forward differences. Under the
hypothesis that only the doubled edge `rel = 0` changes, the join evaluates to
a quadratic in the slope parameter whose leading coefficient is the delta-only
join; hence the second difference is twice that join. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := sum_mul_right (List.range n) f c

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero => simp [rsum, sum_zero]
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := rsum_mul_const n f (rsum m g)

private theorem diff2_add (f g : Int → Int) :
    diff 2 (fun u => f u + g u) 0 = diff 2 f 0 + diff 2 g 0 := by
  simp [diff]; omega

private theorem diff2_rsum (n : Nat) (f : Nat → Int → Int) :
    diff 2 (fun u => rsum n (fun i => f i u)) 0 = rsum n (fun i => diff 2 (f i) 0) := by
  induction n with
  | zero => simp [rsum, diff]
  | succ n ih =>
      rw [show (fun u => rsum (n+1) (fun i => f i u)) =
          (fun u => rsum n (fun i => f i u) + f n u) by funext u; rw [rsum_succ]]
      rw [diff2_add, ih, rsum_succ]

private theorem diff2_square_affine (A d K : Int) :
    diff 2 (fun u => (A + u*d) * (A + u*d) * K) 0 = 2*d*d*K := by
  simp [diff]; grind

private theorem double_eval (n : Nat) (db delta : DB) (b : Bind) (t : Int)
    (hdelta : ∀ r x y, r ≠ 0 → delta r x y = 0) :
    reference pattern n (slope db delta t) b =
      rsum n (fun v => (db 0 (b 0) v + t*delta 0 (b 0) v) *
        ((db 0 (b 0) v + t*delta 0 (b 0) v) *
          (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) := by
  simp [reference, integrate, pattern, value, put, slope, hdelta]
  change rsum n (fun v => rsum n (fun u => rsum n (fun w =>
      (db 0 (b 0) v + t*delta 0 (b 0) v) *
      ((db 0 (b 0) v + t*delta 0 (b 0) v) *
        (db 1 v w * db 2 (b 0) u))))) = _
  apply rsum_congr
  intro v hv
  rw [rsum_comm]
  rw [show (fun j => rsum n (fun i =>
      (db 0 (b 0) v + t*delta 0 (b 0) v) *
      ((db 0 (b 0) v + t*delta 0 (b 0) v) *
        (db 1 v j * db 2 (b 0) i)))) =
      (fun j => rsum n (fun i =>
        ((db 0 (b 0) v + t*delta 0 (b 0) v) *
          ((db 0 (b 0) v + t*delta 0 (b 0) v) * db 2 (b 0) i)) * db 1 v j)) by
    funext j
    apply rsum_congr
    intro i hi
    ac_rfl]
  rw [← rsum_comm, rsum_product]
  rw [rsum_const_mul, rsum_const_mul]
  ac_rfl

private theorem double_delta_eval (n : Nat) (db delta : DB) (b : Bind)
    (_hdelta : ∀ r x y, r ≠ 0 → delta r x y = 0) :
    reference pattern n (fun r => if r == 0 then delta 0 else db r) b =
      rsum n (fun v => delta 0 (b 0) v *
        (delta 0 (b 0) v * (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) := by
  simp [reference, integrate, pattern, value, put]
  change rsum n (fun v => rsum n (fun u => rsum n (fun w =>
      delta 0 (b 0) v * (delta 0 (b 0) v *
        (db 1 v w * db 2 (b 0) u))))) = _
  apply rsum_congr
  intro v hv
  rw [rsum_comm]
  rw [show (fun j => rsum n (fun i => delta 0 (b 0) v *
        (delta 0 (b 0) v * (db 1 v j * db 2 (b 0) i)))) =
      (fun j => rsum n (fun i =>
        (delta 0 (b 0) v * (delta 0 (b 0) v * db 2 (b 0) i)) * db 1 v j)) by
    funext j
    apply rsum_congr
    intro i hi
    ac_rfl]
  rw [← rsum_comm, rsum_product]
  rw [rsum_const_mul, rsum_const_mul]
  ac_rfl

theorem sound_DoubleBilling_SquaredDelta : SquaredDelta.Goal := by
  intro n db delta b hdelta
  change diff 2 (fun u => reference pattern n (slope db delta u) b) 0 = _
  have heval : (fun u => reference pattern n (slope db delta u) b) =
      (fun u => rsum n (fun v =>
        (db 0 (b 0) v + u*delta 0 (b 0) v) *
          ((db 0 (b 0) v + u*delta 0 (b 0) v) *
            (rsum n (db 2 (b 0)) * rsum n (db 1 v))))) := by
    funext u
    exact double_eval n db delta b u hdelta
  rw [heval]
  calc
    diff 2 (fun u => rsum n (fun v =>
        (db 0 (b 0) v + u*delta 0 (b 0) v) *
          ((db 0 (b 0) v + u*delta 0 (b 0) v) *
            (rsum n (db 2 (b 0)) * rsum n (db 1 v))))) 0
        = rsum n (fun v => diff 2 (fun u =>
            (db 0 (b 0) v + u*delta 0 (b 0) v) *
              ((db 0 (b 0) v + u*delta 0 (b 0) v) *
                (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) 0) :=
          diff2_rsum n _
    _ = 2 * rsum n (fun v => delta 0 (b 0) v *
          (delta 0 (b 0) v * (rsum n (db 2 (b 0)) * rsum n (db 1 v)))) := by
          rw [← rsum_const_mul]
          apply rsum_congr
          intro v hv
          simpa [Int.mul_assoc] using
            diff2_square_affine (db 0 (b 0) v) (delta 0 (b 0) v)
              (rsum n (db 2 (b 0)) * rsum n (db 1 v))
    _ = 2 * reference pattern n (fun r => if r == 0 then delta 0 else db r) b := by
          rw [double_delta_eval n db delta b hdelta]

end TinylambOpt.ResearchProofs.Atlas.Goal_DoubleBilling_SquaredDelta
