import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `DoubleBilling.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DoubleBilling_CollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

/-! ## Finite sum helpers (Std only). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private def triple (n : Nat) (f : Nat → Nat → Nat → Int) : Int :=
  rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 => f x1 x3 x2)))

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n + 1) f = rsum n f + f n := by
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

private theorem rsum_neg (n : Nat) (f : Nat → Int) : rsum n (fun i => -f i) = -rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => simp [rsum_succ, ih]; omega

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  simp only [Int.sub_eq_add_neg]
  rw [rsum_add, rsum_neg]

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr; intro j hj; rw [rsum_zero]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n + 1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_ite_eq (n k : Nat) (f : Nat → Int) :
    rsum n (fun i => if i == k then f i else 0) = if k < n then f k else 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      by_cases hkn : k < n
      · have hne : (n == k) = false := by
          by_cases h : n == k
          · have : n = k := Nat.eq_of_beq_eq_true h
            omega
          · simpa using h
        simp [hkn, Nat.lt_succ_of_lt hkn, hne]
      · by_cases hkeq : k == n
        · have hk : k = n := Nat.eq_of_beq_eq_true hkeq
          subst k
          simp [hkn]
        · have hkne : k ≠ n := by intro h; subst k; simp at hkeq
          have hkgt : n < k := by omega
          have hnlt : ¬ k < n + 1 := by omega
          simp [hkn, hnlt]
          intro h
          omega

private theorem rsum_pick (n k : Nat) (f : Nat → Int) (hk : k < n) :
    rsum n (fun i => if i = k then f i else 0) = f k := by
  rw [show (fun i => if i = k then f i else 0) =
      (fun i => if i == k then f i else 0) by
    funext i
    by_cases h : i = k
    · subst i; simp
    · have hb : (i == k) = false := by cases hb' : i == k <;> simp_all
      simp [h, hb]]
  rw [rsum_ite_eq]
  simp [hk]

private theorem triple_add (n : Nat) (f g : Nat → Nat → Nat → Int) :
    triple n (fun x1 x3 x2 => f x1 x3 x2 + g x1 x3 x2) =
      triple n f + triple n g := by
  unfold triple
  rw [← rsum_add]
  apply rsum_congr; intro x1 hx1
  rw [← rsum_add]
  apply rsum_congr; intro x3 hx3
  rw [rsum_add]

private theorem triple_sub (n : Nat) (f g : Nat → Nat → Nat → Int) :
    triple n (fun x1 x3 x2 => f x1 x3 x2 - g x1 x3 x2) =
      triple n f - triple n g := by
  unfold triple
  rw [← rsum_sub]
  apply rsum_congr; intro x1 hx1
  rw [← rsum_sub]
  apply rsum_congr; intro x3 hx3
  rw [rsum_sub]

private theorem triple_const_mul (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    triple n (fun x1 x3 x2 => c * f x1 x3 x2) = c * triple n f := by
  unfold triple
  rw [← rsum_const_mul]
  apply rsum_congr; intro x1 hx1
  rw [← rsum_const_mul]
  apply rsum_congr; intro x3 hx3
  rw [rsum_const_mul]

/-! ## Goal CollisionCorrection: injective assignments as a signed partition sum.

The five set partitions of `freeVars = [1,3,2]` contribute weights
`+1, -1, -1, -1, +2`; point-wise this is exactly inclusion-exclusion over
the three possible collisions of the variables `1, 3, 2`. -/

theorem sound_DoubleBilling_CollisionCorrection : DoubleBilling.CollisionCorrection.Goal := by
  intro n db b
  simp [injective, collapsed, partitions, mobius, factorial, integrate, value, put, pattern]
  let P : Nat → Nat → Nat → Int := fun x1 x3 x2 =>
    db 0 (b 0) x1 * (db 0 (b 0) x1 * (db 1 x1 x2 * db 2 (b 0) x3))
  change rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 =>
      if ((x1 ≠ x3 ∧ x1 ≠ x2) ∧ x3 ≠ x2) then P x1 x3 x2 else 0))) =
    rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 => P x1 x3 x2))) +
      (-(rsum n (fun x1 => rsum n (fun x2 => P x1 x1 x2))) +
        (-(rsum n (fun x3 => rsum n (fun x1 => P x1 x3 x1))) +
          (-(rsum n (fun x1 => rsum n (fun x3 => P x1 x3 x3))) +
            2 * rsum n (fun x1 => P x1 x1 x1))))
  have hpoint : ∀ x1 x3 x2 : Nat,
      (if ((x1 ≠ x3 ∧ x1 ≠ x2) ∧ x3 ≠ x2) then P x1 x3 x2 else 0) =
        P x1 x3 x2 - (if x2 = x3 then P x1 x3 x2 else 0)
          - (if x2 = x1 then P x1 x3 x2 else 0)
          - (if x3 = x1 then P x1 x3 x2 else 0)
          + 2 * (if (x2 = x1 ∧ x3 = x1) then P x1 x3 x2 else 0) := by
    intro x1 x3 x2
    by_cases h13 : x1 = x3 <;> by_cases h12 : x1 = x2 <;>
      by_cases h32 : x3 = x2 <;> simp [h13, h12, h32, eq_comm] <;> omega
  rw [show rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 =>
      if ((x1 ≠ x3 ∧ x1 ≠ x2) ∧ x3 ≠ x2) then P x1 x3 x2 else 0))) =
      triple n (fun x1 x3 x2 =>
        P x1 x3 x2 - (if x2 = x3 then P x1 x3 x2 else 0)
          - (if x2 = x1 then P x1 x3 x2 else 0)
          - (if x3 = x1 then P x1 x3 x2 else 0)
          + 2 * (if (x2 = x1 ∧ x3 = x1) then P x1 x3 x2 else 0)) by
    change triple n (fun x1 x3 x2 =>
      if ((x1 ≠ x3 ∧ x1 ≠ x2) ∧ x3 ≠ x2) then P x1 x3 x2 else 0) = _
    unfold triple
    apply rsum_congr; intro x1 hx1
    apply rsum_congr; intro x3 hx3
    apply rsum_congr; intro x2 hx2
    exact hpoint x1 x3 x2]
  rw [triple_add, triple_sub, triple_sub, triple_sub, triple_const_mul]
  have h23 : triple n (fun x1 x3 x2 => if x2 = x3 then P x1 x3 x2 else 0) =
      rsum n (fun x1 => rsum n (fun x3 => P x1 x3 x3)) := by
    unfold triple
    apply rsum_congr; intro x1 hx1
    apply rsum_congr; intro x3 hx3
    exact rsum_pick n x3 (fun x2 => P x1 x3 x2) hx3
  have h21 : triple n (fun x1 x3 x2 => if x2 = x1 then P x1 x3 x2 else 0) =
      rsum n (fun x3 => rsum n (fun x1 => P x1 x3 x1)) := by
    unfold triple
    calc
      rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 => if x2 = x1 then P x1 x3 x2 else 0)))
          = rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x2 => if x2 = x1 then P x1 x3 x2 else 0))) := by
            rw [rsum_comm]
      _ = rsum n (fun x3 => rsum n (fun x1 => P x1 x3 x1)) := by
            apply rsum_congr; intro x3 hx3
            apply rsum_congr; intro x1 hx1
            exact rsum_pick n x1 (fun x2 => P x1 x3 x2) hx1
  have h31 : triple n (fun x1 x3 x2 => if x3 = x1 then P x1 x3 x2 else 0) =
      rsum n (fun x1 => rsum n (fun x2 => P x1 x1 x2)) := by
    unfold triple
    apply rsum_congr; intro x1 hx1
    calc
      rsum n (fun x3 => rsum n (fun x2 => if x3 = x1 then P x1 x3 x2 else 0))
          = rsum n (fun x2 => rsum n (fun x3 => if x3 = x1 then P x1 x3 x2 else 0)) := by
            rw [rsum_comm]
      _ = rsum n (fun x2 => P x1 x1 x2) := by
            apply rsum_congr; intro x2 hx2
            exact rsum_pick n x1 (fun x3 => P x1 x3 x2) hx1
  have hAll : triple n (fun x1 x3 x2 => if (x2 = x1 ∧ x3 = x1) then P x1 x3 x2 else 0) =
      rsum n (fun x1 => P x1 x1 x1) := by
    unfold triple
    apply rsum_congr; intro x1 hx1
    calc
      rsum n (fun x3 => rsum n (fun x2 => if (x2 = x1 ∧ x3 = x1) then P x1 x3 x2 else 0))
          = rsum n (fun x3 => if x3 = x1 then P x1 x3 x3 else 0) := by
            apply rsum_congr; intro x3 hx3
            by_cases h : x3 = x1
            · subst x3
              simpa [true_and] using (rsum_pick n x1 (fun x2 => P x1 x1 x2) hx1)
            · simpa [h] using (rsum_zero_const n)
      _ = P x1 x1 x1 := by
            rw [rsum_pick n x1 (fun x3 => P x1 x3 x3) hx1]
  have hS0 : rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 => P x1 x3 x2))) =
      triple n P := rfl
  rw [hS0, h23, h21, h31, hAll]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_DoubleBilling_CollisionCorrection
