import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! CollisionCorrection for `MovieCredits`: the injective (all-distinct) join
equals the signed sum over equality-block collapses. The five set partitions
of `freeVars = [2,3,1]` contribute weights `+1,-1,-1,-1,+2`, i.e. pointwise
inclusion-exclusion on the three equality relations of the variables. -/

namespace TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_CollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

/-! ## Finite sum helpers (Std only). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private def triple (n : Nat) (f : Nat → Nat → Nat → Int) : Int :=
  rsum n (fun a => rsum n (fun bb => rsum n (fun c => f a bb c)))

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
    triple n (fun a bb c => f a bb c + g a bb c) =
      triple n f + triple n g := by
  unfold triple
  rw [← rsum_add]
  apply rsum_congr; intro a ha
  rw [← rsum_add]
  apply rsum_congr; intro bb hbb
  rw [rsum_add]

private theorem triple_sub (n : Nat) (f g : Nat → Nat → Nat → Int) :
    triple n (fun a bb c => f a bb c - g a bb c) =
      triple n f - triple n g := by
  unfold triple
  rw [← rsum_sub]
  apply rsum_congr; intro a ha
  rw [← rsum_sub]
  apply rsum_congr; intro bb hbb
  rw [rsum_sub]

private theorem triple_const_mul (n : Nat) (k : Int) (f : Nat → Nat → Nat → Int) :
    triple n (fun a bb c => k * f a bb c) = k * triple n f := by
  unfold triple
  rw [← rsum_const_mul]
  apply rsum_congr; intro a ha
  rw [← rsum_const_mul]
  apply rsum_congr; intro bb hbb
  rw [rsum_const_mul]

/-! ## Goal CollisionCorrection: injective assignments as a signed partition sum. -/

theorem sound_MovieCredits_CollisionCorrection : MovieCredits.CollisionCorrection.Goal := by
  intro n db b
  simp [injective, collapsed, partitions, mobius, factorial, integrate, value, put, pattern]
  let P : Nat → Nat → Nat → Int := fun a bb c => db 0 c a * (db 1 a bb * db 2 bb c)
  change rsum n (fun a => rsum n (fun bb => rsum n (fun c =>
      if ((a ≠ bb ∧ a ≠ c) ∧ bb ≠ c) then P a bb c else 0))) =
    rsum n (fun a => rsum n (fun bb => rsum n (fun c => P a bb c))) +
      (-(rsum n (fun a => rsum n (fun c => P a a c))) +
        (-(rsum n (fun bb => rsum n (fun a => P a bb a))) +
          (-(rsum n (fun a => rsum n (fun bb => P a bb bb))) +
            2 * rsum n (fun a => P a a a))))
  have hpoint : ∀ a bb c : Nat,
      (if ((a ≠ bb ∧ a ≠ c) ∧ bb ≠ c) then P a bb c else 0) =
        P a bb c - (if bb = a then P a bb c else 0)
          - (if c = a then P a bb c else 0)
          - (if c = bb then P a bb c else 0)
          + 2 * (if (bb = a ∧ c = a) then P a bb c else 0) := by
    intro a bb c
    by_cases hab : a = bb <;> by_cases hac : a = c <;>
      by_cases hbc : bb = c <;> simp [hab, hac, hbc, eq_comm] <;> omega
  rw [show rsum n (fun a => rsum n (fun bb => rsum n (fun c =>
      if ((a ≠ bb ∧ a ≠ c) ∧ bb ≠ c) then P a bb c else 0))) =
      triple n (fun a bb c =>
        P a bb c - (if bb = a then P a bb c else 0)
          - (if c = a then P a bb c else 0)
          - (if c = bb then P a bb c else 0)
          + 2 * (if (bb = a ∧ c = a) then P a bb c else 0)) by
    change triple n (fun a bb c =>
      if ((a ≠ bb ∧ a ≠ c) ∧ bb ≠ c) then P a bb c else 0) = _
    unfold triple
    apply rsum_congr; intro a ha
    apply rsum_congr; intro bb hbb
    apply rsum_congr; intro c hc
    exact hpoint a bb c]
  rw [triple_add, triple_sub, triple_sub, triple_sub, triple_const_mul]
  have hba : triple n (fun a bb c => if bb = a then P a bb c else 0) =
      rsum n (fun a => rsum n (fun c => P a a c)) := by
    unfold triple
    apply rsum_congr; intro a ha
    calc
      rsum n (fun bb => rsum n (fun c => if bb = a then P a bb c else 0))
          = rsum n (fun bb => if bb = a then rsum n (fun c => P a a c) else 0) := by
            apply rsum_congr; intro bb hbb
            by_cases h : bb = a
            · subst bb; simp
            · simpa [h] using (rsum_zero_const n)
      _ = rsum n (fun c => P a a c) := by
            rw [rsum_pick n a (fun _ => rsum n (fun c => P a a c)) ha]
  have hca : triple n (fun a bb c => if c = a then P a bb c else 0) =
      rsum n (fun bb => rsum n (fun a => P a bb a)) := by
    unfold triple
    calc
      rsum n (fun a => rsum n (fun bb => rsum n (fun c => if c = a then P a bb c else 0)))
          = rsum n (fun bb => rsum n (fun a => rsum n (fun c => if c = a then P a bb c else 0))) := by
            rw [rsum_comm]
      _ = rsum n (fun bb => rsum n (fun a => P a bb a)) := by
            apply rsum_congr; intro bb hbb
            apply rsum_congr; intro a ha
            exact rsum_pick n a (fun c => P a bb c) ha
  have hcb : triple n (fun a bb c => if c = bb then P a bb c else 0) =
      rsum n (fun a => rsum n (fun bb => P a bb bb)) := by
    unfold triple
    apply rsum_congr; intro a ha
    apply rsum_congr; intro bb hbb
    exact rsum_pick n bb (fun c => P a bb c) hbb
  have hall : triple n (fun a bb c => if (bb = a ∧ c = a) then P a bb c else 0) =
      rsum n (fun a => P a a a) := by
    unfold triple
    apply rsum_congr; intro a ha
    calc
      rsum n (fun bb => rsum n (fun c => if (bb = a ∧ c = a) then P a bb c else 0))
          = rsum n (fun bb => if bb = a then P a a a else 0) := by
            apply rsum_congr; intro bb hbb
            by_cases h : bb = a
            · subst bb
              simpa [true_and] using (rsum_pick n a (fun c => P a a c) ha)
            · simpa [h] using (rsum_zero_const n)
      _ = P a a a := by
            rw [rsum_pick n a (fun _ => P a a a) ha]
  have hS0 : rsum n (fun a => rsum n (fun bb => rsum n (fun c => P a bb c))) =
      triple n P := rfl
  rw [hS0, hba, hca, hcb, hall]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_MovieCredits_CollisionCorrection
