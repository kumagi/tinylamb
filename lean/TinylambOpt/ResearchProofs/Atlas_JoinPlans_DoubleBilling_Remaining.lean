import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBilling.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

/-- Sanity checks on the concrete constants used by the DoubleBilling pattern. -/
example : pattern.freeVars = [1, 3, 2] := by rfl
example : pattern.atoms =
    [(⟨0, 0, 1⟩ : Atom), ⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 0, 3⟩] := by rfl

example : partitions pattern.freeVars =
    [[[1], [3], [2]], [[1, 3], [2]], [[3], [1, 2]], [[1], [3, 2]], [[1, 3, 2]]] := by
  decide

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) =
          rsum m (fun _ => (0 : Int)) by
        apply rsum_congr; intro j hj; simp [rsum]
        ]
      rw [rsum_zero]
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_ite_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : a = n
      · subst a
        have hzero : rsum n (fun x => if x = n then f x else 0) =
            rsum n (fun _ => (0 : Int)) := by
          apply rsum_congr
          intro x hx
          have hxn : x ≠ n := by omega
          simp [hxn]
        rw [hzero, rsum_zero]
        simp
      · have han : a < n := by omega
        rw [ih han]
        have hne : ¬ n = a := by omega
        simp [h, hne]

private def T (n : Nat) (F : Nat → Nat → Nat → Int) : Int :=
  rsum n (fun v1 => rsum n (fun v3 => rsum n (fun v2 => F v1 v3 v2)))

private theorem T_add (n : Nat) (F G : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => F v1 v3 v2 + G v1 v3 v2) = T n F + T n G := by
  unfold T
  rw [show (fun v1 => rsum n (fun v3 => rsum n (fun v2 =>
      F v1 v3 v2 + G v1 v3 v2))) =
      (fun v1 => rsum n (fun v3 => rsum n (fun v2 => F v1 v3 v2)) +
        rsum n (fun v3 => rsum n (fun v2 => G v1 v3 v2))) by
    funext v1
    rw [show (fun v3 => rsum n (fun v2 => F v1 v3 v2 + G v1 v3 v2)) =
        (fun v3 => rsum n (fun v2 => F v1 v3 v2) + rsum n (fun v2 => G v1 v3 v2)) by
      funext v3
      rw [rsum_add]]
    rw [rsum_add]]
  rw [rsum_add]

private theorem T_sub (n : Nat) (F G : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => F v1 v3 v2 - G v1 v3 v2) = T n F - T n G := by
  unfold T
  rw [show (fun v1 => rsum n (fun v3 => rsum n (fun v2 =>
      F v1 v3 v2 - G v1 v3 v2))) =
      (fun v1 => rsum n (fun v3 => rsum n (fun v2 => F v1 v3 v2)) -
        rsum n (fun v3 => rsum n (fun v2 => G v1 v3 v2))) by
    funext v1
    rw [show (fun v3 => rsum n (fun v2 => F v1 v3 v2 - G v1 v3 v2)) =
        (fun v3 => rsum n (fun v2 => F v1 v3 v2) - rsum n (fun v2 => G v1 v3 v2)) by
      funext v3
      rw [rsum_sub]]
    rw [rsum_sub]]
  rw [rsum_sub]

private theorem T_const_mul (n : Nat) (c : Int) (F : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => c * F v1 v3 v2) = c * T n F := by
  unfold T
  rw [show (fun v1 => rsum n (fun v3 => rsum n (fun v2 =>
      c * F v1 v3 v2))) =
      (fun v1 => c * rsum n (fun v3 => rsum n (fun v2 => F v1 v3 v2))) by
    funext v1
    rw [show (fun v3 => rsum n (fun v2 => c * F v1 v3 v2)) =
        (fun v3 => c * rsum n (fun v2 => F v1 v3 v2)) by
      funext v3
      rw [rsum_const_mul]]
    rw [rsum_const_mul]]
  rw [rsum_const_mul]

private theorem T_congr (n : Nat) {F G : Nat → Nat → Nat → Int}
    (h : ∀ v1 v3 v2, F v1 v3 v2 = G v1 v3 v2) : T n F = T n G := by
  unfold T
  apply rsum_congr
  intro v1 hv1
  apply rsum_congr
  intro v3 hv3
  apply rsum_congr
  intro v2 hv2
  exact h v1 v3 v2

private theorem triple_ie_point (v1 v3 v2 : Nat) (F : Int) :
    (if v1 ≠ v3 ∧ v1 ≠ v2 ∧ v3 ≠ v2 then F else 0) =
      F - (if v3 = v1 then F else 0) - (if v2 = v1 then F else 0) -
        (if v2 = v3 then F else 0) +
          2 * (if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then F else 0) := by
  by_cases h13 : v1 = v3
  · by_cases h12 : v1 = v2
    · have h32 : v3 = v2 := by omega
      simp [h13, h12, h32, eq_comm] <;> omega
    · by_cases h32 : v3 = v2
      · have : v1 = v2 := by omega
        contradiction
      · simp [h13, h12, h32, eq_comm] <;> omega
  · by_cases h12 : v1 = v2
    · by_cases h32 : v3 = v2
      · have : v1 = v3 := by omega
        contradiction
      · simp [h13, h12, h32, eq_comm] <;> omega
    · by_cases h32 : v3 = v2
      · simp [h13, h12, h32, eq_comm] <;> omega
      · simp [h13, h12, h32, eq_comm] <;> omega

private theorem T_distinct (n : Nat) (F : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => if v1 ≠ v3 ∧ v1 ≠ v2 ∧ v3 ≠ v2 then F v1 v3 v2 else 0) =
      T n F - T n (fun v1 v3 v2 => if v3 = v1 then F v1 v3 v2 else 0) -
        T n (fun v1 v3 v2 => if v2 = v1 then F v1 v3 v2 else 0) -
          T n (fun v1 v3 v2 => if v2 = v3 then F v1 v3 v2 else 0) +
            2 * T n (fun v1 v3 v2 =>
              if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then F v1 v3 v2 else 0) := by
  rw [show (fun v1 v3 v2 =>
      (if v1 ≠ v3 ∧ v1 ≠ v2 ∧ v3 ≠ v2 then F v1 v3 v2 else 0)) =
      (fun v1 v3 v2 =>
        F v1 v3 v2 - (if v3 = v1 then F v1 v3 v2 else 0) -
          (if v2 = v1 then F v1 v3 v2 else 0) - (if v2 = v3 then F v1 v3 v2 else 0) +
            2 * (if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then F v1 v3 v2 else 0)) by
    funext v1 v3 v2
    exact triple_ie_point v1 v3 v2 (F v1 v3 v2)]
  rw [T_add]
  rw [T_sub, T_sub, T_sub, T_const_mul]

private theorem nodup3 (a c d : Nat) : [a, c, d].Nodup ↔ a ≠ c ∧ a ≠ d ∧ c ≠ d := by
  simp
  constructor
  · intro h
    exact ⟨h.1.1, h.1.2, h.2⟩
  · intro h
    exact ⟨⟨h.1, h.2.1⟩, h.2.2⟩

private def Fdb (db : DB) (b : Bind) (v1 v3 v2 : Nat) : Int :=
  db 0 (b 0) v1 * db 0 (b 0) v1 * db 1 v1 v2 * db 2 (b 0) v3

private theorem injective_as_T (n : Nat) (db : DB) (b : Bind) :
    injective pattern n db b =
      T n (fun v1 v3 v2 =>
        if v1 ≠ v3 ∧ v1 ≠ v2 ∧ v3 ≠ v2 then Fdb db b v1 v3 v2 else 0) := by
  unfold injective T rsum
  simp [Fdb, pattern, integrate, value, put, nodup3, Int.mul_assoc, and_assoc]

private theorem collapsed_all (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1], [3], [2]] = T n (Fdb db b) := by
  unfold collapsed T rsum
  simp [Fdb, pattern, integrate, value, put, Int.mul_assoc]

private theorem T_eq13 (n : Nat) (F : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => if v3 = v1 then F v1 v3 v2 else 0) =
      rsum n (fun v1 => rsum n (fun v2 => F v1 v1 v2)) := by
  unfold T
  apply rsum_congr
  intro v1 hv1
  rw [show (fun v3 => rsum n (fun v2 => if v3 = v1 then F v1 v3 v2 else 0)) =
      (fun v3 => if v3 = v1 then rsum n (fun v2 => F v1 v3 v2) else 0) by
    funext v3
    by_cases h : v3 = v1
    · simp [h]
    · simp [h]
      rw [rsum_zero]]
  rw [rsum_ite_eq_single n v1 hv1]

private theorem collapsed_eq13_raw (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1, 3], [2]] =
      rsum n (fun v1 => rsum n (fun v2 => Fdb db b v1 v1 v2)) := by
  unfold collapsed rsum
  simp [Fdb, pattern, integrate, value, put, Int.mul_assoc]

private theorem collapsed_eq13 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1, 3], [2]] =
      T n (fun v1 v3 v2 => if v3 = v1 then Fdb db b v1 v3 v2 else 0) := by
  rw [collapsed_eq13_raw, T_eq13]

private theorem T_eq12 (n : Nat) (F : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => if v2 = v1 then F v1 v3 v2 else 0) =
      rsum n (fun v1 => rsum n (fun v3 => F v1 v3 v1)) := by
  unfold T
  apply rsum_congr
  intro v1 hv1
  apply rsum_congr
  intro v3 hv3
  exact rsum_ite_eq_single n v1 hv1 (fun v2 => F v1 v3 v2)

private theorem collapsed_eq12_raw (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[3], [1, 2]] =
      rsum n (fun v3 => rsum n (fun v1 => Fdb db b v1 v3 v1)) := by
  unfold collapsed rsum
  simp [Fdb, pattern, integrate, value, put, Int.mul_assoc]

private theorem collapsed_eq12 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[3], [1, 2]] =
      T n (fun v1 v3 v2 => if v2 = v1 then Fdb db b v1 v3 v2 else 0) := by
  rw [collapsed_eq12_raw, T_eq12, rsum_comm]

private theorem T_eq23 (n : Nat) (F : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => if v2 = v3 then F v1 v3 v2 else 0) =
      rsum n (fun v1 => rsum n (fun v3 => F v1 v3 v3)) := by
  unfold T
  apply rsum_congr
  intro v1 hv1
  apply rsum_congr
  intro v3 hv3
  exact rsum_ite_eq_single n v3 hv3 (fun v2 => F v1 v3 v2)

private theorem collapsed_eq23_raw (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1], [3, 2]] =
      rsum n (fun v1 => rsum n (fun v3 => Fdb db b v1 v3 v3)) := by
  unfold collapsed rsum
  simp [Fdb, pattern, integrate, value, put, Int.mul_assoc]

private theorem collapsed_eq23 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1], [3, 2]] =
      T n (fun v1 v3 v2 => if v2 = v3 then Fdb db b v1 v3 v2 else 0) := by
  rw [collapsed_eq23_raw, T_eq23]

private theorem T_eqall (n : Nat) (F : Nat → Nat → Nat → Int) :
    T n (fun v1 v3 v2 => if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then F v1 v3 v2 else 0) =
      rsum n (fun v1 => F v1 v1 v1) := by
  unfold T
  apply rsum_congr
  intro v1 hv1
  rw [show (fun v3 => rsum n (fun v2 => if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then F v1 v3 v2 else 0)) =
      (fun v3 => if v1 = v3 then F v1 v3 v1 else 0) by
    funext v3
    by_cases h13 : v1 = v3
    · subst v3
      simp only [true_and, and_self, ite_true]
      rw [show (fun v2 => if v1 = v2 then F v1 v1 v2 else 0) =
          (fun v2 => if v2 = v1 then F v1 v1 v2 else 0) by
        funext v2
        by_cases h' : v1 = v2
        · subst v2
          simp
        · have h'' : ¬ v2 = v1 := by omega
          simp only [h', h'', ite_false]]
      rw [rsum_ite_eq_single n v1 hv1]
    · have hz : rsum n (fun v2 => if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then F v1 v3 v2 else 0) = 0 := by
        rw [show (fun v2 => if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then F v1 v3 v2 else 0) =
            (fun _ => (0 : Int)) by
          funext v2
          simp [h13]]
        rw [rsum_zero]
      rw [hz]
      simp [h13]]
  rw [show (fun v3 => if v1 = v3 then F v1 v3 v1 else 0) =
      (fun v3 => if v3 = v1 then F v1 v3 v1 else 0) by
    funext v3
    by_cases h' : v1 = v3
    · subst v3
      simp
    · have h'' : ¬ v3 = v1 := by omega
      simp only [h', h'', ite_false]]
  rw [rsum_ite_eq_single n v1 hv1]

private theorem collapsed_eqall_raw (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1, 3, 2]] = rsum n (fun v1 => Fdb db b v1 v1 v1) := by
  unfold collapsed rsum
  simp [Fdb, pattern, integrate, value, put, Int.mul_assoc]

private theorem collapsed_eqall (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1, 3, 2]] =
      T n (fun v1 v3 v2 => if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then Fdb db b v1 v3 v2 else 0) := by
  rw [collapsed_eqall_raw, T_eqall]

private theorem rhs_as_T (n : Nat) (db : DB) (b : Bind) :
    ((partitions pattern.freeVars).map
      (fun bs => mobius bs * collapsed pattern n db b bs)).sum =
      T n (Fdb db b) - T n (fun v1 v3 v2 => if v3 = v1 then Fdb db b v1 v3 v2 else 0) -
        T n (fun v1 v3 v2 => if v2 = v1 then Fdb db b v1 v3 v2 else 0) -
          T n (fun v1 v3 v2 => if v2 = v3 then Fdb db b v1 v3 v2 else 0) +
            2 * T n (fun v1 v3 v2 =>
              if v1 = v3 ∧ v1 = v2 ∧ v3 = v2 then Fdb db b v1 v3 v2 else 0) := by
  rw [show partitions pattern.freeVars =
      [[[1], [3], [2]], [[1, 3], [2]], [[3], [1, 2]], [[1], [3, 2]], [[1, 3, 2]]] by decide]
  simp
  have hm1 : mobius [[1], [3], [2]] = 1 := by decide
  have hm2 : mobius [[1, 3], [2]] = -1 := by decide
  have hm3 : mobius [[3], [1, 2]] = -1 := by decide
  have hm4 : mobius [[1], [3, 2]] = -1 := by decide
  have hm5 : mobius [[1, 3, 2]] = 2 := by decide
  rw [hm1, hm2, hm3, hm4, hm5]
  simp
  rw [collapsed_all n db b, collapsed_eq13 n db b, collapsed_eq12 n db b,
    collapsed_eq23 n db b, collapsed_eqall n db b]
  omega

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.CollisionCorrection.Goal := by
  intro n db b
  rw [injective_as_T n db b, rhs_as_T n db b]
  exact T_distinct n (Fdb db b)

private def nsum (n : Nat) (f : Nat → Nat) : Nat := ((List.range n).map f).sum

private theorem nsum_succ (n : Nat) (f : Nat → Nat) : nsum (n+1) f = nsum n f + f n := by
  simp [nsum, List.range_succ, List.sum_append]

private theorem nsum_congr (n : Nat) {f g : Nat → Nat} (h : ∀ i, i < n → f i = g i) :
    nsum n f = nsum n g := by
  induction n with
  | zero => simp [nsum]
  | succ n ih =>
      rw [nsum_succ, nsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private theorem nsum_le (n : Nat) {f g : Nat → Nat} (h : ∀ i, i < n → f i ≤ g i) :
    nsum n f ≤ nsum n g := by
  induction n with
  | zero => simp [nsum]
  | succ n ih =>
      rw [nsum_succ, nsum_succ]
      exact Nat.add_le_add (ih (fun i hi => h i (Nat.lt_succ_of_lt hi)))
        (h n (Nat.lt_succ_self n))

private theorem nsum_of_int (n : Nat) (f : Nat → Nat) :
    rsum n (fun i => (f i : Int)) = (nsum n f : Int) := by
  induction n with
  | zero => simp [rsum, nsum]
  | succ n ih => rw [rsum_succ, nsum_succ, ih]; simp

private theorem countP_eq_sum (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [List.countP_cons, ih]
      by_cases h : p a = true <;> simp [h, Nat.add_comm, Nat.add_left_comm]

private def E0 (db : DB) (b : Bind) (v : Nat) : Int := if db 0 (b 0) v = 0 then 0 else 1
private def E1 (db : DB) (u v : Nat) : Int := if db 1 u v = 0 then 0 else 1
private def E2 (db : DB) (b : Bind) (v : Nat) : Int := if db 2 (b 0) v = 0 then 0 else 1

private def c0 (db : DB) (b : Bind) (v : Nat) : Nat := if db 0 (b 0) v = 0 then 0 else 1
private def c1 (db : DB) (u v : Nat) : Nat := if db 1 u v = 0 then 0 else 1
private def c2 (db : DB) (b : Bind) (v : Nat) : Nat := if db 2 (b 0) v = 0 then 0 else 1

private theorem E0_sq (db : DB) (b : Bind) (v : Nat) : E0 db b v * E0 db b v = E0 db b v := by
  unfold E0
  by_cases h : db 0 (b 0) v = 0 <;> simp [h]

private theorem E0_of_nat (db : DB) (b : Bind) (v : Nat) : E0 db b v = (c0 db b v : Int) := by
  unfold E0 c0
  by_cases h : db 0 (b 0) v = 0 <;> simp [h]

private theorem E1_of_nat (db : DB) (u v : Nat) : E1 db u v = (c1 db u v : Int) := by
  unfold E1 c1
  by_cases h : db 1 u v = 0 <;> simp [h]

private theorem E2_of_nat (db : DB) (b : Bind) (v : Nat) : E2 db b v = (c2 db b v : Int) := by
  unfold E2 c2
  by_cases h : db 2 (b 0) v = 0 <;> simp [h]

private def Mcount (n : Nat) (db : DB) (b : Bind) : Nat :=
  nsum n (fun v1 => c0 db b v1 * nsum n (fun v2 => c1 db v1 v2))

private def S3count (n : Nat) (db : DB) (b : Bind) : Nat := nsum n (c2 db b)

private theorem reference_ind_raw (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
      rsum n (fun v1 => rsum n (fun v3 => rsum n (fun v2 =>
        E0 db b v1 * E0 db b v1 * E1 db v1 v2 * E2 db b v3))) := by
  unfold reference rsum
  simp [pattern, integrate, value, put, E0, E1, E2, Int.mul_assoc]

private theorem reference_ind_eq (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
      rsum n (fun v1 => E0 db b v1 * rsum n (fun v2 => E1 db v1 v2)) *
        rsum n (fun v3 => E2 db b v3) := by
  rw [reference_ind_raw]
  rw [show (fun v1 => rsum n (fun v3 => rsum n (fun v2 =>
      E0 db b v1 * E0 db b v1 * E1 db v1 v2 * E2 db b v3))) =
      (fun v1 => rsum n (fun v3 => rsum n (fun v2 =>
        E0 db b v1 * E1 db v1 v2 * E2 db b v3))) by
    funext v1
    apply rsum_congr
    intro v3 hv3
    apply rsum_congr
    intro v2 hv2
    rw [E0_sq]]
  rw [show (fun v1 => rsum n (fun v3 => rsum n (fun v2 =>
      E0 db b v1 * E1 db v1 v2 * E2 db b v3))) =
      (fun v1 => rsum n (fun v3 =>
        (E0 db b v1 * E2 db b v3) * rsum n (fun v2 => E1 db v1 v2))) by
    funext v1
    apply rsum_congr
    intro v3 hv3
    rw [show (fun v2 => E0 db b v1 * E1 db v1 v2 * E2 db b v3) =
        (fun v2 => (E0 db b v1 * E2 db b v3) * E1 db v1 v2) by
      funext v2
      ac_rfl]
    rw [rsum_const_mul]]
  rw [show (fun v1 => rsum n (fun v3 =>
      (E0 db b v1 * E2 db b v3) * rsum n (fun v2 => E1 db v1 v2))) =
      (fun v1 => (E0 db b v1 * rsum n (fun v2 => E1 db v1 v2)) *
        rsum n (fun v3 => E2 db b v3)) by
    funext v1
    rw [show (fun v3 => (E0 db b v1 * E2 db b v3) * rsum n (fun v2 => E1 db v1 v2)) =
        (fun v3 => (E0 db b v1 * rsum n (fun v2 => E1 db v1 v2)) * E2 db b v3) by
      funext v3
      ac_rfl]
    rw [rsum_const_mul]]
  rw [rsum_mul_const]

private theorem reference_toNat (n : Nat) (db : DB) (b : Bind) :
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat =
      Mcount n db b * S3count n db b := by
  rw [reference_ind_eq]
  rw [show (fun v1 => E0 db b v1 * rsum n (fun v2 => E1 db v1 v2)) =
      (fun v1 => ((c0 db b v1 * nsum n (fun v2 => c1 db v1 v2) : Nat) : Int)) by
    funext v1
    rw [E0_of_nat]
    rw [show (fun v2 => E1 db v1 v2) = (fun v2 => (c1 db v1 v2 : Int)) by
      funext v2
      exact E1_of_nat db v1 v2]
    rw [nsum_of_int]
    simp]
  rw [nsum_of_int]
  rw [show (fun v3 => E2 db b v3) = (fun v3 => (c2 db b v3 : Int)) by
    funext v3
    exact E2_of_nat db b v3]
  rw [nsum_of_int]
  rw [Int.toNat_mul (by omega) (by omega)]
  simp [Mcount, S3count]

private theorem ne_indicator (x : Int) : (if x ≠ 0 then 1 else 0) = (if x = 0 then 0 else 1) := by
  by_cases h : x = 0 <;> simp [h]

private theorem support0_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨0, 0, 1⟩ : Atom) pattern b = nsum n (c0 db b) := by
  unfold support
  simp [pattern, countP_eq_sum, nsum, c0, ne_indicator]
  rfl

private theorem support2_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1, 1, 2⟩ : Atom) pattern b =
      nsum n (fun u => nsum n (fun v => c1 db u v)) := by
  unfold support
  simp [pattern, countP_eq_sum, nsum, c1, ne_indicator]

private theorem support3_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2, 0, 3⟩ : Atom) pattern b = S3count n db b := by
  unfold support
  simp [pattern, countP_eq_sum, nsum, S3count, c2, ne_indicator]
  rfl

private theorem nsum_eq_zero_iff (n : Nat) {f : Nat → Nat} :
    nsum n f = 0 ↔ ∀ i, i < n → f i = 0 := by
  induction n with
  | zero => simp [nsum]
  | succ n ih =>
      rw [nsum_succ, Nat.add_eq_zero_iff]
      constructor
      · intro h i hi
        by_cases hin : i < n
        · exact (ih.mp h.1) i hin
        · have hin' : i = n := by omega
          subst i
          exact h.2
      · intro h
        exact ⟨ih.mpr (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)⟩

private theorem Mcount_le_s2 (n : Nat) (db : DB) (b : Bind) :
    Mcount n db b ≤ nsum n (fun u => nsum n (fun v => c1 db u v)) := by
  unfold Mcount
  apply nsum_le
  intro v1 hv1
  have hc : c0 db b v1 ≤ 1 := by
    unfold c0
    by_cases h : db 0 (b 0) v1 = 0 <;> simp [h]
  have hle : nsum n (fun v2 => c1 db v1 v2) ≤ nsum n (fun v2 => c1 db v1 v2) := by omega
  simpa using Nat.mul_le_mul hc hle

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling.FractionalCap.Goal := by
  intro n db b weights den hcov
  have hden : 0 < den := hcov.1
  have hwlen : weights.length = pattern.atoms.length := hcov.2.1
  cases weights with
  | nil => simp [pattern] at hwlen
  | cons w0 ws1 =>
    cases ws1 with
    | nil => simp [pattern] at hwlen
    | cons w1 ws2 =>
      cases ws2 with
      | nil => simp [pattern] at hwlen
      | cons w2 ws3 =>
        cases ws3 with
        | nil => simp [pattern] at hwlen
        | cons w3 ws4 =>
          cases ws4 with
          | nil =>
            have hw3 : den ≤ w3 := by
              have h := hcov.2.2 3 (by simp [pattern])
              simpa [pattern] using h
            have hw2 : den ≤ w2 := by
              have h := hcov.2.2 2 (by simp [pattern])
              simpa [pattern] using h
            rw [reference_toNat n db b]
            have hs0 := support0_eq n db b
            have hs2 := support2_eq n db b
            have hs3 := support3_eq n db b
            simp [pattern] at hs0 hs2 hs3
            simp [pattern]
            rw [hs0, hs2, hs3, Nat.mul_pow]
            by_cases hM : Mcount n db b = 0
            · simp [hM, Nat.zero_pow hden]
            by_cases hS3 : S3count n db b = 0
            · simp [hS3, Nat.zero_pow hden]
            have hS3pos : 0 < S3count n db b := Nat.pos_of_ne_zero hS3
            have hS2pos : 0 < nsum n (fun u => nsum n (fun v => c1 db u v)) := by
              have hle := Mcount_le_s2 n db b
              have hMpos : 0 < Mcount n db b := Nat.pos_of_ne_zero hM
              omega
            have hS0pos : 0 < nsum n (c0 db b) := by
              have hnotzero : nsum n (c0 db b) ≠ 0 := by
                intro hz
                have hzeroTerms := (nsum_eq_zero_iff n).mp hz
                have hMzero : Mcount n db b = 0 := by
                  unfold Mcount
                  apply (nsum_eq_zero_iff n).mpr
                  intro v1 hv1
                  rw [hzeroTerms v1 hv1]
                  simp
                exact hM hMzero
              exact Nat.pos_of_ne_zero hnotzero
            have hpowM : (Mcount n db b) ^ den ≤
                (nsum n (fun u => nsum n (fun v => c1 db u v))) ^ w2 := by
              exact Nat.le_trans (Nat.pow_le_pow_left (Mcount_le_s2 n db b) den)
                (Nat.pow_le_pow_right hS2pos hw2)
            have hpowS3 : (S3count n db b) ^ den ≤ (S3count n db b) ^ w3 :=
              Nat.pow_le_pow_right hS3pos hw3
            have hpow0a : 1 ≤ (nsum n (c0 db b)) ^ w0 :=
              Nat.one_le_pow w0 (nsum n (c0 db b)) hS0pos
            have hpow0b : 1 ≤ (nsum n (c0 db b)) ^ w1 :=
              Nat.one_le_pow w1 (nsum n (c0 db b)) hS0pos
            have hone : 1 ≤ (nsum n (c0 db b)) ^ w0 * (nsum n (c0 db b)) ^ w1 := by
              simpa using Nat.mul_le_mul hpow0a hpow0b
            calc
              (Mcount n db b) ^ den * (S3count n db b) ^ den ≤
                  (nsum n (fun u => nsum n (fun v => c1 db u v))) ^ w2 *
                    (S3count n db b) ^ w3 := Nat.mul_le_mul hpowM hpowS3
              _ ≤ ((nsum n (c0 db b)) ^ w0 * (nsum n (c0 db b)) ^ w1) *
                    ((nsum n (fun u => nsum n (fun v => c1 db u v))) ^ w2 *
                      (S3count n db b) ^ w3) := by
                    simpa using Nat.mul_le_mul hone (by omega)
              _ = (nsum n (c0 db b)) ^ w0 *
                    ((nsum n (c0 db b)) ^ w1 *
                      ((nsum n (fun u => nsum n (fun v => c1 db u v))) ^ w2 *
                        (S3count n db b) ^ w3)) := by rw [Nat.mul_assoc]
          | cons e l5 => simp [pattern] at hwlen

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.DoubleBilling.Remaining
