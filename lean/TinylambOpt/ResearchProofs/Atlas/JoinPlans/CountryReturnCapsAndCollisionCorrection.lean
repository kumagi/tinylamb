import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `CountryReturn.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `CountryReturn.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnCapsAndCollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn

/-! ## Generic finite-range and integration helpers (private copies). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n + 1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
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
        apply rsum_congr
        intro j hj
        rw [rsum_zero]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n + 1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_eq_zero_of_zero (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → f i = 0) :
    rsum n f = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]
      simp

private theorem le_rsum_of_mem (n i : Nat) (f : Nat → Int) (hi : i < n)
    (hf : ∀ j, j < n → 0 ≤ f j) : f i ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases hin : i < n
      · have hrec := ih hin (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
        omega
      · have : i = n := by omega
        subst i
        have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        omega

private theorem eq_zero_of_rsum_eq_zero (n : Nat) (f : Nat → Int)
    (hf : ∀ i, i < n → 0 ≤ f i) (hs : rsum n f = 0) : ∀ i, i < n → f i = 0 := by
  induction n with
  | zero => intro i hi; omega
  | succ n ih =>
      rw [rsum_succ] at hs
      have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun i hi => hf i (Nat.lt_succ_of_lt hi))
      have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
      have hpre0 : rsum n f = 0 := by omega
      have hn0 : f n = 0 := by omega
      intro i hi
      by_cases hin : i < n
      · exact ih (fun j hj => hf j (Nat.lt_succ_of_lt hj)) hpre0 i hin
      · have : i = n := by omega
        subst i
        exact hn0

private theorem sum_bool {α} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then (1 : Int) else 0)).sum = (l.countP p : Int) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]; omega
      · have h' : p a = false := by cases hpa : p a <;> simp_all
        simp [h', ih]

private theorem int_sum_nat (l : List Nat) : (l.map (fun x : Nat => (x : Int))).sum = (l.sum : Int) := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem int_countP_eq_rsum (n : Nat) (p : Nat → Bool) :
    ((List.range n).countP p : Int) = rsum n (fun x => if p x then (1 : Int) else 0) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      have hcast : ((List.countP p (List.range n) + List.countP p [n] : Nat) : Int) =
          (List.countP p (List.range n) : Int) + (List.countP p [n] : Int) := by omega
      rw [hcast, ih]
      rw [rsum_succ]
      cases h : p n <;> simp [h]

private theorem int_list_sum_countP (l : List Nat) (n : Nat) (f : Nat → Nat → Bool) :
    ((l.map (fun x => (List.range n).countP (f x))).sum : Int) =
      (l.map (fun x => rsum n (fun y => if f x y then (1 : Int) else 0))).sum := by
  induction l with
  | nil => simp
  | cons x l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hcast : ((List.countP (f x) (List.range n) +
          (List.map (fun x => List.countP (f x) (List.range n)) l).sum : Nat) : Int) =
          (List.countP (f x) (List.range n) : Int) +
          ((List.map (fun x => List.countP (f x) (List.range n)) l).sum : Int) := by omega
      rw [hcast, int_countP_eq_rsum, ih]

/-! ## CountryReturn indicator model. -/

private def indA (db : DB) (b : Bind) (x : Nat) : Int := if db 0 (b 0) x == 0 then 0 else 1
private def indB (db : DB) (x y : Nat) : Int := if db 1 x y == 0 then 0 else 1
private def indC (db : DB) (x y : Nat) : Int := if db 2 x y == 0 then 0 else 1
private def indD (db : DB) (x y : Nat) : Int := if db 1 x y == 0 then 0 else 1
private def indE (db : DB) (b : Bind) (x : Nat) : Int := if db 2 (b 0) x == 0 then 0 else 1

private theorem ne_zero_false_of_beq_zero {z : Int} (h : z == 0) : (z != 0) = false := by
  have hz : z = 0 := beq_iff_eq.mp h
  simp [hz]

private theorem ne_zero_true_of_not_beq_zero {z : Int} (h : ¬ z == 0) : (z != 0) = true := by
  cases h' : z != 0
  · have hz : z = 0 := by
      have : (z == 0) = true := by simpa using h'
      exact beq_iff_eq.mp this
    have hbeq : (z == 0) = true := by simp [hz]
    exact False.elim (h hbeq)
  · rfl

private theorem indA_spec (db : DB) (b : Bind) (x : Nat) :
    indA db b x = if db 0 (b 0) x != 0 then 1 else 0 := by
  by_cases h : db 0 (b 0) x == 0
  · simp [indA, h, ne_zero_false_of_beq_zero h]
  · simp [indA, h, ne_zero_true_of_not_beq_zero h]
private theorem indB_spec (db : DB) (x y : Nat) :
    indB db x y = if db 1 x y != 0 then 1 else 0 := by
  by_cases h : db 1 x y == 0
  · simp [indB, h, ne_zero_false_of_beq_zero h]
  · simp [indB, h, ne_zero_true_of_not_beq_zero h]
private theorem indC_spec (db : DB) (x y : Nat) :
    indC db x y = if db 2 x y != 0 then 1 else 0 := by
  by_cases h : db 2 x y == 0
  · simp [indC, h, ne_zero_false_of_beq_zero h]
  · simp [indC, h, ne_zero_true_of_not_beq_zero h]
private theorem indD_spec (db : DB) (x y : Nat) :
    indD db x y = if db 1 x y != 0 then 1 else 0 := by
  by_cases h : db 1 x y == 0
  · simp [indD, h, ne_zero_false_of_beq_zero h]
  · simp [indD, h, ne_zero_true_of_not_beq_zero h]
private theorem indE_spec (db : DB) (b : Bind) (x : Nat) :
    indE db b x = if db 2 (b 0) x != 0 then 1 else 0 := by
  by_cases h : db 2 (b 0) x == 0
  · simp [indE, h, ne_zero_false_of_beq_zero h]
  · simp [indE, h, ne_zero_true_of_not_beq_zero h]

private theorem indA_nonneg (db : DB) (b : Bind) (x : Nat) : 0 ≤ indA db b x := by
  rw [indA_spec]; split <;> omega
private theorem indB_nonneg (db : DB) (x y : Nat) : 0 ≤ indB db x y := by
  rw [indB_spec]; split <;> omega
private theorem indC_nonneg (db : DB) (x y : Nat) : 0 ≤ indC db x y := by
  rw [indC_spec]; split <;> omega
private theorem indD_nonneg (db : DB) (x y : Nat) : 0 ≤ indD db x y := by
  rw [indD_spec]; split <;> omega
private theorem indE_nonneg (db : DB) (b : Bind) (x : Nat) : 0 ≤ indE db b x := by
  rw [indE_spec]; split <;> omega
private theorem indA_le_one (db : DB) (b : Bind) (x : Nat) : indA db b x ≤ 1 := by
  rw [indA_spec]; split <;> omega
private theorem indB_le_one (db : DB) (x y : Nat) : indB db x y ≤ 1 := by
  rw [indB_spec]; split <;> omega
private theorem indC_le_one (db : DB) (x y : Nat) : indC db x y ≤ 1 := by
  rw [indC_spec]; split <;> omega
private theorem indD_le_one (db : DB) (x y : Nat) : indD db x y ≤ 1 := by
  rw [indD_spec]; split <;> omega
private theorem indE_le_one (db : DB) (b : Bind) (x : Nat) : indE db b x ≤ 1 := by
  rw [indE_spec]; split <;> omega

private def Rfull (n : Nat) (db : DB) (b : Bind) : Int :=
  rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))

private theorem ref_indicator (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b = Rfull n db b := by
  simp [reference, integrate, pattern, value, put, rsum, Rfull,
    indA, indB, indC, indD, indE]

private theorem Rfull_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ Rfull n db b := by
  dsimp [Rfull]
  apply rsum_nonneg; intro x1 hx1
  apply rsum_nonneg; intro x2 hx2
  apply rsum_nonneg; intro x3 hx3
  apply rsum_nonneg; intro x4 hx4
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

/-! ## Support counts and their rsum forms. -/

private def s0 (n : Nat) (db : DB) (b : Bind) : Nat := (List.range n).countP (fun x => db 0 (b 0) x != 0)
private def s1 (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))).sum
private def s2 (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 2 x y != 0))).sum
private def s3 (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))).sum
private def s4 (n : Nat) (db : DB) (b : Bind) : Nat := (List.range n).countP (fun x => db 2 (b 0) x != 0)

private theorem support0_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨0, 0, 1⟩ : Atom) pattern b = s0 n db b := by
  simp [support, pattern, s0]

private theorem support1_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1, 1, 2⟩ : Atom) pattern b = s1 n db := by
  simp [support, pattern, s1]

private theorem support2_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2, 2, 3⟩ : Atom) pattern b = s2 n db := by
  simp [support, pattern, s2]

private theorem support3_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1, 3, 4⟩ : Atom) pattern b = s3 n db := by
  simp [support, pattern, s3]

private theorem support4_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2, 0, 4⟩ : Atom) pattern b = s4 n db b := by
  simp [support, pattern, s4]

private theorem s1_eq_s3 (n : Nat) (db : DB) : s1 n db = s3 n db := by rfl

private theorem rsum_indA (n : Nat) (db : DB) (b : Bind) :
    rsum n (fun x => indA db b x) = (s0 n db b : Int) := by
  rw [show rsum n (fun x => indA db b x) =
      rsum n (fun x => if db 0 (b 0) x != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indA_spec db b i]
  unfold rsum s0
  exact sum_bool (List.range n) (fun x => db 0 (b 0) x != 0)

private theorem rsum_indB (n : Nat) (db : DB) (x : Nat) :
    rsum n (fun y => indB db x y) =
      ((List.range n).countP (fun y => db 1 x y != 0) : Int) := by
  rw [show rsum n (fun y => indB db x y) =
      rsum n (fun y => if db 1 x y != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indB_spec db x i]
  unfold rsum
  exact sum_bool (List.range n) (fun y => db 1 x y != 0)

private theorem rsum_indC (n : Nat) (db : DB) (x : Nat) :
    rsum n (fun y => indC db x y) =
      ((List.range n).countP (fun y => db 2 x y != 0) : Int) := by
  rw [show rsum n (fun y => indC db x y) =
      rsum n (fun y => if db 2 x y != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indC_spec db x i]
  unfold rsum
  exact sum_bool (List.range n) (fun y => db 2 x y != 0)

private theorem rsum_indD (n : Nat) (db : DB) (x : Nat) :
    rsum n (fun y => indD db x y) =
      ((List.range n).countP (fun y => db 1 x y != 0) : Int) := by
  rw [show rsum n (fun y => indD db x y) =
      rsum n (fun y => if db 1 x y != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indD_spec db x i]
  unfold rsum
  exact sum_bool (List.range n) (fun y => db 1 x y != 0)

private theorem rsum_indE (n : Nat) (db : DB) (b : Bind) :
    rsum n (fun x => indE db b x) = (s4 n db b : Int) := by
  rw [show rsum n (fun x => indE db b x) =
      rsum n (fun x => if db 2 (b 0) x != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indE_spec db b i]
  unfold rsum s4
  exact sum_bool (List.range n) (fun x => db 2 (b 0) x != 0)

private theorem rsum_indB2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indB db x y)) = (s1 n db : Int) := by
  simp only [rsum_indB]
  unfold rsum s1
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))))

private theorem rsum_indC2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indC db x y)) = (s2 n db : Int) := by
  simp only [rsum_indC]
  unfold rsum s2
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 2 x y != 0))))

private theorem rsum_indD2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indD db x y)) = (s3 n db : Int) := by
  simp only [rsum_indD]
  unfold rsum s3
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))))

private theorem rowB_le_s3 (n : Nat) (db : DB) (x : Nat) (hx : x < n) :
    rsum n (fun y => indB db x y) ≤ (s3 n db : Int) := by
  rw [← rsum_indD2 n db]
  rw [show (fun y => indB db x y) = (fun y => indD db x y) by
    funext y
    rw [indB_spec, indD_spec]]
  exact le_rsum_of_mem n x (fun i => rsum n (fun y => indD db i y)) hx
    (fun i hi => rsum_nonneg n (fun y => indD db i y) (fun y hy => indD_nonneg db i y))

private theorem rowC_le_s2 (n : Nat) (db : DB) (x : Nat) (hx : x < n) :
    rsum n (fun y => indC db x y) ≤ (s2 n db : Int) := by
  rw [← rsum_indC2 n db]
  exact le_rsum_of_mem n x (fun i => rsum n (fun y => indC db i y)) hx
    (fun i hi => rsum_nonneg n (fun y => indC db i y) (fun y hy => indC_nonneg db i y))

/-! ## The four integral-cover bounds. -/

private theorem quad_product1 (n : Nat) (f : Nat → Int) (g : Nat → Nat → Int) (h : Nat → Int) :
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      f x1 * g x2 x3 * h x4)))) =
      (rsum n f) * (rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) * (rsum n h) := by
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      f x1 * g x2 x3 * h x4))))
        = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            (f x1 * g x2 x3) * rsum n h))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              apply rsum_congr; intro x3 hx3
              rw [rsum_const_mul]
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            (f x1 * rsum n h) * rsum n (fun x3 => g x2 x3))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              rw [show (fun x3 => (f x1 * g x2 x3) * rsum n h) =
                  (fun x3 => (f x1 * rsum n h) * g x2 x3) by
                funext x3; grind]
              rw [rsum_const_mul]
    _ = rsum n (fun x1 =>
            (f x1 * rsum n h) * rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) := by
              apply rsum_congr; intro x1 hx1
              rw [rsum_const_mul]
    _ = (rsum n f) * (rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) * (rsum n h) := by
              rw [show (fun x1 => (f x1 * rsum n h) *
                    rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) =
                  (fun x1 => ((rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) *
                    rsum n h) * f x1) by
                funext x1; grind]
              rw [rsum_const_mul]
              grind

private theorem quad_product2 (n : Nat) (g : Nat → Nat → Int) (h : Nat → Nat → Int) :
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      g x1 x2 * h x3 x4)))) =
      (rsum n (fun x1 => rsum n (fun x2 => g x1 x2))) *
        (rsum n (fun x3 => rsum n (fun x4 => h x3 x4))) := by
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      g x1 x2 * h x3 x4))))
        = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            g x1 x2 * rsum n (fun x4 => h x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              apply rsum_congr; intro x3 hx3
              rw [rsum_const_mul]
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            g x1 x2 * rsum n (fun x3 => rsum n (fun x4 => h x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              rw [rsum_const_mul]
    _ = rsum n (fun x1 =>
          (rsum n (fun x2 => g x1 x2)) * (rsum n (fun x3 => rsum n (fun x4 => h x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              rw [rsum_mul_const]
    _ = (rsum n (fun x1 => rsum n (fun x2 => g x1 x2))) *
          (rsum n (fun x3 => rsum n (fun x4 => h x3 x4))) := by
              rw [rsum_mul_const]

private theorem term_le_ACE (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indA db b x1 * indC db x2 x3 * indE db b x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem term_le_BD (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indB db x1 x2 * indD db x3 x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem term_le_ACD (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indA db b x1 * indC db x2 x3 * indD db x3 x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem term_le_BCE (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indB db x1 x2 * indC db x2 x3 * indE db b x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem full_le_s0s2s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s0 n db b : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indA db b x1 * indC db x2 x3 * indE db b x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_ACE db b x1 x2 x3 x4
    _ = (s0 n db b : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
              rw [quad_product1 n (fun x => indA db b x) (fun x y => indC db x y)
                (fun x => indE db b x)]
              rw [rsum_indA, rsum_indC2, rsum_indE]

private theorem full_le_s1s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s1 n db : Int) * (s3 n db : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indB db x1 x2 * indD db x3 x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_BD db b x1 x2 x3 x4
    _ = (s1 n db : Int) * (s3 n db : Int) := by
              rw [quad_product2 n (fun x y => indB db x y) (fun x y => indD db x y)]
              rw [rsum_indB2, rsum_indD2]

private theorem pull_const_triple (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => rsum n (fun z => c * f x y z))) =
      c * rsum n (fun x => rsum n (fun y => rsum n (fun z => f x y z))) := by
  calc
    rsum n (fun x => rsum n (fun y => rsum n (fun z => c * f x y z)))
        = rsum n (fun x => rsum n (fun y => c * rsum n (fun z => f x y z))) := by
            apply rsum_congr; intro x hx
            apply rsum_congr; intro y hy
            rw [rsum_const_mul]
    _ = rsum n (fun x => c * rsum n (fun y => rsum n (fun z => f x y z))) := by
            apply rsum_congr; intro x hx
            rw [rsum_const_mul]
    _ = c * rsum n (fun x => rsum n (fun y => rsum n (fun z => f x y z))) := by
            rw [rsum_const_mul]

private theorem cd_triple_le (n : Nat) (db : DB) :
    rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indC db x2 x3 * indD db x3 x4))) ≤ (s2 n db : Int) * (s3 n db : Int) := by
  calc
    rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indC db x2 x3 * indD db x3 x4)))
        = rsum n (fun x2 => rsum n (fun x3 =>
            indC db x2 x3 * rsum n (fun x4 => indD db x3 x4))) := by
              apply rsum_congr; intro x2 hx2
              apply rsum_congr; intro x3 hx3
              rw [rsum_const_mul]
    _ ≤ rsum n (fun x2 => rsum n (fun x3 =>
            indC db x2 x3 * (s3 n db : Int))) := by
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              have hC : 0 ≤ indC db x2 x3 := indC_nonneg db x2 x3
              exact Int.mul_le_mul_of_nonneg_left (rowB_le_s3 n db x3 hx3) hC
    _ = rsum n (fun x2 => (rsum n (fun x3 => indC db x2 x3)) * (s3 n db : Int)) := by
              apply rsum_congr; intro x2 hx2
              rw [rsum_mul_const]
    _ = (rsum n (fun x2 => rsum n (fun x3 => indC db x2 x3))) * (s3 n db : Int) := by
              rw [rsum_mul_const]
    _ = (s2 n db : Int) * (s3 n db : Int) := by
              rw [rsum_indC2]

private theorem full_le_s0s2s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s0 n db b : Int) * (s2 n db : Int) * (s3 n db : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indA db b x1 * indC db x2 x3 * indD db x3 x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_ACD db b x1 x2 x3 x4
    _ = rsum n (fun x1 => indA db b x1 *
          rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indC db x2 x3 * indD db x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              rw [show (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    indA db b x1 * indC db x2 x3 * indD db x3 x4))) =
                  (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    indA db b x1 * (indC db x2 x3 * indD db x3 x4)))) by
                funext x2
                apply congrArg List.sum
                apply List.map_congr_left
                intro x3 hx3
                apply congrArg List.sum
                apply List.map_congr_left
                intro x4 hx4
                grind]
              rw [show (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    indA db b x1 * (indC db x2 x3 * indD db x3 x4)))) =
                  (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    (indA db b x1) * (indC db x2 x3 * indD db x3 x4)))) by rfl]
              rw [pull_const_triple]
    _ ≤ rsum n (fun x1 => indA db b x1 * ((s2 n db : Int) * (s3 n db : Int))) := by
              apply rsum_le; intro x1 hx1
              have hA : 0 ≤ indA db b x1 := indA_nonneg db b x1
              exact Int.mul_le_mul_of_nonneg_left (cd_triple_le n db) hA
    _ = (s0 n db b : Int) * (s2 n db : Int) * (s3 n db : Int) := by
              rw [rsum_mul_const]
              rw [rsum_indA]
              grind

private theorem full_le_s1s2s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s1 n db : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indB db x1 x2 * indC db x2 x3 * indE db b x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_BCE db b x1 x2 x3 x4
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            (indB db x1 x2 * rsum n (fun x3 => indC db x2 x3)) *
              (s4 n db b : Int))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              rw [show (fun x3 => rsum n (fun x4 =>
                    indB db x1 x2 * indC db x2 x3 * indE db b x4)) =
                  (fun x3 => (indB db x1 x2 * indC db x2 x3) * rsum n (fun x4 => indE db b x4)) by
                funext x3
                rw [show (fun x4 => indB db x1 x2 * indC db x2 x3 * indE db b x4) =
                    (fun x4 => (indB db x1 x2 * indC db x2 x3) * indE db b x4) by
                  funext x4; grind]
                rw [rsum_const_mul]]
              rw [show (fun x3 => (indB db x1 x2 * indC db x2 x3) * rsum n (fun x4 => indE db b x4)) =
                  (fun x3 => (rsum n (fun x4 => indE db b x4)) * (indB db x1 x2 * indC db x2 x3)) by
                funext x3; grind]
              rw [rsum_const_mul]
              rw [rsum_const_mul]
              rw [rsum_indE]
              grind
    _ ≤ rsum n (fun x1 => rsum n (fun x2 =>
            (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              have hB : 0 ≤ indB db x1 x2 := indB_nonneg db x1 x2
              have hs4 : 0 ≤ (s4 n db b : Int) := by exact_mod_cast (Nat.zero_le (s4 n db b))
              have hrow : rsum n (fun x3 => indC db x2 x3) ≤ (s2 n db : Int) := rowC_le_s2 n db x2 hx2
              have hm : indB db x1 x2 * rsum n (fun x3 => indC db x2 x3) ≤
                  indB db x1 x2 * (s2 n db : Int) :=
                Int.mul_le_mul_of_nonneg_left hrow hB
              exact Int.mul_le_mul_of_nonneg_right hm hs4
    _ = (s1 n db : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
              have hinner : ∀ x1, rsum n (fun x2 =>
                    (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int)) =
                  (rsum n (fun x2 => indB db x1 x2)) * ((s2 n db : Int) * (s4 n db b : Int)) := by
                intro x1
                rw [show (fun x2 => (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int)) =
                    (fun x2 => indB db x1 x2 * ((s2 n db : Int) * (s4 n db b : Int))) by
                  funext x2; grind]
                rw [rsum_mul_const]
              rw [show (fun x1 => rsum n (fun x2 =>
                    (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int))) =
                  (fun x1 => (rsum n (fun x2 => indB db x1 x2)) *
                    ((s2 n db : Int) * (s4 n db b : Int))) by
                funext x1; exact hinner x1]
              rw [rsum_mul_const]
              rw [rsum_indB2]
              grind



/-! ## FractionalCap: four integral covers and the fractional decomposition. -/

private theorem rhs_prod (n : Nat) (db : DB) (b : Bind)
    (w0 w1 w2 w3 w4 : Nat) :
    ((pattern.atoms.zip [w0, w1, w2, w3, w4]).map
      (fun (a, w) => support n db a pattern b ^ w)).prod =
      support n db (⟨0, 0, 1⟩ : Atom) pattern b ^ w0 *
        (support n db (⟨1, 1, 2⟩ : Atom) pattern b ^ w1 *
          (support n db (⟨2, 2, 3⟩ : Atom) pattern b ^ w2 *
            (support n db (⟨1, 3, 4⟩ : Atom) pattern b ^ w3 *
              support n db (⟨2, 0, 4⟩ : Atom) pattern b ^ w4))) := by
  simp [pattern]

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.FractionalCap.Goal := by
  intro n db b weights den hcover
  rcases hcover with ⟨hden, hwlen, hcov⟩
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
          | nil => simp [pattern] at hwlen
          | cons w4 rest =>
            cases rest with
            | nil =>
              rw [rhs_prod n db b w0 w1 w2 w3 w4]
              rw [support0_eq, support1_eq, support2_eq, support3_eq, support4_eq]
              simp [pattern] at hcov
              rcases hcov with ⟨h01, h12, h23, h34⟩
              let N : Nat := (reference pattern n
                (fun r x y => if db r x y == 0 then 0 else 1) b).toNat
              have hNint : (N : Int) = reference pattern n
                  (fun r x y => if db r x y == 0 then 0 else 1) b := by
                dsimp [N]
                rw [ref_indicator]
                exact Int.toNat_of_nonneg (Rfull_nonneg n db b)
              have h025 : N ≤ s0 n db b * s2 n db * s4 n db b := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s0s2s4 n db b
              have h13 : N ≤ s1 n db * s3 n db := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s1s3 n db b
              have h023 : N ≤ s0 n db b * s2 n db * s3 n db := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s0s2s3 n db b
              have h124 : N ≤ s1 n db * s2 n db * s4 n db b := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s1s2s4 n db b
              change N ^ den ≤
                s0 n db b ^ w0 * (s1 n db ^ w1 *
                  (s2 n db ^ w2 * (s3 n db ^ w3 * s4 n db b ^ w4)))
              by_cases hz : s0 n db b = 0 ∨ s1 n db = 0 ∨ s2 n db = 0 ∨
                  s3 n db = 0 ∨ s4 n db b = 0
              · have hN0 : N = 0 := by
                  rcases hz with h | h | h | h | h
                  · have : N ≤ 0 := by simpa [h] using h025
                    omega
                  · have : N ≤ 0 := by simpa [h] using h13
                    omega
                  · have : N ≤ 0 := by simpa [h] using h023
                    omega
                  · have : N ≤ 0 := by simpa [h] using h13
                    omega
                  · have : N ≤ 0 := by simpa [h] using h025
                    omega
                simp [hN0, hden]
              · have hs0 : 1 ≤ s0 n db b := by
                  have hne : s0 n db b ≠ 0 := by intro h; exact hz (Or.inl h)
                  omega
                have hs1 : 1 ≤ s1 n db := by
                  have hne : s1 n db ≠ 0 := by intro h; exact hz (Or.inr (Or.inl h))
                  omega
                have hs2 : 1 ≤ s2 n db := by
                  have hne : s2 n db ≠ 0 := by intro h; exact hz (Or.inr (Or.inr (Or.inl h)))
                  omega
                have hs3 : 1 ≤ s3 n db := by
                  have hne : s3 n db ≠ 0 := by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inl h))))
                  omega
                have hs4 : 1 ≤ s4 n db b := by
                  have hne : s4 n db b ≠ 0 := by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr h))))
                  omega
                let u := min w2 (min w4 den)
                let a := min u w0
                let d := den - w1 - a
                let c := u - a
                let bb := den - u - d
                have hu2 : u ≤ w2 := by dsimp [u]; exact Nat.min_le_left _ _
                have hu4 : u ≤ w4 := by
                  dsimp [u]
                  exact Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_left _ _)
                have hud : u ≤ den := by
                  dsimp [u]
                  exact Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_right _ _)
                have hau : a ≤ u := by dsimp [a]; exact Nat.min_le_left _ _
                have haw0 : a ≤ w0 := by dsimp [a]; exact Nat.min_le_right _ _
                have ha_d_le_w0 : a + d ≤ w0 := by
                  dsimp [d]
                  omega
                have hbc_le_w1 : bb + c ≤ w1 := by
                  dsimp [bb, c, d]
                  omega
                have hacd_le_w2 : a + c + d ≤ w2 := by
                  dsimp [a, c, d]
                  by_cases huw0 : u ≤ w0
                  · rw [Nat.min_eq_left huw0]
                    omega
                  · have hwu : w0 ≤ u := by omega
                    rw [Nat.min_eq_right hwu]
                    have hd0 : den - w1 - w0 = 0 := by omega
                    rw [hd0]
                    omega
                have hd_le_denu : d ≤ den - u := by
                  dsimp [d]
                  by_cases huw0 : u ≤ w0
                  · have ha_eq : a = u := by dsimp [a]; exact Nat.min_eq_left huw0
                    rw [ha_eq]
                    omega
                  · have hwu : w0 ≤ u := by omega
                    have ha_eq : a = w0 := by dsimp [a]; exact Nat.min_eq_right hwu
                    rw [ha_eq]
                    have hd0 : den - w1 - w0 = 0 := by omega
                    rw [hd0]
                    omega
                have hbd_eq : bb + d = den - u := by
                  dsimp [bb]
                  exact Nat.sub_add_cancel hd_le_denu
                have hdenu_le_w3 : den - u ≤ w3 := by
                  dsimp [u]
                  by_cases h2 : w2 ≤ min w4 den
                  · rw [Nat.min_eq_left h2]
                    omega
                  · have hminle : min w4 den ≤ w2 := by omega
                    rw [Nat.min_eq_right hminle]
                    by_cases h4 : w4 ≤ den
                    · rw [Nat.min_eq_left h4]
                      omega
                    · have hdle : den ≤ w4 := by omega
                      rw [Nat.min_eq_right hdle]
                      simp
                have hbd_le_w3 : bb + d ≤ w3 := by rw [hbd_eq]; exact hdenu_le_w3
                have hac_eq : a + c = u := by
                  dsimp [c]
                  omega
                have hac_le_w4 : a + c ≤ w4 := by rw [hac_eq]; exact hu4
                have hsum : a + bb + c + d = den := by
                  rw [show a + bb + c + d = (a + c) + (bb + d) by omega]
                  rw [hac_eq, hbd_eq]
                  omega
                have hpow : N ^ den = N ^ a * N ^ bb * N ^ c * N ^ d := by
                  rw [show den = a + bb + c + d by omega]
                  rw [show a + bb + c + d = (a + bb + c) + d by omega]
                  rw [Nat.pow_add]
                  rw [show a + bb + c = (a + bb) + c by omega]
                  rw [Nat.pow_add, Nat.pow_add]
                have hm0 : s0 n db b ^ (a + d) ≤ s0 n db b ^ w0 :=
                  Nat.pow_le_pow_right hs0 ha_d_le_w0
                have hm1 : s1 n db ^ (bb + c) ≤ s1 n db ^ w1 :=
                  Nat.pow_le_pow_right hs1 hbc_le_w1
                have hm2 : s2 n db ^ (a + c + d) ≤ s2 n db ^ w2 :=
                  Nat.pow_le_pow_right hs2 hacd_le_w2
                have hm3 : s3 n db ^ (bb + d) ≤ s3 n db ^ w3 :=
                  Nat.pow_le_pow_right hs3 hbd_le_w3
                have hm4 : s4 n db b ^ (a + c) ≤ s4 n db b ^ w4 :=
                  Nat.pow_le_pow_right hs4 hac_le_w4
                calc
                  N ^ den = N ^ a * N ^ bb * N ^ c * N ^ d := hpow
                  _ ≤ (((s0 n db b * s2 n db * s4 n db b) ^ a *
                        (s1 n db * s3 n db) ^ bb) *
                        (s1 n db * s2 n db * s4 n db b) ^ c) *
                      (s0 n db b * s2 n db * s3 n db) ^ d := by
                        apply Nat.mul_le_mul
                        · apply Nat.mul_le_mul
                          · apply Nat.mul_le_mul
                            · exact Nat.pow_le_pow_left h025 a
                            · exact Nat.pow_le_pow_left h13 bb
                          · exact Nat.pow_le_pow_left h124 c
                        · exact Nat.pow_le_pow_left h023 d
                  _ = s0 n db b ^ (a + d) * s1 n db ^ (bb + c) *
                        s2 n db ^ (a + c + d) * s3 n db ^ (bb + d) *
                        s4 n db b ^ (a + c) := by
                        simp only [Nat.mul_pow]
                        rw [show ((((s0 n db b ^ a * s2 n db ^ a) * s4 n db b ^ a) *
                              (s1 n db ^ bb * s3 n db ^ bb)) *
                                ((s1 n db ^ c * s2 n db ^ c) * s4 n db b ^ c)) *
                              ((s0 n db b ^ d * s2 n db ^ d) * s3 n db ^ d) =
                            (((s0 n db b ^ a * s0 n db b ^ d) *
                              (s1 n db ^ bb * s1 n db ^ c)) *
                              (((s2 n db ^ a * s2 n db ^ d) * s2 n db ^ c) *
                                ((s3 n db ^ bb * s3 n db ^ d) *
                                  (s4 n db b ^ a * s4 n db b ^ c)))) by ac_rfl]
                        rw [← Nat.pow_add (s0 n db b) a d]
                        rw [← Nat.pow_add (s1 n db) bb c]
                        rw [← Nat.pow_add (s2 n db) a d]
                        rw [← Nat.pow_add (s2 n db) (a + d) c]
                        rw [← Nat.pow_add (s3 n db) bb d]
                        rw [← Nat.pow_add (s4 n db b) a c]
                        rw [show a + d + c = a + c + d by omega]
                        ac_rfl
                  _ ≤ s0 n db b ^ w0 * s1 n db ^ w1 * s2 n db ^ w2 *
                        s3 n db ^ w3 * s4 n db b ^ w4 := by
                        apply Nat.mul_le_mul
                        · apply Nat.mul_le_mul
                          · apply Nat.mul_le_mul
                            · apply Nat.mul_le_mul
                              · exact hm0
                              · exact hm1
                            · exact hm2
                          · exact hm3
                        · exact hm4
                  _ = s0 n db b ^ w0 * (s1 n db ^ w1 *
                        (s2 n db ^ w2 * (s3 n db ^ w3 * s4 n db b ^ w4))) := by ac_rfl
            | cons _ _ => simp [pattern] at hwlen



/-! ## CollisionCorrection: inclusion-exclusion over equality partitions. -/

private theorem list_sum_congr2 {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem list_sum_eq_zero {α} (l : List α) (f : α → Int) (h : ∀ x ∈ l, f x = 0) :
    (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]
      simp

private theorem sum_mul_left2 {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_mul_right2 {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem list_sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

private theorem sum_swap2 {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum =
      (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l' with
  | nil =>
      change (l.map (fun _ => (0 : Int))).sum = 0
      exact list_sum_eq_zero l (fun _ => 0) (fun x hx => rfl)
  | cons b l' ih =>
      rw [show (fun a => (List.map (F a) (b :: l')).sum) =
          (fun a => F a b + (l'.map (F a)).sum) by funext a; rfl]
      rw [list_sum_add, ih]
      simp

private def headOf : List Nat → Nat
  | h :: _ => h
  | [] => 0

private def Heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private def Root (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun bl => bl.contains v)).bind List.head?).getD v

private def constOn (bs : List (List Nat)) (x : Bind) : Bool :=
  bs.all (fun bl => bl.all (fun v => x v == x (headOf bl)))

private def PSb (V : List Nat) (bs : List (List Nat)) : Bool :=
  decide bs.Nodup &&
  bs.all (fun bl => decide bl.Nodup && bl.all (fun v => V.contains v)) &&
  bs.all (fun bl1 => bs.all (fun bl2 =>
    bl1.all (fun v => if bl2.contains v then bl1 == bl2 else true))) &&
  V.all (fun v => bs.any (fun bl => bl.contains v))

private theorem ps_all : (partitions pattern.freeVars).all (fun bs => PSb pattern.freeVars bs) = true := by
  native_decide

private theorem ps_of_mem {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    PSb pattern.freeVars bs = true :=
  List.all_eq_true.mp ps_all bs hbs

private theorem find_eq_some_unique {α} (p : α → Bool) (l : List α) (a : α)
    (ha : a ∈ l) (hp : p a = true) (huniq : ∀ b ∈ l, p b = true → b = a) :
    l.find? p = some a := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x
        simp [hp]
      · have hax : a ≠ x := by intro h; exact hxa h.symm
        have haxs : a ∈ xs := by simpa [hax] using ha
        have hpx : p x = false := by
          cases hpx' : p x with
          | true =>
              have hxeq : x = a := huniq x (by simp) hpx'
              exact False.elim (hxa hxeq)
          | false => rfl
        simp [hpx, ih haxs (fun b hb hpb => huniq b (by simp [hb]) hpb)]

private theorem root_eq_head (bs : List (List Nat)) (hPS : PSb pattern.freeVars bs = true) :
    ∀ bl ∈ bs, ∀ v ∈ bl, Root bs v = headOf bl := by
  simp [PSb] at hPS
  rcases hPS with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro bl hbl v hv
  have hpbl : bl.contains v = true := by simpa using hv
  have huniq : ∀ b ∈ bs, b.contains v = true → b = bl := by
    intro b hb hbv
    have hdeq := hdisj bl hbl b hb v hv
    have hbmem : v ∈ b := by simpa using hbv
    rcases hdeq with hnot | heq
    · exact False.elim (hnot hbmem)
    · exact heq.symm
  have hfind : bs.find? (fun b : List Nat => b.contains v) = some bl :=
    find_eq_some_unique (fun b : List Nat => b.contains v) bs bl hbl hpbl huniq
  unfold Root
  rw [hfind]
  cases bl with
  | nil => simp at hv
  | cons h tl => rfl

private theorem head_mem_heads_cons (bl : List Nat) (bs : List (List Nat))
    (hs : bl.head?.isSome = true) : headOf bl ∈ Heads (bl :: bs) := by
  cases bl with
  | nil => simp at hs
  | cons h tl => simp [Heads, headOf]

private theorem head_mem_heads {bl : List Nat} {bs : List (List Nat)}
    (hbl : bl ∈ bs) : bl.head?.isSome = true → headOf bl ∈ Heads bs := by
  induction bs with
  | nil => simp at hbl
  | cons b bs ih =>
      intro hs
      rcases List.mem_cons.mp hbl with hbeq | hbl
      · subst bl
        exact head_mem_heads_cons b bs hs
      · have htail := ih hbl hs
        cases hb : b.head? with
        | none => simpa [Heads, hb] using htail
        | some h =>
            simp [Heads, hb]
            apply Or.inr
            exact List.mem_filterMap.mp htail

private def HPb (bs : List (List Nat)) : Bool :=
  decide (Heads bs).Nodup && (Heads bs).all (fun h => pattern.freeVars.contains h)

private theorem hp_all : (partitions pattern.freeVars).all HPb = true := by
  native_decide

private theorem heads_spec {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    (Heads bs).Nodup ∧ (∀ h ∈ Heads bs, h ∈ pattern.freeVars) := by
  have h := List.all_eq_true.mp hp_all bs hbs
  simp [HPb] at h
  exact h

private theorem find_none_of_all_false {α} (p : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = false) : l.find? p = none := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.find?, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem root_out {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v, v ∉ pattern.freeVars → Root bs v = v := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  have hfalse : ∀ bl ∈ bs, bl.contains v = false := by
    intro bl hbl
    have hbsub := hsub bl hbl
    by_cases hne : bl.contains v = true
    · have hvbl : v ∈ bl := by simpa using hne
      exact False.elim (hv (hbsub.2 v hvbl))
    · cases hne' : bl.contains v <;> simp_all
  unfold Root
  rw [find_none_of_all_false (fun bl : List Nat => bl.contains v) bs hfalse]
  simp

private theorem root_head {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ h ∈ Heads bs, Root bs h = h := by
  intro h hh
  rcases List.mem_filterMap.mp hh with ⟨bl, hbl, hhbl⟩
  have hhroot : headOf bl = h := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simpa [headOf] using hhbl
  have hvbl : headOf bl ∈ bl := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simp [headOf]
  rw [← hhroot]
  exact root_eq_head bs hPS bl hbl (headOf bl) hvbl

private theorem root_mem_heads {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v ∈ pattern.freeVars, Root bs v ∈ Heads bs := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  rcases hcov v hv with ⟨bl, hbl, hvbl⟩
  rw [root_eq_head bs hPS bl hbl v hvbl]
  exact head_mem_heads hbl (by cases bl with | nil => simp at hvbl | cons h t => simp)

private theorem constOn_iff {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) (x : Bind) :
    constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  constructor
  · intro hc v hv
    rcases hcov v hv with ⟨bl, hbl, hvbl⟩
    rw [root_eq_head bs hPS bl hbl v hvbl]
    have hbeq := List.all_eq_true.mp hc bl hbl
    have hveq := List.all_eq_true.mp hbeq v hvbl
    simpa using hveq
  · intro hx
    apply List.all_eq_true.mpr
    intro bl hbl
    apply List.all_eq_true.mpr
    intro v hvbl
    have hvV : v ∈ pattern.freeVars := (hsub bl hbl).2 v hvbl
    have hrv := hx v hvV
    rw [root_eq_head bs hPS bl hbl v hvbl] at hrv
    simpa using hrv

private theorem put_self2 (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_same2 (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

private theorem put_comm_ne2 (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i == v
  · by_cases hw : i == w
    · have hiv : i = v := Nat.eq_of_beq_eq_true hv
      have hiw : i = w := Nat.eq_of_beq_eq_true hw
      have hvw : v = w := by omega
      simp [hvw] at h
    · simp [put, hv, hw]
  · by_cases hw : i == w <;> simp [put, hv, hw]

private theorem integrate_swap2 (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by cases h' : (v == w) <;> simp_all
    change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    change rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f))
    rw [rsum_comm]
    apply list_sum_congr2
    intro x hx
    apply list_sum_congr2
    intro y hy
    rw [put_comm_ne2 b v w y x hne]

private theorem integrate_append2 (n : Nat) (xs ys : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (xs ++ ys) b f = integrate n xs b (fun b' => integrate n ys b' f) := by
  induction xs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n (vs ++ ys) (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun b' => integrate n ys b' f))).sum
      apply list_sum_congr2
      intro x hx
      exact ih (put b v x)

private theorem integrate_perm2 (n : Nat) {xs ys : List Nat} (h : xs.Perm ys)
    (b : Bind) (f : Bind → Int) : integrate n xs b f = integrate n ys b f := by
  induction h generalizing b with
  | nil => rfl
  | cons x h ih =>
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      apply list_sum_congr2
      intro t ht
      exact ih (put b x t)
  | swap x y l => exact (integrate_swap2 n x y l b f).symm
  | trans h1 h2 ih1 ih2 => exact (ih1 b).trans (ih2 b)

private def bindings (n : Nat) : List Nat → Bind → List Bind
  | [], b => [b]
  | v :: vs, b => (List.range n).flatMap (fun x => bindings n vs (put b v x))

private theorem bindings_sum (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = ((bindings n vs b).map f).sum := by
  induction vs generalizing b with
  | nil => simp [integrate, bindings]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        (((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f).sum
      rw [show ((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f =
          (List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f) by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.map_append, ih2]]
      rw [show ((List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f)).sum =
          ((List.range n).map (fun x => ((bindings n vs (put b v x)).map f).sum)).sum by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.sum_append, ih2]]
      apply list_sum_congr2
      intro x hx
      exact ih (put b v x)

private theorem bindings_fixed (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∉ vs, y v = b v := by
  induction vs generalizing b with
  | nil =>
      intro y hy v hv
      simp [bindings] at hy
      subst y
      rfl
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      have hvw : v ≠ w := by intro h; subst v; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      rw [ih (put b w x) y hy v hvws]
      simp [put, hvw]

private theorem bindings_range (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∈ vs, y v ∈ List.range n := by
  induction vs generalizing b with
  | nil => simp
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      by_cases hvw : w = v
      · subst w
        by_cases htail : v ∈ ws
        · exact ih (put b v x) y hy v htail
        · have heq := bindings_fixed n ws (put b v x) y hy v htail
          rw [heq]
          simpa [put] using hx
      · have htail : v ∈ ws := by
          rcases List.mem_cons.mp hv with h | h
          · exact (hvw h.symm).elim
          · exact h
        exact ih (put b w x) y hy v htail

private theorem integrate_congr_bindings (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ y, y ∈ bindings n vs b → f y = g y) :
    integrate n vs b f = integrate n vs b g := by
  rw [bindings_sum, bindings_sum]
  apply list_sum_congr2
  intro y hy
  exact h y hy

private theorem rsum_ite_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : a = n
      · subst a
        have hzero : rsum n (fun x => if x = n then f x else 0) = 0 := by
          rw [show rsum n (fun x => if x = n then f x else 0) = rsum n (fun _ => (0 : Int)) by
            apply rsum_congr
            intro x hx
            have hxn : x ≠ n := by omega
            simp [hxn]]
          exact rsum_zero_const n
        rw [hzero]
        simp
      · have han : a < n := by omega
        rw [ih han]
        have hne : ¬ n = a := by omega
        simp [hne]

private theorem integrate_zero_of_false (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (h : ∀ y ∈ bindings n vs b, f y = 0) : integrate n vs b f = 0 := by
  rw [bindings_sum]
  apply list_sum_eq_zero
  intro y hy
  exact h y hy

private theorem inner_collapse (n : Nat) (M : List Nat) (Root : Nat → Nat) (y : Bind) (C : Int)
    (hM : M.Nodup) (hRnot : ∀ m ∈ M, Root m ∉ M)
    (hy : ∀ m ∈ M, y (Root m) ∈ List.range n) :
    integrate n M y (fun x => if M.all (fun m => x m == y (Root m)) then C else 0) = C := by
  induction M generalizing y with
  | nil => simp [integrate]
  | cons m Ms ih =>
      have hmMs : m ∉ Ms := (List.nodup_cons.mp hM).1
      have hMnd : Ms.Nodup := (List.nodup_cons.mp hM).2
      have hRnot' : ∀ v ∈ Ms, Root v ∉ Ms := by
        intro v hv hmem
        exact hRnot v (by simp [hv]) (by simp [hmem])
      have hrootne : ∀ v ∈ Ms, Root v ≠ m := by
        intro v hv h_eq
        exact hRnot v (by simp [hv]) (by simp [h_eq])
      have hpoint : ∀ t < n,
          integrate n Ms (put y m t)
            (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) =
          if t = y (Root m) then C else 0 := by
        intro t ht
        by_cases htm : t = y (Root m)
        · subst t
          have hyrange' : ∀ v ∈ Ms, (put y m (y (Root m))) (Root v) ∈ List.range n := by
            intro v hv
            have hne := hrootne v hv
            simpa [put, hne] using hy v (by simp [hv])
          calc
            integrate n Ms (put y m (y (Root m)))
                (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)
                = integrate n Ms (put y m (y (Root m)))
                    (fun x => if Ms.all (fun v => x v == (put y m (y (Root m))) (Root v)) then C else 0) := by
                    apply integrate_congr_bindings
                    intro x hx
                    have hxm : x m = y (Root m) := by
                      rw [bindings_fixed n Ms (put y m (y (Root m))) x hx m hmMs]
                      simp [put]
                    simp [hxm]
                    by_cases hleft : ∀ v ∈ Ms, x v = y (Root v)
                    · have hright : ∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v) := by
                        intro v hv
                        have hv' := hleft v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_left hleft, ite_eq_left hright]
                    · have hrightnot : ¬ (∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v)) := by
                        intro hr
                        apply hleft
                        intro v hv
                        have hv' := hr v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_right hleft, ite_eq_right hrightnot]
            _ = C := ih (put y m (y (Root m))) hMnd hRnot' hyrange'
            _ = (if y (Root m) = y (Root m) then C else 0) := by simp
        · have hzero : integrate n Ms (put y m t)
              (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) = 0 := by
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htm]
          simpa [htm] using hzero
      change rsum n (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) = C
      rw [show (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) =
        (fun t => if t = y (Root m) then C else 0) by
          funext t
          by_cases ht : t < n
          · exact hpoint t ht
          · have htn : y (Root m) < n := by
              have := hy m (by simp)
              simpa using this
            have htne : t ≠ y (Root m) := by omega
            simp [htne]
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htne]]
      rw [rsum_ite_eq_single n (y (Root m)) (by
        have := hy m (by simp)
        simpa using this)]

private theorem perm_append_left2 {α} {l1 l2 : List α} (h : l1.Perm l2) (l3 : List α) :
    (l1 ++ l3).Perm (l2 ++ l3) := by
  induction h with
  | nil => rfl
  | cons x h ih => exact .cons x ih
  | swap x y l => exact .swap x y (l ++ l3)
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_append_right2 (l1 : List Nat) {l3 l4 : List Nat} (h : l3.Perm l4) :
    (l1 ++ l3).Perm (l1 ++ l4) := by
  induction l1 with
  | nil => simpa using h
  | cons a l1 ih => exact .cons a ih

private theorem perm_append2 {l1 l2 l3 l4 : List Nat} (h1 : l1.Perm l2) (h2 : l3.Perm l4) :
    (l1 ++ l3).Perm (l2 ++ l4) :=
  (perm_append_left2 h1 l3).trans (perm_append_right2 l2 h2)

private theorem cons_append_perm (a : Nat) (l : List Nat) : (a :: l).Perm (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons x l ih => exact (List.Perm.swap x a l).trans (.cons x ih)

private theorem filter_append_compl2 (p : Nat → Bool) (l : List Nat) :
    l.Perm ((l.filter p) ++ (l.filter (fun x => !p x))) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : p a = true
      · simpa [ha] using (List.Perm.cons a ih)
      · have ha' : p a = false := by cases h : p a <;> simp_all
        have hstep := (List.Perm.cons a ih).trans
          (perm_append2 (cons_append_perm a (l.filter p))
            (List.Perm.refl (l.filter (fun x => !p x))))
        simpa [ha'] using hstep

private theorem perm_filter_of_nodup2 {l l' : List Nat} (hnd : l.Nodup) (hnd' : l'.Nodup)
    (hsub : ∀ x ∈ l, x ∈ l') : l.Perm (l'.filter (fun x => x ∈ l)) := by
  induction l' generalizing l with
  | nil =>
      have hl : l = [] := by
        cases l with
        | nil => rfl
        | cons a as => exact False.elim (by simpa using hsub a (by simp))
      subst l
      simp
  | cons a l' ih =>
      have hnot : a ∉ l' := (List.nodup_cons.mp hnd').1
      by_cases ha : a ∈ l
      · have htail := ih (hnd.erase a) (List.nodup_cons.mp hnd').2 (by
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnd.not_mem_erase hx
          have hxl := (List.mem_erase_of_ne hxa).mp hx
          simpa [hxa] using hsub x hxl)
        have hab : decide (a ∈ l) = true := by simp [ha]
        have hfilter : (a :: l').filter (fun x => x ∈ l) =
            a :: l'.filter (fun x : Nat => x ∈ l.erase a) := by
          rw [List.filter_cons, ite_eq_left hab]
          congr 1
          apply List.filter_congr
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnot hx
          simp [List.mem_erase_of_ne hxa]
        rw [hfilter]
        exact (List.perm_cons_erase ha).trans (.cons a htail)
      · rw [List.filter_cons]
        simp [ha]
        apply ih hnd (List.nodup_cons.mp hnd').2
        intro x hx
        have hxa : x ≠ a := by intro h; subst x; exact ha hx
        simpa [hxa] using hsub x hx

private theorem collapse_sum (n : Nat) (b : Bind) (F : Bind → Int)
    (bs : List (List Nat)) (hbs : bs ∈ partitions pattern.freeVars)
    (hF : ∀ x y, (∀ v, x v = y v) → F x = F y) :
    integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) =
      integrate n pattern.freeVars b (fun x => if constOn bs x then F x else 0) := by
  have hPS := ps_of_mem hbs
  have hspec := heads_spec hbs
  have hVnd : pattern.freeVars.Nodup := by native_decide
  let H := Heads bs
  let M := pattern.freeVars.filter (fun v => !H.contains v)
  have hHsub : ∀ v ∈ H, v ∈ pattern.freeVars := hspec.2
  have hrootV : ∀ v ∈ pattern.freeVars, Root bs v ∈ H := by
    intro v hv
    exact root_mem_heads hPS v hv
  have hrootH : ∀ h ∈ H, Root bs h = h := by
    intro h hh
    exact root_head hPS h hh
  have hrootOut : ∀ v, v ∉ pattern.freeVars → Root bs v = v := root_out hPS
  have hconst : ∀ x, constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := constOn_iff hPS
  have hfilt : (pattern.freeVars.filter (fun v => H.contains v)).Perm H := by
    have hp := perm_filter_of_nodup2 hspec.1 hVnd hHsub
    simpa using hp.symm
  have hpermV : pattern.freeVars.Perm (H ++ M) := by
    have h1 := filter_append_compl2 H.contains pattern.freeVars
    have h2 : (pattern.freeVars.filter (fun v => H.contains v) ++
        pattern.freeVars.filter (fun v => !H.contains v)).Perm
        (H ++ M) := perm_append2 hfilt (by rfl)
    exact h1.trans h2
  rw [integrate_perm2 n hpermV b (fun x => if constOn bs x then F x else 0)]
  rw [integrate_append2]
  apply integrate_congr_bindings
  intro y hy
  have hyH : ∀ h ∈ H, y h ∈ List.range n := by
    intro h hh
    exact bindings_range n H b y hy h hh
  have hyfixed : ∀ v ∉ H, y v = b v := by
    intro v hv
    exact bindings_fixed n H b y hy v hv
  let x0 : Bind := fun v => y (Root bs v)
  have hx0V : ∀ v ∈ pattern.freeVars, x0 v ∈ List.range n := by
    intro v hv
    have hr := hrootV v hv
    simpa [x0] using hyH (Root bs v) hr
  have hMsub : ∀ v ∈ M, v ∈ pattern.freeVars := by
    intro v hv
    exact (List.mem_filter.mp hv).1
  have hMnotH : ∀ v ∈ M, v ∉ H := by
    intro v hv
    have hnot := (List.mem_filter.mp hv).2
    simpa using hnot
  have hMnd : M.Nodup := hVnd.filter _
  have hRnot : ∀ m ∈ M, Root bs m ∉ M := by
    intro m hm hmem
    have hr := hrootV m (hMsub m hm)
    exact hMnotH (Root bs m) hmem hr
  have hyrange : ∀ m ∈ M, y (Root bs m) ∈ List.range n := by
    intro m hm
    exact hyH (Root bs m) (hrootV m (hMsub m hm))
  have hinner : integrate n M y (fun x => if constOn bs x then F x else 0) = F x0 := by
    calc
      integrate n M y (fun x => if constOn bs x then F x else 0)
          = integrate n M y (fun x => if M.all (fun m => x m == y (Root bs m)) then F x0 else 0) := by
            apply integrate_congr_bindings
            intro x hx
            have hxfixedH : ∀ h ∈ H, x h = y h := by
              intro h hh
              have hnot : h ∉ M := by
                intro hm
                exact hMnotH h hm hh
              rw [bindings_fixed n M y x hx h hnot]
            by_cases hall : M.all (fun m => x m == y (Root bs m)) = true
            · rw [ite_eq_left hall]
              have hxFx0 : ∀ v ∈ pattern.freeVars, x v = x0 v := by
                intro v hv
                by_cases hvH : v ∈ H
                · have hrv := hrootH v hvH
                  change x v = y (Root bs v)
                  rw [hxfixedH v hvH, hrv]
                · have hvM : v ∈ M := by
                    simp [M, hv, hvH]
                  have hvEq := List.all_eq_true.mp hall v hvM
                  change x v = y (Root bs v)
                  simpa using hvEq
              have hcond : ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
                intro v hv
                have hxv := hxFx0 v hv
                have hxroot : x (Root bs v) = y (Root bs v) :=
                  hxfixedH (Root bs v) (hrootV v hv)
                exact hxv.trans (by simpa [x0] using hxroot.symm)
              rw [ite_eq_left ((hconst x).2 hcond)]
              have hxall : ∀ v, x v = x0 v := by
                intro v
                by_cases hv : v ∈ pattern.freeVars
                · exact hxFx0 v hv
                · have hnotM : v ∉ M := by intro hm; exact hv (hMsub v hm)
                  have hx_y : x v = y v := bindings_fixed n M y x hx v hnotM
                  have hnotH : v ∉ H := by intro hh; exact hv (hHsub v hh)
                  have hy_b : y v = b v := bindings_fixed n H b y hy v hnotH
                  have hrv : Root bs v = v := hrootOut v hv
                  dsimp [x0]
                  rw [hrv]
                  rw [hy_b] at hx_y ⊢
                  exact hx_y
              exact hF x x0 hxall
            · have hallnot : ¬ M.all (fun m => x m == y (Root bs m)) = true := by
                intro h
                exact hall h
              rw [ite_eq_right hallnot]
              have hcondnot : ¬ (∀ v ∈ pattern.freeVars, x v = x (Root bs v)) := by
                intro hc
                apply hall
                apply List.all_eq_true.mpr
                intro m hm
                have hcv := hc m (hMsub m hm)
                have hrv := hrootV m (hMsub m hm)
                have hxrv : x (Root bs m) = y (Root bs m) := hxfixedH (Root bs m) hrv
                simp [hcv, hxrv]
              have hconstnot : ¬ constOn bs x = true := by
                intro hx'
                exact hcondnot ((hconst x).1 hx')
              rw [ite_eq_right hconstnot]
      _ = F x0 := inner_collapse n M (fun v => Root bs v) y (F x0) hMnd hRnot hyrange
  simpa [x0] using hinner.symm

private theorem mobius_point (x : Bind) :
    ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
    (if (pattern.freeVars.map x).Nodup then (1 : Int) else 0) := by
  have e21 : (x 2 = x 1) = (x 1 = x 2) := propext eq_comm
  have e31 : (x 3 = x 1) = (x 1 = x 3) := propext eq_comm
  have e41 : (x 4 = x 1) = (x 1 = x 4) := propext eq_comm
  have e32 : (x 3 = x 2) = (x 2 = x 3) := propext eq_comm
  have e42 : (x 4 = x 2) = (x 2 = x 4) := propext eq_comm
  have e43 : (x 4 = x 3) = (x 3 = x 4) := propext eq_comm
  by_cases h12 : x 1 = x 2 <;>
  by_cases h13 : x 1 = x 3 <;>
  by_cases h14 : x 1 = x 4 <;>
  by_cases h23 : x 2 = x 3 <;>
  by_cases h24 : x 2 = x 4 <;>
  by_cases h34 : x 3 = x 4 <;>
  simp [pattern, partitions, mobius, factorial, constOn, headOf,
    e21, e31, e41, e32, e42, e43, h12, h13, h14, h23, h24, h34]

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.CollisionCorrection.Goal := by
  intro n db b
  let V := pattern.freeVars
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v, x v = y v) → F x = F y := by
    intro x y hxy
    dsimp only [F]
    simp [pattern, value, hxy 0, hxy 1, hxy 2, hxy 3, hxy 4]
  have hcoll : ∀ bs ∈ partitions V,
      collapsed pattern n db b bs =
        integrate n V b (fun x => if constOn bs x then F x else 0) := by
    intro bs hbs
    unfold collapsed
    change integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) = _
    exact collapse_sum n b F bs hbs hF
  change integrate n V b (fun x => if (V.map x).Nodup then F x else 0) =
    ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum
  calc
    integrate n V b (fun x => if (V.map x).Nodup then F x else 0)
        = ((bindings n V b).map (fun x => if (V.map x).Nodup then F x else 0)).sum :=
            bindings_sum n V b _
    _ = ((bindings n V b).map (fun x => F x *
          ((partitions V).map (fun bs => mobius bs *
            (if constOn bs x then (1 : Int) else 0))).sum)).sum := by
          apply list_sum_congr2
          intro x hx
          rw [mobius_point x]
          by_cases hN : (V.map x).Nodup <;> simp [hN, V]
    _ = ((bindings n V b).map (fun x => ((partitions V).map (fun bs =>
          mobius bs * (if constOn bs x then F x else 0))).sum)).sum := by
          apply list_sum_congr2
          intro x hx
          rw [show (fun bs => mobius bs * (if constOn bs x then F x else 0)) =
              (fun bs => (mobius bs * (if constOn bs x then (1 : Int) else 0)) * F x) by
            funext bs
            by_cases h : constOn bs x <;> simp [h]]
          rw [sum_mul_right2]
          ac_rfl
    _ = ((partitions V).map (fun bs => mobius bs *
          ((bindings n V b).map (fun x => if constOn bs x then F x else 0)).sum)).sum := by
          rw [show (fun x => ((partitions V).map (fun bs =>
              mobius bs * (if constOn bs x then F x else 0))).sum) =
              (fun x => ((partitions V).map (fun bs =>
                (fun x => mobius bs * (if constOn bs x then F x else 0)) x)).sum) by rfl]
          rw [sum_swap2]
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [sum_mul_left2]
    _ = ((partitions V).map (fun bs => mobius bs *
          integrate n V b (fun x => if constOn bs x then F x else 0))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [bindings_sum]
    _ = ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [hcoll bs hbs]

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturnCapsAndCollisionCorrection
