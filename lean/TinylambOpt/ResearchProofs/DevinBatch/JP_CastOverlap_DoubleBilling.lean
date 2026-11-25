import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.JP_CastOverlap_DoubleBilling

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Finite-range sum helpers (mirroring the style of Atlas_JoinPlans_G1). -/

def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ, ih]
      omega

theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.mul_add]

theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.add_mul]

theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  simp only [Int.sub_eq_add_neg]
  rw [rsum_add]
  have hneg : rsum n (fun i => - g i) = - rsum n g := by
    have := rsum_const_mul n (-1) g
    simpa using this
  rw [hneg]

theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        rw [rsum_zero]]
      exact (rsum_const_zero m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

theorem rsum_product (n m : Nat) (f : Nat → Int) (g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

theorem rsum_mul_rsum (n m : Nat) (f g : Nat → Int) :
    rsum n f * rsum m g = rsum n (fun i => rsum m (fun j => f i * g j)) := by
  rw [rsum_product]

theorem rsum_swap23 (n : Nat) (f : Nat → Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => rsum n (fun k => f i j k))) =
      rsum n (fun i => rsum n (fun k => rsum n (fun j => f i j k))) := by
  apply rsum_congr
  intro i hi
  exact rsum_comm n n (fun j k => f i j k)

theorem rsum_swap34 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => rsum n (fun k => rsum n (fun l => f i j k l)))) =
      rsum n (fun i => rsum n (fun j => rsum n (fun l => rsum n (fun k => f i j k l)))) := by
  apply rsum_congr
  intro i hi
  apply rsum_congr
  intro j hj
  exact rsum_comm n n (fun k l => f i j k l)

theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn : 0 ≤ f n := h n (Nat.lt_succ_self n)
      have hi : 0 ≤ rsum n f := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

theorem list_sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

theorem list_sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

theorem list_sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

theorem list_sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

theorem list_sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

/-- Bridge between 0/1-valued indicator sums and `List.countP`. -/
theorem map_ite_sum_countP (l : List Nat) (c : Nat → Int) :
    ((l.map (fun y => if c y = 0 then (0:Int) else 1)).sum) =
      ((l.countP (fun y => c y != 0) : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons y l ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      by_cases h : c y = 0
      · simp [h, ih]
      · simp [h, ih]
        omega

theorem rsum_ite_countP (n : Nat) (c : Nat → Int) :
    rsum n (fun y => if c y = 0 then (0:Int) else 1) =
      ((List.range n).countP (fun y => c y != 0) : Int) := by
  show ((List.range n).map (fun y => if c y = 0 then (0:Int) else 1)).sum = _
  exact map_ite_sum_countP _ _

/-- `rsum` of a `Nat`-valued function equals the cast of the `Nat` sum. -/
theorem rsum_natCast (n : Nat) (g : Nat → Nat) :
    rsum n (fun x => (g x : Int)) = (((List.range n).map g).sum : Int) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      simp [List.range_succ, List.map_append, List.sum_append]

theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

theorem put_keep_other (b : Bind) (v w x : Nat) (hvw : v ≠ w) :
    put (put b v x) w (b w) = put b v x := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    have hvw' : (v == w) = false := by cases h' : (v == w) <;> simp_all
    simp [put, hvw']
  · by_cases hj : i == w
    · have hiw : i = w := Nat.eq_of_beq_eq_true hj
      subst i
      have hwv' : (w == v) = false := by cases h' : (w == v) <;> simp_all
      simp [put, hwv']
    · simp [put, hi, hj]

theorem put_put_ne_left (b : Bind) (v w x y : Nat) (hvw : v ≠ w) :
    put (put b v x) w y v = x := by
  have hvw' : (v == w) = false := by cases h' : (v == w) <;> simp_all
  simp [put, hvw']

theorem put_put_ne_right (b : Bind) (v w x y : Nat) (hvw : v ≠ w) :
    put (put b v x) w y w = y := by
  simp [put]

theorem put_ne (b : Bind) (v w x : Nat) (hvw : v ≠ w) :
    put b v x w = b w := by
  have hvw' : (w == v) = false := by cases h' : (w == v) <;> simp_all
  simp [put, hvw']

theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      apply list_sum_congr
      intro x hx
      exact ih (put b v x)

theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp [integrate, ih, list_sum_add]

theorem integrate_const_mul (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp [integrate, ih, list_sum_mul_left]

theorem integrate_mul_const (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) (c : Int) :
    integrate n vs b (fun x => f x * c) = integrate n vs b f * c := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp [integrate, ih, list_sum_mul_right]

theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  have h : (fun x => f x - g x) = (fun x => f x + - g x) := by
    funext x
    rw [Int.sub_eq_add_neg]
  rw [h, integrate_add]
  have hneg : integrate n vs b (fun x => - g x) = - integrate n vs b g := by
    have h2 : (fun x => - g x) = (fun x => (-1 : Int) * g x) := by
      funext x; simp
    rw [h2, integrate_const_mul]
    simp
  rw [hneg, Int.sub_eq_add_neg]

theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, list_sum_zero]

theorem integrate_singleton_rsum (n v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n [v] b f = rsum n (fun x => f (put b v x)) := by
  rfl

/-! Second-difference helpers. -/

theorem diff2_add (f g : Int → Int) :
    diff 2 (fun u => f u + g u) 0 = diff 2 f 0 + diff 2 g 0 := by
  simp [diff]
  omega

theorem diff2_rsum (n : Nat) (f : Nat → Int → Int) :
    diff 2 (fun u => rsum n (fun i => f i u)) 0 = rsum n (fun i => diff 2 (f i) 0) := by
  induction n with
  | zero => simp [rsum, diff]
  | succ n ih =>
      rw [show (fun u => rsum (n + 1) (fun i => f i u)) =
          (fun u => rsum n (fun i => f i u) + f n u) by
            funext u
            rw [rsum_succ]]
      rw [diff2_add, ih, rsum_succ]

theorem diff2_square_affine (A d K : Int) :
    diff 2 (fun u => (A + u * d) * (A + u * d) * K) 0 = 2 * d * d * K := by
  simp [diff]
  grind

theorem diff2_integrate (n : Nat) (vs : List Nat) (b : Bind) (F : Bind → Int → Int) :
    diff 2 (fun u => integrate n vs b (fun x => F x u)) 0 =
      integrate n vs b (fun x => diff 2 (fun u => F x u) 0) := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      have h1 : (fun u => integrate n (v :: vs) b (fun x => F x u)) =
          (fun u => rsum n (fun c => integrate n vs (put b v c) (fun x => F x u))) := by
        funext u; rfl
      rw [h1, diff2_rsum]
      have h2 : (fun c => diff 2 (fun u => integrate n vs (put b v c) (fun x => F x u)) 0) =
          (fun c => integrate n vs (put b v c) (fun x => diff 2 (fun u => F x u) 0)) := by
        funext c
        exact ih (put b v c)
      rw [h2]
      rfl

/-! Namespace DoubleBilling -/

namespace DoubleBilling
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

theorem db_pointwise (n : Nat) (db delta : DB) (b x : Bind)
    (hdelta : ∀ r i j, r ≠ 0 → delta r i j = 0) :
    diff 2 (fun u => (pattern.atoms.map (fun a => value (slope db delta u) a x)).prod) 0 =
      2 * (pattern.atoms.map
        (fun a => value (fun r => if r == 0 then delta 0 else db r) a x)).prod := by
  have hd1 : ∀ i j, delta 1 i j = 0 := fun i j => hdelta 1 i j (by decide)
  have hd2 : ∀ i j, delta 2 i j = 0 := fun i j => hdelta 2 i j (by decide)
  simp only [diff]
  simp [pattern, value, slope, hd1, hd2]
  grind

theorem SquaredDelta_sound : SquaredDelta.Goal := by
  intro n db delta b hdelta
  unfold reference
  rw [diff2_integrate n pattern.freeVars b
    (fun x u => (pattern.atoms.map (fun a => value (slope db delta u) a x)).prod)]
  have hcongr : integrate n pattern.freeVars b
        (fun x => diff 2 (fun u =>
          (pattern.atoms.map (fun a => value (slope db delta u) a x)).prod) 0) =
      integrate n pattern.freeVars b
        (fun x => 2 * (pattern.atoms.map
          (fun a => value (fun r => if r == 0 then delta 0 else db r) a x)).prod) :=
    integrate_congr _ _ _ _ _ (fun x => db_pointwise n db delta b x hdelta)
  rw [hcongr, integrate_const_mul]

end DoubleBilling

end TinylambOpt.ResearchProofs.DevinBatch.JP_CastOverlap_DoubleBilling
