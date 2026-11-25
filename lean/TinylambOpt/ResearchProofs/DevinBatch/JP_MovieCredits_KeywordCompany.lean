import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.JP_MovieCredits_KeywordCompany

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Finite-range sum / integrate helpers (adapted from Atlas_JoinPlans_G1). -/

def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) : rsum n f = rsum n g := by
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

theorem rsum_neg (n : Nat) (f : Nat → Int) : rsum n (fun i => - f i) = - rsum n f := by
  simpa using (rsum_const_mul n (-1) f)

theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  simp only [Int.sub_eq_add_neg]
  rw [rsum_add, rsum_neg]

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

theorem rsum_prod3 (n m k : Nat) (f : Nat → Int) (g : Nat → Int) (h : Nat → Int) :
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l))) =
      rsum n f * rsum m g * rsum k h := by
  calc
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l)))
        = rsum n (fun i => rsum m (fun j => (f i * g j) * rsum k h)) := by
            apply rsum_congr
            intro i hi
            apply rsum_congr
            intro j hj
            rw [rsum_const_mul]
    _ = rsum n (fun i => (rsum m (fun j => f i * g j)) * rsum k h) := by
            apply rsum_congr
            intro i hi
            rw [rsum_mul_const]
    _ = rsum n (fun i => (f i * rsum m g) * rsum k h) := by
            apply rsum_congr
            intro i hi
            rw [rsum_const_mul]
    _ = rsum n (fun i => f i * rsum m g) * rsum k h := by rw [rsum_mul_const]
    _ = (rsum n f * rsum m g) * rsum k h := by rw [rsum_mul_const]

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

theorem sq_nonneg_int (z : Int) : 0 ≤ z ^ 2 := Int.sq_nonneg z

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

theorem lagrange_id (n : Nat) (a b : Nat → Int) :
    rsum n (fun i => a i * a i) * rsum n (fun i => b i * b i) -
        (rsum n (fun i => a i * b i)) ^ 2 =
      rsum n (fun j => rsum j (fun i => (a j * b i - a i * b j) ^ 2)) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      let A := rsum n (fun i => a i * a i)
      let B := rsum n (fun i => b i * b i)
      let S := rsum n (fun i => a i * b i)
      rw [rsum_succ, rsum_succ, rsum_succ, rsum_succ]
      change (A + a n * a n) * (B + b n * b n) - (S + a n * b n) ^ 2 =
        rsum n (fun j => rsum j (fun i => (a j * b i - a i * b j) ^ 2)) +
          rsum n (fun i => (a n * b i - a i * b n) ^ 2)
      rw [show (A + a n * a n) * (B + b n * b n) - (S + a n * b n) ^ 2 =
          (A * B - S ^ 2) + (A * (b n * b n) + B * (a n * a n) - 2 * S * (a n * b n)) by
            grind]
      rw [show A * B - S ^ 2 =
          rsum n (fun j => rsum j (fun i => (a j * b i - a i * b j) ^ 2)) by
            exact ih]
      congr 1
      have hfun : (fun i => (a n * b i - a i * b n) ^ 2) =
          (fun i => (a n * a n) * (b i * b i) + (b n * b n) * (a i * a i) -
            2 * a n * b n * (a i * b i)) := by
        funext i
        grind
      rw [hfun]
      rw [rsum_sub, rsum_add, rsum_const_mul, rsum_const_mul, rsum_const_mul]
      dsimp only [A, B, S]
      grind

theorem cauchy_schwarz (n : Nat) (a b : Nat → Int) :
    (rsum n (fun i => a i * b i)) ^ 2 ≤
      rsum n (fun i => a i * a i) * rsum n (fun i => b i * b i) := by
  have hnon : 0 ≤ rsum n (fun i => a i * a i) * rsum n (fun i => b i * b i) -
      (rsum n (fun i => a i * b i)) ^ 2 := by
    rw [lagrange_id]
    apply rsum_nonneg
    intro j hj
    apply rsum_nonneg
    intro i hi
    exact sq_nonneg_int _
  omega

theorem sum_sq_nonneg (l : List Int) : 0 ≤ (l.map (fun z => z*z)).sum := by
  induction l with
  | nil => simp
  | cons z l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hz : 0 ≤ z*z := by
        rw [show z*z = z^2 by rw [Int.pow_succ, Int.pow_succ]; simp]
        exact Int.sq_nonneg z
      omega

theorem aux (l : List (Int × Int)) (a b : Int) :
  (l.map (fun p => p.1*p.1)).sum * (b*b) + (l.map (fun p => p.2*p.2)).sum * (a*a) -
    2*(l.map (fun p => p.1*p.2)).sum*a*b =
  (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      grind


theorem sum_sq_nonneg2 (l : List (Int × Int)) (a b : Int) :
    0 ≤ (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hp : 0 ≤ (a*p.2 - p.1*b)^2 := Int.sq_nonneg _
      omega

theorem cs_list : ∀ l : List (Int × Int),
    (l.map (fun p => p.1 * p.2)).sum ^ 2 ≤
      (l.map (fun p => p.1 * p.1)).sum * (l.map (fun p => p.2 * p.2)).sum
  | [] => by simp
  | (a,b)::l => by
      simp only [List.map_cons, List.sum_cons]
      have ih := cs_list l
      have hdiff : 0 ≤ (a*a + (l.map (fun p => p.1*p.1)).sum) *
              (b*b + (l.map (fun p => p.2*p.2)).sum) -
            (a*b + (l.map (fun p => p.1*p.2)).sum)^2 := by
        rw [show (a*a + (l.map (fun p => p.1*p.1)).sum) *
              (b*b + (l.map (fun p => p.2*p.2)).sum) -
              (a*b + (l.map (fun p => p.1*p.2)).sum)^2 =
            ((l.map (fun p => p.1*p.1)).sum * (l.map (fun p => p.2*p.2)).sum -
              (l.map (fun p => p.1*p.2)).sum^2) +
            ((l.map (fun p => p.1*p.1)).sum * (b*b) + (l.map (fun p => p.2*p.2)).sum * (a*a) -
              2*(l.map (fun p => p.1*p.2)).sum*a*b) by grind]
        rw [aux]
        have h1 : 0 ≤ (l.map (fun p => p.1*p.1)).sum * (l.map (fun p => p.2*p.2)).sum -
              (l.map (fun p => p.1*p.2)).sum^2 := by omega
        have h2 : 0 ≤ (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := sum_sq_nonneg2 l a b
        omega
      have hconv : ∀ (x y : Int), 0 ≤ y - x → x ≤ y := by
        intro x y h
        omega
      exact hconv _ _ hdiff
theorem list_pairs_sum (l : List Nat) (n : Nat) (f : Nat → Nat → Int) :
    (l.map (fun i => rsum n (fun j => f i j))).sum =
      ((l.flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
        (fun p => f p.1 p.2)).sum := by
  induction l with
  | nil => simp
  | cons i l ih =>
      simp only [List.map_cons, List.sum_cons, List.flatMap_cons]
      rw [ih]
      simp [rsum, List.map_append, List.sum_append, Function.comp_def]
theorem rsum_pairs (n : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => f i j)) =
      (((List.range n).flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
        (fun p => f p.1 p.2)).sum := by
  exact list_pairs_sum (List.range n) n f

/-! Generic list-sum helpers -/

theorem list_sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

theorem list_sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

theorem list_sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

theorem list_sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

theorem list_sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

theorem put_put_same (b : Bind) (v x y : Nat) : put (put b v x) v y = put b v y := by
  funext i
  by_cases h : i == v <;> simp [put, h]

theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
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

theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by
      cases h' : (v == w) <;> simp_all
    change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    change rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f))
    rw [rsum_comm]
    apply list_sum_congr
    intro x hx
    apply list_sum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

theorem integrate_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [integrate_swap]
      change ((List.range n).map (fun x => integrate n (v :: ws) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum
      exact integrate_congr n [w] b _ _ ih

theorem integrate_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ [v]) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      change ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n ws (put b w x) (fun b' => integrate n [v] b' f))).sum
      apply list_sum_congr
      intro x hx
      exact ih (put b w x)

theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih => simp [integrate, ih, list_sum_zero]

theorem integrate_head (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n [v] b (fun b' => integrate n vs b' f) := by
  rfl

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

theorem integrate_neg (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b (fun x => - f x) = - integrate n vs b f := by
  rw [Int.neg_eq_neg_one_mul]
  rw [← integrate_const_mul n vs b (-1) f]
  apply integrate_congr
  intro x
  simp

theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  have h : (fun x => f x - g x) = (fun x => f x + - g x) := by
    funext x
    rw [Int.sub_eq_add_neg]
  rw [h, integrate_add, integrate_neg]
  rw [Int.sub_eq_add_neg]

theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [integrate_add, ih]

theorem integrate_singleton_rsum (n v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n [v] b f = rsum n (fun x => f (put b v x)) := by
  rfl

theorem list_sum_filter_eq (l : List Nat) (p : Nat → Bool) (g : Nat → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a = true
      · simp [hp, ih (fun x hx => h x (by simp [hx]))]
      · have hp' : p a = false := by cases h' : p a <;> simp_all
        simp [hp', h a (by simp) hp', ih (fun x hx => h x (by simp [hx]))]

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

theorem candidates_mem (vs : List Nat) (n : Nat) (b : Bind) (v : Nat) (hv : v ∈ vs) :
    (if vs.contains v then List.range n else [b v]) = List.range n := by
  simp [hv]

theorem candidates_notmem (vs : List Nat) (n : Nat) (b : Bind) (v : Nat) (hv : v ∉ vs) :
    (if vs.contains v then List.range n else [b v]) = [b v] := by
  simp [hv]

theorem erase_eq_filter_ne (vs : List Nat) (hnd : vs.Nodup) (v : Nat) :
    vs.erase v = vs.filter (fun x => x != v) := by
  induction vs with
  | nil => simp
  | cons a as ih =>
      by_cases h : a == v
      · have ha : a = v := Nat.eq_of_beq_eq_true h
        subst a
        rw [List.erase_cons_head]
        have hnot : v ∉ as := (List.nodup_cons.mp hnd).1
        have hf : as.filter (fun x : Nat => x != v) = as := by
          apply List.filter_eq_self.mpr
          intro x hx
          have hxv : x ≠ v := by intro hxv; subst x; exact hnot hx
          cases h' : (x == v) <;> simp_all
        simp [hf]
      · have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
        rw [List.erase_cons_tail h, ih hndas]
        have hane : (a != v) = true := by cases h' : (a == v) <;> simp_all
        rw [List.filter_cons]
        simp [hane]

theorem erase_erase_eq_filter (vs : List Nat) (hnd : vs.Nodup) (v w : Nat) :
    (vs.erase v).erase w = vs.filter (fun x => x != v && x != w) := by
  rw [erase_eq_filter_ne vs hnd v]
  have hnd' : (vs.filter (fun x : Nat => x != v)).Nodup := hnd.filter _
  rw [erase_eq_filter_ne (vs.filter (fun x : Nat => x != v)) hnd' w]
  rw [List.filter_filter]
  apply List.filter_congr
  intro x hx
  cases hv : (x == v) <;> cases hw : (x == w) <;> simp_all [Bool.and_comm]

theorem integrate_extract (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (hnd : vs.Nodup) (hv : v ∈ vs) :
    integrate n vs b f = integrate n [v] b (fun b' => integrate n (vs.erase v) b' f) := by
  induction vs generalizing b f with
  | nil => simp at hv
  | cons w ws ih =>
      by_cases hwv : w == v
      · have hwv' : w = v := Nat.eq_of_beq_eq_true hwv
        subst w
        rw [List.erase_cons_head]
        exact integrate_head n v ws b f
      · have hvw : v ∈ ws := by
          simp only [List.mem_cons] at hv
          rcases hv with rfl | hv
          · simp at hwv
          · exact hv
        have hndws : ws.Nodup := (List.nodup_cons.mp hnd).2
        calc
          integrate n (w :: ws) b f
              = integrate n ws b (fun b' => integrate n [w] b' f) := integrate_cons_singleton n w ws b f
          _ = integrate n [v] b (fun b' => integrate n (ws.erase v) b'
                (fun b'' => integrate n [w] b'' f)) := ih b (fun b' => integrate n [w] b' f) hndws hvw
          _ = integrate n [v] b (fun b' => integrate n (w :: ws.erase v) b' f) := by
                apply integrate_congr
                intro b'
                exact (integrate_cons_singleton n w (ws.erase v) b' f).symm
          _ = integrate n [v] b (fun b' => integrate n ((w :: ws).erase v) b' f) := by
                rw [List.erase_cons_tail hwv]


def prodAtoms (db : DB) (atoms : List Atom) (x : Bind) : Int :=
  (atoms.map (fun a => value db a x)).prod

/-! Permutation / append / congruence lemmas for `integrate`. -/

theorem integrate_perm (n : Nat) (b : Bind) (f : Bind → Int) {l₁ l₂ : List Nat}
    (h : l₁.Perm l₂) : l₁.Nodup → integrate n l₁ b f = integrate n l₂ b f := by
  induction h with
  | refl => intro hnd; rfl
  | symm h ih =>
      intro hnd
      have hnd2 : l₂.Nodup := hnd.perm (List.Perm.symm h)
      exact (ih hnd2).symm
  | trans h1 h2 ih1 ih2 =>
      intro hnd
      have hnd2 : l₂.Nodup := hnd.perm h1
      exact (ih1 hnd).trans (ih2 hnd2)
  | swap x y l =>
      intro hnd
      exact integrate_swap n x y l b f
  | cons x h ih =>
      intro hnd
      have hnd1 : l₁.Nodup := (List.nodup_cons.mp hnd).2
      change ((List.range n).map (fun t => integrate n l₁ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n l₂ (put b x t) f)).sum
      apply list_sum_congr
      intro t ht
      exact ih hnd1

theorem integrate_append (n : Nat) (vs ws : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ ws) b f = integrate n vs b (fun b' => integrate n ws b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun t => integrate n (vs ++ ws) (put b v t) f)).sum =
        ((List.range n).map (fun t => integrate n vs (put b v t)
          (fun b' => integrate n ws b' f))).sum
      apply list_sum_congr
      intro t ht
      exact ih (put b v t)

theorem integrate_congr_range (n : Nat) (D R : List Nat) (b : Bind)
    (f g : Bind → Int)
    (h : ∀ y, (∀ v ∈ D, y v < n) → f y = g y)
    (hb : ∀ v ∈ D, v ∉ R → b v < n)
    (hRD : ∀ v ∈ R, v ∈ D) (hR : R.Nodup) :
    integrate n R b f = integrate n R b g := by
  induction R generalizing b with
  | nil =>
      apply h
      intro v hv
      exact hb v hv (by simp)
  | cons w ws ih =>
      have hws : ws.Nodup := (List.nodup_cons.mp hR).2
      change ((List.range n).map (fun t => integrate n ws (put b w t) f)).sum =
        ((List.range n).map (fun t => integrate n ws (put b w t) g)).sum
      apply list_sum_congr
      intro t ht
      have ht' : t < n := List.mem_range.mp ht
      apply ih (put b w t)
      · intro y hy
        exact h y (fun v hv => by
          by_cases hvw : v = w
          · subst v
            rw [put_self]
            exact ht'
          · have hvR : v ∉ ws := by
              intro hvm
              exact (List.nodup_cons.mp hR).1 (hvw ▸ hvm)
            rw [put_keep_other b w t v hvw]
            exact hb v hv (fun hvm => hvR hvm))
      · intro v hv hvR
        by_cases hvw : v = w
        · subst v
          rw [put_self]
          exact ht'
        · have hvws : v ∉ ws := by
            intro hvm
            apply hvR
            simp [hvw, hvm]
          rw [put_keep_other b w t v hvw]
          exact hb v hv (fun hvm => hvws hvm)
      · intro v hv
        exact hRD v (by simp [hv])
      · exact hws

theorem integrate_agree (n : Nat) (D R : List Nat) (b₁ b₂ : Bind) (f : Bind → Int)
    (h : ∀ y₁ y₂, (∀ v ∈ D, y₁ v = y₂ v) → f y₁ = f y₂)
    (hb : ∀ v ∈ D, v ∉ R → b₁ v = b₂ v)
    (hRD : ∀ v ∈ R, v ∈ D) (hR : R.Nodup) :
    integrate n R b₁ f = integrate n R b₂ f := by
  induction R generalizing b₁ b₂ with
  | nil =>
      apply h
      intro v hv
      exact hb v hv (by simp)
  | cons w ws ih =>
      have hws : ws.Nodup := (List.nodup_cons.mp hR).2
      change ((List.range n).map (fun t => integrate n ws (put b₁ w t) f)).sum =
        ((List.range n).map (fun t => integrate n ws (put b₂ w t) f)).sum
      apply list_sum_congr
      intro t ht
      apply ih (put b₁ w t) (put b₂ w t)
      · intro y₁ y₂ hy
        exact h y₁ y₂ (fun v hv => by
          by_cases hvw : v = w
          · subst v
            rw [put_self, put_self]
          · have hvws : v ∉ ws := by
              intro hvm
              exact (List.nodup_cons.mp hR).1 (hvw ▸ by simp [hvm])
            rw [put_keep_other b₁ w t v hvw, put_keep_other b₂ w t v hvw]
            exact hy v hv)
      · intro v hv hvR
        by_cases hvw : v = w
        · subst v
          rw [put_self, put_self]
        · have hvws : v ∉ ws := by
            intro hvm
            apply hvR
            simp [hvw, hvm]
          rw [put_keep_other b₁ w t v hvw, put_keep_other b₂ w t v hvw]
          exact hb v hv (fun hvm => hvws hvm)
      · intro v hv
        exact hRD v (by simp [hv])
      · exact hws

theorem integrate_ite_const (n : Nat) (vs : List Nat) (b : Bind) (c : Prop)
    [Decidable c] (f : Bind → Int) :
    integrate n vs b (fun x => if c then f x else 0) = if c then integrate n vs b f else 0 := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp only [integrate]
      by_cases hc : c <;> simp [hc, ih, list_sum_zero]

theorem rsum_pick (n c : Nat) (J : Int) :
    rsum n (fun t => if t = c then J else 0) = if c < n then J else 0 := by
  induction n generalizing c with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      by_cases hcn : c < n
      · have hcs : ¬ (n = c) := by omega
        simp [hcn, hcs]
      · by_cases hce : c = n
        · subst c
          simp [hcn, Nat.lt_succ_self n]
        · have hcg : n < c := by omega
          have hcs : ¬ (n = c) := by omega
          have hcn' : ¬ (c < n + 1) := by omega
          simp [hcn, hce, hcg, hcn', hcs, Nat.lt_succ_self n]

theorem ite_and_int (a b : Prop) [Decidable a] [Decidable b] (K : Int) :
    (if a ∧ b then K else 0) = if a then (if b then K else 0) else 0 := by
  by_cases ha : a <;> by_cases hb : b <;> simp [ha, hb]

/-- If `w ∉ vs`, every bind produced by `integrate n vs b` agrees with `b`
at `w`; hence integrands agreeing on such binds give equal integrals. -/
theorem integrate_congr_fix (n : Nat) (vs : List Nat) (b : Bind)
    (f g : Bind → Int) (w : Nat) (hw : w ∉ vs)
    (h : ∀ y, y w = b w → f y = g y) :
    integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b rfl
  | cons u us ih =>
      have hwu : w ≠ u := by
        intro h2; subst w; exact hw (by simp)
      have hwus : w ∉ us := by
        intro h2; exact hw (by simp [h2])
      change ((List.range n).map (fun t => integrate n us (put b u t) f)).sum =
        ((List.range n).map (fun t => integrate n us (put b u t) g)).sum
      apply list_sum_congr
      intro t ht
      apply ih (put b u t) hwus
      intro y hy
      apply h y
      rw [hy, put_keep_other b u t w hwu]

/-- Integrate of a pointwise-`ite`-constant integrand. -/
theorem integrate_pin (n : Nat) (vs : List Nat) (hnd : vs.Nodup) (b : Bind)
    (c : Nat → Nat) (K : Int) :
    integrate n vs b (fun y => if (∀ v ∈ vs, y v = c v) then K else 0) =
      if (∀ v ∈ vs, c v < n) then K else 0 := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v ws ih =>
      have hvw : v ∉ ws := (List.nodup_cons.mp hnd).1
      have hws : ws.Nodup := (List.nodup_cons.mp hnd).2
      -- rewrite the integrand: ∀ over v::ws splits into y v = c v ∧ rest
      have hF : (fun y => if (∀ u ∈ v :: ws, y u = c u) then K else 0) =
          (fun y => if y v = c v then
            (if (∀ u ∈ ws, y u = c u) then K else 0) else 0) := by
        funext y
        have hsplit : (∀ u ∈ v :: ws, y u = c u) ↔
            (y v = c v) ∧ (∀ u ∈ ws, y u = c u) := by
          constructor
          · intro h
            exact ⟨h v (by simp), fun u hu => h u (by simp [hu])⟩
          · rintro ⟨hv, hw⟩ u hu
            simp only [List.mem_cons] at hu
            rcases hu with rfl | hu
            · exact hv
            · exact hw u hu
        rw [ite_and_int]
        congr 1
        · exact propext hsplit
        · rfl
      rw [hF]
      change ((List.range n).map (fun t =>
          integrate n ws (put b v t)
            (fun y => if y v = c v then
              (if (∀ u ∈ ws, y u = c u) then K else 0) else 0))).sum = _
      have hterm : ∀ t ∈ List.range n,
          integrate n ws (put b v t)
            (fun y => if y v = c v then
              (if (∀ u ∈ ws, y u = c u) then K else 0) else 0) =
          if t = c v then
            (if (∀ u ∈ ws, c u < n) then K else 0) else 0 := by
        intro t ht
        -- produced y satisfy y v = (put b v t) v = t; rewrite the fixed coord
        have hfix := integrate_congr_fix n ws (put b v t)
          (fun y => if y v = c v then
            (if (∀ u ∈ ws, y u = c u) then K else 0) else 0)
          (fun y => if (put b v t) v = c v then
            (if (∀ u ∈ ws, y u = c u) then K else 0) else 0)
          v (by simp [hvw])
          (fun y hyv => by rw [hyv, put_self])
        rw [hfix, put_self]
        -- now ite(t = c v) is constant w.r.t. y
        have h2 := integrate_ite_const n ws (put b v t) (t = c v)
          (fun y => if (∀ u ∈ ws, y u = c u) then K else 0)
        rw [h2]
        congr 1
        · rfl
        · exact ih (put b v t) hws
      rw [list_sum_congr _ _ _ hterm]
      -- sum over t of ite(t = c v) * J
      rw [rsum_pick n (c v) (if (∀ u ∈ ws, c u < n) then K else 0)]
      by_cases hcv : c v < n
      · have hws' : (∀ u ∈ v :: ws, c u < n) ↔
            (c v < n) ∧ (∀ u ∈ ws, c u < n) := by
          constructor
          · intro h
            exact ⟨h v (by simp), fun u hu => h u (by simp [hu])⟩
          · rintro ⟨hv, hw⟩ u hu
            simp only [List.mem_cons] at hu
            rcases hu with rfl | hu
            · exact hv
            · exact hw u hu
        have : (if (∀ u ∈ v :: ws, c u < n) then K else 0) =
            if (c v < n) then (if (∀ u ∈ ws, c u < n) then K else 0) else 0 := by
          rw [ite_and_int]
          congr 1
          exact propext hws'
        rw [this, hcv]
        simp
      · have hws' : ¬ (∀ u ∈ v :: ws, c u < n) := by
          intro h
          exact hcv (h v (by simp))
        simp [hcv, hws']

end TinylambOpt.ResearchProofs.DevinBatch.JP_MovieCredits_KeywordCompany
