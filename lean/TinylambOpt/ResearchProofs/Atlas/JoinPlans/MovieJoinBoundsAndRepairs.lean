import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved (15 claims, including the following)

- `CastOverlap.AnchoredRepair` — `∀ n old new b, reference pattern n new b - reference
  pattern n old b = (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old
  new b i a)).sum`
- `CastOverlap.BoundaryMessages` — `∀ n db b, elimination pattern n db b = reference
  pattern n db b`
- `CastOverlap.GramSquare` — `∀ n db b, reference pattern n db b = total n (fun x =>
  total n (fun y => (overlap n db 0 x y)^2))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundsAndRepairs

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Finite-range sum helpers (Std only). -/

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

/-! Generic factor-elimination helpers -/

def Respects (f : Factor) : Prop :=
  ∀ b b', (∀ v, f.scope.contains v = true → b v = b' v) → f.get b = f.get b'

theorem respects_value (db : DB) (a : Atom) :
    Respects ⟨[a.src, a.dst], value db a⟩ := by
  intro b b' h
  simp only [value]
  rw [h a.src (by simp), h a.dst (by simp)]

theorem product_respects_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    {b b' : Bind} (heq : ∀ f ∈ fs, ∀ v, f.scope.contains v = true → b v = b' v) :
    product fs b = product fs b' := by
  induction fs with
  | nil => rfl
  | cons f fs ih =>
      simp only [product, List.map_cons, List.prod_cons]
      rw [h f (by simp) b b' (heq f (by simp))]
      change f.get b' * product fs b = f.get b' * product fs b'
      rw [ih (fun g hg => h g (by simp [hg])) (fun g hg => heq g (by simp [hg]))]

theorem product_put_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    (hnot : ∀ f ∈ fs, f.scope.contains v = false) (b : Bind) (x : Nat) :
    product fs (put b v x) = product fs b := by
  apply product_respects_eq h
  intro f hf w hw
  by_cases hvw : w == v
  · have hwv : w = v := Nat.eq_of_beq_eq_true hvw
    subst w
    rw [hnot f hf] at hw
    simp at hw
  · simp [put, hvw]

theorem partition_loop_spec (p : α → Bool) : ∀ (l a b : List α),
    (List.partition.loop p l (a, b)).fst = a.reverse ++ l.filter p ∧
    (List.partition.loop p l (a, b)).snd = b.reverse ++ l.filter (fun x => !p x)
  | [], a, b => by simp [List.partition.loop]
  | x :: xs, a, b => by
      by_cases h : p x = true
      · simp [List.partition.loop, h, partition_loop_spec p xs]
      · simp [List.partition.loop, h, partition_loop_spec p xs]

theorem partition_filters (p : α → Bool) (l : List α) :
    (List.partition p l).fst = l.filter p ∧
    (List.partition p l).snd = l.filter (fun x => !p x) := by
  simpa [List.partition] using partition_loop_spec p l [] []

theorem mem_partition_p (p : α → Bool) {l : List α} {a : α} :
    a ∈ (List.partition p l).fst → p a = true := by
  rw [(partition_filters p l).1]
  intro h
  exact (List.mem_filter.mp h).2

theorem mem_partition_notp (p : α → Bool) {l : List α} {a : α} :
    a ∈ (List.partition p l).snd → p a = false := by
  rw [(partition_filters p l).2]
  intro h
  have hn := (List.mem_filter.mp h).2
  by_cases hp : p a = true
  · simp [hp] at hn
  · simpa using hp

theorem product_filter_mul (p : α → Bool) (l : List α) (g : α → Int) :
    ((l.filter p).map g).prod * ((l.filter (fun x => !p x)).map g).prod = (l.map g).prod := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true <;> simp [h, ih, Int.mul_assoc, Int.mul_left_comm]

theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    ((List.partition p l).fst.map (fun f => f.get b)).prod *
      ((List.partition p l).snd.map (fun f => f.get b)).prod =
    (l.map (fun f => f.get b)).prod := by
  rw [(partition_filters p l).1, (partition_filters p l).2]
  exact product_filter_mul p l (fun f => f.get b)

theorem mem_of_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l := by
  exact (List.mem_partition (p := fun g : Factor => g.scope.contains v) (l := l) (a := f)).mpr (Or.inl hf)

theorem mem_of_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l := by
  exact (List.mem_partition (p := fun g : Factor => g.scope.contains v) (l := l) (a := f)).mpr (Or.inr hf)

theorem respects_msg (n v : Nat) (inc : List Factor) (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  change ((List.range n).map (fun x => product inc (put b v x))).sum =
    ((List.range n).map (fun x => product inc (put b' v x))).sum
  apply list_sum_congr
  intro x hx
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w == v
  · have hwv' : w = v := Nat.eq_of_beq_eq_true hwv
    subst w
    simp [put_self]
  · simp [put, hwv]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwErase : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    have hwne : (w != v) = true := by
      cases h' : (w == v) <;> simp_all
    have hmem : w ∈ (inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v) := by
      apply List.mem_filter.mpr
      exact ⟨hwErase, hwne⟩
    simpa using hmem

theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (eliminate n v fs) b = integrate n [v] b (product fs) := by
  dsimp only [eliminate]
  let inc := (fs.partition (fun f : Factor => f.scope.contains v)).fst
  let rest := (fs.partition (fun f : Factor => f.scope.contains v)).snd
  have hinc_res : ∀ f ∈ inc, Respects f := by
    intro f hf
    exact hres f (mem_of_partition_fst hf)
  have hrest_res : ∀ f ∈ rest, Respects f := by
    intro f hf
    exact hres f (mem_of_partition_snd hf)
  have hrest_not : ∀ f ∈ rest, f.scope.contains v = false := by
    intro f hf
    exact mem_partition_notp (fun g : Factor => g.scope.contains v) hf
  change (integrate n [v] b (fun x => product inc x)) * product rest b =
    integrate n [v] b (product fs)
  change ((List.range n).map (fun x => product inc (put b v x))).sum * product rest b =
    ((List.range n).map (fun x => product fs (put b v x))).sum
  rw [← list_sum_mul_right]
  apply list_sum_congr
  intro x hx
  change (List.map (fun f => f.get (put b v x)) inc).prod * product rest b =
    (List.map (fun f => f.get (put b v x)) fs).prod
  rw [← product_put_eq hrest_res hrest_not b x]
  change (List.map (fun f => f.get (put b v x)) inc).prod *
      (List.map (fun f => f.get (put b v x)) rest).prod =
    (List.map (fun f => f.get (put b v x)) fs).prod
  dsimp only [inc, rest]
  rw [product_partition_get (fun f : Factor => f.scope.contains v) fs (put b v x)]

theorem respects_eliminate (n v : Nat) (fs : List Factor) (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  dsimp only [eliminate] at hf
  simp only [List.mem_cons] at hf
  rcases hf with rfl | hf
  · exact respects_msg n v (fs.partition (fun g : Factor => g.scope.contains v)).fst (by
      intro g hg
      exact hres g (mem_of_partition_fst hg))
  · exact hres f (mem_of_partition_snd hf)

theorem product_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (vs.foldl (fun fs v => eliminate n v fs) fs) b = integrate n vs b (product fs) := by
  induction vs generalizing fs with
  | nil => rfl
  | cons v vs ih =>
      change product (vs.foldl (fun fs v => eliminate n v fs) (eliminate n v fs)) b =
        integrate n (v :: vs) b (product fs)
      rw [ih (eliminate n v fs) (respects_eliminate n v fs hres)]
      calc
        integrate n vs b (product (eliminate n v fs))
            = integrate n vs b (fun x => integrate n [v] x (product fs)) := by
                apply integrate_congr
                intro x
                exact product_eliminate n v fs x hres
        _ = integrate n (v :: vs) b (product fs) := (integrate_cons_singleton n v vs b (product fs)).symm

theorem elimination_eq_reference (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    elimination p n db b = reference p n db b := by
  unfold elimination reference
  rw [product_fold_eliminate]
  · apply integrate_congr
    intro x
    simp [factors, product, Function.comp_def]
  · intro f hf
    rcases List.mem_map.mp hf with ⟨a, ha, rfl⟩
    exact respects_value db a

/-! Generic telescoping / pivot helpers -/

def prodAtoms (db : DB) (atoms : List Atom) (x : Bind) : Int :=
  (atoms.map (fun a => value db a x)).prod

def crossProd (atoms : List Atom) (old new : DB) (i : Nat) (x : Bind) : Int :=
  ((atoms.take i).map (fun a => value new a x)).prod *
  ((atoms.drop (i+1)).map (fun a => value old a x)).prod

def idxSum (atoms : List Atom) (F : Nat → Atom → Int) : Int :=
  match atoms with
  | [] => 0
  | a :: as => F 0 a + idxSum as (fun i b => F (i+1) b)

theorem idxSum_congr (atoms : List Atom) (F G : Nat → Atom → Int)
    (h : ∀ i a, F i a = G i a) : idxSum atoms F = idxSum atoms G := by
  induction atoms generalizing F G with
  | nil => rfl
  | cons a as ih =>
      simp only [idxSum]
      rw [h 0 a, ih (fun i b => F (i+1) b) (fun i b => G (i+1) b) (fun i b => h (i+1) b)]

theorem idxSum_mul_left (atoms : List Atom) (c : Int) (F : Nat → Atom → Int) :
    idxSum atoms (fun i a => c * F i a) = c * idxSum atoms F := by
  induction atoms generalizing F with
  | nil => simp [idxSum]
  | cons a as ih =>
      simp only [idxSum]
      rw [ih (fun i b => F (i+1) b)]
      rw [Int.mul_add]

theorem zipIdx_sum_offset (atoms : List Atom) (F : Nat → Atom → Int) (k : Nat) :
    ((atoms.zipIdx k).map (fun (a, i) => F i a)).sum = idxSum atoms (fun i a => F (k + i) a) := by
  induction atoms generalizing k with
  | nil => simp [idxSum]
  | cons a as ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [ih (k+1)]
      simp only [idxSum]
      have hshift : (fun i b => F (k + (i+1)) b) = (fun i b => F ((k+1) + i) b) := by
        funext i b
        congr 1
        omega
      rw [hshift]
      simp

theorem crossProd_zero (a : Atom) (atoms : List Atom) (old new : DB) (x : Bind) :
    crossProd (a :: atoms) old new 0 x = prodAtoms old atoms x := by
  simp [crossProd, prodAtoms]

theorem crossProd_succ (a : Atom) (atoms : List Atom) (old new : DB) (i : Nat) (x : Bind) :
    crossProd (a :: atoms) old new (i+1) x = value new a x * crossProd atoms old new i x := by
  simp [crossProd, List.take, List.drop, Int.mul_assoc]

theorem prodAtoms_diff (atoms : List Atom) (old new : DB) (x : Bind) :
    prodAtoms new atoms x - prodAtoms old atoms x =
      idxSum atoms (fun i a => (value new a x - value old a x) * crossProd atoms old new i x) := by
  induction atoms with
  | nil => simp [prodAtoms, idxSum]
  | cons a as ih =>
      change value new a x * prodAtoms new as x - value old a x * prodAtoms old as x =
        idxSum (a :: as) (fun i b => (value new b x - value old b x) * crossProd (a :: as) old new i x)
      simp only [idxSum]
      rw [crossProd_zero a as old new x]
      rw [show idxSum as (fun i b => (value new b x - value old b x) * crossProd (a :: as) old new (i+1) x) =
          idxSum as (fun i b => value new a x * ((value new b x - value old b x) * crossProd as old new i x)) by
        apply idxSum_congr
        intro i b
        rw [crossProd_succ]
        ac_rfl]
      rw [idxSum_mul_left]
      rw [← ih]
      change value new a x * prodAtoms new as x - value old a x * prodAtoms old as x =
        (value new a x - value old a x) * prodAtoms old as x +
          value new a x * (prodAtoms new as x - prodAtoms old as x)
      rw [Int.sub_mul, Int.mul_sub]
      omega

theorem integrate_idxSum (n : Nat) (vs : List Nat) (b : Bind) (atoms : List Atom)
    (F : Nat → Atom → Bind → Int) :
    integrate n vs b (fun x => idxSum atoms (fun i a => F i a x)) =
      idxSum atoms (fun i a => integrate n vs b (F i a)) := by
  induction atoms generalizing F with
  | nil => simp [idxSum, integrate_zero]
  | cons a as ih =>
      simp only [idxSum]
      rw [integrate_add, ih (fun i a => F (i+1) a)]

theorem filter_singleton_sum (x : Nat) (p : Nat → Bool) (g : Nat → Int)
    (hzero : p x = false → g x = 0) :
    (([x].filter p).map g).sum = g x := by
  rw [List.filter_cons]
  by_cases hp : p x = true
  · simp [hp]
  · have hp' : p x = false := by cases h' : p x <;> simp_all
    simp [hp', hzero hp']

theorem pivot_delta_factor_aux (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (u v : Nat) (hsd : a.src ≠ a.dst) (vars : List Nat)
    (hsrc : a.src ∉ vars) (hdst : a.dst ∉ vars)
    (hbsrc : b a.src = u) (hbdst : b a.dst = v) :
    integrate n vars b
        (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x) =
      (new a.rel u v - old a.rel u v) *
        integrate n vars b (fun x => crossProd p.atoms old new j x) := by
  induction vars generalizing b with
  | nil => simp [integrate, value, hbsrc, hbdst]
  | cons w ws ih =>
      have hsrc_ws : a.src ∉ ws := by intro h; exact hsrc (by simp [h])
      have hdst_ws : a.dst ∉ ws := by intro h; exact hdst (by simp [h])
      have hw_src : w ≠ a.src := by intro h; subst w; exact hsrc (by simp)
      have hw_dst : w ≠ a.dst := by intro h; subst w; exact hdst (by simp)
      simp only [integrate]
      rw [← list_sum_mul_left]
      apply list_sum_congr
      intro x hx
      apply ih (put b w x) hsrc_ws hdst_ws
      · have h : (a.src == w) = false := by cases h' : (a.src == w) <;> simp_all
        simp [put, h, hbsrc]
      · have h : (a.dst == w) = false := by cases h' : (a.dst == w) <;> simp_all
        simp [put, h, hbdst]

theorem pivot_delta_factor (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (u v : Nat) (hsd : a.src ≠ a.dst) (vars : List Nat)
    (hsrc : a.src ∉ vars) (hdst : a.dst ∉ vars) :
    integrate n vars (put (put b a.src u) a.dst v)
        (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x) =
      (new a.rel u v - old a.rel u v) *
        integrate n vars (put (put b a.src u) a.dst v)
          (fun x => crossProd p.atoms old new j x) := by
  apply pivot_delta_factor_aux p n old new (put (put b a.src u) a.dst v) j a u v hsd vars hsrc hdst
  · exact put_put_ne_left b a.src a.dst u v hsd
  · exact put_put_ne_right b a.src a.dst u v hsd

theorem pivot_zero_of_eq (r u v : Nat) (old new : DB) :
    (old r u v != new r u v) = false → new r u v - old r u v = 0 := by
  intro h
  have heq : old r u v = new r u v := by
    cases h' : (old r u v == new r u v) <;> simp_all
  rw [heq]
  simp

theorem pivot_integral_eq (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) :
    integrate n p.freeVars b
        (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x) =
      pivotTerm p n old new b j a := by
  let vs := p.freeVars
  let vars := vs.filter (fun v => v != a.src && v != a.dst)
  let f : Bind → Int := fun x =>
    (value new a x - value old a x) * crossProd p.atoms old new j x
  have hsrc_not : a.src ∉ vars := by simp [vars]
  have hdst_not : a.dst ∉ vars := by simp [vars]
  by_cases hs : a.src ∈ vs
  · by_cases hd : a.dst ∈ vs
    · have hnd_erase : (vs.erase a.src).Nodup := hnd.erase _
      have hd_erase : a.dst ∈ vs.erase a.src := by
        rw [erase_eq_filter_ne vs hnd a.src]
        apply List.mem_filter.mpr
        constructor
        · exact hd
        · have hne : a.dst ≠ a.src := hsd.symm
          cases h' : (a.dst == a.src) <;> simp_all
      calc
        integrate n vs b f
            = integrate n [a.src] b (fun b' => integrate n (vs.erase a.src) b' f) :=
                integrate_extract n a.src vs b f hnd hs
        _ = integrate n [a.src] b (fun b' => integrate n [a.dst] b'
              (fun b'' => integrate n ((vs.erase a.src).erase a.dst) b'' f)) := by
                apply integrate_congr
                intro b'
                exact integrate_extract n a.dst (vs.erase a.src) b' f hnd_erase hd_erase
        _ = integrate n [a.src] b (fun b' => integrate n [a.dst] b'
              (fun b'' => integrate n vars b'' f)) := by
                apply integrate_congr; intro b'
                apply integrate_congr; intro b''
                rw [show (vs.erase a.src).erase a.dst = vars from erase_erase_eq_filter vs hnd a.src a.dst]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              rw [candidates_mem vs n b a.src hs, candidates_mem vs n b a.dst hd]
              rw [integrate_singleton_rsum]
              apply rsum_congr
              intro u hu
              rw [integrate_singleton_rsum]
              change rsum n (fun v =>
                  integrate n vars (put (put b a.src u) a.dst v)
                    (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x)) =
                (((List.range n).filter (fun v => old a.rel u v != new a.rel u v)).map (fun v =>
                  (new a.rel u v - old a.rel u v) *
                    integrate n vars (put (put b a.src u) a.dst v)
                      (fun x => crossProd p.atoms old new j x))).sum
              have hfac : (fun v => integrate n vars (put (put b a.src u) a.dst v)
                    (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x)) =
                  (fun v => (new a.rel u v - old a.rel u v) *
                    integrate n vars (put (put b a.src u) a.dst v)
                      (fun x => crossProd p.atoms old new j x)) := by
                funext v
                exact pivot_delta_factor p n old new b j a u v hsd vars hsrc_not hdst_not
              rw [hfac]
              rw [list_sum_filter_eq (List.range n) (fun v => old a.rel u v != new a.rel u v)
                    (fun v => (new a.rel u v - old a.rel u v) *
                      integrate n vars (put (put b a.src u) a.dst v)
                        (fun x => crossProd p.atoms old new j x))
                    (by
                      intro v hv hpf
                      rw [pivot_zero_of_eq a.rel u v old new hpf]
                      simp)]
              rfl
    · have hvars : vs.erase a.src = vars := by
        rw [erase_eq_filter_ne vs hnd a.src]
        apply List.filter_congr
        intro x hx
        by_cases hxs : x == a.src
        · have hxsrc : x = a.src := Nat.eq_of_beq_eq_true hxs
          subst x
          simp
        · by_cases hxd : x == a.dst
          · have hxdst : x = a.dst := Nat.eq_of_beq_eq_true hxd
            subst x
            exact False.elim (hd hx)
          · have hnxs : x ≠ a.src := by
              intro heq; subst x; exact hxs (by simp)
            have hnxd : x ≠ a.dst := by
              intro heq; subst x; exact hxd (by simp)
            simp [hnxs, hnxd]
      calc
        integrate n vs b f
            = integrate n [a.src] b (fun b' => integrate n (vs.erase a.src) b' f) :=
                integrate_extract n a.src vs b f hnd hs
        _ = integrate n [a.src] b (fun b' => integrate n vars b' f) := by
                apply integrate_congr; intro b'; rw [hvars]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              rw [candidates_mem vs n b a.src hs, candidates_notmem vs n b a.dst hd]
              rw [integrate_singleton_rsum]
              apply rsum_congr
              intro u hu
              change integrate n vars (put b a.src u)
                    (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x) =
                (([b a.dst].filter (fun v => old a.rel u v != new a.rel u v)).map (fun v =>
                  (new a.rel u v - old a.rel u v) *
                    integrate n vars (put (put b a.src u) a.dst v)
                      (fun x => crossProd p.atoms old new j x))).sum
              rw [filter_singleton_sum (b a.dst)
                    (fun v => old a.rel u v != new a.rel u v)
                    (fun v => (new a.rel u v - old a.rel u v) *
                      integrate n vars (put (put b a.src u) a.dst v)
                        (fun x => crossProd p.atoms old new j x))
                    (by
                      intro hpf
                      rw [pivot_zero_of_eq a.rel u (b a.dst) old new hpf]
                      simp)]
              rw [← pivot_delta_factor p n old new b j a u (b a.dst) hsd vars hsrc_not hdst_not]
              rw [put_keep_other b a.src a.dst u hsd]
  · by_cases hd : a.dst ∈ vs
    · have hvars : vs.erase a.dst = vars := by
        rw [erase_eq_filter_ne vs hnd a.dst]
        apply List.filter_congr
        intro x hx
        by_cases hxd : x == a.dst
        · have hxdst : x = a.dst := Nat.eq_of_beq_eq_true hxd
          subst x
          simp
        · by_cases hxs : x == a.src
          · have hxsrc : x = a.src := Nat.eq_of_beq_eq_true hxs
            subst x
            exact False.elim (hs hx)
          · have hnxs : x ≠ a.src := by
              intro heq; subst x; exact hxs (by simp)
            have hnxd : x ≠ a.dst := by
              intro heq; subst x; exact hxd (by simp)
            simp [hnxs, hnxd]
      calc
        integrate n vs b f
            = integrate n [a.dst] b (fun b' => integrate n (vs.erase a.dst) b' f) :=
                integrate_extract n a.dst vs b f hnd hd
        _ = integrate n [a.dst] b (fun b' => integrate n vars b' f) := by
                apply integrate_congr; intro b'; rw [hvars]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              rw [candidates_notmem vs n b a.src hs, candidates_mem vs n b a.dst hd]
              rw [integrate_singleton_rsum]
              let I1 : Nat → Int := fun v =>
                integrate n vars (put b a.dst v)
                  (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x)
              let I2 : Nat → Nat → Int := fun u v =>
                (new a.rel u v - old a.rel u v) *
                  integrate n vars (put (put b a.src u) a.dst v)
                    (fun x => crossProd p.atoms old new j x)
              let S : Nat → Int := fun u =>
                (((List.range n).filter (fun v => old a.rel u v != new a.rel u v)).map
                  (fun v => I2 u v)).sum
              change rsum n I1 =
                ((([b a.src].filter (fun _ => true)).map (fun u => S u)).sum)
              rw [filter_singleton_sum (b a.src) (fun _ => true) S (by intro h; cases h)]
              dsimp only [S]
              rw [list_sum_filter_eq (List.range n)
                    (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v)
                    (I2 (b a.src))
                    (by
                      intro v hv hpf
                      dsimp only [I2]
                      rw [pivot_zero_of_eq a.rel (b a.src) v old new hpf]
                      simp)]
              change rsum n I1 = rsum n (I2 (b a.src))
              apply rsum_congr
              intro v hv
              dsimp only [I1, I2]
              rw [show put (put b a.src (b a.src)) a.dst v = put b a.dst v by rw [put_same]]
              rw [pivot_delta_factor_aux p n old new (put b a.dst v) j a (b a.src) v
                    hsd vars hsrc_not hdst_not
                    (by
                      have h : (a.src == a.dst) = false := by
                        cases h' : (a.src == a.dst) <;> simp_all
                      simp [put, h])
                    (by simp [put])]
    · have hvars : vs = vars := by
        dsimp only [vars]
        have hf : (List.filter (fun v => v != a.src && v != a.dst) vs) = vs := by
          apply List.filter_eq_self.mpr
          intro x hx
          have hnxs : x ≠ a.src := by intro heq; subst x; exact hs hx
          have hnxd : x ≠ a.dst := by intro heq; subst x; exact hd hx
          cases h1 : (x == a.src) <;> cases h2 : (x == a.dst) <;> simp_all
        exact hf.symm
      calc
        integrate n vs b f = integrate n vars b f := by rw [hvars]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              rw [candidates_notmem vs n b a.src hs, candidates_notmem vs n b a.dst hd]
              let I2 : Nat → Nat → Int := fun u v =>
                (new a.rel u v - old a.rel u v) *
                  integrate n vars (put (put b a.src u) a.dst v)
                    (fun x => crossProd p.atoms old new j x)
              let T : Nat → Int := fun u =>
                ((([b a.dst].filter (fun v => old a.rel u v != new a.rel u v)).map
                  (fun v => I2 u v)).sum)
              change integrate n vars b
                    (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x) =
                ((([b a.src].filter (fun _ => true)).map (fun u => T u)).sum)
              rw [filter_singleton_sum (b a.src) (fun _ => true) T (by intro h; cases h)]
              dsimp only [T]
              rw [filter_singleton_sum (b a.dst)
                    (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v)
                    (I2 (b a.src))
                    (by
                      intro hpf
                      dsimp only [I2]
                      rw [pivot_zero_of_eq a.rel (b a.src) (b a.dst) old new hpf]
                      simp)]
              change integrate n vars b
                    (fun x => (value new a x - value old a x) * crossProd p.atoms old new j x) =
                I2 (b a.src) (b a.dst)
              dsimp only [I2]
              rw [show put (put b a.src (b a.src)) a.dst (b a.dst) = b by
                    rw [put_same, put_same]]
              rw [pivot_delta_factor_aux p n old new b j a (b a.src) (b a.dst)
                    hsd vars hsrc_not hdst_not rfl rfl]

theorem anchored_repair_generic (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (hnd : p.freeVars.Nodup) (hsd : ∀ a ∈ p.atoms, a.src ≠ a.dst) :
    reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
  have hsub : (∀ x, prodAtoms new p.atoms x - prodAtoms old p.atoms x =
      (p.atoms.zipIdx.map (fun (a, i) =>
        (value new a x - value old a x) * crossProd p.atoms old new i x)).sum) := by
    intro x
    rw [prodAtoms_diff]
    have hz := (zipIdx_sum_offset p.atoms
      (fun i a => (value new a x - value old a x) * crossProd p.atoms old new i x) 0).symm
    simpa using hz
  calc
    reference p n new b - reference p n old b
        = integrate n p.freeVars b (fun x => prodAtoms new p.atoms x) -
            integrate n p.freeVars b (fun x => prodAtoms old p.atoms x) := by
              rfl
    _ = integrate n p.freeVars b (fun x => prodAtoms new p.atoms x - prodAtoms old p.atoms x) :=
          (integrate_sub n p.freeVars b (fun x => prodAtoms new p.atoms x)
            (fun x => prodAtoms old p.atoms x)).symm
    _ = integrate n p.freeVars b (fun x => (p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * crossProd p.atoms old new i x)).sum) := by
            apply integrate_congr
            intro x
            rw [hsub x]
    _ = (p.atoms.zipIdx.map (fun (a, i) => integrate n p.freeVars b (fun x =>
          (value new a x - value old a x) * crossProd p.atoms old new i x))).sum := by
            rw [integrate_list_sum]
    _ = (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
            apply list_sum_congr
            intro ai hai
            rcases ai with ⟨a, i⟩
            have hz := List.mem_zipIdx' hai
            have ha_mem : a ∈ p.atoms := by
              rw [hz.2]
              exact List.get_mem p.atoms ⟨i, hz.1⟩
            exact pivot_integral_eq p n old new b i a hnd (hsd a ha_mem)

/-! Namespace MovieCredits: BoundaryMessages -/

namespace MovieCredits
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

theorem TriangleNorm_sound : TriangleNorm.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, normSq, dot, total]
  change (rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      db 0 z x * (db 1 x y * db 2 y z))))) ^ 2 ≤
    (rsum n (fun z => rsum n (fun x => db 0 z x * db 0 z x))) *
    (rsum n (fun x => rsum n (fun y => db 1 x y * db 1 x y))) *
    (rsum n (fun y => rsum n (fun z => db 2 y z * db 2 y z)))
  let A : Nat → Nat → Int := fun z x => db 0 z x
  let B : Nat → Nat → Int := fun x y => db 1 x y
  let C : Nat → Nat → Int := fun y z => db 2 y z
  let E : Nat → Nat → Int := fun y z => rsum n (fun x => A z x * B x y)
  change (rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      A z x * (B x y * C y z))))) ^ 2 ≤
    (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
    (rsum n (fun x => rsum n (fun y => B x y * B x y))) *
    (rsum n (fun y => rsum n (fun z => C y z * C y z)))
  have hT : rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      A z x * (B x y * C y z)))) =
      rsum n (fun y => rsum n (fun z => C y z * E y z)) := by
    calc
      rsum n (fun x => rsum n (fun y => rsum n (fun z => A z x * (B x y * C y z))))
          = rsum n (fun y => rsum n (fun x => rsum n (fun z => A z x * (B x y * C y z)))) := by
              rw [rsum_comm]
      _ = rsum n (fun y => rsum n (fun z => rsum n (fun x => A z x * (B x y * C y z)))) := by
              apply rsum_congr; intro y hy
              rw [rsum_comm]
      _ = rsum n (fun y => rsum n (fun z => C y z * rsum n (fun x => A z x * B x y))) := by
              apply rsum_congr; intro y hy
              apply rsum_congr; intro z hz
              rw [show (fun x => A z x * (B x y * C y z)) =
                  (fun x => C y z * (A z x * B x y)) by
                    funext x; ac_rfl]
              rw [rsum_const_mul]
      _ = rsum n (fun y => rsum n (fun z => C y z * E y z)) := by rfl
  rw [hT]
  have hcs1 : (rsum n (fun y => rsum n (fun z => C y z * E y z))) ^ 2 ≤
      (rsum n (fun y => rsum n (fun z => C y z * C y z))) *
      (rsum n (fun y => rsum n (fun z => E y z * E y z))) := by
    repeat rw [rsum_pairs]
    let ps := (List.range n).flatMap (fun i => (List.range n).map (fun j => (i, j)))
    have h := cs_list (ps.map (fun p : Nat × Nat => (C p.1 p.2, E p.1 p.2)))
    simpa [ps, List.map_map, Function.comp_def] using h
  have hE2 : ∀ y z, (E y z) ^ 2 ≤
      (rsum n (fun x => A z x * A z x)) * (rsum n (fun x => B x y * B x y)) := by
    intro y z
    dsimp only [E]
    exact cauchy_schwarz n (fun x => A z x) (fun x => B x y)
  have hsumE : rsum n (fun y => rsum n (fun z => E y z * E y z)) ≤
      (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
      (rsum n (fun y => rsum n (fun x => B x y * B x y))) := by
    calc
      rsum n (fun y => rsum n (fun z => E y z * E y z))
          ≤ rsum n (fun y => rsum n (fun z =>
              (rsum n (fun x => A z x * A z x)) * (rsum n (fun x => B x y * B x y)))) := by
              apply rsum_le; intro y hy
              apply rsum_le; intro z hz
              have hz' := hE2 y z
              rw [Int.pow_succ, Int.pow_succ] at hz'
              simpa using hz'
      _ = rsum n (fun y => (rsum n (fun x => B x y * B x y)) *
              rsum n (fun z => rsum n (fun x => A z x * A z x))) := by
              apply rsum_congr; intro y hy
              rw [show (fun z => (rsum n (fun x => A z x * A z x)) *
                    (rsum n (fun x => B x y * B x y))) =
                  (fun z => (rsum n (fun x => B x y * B x y)) *
                    (rsum n (fun x => A z x * A z x))) by
                    funext z; ac_rfl]
              rw [rsum_const_mul]
      _ = (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
              rsum n (fun y => rsum n (fun x => B x y * B x y)) := by
              rw [show (fun y => (rsum n (fun x => B x y * B x y)) *
                    (rsum n (fun z => rsum n (fun x => A z x * A z x)))) =
                  (fun y => (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
                    (rsum n (fun x => B x y * B x y))) by
                    funext y; ac_rfl]
              rw [rsum_const_mul]
  have hCnon : 0 ≤ rsum n (fun y => rsum n (fun z => C y z * C y z)) := by
    apply rsum_nonneg; intro y hy
    apply rsum_nonneg; intro z hz
    rw [show C y z * C y z = (C y z) ^ 2 by rw [Int.pow_succ, Int.pow_succ]; simp]
    exact Int.sq_nonneg _
  calc
    (rsum n (fun y => rsum n (fun z => C y z * E y z))) ^ 2
        ≤ (rsum n (fun y => rsum n (fun z => C y z * C y z))) *
            (rsum n (fun y => rsum n (fun z => E y z * E y z))) := hcs1
    _ ≤ (rsum n (fun y => rsum n (fun z => C y z * C y z))) *
          ((rsum n (fun z => rsum n (fun x => A z x * A z x))) *
           (rsum n (fun y => rsum n (fun x => B x y * B x y)))) := by
          exact Int.mul_le_mul_of_nonneg_left hsumE hCnon
    _ = (rsum n (fun z => rsum n (fun x => A z x * A z x))) *
        (rsum n (fun x => rsum n (fun y => B x y * B x y))) *
        (rsum n (fun y => rsum n (fun z => C y z * C y z))) := by
          rw [rsum_comm n n (fun x y => B x y * B x y)]
          ac_rfl

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)

end MovieCredits

/-! Namespace KeywordCompany: TerminalKey -/

namespace KeywordCompany
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany

theorem TerminalKey_sound : TerminalKey.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, dot, total]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3))))) =
    rsum n (fun t => (rsum n (fun k => db 0 (b 0) k * db 2 k t)) *
                      (rsum n (fun k => db 1 (b 0) k * db 3 k t)))
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3)))))
      = rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 =>
        db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3))))) := by
          apply rsum_congr
          intro x1 hx1
          rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x2 =>
        db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3))))) := by
          rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x1 => rsum n (fun x2 =>
        (db 0 (b 0) x1 * db 2 x1 x3) * (db 1 (b 0) x2 * db 3 x2 x3)))) := by
          apply rsum_congr; intro x3 h3
          apply rsum_congr; intro x1 h1
          apply rsum_congr; intro x2 h2
          ac_rfl
    _ = rsum n (fun x3 => (rsum n (fun x1 => db 0 (b 0) x1 * db 2 x1 x3)) *
                           (rsum n (fun x2 => db 1 (b 0) x2 * db 3 x2 x3))) := by
          apply rsum_congr; intro x3 h3
          rw [rsum_product]

end KeywordCompany

/-! Namespace KeywordCompany: BoundaryMessages -/

namespace KeywordCompany
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)

end KeywordCompany

/-! Namespace CastOverlap: BoundaryMessages -/

namespace CastOverlap
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CastOverlap

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

theorem GramSquare_sound : GramSquare.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, dot, total, overlap]
  change rsum n (fun z1 => rsum n (fun z2 => rsum n (fun x => rsum n (fun y =>
      db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) =
    rsum n (fun x => rsum n (fun y => (rsum n (fun k => db 0 x k * db 0 y k)) ^ 2))
  calc
    rsum n (fun z1 => rsum n (fun z2 => rsum n (fun x => rsum n (fun y =>
        db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2))))))
        = rsum n (fun x => rsum n (fun y => rsum n (fun z1 => rsum n (fun z2 =>
            db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
            calc
              rsum n (fun z1 => rsum n (fun z2 => rsum n (fun x => rsum n (fun y =>
                  db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2))))))
                = rsum n (fun z1 => rsum n (fun x => rsum n (fun z2 => rsum n (fun y =>
                    db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
                    exact rsum_swap23 n (fun z1 z2 x => rsum n (fun y =>
                      db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2))))
              _ = rsum n (fun x => rsum n (fun z1 => rsum n (fun z2 => rsum n (fun y =>
                    db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
                    rw [rsum_comm n n (fun z1 x => rsum n (fun z2 => rsum n (fun y =>
                      db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))]
              _ = rsum n (fun x => rsum n (fun z1 => rsum n (fun y => rsum n (fun z2 =>
                    db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
                    apply rsum_congr; intro x hx
                    exact rsum_swap23 n (fun z1 z2 y =>
                      db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))
              _ = rsum n (fun x => rsum n (fun y => rsum n (fun z1 => rsum n (fun z2 =>
                    db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))))) := by
                    apply rsum_congr; intro x hx
                    rw [rsum_comm n n (fun z1 y => rsum n (fun z2 =>
                      db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2))))]
    _ = rsum n (fun x => rsum n (fun y =>
            (rsum n (fun z1 => db 0 x z1 * db 0 y z1)) *
            (rsum n (fun z2 => db 0 y z2 * db 0 x z2)))) := by
            apply rsum_congr; intro x hx
            apply rsum_congr; intro y hy
            rw [show (fun z1 => rsum n (fun z2 =>
                db 0 x z1 * (db 0 y z1 * (db 0 y z2 * db 0 x z2)))) =
                (fun z1 => rsum n (fun z2 =>
                  (db 0 x z1 * db 0 y z1) * (db 0 y z2 * db 0 x z2))) by
                  funext z1
                  apply rsum_congr; intro z2 hz2
                  ac_rfl]
            rw [rsum_product]
    _ = rsum n (fun x => rsum n (fun y => (rsum n (fun k => db 0 x k * db 0 y k)) ^ 2)) := by
            apply rsum_congr; intro x hx
            apply rsum_congr; intro y hy
            rw [show (fun z2 => db 0 y z2 * db 0 x z2) = (fun z2 => db 0 x z2 * db 0 y z2) by
                  funext z2; ac_rfl]
            rw [Int.pow_succ, Int.pow_succ]
            simp

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)

end CastOverlap

/-! Namespace DoubleBilling: BoundaryMessages -/

namespace DoubleBilling
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.DoubleBilling

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

theorem SquaredDelta_sound : SquaredDelta.Goal := by
  intro n db delta b hdelta
  have hd1 : ∀ x y, delta 1 x y = 0 := by intro x y; exact hdelta 1 x y (by decide)
  have hd2 : ∀ x y, delta 2 x y = 0 := by intro x y; exact hdelta 2 x y (by decide)
  simp [reference, integrate, pattern, value, put, slope, hd1, hd2]
  change diff 2 (fun u => rsum n (fun x => rsum n (fun x1 => rsum n (fun x2 =>
      (db 0 (b 0) x + u * delta 0 (b 0) x) *
        ((db 0 (b 0) x + u * delta 0 (b 0) x) * (db 1 x x2 * db 2 (b 0) x1)))))) 0 =
    2 * rsum n (fun x => rsum n (fun x1 => rsum n (fun x2 =>
      delta 0 (b 0) x * (delta 0 (b 0) x * (db 1 x x2 * db 2 (b 0) x1)))))
  rw [diff2_rsum]
  rw [← rsum_const_mul]
  apply rsum_congr
  intro x hx
  rw [diff2_rsum]
  rw [← rsum_const_mul]
  apply rsum_congr
  intro x1 hx1
  rw [diff2_rsum]
  rw [← rsum_const_mul]
  apply rsum_congr
  intro x2 hx2
  rw [show (fun u => (db 0 (b 0) x + u * delta 0 (b 0) x) *
       ((db 0 (b 0) x + u * delta 0 (b 0) x) * (db 1 x x2 * db 2 (b 0) x1))) =
       (fun u => (db 0 (b 0) x + u * delta 0 (b 0) x) *
         (db 0 (b 0) x + u * delta 0 (b 0) x) * (db 1 x x2 * db 2 (b 0) x1)) by
         funext u
         ac_rfl]
  rw [diff2_square_affine]
  ac_rfl

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)

end DoubleBilling

/-! Namespace PostDiamond: BoundaryMessages -/

namespace PostDiamond
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference pattern n db b

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

theorem SquarePathCounts_sound : SquarePathCounts.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, total]
  change rsum n (fun x => rsum n (fun y => rsum n (fun t =>
      db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) =
    rsum n (fun t => (rsum n (fun k => db 0 (b 0) k * db 0 k t)) ^ 2)
  calc
    rsum n (fun x => rsum n (fun y => rsum n (fun t =>
        db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t)))))
        = rsum n (fun t => rsum n (fun x => rsum n (fun y =>
            db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) := by
            calc
              rsum n (fun x => rsum n (fun y => rsum n (fun t =>
                  db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t)))))
                = rsum n (fun y => rsum n (fun x => rsum n (fun t =>
                    db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) := by
                    rw [rsum_comm n n (fun x y => rsum n (fun t =>
                      db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))]
              _ = rsum n (fun y => rsum n (fun t => rsum n (fun x =>
                    db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) := by
                    apply rsum_congr; intro y hy
                    rw [rsum_comm n n (fun x t =>
                      db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t)))]
              _ = rsum n (fun t => rsum n (fun y => rsum n (fun x =>
                    db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) := by
                    rw [rsum_comm n n (fun y t => rsum n (fun x =>
                      db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))]
              _ = rsum n (fun t => rsum n (fun x => rsum n (fun y =>
                    db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))))) := by
                    apply rsum_congr; intro t ht
                    rw [rsum_comm n n (fun y x =>
                      db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t)))]
    _ = rsum n (fun t => rsum n (fun x =>
            (db 0 (b 0) x * db 0 x t) * rsum n (fun y => db 0 (b 0) y * db 0 y t))) := by
            apply rsum_congr; intro t ht
            apply rsum_congr; intro x hx
            rw [show (fun y => db 0 (b 0) x * (db 0 (b 0) y * (db 0 x t * db 0 y t))) =
                (fun y => (db 0 (b 0) x * db 0 x t) * (db 0 (b 0) y * db 0 y t)) by
                  funext y
                  ac_rfl]
            rw [rsum_const_mul]
    _ = rsum n (fun t => (rsum n (fun x => db 0 (b 0) x * db 0 x t)) *
            rsum n (fun y => db 0 (b 0) y * db 0 y t)) := by
            apply rsum_congr; intro t ht
            rw [rsum_mul_const]
    _ = rsum n (fun t => (rsum n (fun k => db 0 (b 0) k * db 0 k t)) ^ 2) := by
            apply rsum_congr; intro t ht
            rw [Int.pow_succ, Int.pow_succ]
            simp

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)

end PostDiamond

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieJoinBoundsAndRepairs
