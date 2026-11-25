import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierClique.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans

private abbrev SCdef : Pattern := TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern
private abbrev SC : Pattern := SCdef

/-! Finite-range sum helpers (Std only). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

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

private theorem rsum_congr_all {f g : Nat → Int} (h : ∀ i, f i = g i) (n : Nat) :
    rsum n f = rsum n g := by
  apply rsum_congr
  intro i hi
  exact h i

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ, ih]
      omega

private theorem rsum_neg (n : Nat) (f : Nat → Int) : rsum n (fun i => - f i) = - rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.neg_add]

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  rw [show (fun i => f i - g i) = (fun i => f i + - g i) by funext i; rw [Int.sub_eq_add_neg]]
  rw [rsum_add, rsum_neg, Int.sub_eq_add_neg]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
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
      exact (rsum_const_zero m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_product (n m : Nat) (f : Nat → Int) (g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n (fun i => f i * rsum m g) := by
      apply rsum_congr
      intro i hi
      rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

/-- Generic list-sum helpers. -/

private theorem list_sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem list_sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem list_sum_neg {α} (l : List α) (f : α → Int) :
    (l.map (fun x => - f x)).sum = - (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp only [List.map_cons, List.sum_cons]; rw [ih]; omega

private theorem list_sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem cast_list_sum (l : List Nat) : ((l.sum : Nat) : Int) = (l.map (fun x : Nat => (x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_put_same (b : Bind) (v x y : Nat) : put (put b v x) v y = put b v y := by
  funext i
  by_cases h : i == v <;> simp [put, h]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
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

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      apply list_sum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
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

private theorem integrate_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [integrate_swap]
      change ((List.range n).map (fun x => integrate n (v :: ws) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum
      exact integrate_congr n [w] b _ _ ih

private theorem integrate_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ [v]) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      change ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n ws (put b w x)
          (fun b' => integrate n [v] b' f))).sum
      apply list_sum_congr
      intro x hx
      exact ih (put b w x)

private theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

private theorem integrate_singleton_rsum (n v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n [v] b f = rsum n (fun x => f (put b v x)) := by rfl

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun x => f x + g x))).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum +
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      rw [show (fun x => integrate n vs (put b v x) (fun x => f x + g x)) =
          (fun x => integrate n vs (put b v x) f + integrate n vs (put b v x) g) by
          funext x; exact ih (put b v x)]
      rw [list_sum_add]

private theorem integrate_neg (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b (fun x => - f x) = - integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun x => - f x))).sum =
        - ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum
      rw [show (fun x => integrate n vs (put b v x) (fun x => - f x)) =
          (fun x => - integrate n vs (put b v x) f) by
          funext x; exact ih (put b v x)]
      rw [list_sum_neg]

private theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  rw [show (fun x => f x - g x) = (fun x => f x + - g x) by funext x; rw [Int.sub_eq_add_neg]]
  rw [show (fun x => f x + - g x) = (fun x => f x + - (g x)) by rfl]
  rw [integrate_add, integrate_neg, Int.sub_eq_add_neg]

private theorem list_sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun _ => (0 : Int)))).sum = 0
      rw [show (fun x => integrate n vs (put b v x) (fun _ => (0 : Int))) =
          (fun _ => (0 : Int)) by
          funext x
          exact ih (put b v x)]
      apply list_sum_zero

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [integrate_add, ih]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

private theorem put_keep_other (b : Bind) (v w x : Nat) (hvw : v ≠ w) :
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

private theorem put_put_ne_left (b : Bind) (v w x y : Nat) (hvw : v ≠ w) :
    put (put b v x) w y v = x := by
  have hvw' : (v == w) = false := by cases h' : (v == w) <;> simp_all
  simp [put, hvw']

private theorem list_sum_filter_eq (l : List Nat) (p : Nat → Bool) (g : Nat → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a = true
      · simp [hp, ih (fun x hx => h x (by simp [hx]))]
      · have hp' : p a = false := by cases h' : p a <;> simp_all
        simp [hp', h a (by simp) hp', ih (fun x hx => h x (by simp [hx]))]

/-- The indicator matrix of a DB. -/

private def ind (db : DB) (r x y : Nat) : Int := if db r x y == 0 then 0 else 1

private theorem ind_zero_one (db : DB) (r x y : Nat) :
    ind db r x y = 0 ∨ ind db r x y = 1 := by
  by_cases h : db r x y == 0 <;> simp [ind, h]

private theorem ind_nonneg (db : DB) (r x y : Nat) : 0 ≤ ind db r x y := by
  rcases ind_zero_one db r x y with h | h <;> simp [h]

private theorem ind_le_one (db : DB) (r x y : Nat) : ind db r x y ≤ 1 := by
  rcases ind_zero_one db r x y with h | h <;> simp [h]

private theorem ind_eq_zero (db : DB) (r x y : Nat) :
    (db r x y != 0) = true ↔ ind db r x y = 1 := by
  by_cases h : db r x y == 0
  · have hn : (db r x y != 0) = false := by cases h' : (db r x y != 0) <;> simp_all
    simp [ind, h, hn]
  · have hn : (db r x y == 0) = false := by cases h' : (db r x y == 0) <;> simp_all
    have hne : (db r x y != 0) = true := by cases h' : (db r x y != 0) <;> simp_all
    simp [ind, h, hne]

private theorem countP_eq_rsum_ind (n : Nat) (db : DB) (r x : Nat) :
    ((List.range n).countP (fun y => db r x y != 0) : Int) = rsum n (fun y => ind db r x y) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [List.range_succ, List.countP_append, List.countP_singleton, rsum_succ]
      rw [show ((List.countP (fun y => db r x y != 0) (List.range n) +
          (if db r x n != 0 then 1 else 0) : Nat) : Int) =
          (List.countP (fun y => db r x y != 0) (List.range n) : Int) +
          ((if db r x n != 0 then 1 else 0 : Nat) : Int) by simp]
      rw [ih]
      by_cases h : db r x n == 0
      · have hn : (db r x n != 0) = false := by cases h' : (db r x n != 0) <;> simp_all
        simp [ind, h, hn]
      · have hn : (db r x n != 0) = true := by cases h' : (db r x n != 0) <;> simp_all
        simp [ind, h, hn]

private theorem mass_eq_rsum_ind (n : Nat) (db : DB) (r : Nat) :
    ((((List.range n).map (fun x => (List.range n).countP (fun y => db r x y != 0))).sum : Nat) : Int) =
      rsum n (fun x => rsum n (fun y => ind db r x y)) := by
  rw [cast_list_sum]
  rw [show (List.map (fun x : Nat => (x : Int)) ((List.range n).map
      (fun x => (List.range n).countP (fun y => db r x y != 0)))) =
      (List.range n).map (fun x => ((List.range n).countP (fun y => db r x y != 0) : Int)) by
      simp [Function.comp_def]]
  apply list_sum_congr
  intro x hx
  exact countP_eq_rsum_ind n db r x

private theorem support_eq_mass (n : Nat) (db : DB) (a : Atom) (b : Bind)
    (hsrc : a.src ∈ SC.freeVars) (hdst : a.dst ∈ SC.freeVars) :
    (support n db a SC b : Int) =
      rsum n (fun x => rsum n (fun y => ind db a.rel x y)) := by
  simp only [support]
  rw [show (if SC.freeVars.contains a.src then List.range n else [b a.src]) = List.range n by
    simp [hsrc]]
  rw [show (if SC.freeVars.contains a.dst then List.range n else [b a.dst]) = List.range n by
    simp [hdst]]
  exact mass_eq_rsum_ind n db a.rel

private theorem reference_indicator_expand (n : Nat) (db : DB) (b : Bind) :
    reference SC n (fun r x y => if db r x y == 0 then 0 else 1) b =
      rsum n (fun a3 => rsum n (fun a2 => rsum n (fun a1 => rsum n (fun a0 =>
        ind db 0 a0 a1 * ind db 0 a0 a2 * ind db 0 a0 a3 *
        ind db 0 a1 a2 * ind db 0 a1 a3 * ind db 0 a2 a3)))) := by
  unfold reference
  change integrate n [3, 2, 1, 0] b
      (fun x => ([⟨0,0,1⟩, ⟨0,0,2⟩, ⟨0,0,3⟩, ⟨0,1,2⟩, ⟨0,1,3⟩, ⟨0,2,3⟩].map
        (fun a => value (fun r x y => if db r x y == 0 then 0 else 1) a x)).prod) = _
  simp [integrate, ind, value, put, rsum, Int.mul_assoc]

private theorem ind_prod_le_pair (db : DB) (a0 a1 a2 a3 : Nat) :
    ind db 0 a0 a1 * ind db 0 a0 a2 * ind db 0 a0 a3 *
        ind db 0 a1 a2 * ind db 0 a1 a3 * ind db 0 a2 a3 ≤
      ind db 0 a0 a1 * ind db 0 a2 a3 := by
  rcases ind_zero_one db 0 a0 a1 with h01 | h01 <;>
  rcases ind_zero_one db 0 a0 a2 with h02 | h02 <;>
  rcases ind_zero_one db 0 a0 a3 with h03 | h03 <;>
  rcases ind_zero_one db 0 a1 a2 with h12 | h12 <;>
  rcases ind_zero_one db 0 a1 a3 with h13 | h13 <;>
  rcases ind_zero_one db 0 a2 a3 with h23 | h23 <;>
  simp [h01, h02, h03, h12, h13, h23]

private theorem reference_indicator_le_support_sq (n : Nat) (db : DB) (b : Bind) :
    reference SC n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      (support n db ⟨0, 0, 1⟩ SC b : Int) *
        (support n db ⟨0, 2, 3⟩ SC b : Int) := by
  rw [reference_indicator_expand]
  have hle : rsum n (fun a3 => rsum n (fun a2 => rsum n (fun a1 => rsum n (fun a0 =>
        ind db 0 a0 a1 * ind db 0 a0 a2 * ind db 0 a0 a3 *
        ind db 0 a1 a2 * ind db 0 a1 a3 * ind db 0 a2 a3)))) ≤
      rsum n (fun a3 => rsum n (fun a2 => rsum n (fun a1 => rsum n (fun a0 =>
        ind db 0 a0 a1 * ind db 0 a2 a3)))) := by
    apply rsum_le; intro a3 ha3
    apply rsum_le; intro a2 ha2
    apply rsum_le; intro a1 ha1
    apply rsum_le; intro a0 ha0
    exact ind_prod_le_pair db a0 a1 a2 a3
  calc
    rsum n (fun a3 => rsum n (fun a2 => rsum n (fun a1 => rsum n (fun a0 =>
        ind db 0 a0 a1 * ind db 0 a0 a2 * ind db 0 a0 a3 *
        ind db 0 a1 a2 * ind db 0 a1 a3 * ind db 0 a2 a3))))
        ≤ rsum n (fun a3 => rsum n (fun a2 => rsum n (fun a1 => rsum n (fun a0 =>
            ind db 0 a0 a1 * ind db 0 a2 a3)))) := hle
    _ = rsum n (fun a3 => rsum n (fun a2 =>
          (rsum n (fun a1 => rsum n (fun a0 => ind db 0 a0 a1))) * ind db 0 a2 a3)) := by
          apply rsum_congr; intro a3 ha3
          apply rsum_congr; intro a2 ha2
          rw [show (fun a1 => rsum n (fun a0 => ind db 0 a0 a1 * ind db 0 a2 a3)) =
              (fun a1 => rsum n (fun a0 => ind db 0 a0 a1) * ind db 0 a2 a3) by
              funext a1
              rw [rsum_mul_const]]
          rw [rsum_mul_const]
    _ = (rsum n (fun a1 => rsum n (fun a0 => ind db 0 a0 a1))) *
          rsum n (fun a2 => rsum n (fun a3 => ind db 0 a2 a3)) := by
          rw [rsum_comm n n (fun a3 a2 =>
              (rsum n (fun a1 => rsum n (fun a0 => ind db 0 a0 a1))) * ind db 0 a2 a3)]
          rw [show rsum n (fun a2 => rsum n (fun a3 =>
                (rsum n (fun a1 => rsum n (fun a0 => ind db 0 a0 a1))) * ind db 0 a2 a3)) =
              rsum n (fun a2 => (rsum n (fun a1 => rsum n (fun a0 => ind db 0 a0 a1))) *
                rsum n (fun a3 => ind db 0 a2 a3)) by
                apply rsum_congr; intro a2 ha2
                rw [rsum_const_mul]]
          rw [rsum_const_mul]
    _ = (rsum n (fun x => rsum n (fun y => ind db 0 x y))) *
          (rsum n (fun x => rsum n (fun y => ind db 0 x y))) := by
          rw [show rsum n (fun a1 => rsum n (fun a0 => ind db 0 a0 a1)) =
              rsum n (fun x => rsum n (fun y => ind db 0 x y)) by rw [rsum_comm]]
    _ = (support n db ⟨0, 0, 1⟩ SC b : Int) *
          (support n db ⟨0, 2, 3⟩ SC b : Int) := by
          rw [show (support n db ⟨0, 0, 1⟩ SC b : Int) =
              rsum n (fun x => rsum n (fun y => ind db 0 x y)) by
                exact support_eq_mass n db ⟨0, 0, 1⟩ b (by native_decide) (by native_decide)]
          rw [show (support n db ⟨0, 2, 3⟩ SC b : Int) =
              rsum n (fun x => rsum n (fun y => ind db 0 x y)) by
                exact support_eq_mass n db ⟨0, 2, 3⟩ b (by native_decide) (by native_decide)]

private theorem reference_indicator_nonneg (n : Nat) (db : DB) (b : Bind) :
    0 ≤ reference SC n (fun r x y => if db r x y == 0 then 0 else 1) b := by
  rw [reference_indicator_expand]
  apply rsum_nonneg; intro a3 ha3
  apply rsum_nonneg; intro a2 ha2
  apply rsum_nonneg; intro a1 ha1
  apply rsum_nonneg; intro a0 ha0
  rcases ind_zero_one db 0 a0 a1 with h01 | h01 <;>
  rcases ind_zero_one db 0 a0 a2 with h02 | h02 <;>
  rcases ind_zero_one db 0 a0 a3 with h03 | h03 <;>
  rcases ind_zero_one db 0 a1 a2 with h12 | h12 <;>
  rcases ind_zero_one db 0 a1 a3 with h13 | h13 <;>
  rcases ind_zero_one db 0 a2 a3 with h23 | h23 <;>
  simp [h01, h02, h03, h12, h13, h23]

private theorem pow_mono (a : Nat) (ha : 1 ≤ a) : ∀ {k l : Nat}, k ≤ l → a ^ k ≤ a ^ l := by
  intro k l hkl
  obtain ⟨d, hd⟩ := Nat.exists_eq_add_of_le hkl
  subst l
  rw [Nat.pow_add]
  exact Nat.le_mul_of_pos_right _ (Nat.pow_pos ha)

private theorem cover_sum_weights (weights : List Nat) (den : Nat)
    (hcov : cover SC weights den) : 2 * den ≤ weights.sum := by
  unfold cover at hcov
  rcases hcov with ⟨hden, hlen, hc⟩
  have hlen6 : SC.atoms.length = 6 := by native_decide
  have hwlen : weights.length = 6 := by rw [hlen, hlen6]
  cases weights with
  | nil => simp at hwlen
  | cons w0 ws1 =>
    cases ws1 with
    | nil => simp at hwlen
    | cons w1 ws2 =>
      cases ws2 with
      | nil => simp at hwlen
      | cons w2 ws3 =>
        cases ws3 with
        | nil => simp at hwlen
        | cons w3 ws4 =>
          cases ws4 with
          | nil => simp at hwlen
          | cons w4 ws5 =>
            cases ws5 with
            | nil => simp at hwlen
            | cons w5 ws6 =>
              cases ws6 with
              | nil =>
                  have h3 := hc 3 (by native_decide)
                  have h2 := hc 2 (by native_decide)
                  have h1 := hc 1 (by native_decide)
                  have h0 := hc 0 (by native_decide)
                  simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] at h3 h2 h1 h0
                  simp
                  omega
              | cons w6 ws7 =>
                  simp at hwlen

private theorem prod_pow_eq_pow_sum (m : Nat) : ∀ ws : List Nat,
    (ws.map (fun w => m ^ w)).prod = m ^ ws.sum := by
  intro ws
  induction ws with
  | nil => simp
  | cons w ws ih => simp [ih, Nat.pow_add]

/- FractionalCap for the four-vertex clique. -/

namespace SupplierClique
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique

theorem sound_FractionalCap : FractionalCap.Goal := by
  intro n db b weights den hcov
  let c : Int := reference SC n (fun r x y => if db r x y == 0 then 0 else 1) b
  let s : Nat := support n db (⟨0, 0, 1⟩) SC b
  have hs5 : support n db ⟨0, 2, 3⟩ SC b = s := by
    dsimp only [s]
    simp only [support]
    simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]
  have hle : c ≤ (s * s : Nat) := by
    dsimp only [c, s]
    simpa [hs5] using reference_indicator_le_support_sq n db b
  have hnon : 0 ≤ c := by
    dsimp only [c]
    exact reference_indicator_nonneg n db b
  have hcnat : c.toNat ≤ s * s := (Int.toNat_le).mpr (by exact_mod_cast hle)
  have hcovsum : 2 * den ≤ weights.sum := cover_sum_weights weights den hcov
  have hrhs : ((SC.atoms.zip weights).map (fun (a, w) =>
      support n db a SC b ^ w)).prod = s ^ weights.sum := by
    have hsup : ∀ a ∈ SC.atoms, support n db a SC b = s := by
      intro a ha
      have ha' : a = ⟨0,0,1⟩ ∨ a = ⟨0,0,2⟩ ∨ a = ⟨0,0,3⟩ ∨
          a = ⟨0,1,2⟩ ∨ a = ⟨0,1,3⟩ ∨ a = ⟨0,2,3⟩ := by
        simpa [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] using ha
      rcases ha' with rfl | rfl | rfl | rfl | rfl | rfl <;>
        simp [s, support, SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]
    have hzip : ∀ (ws : List Nat), ws.length = 6 →
        (SC.atoms.zip ws).map (fun (a, w) => support n db a SC b ^ w) =
          ws.map (fun w => s ^ w) := by
      intro ws hwlen
      cases ws with
      | nil => simp at hwlen
      | cons w0 ws1 =>
        cases ws1 with
        | nil => simp at hwlen
        | cons w1 ws2 =>
          cases ws2 with
          | nil => simp at hwlen
          | cons w2 ws3 =>
            cases ws3 with
            | nil => simp at hwlen
            | cons w3 ws4 =>
              cases ws4 with
              | nil => simp at hwlen
              | cons w4 ws5 =>
                cases ws5 with
                | nil => simp at hwlen
                | cons w5 ws6 =>
                  cases ws6 with
                  | nil =>
                    simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]
                    refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
                    · simpa [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] using congrArg (fun z : Nat => z ^ w0) (hsup ⟨0,0,1⟩ (by simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]))
                    · simpa [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] using congrArg (fun z : Nat => z ^ w1) (hsup ⟨0,0,2⟩ (by simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]))
                    · simpa [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] using congrArg (fun z : Nat => z ^ w2) (hsup ⟨0,0,3⟩ (by simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]))
                    · simpa [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] using congrArg (fun z : Nat => z ^ w3) (hsup ⟨0,1,2⟩ (by simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]))
                    · simpa [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] using congrArg (fun z : Nat => z ^ w4) (hsup ⟨0,1,3⟩ (by simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]))
                    · simpa [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern] using congrArg (fun z : Nat => z ^ w5) (hsup ⟨0,2,3⟩ (by simp [SC, SCdef, TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique.pattern]))
                  | cons w6 ws7 => simp at hwlen
    have hlen : weights.length = SC.atoms.length := by
      have hcov' := hcov
      unfold cover at hcov'
      exact hcov'.2.1
    have hlen6atoms : SC.atoms.length = 6 := by native_decide
    rw [hzip weights (by omega)]
    exact prod_pow_eq_pow_sum s weights
  change c.toNat ^ den ≤ ((SC.atoms.zip weights).map
      (fun (a, w) => support n db a SC b ^ w)).prod
  by_cases hs0 : s = 0
  · have hc0 : c.toNat = 0 := by
      have : c.toNat ≤ 0 := by simpa [hs0] using hcnat
      omega
    rw [hrhs, hc0]
    simp [hs0, hcov.1]
  · have hs1 : 1 ≤ s := Nat.succ_le_of_lt (Nat.pos_of_ne_zero hs0)
    calc
      c.toNat ^ den ≤ (s * s) ^ den := Nat.pow_le_pow_left hcnat den
      _ = s ^ (2 * den) := by
        rw [show s * s = s ^ 2 by rw [Nat.pow_succ]; simp]
        rw [← Nat.pow_mul]
      _ ≤ s ^ weights.sum := pow_mono _ hs1 hcovsum
      _ = ((SC.atoms.zip weights).map (fun (a, w) =>
          support n db a SC b ^ w)).prod := hrhs.symm

end SupplierClique

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierClique.Remaining
