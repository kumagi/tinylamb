import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `HubWithChords.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsFractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ---------------------------------------------------------------------------
    Finite range sums and a natural-number mirror of `integrate`.
   --------------------------------------------------------------------------- -/

private def rsum (n : Nat) (f : Nat → Nat) : Nat := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Nat) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Nat) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Nat} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => 0) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]

private theorem rsum_add (n : Nat) (f g : Nat → Nat) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_mul_left (n : Nat) (c : Nat) (f : Nat → Nat) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Nat.mul_add]

private theorem rsum_mul_right (n : Nat) (f : Nat → Nat) (c : Nat) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Nat.add_mul]

private theorem rsum_le (n : Nat) (f g : Nat → Nat) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      exact Nat.add_le_add (ih (fun i hi => h i (Nat.lt_succ_of_lt hi))) (h n (Nat.lt_succ_self n))

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Nat) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => 0) by
        apply rsum_congr; intro j hj; rw [rsum_zero]]
      rw [rsum_zero_const]
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_le_mul_of_le (n : Nat) (f g : Nat → Nat) (c : Nat)
    (h : ∀ i, i < n → f i ≤ c * g i) : rsum n f ≤ c * rsum n g := by
  rw [← rsum_mul_left]
  exact rsum_le n f (fun i => c * g i) h

private theorem foldl_max_cons (x : Nat) (l : List Nat) :
    (x :: l).foldl Nat.max 0 = Nat.max x (l.foldl Nat.max 0) := by
  change l.foldl Nat.max (Nat.max 0 x) = Nat.max x (l.foldl Nat.max 0)
  simp only [Nat.zero_max]
  induction l generalizing x with
  | nil => simp
  | cons y ys ih =>
      rw [List.foldl_cons, ih (Nat.max x y), List.foldl_cons]
      rw [show Nat.max 0 y = y by simp]
      rw [ih y]
      ac_rfl

private theorem list_max_le {α} (f : α → Nat) (l : List α) (B : Nat)
    (h : ∀ a ∈ l, f a ≤ B) : (l.map f).foldl Nat.max 0 ≤ B := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [List.map_cons, foldl_max_cons]
      exact Nat.max_le.mpr ⟨h a (by simp), ih (fun x hx => h x (by simp [hx]))⟩

private theorem le_foldl_max {α} (f : α → Nat) (l : List α) (a : α) (ha : a ∈ l) :
    f a ≤ (l.map f).foldl Nat.max 0 := by
  induction l with
  | nil => simp at ha
  | cons b l ih =>
      rw [List.map_cons, foldl_max_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · exact Nat.le_max_left _ _
      · exact Nat.le_trans (ih ha) (Nat.le_max_right _ _)

private def iN (n : Nat) : List Nat → (Nat → Nat) → ((Nat → Nat) → Nat) → Nat
  | [], b, f => f b
  | v :: vs, b, f => rsum n (fun x => iN n vs (put b v x) f)

private theorem iN_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Nat)
    (h : ∀ x, f x = g x) : iN n vs b f = iN n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      simp only [iN]
      apply rsum_congr
      intro x hx
      exact ih (put b v x)

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

private theorem iN_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Nat) :
    iN n (v :: w :: vs) b f = iN n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by cases h' : (v == w) <;> simp_all
    simp only [iN]
    rw [rsum_comm]
    apply rsum_congr
    intro x hx
    apply rsum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem iN_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Nat) :
    iN n (v :: vs) b f = iN n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [iN_swap]
      simp only [iN]
      apply rsum_congr
      intro x hx
      exact ih (put b w x)

private theorem iN_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Nat) :
    iN n (vs ++ [v]) b f = iN n vs b (fun b' => iN n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      simp only [iN]
      apply rsum_congr
      intro x hx
      exact ih (put b w x)

private theorem iN_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Nat) :
    iN n (v :: vs) b f = iN n vs b (fun b' => iN n [v] b' f) := by
  rw [iN_rotate, iN_snoc]

private theorem int_sum_cast (l : List Nat) : ((l.sum : Nat) : Int) =
    (l.map (fun x : Nat => (x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem int_list_sum_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ a ∈ l, f a = g a) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem iN_cast (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Nat) :
    integrate n vs b (fun x => (f x : Int)) = (iN n vs b f : Int) := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun y => (f y : Int)))).sum =
        (((List.range n).map (fun x => iN n vs (put b v x) f)).sum : Int)
      rw [int_sum_cast, List.map_map]
      apply int_list_sum_congr
      intro x hx
      rw [ih]
      rfl

private theorem iN_rotate_at (n : Nat) (pre : List Nat) (v : Nat) (vs : List Nat)
    (b : Bind) (f : Bind → Nat) :
    iN n (pre ++ [v] ++ vs) b f = iN n (pre ++ vs ++ [v]) b f := by
  induction pre generalizing b with
  | nil => simpa using iN_rotate n v vs b f
  | cons u pre ih =>
      simp only [List.cons_append, iN]
      apply rsum_congr
      intro x hx
      exact ih (put b u x)

private theorem iN_swap_at (n : Nat) (pre : List Nat) (v w : Nat) (vs : List Nat)
    (b : Bind) (f : Bind → Nat) :
    iN n (pre ++ [v, w] ++ vs) b f = iN n (pre ++ [w, v] ++ vs) b f := by
  induction pre generalizing b with
  | nil => simpa using iN_swap n v w vs b f
  | cons u pre ih =>
      simp only [List.cons_append, iN]
      apply rsum_congr
      intro x hx
      exact ih (put b u x)

private theorem countP_eq_sum_ind (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a <;> simp [h, ih] <;> omega

private theorem rsum_factor5 (n : Nat) (F : Nat → Nat) (Q : Nat → Nat → Nat)
    (R : Nat → Nat → Nat) :
    rsum n (fun x5 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => F x5 * Q x1 x2 * R x3 x4))))) =
      (rsum n F) * (rsum n (fun x1 => rsum n (fun x2 => Q x1 x2))) *
        (rsum n (fun x3 => rsum n (fun x4 => R x3 x4))) := by
  let V : Nat → Nat := fun x3 => rsum n (fun x4 => R x3 x4)
  let U : Nat → Nat := fun x1 => rsum n (fun x2 => Q x1 x2)
  let Vtot : Nat := rsum n V
  let Utot : Nat := rsum n U
  have h4 : ∀ x5 x1 x2 x3,
      rsum n (fun x4 => F x5 * Q x1 x2 * R x3 x4) = (F x5 * Q x1 x2) * V x3 := by
    intro x5 x1 x2 x3
    dsimp [V]
    rw [rsum_mul_left]
  have h3 : ∀ x5 x1 x2,
      rsum n (fun x3 => (F x5 * Q x1 x2) * V x3) = (F x5 * Q x1 x2) * Vtot := by
    intro x5 x1 x2
    dsimp [Vtot]
    rw [rsum_mul_left]
  have h2 : ∀ x5 x1,
      rsum n (fun x2 => (F x5 * Q x1 x2) * Vtot) = (F x5 * U x1) * Vtot := by
    intro x5 x1
    dsimp [U]
    rw [show (fun x2 => (F x5 * Q x1 x2) * Vtot) = (fun x2 => F x5 * Q x1 x2 * Vtot) by rfl]
    rw [rsum_mul_right, rsum_mul_left]
  have h1 : ∀ x5, rsum n (fun x1 => (F x5 * U x1) * Vtot) = (F x5 * Utot) * Vtot := by
    intro x5
    dsimp [Utot]
    rw [show (fun x1 => (F x5 * U x1) * Vtot) = (fun x1 => F x5 * U x1 * Vtot) by rfl]
    rw [rsum_mul_right, rsum_mul_left]
  calc
    rsum n (fun x5 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => F x5 * Q x1 x2 * R x3 x4)))))
        = rsum n (fun x5 => rsum n (fun x1 => rsum n (fun x2 =>
            rsum n (fun x3 => (F x5 * Q x1 x2) * V x3)))) := by
              apply rsum_congr; intro x5 hx5
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              apply rsum_congr; intro x3 hx3
              exact h4 x5 x1 x2 x3
    _ = rsum n (fun x5 => rsum n (fun x1 => rsum n (fun x2 =>
            (F x5 * Q x1 x2) * Vtot))) := by
              apply rsum_congr; intro x5 hx5
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              exact h3 x5 x1 x2
    _ = rsum n (fun x5 => rsum n (fun x1 => (F x5 * U x1) * Vtot)) := by
              apply rsum_congr; intro x5 hx5
              apply rsum_congr; intro x1 hx1
              exact h2 x5 x1
    _ = rsum n (fun x5 => (F x5 * Utot) * Vtot) := by
              apply rsum_congr; intro x5 hx5
              exact h1 x5
    _ = (rsum n F) * Utot * Vtot := by
              rw [show (fun x5 => (F x5 * Utot) * Vtot) = (fun x5 => F x5 * (Utot * Vtot)) by
                funext x5; rw [Nat.mul_assoc]]
              rw [rsum_mul_right]
              rw [← Nat.mul_assoc]
    _ = (rsum n F) * (rsum n (fun x1 => rsum n (fun x2 => Q x1 x2))) *
        (rsum n (fun x3 => rsum n (fun x4 => R x3 x4))) := by rfl

private theorem pow_le_pow_left' {a b : Nat} (h : a ≤ b) (k : Nat) : a^k ≤ b^k :=
  Nat.pow_le_pow_left h k

private theorem pow_le_of_le_exp {x : Nat} (hx : 1 ≤ x) {m k : Nat} (hmk : m ≤ k) :
    x^m ≤ x^k := by
  obtain ⟨t, rfl⟩ := Nat.exists_eq_add_of_le hmk
  rw [Nat.pow_add]
  exact Nat.le_mul_of_pos_right _ (Nat.pow_pos hx)

private theorem pow_le_pow_of_le_of_le_exp {a b : Nat} (hab : a ≤ b) (hb : 1 ≤ b)
    {m k : Nat} (hmk : m ≤ k) : a^m ≤ b^k := by
  exact Nat.le_trans (pow_le_pow_left' hab m) (pow_le_of_le_exp hb hmk)

/-! ---------------------------------------------------------------------------
    Fractional edge-cover bound for the hub-with-chords pattern.
   --------------------------------------------------------------------------- -/

private def hubInd (db : DB) (r x y : Nat) : Nat := if db r x y == 0 then 0 else 1

private def hubQ1 (db : DB) (x0 x1 x2 : Nat) : Nat :=
  hubInd db 0 x0 x1 * hubInd db 1 x0 x2 * hubInd db 5 x1 x2

private def hubQ2 (db : DB) (x0 x3 x4 : Nat) : Nat :=
  hubInd db 2 x0 x3 * hubInd db 3 x0 x4 * hubInd db 6 x3 x4

private def hubG (db : DB) (x : Bind) : Nat :=
  hubInd db 0 (x 0) (x 1) * hubInd db 1 (x 0) (x 2) *
  hubInd db 2 (x 0) (x 3) * hubInd db 3 (x 0) (x 4) *
  hubInd db 4 (x 0) (x 5) * hubInd db 5 (x 1) (x 2) *
  hubInd db 6 (x 3) (x 4)

private theorem hubG_put (db : DB) (b : Bind) (x0 x5 x1 x2 x3 x4 : Nat) :
    hubG db (put (put (put (put (put (put b 0 x0) 5 x5) 1 x1) 2 x2) 3 x3) 4 x4) =
      hubInd db 4 x0 x5 * hubQ1 db x0 x1 x2 * hubQ2 db x0 x3 x4 := by
  simp [hubG, hubQ1, hubQ2, put]
  ac_rfl

private theorem ref_reorder (n : Nat) (db : DB) (b : Bind) :
    iN n [1, 2, 5, 4, 3, 0] b (hubG db) = iN n [0, 5, 1, 2, 3, 4] b (hubG db) := by
  calc
    iN n [1, 2, 5, 4, 3, 0] b (hubG db)
        = iN n [2, 5, 4, 3, 0, 1] b (hubG db) := by
            simpa using iN_rotate_at n [] 1 [2, 5, 4, 3, 0] b (hubG db)
    _ = iN n [5, 4, 3, 0, 1, 2] b (hubG db) := by
            simpa using iN_rotate_at n [] 2 [5, 4, 3, 0, 1] b (hubG db)
    _ = iN n [4, 3, 0, 1, 2, 5] b (hubG db) := by
            simpa using iN_rotate_at n [] 5 [4, 3, 0, 1, 2] b (hubG db)
    _ = iN n [3, 0, 1, 2, 5, 4] b (hubG db) := by
            simpa using iN_rotate_at n [] 4 [3, 0, 1, 2, 5] b (hubG db)
    _ = iN n [0, 1, 2, 5, 4, 3] b (hubG db) := by
            simpa using iN_rotate_at n [] 3 [0, 1, 2, 5, 4] b (hubG db)
    _ = iN n [0, 2, 5, 4, 3, 1] b (hubG db) := by
            simpa using iN_rotate_at n [0] 1 [2, 5, 4, 3] b (hubG db)
    _ = iN n [0, 5, 4, 3, 1, 2] b (hubG db) := by
            simpa using iN_rotate_at n [0] 2 [5, 4, 3, 1] b (hubG db)
    _ = iN n [0, 5, 3, 1, 2, 4] b (hubG db) := by
            simpa using iN_rotate_at n [0, 5] 4 [3, 1, 2] b (hubG db)
    _ = iN n [0, 5, 1, 2, 4, 3] b (hubG db) := by
            simpa using iN_rotate_at n [0, 5] 3 [1, 2, 4] b (hubG db)
    _ = iN n [0, 5, 1, 2, 3, 4] b (hubG db) := by
            simpa using iN_swap_at n [0, 5, 1, 2] 4 3 [] b (hubG db)

private theorem ref_factor (n : Nat) (db : DB) (b : Bind) :
    iN n [1, 2, 5, 4, 3, 0] b (hubG db) =
      rsum n (fun x0 => (rsum n (fun x5 => hubInd db 4 x0 x5)) *
        (rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2))) *
        (rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4)))) := by
  rw [ref_reorder]
  change rsum n (fun x0 => iN n [5, 1, 2, 3, 4] (put b 0 x0) (hubG db)) = _
  apply rsum_congr
  intro x0 hx0
  simp only [iN]
  simp only [hubG_put]
  exact rsum_factor5 n (fun x5 => hubInd db 4 x0 x5)
    (fun x1 x2 => hubQ1 db x0 x1 x2) (fun x3 x4 => hubQ2 db x0 x3 x4)

private theorem ind_eq (db : DB) (r x y : Nat) :
    (if db r x y == 0 then (0 : Int) else 1) = (hubInd db r x y : Int) := by
  by_cases h : db r x y == 0 <;> simp [hubInd, h]

private theorem ref_integrand (db : DB) (x : Bind) :
    (List.map (fun a : Atom =>
      value (fun r u v => if db r u v == 0 then 0 else 1) a x) pattern.atoms).prod =
      (hubG db x : Int) := by
  simp only [pattern, value, List.map_cons, List.map_nil, List.prod_cons, List.prod_nil]
  rw [ind_eq db 0 (x 0) (x 1), ind_eq db 1 (x 0) (x 2),
    ind_eq db 2 (x 0) (x 3), ind_eq db 3 (x 0) (x 4),
    ind_eq db 4 (x 0) (x 5), ind_eq db 5 (x 1) (x 2),
    ind_eq db 6 (x 3) (x 4)]
  simp [hubG]
  ac_rfl

private theorem reference_toNat (n : Nat) (db : DB) (b : Bind) :
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat =
      iN n [1, 2, 5, 4, 3, 0] b (hubG db) := by
  unfold reference
  have hfun : (fun x : Bind => (List.map (fun a : Atom =>
      value (fun r u v => if db r u v == 0 then 0 else 1) a x) pattern.atoms).prod) =
      (fun x : Bind => (hubG db x : Int)) := by
    funext x
    exact ref_integrand db x
  rw [hfun]
  rw [iN_cast]
  simp [pattern]

private theorem support_hub (n : Nat) (db : DB) (b : Bind) (r s d : Nat)
    (hs : s ∈ pattern.freeVars) (hd : d ∈ pattern.freeVars) :
    support n db ⟨r, s, d⟩ pattern b =
      rsum n (fun x => rsum n (fun y => hubInd db r x y)) := by
  unfold support
  rw [show pattern.freeVars = [1, 2, 5, 4, 3, 0] by rfl]
  have hs' : s = 1 ∨ s = 2 ∨ s = 5 ∨ s = 4 ∨ s = 3 ∨ s = 0 := by simpa [pattern] using hs
  have hd' : d = 1 ∨ d = 2 ∨ d = 5 ∨ d = 4 ∨ d = 3 ∨ d = 0 := by simpa [pattern] using hd
  simp [hs', hd', countP_eq_sum_ind, rsum, hubInd]

private theorem le_sum_of_mem {α} (l : List α) (f : α → Nat) (a : α) (ha : a ∈ l) :
    f a ≤ (l.map f).sum := by
  induction l with
  | nil => simp at ha
  | cons b l ih =>
      simp only [List.map_cons, List.sum_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · exact Nat.le_add_right _ _
      · exact Nat.le_trans (ih ha) (Nat.le_add_left _ _)

private theorem le_rsum_of_mem (n : Nat) (f : Nat → Nat) (i : Nat) (hi : i < n) :
    f i ≤ rsum n f := by
  unfold rsum
  exact le_sum_of_mem (List.range n) f i (List.mem_range.mpr hi)

private theorem rsum_eq_zero_iff (n : Nat) (f : Nat → Nat) :
    rsum n f = 0 ↔ ∀ i, i < n → f i = 0 := by
  constructor
  · intro h i hi
    have hle := le_rsum_of_mem n f i hi
    omega
  · intro h
    calc
      rsum n f = rsum n (fun _ => 0) := by
        apply rsum_congr
        intro i hi
        exact h i hi
      _ = 0 := rsum_zero_const n

private theorem rsum_product (n : Nat) (f g : Nat → Nat) :
    rsum n (fun i => rsum n (fun j => f i * g j)) = rsum n f * rsum n g := by
  calc
    rsum n (fun i => rsum n (fun j => f i * g j))
        = rsum n (fun i => f i * rsum n g) := by
            apply rsum_congr; intro i hi
            rw [rsum_mul_left]
    _ = rsum n f * rsum n g := by rw [rsum_mul_right]

private theorem ind_nonneg_one (db : DB) (r x y : Nat) : hubInd db r x y ≤ 1 := by
  by_cases h : db r x y == 0 <;> simp [hubInd, h]

private theorem hubQ1_le_a5 (db : DB) (x0 x1 x2 : Nat) :
    hubQ1 db x0 x1 x2 ≤ hubInd db 5 x1 x2 := by
  have hab : hubInd db 0 x0 x1 * hubInd db 1 x0 x2 ≤ 1 :=
    Nat.mul_le_mul (ind_nonneg_one db 0 x0 x1) (ind_nonneg_one db 1 x0 x2)
  simpa [hubQ1] using Nat.mul_le_mul_right (hubInd db 5 x1 x2) hab

private theorem hubQ1_le_a0a1 (db : DB) (x0 x1 x2 : Nat) :
    hubQ1 db x0 x1 x2 ≤ hubInd db 0 x0 x1 * hubInd db 1 x0 x2 := by
  have hc : hubInd db 5 x1 x2 ≤ 1 := ind_nonneg_one db 5 x1 x2
  simpa [hubQ1] using Nat.mul_le_mul_left (hubInd db 0 x0 x1 * hubInd db 1 x0 x2) hc

private theorem hubQ2_le_a6 (db : DB) (x0 x3 x4 : Nat) :
    hubQ2 db x0 x3 x4 ≤ hubInd db 6 x3 x4 := by
  have hab : hubInd db 2 x0 x3 * hubInd db 3 x0 x4 ≤ 1 :=
    Nat.mul_le_mul (ind_nonneg_one db 2 x0 x3) (ind_nonneg_one db 3 x0 x4)
  simpa [hubQ2] using Nat.mul_le_mul_right (hubInd db 6 x3 x4) hab

private theorem hubQ2_le_a2a3 (db : DB) (x0 x3 x4 : Nat) :
    hubQ2 db x0 x3 x4 ≤ hubInd db 2 x0 x3 * hubInd db 3 x0 x4 := by
  have hc : hubInd db 6 x3 x4 ≤ 1 := ind_nonneg_one db 6 x3 x4
  simpa [hubQ2] using Nat.mul_le_mul_left (hubInd db 2 x0 x3 * hubInd db 3 x0 x4) hc

private theorem U_le_s5 (n : Nat) (db : DB) (x0 : Nat) :
    rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2)) ≤
      rsum n (fun x1 => rsum n (fun y => hubInd db 5 x1 y)) := by
  apply rsum_le
  intro x1 hx1
  apply rsum_le
  intro x2 hx2
  exact hubQ1_le_a5 db x0 x1 x2

private theorem U_le_s0s1 (n : Nat) (db : DB) (x0 : Nat) (hx0 : x0 < n) :
    rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2)) ≤
      (rsum n (fun x => rsum n (fun y => hubInd db 0 x y))) *
      (rsum n (fun x => rsum n (fun y => hubInd db 1 x y))) := by
  have h1 : rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2)) ≤
      rsum n (fun x1 => rsum n (fun x2 =>
        hubInd db 0 x0 x1 * hubInd db 1 x0 x2)) := by
    apply rsum_le; intro x1 hx1
    apply rsum_le; intro x2 hx2
    exact hubQ1_le_a0a1 db x0 x1 x2
  rw [rsum_product] at h1
  have hrow0 : rsum n (fun x1 => hubInd db 0 x0 x1) ≤
      rsum n (fun x => rsum n (fun y => hubInd db 0 x y)) :=
    le_rsum_of_mem n (fun x => rsum n (fun y => hubInd db 0 x y)) x0 hx0
  have hrow1 : rsum n (fun x2 => hubInd db 1 x0 x2) ≤
      rsum n (fun x => rsum n (fun y => hubInd db 1 x y)) :=
    le_rsum_of_mem n (fun x => rsum n (fun y => hubInd db 1 x y)) x0 hx0
  exact Nat.le_trans h1 (Nat.mul_le_mul hrow0 hrow1)

private theorem V_le_s6 (n : Nat) (db : DB) (x0 : Nat) :
    rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4)) ≤
      rsum n (fun x3 => rsum n (fun y => hubInd db 6 x3 y)) := by
  apply rsum_le
  intro x3 hx3
  apply rsum_le
  intro x4 hx4
  exact hubQ2_le_a6 db x0 x3 x4

private theorem V_le_s2s3 (n : Nat) (db : DB) (x0 : Nat) (hx0 : x0 < n) :
    rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4)) ≤
      (rsum n (fun x => rsum n (fun y => hubInd db 2 x y))) *
      (rsum n (fun x => rsum n (fun y => hubInd db 3 x y))) := by
  have h1 : rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4)) ≤
      rsum n (fun x3 => rsum n (fun x4 =>
        hubInd db 2 x0 x3 * hubInd db 3 x0 x4)) := by
    apply rsum_le; intro x3 hx3
    apply rsum_le; intro x4 hx4
    exact hubQ2_le_a2a3 db x0 x3 x4
  rw [rsum_product] at h1
  have hrow2 : rsum n (fun x3 => hubInd db 2 x0 x3) ≤
      rsum n (fun x => rsum n (fun y => hubInd db 2 x y)) :=
    le_rsum_of_mem n (fun x => rsum n (fun y => hubInd db 2 x y)) x0 hx0
  have hrow3 : rsum n (fun x4 => hubInd db 3 x0 x4) ≤
      rsum n (fun x => rsum n (fun y => hubInd db 3 x y)) :=
    le_rsum_of_mem n (fun x => rsum n (fun y => hubInd db 3 x y)) x0 hx0
  exact Nat.le_trans h1 (Nat.mul_le_mul hrow2 hrow3)

private theorem U_zero_of_s0 (n : Nat) (db : DB) (x0 : Nat)
    (h0 : ∀ x, x < n → hubInd db 0 x0 x = 0) :
    rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2)) = 0 := by
  apply (rsum_eq_zero_iff n _).mpr
  intro x1 hx1
  apply (rsum_eq_zero_iff n _).mpr
  intro x2 hx2
  simp [hubQ1, h0 x1 hx1]

private theorem U_zero_of_s1 (n : Nat) (db : DB) (x0 : Nat)
    (h1 : ∀ x, x < n → hubInd db 1 x0 x = 0) :
    rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2)) = 0 := by
  apply (rsum_eq_zero_iff n _).mpr
  intro x1 hx1
  apply (rsum_eq_zero_iff n _).mpr
  intro x2 hx2
  simp [hubQ1, h1 x2 hx2]

private theorem U_zero_of_s5 (n : Nat) (db : DB) (x0 : Nat)
    (h5 : ∀ x y, x < n → y < n → hubInd db 5 x y = 0) :
    rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2)) = 0 := by
  apply (rsum_eq_zero_iff n _).mpr
  intro x1 hx1
  apply (rsum_eq_zero_iff n _).mpr
  intro x2 hx2
  simp [hubQ1, h5 x1 x2 hx1 hx2]

private theorem V_zero_of_s2 (n : Nat) (db : DB) (x0 : Nat)
    (h2 : ∀ x, x < n → hubInd db 2 x0 x = 0) :
    rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4)) = 0 := by
  apply (rsum_eq_zero_iff n _).mpr
  intro x3 hx3
  apply (rsum_eq_zero_iff n _).mpr
  intro x4 hx4
  simp [hubQ2, h2 x3 hx3]

private theorem V_zero_of_s3 (n : Nat) (db : DB) (x0 : Nat)
    (h3 : ∀ x, x < n → hubInd db 3 x0 x = 0) :
    rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4)) = 0 := by
  apply (rsum_eq_zero_iff n _).mpr
  intro x3 hx3
  apply (rsum_eq_zero_iff n _).mpr
  intro x4 hx4
  simp [hubQ2, h3 x4 hx4]

private theorem V_zero_of_s6 (n : Nat) (db : DB) (x0 : Nat)
    (h6 : ∀ x y, x < n → y < n → hubInd db 6 x y = 0) :
    rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4)) = 0 := by
  apply (rsum_eq_zero_iff n _).mpr
  intro x3 hx3
  apply (rsum_eq_zero_iff n _).mpr
  intro x4 hx4
  simp [hubQ2, h6 x3 x4 hx3 hx4]

private theorem exists_foldl_max {α} (f : α → Nat) (l : List α) (hne : l ≠ []) :
    ∃ a ∈ l, (l.map f).foldl Nat.max 0 = f a := by
  induction l with
  | nil => contradiction
  | cons a l ih =>
      rw [List.map_cons, foldl_max_cons]
      by_cases hle : (l.map f).foldl Nat.max 0 ≤ f a
      · exact ⟨a, by simp, Nat.max_eq_left hle⟩
      · have hlt : f a ≤ (l.map f).foldl Nat.max 0 := by omega
        have hnel : l ≠ [] := by
          intro hl
          subst l
          simp at hle
        rcases ih hnel with ⟨b, hb, heq⟩
        have hm : (f a).max (List.foldl Nat.max 0 (List.map f l)) =
            List.foldl Nat.max 0 (List.map f l) := Nat.max_eq_right hlt
        exact ⟨b, by simp [hb], by rw [hm, heq]⟩

private theorem le_pow_self_of_pos_exp {x d : Nat} (hd : 0 < d) : x ≤ x^d := by
  cases d with
  | zero => omega
  | succ d =>
      by_cases hx : x = 0
      · subst x
        simp
      · have hx1 : 1 ≤ x := Nat.succ_le_of_lt (Nat.pos_of_ne_zero hx)
        have hxd : 1 ≤ x^d := by
          have hp : 0 < x^d := Nat.pow_pos hx1
          omega
        rw [Nat.pow_succ]
        simpa [Nat.mul_comm] using Nat.mul_le_mul_right x hxd

private theorem prod_le_prod {α} (l : List α) (f g : α → Nat)
    (h : ∀ a ∈ l, f a ≤ g a) : (l.map f).prod ≤ (l.map g).prod := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.prod_cons]
      exact Nat.mul_le_mul (h a (by simp)) (ih (fun x hx => h x (by simp [hx])))

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords.FractionalCap.Goal := by
  intro n db b weights den hcov
  have hden : 0 < den := hcov.1
  have hlen : weights.length = 7 := by
    simpa [pattern] using hcov.2.1
  let w0 := weights.getD 0 0
  let w1 := weights.getD 1 0
  let w2 := weights.getD 2 0
  let w3 := weights.getD 3 0
  let w4 := weights.getD 4 0
  let w5 := weights.getD 5 0
  let w6 := weights.getD 6 0
  have hwshape : weights = [w0, w1, w2, w3, w4, w5, w6] := by
    dsimp [w0, w1, w2, w3, w4, w5, w6]
    have hw : weights = (List.range 7).map (fun i => weights.getD i 0) := by
      apply List.ext_getElem
      · simp [hlen]
      · intro i hi1 hi2
        rw [List.getElem_map, List.getElem_range]
        have hsome : weights[i]? = some weights[i] := List.getElem?_eq_getElem hi1
        simp [List.getD, hsome]
    rw [hw]
    rfl
  rw [hwshape] at hcov ⊢
  have hvcov := hcov.2.2
  let s0 := rsum n (fun x => rsum n (fun y => hubInd db 0 x y))
  let s1 := rsum n (fun x => rsum n (fun y => hubInd db 1 x y))
  let s2 := rsum n (fun x => rsum n (fun y => hubInd db 2 x y))
  let s3 := rsum n (fun x => rsum n (fun y => hubInd db 3 x y))
  let s4 := rsum n (fun x => rsum n (fun y => hubInd db 4 x y))
  let s5 := rsum n (fun x => rsum n (fun y => hubInd db 5 x y))
  let s6 := rsum n (fun x => rsum n (fun y => hubInd db 6 x y))
  let D : Nat → Nat := fun x0 => rsum n (fun x5 => hubInd db 4 x0 x5)
  let U : Nat → Nat := fun x0 => rsum n (fun x1 => rsum n (fun x2 => hubQ1 db x0 x1 x2))
  let V : Nat → Nat := fun x0 => rsum n (fun x3 => rsum n (fun x4 => hubQ2 db x0 x3 x4))
  have hv4 : den ≤ w4 := by
    simpa [pattern] using hvcov 5 (by simp [pattern])
  have hv15 : den ≤ w0 + w5 := by
    simpa [pattern] using hvcov 1 (by simp [pattern])
  have hv25 : den ≤ w1 + w5 := by
    simpa [pattern] using hvcov 2 (by simp [pattern])
  have hv36 : den ≤ w2 + w6 := by
    simpa [pattern] using hvcov 3 (by simp [pattern])
  have hv46 : den ≤ w3 + w6 := by
    simpa [pattern] using hvcov 4 (by simp [pattern])
  have hSref : (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat =
      rsum n (fun x0 => D x0 * U x0 * V x0) := by
    rw [reference_toNat, ref_factor]
  have hs0supp : support n db ⟨0, 0, 1⟩ pattern b = s0 := by
    simpa [s0] using support_hub n db b 0 0 1 (by simp [pattern]) (by simp [pattern])
  have hs1supp : support n db ⟨1, 0, 2⟩ pattern b = s1 := by
    simpa [s1] using support_hub n db b 1 0 2 (by simp [pattern]) (by simp [pattern])
  have hs2supp : support n db ⟨2, 0, 3⟩ pattern b = s2 := by
    simpa [s2] using support_hub n db b 2 0 3 (by simp [pattern]) (by simp [pattern])
  have hs3supp : support n db ⟨3, 0, 4⟩ pattern b = s3 := by
    simpa [s3] using support_hub n db b 3 0 4 (by simp [pattern]) (by simp [pattern])
  have hs4supp : support n db ⟨4, 0, 5⟩ pattern b = s4 := by
    simpa [s4] using support_hub n db b 4 0 5 (by simp [pattern]) (by simp [pattern])
  have hs5supp : support n db ⟨5, 1, 2⟩ pattern b = s5 := by
    simpa [s5] using support_hub n db b 5 1 2 (by simp [pattern]) (by simp [pattern])
  have hs6supp : support n db ⟨6, 3, 4⟩ pattern b = s6 := by
    simpa [s6] using support_hub n db b 6 3 4 (by simp [pattern]) (by simp [pattern])
  have hRHS : ((pattern.atoms.zip [w0, w1, w2, w3, w4, w5, w6]).map
      (fun (a, w) => support n db a pattern b ^ w)).prod =
      s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
    rw [show pattern.atoms = [⟨0, 0, 1⟩, ⟨1, 0, 2⟩, ⟨2, 0, 3⟩, ⟨3, 0, 4⟩,
      ⟨4, 0, 5⟩, ⟨5, 1, 2⟩, ⟨6, 3, 4⟩] by rfl]
    simp only [List.zip_cons_cons, List.map_cons, List.prod_cons]
    rw [hs0supp, hs1supp, hs2supp, hs3supp, hs4supp, hs5supp, hs6supp]
    simp [s0, s1, s2, s3, s4, s5, s6]
    ac_rfl
  rw [hRHS]
  rw [hSref]
  by_cases hn : n = 0
  · subst n
    simp [rsum, hden]
  have hnpos : 0 < n := Nat.pos_of_ne_zero hn
  by_cases hzero : s0 = 0 ∨ s1 = 0 ∨ s2 = 0 ∨ s3 = 0 ∨ s4 = 0 ∨ s5 = 0 ∨ s6 = 0
  · have hSzero : rsum n (fun x0 => D x0 * U x0 * V x0) = 0 := by
      apply (rsum_eq_zero_iff n _).mpr
      intro x0 hx0
      rcases hzero with h0 | h1 | h2 | h3 | h4 | h5 | h6
      · have h0' : ∀ y, y < n → hubInd db 0 x0 y = 0 := by
          have hrow : rsum n (fun y => hubInd db 0 x0 y) = 0 :=
            (rsum_eq_zero_iff n (fun x => rsum n (fun y => hubInd db 0 x y))).mp h0 x0 hx0
          exact (rsum_eq_zero_iff n (fun y => hubInd db 0 x0 y)).mp hrow
        simp [U, U_zero_of_s0 n db x0 h0']
      · have h1' : ∀ y, y < n → hubInd db 1 x0 y = 0 := by
          have hrow : rsum n (fun y => hubInd db 1 x0 y) = 0 :=
            (rsum_eq_zero_iff n (fun x => rsum n (fun y => hubInd db 1 x y))).mp h1 x0 hx0
          exact (rsum_eq_zero_iff n (fun y => hubInd db 1 x0 y)).mp hrow
        simp [U, U_zero_of_s1 n db x0 h1']
      · have h2' : ∀ y, y < n → hubInd db 2 x0 y = 0 := by
          have hrow : rsum n (fun y => hubInd db 2 x0 y) = 0 :=
            (rsum_eq_zero_iff n (fun x => rsum n (fun y => hubInd db 2 x y))).mp h2 x0 hx0
          exact (rsum_eq_zero_iff n (fun y => hubInd db 2 x0 y)).mp hrow
        simp [V, V_zero_of_s2 n db x0 h2']
      · have h3' : ∀ y, y < n → hubInd db 3 x0 y = 0 := by
          have hrow : rsum n (fun y => hubInd db 3 x0 y) = 0 :=
            (rsum_eq_zero_iff n (fun x => rsum n (fun y => hubInd db 3 x y))).mp h3 x0 hx0
          exact (rsum_eq_zero_iff n (fun y => hubInd db 3 x0 y)).mp hrow
        simp [V, V_zero_of_s3 n db x0 h3']
      · have h4' : D x0 = 0 := by
          dsimp [D]
          exact (rsum_eq_zero_iff n (fun x => rsum n (fun y => hubInd db 4 x y))).mp h4 x0 hx0
        simp [h4']
      · have h5' : ∀ x y, x < n → y < n → hubInd db 5 x y = 0 := by
          intro x y hx hy
          have hrow : rsum n (fun z => hubInd db 5 x z) = 0 :=
            (rsum_eq_zero_iff n (fun x => rsum n (fun y => hubInd db 5 x y))).mp h5 x hx
          exact (rsum_eq_zero_iff n (fun z => hubInd db 5 x z)).mp hrow y hy
        simp [U, U_zero_of_s5 n db x0 h5']
      · have h6' : ∀ x y, x < n → y < n → hubInd db 6 x y = 0 := by
          intro x y hx hy
          have hrow : rsum n (fun z => hubInd db 6 x z) = 0 :=
            (rsum_eq_zero_iff n (fun x => rsum n (fun y => hubInd db 6 x y))).mp h6 x hx
          exact (rsum_eq_zero_iff n (fun z => hubInd db 6 x z)).mp hrow y hy
        simp [V, V_zero_of_s6 n db x0 h6']
    rw [hSzero]
    simp [hden]
  · have hs0p : 1 ≤ s0 := by omega
    have hs1p : 1 ≤ s1 := by omega
    have hs2p : 1 ≤ s2 := by omega
    have hs3p : 1 ≤ s3 := by omega
    have hs4p : 1 ≤ s4 := by omega
    have hs5p : 1 ≤ s5 := by omega
    have hs6p : 1 ≤ s6 := by omega
    let a := min w5 den
    let c := min w6 den
    have haw5 : a ≤ w5 := Nat.min_le_left _ _
    have haden : a ≤ den := Nat.min_le_right _ _
    have hcw6 : c ≤ w6 := Nat.min_le_left _ _
    have hcden : c ≤ den := Nat.min_le_right _ _
    have hda0 : den - a ≤ w0 := by
      by_cases hw : w5 ≤ den
      · have ha : a = w5 := Nat.min_eq_left hw
        omega
      · have ha : a = den := Nat.min_eq_right (by omega : den ≤ w5)
        omega
    have hda1 : den - a ≤ w1 := by
      by_cases hw : w5 ≤ den
      · have ha : a = w5 := Nat.min_eq_left hw
        omega
      · have ha : a = den := Nat.min_eq_right (by omega : den ≤ w5)
        omega
    have hdc2 : den - c ≤ w2 := by
      by_cases hw : w6 ≤ den
      · have hc : c = w6 := Nat.min_eq_left hw
        omega
      · have hc : c = den := Nat.min_eq_right (by omega : den ≤ w6)
        omega
    have hdc3 : den - c ≤ w3 := by
      by_cases hw : w6 ≤ den
      · have hc : c = w6 := Nat.min_eq_left hw
        omega
      · have hc : c = den := Nat.min_eq_right (by omega : den ≤ w6)
        omega
    let B := s5 ^ a * s0 ^ (den - a) * s1 ^ (den - a) *
      s6 ^ c * s2 ^ (den - c) * s3 ^ (den - c)
    have hYpow : ∀ x0, x0 < n → (U x0 * V x0) ^ den ≤ B := by
      intro x0 hx0
      have hU5 : U x0 ^ a ≤ s5 ^ a := pow_le_pow_left' (U_le_s5 n db x0) a
      have hU01 : U x0 ^ (den - a) ≤ (s0 * s1) ^ (den - a) :=
        pow_le_pow_left' (U_le_s0s1 n db x0 hx0) (den - a)
      have hV6 : V x0 ^ c ≤ s6 ^ c := pow_le_pow_left' (V_le_s6 n db x0) c
      have hV23 : V x0 ^ (den - c) ≤ (s2 * s3) ^ (den - c) :=
        pow_le_pow_left' (V_le_s2s3 n db x0 hx0) (den - c)
      have hUsplit : U x0 ^ den = U x0 ^ a * U x0 ^ (den - a) := by
        rw [← Nat.pow_add]
        congr 1
        omega
      have hVsplit : V x0 ^ den = V x0 ^ c * V x0 ^ (den - c) := by
        rw [← Nat.pow_add]
        congr 1
        omega
      have hU : U x0 ^ den ≤ s5 ^ a * (s0 * s1) ^ (den - a) := by
        rw [hUsplit]
        exact Nat.mul_le_mul hU5 hU01
      have hV : V x0 ^ den ≤ s6 ^ c * (s2 * s3) ^ (den - c) := by
        rw [hVsplit]
        exact Nat.mul_le_mul hV6 hV23
      rw [Nat.mul_pow]
      rw [Nat.mul_pow] at hU hV
      calc
        U x0 ^ den * V x0 ^ den ≤ (s5 ^ a * (s0 ^ (den - a) * s1 ^ (den - a))) *
            (s6 ^ c * (s2 ^ (den - c) * s3 ^ (den - c))) := Nat.mul_le_mul hU hV
        _ = B := by dsimp [B]; ac_rfl
    let Y : Nat → Nat := fun x0 => U x0 * V x0
    let Ymax := ((List.range n).map Y).foldl Nat.max 0
    have hYle : ∀ x0, x0 < n → Y x0 ≤ Ymax := by
      intro x0 hx0
      exact le_foldl_max Y (List.range n) x0 (List.mem_range.mpr hx0)
    have hYmaxpow : Ymax ^ den ≤ B := by
      rcases exists_foldl_max Y (List.range n) (by simp [hn]) with ⟨x0, hx0m, hmax⟩
      have hx0 : x0 < n := List.mem_range.mp hx0m
      have hYmaxeq : Ymax = Y x0 := hmax
      rw [hYmaxeq]
      exact hYpow x0 hx0
    have hSle : rsum n (fun x0 => D x0 * U x0 * V x0) ≤ s4 * Ymax := by
      have h1 : rsum n (fun x0 => D x0 * Y x0) ≤ rsum n (fun x0 => D x0 * Ymax) := by
        apply rsum_le
        intro x0 hx0
        exact Nat.mul_le_mul_left (D x0) (hYle x0 hx0)
      have h2 : rsum n (fun x0 => D x0 * Ymax) = (rsum n D) * Ymax := by
        rw [rsum_mul_right]
      calc
        rsum n (fun x0 => D x0 * U x0 * V x0) = rsum n (fun x0 => D x0 * Y x0) := by
          apply rsum_congr
          intro x0 hx0
          dsimp [Y]
          rw [Nat.mul_assoc]
        _ ≤ rsum n (fun x0 => D x0 * Ymax) := h1
        _ = (rsum n D) * Ymax := h2
        _ = s4 * Ymax := by rfl
    have hSpow : (rsum n (fun x0 => D x0 * U x0 * V x0)) ^ den ≤ s4 ^ den * B := by
      have h1 := pow_le_pow_left' hSle den
      rw [Nat.mul_pow] at h1
      exact Nat.le_trans h1 (Nat.mul_le_mul_left (s4 ^ den) hYmaxpow)
    let facs : List (Nat × Nat × Nat) :=
      [(s5, a, w5), (s0, den - a, w0), (s1, den - a, w1),
       (s6, c, w6), (s2, den - c, w2), (s3, den - c, w3), (s4, den, w4)]
    have hprod : (facs.map (fun p => p.1 ^ p.2.1)).prod ≤
        (facs.map (fun p => p.1 ^ p.2.2)).prod := by
      apply prod_le_prod
      intro p hp
      simp [facs] at hp
      rcases hp with rfl | rfl | rfl | rfl | rfl | rfl | rfl
      · exact pow_le_of_le_exp hs5p haw5
      · exact pow_le_of_le_exp hs0p hda0
      · exact pow_le_of_le_exp hs1p hda1
      · exact pow_le_of_le_exp hs6p hcw6
      · exact pow_le_of_le_exp hs2p hdc2
      · exact pow_le_of_le_exp hs3p hdc3
      · exact pow_le_of_le_exp hs4p hv4
    calc
      (rsum n (fun x0 => D x0 * U x0 * V x0)) ^ den ≤ s4 ^ den * B := hSpow
      _ = (facs.map (fun p => p.1 ^ p.2.1)).prod := by
          dsimp [B, facs]
          simp
          ac_rfl
      _ ≤ (facs.map (fun p => p.1 ^ p.2.2)).prod := hprod
      _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
          dsimp [facs]
          simp
          ac_rfl

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChordsFractionalCap
