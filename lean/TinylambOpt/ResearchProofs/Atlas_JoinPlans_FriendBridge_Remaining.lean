import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridge.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! ## Finite sum helpers (Std only). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private def triple (n : Nat) (f : Nat → Nat → Nat → Int) : Int :=
  rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => f x1 x2 x3)))

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
        apply rsum_congr; intro j hj; rw [rsum_zero]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n + 1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr; intro i hi
            rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hii => h i (Nat.lt_succ_of_lt hii))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hii => h i (Nat.lt_succ_of_lt hii))
      omega

private theorem sum_bool {α} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then (1 : Int) else 0)).sum = (l.countP p : Int) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]; omega
      · have h' : p a = false := by cases hpa : p a <;> simp_all
        simp [h', ih]

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
    triple n (fun x1 x2 x3 => f x1 x2 x3 + g x1 x2 x3) =
      triple n f + triple n g := by
  unfold triple
  rw [← rsum_add]
  apply rsum_congr; intro x1 hx1
  rw [← rsum_add]
  apply rsum_congr; intro x2 hx2
  rw [rsum_add]

private theorem triple_sub (n : Nat) (f g : Nat → Nat → Nat → Int) :
    triple n (fun x1 x2 x3 => f x1 x2 x3 - g x1 x2 x3) =
      triple n f - triple n g := by
  unfold triple
  rw [← rsum_sub]
  apply rsum_congr; intro x1 hx1
  rw [← rsum_sub]
  apply rsum_congr; intro x2 hx2
  rw [rsum_sub]

private theorem triple_const_mul (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    triple n (fun x1 x2 x3 => c * f x1 x2 x3) = c * triple n f := by
  unfold triple
  rw [← rsum_const_mul]
  apply rsum_congr; intro x1 hx1
  rw [← rsum_const_mul]
  apply rsum_congr; intro x2 hx2
  rw [rsum_const_mul]

private theorem bool_eq_false_of_ne {a b : Nat} (h : a ≠ b) : (a == b) = false := by
  cases h' : a == b
  · rfl
  · have : a = b := Nat.eq_of_beq_eq_true h'
    exact False.elim (h this)

/-! ## FriendBridge path indicators and explicit reference. -/

private def indA (db : DB) (b : Bind) (x : Nat) : Int := if db 0 (b 0) x == 0 then 0 else 1
private def indB (db : DB) (x y : Nat) : Int := if db 1 x y == 0 then 0 else 1
private def indC (db : DB) (x y : Nat) : Int := if db 2 x y == 0 then 0 else 1
private def indD (db : DB) (b : Bind) (x : Nat) : Int := if db 3 x (b 4) == 0 then 0 else 1

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

private theorem indD_spec (db : DB) (b : Bind) (x : Nat) :
    indD db b x = if db 3 x (b 4) != 0 then 1 else 0 := by
  by_cases h : db 3 x (b 4) == 0
  · simp [indD, h, ne_zero_false_of_beq_zero h]
  · simp [indD, h, ne_zero_true_of_not_beq_zero h]

private theorem indA_nonneg (db : DB) (b : Bind) (x : Nat) : 0 ≤ indA db b x := by
  rw [indA_spec]; split <;> omega

private theorem indB_nonneg (db : DB) (x y : Nat) : 0 ≤ indB db x y := by
  rw [indB_spec]; split <;> omega

private theorem indC_nonneg (db : DB) (x y : Nat) : 0 ≤ indC db x y := by
  rw [indC_spec]; split <;> omega

private theorem indD_nonneg (db : DB) (b : Bind) (x : Nat) : 0 ≤ indD db b x := by
  rw [indD_spec]; split <;> omega

private theorem indA_le_one (db : DB) (b : Bind) (x : Nat) : indA db b x ≤ 1 := by
  rw [indA_spec]; split <;> omega

private theorem indB_le_one (db : DB) (x y : Nat) : indB db x y ≤ 1 := by
  rw [indB_spec]; split <;> omega

private theorem indC_le_one (db : DB) (x y : Nat) : indC db x y ≤ 1 := by
  rw [indC_spec]; split <;> omega

private theorem indD_le_one (db : DB) (b : Bind) (x : Nat) : indD db b x ≤ 1 := by
  rw [indD_spec]; split <;> omega

private theorem ref_indicator (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
      triple n (fun x1 x2 x3 =>
        indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3))) := by
  simp [reference, integrate, pattern, value, put, rsum, triple, indA, indB, indC, indD]

/-! ## Support counts and the three integral-cover bounds. -/

private def sA (n : Nat) (db : DB) (b : Bind) : Nat := (List.range n).countP (fun x => db 0 (b 0) x != 0)
private def sB (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))).sum
private def sC (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 2 x y != 0))).sum
private def sD (n : Nat) (db : DB) (b : Bind) : Nat := (List.range n).countP (fun x => db 3 x (b 4) != 0)

private theorem support0_eq (n : Nat) (db : DB) (b : Bind) :
    support n db ⟨0, 0, 1⟩ pattern b = sA n db b := by
  simp [support, pattern, sA]

private theorem support1_eq (n : Nat) (db : DB) (b : Bind) :
    support n db ⟨1, 1, 2⟩ pattern b = sB n db := by
  simp [support, pattern, sB]

private theorem support2_eq (n : Nat) (db : DB) (b : Bind) :
    support n db ⟨2, 2, 3⟩ pattern b = sC n db := by
  simp [support, pattern, sC]

private theorem sum_bool_nat {α} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then (1 : Nat) else 0)).sum = l.countP p := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]; omega
      · have h' : p a = false := by cases hpa : p a <;> simp_all
        simp [h', ih]

private theorem if_eq_zero_iff_ne (z : Int) :
    (if z = 0 then (0 : Nat) else 1) = if z != 0 then 1 else 0 := by
  by_cases h : z == 0
  · have hz : z = 0 := beq_iff_eq.mp h
    simp [hz]
  · have hz : z ≠ 0 := by intro hz; exact h (by simp [hz])
    simp [hz]

private theorem support3_eq (n : Nat) (db : DB) (b : Bind) :
    support n db ⟨3, 3, 4⟩ pattern b = sD n db b := by
  unfold support sD
  simp [pattern]
  rw [show (List.range n).map (fun x => if db 3 x (b 4) = 0 then 0 else 1) =
      (List.range n).map (fun x => if db 3 x (b 4) != 0 then 1 else 0) by
    apply List.map_congr_left
    intro x hx
    exact if_eq_zero_iff_ne (db 3 x (b 4))]
  exact sum_bool_nat (List.range n) (fun x => db 3 x (b 4) != 0)

private theorem int_sum_nat (l : List Nat) : (l.map (fun x : Nat => (x : Int))).sum = (l.sum : Int) := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem rsum_indA (n : Nat) (db : DB) (b : Bind) :
    rsum n (fun x => indA db b x) = (sA n db b : Int) := by
  rw [show rsum n (fun x => indA db b x) =
      rsum n (fun x => if db 0 (b 0) x != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indA_spec db b i]
  unfold rsum sA
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

private theorem rsum_indB2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indB db x y)) = (sB n db : Int) := by
  simp only [rsum_indB]
  unfold rsum sB
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))))

private theorem rsum_indC2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indC db x y)) = (sC n db : Int) := by
  simp only [rsum_indC]
  unfold rsum sC
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 2 x y != 0))))

private theorem rsum_indD (n : Nat) (db : DB) (b : Bind) :
    rsum n (fun x => indD db b x) = (sD n db b : Int) := by
  rw [show rsum n (fun x => indD db b x) =
      rsum n (fun x => if db 3 x (b 4) != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indD_spec db b i]
  unfold rsum sD
  exact sum_bool (List.range n) (fun x => db 3 x (b 4) != 0)

private theorem term_le_AC (db : DB) (b : Bind) (x1 x2 x3 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3)) ≤
      indA db b x1 * indC db x2 x3 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec]
  split <;> split <;> split <;> split <;> omega

private theorem term_le_BC (db : DB) (b : Bind) (x1 x2 x3 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3)) ≤
      indB db x1 x2 * indC db x2 x3 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec]
  split <;> split <;> split <;> split <;> omega

private theorem term_le_BD (db : DB) (b : Bind) (x1 x2 x3 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3)) ≤
      indB db x1 x2 * indD db b x3 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec]
  split <;> split <;> split <;> split <;> omega

private theorem sum_AC (n : Nat) (db : DB) (b : Bind) :
    triple n (fun x1 x2 x3 => indA db b x1 * indC db x2 x3) =
      (sA n db b : Int) * (sC n db : Int) := by
  unfold triple
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
        indA db b x1 * indC db x2 x3)))
        = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 =>
            indA db b x1 * indC db x2 x3))) := by rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x1 =>
            indA db b x1 * rsum n (fun x3 => indC db x2 x3))) := by
            apply rsum_congr; intro x2 hx2
            apply rsum_congr; intro x1 hx1
            rw [rsum_const_mul]
    _ = rsum n (fun x2 => (rsum n (fun x1 => indA db b x1)) *
            rsum n (fun x3 => indC db x2 x3)) := by
            apply rsum_congr; intro x2 hx2
            rw [rsum_mul_const]
    _ = (rsum n (fun x1 => indA db b x1)) *
            rsum n (fun x2 => rsum n (fun x3 => indC db x2 x3)) := by
            rw [rsum_const_mul]
    _ = (sA n db b : Int) * (sC n db : Int) := by
            rw [rsum_indA, rsum_indC2]

private theorem rsum_mul_le_products (n : Nat) (a b : Nat → Int)
    (ha : ∀ i, i < n → 0 ≤ a i) (hb : ∀ i, i < n → 0 ≤ b i) :
    rsum n (fun i => a i * b i) ≤ rsum n a * rsum n b := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ]
      have han : 0 ≤ a n := ha n (Nat.lt_succ_self n)
      have hbn : 0 ≤ b n := hb n (Nat.lt_succ_self n)
      have hia := ih (fun i hi => ha i (Nat.lt_succ_of_lt hi))
        (fun i hi => hb i (Nat.lt_succ_of_lt hi))
      have hsa : 0 ≤ rsum n a := rsum_nonneg n a (fun i hi => ha i (Nat.lt_succ_of_lt hi))
      have hsb : 0 ≤ rsum n b := rsum_nonneg n b (fun i hi => hb i (Nat.lt_succ_of_lt hi))
      simp only [Int.add_mul, Int.mul_add]
      have hp1 : 0 ≤ rsum n a * b n := Int.mul_nonneg hsa hbn
      have hp2 : 0 ≤ a n * rsum n b := Int.mul_nonneg han hsb
      omega

private theorem sum_BC (n : Nat) (db : DB) :
    triple n (fun x1 x2 x3 => indB db x1 x2 * indC db x2 x3) ≤
      (sB n db : Int) * (sC n db : Int) := by
  unfold triple
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
        indB db x1 x2 * indC db x2 x3)))
        = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 =>
            indB db x1 x2 * indC db x2 x3))) := by rw [rsum_comm]
    _ = rsum n (fun x2 => rsum n (fun x1 =>
            indB db x1 x2 * rsum n (fun x3 => indC db x2 x3))) := by
            apply rsum_congr; intro x2 hx2
            apply rsum_congr; intro x1 hx1
            rw [rsum_const_mul]
    _ = rsum n (fun x2 => (rsum n (fun x1 => indB db x1 x2)) *
            rsum n (fun x3 => indC db x2 x3)) := by
            apply rsum_congr; intro x2 hx2
            rw [rsum_mul_const]
    _ ≤ (rsum n (fun x2 => rsum n (fun x1 => indB db x1 x2))) *
            (rsum n (fun x2 => rsum n (fun x3 => indC db x2 x3))) := by
            apply rsum_mul_le_products
            · intro i hi
              exact rsum_nonneg n (fun j => indB db j i) (fun j hj => indB_nonneg db j i)
            · intro i hi
              exact rsum_nonneg n (fun j => indC db i j) (fun j hj => indC_nonneg db i j)
    _ = (sB n db : Int) * (sC n db : Int) := by
            rw [show rsum n (fun x2 => rsum n (fun x1 => indB db x1 x2)) =
                rsum n (fun x1 => rsum n (fun x2 => indB db x1 x2)) by rw [rsum_comm]]
            rw [rsum_indB2, rsum_indC2]

private theorem sum_BD (n : Nat) (db : DB) (b : Bind) :
    triple n (fun x1 x2 x3 => indB db x1 x2 * indD db b x3) =
      (sB n db : Int) * (sD n db b : Int) := by
  unfold triple
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
        indB db x1 x2 * indD db b x3)))
        = rsum n (fun x1 => rsum n (fun x2 =>
            indB db x1 x2 * rsum n (fun x3 => indD db b x3))) := by
            apply rsum_congr; intro x1 hx1
            apply rsum_congr; intro x2 hx2
            rw [rsum_const_mul]
    _ = rsum n (fun x1 => (rsum n (fun x2 => indB db x1 x2)) *
            rsum n (fun x3 => indD db b x3)) := by
            apply rsum_congr; intro x1 hx1
            rw [rsum_mul_const]
    _ = (rsum n (fun x1 => rsum n (fun x2 => indB db x1 x2))) *
            rsum n (fun x3 => indD db b x3) := by
            rw [rsum_mul_const]
    _ = (sB n db : Int) * (sD n db b : Int) := by
            rw [rsum_indB2, rsum_indD]

private theorem ref_le_AC (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      (sA n db b : Int) * (sC n db : Int) := by
  rw [ref_indicator]
  calc
    triple n (fun x1 x2 x3 =>
        indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3)))
        ≤ triple n (fun x1 x2 x3 => indA db b x1 * indC db x2 x3) := by
            unfold triple
            apply rsum_le; intro x1 hx1
            apply rsum_le; intro x2 hx2
            apply rsum_le; intro x3 hx3
            exact term_le_AC db b x1 x2 x3
    _ = (sA n db b : Int) * (sC n db : Int) := sum_AC n db b

private theorem ref_le_BC (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      (sB n db : Int) * (sC n db : Int) := by
  rw [ref_indicator]
  calc
    triple n (fun x1 x2 x3 =>
        indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3)))
        ≤ triple n (fun x1 x2 x3 => indB db x1 x2 * indC db x2 x3) := by
            unfold triple
            apply rsum_le; intro x1 hx1
            apply rsum_le; intro x2 hx2
            apply rsum_le; intro x3 hx3
            exact term_le_BC db b x1 x2 x3
    _ ≤ (sB n db : Int) * (sC n db : Int) := sum_BC n db

private theorem ref_le_BD (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      (sB n db : Int) * (sD n db b : Int) := by
  rw [ref_indicator]
  calc
    triple n (fun x1 x2 x3 =>
        indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3)))
        ≤ triple n (fun x1 x2 x3 => indB db x1 x2 * indD db b x3) := by
            unfold triple
            apply rsum_le; intro x1 hx1
            apply rsum_le; intro x2 hx2
            apply rsum_le; intro x3 hx3
            exact term_le_BD db b x1 x2 x3
    _ = (sB n db : Int) * (sD n db b : Int) := sum_BD n db b

private theorem ref_nonneg (n : Nat) (db : DB) (b : Bind) :
    0 ≤ reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b := by
  rw [ref_indicator]
  unfold triple
  apply rsum_nonneg; intro x1 hx1
  apply rsum_nonneg; intro x2 hx2
  apply rsum_nonneg; intro x3 hx3
  change 0 ≤ indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * indD db b x3))
  rw [indA_spec, indB_spec, indC_spec, indD_spec]
  split <;> split <;> split <;> split <;> omega

/-! ## Goal FractionalCap: fractional edge cover bounds the support-path count. -/

private theorem rhs_prod (n : Nat) (db : DB) (b : Bind) (w0 w1 w2 w3 : Nat) :
    ((pattern.atoms.zip [w0, w1, w2, w3]).map
      (fun (a, w) => support n db a pattern b ^ w)).prod =
      support n db ⟨0, 0, 1⟩ pattern b ^ w0 *
        (support n db ⟨1, 1, 2⟩ pattern b ^ w1 *
          (support n db ⟨2, 2, 3⟩ pattern b ^ w2 *
            support n db ⟨3, 3, 4⟩ pattern b ^ w3)) := by
  simp [pattern]

theorem sound_FractionalCap : FractionalCap.Goal := by
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
          | nil =>
              let N : Nat := (reference pattern n
                (fun r x y => if db r x y == 0 then 0 else 1) b).toNat
              have hNint : (N : Int) = reference pattern n
                  (fun r x y => if db r x y == 0 then 0 else 1) b := by
                dsimp [N]
                exact Int.toNat_of_nonneg (ref_nonneg n db b)
              rw [rhs_prod]
              rw [support0_eq, support1_eq, support2_eq, support3_eq]
              simp [pattern] at hcov
              change N ^ den ≤
                sA n db b ^ w0 * (sB n db ^ w1 * (sC n db ^ w2 * sD n db b ^ w3))
              rcases hcov with ⟨h01, h12, h23⟩
              have hACint : (N : Int) ≤ (sA n db b : Int) * (sC n db : Int) := by
                rw [hNint]
                exact ref_le_AC n db b
              have hBCint : (N : Int) ≤ (sB n db : Int) * (sC n db : Int) := by
                rw [hNint]
                exact ref_le_BC n db b
              have hBDint : (N : Int) ≤ (sB n db : Int) * (sD n db b : Int) := by
                rw [hNint]
                exact ref_le_BD n db b
              have hAC : N ≤ sA n db b * sC n db := Int.ofNat_le.mp (by simpa using hACint)
              have hBC : N ≤ sB n db * sC n db := Int.ofNat_le.mp (by simpa using hBCint)
              have hBD : N ≤ sB n db * sD n db b := Int.ofNat_le.mp (by simpa using hBDint)
              by_cases hA0 : sA n db b = 0
              · have hAC0 : N ≤ 0 := by simpa [hA0] using hAC
                have hN0 : N = 0 := by omega
                simp [hN0, hA0, hden]
              by_cases hB0 : sB n db = 0
              · have hBC0 : N ≤ 0 := by simpa [hB0] using hBC
                have hN0 : N = 0 := by omega
                simp [hN0, hB0, hden]
              by_cases hC0 : sC n db = 0
              · have hAC0 : N ≤ 0 := by simpa [hC0] using hAC
                have hN0 : N = 0 := by omega
                simp [hN0, hC0, hden]
              by_cases hD0 : sD n db b = 0
              · have hBD0 : N ≤ 0 := by simpa [hD0] using hBD
                have hN0 : N = 0 := by omega
                simp [hN0, hD0, hden]
              have hApos : 0 < sA n db b := by omega
              have hBpos : 0 < sB n db := by omega
              have hCpos : 0 < sC n db := by omega
              have hDpos : 0 < sD n db b := by omega
              let α := min (min w0 w2) den
              let γ := min (min w1 w3) (den - α)
              let β := den - α - γ
              have hα_le_den : α ≤ den := by dsimp [α]; exact Nat.min_le_right _ _
              have hα_le_w0 : α ≤ w0 := by
                dsimp [α]
                exact Nat.le_trans (Nat.min_le_left _ _) (Nat.min_le_left _ _)
              have hα_le_w2 : α ≤ w2 := by
                dsimp [α]
                exact Nat.le_trans (Nat.min_le_left _ _) (Nat.min_le_right _ _)
              have hγ_le_dena : γ ≤ den - α := by dsimp [γ]; exact Nat.min_le_right _ _
              have hγ_le_w1 : γ ≤ w1 := by
                dsimp [γ]
                exact Nat.le_trans (Nat.min_le_left _ _) (Nat.min_le_left _ _)
              have hγ_le_w3 : γ ≤ w3 := by
                dsimp [γ]
                exact Nat.le_trans (Nat.min_le_left _ _) (Nat.min_le_right _ _)
              have hα_ge : den - w1 ≤ α := by
                dsimp [α]
                exact (Nat.le_min).2 ⟨(Nat.le_min).2 ⟨by omega, by omega⟩, by omega⟩
              have hγ_ge : den - w2 ≤ γ := by
                dsimp [γ]
                exact (Nat.le_min).2 ⟨(Nat.le_min).2 ⟨by omega, by omega⟩, by omega⟩
              have hsub1 : den - α - γ + γ = den - α := Nat.sub_add_cancel hγ_le_dena
              have hβγ_le_w1 : β + γ ≤ w1 := by dsimp [β]; omega
              have hαβ_le_w2 : α + β ≤ w2 := by dsimp [β]; omega
              have hsumabc : α + β + γ = den := by dsimp [β]; omega
              have hsA_pow : sA n db b ^ α ≤ sA n db b ^ w0 :=
                Nat.pow_le_pow_right hApos hα_le_w0
              have hsB_pow : sB n db ^ β * sB n db ^ γ ≤ sB n db ^ w1 := by
                rw [← Nat.pow_add]
                exact Nat.pow_le_pow_right hBpos hβγ_le_w1
              have hsC_pow : sC n db ^ α * sC n db ^ β ≤ sC n db ^ w2 := by
                rw [← Nat.pow_add]
                exact Nat.pow_le_pow_right hCpos hαβ_le_w2
              have hsD_pow : sD n db b ^ γ ≤ sD n db b ^ w3 :=
                Nat.pow_le_pow_right hDpos hγ_le_w3
              have hNpow : N ^ den = N ^ α * N ^ β * N ^ γ := by
                rw [show den = α + β + γ by omega]
                rw [Nat.pow_add, Nat.pow_add]
              calc
                N ^ den = N ^ α * N ^ β * N ^ γ := hNpow
                _ ≤ ((sA n db b) * (sC n db)) ^ α *
                      ((sB n db) * (sC n db)) ^ β *
                      ((sB n db) * (sD n db b)) ^ γ := by
                    apply Nat.mul_le_mul
                    · apply Nat.mul_le_mul
                      · exact Nat.pow_le_pow_left hAC α
                      · exact Nat.pow_le_pow_left hBC β
                    · exact Nat.pow_le_pow_left hBD γ
                _ = (sA n db b ^ α * sC n db ^ α) *
                      (sB n db ^ β * sC n db ^ β) *
                      (sB n db ^ γ * sD n db b ^ γ) := by
                    simp only [Nat.mul_pow]
                _ = (sA n db b ^ α * (sB n db ^ β * sB n db ^ γ)) *
                      ((sC n db ^ α * sC n db ^ β) * sD n db b ^ γ) := by
                    ac_rfl
                _ ≤ (sA n db b ^ w0 * sB n db ^ w1) *
                      (sC n db ^ w2 * sD n db b ^ w3) := by
                    apply Nat.mul_le_mul
                    · exact Nat.mul_le_mul hsA_pow hsB_pow
                    · exact Nat.mul_le_mul hsC_pow hsD_pow
                _ = sA n db b ^ w0 * (sB n db ^ w1 * (sC n db ^ w2 * sD n db b ^ w3)) := by
                    ac_rfl
          | cons w4 _ => simp [pattern] at hwlen

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  simp [injective, collapsed, partitions, mobius, factorial, integrate, value, put, pattern]
  let P : Nat → Nat → Nat → Int := fun x1 x2 x3 =>
    db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * db 3 x3 (b 4)))
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      if ((x1 ≠ x2 ∧ x1 ≠ x3) ∧ x2 ≠ x3) then P x1 x2 x3 else 0))) =
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => P x1 x2 x3))) +
      (-(rsum n (fun x1 => rsum n (fun x3 => P x1 x1 x3))) +
        (-(rsum n (fun x2 => rsum n (fun x1 => P x1 x2 x1))) +
          (-(rsum n (fun x1 => rsum n (fun x2 => P x1 x2 x2))) +
            2 * rsum n (fun x1 => P x1 x1 x1))))
  have hpoint : ∀ x1 x2 x3 : Nat,
      (if ((x1 ≠ x2 ∧ x1 ≠ x3) ∧ x2 ≠ x3) then P x1 x2 x3 else 0) =
        P x1 x2 x3 - (if x2 = x1 then P x1 x2 x3 else 0)
          - (if x3 = x1 then P x1 x2 x3 else 0)
          - (if x3 = x2 then P x1 x2 x3 else 0)
          + 2 * (if (x2 = x1 ∧ x3 = x1) then P x1 x2 x3 else 0) := by
    intro x1 x2 x3
    by_cases h12 : x1 = x2 <;> by_cases h13 : x1 = x3 <;>
      by_cases h23 : x2 = x3 <;> simp [h12, h13, h23, eq_comm] <;> omega
  rw [show rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      if ((x1 ≠ x2 ∧ x1 ≠ x3) ∧ x2 ≠ x3) then P x1 x2 x3 else 0))) =
      triple n (fun x1 x2 x3 =>
        P x1 x2 x3 - (if x2 = x1 then P x1 x2 x3 else 0)
          - (if x3 = x1 then P x1 x2 x3 else 0)
          - (if x3 = x2 then P x1 x2 x3 else 0)
          + 2 * (if (x2 = x1 ∧ x3 = x1) then P x1 x2 x3 else 0)) by
    change triple n (fun x1 x2 x3 =>
      if ((x1 ≠ x2 ∧ x1 ≠ x3) ∧ x2 ≠ x3) then P x1 x2 x3 else 0) = _
    unfold triple
    apply rsum_congr; intro x1 hx1
    apply rsum_congr; intro x2 hx2
    apply rsum_congr; intro x3 hx3
    exact hpoint x1 x2 x3]
  rw [triple_add, triple_sub, triple_sub, triple_sub, triple_const_mul]
  have h12 : triple n (fun x1 x2 x3 => if x2 = x1 then P x1 x2 x3 else 0) =
      rsum n (fun x1 => rsum n (fun x3 => P x1 x1 x3)) := by
    unfold triple
    apply rsum_congr; intro x1 hx1
    calc
      rsum n (fun x2 => rsum n (fun x3 => if x2 = x1 then P x1 x2 x3 else 0))
          = rsum n (fun x2 => if x2 = x1 then rsum n (fun x3 => P x1 x1 x3) else 0) := by
            apply rsum_congr; intro x2 hx2
            by_cases h : x2 = x1
            · subst x2; simp
            · simpa [h] using (rsum_zero_const n)
      _ = rsum n (fun x3 => P x1 x1 x3) := by
            rw [rsum_pick n x1 (fun _ => rsum n (fun x3 => P x1 x1 x3)) hx1]
  have h13 : triple n (fun x1 x2 x3 => if x3 = x1 then P x1 x2 x3 else 0) =
      rsum n (fun x2 => rsum n (fun x1 => P x1 x2 x1)) := by
    unfold triple
    calc
      rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => if x3 = x1 then P x1 x2 x3 else 0)))
          = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 => if x3 = x1 then P x1 x2 x3 else 0))) := by
            rw [rsum_comm]
      _ = rsum n (fun x2 => rsum n (fun x1 => P x1 x2 x1)) := by
            apply rsum_congr; intro x2 hx2
            apply rsum_congr; intro x1 hx1
            exact rsum_pick n x1 (fun x3 => P x1 x2 x3) hx1
  have h23 : triple n (fun x1 x2 x3 => if x3 = x2 then P x1 x2 x3 else 0) =
      rsum n (fun x1 => rsum n (fun x2 => P x1 x2 x2)) := by
    unfold triple
    apply rsum_congr; intro x1 hx1
    apply rsum_congr; intro x2 hx2
    exact rsum_pick n x2 (fun x3 => P x1 x2 x3) hx2
  have h123 : triple n (fun x1 x2 x3 => if (x2 = x1 ∧ x3 = x1) then P x1 x2 x3 else 0) =
      rsum n (fun x1 => P x1 x1 x1) := by
    unfold triple
    apply rsum_congr; intro x1 hx1
    calc
      rsum n (fun x2 => rsum n (fun x3 => if (x2 = x1 ∧ x3 = x1) then P x1 x2 x3 else 0))
          = rsum n (fun x2 => if x2 = x1 then P x1 x1 x1 else 0) := by
            apply rsum_congr; intro x2 hx2
            by_cases h : x2 = x1
            · subst x2
              simpa [true_and] using (rsum_pick n x1 (fun x3 => P x1 x1 x3) hx1)
            · simpa [h] using (rsum_zero_const n)
      _ = P x1 x1 x1 := by
            rw [rsum_pick n x1 (fun _ => P x1 x1 x1) hx1]
  have hS0 : rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => P x1 x2 x3))) =
      triple n P := rfl
  rw [hS0, h12, h13, h23, h123]
  omega

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.FriendBridge.Remaining

