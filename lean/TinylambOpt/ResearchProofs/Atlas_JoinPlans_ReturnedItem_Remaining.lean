import TinylambOpt.ResearchGoals.Atlas.JoinPlans

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnedItem.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReturnedItem

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Finite-range sum helpers (Std only). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_const_zero (n : Nat) : rsum n (fun _ => (0 : Int)) = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, ih]; simp

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

private theorem rsum_neg (n : Nat) (f : Nat → Int) : rsum n (fun i => - f i) = - rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih]; omega

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  simp only [Int.sub_eq_add_neg]
  rw [rsum_add, rsum_neg]

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = (0 : Int) by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        simp [rsum]]
      rw [rsum_const_zero]
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
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

private theorem rsum_le_total (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i)
    (i : Nat) (hi : i < n) : f i ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      have hr := rsum_nonneg n f (fun j hj => h j (Nat.lt_succ_of_lt hj))
      by_cases hEq : i = n
      · have hn := h n (Nat.lt_succ_self n)
        subst i
        omega
      · have hi' : i < n := by omega
        have hn := h n (Nat.lt_succ_self n)
        have ih' := ih (fun j hj => h j (Nat.lt_succ_of_lt hj)) hi'
        omega

private theorem rsum_mul_const_right (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_prod_sep (n m : Nat) (f : Nat → Int) (g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih]
      rw [show rsum m (fun j => f n * g j) = f n * rsum m g by
        simpa using rsum_const_mul m (f n) g]
      rw [Int.add_mul]

private theorem rsum_ite_single (n a : Nat) (g : Nat → Int) :
    rsum n (fun b => if a = b then g b else 0) = if a < n then g a else 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      by_cases h : a = n
      · subst n
        simp
      · by_cases han : a < n
        · simp [han, Nat.lt_succ_of_lt han, h]
        · have hange : ¬ a < n+1 := by omega
          simp [han, hange, h]

/-! Inclusion–exclusion for a three-variable "all distinct" indicator. -/

private theorem triple_inclusion (n : Nat) (f : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      if a ≠ b ∧ a ≠ c ∧ b ≠ c then f a b c else 0))) =
    rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c))) -
    rsum n (fun a => rsum n (fun c => f a a c)) -
    rsum n (fun a => rsum n (fun b => f a b a)) -
    rsum n (fun a => rsum n (fun b => f a b b)) +
    2 * rsum n (fun a => f a a a) := by
  let D := fun a b c => if a ≠ b ∧ a ≠ c ∧ b ≠ c then f a b c else 0
  let P := fun a b c => if a = b then f a b c else 0
  let Q := fun a b c => if a = c then f a b c else 0
  let R := fun a b c => if b = c then f a b c else 0
  let E := fun a b c => if a = b ∧ b = c then f a b c else 0
  have hpoint : ∀ a b c, D a b c = f a b c - P a b c - Q a b c - R a b c + 2 * E a b c := by
    intro a b c
    dsimp [D, P, Q, R, E]
    by_cases hab : a = b <;> by_cases hac : a = c <;> by_cases hbc : b = c <;>
      simp_all <;> omega
  have hinner : ∀ a b,
      rsum n (fun c => D a b c) =
        rsum n (fun c => f a b c) - rsum n (fun c => P a b c) -
        rsum n (fun c => Q a b c) - rsum n (fun c => R a b c) +
        2 * rsum n (fun c => E a b c) := by
    intro a b
    rw [show (fun c => D a b c) = (fun c => f a b c - P a b c - Q a b c - R a b c +
        2 * E a b c) by funext c; exact hpoint a b c]
    rw [rsum_add, rsum_sub, rsum_sub, rsum_sub, rsum_const_mul]
  have hmid : ∀ a, a < n →
      rsum n (fun b => rsum n (fun c => D a b c)) =
        rsum n (fun b => rsum n (fun c => f a b c)) -
        rsum n (fun c => f a a c) -
        rsum n (fun b => f a b a) -
        rsum n (fun b => f a b b) +
        2 * f a a a := by
    intro a ha
    rw [show (fun b => rsum n (fun c => D a b c)) =
        (fun b => rsum n (fun c => f a b c) - rsum n (fun c => P a b c) -
          rsum n (fun c => Q a b c) - rsum n (fun c => R a b c) +
          2 * rsum n (fun c => E a b c)) by funext b; exact hinner a b]
    rw [rsum_add, rsum_sub, rsum_sub, rsum_sub, rsum_const_mul]
    have hP : rsum n (fun b => rsum n (fun c => P a b c)) = rsum n (fun c => f a a c) := by
      rw [show (fun b => rsum n (fun c => P a b c)) =
          (fun b => if a = b then rsum n (fun c => f a b c) else 0) by
        funext b
        dsimp [P]
        by_cases hab : a = b
        · subst b; simp
        · simp [hab, rsum_const_zero]]
      rw [rsum_ite_single n a (fun b => rsum n (fun c => f a b c))]
      simp [ha]
    have hQ : rsum n (fun b => rsum n (fun c => Q a b c)) = rsum n (fun b => f a b a) := by
      rw [show (fun b => rsum n (fun c => Q a b c)) =
          (fun b => if a < n then f a b a else 0) by
        funext b
        dsimp [Q]
        rw [rsum_ite_single n a (fun c => f a b c)]]
      simp [ha]
    have hR : rsum n (fun b => rsum n (fun c => R a b c)) = rsum n (fun b => f a b b) := by
      apply rsum_congr
      intro b hb
      dsimp [R]
      rw [rsum_ite_single n b (fun c => f a b c)]
      simp [hb]
    have hE : rsum n (fun b => rsum n (fun c => E a b c)) = f a a a := by
      rw [show (fun b => rsum n (fun c => E a b c)) =
          (fun b => if a = b then (if b < n then f a b b else 0) else 0) by
        funext b
        dsimp [E]
        by_cases hab : a = b
        · subst b
          simp
          rw [rsum_ite_single n a (fun c => f a a c)]
        · simp [hab, rsum_const_zero]]
      rw [show (fun b => if a = b then (if b < n then f a b b else 0) else 0) =
          (fun b => if a = b then f a b b else 0) by
        funext b
        by_cases hab : a = b
        · subst b
          simp [ha]
        · simp [hab]]
      rw [rsum_ite_single n a (fun b => f a b b)]
      simp [ha]
    rw [hP, hQ, hR, hE]
  calc
    rsum n (fun a => rsum n (fun b => rsum n (fun c => D a b c)))
        = rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c)) -
            rsum n (fun c => f a a c) - rsum n (fun b => f a b a) -
            rsum n (fun b => f a b b) + 2 * f a a a) := by
              apply rsum_congr
              intro a ha
              exact hmid a ha
    _ = rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c))) -
        rsum n (fun a => rsum n (fun c => f a a c)) -
        rsum n (fun a => rsum n (fun b => f a b a)) -
        rsum n (fun a => rsum n (fun b => f a b b)) +
        2 * rsum n (fun a => f a a a) := by
          rw [rsum_add, rsum_sub, rsum_sub, rsum_sub, rsum_const_mul]

private theorem rsum_ofNat (n : Nat) (f : Nat → Nat) :
    rsum n (fun x => (f x : Int)) = (((List.range n).map f).sum : Int) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih, List.range_succ, List.map_append, List.sum_append]
      simp

private theorem ind_sum_count (n : Nat) (P : Nat → Bool) :
    ((List.range n).map (fun x => if P x then (1 : Nat) else 0)).sum =
      (List.range n).countP P := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rw [List.countP_append]
      cases P n <;> simp

private theorem ind_sum_count_int (n : Nat) (P : Nat → Bool) :
    rsum n (fun x => if P x then (1 : Int) else 0) = ((List.range n).countP P : Int) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih, List.range_succ, List.countP_append]
      by_cases hpn : P n = true <;> simp [hpn]

private theorem nat_mul_pow (x y n : Nat) : (x * y) ^ n = x ^ n * y ^ n := by
  induction n with
  | zero => simp
  | succ n ih => rw [Nat.pow_succ, ih, Nat.pow_succ, Nat.pow_succ]; ac_rfl

private theorem int_toNat_pow (x : Int) (hx : 0 ≤ x) (n : Nat) :
    (x ^ n).toNat = x.toNat ^ n := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show x ^ (n+1) = x ^ n * x by simpa [Nat.succ_eq_add_one] using Int.pow_succ x n]
      rw [Int.toNat_mul (Int.pow_nonneg hx) hx]
      rw [ih]
      rw [Nat.pow_succ]

/-! Feasibility of the exponent system for a fractional edge cover. -/

private theorem cover_feasible (w0 w1 w2 w3 w4 den : Nat)
    (h1 : den ≤ w0 + w1) (h2 : den ≤ w1 + w2 + w4) (h3 : den ≤ w2 + w3) :
    ∃ a b c d : Nat, a + b + c + d = den ∧ a + d ≤ w0 ∧ b + c ≤ w1 ∧
      a + c ≤ w2 ∧ b + d ≤ w3 ∧ d ≤ w4 := by
  let A := min w0 (min w2 den)
  let B := min w1 (min w3 (den - A))
  let C := min (w2 - A) (min (w1 - B) (den - A - B))
  let D := den - A - B - C
  refine ⟨A, B, C, D, ?_⟩
  dsimp [A, B, C, D]
  have hA1 : min w0 (min w2 den) ≤ w0 := Nat.min_le_left _ _
  have hA2 : min w0 (min w2 den) ≤ min w2 den := Nat.min_le_right _ _
  have hA3 : min w2 den ≤ w2 := Nat.min_le_left _ _
  have hA4 : min w2 den ≤ den := Nat.min_le_right _ _
  have hB1 : min w1 (min w3 (den - min w0 (min w2 den))) ≤ w1 := Nat.min_le_left _ _
  have hB2 : min w1 (min w3 (den - min w0 (min w2 den))) ≤ min w3 (den - min w0 (min w2 den)) := Nat.min_le_right _ _
  have hB3 : min w3 (den - min w0 (min w2 den)) ≤ w3 := Nat.min_le_left _ _
  have hB4 : min w3 (den - min w0 (min w2 den)) ≤ den - min w0 (min w2 den) := Nat.min_le_right _ _
  have hC1 : min (w2 - min w0 (min w2 den)) (min (w1 - min w1 (min w3 (den - min w0 (min w2 den)))) (den - min w0 (min w2 den) - min w1 (min w3 (den - min w0 (min w2 den))))) ≤ w2 - min w0 (min w2 den) := Nat.min_le_left _ _
  have hC2 : min (w2 - min w0 (min w2 den)) (min (w1 - min w1 (min w3 (den - min w0 (min w2 den)))) (den - min w0 (min w2 den) - min w1 (min w3 (den - min w0 (min w2 den))))) ≤ min (w1 - min w1 (min w3 (den - min w0 (min w2 den)))) (den - min w0 (min w2 den) - min w1 (min w3 (den - min w0 (min w2 den)))) := Nat.min_le_right _ _
  have hC3 : min (w1 - min w1 (min w3 (den - min w0 (min w2 den)))) (den - min w0 (min w2 den) - min w1 (min w3 (den - min w0 (min w2 den)))) ≤ w1 - min w1 (min w3 (den - min w0 (min w2 den))) := Nat.min_le_left _ _
  have hC4 : min (w1 - min w1 (min w3 (den - min w0 (min w2 den)))) (den - min w0 (min w2 den) - min w1 (min w3 (den - min w0 (min w2 den)))) ≤ den - min w0 (min w2 den) - min w1 (min w3 (den - min w0 (min w2 den))) := Nat.min_le_right _ _
  omega

/-! ReturnedItem: CollisionCorrection. -/

theorem sound_FractionalCap : FractionalCap.Goal := by
  intro n db b weights den hcov
  simp [cover, pattern, reference, integrate, put, value, support, List.mem_cons, List.find?] at hcov ⊢
  rcases weights with _ | ⟨w0, _ | ⟨w1, _ | ⟨w2, _ | ⟨w3, _ | ⟨w4, ws⟩⟩⟩⟩⟩
  · simp at hcov
  · simp at hcov
  · simp at hcov
  · simp at hcov
  · simp at hcov
  · simp at hcov
    rcases hcov with ⟨hden, hws, hc2, hc3a, hc3b⟩
    subst ws
    have hc3 : den ≤ w1 + w2 + w4 := by omega
    let p0 : Nat → Int := fun x => if db 0 (b 0) x = 0 then 0 else 1
    let p1 : Nat → Nat → Int := fun x y => if db 1 x y = 0 then 0 else 1
    let p2 : Nat → Nat → Int := fun y z => if db 2 y z = 0 then 0 else 1
    let p3 : Nat → Int := fun z => if db 3 z (b 0) = 0 then 0 else 1
    let p4 : Nat → Int := fun y => if db 2 (b 0) y = 0 then 0 else 1
    let S : Int := rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      p0 x * p1 x y * p2 y z * p3 z * p4 y)))
    let s0 : Int := rsum n p0
    let s1 : Int := rsum n (fun x => rsum n (fun y => p1 x y))
    let s2 : Int := rsum n (fun y => rsum n (fun z => p2 y z))
    let s3 : Int := rsum n p3
    let s4 : Int := rsum n p4
    have hp0n : ∀ x, 0 ≤ p0 x := by intro x; dsimp [p0]; split <;> omega
    have hp0l : ∀ x, p0 x ≤ 1 := by intro x; dsimp [p0]; split <;> omega
    have hp1n : ∀ x y, 0 ≤ p1 x y := by intro x y; dsimp [p1]; split <;> omega
    have hp1l : ∀ x y, p1 x y ≤ 1 := by intro x y; dsimp [p1]; split <;> omega
    have hp2n : ∀ y z, 0 ≤ p2 y z := by intro y z; dsimp [p2]; split <;> omega
    have hp2l : ∀ y z, p2 y z ≤ 1 := by intro y z; dsimp [p2]; split <;> omega
    have hp3n : ∀ z, 0 ≤ p3 z := by intro z; dsimp [p3]; split <;> omega
    have hp3l : ∀ z, p3 z ≤ 1 := by intro z; dsimp [p3]; split <;> omega
    have hp4n : ∀ y, 0 ≤ p4 y := by intro y; dsimp [p4]; split <;> omega
    have hp4l : ∀ y, p4 y ≤ 1 := by intro y; dsimp [p4]; split <;> omega
    have hpt1 : ∀ x y z, p0 x * p1 x y * p2 y z * p3 z * p4 y ≤ p0 x * p2 y z := by
      intro x y z
      dsimp [p0,p1,p2,p3,p4]
      by_cases h0 : db 0 (b 0) x = 0 <;> by_cases h1 : db 1 x y = 0 <;>
        by_cases h2 : db 2 y z = 0 <;> by_cases h3 : db 3 z (b 0) = 0 <;>
        by_cases h4 : db 2 (b 0) y = 0 <;> simp [h0,h1,h2,h3,h4]
    have hpt2 : ∀ x y z, p0 x * p1 x y * p2 y z * p3 z * p4 y ≤ p1 x y * p3 z := by
      intro x y z
      dsimp [p0,p1,p2,p3,p4]
      by_cases h0 : db 0 (b 0) x = 0 <;> by_cases h1 : db 1 x y = 0 <;>
        by_cases h2 : db 2 y z = 0 <;> by_cases h3 : db 3 z (b 0) = 0 <;>
        by_cases h4 : db 2 (b 0) y = 0 <;> simp [h0,h1,h2,h3,h4]
    have hpt3 : ∀ x y z, p0 x * p1 x y * p2 y z * p3 z * p4 y ≤ p0 x * p3 z * p4 y := by
      intro x y z
      dsimp [p0,p1,p2,p3,p4]
      by_cases h0 : db 0 (b 0) x = 0 <;> by_cases h1 : db 1 x y = 0 <;>
        by_cases h2 : db 2 y z = 0 <;> by_cases h3 : db 3 z (b 0) = 0 <;>
        by_cases h4 : db 2 (b 0) y = 0 <;> simp [h0,h1,h2,h3,h4]
    have hpt5 : ∀ x y z, p0 x * p1 x y * p2 y z * p3 z * p4 y ≤ p1 x y * p2 y z := by
      intro x y z
      dsimp [p0,p1,p2,p3,p4]
      by_cases h0 : db 0 (b 0) x = 0 <;> by_cases h1 : db 1 x y = 0 <;>
        by_cases h2 : db 2 y z = 0 <;> by_cases h3 : db 3 z (b 0) = 0 <;>
        by_cases h4 : db 2 (b 0) y = 0 <;> simp [h0,h1,h2,h3,h4]
    have hE1 : S ≤ s0 * s2 := by
      dsimp [S, s0, s2]
      calc
        rsum n (fun x => rsum n (fun y => rsum n (fun z =>
            p0 x * p1 x y * p2 y z * p3 z * p4 y)))
            ≤ rsum n (fun x => rsum n (fun y => rsum n (fun z => p0 x * p2 y z))) := by
                apply rsum_le; intro x hx
                apply rsum_le; intro y hy
                apply rsum_le; intro z hz
                exact hpt1 x y z
        _ = rsum n (fun x => rsum n (fun y => p0 x * rsum n (fun z => p2 y z))) := by
                apply rsum_congr; intro x hx
                apply rsum_congr; intro y hy
                rw [rsum_const_mul]
        _ = rsum n (fun x => p0 x * rsum n (fun y => rsum n (fun z => p2 y z))) := by
                apply rsum_congr; intro x hx
                rw [rsum_const_mul]
        _ = rsum n p0 * rsum n (fun y => rsum n (fun z => p2 y z)) := by
                rw [rsum_mul_const_right]
    have hE2 : S ≤ s1 * s3 := by
      dsimp [S, s1, s3]
      calc
        rsum n (fun x => rsum n (fun y => rsum n (fun z =>
            p0 x * p1 x y * p2 y z * p3 z * p4 y)))
            ≤ rsum n (fun x => rsum n (fun y => rsum n (fun z => p1 x y * p3 z))) := by
                apply rsum_le; intro x hx
                apply rsum_le; intro y hy
                apply rsum_le; intro z hz
                exact hpt2 x y z
        _ = rsum n (fun x => rsum n (fun y => p1 x y * rsum n p3)) := by
                apply rsum_congr; intro x hx
                apply rsum_congr; intro y hy
                rw [rsum_const_mul]
        _ = rsum n (fun x => rsum n (fun y => p1 x y) * rsum n p3) := by
                apply rsum_congr; intro x hx
                rw [rsum_mul_const_right]
        _ = rsum n (fun x => rsum n (fun y => p1 x y)) * rsum n p3 := by
                rw [rsum_mul_const_right]
    have hE3 : S ≤ s0 * s3 * s4 := by
      dsimp [S, s0, s3, s4]
      calc
        rsum n (fun x => rsum n (fun y => rsum n (fun z =>
            p0 x * p1 x y * p2 y z * p3 z * p4 y)))
            ≤ rsum n (fun x => rsum n (fun y => rsum n (fun z => p0 x * p3 z * p4 y))) := by
                apply rsum_le; intro x hx
                apply rsum_le; intro y hy
                apply rsum_le; intro z hz
                exact hpt3 x y z
        _ = rsum n (fun x => rsum n (fun y => rsum n (fun z => (p0 x * p4 y) * p3 z))) := by
                apply rsum_congr; intro x hx; apply rsum_congr; intro y hy; apply rsum_congr; intro z hz
                ac_rfl
        _ = rsum n (fun x => rsum n (fun y => (p0 x * p4 y) * rsum n p3)) := by
                apply rsum_congr; intro x hx; apply rsum_congr; intro y hy
                rw [rsum_const_mul]
        _ = rsum n (fun x => rsum n (fun y => (p0 x * p4 y)) * rsum n p3) := by
                apply rsum_congr; intro x hx; rw [rsum_mul_const_right]
        _ = rsum n (fun x => p0 x * rsum n p4 * rsum n p3) := by
                apply rsum_congr; intro x hx
                rw [show rsum n (fun y => p0 x * p4 y) = p0 x * rsum n p4 by rw [rsum_const_mul]]
        _ = rsum n p0 * rsum n p4 * rsum n p3 := by
                rw [show (fun x => p0 x * rsum n p4 * rsum n p3) =
                    (fun x => (p0 x * rsum n p4) * rsum n p3) by funext x; ac_rfl]
                rw [rsum_mul_const_right]
                rw [show rsum n (fun x => p0 x * rsum n p4) = rsum n p0 * rsum n p4 by
                  rw [rsum_mul_const_right]]
        _ = rsum n p0 * rsum n p3 * rsum n p4 := by ac_rfl
    have hE5 : S ≤ s1 * s2 := by
      dsimp [S, s1, s2]
      calc
        rsum n (fun x => rsum n (fun y => rsum n (fun z =>
            p0 x * p1 x y * p2 y z * p3 z * p4 y)))
            ≤ rsum n (fun x => rsum n (fun y => rsum n (fun z => p1 x y * p2 y z))) := by
                apply rsum_le; intro x hx
                apply rsum_le; intro y hy
                apply rsum_le; intro z hz
                exact hpt5 x y z
        _ = rsum n (fun y => rsum n (fun x => rsum n (fun z => p1 x y * p2 y z))) := by
                rw [rsum_comm]
        _ = rsum n (fun y => (rsum n (fun x => p1 x y)) * (rsum n (fun z => p2 y z))) := by
                apply rsum_congr; intro y hy; rw [rsum_prod_sep]
        _ ≤ rsum n (fun y => (rsum n (fun x => p1 x y)) * (rsum n (fun y' => rsum n (fun z => p2 y' z)))) := by
                apply rsum_le; intro y hy
                exact Int.mul_le_mul_of_nonneg_left
                  (rsum_le_total n (fun y' => rsum n (fun z => p2 y' z))
                    (fun y' hy' => rsum_nonneg n (fun z => p2 y' z) (fun z hz => hp2n y' z)) y hy)
                  (rsum_nonneg n (fun x => p1 x y) (fun x hx => hp1n x y))
        _ = rsum n (fun x => rsum n (fun y => p1 x y)) * rsum n (fun y' => rsum n (fun z => p2 y' z)) := by
                rw [rsum_mul_const_right]
                rw [rsum_comm]
    have hS0 : 0 ≤ S := by
      dsimp [S]
      apply rsum_nonneg; intro x hx
      apply rsum_nonneg; intro y hy
      apply rsum_nonneg; intro z hz
      exact Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg (hp0n x) (hp1n x y)) (hp2n y z)) (hp3n z)) (hp4n y)
    have hs00 : 0 ≤ s0 := by dsimp [s0]; apply rsum_nonneg; intro x hx; exact hp0n x
    have hs11 : 0 ≤ s1 := by dsimp [s1]; apply rsum_nonneg; intro x hx; apply rsum_nonneg; intro y hy; exact hp1n x y
    have hs22 : 0 ≤ s2 := by dsimp [s2]; apply rsum_nonneg; intro y hy; apply rsum_nonneg; intro z hz; exact hp2n y z
    have hs33 : 0 ≤ s3 := by dsimp [s3]; apply rsum_nonneg; intro z hz; exact hp3n z
    have hs44 : 0 ≤ s4 := by dsimp [s4]; apply rsum_nonneg; intro y hy; exact hp4n y
    let N : Nat := S.toNat
    let N0 : Nat := s0.toNat
    let N1 : Nat := s1.toNat
    let N2 : Nat := s2.toNat
    let N3 : Nat := s3.toNat
    let N4 : Nat := s4.toNat
    have hE1N : N ≤ N0 * N2 := by
      dsimp [N, N0, N2]
      rw [← Int.toNat_mul hs00 hs22]
      exact Int.toNat_le_toNat hE1
    have hE2N : N ≤ N1 * N3 := by
      dsimp [N, N1, N3]
      rw [← Int.toNat_mul hs11 hs33]
      exact Int.toNat_le_toNat hE2
    have hE3N : N ≤ N0 * N3 * N4 := by
      dsimp [N, N0, N3, N4]
      rw [← show (s0 * s3 * s4).toNat = s0.toNat * s3.toNat * s4.toNat by
        rw [Int.toNat_mul (Int.mul_nonneg hs00 hs33) hs44]
        rw [Int.toNat_mul hs00 hs33]]
      exact Int.toNat_le_toNat hE3
    have hE5N : N ≤ N1 * N2 := by
      dsimp [N, N1, N2]
      rw [← Int.toNat_mul hs11 hs22]
      exact Int.toNat_le_toNat hE5
    rcases cover_feasible w0 w1 w2 w3 w4 den hc2 hc3 hc3b with
      ⟨aa, bb, cc, dd, hsum, haa, hbb, hcc, hbb3, hdd⟩
    have hcombN : N ^ den ≤ N0 ^ w0 * N1 ^ w1 * N2 ^ w2 * N3 ^ w3 * N4 ^ w4 := by
      by_cases hN0 : N0 = 0
      · have hN : N = 0 := by
          have htmp : N ≤ 0 := by simpa [hN0] using hE1N
          omega
        rw [hN, Nat.zero_pow hden]
        exact Nat.zero_le _
      by_cases hN1 : N1 = 0
      · have hN : N = 0 := by
          have htmp : N ≤ 0 := by simpa [hN1] using hE2N
          omega
        rw [hN, Nat.zero_pow hden]
        exact Nat.zero_le _
      by_cases hN2 : N2 = 0
      · have hN : N = 0 := by
          have htmp : N ≤ 0 := by simpa [hN2] using hE1N
          omega
        rw [hN, Nat.zero_pow hden]
        exact Nat.zero_le _
      by_cases hN3 : N3 = 0
      · have hN : N = 0 := by
          have htmp : N ≤ 0 := by simpa [hN3] using hE2N
          omega
        rw [hN, Nat.zero_pow hden]
        exact Nat.zero_le _
      by_cases hN4 : N4 = 0
      · have hN : N = 0 := by
          have htmp : N ≤ 0 := by simpa [hN4] using hE3N
          omega
        rw [hN, Nat.zero_pow hden]
        exact Nat.zero_le _
      have hN0pos : 0 < N0 := by omega
      have hN1pos : 0 < N1 := by omega
      have hN2pos : 0 < N2 := by omega
      have hN3pos : 0 < N3 := by omega
      have hN4pos : 0 < N4 := by omega
      have hp1' : N ^ aa ≤ N0 ^ aa * N2 ^ aa := by
        simpa [nat_mul_pow] using (Nat.pow_le_pow_left hE1N aa)
      have hp2' : N ^ bb ≤ N1 ^ bb * N3 ^ bb := by
        simpa [nat_mul_pow] using (Nat.pow_le_pow_left hE2N bb)
      have hp5' : N ^ cc ≤ N1 ^ cc * N2 ^ cc := by
        simpa [nat_mul_pow] using (Nat.pow_le_pow_left hE5N cc)
      have hp3' : N ^ dd ≤ N0 ^ dd * N3 ^ dd * N4 ^ dd := by
        have ht : N ^ dd ≤ (N0 * N3 * N4) ^ dd := Nat.pow_le_pow_left hE3N dd
        simpa [nat_mul_pow] using ht
      have hNpow : N ^ den = N ^ aa * N ^ bb * N ^ cc * N ^ dd := by
        rw [← hsum]
        repeat rw [Nat.pow_add]
      rw [hNpow]
      calc
        N ^ aa * N ^ bb * N ^ cc * N ^ dd
            ≤ (N0 ^ aa * N2 ^ aa) * (N1 ^ bb * N3 ^ bb) *
                (N1 ^ cc * N2 ^ cc) * (N0 ^ dd * N3 ^ dd * N4 ^ dd) := by
                  exact Nat.mul_le_mul (Nat.mul_le_mul (Nat.mul_le_mul hp1' hp2') hp5') hp3'
        _ = (N0 ^ aa * N0 ^ dd) * (N1 ^ bb * N1 ^ cc) *
                (N2 ^ aa * N2 ^ cc) * (N3 ^ bb * N3 ^ dd) * N4 ^ dd := by ac_rfl
        _ ≤ N0 ^ w0 * N1 ^ w1 * N2 ^ w2 * N3 ^ w3 * N4 ^ w4 := by
              apply Nat.mul_le_mul
              · apply Nat.mul_le_mul
                · apply Nat.mul_le_mul
                  · apply Nat.mul_le_mul
                    · rw [← Nat.pow_add]
                      exact Nat.pow_le_pow_right hN0pos haa
                    · rw [← Nat.pow_add]
                      exact Nat.pow_le_pow_right hN1pos hbb
                  · rw [← Nat.pow_add]
                    exact Nat.pow_le_pow_right hN2pos hcc
                · rw [← Nat.pow_add]
                  exact Nat.pow_le_pow_right hN3pos hbb3
              · exact Nat.pow_le_pow_right hN4pos hdd
    have hp0eq : ∀ x, p0 x = (if db 0 (b 0) x != 0 then (1 : Int) else 0) := by
      intro x
      dsimp [p0]
      by_cases h : db 0 (b 0) x = 0
      · have hb : (db 0 (b 0) x != 0) = false := by simp [h]
        simp [h, hb]
      · have hb : (db 0 (b 0) x != 0) = true := by cases h' : (db 0 (b 0) x != 0) <;> simp_all
        simp [h, hb]
    have hp1eq : ∀ x y, p1 x y = (if db 1 x y != 0 then (1 : Int) else 0) := by
      intro x y
      dsimp [p1]
      by_cases h : db 1 x y = 0
      · have hb : (db 1 x y != 0) = false := by simp [h]
        simp [h, hb]
      · have hb : (db 1 x y != 0) = true := by cases h' : (db 1 x y != 0) <;> simp_all
        simp [h, hb]
    have hp2eq : ∀ y z, p2 y z = (if db 2 y z != 0 then (1 : Int) else 0) := by
      intro y z
      dsimp [p2]
      by_cases h : db 2 y z = 0
      · have hb : (db 2 y z != 0) = false := by simp [h]
        simp [h, hb]
      · have hb : (db 2 y z != 0) = true := by cases h' : (db 2 y z != 0) <;> simp_all
        simp [h, hb]
    have hp3eq : ∀ z, p3 z = (if db 3 z (b 0) != 0 then (1 : Int) else 0) := by
      intro z
      dsimp [p3]
      by_cases h : db 3 z (b 0) = 0
      · have hb : (db 3 z (b 0) != 0) = false := by simp [h]
        simp [h, hb]
      · have hb : (db 3 z (b 0) != 0) = true := by cases h' : (db 3 z (b 0) != 0) <;> simp_all
        simp [h, hb]
    have hp4eq : ∀ y, p4 y = (if db 2 (b 0) y != 0 then (1 : Int) else 0) := by
      intro y
      dsimp [p4]
      by_cases h : db 2 (b 0) y = 0
      · have hb : (db 2 (b 0) y != 0) = false := by simp [h]
        simp [h, hb]
      · have hb : (db 2 (b 0) y != 0) = true := by cases h' : (db 2 (b 0) y != 0) <;> simp_all
        simp [h, hb]
    have hs0eq : s0 = ((List.range n).countP (fun y => db 0 (b 0) y != 0) : Int) := by
      dsimp [s0]
      rw [← ind_sum_count_int n (fun y => db 0 (b 0) y != 0)]
      apply rsum_congr
      intro x hx
      exact hp0eq x
    have hs4eq : s4 = ((List.range n).countP (fun y => db 2 (b 0) y != 0) : Int) := by
      dsimp [s4]
      rw [← ind_sum_count_int n (fun y => db 2 (b 0) y != 0)]
      apply rsum_congr
      intro x hx
      exact hp4eq x
    have hs1eq : s1 = (((List.map (fun x => (List.range n).countP (fun y => db 1 x y != 0)) (List.range n)).sum : Nat) : Int) := by
      dsimp [s1]
      rw [← rsum_ofNat n (fun x => (List.range n).countP (fun y => db 1 x y != 0))]
      apply rsum_congr
      intro x hx
      rw [← ind_sum_count_int n (fun y => db 1 x y != 0)]
      apply rsum_congr
      intro y hy
      exact hp1eq x y
    have hs2eq : s2 = (((List.map (fun x => (List.range n).countP (fun y => db 2 x y != 0)) (List.range n)).sum : Nat) : Int) := by
      dsimp [s2]
      rw [← rsum_ofNat n (fun x => (List.range n).countP (fun y => db 2 x y != 0))]
      apply rsum_congr
      intro x hx
      rw [← ind_sum_count_int n (fun y => db 2 x y != 0)]
      apply rsum_congr
      intro y hy
      exact hp2eq x y
    have hs3eq : s3 = (((List.map (fun x => if db 3 x (b 0) = 0 then (0 : Nat) else 1) (List.range n)).sum : Nat) : Int) := by
      dsimp [s3]
      rw [← rsum_ofNat n (fun x => if db 3 x (b 0) = 0 then (0 : Nat) else 1)]
      apply rsum_congr
      intro x hx
      dsimp [p3]
      by_cases h : db 3 x (b 0) = 0 <;> simp [h]
    have hsup0 : (List.range n).countP (fun y => db 0 (b 0) y != 0) = N0 := by
      dsimp [N0]
      rw [hs0eq]
      simp
    have hsup4 : (List.range n).countP (fun y => db 2 (b 0) y != 0) = N4 := by
      dsimp [N4]
      rw [hs4eq]
      simp
    have hsup1 : (List.map (fun x => (List.range n).countP (fun y => db 1 x y != 0)) (List.range n)).sum = N1 := by
      dsimp [N1]
      rw [hs1eq]
      simp
    have hsup2 : (List.map (fun x => (List.range n).countP (fun y => db 2 x y != 0)) (List.range n)).sum = N2 := by
      dsimp [N2]
      rw [hs2eq]
      simp
    have hsup3 : (List.map (fun x => if db 3 x (b 0) = 0 then (0 : Nat) else 1) (List.range n)).sum = N3 := by
      dsimp [N3]
      rw [hs3eq]
      simp
    simp
    rw [hsup0, hsup1, hsup2, hsup3, hsup4]
    have hS_eq :
        ((List.map (fun x =>
          (List.map (fun y =>
            (List.map (fun z => (if db 0 (b 0) x = 0 then (0 : Int) else 1) *
              ((if db 1 x y = 0 then (0 : Int) else 1) *
                ((if db 2 y z = 0 then (0 : Int) else 1) *
                  ((if db 3 z (b 0) = 0 then (0 : Int) else 1) *
                    (if db 2 (b 0) y = 0 then (0 : Int) else 1)))))
              (List.range n)).sum)
            (List.range n)).sum)
          (List.range n)).sum) = S := by
      dsimp [S, p0, p1, p2, p3, p4]
      simp [rsum, Int.mul_assoc]
    rw [hS_eq]
    rw [show N0 ^ w0 * (N1 ^ w1 * (N2 ^ w2 * (N3 ^ w3 * N4 ^ w4))) =
        N0 ^ w0 * N1 ^ w1 * N2 ^ w2 * N3 ^ w3 * N4 ^ w4 by ac_rfl]
    change N ^ den ≤ N0 ^ w0 * N1 ^ w1 * N2 ^ w2 * N3 ^ w3 * N4 ^ w4
    exact hcombN

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  let W : Nat → Nat → Nat → Int := fun x y z =>
    db 0 (b 0) x * (db 1 x y * (db 2 y z * (db 3 z (b 0) * db 2 (b 0) y)))
  simp [injective, collapsed, partitions, mobius, factorial, pattern, integrate, put, value,
    List.mem_cons, List.find?]
  change
    rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      if (x ≠ y ∧ x ≠ z) ∧ y ≠ z then W x y z else 0))) =
    rsum n (fun x => rsum n (fun y => rsum n (fun z => W x y z))) +
    (-(rsum n (fun x => rsum n (fun z => W x x z))) +
      (-(rsum n (fun y => rsum n (fun x => W x y x))) +
        (-(rsum n (fun x => rsum n (fun y => W x y y))) +
          2 * rsum n (fun x => W x x x))))
  have h := triple_inclusion n W
  have hfun : (fun x => rsum n (fun y => rsum n (fun z =>
      if (x ≠ y ∧ x ≠ z) ∧ y ≠ z then W x y z else 0))) =
      (fun x => rsum n (fun y => rsum n (fun z =>
      if x ≠ y ∧ x ≠ z ∧ y ≠ z then W x y z else 0))) := by
        funext x; apply rsum_congr; intro y hy; apply rsum_congr; intro z hz
        by_cases hxy : x = y <;> by_cases hxz : x = z <;> by_cases hyz : y = z <;> simp [hxy, hxz, hyz]
  rw [hfun]
  rw [h]
  have hswap : rsum n (fun a => rsum n (fun b => W a b a)) =
      rsum n (fun b => rsum n (fun a => W a b a)) :=
    rsum_comm n n (fun a b => W a b a)
  rw [hswap]
  omega
