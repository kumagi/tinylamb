import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `KeywordCompany.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `KeywordCompany.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.KeywordCompanyCapsAndCollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany

/-! Finite range-sum helpers (Std only). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

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

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        exact rsum_zero _]
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

private theorem rsum_prod2 (n m : Nat) (f : Nat → Int) (g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

private theorem nat_sum_cast (l : List Nat) : ((l.sum : Nat) : Int) = (l.map (fun x : Nat => (x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem length_filter_eq_sum {α} (l : List α) (p : α → Bool) :
    (l.filter p).length = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a = true <;> simp [h, ih] <;> omega

/-! Indicator database: all atom values are 0 or 1. -/

private def ind (db : DB) : DB := fun r x y => if db r x y == 0 then 0 else 1

private theorem ind_nonneg (db : DB) (r x y : Nat) : 0 ≤ ind db r x y := by
  by_cases h : db r x y == 0 <;> simp [ind, h]

private theorem ind_le_one (db : DB) (r x y : Nat) : ind db r x y ≤ 1 := by
  by_cases h : db r x y == 0 <;> simp [ind, h]

private theorem ind01 (db : DB) (r x y : Nat) : ind db r x y = 0 ∨ ind db r x y = 1 := by
  by_cases h : db r x y == 0
  · left; simp [ind, h]
  · right; simp [ind, h]

private theorem ind_ne_zero (db : DB) (r x y : Nat) :
    (ind db r x y != 0) = (db r x y != 0) := by
  by_cases h : db r x y == 0
  · have h' : db r x y = 0 := beq_iff_eq.mp h
    simp [ind, h, h']
  · have h' : db r x y ≠ 0 := by intro he; exact h (by simp [he])
    have hb : (db r x y != 0) = true := by simpa using h'
    simp [ind, h, hb]

private theorem mul_le_self_of01 (a b : Int) (ha0 : 0 ≤ a) (hb : b = 0 ∨ b = 1) : a * b ≤ a := by
  rcases hb with rfl | rfl
  · simpa using ha0
  · simp

private theorem mul_le_mul_right_nonneg {a b c : Int} (h : a ≤ b) (hc : 0 ≤ c) : a * c ≤ b * c := by
  by_cases hc0 : c = 0
  · subst c
    simp
  · have hcpos : 0 < c := by omega
    exact (Int.mul_le_mul_right hcpos).mpr h

private theorem term_le_rsum (n : Nat) (f : Nat → Int)
    (h : ∀ i, i < n → 0 ≤ f i) (z : Nat) (hz : z < n) : f z ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases hzn : z = n
      · subst z
        have hn : 0 ≤ rsum n f := rsum_nonneg n f (fun i hi => h i (Nat.lt_succ_of_lt hi))
        omega
      · have hzlt : z < n := by omega
        have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi)) hzlt
        have hn : 0 ≤ f n := h n (Nat.lt_succ_self n)
        omega

/-! Closed forms for reference and supports of the four atoms. -/

private theorem reference_form (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (ind db) b =
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        (ind db 0 (b 0) x) * ((ind db 1 (b 0) y) *
          ((ind db 2 x z) * (ind db 3 y z)))))) := by
  simp [reference, integrate, pattern, value, put, rsum]

private theorem support0_int (n : Nat) (db : DB) (b : Bind) :
    (support n db ⟨0, 0, 1⟩ pattern b : Int) = rsum n (fun x => ind db 0 (b 0) x) := by
  simp [support, pattern, List.countP_eq_length_filter, ind_ne_zero]
  rw [length_filter_eq_sum, nat_sum_cast, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  by_cases h : db 0 (b 0) x == 0
  · have h' : db 0 (b 0) x = 0 := beq_iff_eq.mp h
    simp [ind, h']
  · have h' : db 0 (b 0) x ≠ 0 := by intro he; exact h (by simp [he])
    simp [ind, h']

private theorem support1_int (n : Nat) (db : DB) (b : Bind) :
    (support n db ⟨1, 0, 2⟩ pattern b : Int) = rsum n (fun x => ind db 1 (b 0) x) := by
  simp [support, pattern, List.countP_eq_length_filter, ind_ne_zero]
  rw [length_filter_eq_sum, nat_sum_cast, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  by_cases h : db 1 (b 0) x == 0
  · have h' : db 1 (b 0) x = 0 := beq_iff_eq.mp h
    simp [ind, h']
  · have h' : db 1 (b 0) x ≠ 0 := by intro he; exact h (by simp [he])
    simp [ind, h']

private theorem support2_int (n : Nat) (db : DB) (b : Bind) :
    (support n db ⟨2, 1, 3⟩ pattern b : Int) =
      rsum n (fun x => rsum n (fun z => ind db 2 x z)) := by
  simp [support, pattern, List.countP_eq_length_filter, ind_ne_zero]
  rw [show (List.map (fun x => ((List.range n).filter (fun y => db 2 x y != 0)).length)
      (List.range n)).sum =
      (List.map (fun x => (List.map (fun y => (if db 2 x y != 0 then (1:Nat) else 0))
        (List.range n)).sum) (List.range n)).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact length_filter_eq_sum (List.range n) (fun y => db 2 x y != 0)]
  rw [nat_sum_cast, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  rw [nat_sum_cast, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro z hz
  by_cases h : db 2 x z == 0
  · have h' : db 2 x z = 0 := beq_iff_eq.mp h
    simp [ind, h']
  · have h' : db 2 x z ≠ 0 := by intro he; exact h (by simp [he])
    simp [ind, h']

private theorem support3_int (n : Nat) (db : DB) (b : Bind) :
    (support n db ⟨3, 2, 3⟩ pattern b : Int) =
      rsum n (fun y => rsum n (fun z => ind db 3 y z)) := by
  simp [support, pattern, List.countP_eq_length_filter, ind_ne_zero]
  rw [show (List.map (fun x => ((List.range n).filter (fun y => db 3 x y != 0)).length)
      (List.range n)).sum =
      (List.map (fun x => (List.map (fun y => (if db 3 x y != 0 then (1:Nat) else 0))
        (List.range n)).sum) (List.range n)).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact length_filter_eq_sum (List.range n) (fun y => db 3 x y != 0)]
  rw [nat_sum_cast, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  rw [nat_sum_cast, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro z hz
  by_cases h : db 3 x z == 0
  · have h' : db 3 x z = 0 := beq_iff_eq.mp h
    simp [ind, h']
  · have h' : db 3 x z ≠ 0 := by intro he; exact h (by simp [he])
    simp [ind, h']

/-! Three integral edge-cover bounds for the indicator join. -/

private theorem bound03 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (ind db) b ≤
      (support n db ⟨0, 0, 1⟩ pattern b : Int) *
      (support n db ⟨3, 2, 3⟩ pattern b : Int) := by
  rw [reference_form, support0_int, support3_int]
  have hpoint : ∀ x y z : Nat,
      ind db 0 (b 0) x * (ind db 1 (b 0) y * (ind db 2 x z * ind db 3 y z)) ≤
        ind db 0 (b 0) x * ind db 3 y z := by
    intro x y z
    have hA := ind01 db 0 (b 0) x
    have hB := ind01 db 1 (b 0) y
    have hC := ind01 db 2 x z
    have hD := ind01 db 3 y z
    rcases hA with hA | hA <;> rcases hB with hB | hB <;>
      rcases hC with hC | hC <;> rcases hD with hD | hD <;> simp [hA, hB, hC, hD]
  have hle :
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 0 (b 0) x * (ind db 1 (b 0) y * (ind db 2 x z * ind db 3 y z))))) ≤
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 0 (b 0) x * ind db 3 y z))) := by
    apply rsum_le
    intro x hx
    apply rsum_le
    intro y hy
    apply rsum_le
    intro z hz
    exact hpoint x y z
  have hfac :
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 0 (b 0) x * ind db 3 y z))) =
      (rsum n (fun x => ind db 0 (b 0) x)) *
        (rsum n (fun y => rsum n (fun z => ind db 3 y z))) := by
    rw [show (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 0 (b 0) x * ind db 3 y z))) =
        (fun x => rsum n (fun y => ind db 0 (b 0) x * rsum n (fun z => ind db 3 y z))) by
      funext x
      apply congrArg (rsum n)
      funext y
      exact rsum_const_mul n (ind db 0 (b 0) x) (fun z => ind db 3 y z)]
    rw [show (fun x => rsum n (fun y => ind db 0 (b 0) x * rsum n (fun z => ind db 3 y z))) =
        (fun x => ind db 0 (b 0) x * rsum n (fun y => rsum n (fun z => ind db 3 y z))) by
      funext x
      exact rsum_const_mul n (ind db 0 (b 0) x) (fun y => rsum n (fun z => ind db 3 y z))]
    exact rsum_mul_const n (fun x => ind db 0 (b 0) x)
      (rsum n (fun y => rsum n (fun z => ind db 3 y z)))
  rw [hfac] at hle
  exact hle

private theorem bound12 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (ind db) b ≤
      (support n db ⟨1, 0, 2⟩ pattern b : Int) *
      (support n db ⟨2, 1, 3⟩ pattern b : Int) := by
  rw [reference_form, support1_int, support2_int]
  have hpoint : ∀ x y z : Nat,
      ind db 0 (b 0) x * (ind db 1 (b 0) y * (ind db 2 x z * ind db 3 y z)) ≤
        ind db 1 (b 0) y * ind db 2 x z := by
    intro x y z
    have hA := ind01 db 0 (b 0) x
    have hB := ind01 db 1 (b 0) y
    have hC := ind01 db 2 x z
    have hD := ind01 db 3 y z
    rcases hA with hA | hA <;> rcases hB with hB | hB <;>
      rcases hC with hC | hC <;> rcases hD with hD | hD <;> simp [hA, hB, hC, hD]
  have hle :
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 0 (b 0) x * (ind db 1 (b 0) y * (ind db 2 x z * ind db 3 y z))))) ≤
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 1 (b 0) y * ind db 2 x z))) := by
    apply rsum_le
    intro x hx
    apply rsum_le
    intro y hy
    apply rsum_le
    intro z hz
    exact hpoint x y z
  have hfac :
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 1 (b 0) y * ind db 2 x z))) =
      (rsum n (fun y => ind db 1 (b 0) y)) *
        (rsum n (fun x => rsum n (fun z => ind db 2 x z))) := by
    rw [rsum_comm n n (fun x y => rsum n (fun z => ind db 1 (b 0) y * ind db 2 x z))]
    rw [show (fun y => rsum n (fun x => rsum n (fun z =>
        ind db 1 (b 0) y * ind db 2 x z))) =
        (fun y => rsum n (fun x => ind db 1 (b 0) y * rsum n (fun z => ind db 2 x z))) by
      funext y
      apply congrArg (rsum n)
      funext x
      exact rsum_const_mul n (ind db 1 (b 0) y) (fun z => ind db 2 x z)]
    rw [show (fun y => rsum n (fun x => ind db 1 (b 0) y * rsum n (fun z => ind db 2 x z))) =
        (fun y => ind db 1 (b 0) y * rsum n (fun x => rsum n (fun z => ind db 2 x z))) by
      funext y
      exact rsum_const_mul n (ind db 1 (b 0) y) (fun x => rsum n (fun z => ind db 2 x z))]
    exact rsum_mul_const n (fun y => ind db 1 (b 0) y)
      (rsum n (fun x => rsum n (fun z => ind db 2 x z)))
  rw [hfac] at hle
  exact hle

private theorem bound23 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (ind db) b ≤
      (support n db ⟨2, 1, 3⟩ pattern b : Int) *
      (support n db ⟨3, 2, 3⟩ pattern b : Int) := by
  rw [reference_form, support2_int, support3_int]
  have hpoint : ∀ x y z : Nat,
      ind db 0 (b 0) x * (ind db 1 (b 0) y * (ind db 2 x z * ind db 3 y z)) ≤
        ind db 2 x z * ind db 3 y z := by
    intro x y z
    have hA := ind01 db 0 (b 0) x
    have hB := ind01 db 1 (b 0) y
    have hC := ind01 db 2 x z
    have hD := ind01 db 3 y z
    rcases hA with hA | hA <;> rcases hB with hB | hB <;>
      rcases hC with hC | hC <;> rcases hD with hD | hD <;> simp [hA, hB, hC, hD]
  have hle :
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 0 (b 0) x * (ind db 1 (b 0) y * (ind db 2 x z * ind db 3 y z))))) ≤
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 2 x z * ind db 3 y z))) := by
    apply rsum_le
    intro x hx
    apply rsum_le
    intro y hy
    apply rsum_le
    intro z hz
    exact hpoint x y z
  have hrearr :
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 2 x z * ind db 3 y z))) =
      rsum n (fun z =>
        (rsum n (fun x => ind db 2 x z)) * (rsum n (fun y => ind db 3 y z))) := by
    rw [show (fun x => rsum n (fun y => rsum n (fun z =>
        ind db 2 x z * ind db 3 y z))) =
        (fun x => rsum n (fun z => rsum n (fun y => ind db 2 x z * ind db 3 y z))) by
      funext x
      exact rsum_comm n n (fun y z => ind db 2 x z * ind db 3 y z)]
    rw [show (fun x => rsum n (fun z => rsum n (fun y =>
        ind db 2 x z * ind db 3 y z))) =
        (fun x => rsum n (fun z => ind db 2 x z * rsum n (fun y => ind db 3 y z))) by
      funext x
      apply congrArg (rsum n)
      funext z
      exact rsum_const_mul n (ind db 2 x z) (fun y => ind db 3 y z)]
    rw [rsum_comm n n (fun x z => ind db 2 x z * rsum n (fun y => ind db 3 y z))]
    apply rsum_congr
    intro z hz
    exact rsum_mul_const n (fun x => ind db 2 x z) (rsum n (fun y => ind db 3 y z))
  rw [hrearr] at hle
  let s2 : Int := rsum n (fun x => rsum n (fun z => ind db 2 x z))
  have hcolC_le (z : Nat) (hz : z < n) :
      rsum n (fun x => ind db 2 x z) ≤ s2 := by
    dsimp only [s2]
    apply rsum_le
    intro x hx
    exact term_le_rsum n (fun z' => ind db 2 x z')
      (fun z' hz' => ind_nonneg db 2 x z') z hz
  have hcolD_nonneg (z : Nat) : 0 ≤ rsum n (fun y => ind db 3 y z) :=
    rsum_nonneg n (fun y => ind db 3 y z) (fun y hy => ind_nonneg db 3 y z)
  have hsumle :
      rsum n (fun z => (rsum n (fun x => ind db 2 x z)) *
        (rsum n (fun y => ind db 3 y z))) ≤
      rsum n (fun z => s2 * (rsum n (fun y => ind db 3 y z))) := by
    apply rsum_le
    intro z hz
    exact mul_le_mul_right_nonneg (hcolC_le z hz) (hcolD_nonneg z)
  rw [rsum_const_mul] at hsumle
  have hcommD : rsum n (fun z => rsum n (fun y => ind db 3 y z)) =
      rsum n (fun y => rsum n (fun z => ind db 3 y z)) :=
    rsum_comm n n (fun z y => ind db 3 y z)
  rw [hcommD] at hsumle
  dsimp only [s2] at hsumle
  omega

/-! Combining the bounds by induction on the cover denominator. -/

private theorem exists_cover_bound (w0 w1 w2 w3 den : Nat) (hden : 0 < den)
    (hc1 : den ≤ w0 + w2) (hc2 : den ≤ w1 + w3) (hc3 : den ≤ w2 + w3) :
    (1 ≤ w0 ∧ 1 ≤ w3) ∨ (1 ≤ w1 ∧ 1 ≤ w2) ∨ (den ≤ w2 ∧ den ≤ w3) := by
  omega

private theorem pow_nonneg_int (a : Int) (ha : 0 ≤ a) : ∀ k : Nat, 0 ≤ a ^ k := by
  intro k
  induction k with
  | zero => simp
  | succ k ih =>
      rw [Int.pow_succ]
      exact Int.mul_nonneg ih ha

private theorem one_le_pow_int (a : Int) (ha : 1 ≤ a) : ∀ k : Nat, 1 ≤ a ^ k := by
  intro k
  induction k with
  | zero => simp
  | succ k ih =>
      rw [Int.pow_succ]
      have hnon : 0 ≤ a := by omega
      have hstep : (1 : Int) * a ≤ a ^ k * a := mul_le_mul_right_nonneg ih hnon
      omega

private theorem pow_mono_int (a : Int) (ha : 1 ≤ a) {m n : Nat} (h : m ≤ n) : a ^ m ≤ a ^ n := by
  induction h with
  | refl => omega
  | @step n hmn ih =>
      rw [Int.pow_succ]
      have hnon : 0 ≤ a ^ n := pow_nonneg_int a (by omega) n
      have hstep : (1 : Int) * a ^ n ≤ a * a ^ n := mul_le_mul_right_nonneg ha hnon
      rw [Int.mul_comm a (a ^ n)] at hstep
      omega

private theorem reference_nonneg (n : Nat) (db : DB) (b : Bind) :
    0 ≤ reference pattern n (ind db) b := by
  rw [reference_form]
  apply rsum_nonneg
  intro x hx
  apply rsum_nonneg
  intro y hy
  apply rsum_nonneg
  intro z hz
  exact Int.mul_nonneg (ind_nonneg db 0 (b 0) x)
    (Int.mul_nonneg (ind_nonneg db 1 (b 0) y)
      (Int.mul_nonneg (ind_nonneg db 2 x z) (ind_nonneg db 3 y z)))

private theorem mul_le_mul_two {A B C D : Int} (h : A ≤ B) (hC : 1 ≤ C) (hD : 1 ≤ D)
    (hA : 0 ≤ A) (hB : 0 ≤ B) : A ≤ B * C * D := by
  have h1 : A * 1 ≤ B * C := Int.mul_le_mul h hC (by omega) hB
  have hBC : 0 ≤ B * C := Int.mul_nonneg hB (by omega)
  have h2 : A * 1 * 1 ≤ B * C * D := Int.mul_le_mul h1 hD (by omega) hBC
  simpa using h2

private theorem mul_pow_int (a b : Int) : ∀ n : Nat, (a * b) ^ n = a ^ n * b ^ n := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [Int.pow_succ, ih, Int.pow_succ, Int.pow_succ]
      ac_rfl

private theorem pow_mono_base (a b : Int) (ha : 0 ≤ a) (h : a ≤ b) :
    ∀ n : Nat, a ^ n ≤ b ^ n := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [Int.pow_succ, Int.pow_succ]
      exact Int.mul_le_mul ih h ha (pow_nonneg_int b (by omega) n)

private theorem frac_generic (J S0 S1 S2 S3 : Int)
    (hJ0 : 0 ≤ J) (hS0 : 1 ≤ S0) (hS1 : 1 ≤ S1) (hS2 : 1 ≤ S2) (hS3 : 1 ≤ S3)
    (hb03 : J ≤ S0 * S3) (hb12 : J ≤ S1 * S2) (hb23 : J ≤ S2 * S3) :
    ∀ den w0 w1 w2 w3 : Nat, 0 < den →
      den ≤ w0 + w2 → den ≤ w1 + w3 → den ≤ w2 + w3 →
      J ^ den ≤ S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 := by
  intro den
  induction den with
  | zero =>
      intro w0 w1 w2 w3 hden
      omega
  | succ den ih =>
      intro w0 w1 w2 w3 hden hc1 hc2 hc3
      have hex := exists_cover_bound w0 w1 w2 w3 (den+1) (by omega) hc1 hc2 hc3
      rcases hex with h03 | h12 | h23
      · by_cases hz : den = 0
        · subst den
          have hs0p : S0 ^ 1 ≤ S0 ^ w0 := by simpa using pow_mono_int S0 hS0 h03.1
          have hs3p : S3 ^ 1 ≤ S3 ^ w3 := by simpa using pow_mono_int S3 hS3 h03.2
          have hpair : S0 * S3 ≤ S0 ^ w0 * S3 ^ w3 := by
            simpa [Int.pow_one] using Int.mul_le_mul hs0p hs3p
              (by simpa using pow_nonneg_int S3 (by omega) 1)
              (pow_nonneg_int S0 (by omega) w0)
          have hbig_pair : S0 * S3 ≤ (S0 ^ w0 * S3 ^ w3) * S1 ^ w1 * S2 ^ w2 := by
            apply mul_le_mul_two hpair
            · exact one_le_pow_int S1 hS1 w1
            · exact one_le_pow_int S2 hS2 w2
            · omega
            · exact Int.mul_nonneg (pow_nonneg_int S0 (by omega) w0)
                (pow_nonneg_int S3 (by omega) w3)
          have hbig : S0 * S3 ≤ S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 := by
            simpa [Int.mul_assoc, Int.mul_left_comm, Int.mul_comm] using hbig_pair
          rw [Int.pow_succ, Int.pow_zero, Int.one_mul]
          omega
        · have hdenpos : 0 < den := by omega
          let w0' := w0 - 1
          let w3' := w3 - 1
          have hc1' : den ≤ w0' + w2 := by dsimp only [w0']; omega
          have hc2' : den ≤ w1 + w3' := by dsimp only [w3']; omega
          have hc3' : den ≤ w2 + w3' := by dsimp only [w3']; omega
          have hih := ih w0' w1 w2 w3' hdenpos hc1' hc2' hc3'
          have hnn : 0 ≤ S0 ^ w0' * S1 ^ w1 * S2 ^ w2 * S3 ^ w3' :=
            Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg
              (pow_nonneg_int S0 (by omega) w0') (pow_nonneg_int S1 (by omega) w1))
              (pow_nonneg_int S2 (by omega) w2)) (pow_nonneg_int S3 (by omega) w3')
          have hprod : J ^ den * J ≤ (S0 ^ w0' * S1 ^ w1 * S2 ^ w2 * S3 ^ w3') * (S0 * S3) :=
            Int.mul_le_mul hih hb03 hJ0 hnn
          rw [Int.pow_succ]
          have hw0eq : w0' + 1 = w0 := by dsimp only [w0']; exact Nat.sub_add_cancel h03.1
          have hw3eq : w3' + 1 = w3 := by dsimp only [w3']; exact Nat.sub_add_cancel h03.2
          have hcalc : (S0 ^ w0' * S1 ^ w1 * S2 ^ w2 * S3 ^ w3') * (S0 * S3) =
              S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 := by
            rw [← hw0eq, ← hw3eq]
            rw [Int.pow_succ, Int.pow_succ]
            ac_rfl
          rw [hcalc] at hprod
          exact hprod
      · by_cases hz : den = 0
        · subst den
          have hs1p : S1 ^ 1 ≤ S1 ^ w1 := by simpa using pow_mono_int S1 hS1 h12.1
          have hs2p : S2 ^ 1 ≤ S2 ^ w2 := by simpa using pow_mono_int S2 hS2 h12.2
          have hpair : S1 * S2 ≤ S1 ^ w1 * S2 ^ w2 := by
            simpa [Int.pow_one] using Int.mul_le_mul hs1p hs2p
              (by simpa using pow_nonneg_int S2 (by omega) 1)
              (pow_nonneg_int S1 (by omega) w1)
          have hbig_pair : S1 * S2 ≤ (S1 ^ w1 * S2 ^ w2) * S0 ^ w0 * S3 ^ w3 := by
            apply mul_le_mul_two hpair
            · exact one_le_pow_int S0 hS0 w0
            · exact one_le_pow_int S3 hS3 w3
            · omega
            · exact Int.mul_nonneg (pow_nonneg_int S1 (by omega) w1)
                (pow_nonneg_int S2 (by omega) w2)
          have hbig : S1 * S2 ≤ S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 := by
            simpa [Int.mul_assoc, Int.mul_left_comm, Int.mul_comm] using hbig_pair
          rw [Int.pow_succ, Int.pow_zero, Int.one_mul]
          omega
        · have hdenpos : 0 < den := by omega
          let w1' := w1 - 1
          let w2' := w2 - 1
          have hc1' : den ≤ w0 + w2' := by dsimp only [w2']; omega
          have hc2' : den ≤ w1' + w3 := by dsimp only [w1']; omega
          have hc3' : den ≤ w2' + w3 := by dsimp only [w2']; omega
          have hih := ih w0 w1' w2' w3 hdenpos hc1' hc2' hc3'
          have hnn : 0 ≤ S0 ^ w0 * S1 ^ w1' * S2 ^ w2' * S3 ^ w3 :=
            Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg
              (pow_nonneg_int S0 (by omega) w0) (pow_nonneg_int S1 (by omega) w1'))
              (pow_nonneg_int S2 (by omega) w2')) (pow_nonneg_int S3 (by omega) w3)
          have hprod : J ^ den * J ≤ (S0 ^ w0 * S1 ^ w1' * S2 ^ w2' * S3 ^ w3) * (S1 * S2) :=
            Int.mul_le_mul hih hb12 hJ0 hnn
          rw [Int.pow_succ]
          have hw1eq : w1' + 1 = w1 := by dsimp only [w1']; exact Nat.sub_add_cancel h12.1
          have hw2eq : w2' + 1 = w2 := by dsimp only [w2']; exact Nat.sub_add_cancel h12.2
          have hcalc : (S0 ^ w0 * S1 ^ w1' * S2 ^ w2' * S3 ^ w3) * (S1 * S2) =
              S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 := by
            rw [← hw1eq, ← hw2eq]
            rw [Int.pow_succ, Int.pow_succ]
            ac_rfl
          rw [hcalc] at hprod
          exact hprod
      · have hJpow : J ^ (den+1) ≤ (S2 * S3) ^ (den+1) :=
          pow_mono_base J (S2 * S3) hJ0 hb23 (den+1)
        rw [mul_pow_int] at hJpow
        have hs2p : S2 ^ (den+1) ≤ S2 ^ w2 := pow_mono_int S2 hS2 h23.1
        have hs3p : S3 ^ (den+1) ≤ S3 ^ w3 := pow_mono_int S3 hS3 h23.2
        have hpair : S2 ^ (den+1) * S3 ^ (den+1) ≤ S2 ^ w2 * S3 ^ w3 := by
          exact Int.mul_le_mul hs2p hs3p (pow_nonneg_int S3 (by omega) (den+1))
            (pow_nonneg_int S2 (by omega) w2)
        have hbig_pair : S2 ^ (den+1) * S3 ^ (den+1) ≤ (S2 ^ w2 * S3 ^ w3) * S0 ^ w0 * S1 ^ w1 := by
          apply mul_le_mul_two hpair
          · exact one_le_pow_int S0 hS0 w0
          · exact one_le_pow_int S1 hS1 w1
          · exact Int.mul_nonneg (pow_nonneg_int S2 (by omega) (den+1))
              (pow_nonneg_int S3 (by omega) (den+1))
          · exact Int.mul_nonneg (pow_nonneg_int S2 (by omega) w2)
              (pow_nonneg_int S3 (by omega) w3)
        have hbig : S2 ^ (den+1) * S3 ^ (den+1) ≤
            S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 := by
          simpa [Int.mul_assoc, Int.mul_left_comm, Int.mul_comm] using hbig_pair
        omega

/-! Goal: fractional edge-cover bound on the indicator join. -/

private theorem nat_of_int_bound {A B : Nat} (h : (A : Int) ≤ (B : Int)) : A ≤ B :=
  Int.ofNat_le.mp h

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany.FractionalCap.Goal := by
  intro n db b weights den hcov
  have hlen : weights.length = 4 := hcov.2.1
  cases weights with
  | nil => simp at hlen
  | cons w0 ws =>
    cases ws with
    | nil => simp at hlen
    | cons w1 ws =>
      cases ws with
      | nil => simp at hlen
      | cons w2 ws =>
        cases ws with
        | nil => simp at hlen
        | cons w3 ws =>
          cases ws with
          | nil =>
              have hc : 0 < den ∧ den ≤ w0 + w2 ∧ den ≤ w1 + w3 ∧ den ≤ w2 + w3 := by
                simpa [cover, pattern] using hcov
              rcases hc with ⟨hdenpos, hc1, hc2, hc3⟩
              let s0 := support n db ⟨0, 0, 1⟩ pattern b
              let s1 := support n db ⟨1, 0, 2⟩ pattern b
              let s2 := support n db ⟨2, 1, 3⟩ pattern b
              let s3 := support n db ⟨3, 2, 3⟩ pattern b
              have hRHS :
                  ((pattern.atoms.zip [w0, w1, w2, w3]).map
                    (fun (a, w) => support n db a pattern b ^ w)).prod =
                  s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 := by
                simp [pattern, s0, s1, s2, s3]
                ac_rfl
              rw [show (fun r x y => if db r x y == 0 then 0 else 1) = ind db by rfl]
              rw [hRHS]
              by_cases hz0 : s0 = 0
              · have hb := bound03 n db b
                have hJle0 : reference pattern n (ind db) b ≤ 0 := by
                  simpa [s0, hz0] using hb
                have hJ0 : reference pattern n (ind db) b = 0 := by
                  have := reference_nonneg n db b
                  omega
                simp [hJ0, Nat.zero_pow hdenpos]
              · by_cases hz1 : s1 = 0
                · have hb := bound12 n db b
                  have hJle0 : reference pattern n (ind db) b ≤ 0 := by
                    simpa [s1, hz1] using hb
                  have hJ0 : reference pattern n (ind db) b = 0 := by
                    have := reference_nonneg n db b
                    omega
                  simp [hJ0, Nat.zero_pow hdenpos]
                · by_cases hz2 : s2 = 0
                  · have hb := bound23 n db b
                    have hJle0 : reference pattern n (ind db) b ≤ 0 := by
                      simpa [s2, hz2] using hb
                    have hJ0 : reference pattern n (ind db) b = 0 := by
                      have := reference_nonneg n db b
                      omega
                    simp [hJ0, Nat.zero_pow hdenpos]
                  · by_cases hz3 : s3 = 0
                    · have hb := bound03 n db b
                      have hJle0 : reference pattern n (ind db) b ≤ 0 := by
                        simpa [s3, hz3] using hb
                      have hJ0 : reference pattern n (ind db) b = 0 := by
                        have := reference_nonneg n db b
                        omega
                      simp [hJ0, Nat.zero_pow hdenpos]
                    · have hs0i : 1 ≤ (s0 : Int) := by omega
                      have hs1i : 1 ≤ (s1 : Int) := by omega
                      have hs2i : 1 ≤ (s2 : Int) := by omega
                      have hs3i : 1 ≤ (s3 : Int) := by omega
                      have hint :
                          (reference pattern n (ind db) b) ^ den ≤
                            (s0 : Int) ^ w0 * (s1 : Int) ^ w1 *
                              (s2 : Int) ^ w2 * (s3 : Int) ^ w3 := by
                        simpa [s0, s1, s2, s3] using
                          (frac_generic (reference pattern n (ind db) b)
                            (support n db ⟨0, 0, 1⟩ pattern b : Int)
                            (support n db ⟨1, 0, 2⟩ pattern b : Int)
                            (support n db ⟨2, 1, 3⟩ pattern b : Int)
                            (support n db ⟨3, 2, 3⟩ pattern b : Int)
                            (reference_nonneg n db b) hs0i hs1i hs2i hs3i
                            (bound03 n db b) (bound12 n db b) (bound23 n db b)
                            den w0 w1 w2 w3 hdenpos hc1 hc2 hc3)
                      have hnat : (reference pattern n (ind db) b).toNat ^ den ≤
                          s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 := by
                        apply nat_of_int_bound
                        rw [Int.natCast_pow]
                        rw [show ((reference pattern n (ind db) b).toNat : Int) =
                            reference pattern n (ind db) b from
                          Int.toNat_of_nonneg (reference_nonneg n db b)]
                        simpa [Int.natCast_mul, Int.natCast_pow] using hint
                      exact hnat
          | cons w4 ws => simp at hlen

/-! Inclusion-exclusion over equality partitions (Möbius on the partition lattice). -/

private theorem rsum_ite_eq (n k : Nat) (f : Nat → Int) :
    rsum n (fun i => if i == k then f i else 0) = if k < n then f k else 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      by_cases hkn : k < n
      · have hne : (n == k) = false := by
          by_cases h : n == k
          · have h' : n = k := beq_iff_eq.mp h
            omega
          · simpa using h
        simp [hkn, Nat.lt_succ_of_lt hkn, hne]
      · by_cases hkeq : k == n
        · have hk : k = n := beq_iff_eq.mp hkeq
          subst k
          simp [hkn]
        · have hkne : k ≠ n := by intro h; subst h; simp at hkeq
          have hnlt : ¬ k < n + 1 := by omega
          simp [hkn, hnlt]
          intro heq
          exact False.elim (hkne heq.symm)

private theorem rsum_ite_eq_prop (n k : Nat) (f : Nat → Int) :
    rsum n (fun i => if i = k then f i else 0) = if k < n then f k else 0 := by
  simpa using rsum_ite_eq n k f

private def P3 (db : DB) (b : Bind) (x1 x2 x3 : Nat) : Int :=
  db 0 (b 0) x1 * (db 1 (b 0) x2 * (db 2 x1 x3 * db 3 x2 x3))

private def rsum3 (n : Nat) (f : Nat → Nat → Nat → Int) : Int :=
  rsum n (fun x => rsum n (fun y => rsum n (fun z => f x y z)))

private theorem rsum3_add (n : Nat) (f g : Nat → Nat → Nat → Int) :
    rsum3 n (fun x y z => f x y z + g x y z) = rsum3 n f + rsum3 n g := by
  unfold rsum3
  rw [show (fun x => rsum n (fun y => rsum n (fun z => f x y z + g x y z))) =
      (fun x => rsum n (fun y =>
        (rsum n (fun z => f x y z)) + (rsum n (fun z => g x y z)))) by
    funext x
    apply congrArg (rsum n)
    funext y
    exact rsum_add n (fun z => f x y z) (fun z => g x y z)]
  rw [show (fun x => rsum n (fun y =>
        (rsum n (fun z => f x y z)) + (rsum n (fun z => g x y z)))) =
      (fun x => (rsum n (fun y => rsum n (fun z => f x y z))) +
        (rsum n (fun y => rsum n (fun z => g x y z)))) by
    funext x
    exact rsum_add n (fun y => rsum n (fun z => f x y z)) (fun y => rsum n (fun z => g x y z))]
  rw [rsum_add]

private theorem rsum3_const_mul (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    rsum3 n (fun x y z => c * f x y z) = c * rsum3 n f := by
  unfold rsum3
  rw [show (fun x => rsum n (fun y => rsum n (fun z => c * f x y z))) =
      (fun x => rsum n (fun y => c * rsum n (fun z => f x y z))) by
    funext x
    apply congrArg (rsum n)
    funext y
    exact rsum_const_mul n c (fun z => f x y z)]
  rw [show (fun x => rsum n (fun y => c * rsum n (fun z => f x y z))) =
      (fun x => c * rsum n (fun y => rsum n (fun z => f x y z))) by
    funext x
    exact rsum_const_mul n c (fun y => rsum n (fun z => f x y z))]
  exact rsum_const_mul n c _

private theorem rsum3_neg (n : Nat) (f : Nat → Nat → Nat → Int) :
    rsum3 n (fun x y z => - f x y z) = - rsum3 n f := by
  rw [show (fun x y z => - f x y z) = (fun x y z => (-1 : Int) * f x y z) by
    funext x y z
    rw [Int.neg_eq_neg_one_mul]]
  rw [rsum3_const_mul]
  rw [← Int.neg_eq_neg_one_mul]

private theorem rsum3_sub (n : Nat) (f g : Nat → Nat → Nat → Int) :
    rsum3 n (fun x y z => f x y z - g x y z) = rsum3 n f - rsum3 n g := by
  rw [show (fun x y z => f x y z - g x y z) =
      (fun x y z => f x y z + (- g x y z)) by
    funext x y z
    rw [Int.sub_eq_add_neg]]
  rw [rsum3_add, rsum3_neg]
  rw [Int.sub_eq_add_neg]

private theorem collapsed0 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1], [2], [3]] = rsum3 n (P3 db b) := by
  simp [collapsed, pattern, integrate, value, put, rsum, rsum3, P3]

private theorem collapsed12 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1, 2], [3]] =
      rsum3 n (fun x1 x2 x3 => if x2 == x1 then P3 db b x1 x2 x3 else 0) := by
  simp [collapsed, pattern, integrate, value, put, rsum3]
  change rsum n (fun x1 => rsum n (fun z => P3 db b x1 x1 z)) =
    rsum n (fun x1 => rsum n (fun y => rsum n (fun z =>
      if y = x1 then P3 db b x1 y z else 0)))
  apply rsum_congr
  intro x1 hx1
  rw [rsum_comm]
  apply rsum_congr
  intro z hz
  rw [rsum_ite_eq_prop]
  simp [hx1]

private theorem collapsed13 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[2], [1, 3]] =
      rsum3 n (fun x1 x2 x3 => if x3 == x1 then P3 db b x1 x2 x3 else 0) := by
  simp [collapsed, pattern, integrate, value, put, rsum3]
  change rsum n (fun x2 => rsum n (fun x1 => P3 db b x1 x2 x1)) =
    rsum n (fun x1 => rsum n (fun y => rsum n (fun z =>
      if z = x1 then P3 db b x1 y z else 0)))
  rw [rsum_comm]
  apply rsum_congr
  intro x1 hx1
  apply rsum_congr
  intro x2 hx2
  rw [rsum_ite_eq_prop]
  simp [hx1]

private theorem collapsed23 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1], [2, 3]] =
      rsum3 n (fun x1 x2 x3 => if x3 == x2 then P3 db b x1 x2 x3 else 0) := by
  simp [collapsed, pattern, integrate, value, put, rsum3]
  change rsum n (fun x1 => rsum n (fun x2 => P3 db b x1 x2 x2)) =
    rsum n (fun x1 => rsum n (fun y => rsum n (fun z =>
      if z = y then P3 db b x1 y z else 0)))
  apply rsum_congr
  intro x1 hx1
  apply rsum_congr
  intro x2 hx2
  rw [rsum_ite_eq_prop]
  simp [hx2]

private theorem collapsed123 (n : Nat) (db : DB) (b : Bind) :
    collapsed pattern n db b [[1, 2, 3]] =
      rsum3 n (fun x1 x2 x3 =>
        if x2 == x1 && x3 == x1 then P3 db b x1 x2 x3 else 0) := by
  simp [collapsed, pattern, integrate, value, put, rsum3]
  change rsum n (fun x1 => P3 db b x1 x1 x1) =
    rsum n (fun x1 => rsum n (fun y => rsum n (fun z =>
      if y = x1 ∧ z = x1 then P3 db b x1 y z else 0)))
  apply rsum_congr
  intro x1 hx1
  rw [show (fun y => rsum n (fun z =>
        if y = x1 ∧ z = x1 then P3 db b x1 y z else 0)) =
      (fun y => if y = x1 then P3 db b x1 y x1 else 0) by
    funext y
    rw [show (fun z => if y = x1 ∧ z = x1 then P3 db b x1 y z else 0) =
        (fun z => if y = x1 then (if z = x1 then P3 db b x1 y z else 0) else 0) by
      funext z
      by_cases hy : y = x1 <;> by_cases hz : z = x1 <;> simp [hy, hz]]
    by_cases hy : y = x1
    · subst y
      simp
      rw [rsum_ite_eq_prop]
      simp [hx1]
    · simp [rsum_const_zero, hy]]
  rw [rsum_ite_eq_prop]
  simp [hx1]

private theorem injective_form (n : Nat) (db : DB) (b : Bind) :
    injective pattern n db b =
      rsum3 n (fun x1 x2 x3 =>
        if [x1, x2, x3].Nodup then P3 db b x1 x2 x3 else 0) := by
  simp [injective, pattern, integrate, value, put, rsum, rsum3, P3]

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany.CollisionCorrection.Goal := by
  intro n db b
  rw [injective_form]
  let P : Nat → Nat → Nat → Int := P3 db b
  let C12 : Nat → Nat → Nat → Int := fun x1 x2 x3 =>
    if x2 == x1 then P x1 x2 x3 else 0
  let C13 : Nat → Nat → Nat → Int := fun x1 x2 x3 =>
    if x3 == x1 then P x1 x2 x3 else 0
  let C23 : Nat → Nat → Nat → Int := fun x1 x2 x3 =>
    if x3 == x2 then P x1 x2 x3 else 0
  let Call : Nat → Nat → Nat → Int := fun x1 x2 x3 =>
    if x2 == x1 && x3 == x1 then P x1 x2 x3 else 0
  have hRHS :
      ((partitions pattern.freeVars).map
        (fun bs => mobius bs * collapsed pattern n db b bs)).sum =
      rsum3 n P - rsum3 n C12 - rsum3 n C13 - rsum3 n C23 + 2 * rsum3 n Call := by
    change ((partitions [1, 2, 3]).map
      (fun bs => mobius bs * collapsed pattern n db b bs)).sum =
      rsum3 n P - rsum3 n C12 - rsum3 n C13 - rsum3 n C23 + 2 * rsum3 n Call
    simp [partitions, mobius, factorial, collapsed0, collapsed12, collapsed13,
      collapsed23, collapsed123, P, C12, C13, C23, Call]
    omega
  have hpoint : ∀ x1 x2 x3 : Nat,
      (if [x1, x2, x3].Nodup then P3 db b x1 x2 x3 else 0) =
        P x1 x2 x3 - C12 x1 x2 x3 - C13 x1 x2 x3 - C23 x1 x2 x3 +
          2 * Call x1 x2 x3 := by
    intro x1 x2 x3
    dsimp only [P, C12, C13, C23, Call]
    by_cases h12 : x1 = x2 <;> by_cases h13 : x1 = x3 <;> by_cases h23 : x2 = x3 <;>
      simp [h12, h13, h23] <;> omega
  have hcombine :
      rsum3 n (fun x1 x2 x3 =>
        P x1 x2 x3 - C12 x1 x2 x3 - C13 x1 x2 x3 - C23 x1 x2 x3 +
          2 * Call x1 x2 x3) =
      rsum3 n P - rsum3 n C12 - rsum3 n C13 - rsum3 n C23 + 2 * rsum3 n Call := by
    rw [show (fun x1 x2 x3 =>
        P x1 x2 x3 - C12 x1 x2 x3 - C13 x1 x2 x3 - C23 x1 x2 x3 +
          2 * Call x1 x2 x3) =
        (fun x1 x2 x3 =>
        ((P x1 x2 x3 - C12 x1 x2 x3) - C13 x1 x2 x3) - C23 x1 x2 x3 +
          2 * Call x1 x2 x3) by
      funext x1 x2 x3
      omega]
    rw [rsum3_add]
    rw [rsum3_sub]
    rw [rsum3_sub]
    rw [rsum3_sub]
    rw [rsum3_const_mul]
  calc
    rsum3 n (fun x1 x2 x3 =>
        if [x1, x2, x3].Nodup then P3 db b x1 x2 x3 else 0)
        = rsum3 n (fun x1 x2 x3 =>
            P x1 x2 x3 - C12 x1 x2 x3 - C13 x1 x2 x3 - C23 x1 x2 x3 +
              2 * Call x1 x2 x3) := by
            apply rsum_congr
            intro x1 hx1
            apply rsum_congr
            intro x2 hx2
            apply rsum_congr
            intro x3 hx3
            exact hpoint x1 x2 x3
    _ = rsum3 n P - rsum3 n C12 - rsum3 n C13 - rsum3 n C23 + 2 * rsum3 n Call := hcombine
    _ = ((partitions pattern.freeVars).map
          (fun bs => mobius bs * collapsed pattern n db b bs)).sum := hRHS.symm

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.KeywordCompanyCapsAndCollisionCorrection
