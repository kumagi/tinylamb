import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `IntersectingCycles.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `IntersectingCycles.FractionalCap` — `∀ n db b weights den, cover pattern weights den
  → (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den
  ≤ ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^
  w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 100000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.IntersectingCyclesCapsAndCollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles

/-- Sanity checks on the concrete pattern. -/
example : pattern.freeVars = [4, 5, 1, 2, 3, 0] := by rfl
example : pattern.atoms =
    [⟨0, 0, 1⟩, ⟨0, 1, 2⟩, ⟨0, 2, 3⟩, ⟨0, 3, 0⟩,
     ⟨1, 2, 4⟩, ⟨2, 4, 5⟩, ⟨2, 5, 3⟩] := by rfl

private abbrev fv : List Nat := pattern.freeVars

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
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

private theorem rsum_neg (n : Nat) (f : Nat → Int) : rsum n (fun i => -f i) = -rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih]; omega

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  rw [show (fun i => f i - g i) = (fun i => f i + (-g i)) by
    funext i; rw [Int.sub_eq_add_neg]]
  rw [rsum_add, rsum_neg]
  omega

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

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr; intro j hj; simp [rsum]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
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

private theorem int_sum_nat (l : List Nat) : (l.map (fun x : Nat => (x : Int))).sum = (l.sum : Int) := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

/-! ## FractionalCap: indicator expansion and integral cover bounds. -/

private def I (db : DB) (r x y : Nat) : Int := if db r x y == 0 then 0 else 1

private theorem I_nonneg (db : DB) (r x y : Nat) : 0 ≤ I db r x y := by
  by_cases h : db r x y == 0 <;> simp [I, h]

private theorem I_le_one (db : DB) (r x y : Nat) : I db r x y ≤ 1 := by
  by_cases h : db r x y == 0 <;> simp [I, h]

private theorem I_eq_ne (db : DB) (r x y : Nat) :
    I db r x y = if db r x y != 0 then 1 else 0 := by
  by_cases h : db r x y == 0
  · have hz : db r x y = 0 := beq_iff_eq.mp h
    simp [I, h, hz]
  · have hn : db r x y ≠ 0 := by intro he; exact h (by simp [he])
    have ht : (db r x y != 0) = true := by simp [hn]
    simp [I, h, ht]

private theorem countP_eq_sumI (n : Nat) (db : DB) (r x : Nat) :
    ((List.range n).countP (fun y => db r x y != 0) : Int) =
      ((List.range n).map (fun y => I db r x y)).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, List.map_append, List.sum_append]
      rw [show ((List.countP (fun y => db r x y != 0) (List.range n) +
          [n].countP (fun y => db r x y != 0) : Nat) : Int) =
          (List.countP (fun y => db r x y != 0) (List.range n) : Int) +
          ([n].countP (fun y => db r x y != 0) : Int) by omega]
      rw [ih]
      by_cases hz : db r x n = 0 <;> simp [I, hz]

private def S (n : Nat) (db : DB) (r : Nat) : Int :=
  rsum n (fun x => rsum n (fun y => I db r x y))

private theorem support_eq_S (n : Nat) (db : DB) (b : Bind) (r src dst : Nat)
    (hs : src ∈ fv) (hd : dst ∈ fv) :
    (support n db ⟨r, src, dst⟩ pattern b : Int) = S n db r := by
  have hsrc2 : [4, 5, 1, 2, 3, 0].contains src = true := by
    simpa [fv, pattern] using hs
  have hdst2 : [4, 5, 1, 2, 3, 0].contains dst = true := by
    simpa [fv, pattern] using hd
  unfold support S rsum
  simp only [pattern, Atom.src, Atom.dst, hsrc2, hdst2, ↓reduceIte]
  rw [← int_sum_nat ((List.range n).map (fun x => List.countP (fun y => db r x y != 0) (List.range n)))]
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  exact countP_eq_sumI n db r x

private theorem ref_eq_sum (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
      rsum n (fun v4 => rsum n (fun v5 => rsum n (fun v1 => rsum n (fun v2 =>
        rsum n (fun v3 => rsum n (fun v0 =>
          I db 0 v0 v1 * I db 0 v1 v2 * I db 0 v2 v3 * I db 0 v3 v0 *
            I db 1 v2 v4 * I db 2 v4 v5 * I db 2 v5 v3)))))) := by
  simp [reference, pattern, integrate, value, put, rsum, I, Int.mul_assoc]

private theorem ref_nonneg (n : Nat) (db : DB) (b : Bind) :
    0 ≤ reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b := by
  rw [ref_eq_sum]
  apply rsum_nonneg; intro v4 h4
  apply rsum_nonneg; intro v5 h5
  apply rsum_nonneg; intro v1 h1
  apply rsum_nonneg; intro v2 h2
  apply rsum_nonneg; intro v3 h3
  apply rsum_nonneg; intro v0 h0
  have h01 := I_nonneg db 0 v0 v1
  have h12 := I_nonneg db 0 v1 v2
  have h23 := I_nonneg db 0 v2 v3
  have h30 := I_nonneg db 0 v3 v0
  have h24 := I_nonneg db 1 v2 v4
  have h45 := I_nonneg db 2 v4 v5
  have h53 := I_nonneg db 2 v5 v3
  exact Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg
    (Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg h01 h12) h23) h30) h24) h45) h53

private theorem integrate_mono (n : Nat) (vs : List Nat) (b : Bind) {f g : Bind → Int}
    (h : ∀ x, f x ≤ g x) : integrate n vs b f ≤ integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      simp only [integrate]
      apply rsum_le
      intro x hx
      exact ih (put b v x)

private theorem mul_le_one_nonneg {a b : Int} (ha0 : 0 ≤ a) (ha1 : a ≤ 1)
    (hb0 : 0 ≤ b) (hb1 : b ≤ 1) : a * b ≤ 1 := by
  have h := Int.mul_le_mul ha1 hb1 hb0 (by omega : (0 : Int) ≤ 1)
  simpa using h

private theorem drop_mul {a b : Int} (ha0 : 0 ≤ a) (hb0 : 0 ≤ b) (hb1 : b ≤ 1) :
    a * b ≤ a := by
  have h := Int.mul_le_mul_of_nonneg_left hb1 ha0
  simpa using h

private theorem rsum6_prod (n : Nat) (f1 f2 f3 : Nat → Nat → Int) :
    rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => rsum n (fun x5 => f1 x0 x1 * f2 x2 x3 * f3 x4 x5)))))) =
    (rsum n (fun x => rsum n (fun y => f1 x y))) *
      (rsum n (fun x => rsum n (fun y => f2 x y))) *
        (rsum n (fun x => rsum n (fun y => f3 x y))) := by
  let S3 := rsum n (fun x => rsum n (fun y => f3 x y))
  let S2 := rsum n (fun x => rsum n (fun y => f2 x y))
  let S1 := rsum n (fun x => rsum n (fun y => f1 x y))
  calc
    rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => rsum n (fun x5 => f1 x0 x1 * f2 x2 x3 * f3 x4 x5))))))
        = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            rsum n (fun x4 => (f1 x0 x1 * f2 x2 x3) *
              rsum n (fun x5 => f3 x4 x5)))))) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              apply rsum_congr; intro x2 h2
              apply rsum_congr; intro x3 h3
              apply rsum_congr; intro x4 h4
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
          (f1 x0 x1 * f2 x2 x3) * S3)))) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              apply rsum_congr; intro x2 h2
              apply rsum_congr; intro x3 h3
              dsimp [S3]
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 =>
          (f1 x0 x1 * S3) * rsum n (fun x3 => f2 x2 x3)))) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              apply rsum_congr; intro x2 h2
              rw [show (fun x3 => (f1 x0 x1 * f2 x2 x3) * S3) =
                  (fun x3 => (f1 x0 x1 * S3) * f2 x2 x3) by
                funext x3; ac_rfl]
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => rsum n (fun x1 =>
          (f1 x0 x1 * S3) * S2)) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              dsimp [S2]
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => (S3 * S2) * rsum n (fun x1 => f1 x0 x1)) := by
              apply rsum_congr; intro x0 h0
              rw [show (fun x1 => (f1 x0 x1 * S3) * S2) =
                  (fun x1 => (S3 * S2) * f1 x0 x1) by
                funext x1; ac_rfl]
              rw [rsum_const_mul]
    _ = (S3 * S2) * S1 := by
              dsimp [S1]
              rw [rsum_const_mul]
    _ = S1 * S2 * S3 := by ac_rfl

private theorem integrate_pairs_canonical (n : Nat) (db : DB) (b : Bind) (r1 r2 r3 : Nat) :
    integrate n [0, 1, 2, 3, 4, 5] b
      (fun x => I db r1 (x 0) (x 1) * I db r2 (x 2) (x 3) * I db r3 (x 4) (x 5)) =
        S n db r1 * S n db r2 * S n db r3 := by
  simp [integrate, S, rsum, put]
  exact rsum6_prod n (fun x y => I db r1 x y) (fun x y => I db r2 x y) (fun x y => I db r3 x y)

private theorem prod7_le_keep3 (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*c*f := by
  have hkeep : 0 ≤ a*c*f := Int.mul_nonneg (Int.mul_nonneg ha0 hc0) hf0
  have hbd : b*d ≤ 1 := mul_le_one_nonneg hb0 hb1 hd0 hd1
  have heg : e*g ≤ 1 := mul_le_one_nonneg he0 he1 hg0 hg1
  have hdrop : b*d*(e*g) ≤ 1 :=
    mul_le_one_nonneg (Int.mul_nonneg hb0 hd0) hbd (Int.mul_nonneg he0 hg0) heg
  have hdrop' : b*d*e*g ≤ 1 := by
    rw [show b*d*e*g = b*d*(e*g) by ac_rfl]
    exact hdrop
  calc
    a*b*c*d*e*f*g = (a*c*f)*(b*d*e*g) := by ac_rfl
    _ ≤ (a*c*f)*1 := Int.mul_le_mul_of_nonneg_left hdrop' hkeep
    _ = a*c*f := by simp

private theorem prod7_le_keep4 (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*b*e*g := by
  have hab : 0 ≤ a*b := Int.mul_nonneg ha0 hb0
  have habeg : 0 ≤ a*b*e := Int.mul_nonneg hab he0
  have hkeep : 0 ≤ a*b*e*g := Int.mul_nonneg habeg hg0
  have hcd : c*d ≤ 1 := mul_le_one_nonneg hc0 hc1 hd0 hd1
  have hf1' : f ≤ 1 := hf1
  have hdrop : c*d*f ≤ 1 := by
    calc
      c*d*f = (c*d)*f := by ac_rfl
      _ ≤ 1*1 := Int.mul_le_mul hcd hf1' hf0 (by omega : (0 : Int) ≤ 1)
      _ = 1 := by simp
  calc
    a*b*c*d*e*f*g = (a*b*e*g)*(c*d*f) := by ac_rfl
    _ ≤ (a*b*e*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*b*e*g := by simp

private theorem rsum_mul_le_products_aux (n : Nat) (a b : Nat → Int)
    (ha : ∀ i, i < n → 0 ≤ a i) (hb : ∀ i, i < n → 0 ≤ b i) :
    rsum n (fun i => a i * b i) ≤ rsum n a * rsum n b := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ]
      have han : 0 ≤ a n := ha n (Nat.lt_succ_self n)
      have hbn : 0 ≤ b n := hb n (Nat.lt_succ_self n)
      have hia := ih (fun i hii => ha i (Nat.lt_succ_of_lt hii))
        (fun i hii => hb i (Nat.lt_succ_of_lt hii))
      have hsa : 0 ≤ rsum n a := rsum_nonneg n a (fun i hi => ha i (Nat.lt_succ_of_lt hi))
      have hsb : 0 ≤ rsum n b := rsum_nonneg n b (fun i hi => hb i (Nat.lt_succ_of_lt hi))
      simp only [Int.add_mul, Int.mul_add]
      have hp1 : 0 ≤ rsum n a * b n := Int.mul_nonneg hsa hbn
      have hp2 : 0 ≤ a n * rsum n b := Int.mul_nonneg han hsb
      omega

private theorem rsum_path_le (n : Nat) (f g : Nat → Nat → Int)
    (hf0 : ∀ m a, 0 ≤ f m a) (hf1 : ∀ m a, f m a ≤ 1)
    (hg0 : ∀ m b, 0 ≤ g m b) (hg1 : ∀ m b, g m b ≤ 1) :
    rsum n (fun m => rsum n (fun a => rsum n (fun b => f m a * g m b))) ≤
      (rsum n (fun m => rsum n (fun a => f m a))) *
        (rsum n (fun m => rsum n (fun b => g m b))) := by
  calc
    rsum n (fun m => rsum n (fun a => rsum n (fun b => f m a * g m b)))
        = rsum n (fun m => (rsum n (fun a => f m a)) *
            rsum n (fun b => g m b)) := by
            apply rsum_congr; intro m hm
            rw [show (fun a => rsum n (fun b => f m a * g m b)) =
                (fun a => f m a * rsum n (fun b => g m b)) by
              funext a
              rw [rsum_const_mul]]
            rw [rsum_mul_const]
    _ ≤ (rsum n (fun m => rsum n (fun a => f m a))) *
          (rsum n (fun m => rsum n (fun b => g m b))) := by
            apply rsum_mul_le_products_aux
            · intro m hm
              exact rsum_nonneg n (fun a => f m a) (fun i hi => hf0 m i)
            · intro m hm
              exact rsum_nonneg n (fun b => g m b) (fun i hi => hg0 m i)

private theorem integrate_path_canonical (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [0, 1, 2] b
      (fun x => I db r1 (x 0) (x 1) * I db r2 (x 1) (x 2)) ≤
        S n db r1 * S n db r2 := by
  simp only [integrate, S, put, List.map_cons, List.sum_cons, List.map_nil, List.sum_nil]
  change rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      I db r1 x y * I db r2 y z))) ≤
    (rsum n (fun x => rsum n (fun y => I db r1 x y))) *
      (rsum n (fun y => rsum n (fun z => I db r2 y z)))
  rw [rsum_comm]
  rw [show (rsum n (fun x => rsum n (fun y => I db r1 x y))) =
      rsum n (fun y => rsum n (fun x => I db r1 x y)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m b => I db r2 m b)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m b => I_nonneg db r2 m b) (fun m b => I_le_one db r2 m b)

private theorem rsum6_split (n : Nat) (f g : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      rsum n (fun e => rsum n (fun h => f a b c * g d e h)))))) =
    (rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c)))) *
      (rsum n (fun d => rsum n (fun e => rsum n (fun h => g d e h)))) := by
  let Gtot := rsum n (fun d => rsum n (fun e => rsum n (fun h => g d e h)))
  let Ftot := rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c)))
  calc
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      rsum n (fun e => rsum n (fun h => f a b c * g d e h))))))
        = rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
            f a b c * rsum n (fun e => rsum n (fun h => g d e h)))))) := by
              apply rsum_congr; intro a ha
              apply rsum_congr; intro b hb
              apply rsum_congr; intro c hc
              apply rsum_congr; intro d hd
              rw [show (fun e => rsum n (fun h => f a b c * g d e h)) =
                  (fun e => f a b c * rsum n (fun h => g d e h)) by
                funext e
                rw [rsum_const_mul]]
              rw [rsum_const_mul]
    _ = rsum n (fun a => rsum n (fun b => rsum n (fun c =>
            f a b c * Gtot))) := by
              apply rsum_congr; intro a ha
              apply rsum_congr; intro b hb
              apply rsum_congr; intro c hc
              dsimp [Gtot]
              rw [rsum_const_mul]
    _ = rsum n (fun a => rsum n (fun b =>
            Gtot * rsum n (fun c => f a b c))) := by
              apply rsum_congr; intro a ha
              apply rsum_congr; intro b hb
              rw [show (fun c => f a b c * Gtot) =
                  (fun c => Gtot * f a b c) by
                funext c; ac_rfl]
              rw [rsum_const_mul]
    _ = rsum n (fun a => Gtot * (rsum n (fun b => rsum n (fun c => f a b c)))) := by
              apply rsum_congr; intro a ha
              rw [rsum_const_mul]
    _ = Gtot * Ftot := by
              dsimp [Ftot]
              rw [rsum_const_mul]
    _ = Ftot * Gtot := by ac_rfl

private theorem integrate_split_canonical (n : Nat) (b : Bind) (f g : Nat → Nat → Nat → Int) :
    integrate n [0, 1, 2, 3, 4, 5] b
      (fun x => f (x 0) (x 1) (x 2) * g (x 3) (x 4) (x 5)) =
      integrate n [0, 1, 2] b (fun x => f (x 0) (x 1) (x 2)) *
        integrate n [3, 4, 5] b (fun x => g (x 3) (x 4) (x 5)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n f g

private theorem integrate_perm (n : Nat) {vs ws : List Nat} (h : vs.Perm ws)
    (b : Bind) (f : Bind → Int) : integrate n vs b f = integrate n ws b f := by
  induction h generalizing b with
  | nil => rfl
  | cons v h ih =>
      simp only [integrate]
      apply congrArg List.sum
      apply List.map_congr_left
      intro x hx
      exact ih (put b v x)
  | swap v w vs =>
      by_cases hvw : v = w
      · subst w; rfl
      · change rsum n (fun c => rsum n (fun a => integrate n vs (put (put b w c) v a) f)) =
            rsum n (fun a => rsum n (fun c => integrate n vs (put (put b v a) w c) f))
        rw [rsum_comm]
        apply congrArg List.sum
        apply List.map_congr_left
        intro c hc
        apply congrArg List.sum
        apply List.map_congr_left
        intro a ha
        congr 1
        funext i
        by_cases hiv : i == v
        · by_cases hiw : i == w
          · have hiv' : i = v := beq_iff_eq.mp hiv
            have hiw' : i = w := beq_iff_eq.mp hiw
            exfalso
            exact hvw (hiv'.symm.trans hiw')
          · simp [put, hiv, hiw]
        · by_cases hiw : i == w <;> simp [put, hiv, hiw]
  | trans h1 h2 ih1 ih2 => exact (ih1 b).trans (ih2 b)


/-! ## Cover bounds for FractionalCap. -/

private def Fprod (db : DB) (x : Bind) : Int :=
  I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2) * I db 0 (x 2) (x 3) * I db 0 (x 3) (x 0) *
    I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)

private theorem ref_eq_integrate (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
      integrate n fv b (Fprod db) := by
  simp only [reference, fv, pattern, value]
  apply congrArg (integrate n fv b)
  funext x
  simp [Fprod, I]
  ac_rfl

private theorem prod3_le_one (a b c : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) : a*b*c ≤ 1 := by
  have hab : a*b ≤ 1 := mul_le_one_nonneg ha0 ha1 hb0 hb1
  have hab0 : 0 ≤ a*b := Int.mul_nonneg ha0 hb0
  calc
    a*b*c = (a*b)*c := by ac_rfl
    _ ≤ 1*1 := Int.mul_le_mul hab hc1 hc0 (by omega : (0 : Int) ≤ 1)
    _ = 1 := by simp

private theorem prod4_le_one (a b c d : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1) : a*b*c*d ≤ 1 := by
  have hab : a*b ≤ 1 := mul_le_one_nonneg ha0 ha1 hb0 hb1
  have hab0 : 0 ≤ a*b := Int.mul_nonneg ha0 hb0
  have habc : a*b*c ≤ 1 := mul_le_one_nonneg hab0 hab hc0 hc1
  have habc0 : 0 ≤ a*b*c := Int.mul_nonneg hab0 hc0
  calc
    a*b*c*d = (a*b*c)*d := by ac_rfl
    _ ≤ 1*1 := Int.mul_le_mul habc hd1 hd0 (by omega : (0 : Int) ≤ 1)
    _ = 1 := by simp

private theorem prod7_le_keep3_aeg (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*e*g := by
  have hkeep : 0 ≤ a*e*g := Int.mul_nonneg (Int.mul_nonneg ha0 he0) hg0
  have hdrop : b*c*d*f ≤ 1 := prod4_le_one b c d f hb0 hb1 hc0 hc1 hd0 hd1 hf0 hf1
  calc
    a*b*c*d*e*f*g = (a*e*g)*(b*c*d*f) := by ac_rfl
    _ ≤ (a*e*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*e*g := by simp

private theorem prod7_le_keep3_bdf (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ b*d*f := by
  have hkeep : 0 ≤ b*d*f := Int.mul_nonneg (Int.mul_nonneg hb0 hd0) hf0
  have hdrop : a*c*e*g ≤ 1 := prod4_le_one a c e g ha0 ha1 hc0 hc1 he0 he1 hg0 hg1
  calc
    a*b*c*d*e*f*g = (b*d*f)*(a*c*e*g) := by ac_rfl
    _ ≤ (b*d*f)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = b*d*f := by simp

private theorem prod7_le_keep4_abfg (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*b*f*g := by
  have hkeep : 0 ≤ a*b*f*g := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha0 hb0) hf0) hg0
  have hdrop : c*d*e ≤ 1 := prod3_le_one c d e hc0 hc1 hd0 hd1 he0 he1
  calc
    a*b*c*d*e*f*g = (a*b*f*g)*(c*d*e) := by ac_rfl
    _ ≤ (a*b*f*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*b*f*g := by simp

private theorem prod7_le_keep4_adef (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*d*e*f := by
  have hkeep : 0 ≤ a*d*e*f := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha0 hd0) he0) hf0
  have hdrop : b*c*g ≤ 1 := prod3_le_one b c g hb0 hb1 hc0 hc1 hg0 hg1
  calc
    a*b*c*d*e*f*g = (a*d*e*f)*(b*c*g) := by ac_rfl
    _ ≤ (a*d*e*f)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*d*e*f := by simp

private theorem prod7_le_keep4_bdeg (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ b*d*e*g := by
  have hkeep : 0 ≤ b*d*e*g := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg hb0 hd0) he0) hg0
  have hdrop : a*c*f ≤ 1 := prod3_le_one a c f ha0 ha1 hc0 hc1 hf0 hf1
  calc
    a*b*c*d*e*f*g = (b*d*e*g)*(a*c*f) := by ac_rfl
    _ ≤ (b*d*e*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = b*d*e*g := by simp

private theorem I_all_nonneg_le_one (db : DB) (x : Bind) :
    (0 ≤ I db 0 (x 0) (x 1) ∧ I db 0 (x 0) (x 1) ≤ 1) ∧
    (0 ≤ I db 0 (x 1) (x 2) ∧ I db 0 (x 1) (x 2) ≤ 1) ∧
    (0 ≤ I db 0 (x 2) (x 3) ∧ I db 0 (x 2) (x 3) ≤ 1) ∧
    (0 ≤ I db 0 (x 3) (x 0) ∧ I db 0 (x 3) (x 0) ≤ 1) ∧
    (0 ≤ I db 1 (x 2) (x 4) ∧ I db 1 (x 2) (x 4) ≤ 1) ∧
    (0 ≤ I db 2 (x 4) (x 5) ∧ I db 2 (x 4) (x 5) ≤ 1) ∧
    (0 ≤ I db 2 (x 5) (x 3) ∧ I db 2 (x 5) (x 3) ≤ 1) := by
  refine ⟨⟨I_nonneg db 0 (x 0) (x 1), I_le_one db 0 (x 0) (x 1)⟩,
    ⟨I_nonneg db 0 (x 1) (x 2), I_le_one db 0 (x 1) (x 2)⟩,
    ⟨I_nonneg db 0 (x 2) (x 3), I_le_one db 0 (x 2) (x 3)⟩,
    ⟨I_nonneg db 0 (x 3) (x 0), I_le_one db 0 (x 3) (x 0)⟩,
    ⟨I_nonneg db 1 (x 2) (x 4), I_le_one db 1 (x 2) (x 4)⟩,
    ⟨I_nonneg db 2 (x 4) (x 5), I_le_one db 2 (x 4) (x 5)⟩,
    ⟨I_nonneg db 2 (x 5) (x 3), I_le_one db 2 (x 5) (x 3)⟩⟩

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change rsum n (fun x => integrate n vs (put b v x) (fun _ => (0 : Int))) = 0
      rw [show (fun x => integrate n vs (put b v x) (fun _ => (0 : Int))) =
          (fun _ => (0 : Int)) by
        funext x
        exact ih (put b v x)]
      exact rsum_zero_const n

private theorem S_nonneg (n : Nat) (db : DB) (r : Nat) : 0 ≤ S n db r := by
  unfold S
  apply rsum_nonneg; intro x hx
  apply rsum_nonneg; intro y hy
  exact I_nonneg db r x y

private theorem integrate_nonneg (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (hf : ∀ x, 0 ≤ f x) : 0 ≤ integrate n vs b f := by
  rw [← integrate_zero n vs b]
  apply integrate_mono
  intro x
  exact hf x


/-! ## Specialized factorization and path bounds. -/

private theorem pairF_046 (n : Nat) (db : DB) (b : Bind) :
    integrate n [0, 1, 2, 4, 5, 3] b
      (fun x => I db 0 (x 0) (x 1) * I db 1 (x 2) (x 4) * I db 2 (x 5) (x 3)) =
        S n db 0 * S n db 1 * S n db 2 := by
  simp [integrate, S, rsum, put]
  exact rsum6_prod n (fun x y => I db 0 x y) (fun x y => I db 1 x y) (fun x y => I db 2 x y)

private theorem pairF_135 (n : Nat) (db : DB) (b : Bind) :
    integrate n [1, 2, 3, 0, 4, 5] b
      (fun x => I db 0 (x 1) (x 2) * I db 0 (x 3) (x 0) * I db 2 (x 4) (x 5)) =
        S n db 0 * S n db 0 * S n db 2 := by
  simp [integrate, S, rsum, put]
  exact rsum6_prod n (fun x y => I db 0 x y) (fun x y => I db 0 x y) (fun x y => I db 2 x y)

private theorem split_0156 (n : Nat) (db : DB) (b : Bind) :
    integrate n [0, 1, 2, 4, 5, 3] b
      (fun x => (I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)) *
        (I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3))) =
      integrate n [0, 1, 2] b (fun x => I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)) *
        integrate n [4, 5, 3] b (fun x => I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n (fun a c d => I db 0 a c * I db 0 c d)
    (fun e f h => I db 2 e f * I db 2 f h)

private theorem split_0345 (n : Nat) (db : DB) (b : Bind) :
    integrate n [0, 1, 3, 2, 4, 5] b
      (fun x => (I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)) *
        (I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5))) =
      integrate n [0, 1, 3] b (fun x => I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)) *
        integrate n [2, 4, 5] b (fun x => I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n (fun a c d => I db 0 a c * I db 0 d a)
    (fun e f h => I db 1 e f * I db 2 f h)

private theorem split_1346 (n : Nat) (db : DB) (b : Bind) :
    integrate n [1, 2, 4, 0, 3, 5] b
      (fun x => (I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)) *
        (I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3))) =
      integrate n [1, 2, 4] b (fun x => I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)) *
        integrate n [0, 3, 5] b (fun x => I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n (fun a c d => I db 0 a c * I db 1 c d)
    (fun e f h => I db 0 f e * I db 2 h f)

private theorem path_453 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [4, 5, 3] b
      (fun x => I db r1 (x 4) (x 5) * I db r2 (x 5) (x 3)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 a m * I db r2 m z))) ≤
    (rsum n (fun a => rsum n (fun m => I db r1 a m))) *
      (rsum n (fun m => rsum n (fun z => I db r2 m z)))
  rw [rsum_comm]
  rw [show (rsum n (fun a => rsum n (fun m => I db r1 a m))) =
      rsum n (fun m => rsum n (fun a => I db r1 a m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m z => I db r2 m z)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m z => I_nonneg db r2 m z) (fun m z => I_le_one db r2 m z)

private theorem path_013 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [0, 1, 3] b
      (fun x => I db r1 (x 0) (x 1) * I db r2 (x 3) (x 0)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun m => rsum n (fun a => rsum n (fun z =>
      I db r1 m a * I db r2 z m))) ≤
    (rsum n (fun x => rsum n (fun y => I db r1 x y))) *
      (rsum n (fun x => rsum n (fun y => I db r2 x y)))
  rw [show (rsum n (fun x => rsum n (fun y => I db r2 x y))) =
      rsum n (fun m => rsum n (fun z => I db r2 z m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 m a) (fun m z => I db r2 z m)
    (fun m a => I_nonneg db r1 m a) (fun m a => I_le_one db r1 m a)
    (fun m z => I_nonneg db r2 z m) (fun m z => I_le_one db r2 z m)

private theorem path_245 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [2, 4, 5] b
      (fun x => I db r1 (x 2) (x 4) * I db r2 (x 4) (x 5)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 a m * I db r2 m z))) ≤
    (rsum n (fun a => rsum n (fun m => I db r1 a m))) *
      (rsum n (fun m => rsum n (fun z => I db r2 m z)))
  rw [rsum_comm]
  rw [show (rsum n (fun a => rsum n (fun m => I db r1 a m))) =
      rsum n (fun m => rsum n (fun a => I db r1 a m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m z => I db r2 m z)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m z => I_nonneg db r2 m z) (fun m z => I_le_one db r2 m z)

private theorem path_124 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [1, 2, 4] b
      (fun x => I db r1 (x 1) (x 2) * I db r2 (x 2) (x 4)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 a m * I db r2 m z))) ≤
    (rsum n (fun a => rsum n (fun m => I db r1 a m))) *
      (rsum n (fun m => rsum n (fun z => I db r2 m z)))
  rw [rsum_comm]
  rw [show (rsum n (fun a => rsum n (fun m => I db r1 a m))) =
      rsum n (fun m => rsum n (fun a => I db r1 a m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m z => I db r2 m z)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m z => I_nonneg db r2 m z) (fun m z => I_le_one db r2 m z)

private theorem path_035 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [0, 3, 5] b
      (fun x => I db r1 (x 3) (x 0) * I db r2 (x 5) (x 3)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 m a * I db r2 z m))) ≤
    (rsum n (fun x => rsum n (fun y => I db r1 x y))) *
      (rsum n (fun x => rsum n (fun y => I db r2 x y)))
  rw [rsum_comm]
  rw [show (rsum n (fun x => rsum n (fun y => I db r1 x y))) =
      rsum n (fun m => rsum n (fun a => I db r1 m a)) by rw [rsum_comm]]
  rw [show (rsum n (fun x => rsum n (fun y => I db r2 x y))) =
      rsum n (fun m => rsum n (fun z => I db r2 z m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 m a) (fun m z => I db r2 z m)
    (fun m a => I_nonneg db r1 m a) (fun m a => I_le_one db r1 m a)
    (fun m z => I_nonneg db r2 z m) (fun m z => I_le_one db r2 z m)

private theorem prod_nonneg_of_S (n : Nat) (db : DB) (r s : Nat) :
    0 ≤ S n db r * S n db s := Int.mul_nonneg (S_nonneg n db r) (S_nonneg n db s)

private theorem bound_025 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 2 := by
  rw [ref_eq_integrate]
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b
        (fun x => I db 0 (x 0) (x 1) * I db 0 (x 2) (x 3) * I db 2 (x 4) (x 5)) := by
          apply integrate_mono
          intro x
          dsimp [Fprod]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          exact prod7_le_keep3 (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = S n db 0 * S n db 0 * S n db 2 := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,2,3,4,5]) b]
          exact integrate_pairs_canonical n db b 0 0 2

private theorem bound_046 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 1 * S n db 2 := by
  rw [ref_eq_integrate]
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b
        (fun x => I db 0 (x 0) (x 1) * I db 1 (x 2) (x 4) * I db 2 (x 5) (x 3)) := by
          apply integrate_mono
          intro x
          dsimp [Fprod]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          exact prod7_le_keep3_aeg (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = S n db 0 * S n db 1 * S n db 2 := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,2,4,5,3]) b]
          exact pairF_046 n db b

private theorem bound_135 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 2 := by
  rw [ref_eq_integrate]
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b
        (fun x => I db 0 (x 1) (x 2) * I db 0 (x 3) (x 0) * I db 2 (x 4) (x 5)) := by
          apply integrate_mono
          intro x
          dsimp [Fprod]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          exact prod7_le_keep3_bdf (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = S n db 0 * S n db 0 * S n db 2 := by
          rw [integrate_perm n (by native_decide : fv.Perm [1,2,3,0,4,5]) b]
          exact pairF_135 n db b

private theorem bound_0156 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 2 * S n db 2 := by
  rw [ref_eq_integrate]
  let A : Bind → Int := fun x => I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)
  let B : Bind → Int := fun x => I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b (fun x => A x * B x) := by
          apply integrate_mono
          intro x
          dsimp [Fprod, A, B]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          rw [show (I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)) *
              (I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)) =
              I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2) * I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3) by ac_rfl]
          exact prod7_le_keep4_abfg (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = integrate n [0,1,2,4,5,3] b (fun x => A x * B x) := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,2,4,5,3]) b]
    _ = integrate n [0,1,2] b A * integrate n [4,5,3] b B := by
          dsimp [A, B]
          exact split_0156 n db b
    _ ≤ (S n db 0 * S n db 0) * (S n db 2 * S n db 2) := by
          apply Int.mul_le_mul
          · exact integrate_path_canonical n db b 0 0
          · exact path_453 n db b 2 2
          · apply integrate_nonneg
            intro x
            dsimp [B]
            exact Int.mul_nonneg (I_nonneg db 2 (x 4) (x 5)) (I_nonneg db 2 (x 5) (x 3))
          · exact Int.mul_nonneg (S_nonneg n db 0) (S_nonneg n db 0)
    _ = S n db 0 * S n db 0 * S n db 2 * S n db 2 := by ac_rfl

private theorem bound_0345 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 1 * S n db 2 := by
  rw [ref_eq_integrate]
  let A : Bind → Int := fun x => I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)
  let B : Bind → Int := fun x => I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5)
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b (fun x => A x * B x) := by
          apply integrate_mono
          intro x
          dsimp [Fprod, A, B]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          rw [show (I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)) *
              (I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5)) =
              I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0) * I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5) by ac_rfl]
          exact prod7_le_keep4_adef (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = integrate n [0,1,3,2,4,5] b (fun x => A x * B x) := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,3,2,4,5]) b]
    _ = integrate n [0,1,3] b A * integrate n [2,4,5] b B := by
          dsimp [A, B]
          exact split_0345 n db b
    _ ≤ (S n db 0 * S n db 0) * (S n db 1 * S n db 2) := by
          apply Int.mul_le_mul
          · exact path_013 n db b 0 0
          · exact path_245 n db b 1 2
          · apply integrate_nonneg
            intro x
            dsimp [B]
            exact Int.mul_nonneg (I_nonneg db 1 (x 2) (x 4)) (I_nonneg db 2 (x 4) (x 5))
          · exact Int.mul_nonneg (S_nonneg n db 0) (S_nonneg n db 0)
    _ = S n db 0 * S n db 0 * S n db 1 * S n db 2 := by ac_rfl

private theorem bound_1346 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 1 * S n db 2 := by
  rw [ref_eq_integrate]
  let A : Bind → Int := fun x => I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)
  let B : Bind → Int := fun x => I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3)
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b (fun x => A x * B x) := by
          apply integrate_mono
          intro x
          dsimp [Fprod, A, B]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          rw [show (I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)) *
              (I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3)) =
              I db 0 (x 1) (x 2) * I db 0 (x 3) (x 0) * I db 1 (x 2) (x 4) * I db 2 (x 5) (x 3) by ac_rfl]
          exact prod7_le_keep4_bdeg (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = integrate n [1,2,4,0,3,5] b (fun x => A x * B x) := by
          rw [integrate_perm n (by native_decide : fv.Perm [1,2,4,0,3,5]) b]
    _ = integrate n [1,2,4] b A * integrate n [0,3,5] b B := by
          dsimp [A, B]
          exact split_1346 n db b
    _ ≤ (S n db 0 * S n db 1) * (S n db 0 * S n db 2) := by
          apply Int.mul_le_mul
          · exact path_124 n db b 0 1
          · exact path_035 n db b 0 2
          · apply integrate_nonneg
            intro x
            dsimp [B]
            exact Int.mul_nonneg (I_nonneg db 0 (x 3) (x 0)) (I_nonneg db 2 (x 5) (x 3))
          · exact Int.mul_nonneg (S_nonneg n db 0) (S_nonneg n db 1)
    _ = S n db 0 * S n db 0 * S n db 1 * S n db 2 := by ac_rfl


private def vcov7 (w0 w1 w2 w3 w4 w5 w6 d : Nat) : Prop :=
  d ≤ w0 + w3 ∧ d ≤ w0 + w1 ∧ d ≤ w1 + w2 + w4 ∧
    d ≤ w2 + w3 + w6 ∧ d ≤ w4 + w5 ∧ d ≤ w5 + w6

private theorem peel7 (w0 w1 w2 w3 w4 w5 w6 d : Nat) (hd : 0 < d)
    (hc : vcov7 w0 w1 w2 w3 w4 w5 w6 d) :
    (0 < w0 ∧ 0 < w2 ∧ 0 < w5 ∧ vcov7 (w0-1) w1 (w2-1) w3 w4 (w5-1) w6 (d-1)) ∨
    (0 < w0 ∧ 0 < w4 ∧ 0 < w6 ∧ vcov7 (w0-1) w1 w2 w3 (w4-1) w5 (w6-1) (d-1)) ∨
    (0 < w1 ∧ 0 < w3 ∧ 0 < w5 ∧ vcov7 w0 (w1-1) w2 (w3-1) w4 (w5-1) w6 (d-1)) ∨
    (0 < w0 ∧ 0 < w1 ∧ 0 < w5 ∧ 0 < w6 ∧
      vcov7 (w0-1) (w1-1) w2 w3 w4 (w5-1) (w6-1) (d-1)) ∨
    (0 < w0 ∧ 0 < w3 ∧ 0 < w4 ∧ 0 < w5 ∧
      vcov7 (w0-1) w1 w2 (w3-1) (w4-1) (w5-1) w6 (d-1)) ∨
    (0 < w1 ∧ 0 < w3 ∧ 0 < w4 ∧ 0 < w6 ∧
      vcov7 w0 (w1-1) w2 (w3-1) (w4-1) w5 (w6-1) (d-1)) := by
  unfold vcov7 at hc ⊢
  by_cases h0 : w0 = 0 <;> by_cases h1 : w1 = 0 <;> by_cases h2 : w2 = 0 <;>
  by_cases h3 : w3 = 0 <;> by_cases h4 : w4 = 0 <;> by_cases h5 : w5 = 0 <;>
  by_cases h6 : w6 = 0
  all_goals
    first
    | exact Or.inl ⟨by omega, by omega, by omega, by omega⟩
    | exact Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega⟩)
    | exact Or.inr (Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega⟩))
    | exact Or.inr (Or.inr (Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega, by omega⟩)))
    | exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega, by omega⟩))))
    | exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inr ⟨by omega, by omega, by omega, by omega, by omega⟩))))

private theorem N_zero_of_s_zero (N s0 s1 s2 s3 s4 s5 s6 : Nat)
    (h025 : N ≤ s0 * s2 * s5) (h046 : N ≤ s0 * s4 * s6)
    (h135 : N ≤ s1 * s3 * s5) (h0156 : N ≤ s0 * s1 * s5 * s6)
    (h0345 : N ≤ s0 * s3 * s4 * s5) (h1346 : N ≤ s1 * s3 * s4 * s6)
    (hz : s0 = 0 ∨ s1 = 0 ∨ s2 = 0 ∨ s3 = 0 ∨ s4 = 0 ∨ s5 = 0 ∨ s6 = 0) : N = 0 := by
  rcases hz with h0 | h1 | h2 | h3 | h4 | h5 | h6
  all_goals
    first
    | have hh := h025; simp [h0] at hh; omega
    | have hh := h135; simp [h1] at hh; omega
    | have hh := h025; simp [h2] at hh; omega
    | have hh := h135; simp [h3] at hh; omega
    | have hh := h046; simp [h4] at hh; omega
    | have hh := h025; simp [h5] at hh; omega
    | have hh := h046; simp [h6] at hh; omega

private theorem prod7_pos (s0 s1 s2 s3 s4 s5 s6 w0 w1 w2 w3 w4 w5 w6 : Nat)
    (h0 : 0 < s0) (h1 : 0 < s1) (h2 : 0 < s2) (h3 : 0 < s3)
    (h4 : 0 < s4) (h5 : 0 < s5) (h6 : 0 < s6) :
    0 < s0^w0 * s1^w1 * s2^w2 * s3^w3 * s4^w4 * s5^w5 * s6^w6 := by
  have p0 : 0 < s0^w0 := Nat.pow_pos h0
  have p1 : 0 < s1^w1 := Nat.pow_pos h1
  have p2 : 0 < s2^w2 := Nat.pow_pos h2
  have p3 : 0 < s3^w3 := Nat.pow_pos h3
  have p4 : 0 < s4^w4 := Nat.pow_pos h4
  have p5 : 0 < s5^w5 := Nat.pow_pos h5
  have p6 : 0 < s6^w6 := Nat.pow_pos h6
  exact Nat.mul_pos (Nat.mul_pos (Nat.mul_pos (Nat.mul_pos (Nat.mul_pos (Nat.mul_pos p0 p1) p2) p3) p4) p5) p6

private theorem frac_bound (N s0 s1 s2 s3 s4 s5 s6 : Nat)
    (hs0 : 0 < s0) (hs1 : 0 < s1) (hs2 : 0 < s2) (hs3 : 0 < s3)
    (hs4 : 0 < s4) (hs5 : 0 < s5) (hs6 : 0 < s6)
    (d w0 w1 w2 w3 w4 w5 w6 : Nat)
    (h025 : N ≤ s0 * s2 * s5) (h046 : N ≤ s0 * s4 * s6)
    (h135 : N ≤ s1 * s3 * s5) (h0156 : N ≤ s0 * s1 * s5 * s6)
    (h0345 : N ≤ s0 * s3 * s4 * s5) (h1346 : N ≤ s1 * s3 * s4 * s6)
    (hc : vcov7 w0 w1 w2 w3 w4 w5 w6 d) :
    N ^ d ≤ s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
  induction d using Nat.strongRecOn generalizing w0 w1 w2 w3 w4 w5 w6 with
  | ind d ih =>
      by_cases hd0 : d = 0
      · subst d
        simp
        have hp := prod7_pos s0 s1 s2 s3 s4 s5 s6 w0 w1 w2 w3 w4 w5 w6
          hs0 hs1 hs2 hs3 hs4 hs5 hs6
        exact Nat.succ_le_of_lt hp
      · have hd : 0 < d := by omega
        rcases peel7 w0 w1 w2 w3 w4 w5 w6 d hd hc with hp | hp | hp | hp | hp | hp
        · rcases hp with ⟨hw0, hw2, hw5, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) w1 (w2-1) w3 w4 (w5-1) w6 hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ w1 * s2 ^ (w2-1) * s3 ^ w3 * s4 ^ w4 *
                s5 ^ (w5-1) * s6 ^ w6) * (s0 * s2 * s5) := Nat.mul_le_mul hi h025
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e2 : w2 - 1 + 1 = w2 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e2, ← e5]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w2 - 1 + 1) - 1 = w2 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw0, hw4, hw6, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) w1 w2 w3 (w4-1) w5 (w6-1) hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ (w4-1) * s5 ^ w5 *
                s6 ^ (w6-1)) * (s0 * s4 * s6) := Nat.mul_le_mul hi h046
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e4 : w4 - 1 + 1 = w4 := Nat.sub_add_cancel (by omega)
            have e6 : w6 - 1 + 1 = w6 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e4, ← e6]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w4 - 1 + 1) - 1 = w4 - 1 by omega]
            simp only [show (w6 - 1 + 1) - 1 = w6 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw1, hw3, hw5, hc'⟩
          have hi := ih (d-1) (by omega) w0 (w1-1) w2 (w3-1) w4 (w5-1) w6 hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ w0 * s1 ^ (w1-1) * s2 ^ w2 * s3 ^ (w3-1) * s4 ^ w4 *
                s5 ^ (w5-1) * s6 ^ w6) * (s1 * s3 * s5) := Nat.mul_le_mul hi h135
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e1 : w1 - 1 + 1 = w1 := Nat.sub_add_cancel (by omega)
            have e3 : w3 - 1 + 1 = w3 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            rw [← e1, ← e3, ← e5]
            simp only [show (w1 - 1 + 1) - 1 = w1 - 1 by omega]
            simp only [show (w3 - 1 + 1) - 1 = w3 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw0, hw1, hw5, hw6, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) (w1-1) w2 w3 w4 (w5-1) (w6-1) hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ (w1-1) * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 *
                s5 ^ (w5-1) * s6 ^ (w6-1)) * (s0 * s1 * s5 * s6) := Nat.mul_le_mul hi h0156
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e1 : w1 - 1 + 1 = w1 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            have e6 : w6 - 1 + 1 = w6 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e1, ← e5, ← e6]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w1 - 1 + 1) - 1 = w1 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [show (w6 - 1 + 1) - 1 = w6 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw0, hw3, hw4, hw5, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) w1 w2 (w3-1) (w4-1) (w5-1) w6 hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ w1 * s2 ^ w2 * s3 ^ (w3-1) * s4 ^ (w4-1) *
                s5 ^ (w5-1) * s6 ^ w6) * (s0 * s3 * s4 * s5) := Nat.mul_le_mul hi h0345
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e3 : w3 - 1 + 1 = w3 := Nat.sub_add_cancel (by omega)
            have e4 : w4 - 1 + 1 = w4 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e3, ← e4, ← e5]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w3 - 1 + 1) - 1 = w3 - 1 by omega]
            simp only [show (w4 - 1 + 1) - 1 = w4 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw1, hw3, hw4, hw6, hc'⟩
          have hi := ih (d-1) (by omega) w0 (w1-1) w2 (w3-1) (w4-1) w5 (w6-1) hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ w0 * s1 ^ (w1-1) * s2 ^ w2 * s3 ^ (w3-1) * s4 ^ (w4-1) *
                s5 ^ w5 * s6 ^ (w6-1)) * (s1 * s3 * s4 * s6) := Nat.mul_le_mul hi h1346
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e1 : w1 - 1 + 1 = w1 := Nat.sub_add_cancel (by omega)
            have e3 : w3 - 1 + 1 = w3 := Nat.sub_add_cancel (by omega)
            have e4 : w4 - 1 + 1 = w4 := Nat.sub_add_cancel (by omega)
            have e6 : w6 - 1 + 1 = w6 := Nat.sub_add_cancel (by omega)
            rw [← e1, ← e3, ← e4, ← e6]
            simp only [show (w1 - 1 + 1) - 1 = w1 - 1 by omega]
            simp only [show (w3 - 1 + 1) - 1 = w3 - 1 by omega]
            simp only [show (w4 - 1 + 1) - 1 = w4 - 1 by omega]
            simp only [show (w6 - 1 + 1) - 1 = w6 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl


theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles.FractionalCap.Goal := by
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
          | cons w4 ws5 =>
            cases ws5 with
            | nil => simp [pattern] at hwlen
            | cons w5 ws6 =>
              cases ws6 with
              | nil => simp [pattern] at hwlen
              | cons w6 ws7 =>
                cases ws7 with
                | nil =>
                    let N : Nat := (reference pattern n
                      (fun r x y => if db r x y == 0 then 0 else 1) b).toNat
                    let s0 := support n db ⟨0,0,1⟩ pattern b
                    let s1 := support n db ⟨0,1,2⟩ pattern b
                    let s2 := support n db ⟨0,2,3⟩ pattern b
                    let s3 := support n db ⟨0,3,0⟩ pattern b
                    let s4 := support n db ⟨1,2,4⟩ pattern b
                    let s5 := support n db ⟨2,4,5⟩ pattern b
                    let s6 := support n db ⟨2,5,3⟩ pattern b
                    have hNint : (N : Int) = reference pattern n
                        (fun r x y => if db r x y == 0 then 0 else 1) b := by
                      dsimp [N]
                      exact Int.toNat_of_nonneg (ref_nonneg n db b)
                    have hs0 : (s0 : Int) = S n db 0 := by
                      dsimp [s0]; apply support_eq_S <;> native_decide
                    have hs1 : (s1 : Int) = S n db 0 := by
                      dsimp [s1]; apply support_eq_S <;> native_decide
                    have hs2 : (s2 : Int) = S n db 0 := by
                      dsimp [s2]; apply support_eq_S <;> native_decide
                    have hs3 : (s3 : Int) = S n db 0 := by
                      dsimp [s3]; apply support_eq_S <;> native_decide
                    have hs4 : (s4 : Int) = S n db 1 := by
                      dsimp [s4]; apply support_eq_S <;> native_decide
                    have hs5 : (s5 : Int) = S n db 2 := by
                      dsimp [s5]; apply support_eq_S <;> native_decide
                    have hs6 : (s6 : Int) = S n db 2 := by
                      dsimp [s6]; apply support_eq_S <;> native_decide
                    have h025 : N ≤ s0 * s2 * s5 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs2, hs5]
                      simpa using bound_025 n db b
                    have h046 : N ≤ s0 * s4 * s6 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs4, hs6]
                      simpa using bound_046 n db b
                    have h135 : N ≤ s1 * s3 * s5 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs1, hs3, hs5]
                      simpa using bound_135 n db b
                    have h0156 : N ≤ s0 * s1 * s5 * s6 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs1, hs5, hs6]
                      simpa using bound_0156 n db b
                    have h0345 : N ≤ s0 * s3 * s4 * s5 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs3, hs4, hs5]
                      simpa using bound_0345 n db b
                    have h1346 : N ≤ s1 * s3 * s4 * s6 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs1, hs3, hs4, hs6]
                      simpa using bound_1346 n db b
                    have hvc : vcov7 w0 w1 w2 w3 w4 w5 w6 den := by
                      simp [pattern] at hcov
                      rcases hcov with ⟨hv0, hv1, hv2, hv3, hv4, hv5⟩
                      exact ⟨by omega, by omega, by omega, by omega, by omega, by omega⟩
                    have hNdef : (reference pattern n
                        (fun r x y => if db r x y == 0 then 0 else 1) b).toNat = N := rfl
                    rw [hNdef]
                    by_cases hz : s0 = 0 ∨ s1 = 0 ∨ s2 = 0 ∨ s3 = 0 ∨ s4 = 0 ∨ s5 = 0 ∨ s6 = 0
                    · have hN0 := N_zero_of_s_zero N s0 s1 s2 s3 s4 s5 s6
                        h025 h046 h135 h0156 h0345 h1346 hz
                      rw [hN0]
                      have hzpow : (0 : Nat) ^ den = 0 := by
                        cases den with
                        | zero => omega
                        | succ k => simp
                      rw [hzpow]
                      exact Nat.zero_le _
                    · have hs0p : 0 < s0 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inl h))
                      have hs1p : 0 < s1 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inl h)))
                      have hs2p : 0 < s2 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inl h))))
                      have hs3p : 0 < s3 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inl h)))))
                      have hs4p : 0 < s4 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl h))))))
                      have hs5p : 0 < s5 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl h)))))))
                      have hs6p : 0 < s6 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr h)))))))
                      simpa [s0, s1, s2, s3, s4, s5, s6, pattern, Nat.mul_assoc] using
                        (frac_bound N s0 s1 s2 s3 s4 s5 s6 hs0p hs1p hs2p hs3p hs4p hs5p hs6p den w0 w1 w2 w3 w4 w5 w6
                          h025 h046 h135 h0156 h0345 h1346 hvc)
                | cons w7 _ => simp [pattern] at hwlen

/-! ## CollisionCorrection: Möbius inversion on equality partitions. -/

/-- Partition-helper definitions mirroring the `collapsed` definition. -/
private def headOf : List Nat → Nat
  | h :: _ => h
  | [] => 0

private def Heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private def Root (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun bl => bl.contains v)).bind List.head?).getD v

private def constOn (bs : List (List Nat)) (x : Bind) : Bool :=
  bs.all (fun bl => bl.all (fun v => x v == x (headOf bl)))

/-- Boolean structural spec of a partition of the fixed variable list. -/
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

/-- Integration helpers (Std-only). -/
private theorem sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_eq_zero {α} (l : List α) (f : α → Int) (h : ∀ x ∈ l, f x = 0) :
    (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]
      simp

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

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

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
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
    apply sum_congr
    intro x hx
    apply sum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem integrate_append (n : Nat) (xs ys : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (xs ++ ys) b f = integrate n xs b (fun b' => integrate n ys b' f) := by
  induction xs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n (vs ++ ys) (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun b' => integrate n ys b' f))).sum
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem cc_integrate_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys)
    (b : Bind) (f : Bind → Int) : integrate n xs b f = integrate n ys b f := by
  induction h generalizing b with
  | nil => rfl
  | cons x h ih =>
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      apply sum_congr
      intro t ht
      exact ih (put b x t)
  | swap x y l => exact (integrate_swap n x y l b f).symm
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
      apply sum_congr
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
  apply sum_congr
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
  apply sum_eq_zero
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

private theorem perm_append_left {α} {l1 l2 : List α} (h : l1.Perm l2) (l3 : List α) :
    (l1 ++ l3).Perm (l2 ++ l3) := by
  induction h with
  | nil => rfl
  | cons x h ih => exact .cons x ih
  | swap x y l => exact .swap x y (l ++ l3)
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_append_right {α} (l1 : List α) {l3 l4 : List α} (h : l3.Perm l4) :
    (l1 ++ l3).Perm (l1 ++ l4) := by
  induction l1 with
  | nil => simpa using h
  | cons a l1 ih => exact .cons a ih

private theorem perm_append {α} {l1 l2 l3 l4 : List α} (h1 : l1.Perm l2) (h2 : l3.Perm l4) :
    (l1 ++ l3).Perm (l2 ++ l4) := by
  exact (perm_append_left h1 l3).trans (perm_append_right l2 h2)

private theorem cons_append_perm (a : Nat) (l : List Nat) : (a :: l).Perm (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons x l ih => exact (List.Perm.swap x a l).trans (.cons x ih)

private theorem filter_append_compl (p : Nat → Bool) (l : List Nat) :
    l.Perm ((l.filter p) ++ (l.filter (fun x => !p x))) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : p a = true
      · simpa [ha] using (List.Perm.cons a ih)
      · have ha' : p a = false := by cases h : p a <;> simp_all
        have hstep := (List.Perm.cons a ih).trans
          (perm_append (cons_append_perm a (l.filter p))
            (List.Perm.refl (l.filter (fun x => !p x))))
        simpa [ha'] using hstep

private theorem perm_filter_of_nodup {l l' : List Nat} (hnd : l.Nodup) (hnd' : l'.Nodup)
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
    (hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → x 0 = y 0 → F x = F y) :
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
    have hp := perm_filter_of_nodup hspec.1 hVnd hHsub
    simpa using hp.symm
  have hpermV : pattern.freeVars.Perm (H ++ M) := by
    have h1 := filter_append_compl H.contains pattern.freeVars
    have h2 : (pattern.freeVars.filter (fun v => H.contains v) ++
        pattern.freeVars.filter (fun v => !H.contains v)).Perm
        (H ++ M) := perm_append hfilt (by rfl)
    exact h1.trans h2
  rw [cc_integrate_perm n hpermV b (fun x => if constOn bs x then F x else 0)]
  rw [integrate_append]
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
              have hx0eq : x 0 = x0 0 := hxFx0 0 (by native_decide)
              exact hF x x0 hxFx0 hx0eq
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

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum = (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l' with
  | nil =>
      change (l.map (fun _ => (0 : Int))).sum = 0
      exact sum_zero l
  | cons b l' ih =>
      rw [show (fun a => (List.map (F a) (b :: l')).sum) =
          (fun a => F a b + (l'.map (F a)).sum) by funext a; rfl]
      rw [sum_add, ih]
      simp

private theorem constOn_eq_of_eq_on (bs : List (List Nat)) (hbs : bs ∈ partitions fv)
    (x y : Bind)
    (heqv : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (y v = y w))) :
    constOn bs x = constOn bs y := by
  have hPS := ps_of_mem hbs
  simp [PSb] at hPS
  rcases hPS with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  unfold constOn
  let A : Bool := bs.all (fun bl => bl.all (fun v => x v == x (headOf bl)))
  let B : Bool := bs.all (fun bl => bl.all (fun v => y v == y (headOf bl)))
  have hforward : A = true → B = true := by
    intro hx
    change bs.all (fun bl => bl.all (fun v => x v == x (headOf bl))) = true at hx
    dsimp [B]
    apply List.all_eq_true.mpr
    intro bl hbl
    apply List.all_eq_true.mpr
    intro v hvbl
    have hvV : v ∈ fv := (hsub bl hbl).2 v hvbl
    have hheadV : headOf bl ∈ fv := by
      cases bl with
      | nil => simp at hvbl
      | cons h t => simpa [headOf] using (hsub (h::t) hbl).2 h (by simp)
    have hxv : (x v == x (headOf bl)) = true :=
      List.all_eq_true.mp (List.all_eq_true.mp hx bl hbl) v hvbl
    have heq0 : x v = x (headOf bl) := beq_iff_eq.mp hxv
    have heq : y v = y (headOf bl) :=
      Iff.mp (heqv v (headOf bl) hvV hheadV) heq0
    exact beq_iff_eq.mpr heq
  have hbackward : B = true → A = true := by
    intro hy
    change bs.all (fun bl => bl.all (fun v => y v == y (headOf bl))) = true at hy
    dsimp [A]
    apply List.all_eq_true.mpr
    intro bl hbl
    apply List.all_eq_true.mpr
    intro v hvbl
    have hvV : v ∈ fv := (hsub bl hbl).2 v hvbl
    have hheadV : headOf bl ∈ fv := by
      cases bl with
      | nil => simp at hvbl
      | cons h t => simpa [headOf] using (hsub (h::t) hbl).2 h (by simp)
    have hyv : (y v == y (headOf bl)) = true :=
      List.all_eq_true.mp (List.all_eq_true.mp hy bl hbl) v hvbl
    have heq0 : y v = y (headOf bl) := beq_iff_eq.mp hyv
    have heq : x v = x (headOf bl) :=
      Iff.mpr (heqv v (headOf bl) hvV hheadV) heq0
    exact beq_iff_eq.mpr heq
  by_cases hxA : A = true
  · have hyB := hforward hxA
    change A = B
    simp [hxA, hyB]
  · have hxA' : A = false := by cases h : A <;> simp_all
    by_cases hyB : B = true
    · have hxA2 := hbackward hyB
      simp [hxA'] at hxA2
    · change A = B
      simp [hxA', hyB]

private theorem nodup_map_eq_of_eq_on (x y : Bind)
    (heqv : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (y v = y w))) :
    (fv.map x).Nodup = (fv.map y).Nodup := by
  have h4_5 : (x 4 = x 5) ↔ (y 4 = y 5) :=
    heqv 4 5 (by native_decide) (by native_decide)
  have h4_1 : (x 4 = x 1) ↔ (y 4 = y 1) :=
    heqv 4 1 (by native_decide) (by native_decide)
  have h4_2 : (x 4 = x 2) ↔ (y 4 = y 2) :=
    heqv 4 2 (by native_decide) (by native_decide)
  have h4_3 : (x 4 = x 3) ↔ (y 4 = y 3) :=
    heqv 4 3 (by native_decide) (by native_decide)
  have h4_0 : (x 4 = x 0) ↔ (y 4 = y 0) :=
    heqv 4 0 (by native_decide) (by native_decide)
  have h5_1 : (x 5 = x 1) ↔ (y 5 = y 1) :=
    heqv 5 1 (by native_decide) (by native_decide)
  have h5_2 : (x 5 = x 2) ↔ (y 5 = y 2) :=
    heqv 5 2 (by native_decide) (by native_decide)
  have h5_3 : (x 5 = x 3) ↔ (y 5 = y 3) :=
    heqv 5 3 (by native_decide) (by native_decide)
  have h5_0 : (x 5 = x 0) ↔ (y 5 = y 0) :=
    heqv 5 0 (by native_decide) (by native_decide)
  have h1_2 : (x 1 = x 2) ↔ (y 1 = y 2) :=
    heqv 1 2 (by native_decide) (by native_decide)
  have h1_3 : (x 1 = x 3) ↔ (y 1 = y 3) :=
    heqv 1 3 (by native_decide) (by native_decide)
  have h1_0 : (x 1 = x 0) ↔ (y 1 = y 0) :=
    heqv 1 0 (by native_decide) (by native_decide)
  have h2_3 : (x 2 = x 3) ↔ (y 2 = y 3) :=
    heqv 2 3 (by native_decide) (by native_decide)
  have h2_0 : (x 2 = x 0) ↔ (y 2 = y 0) :=
    heqv 2 0 (by native_decide) (by native_decide)
  have h3_0 : (x 3 = x 0) ↔ (y 3 = y 0) :=
    heqv 3 0 (by native_decide) (by native_decide)
  simp [fv, pattern, List.nodup_cons, h4_5, h4_1, h4_2, h4_3, h4_0, h5_1, h5_2, h5_3, h5_0, h1_2, h1_3, h1_0, h2_3, h2_0, h3_0]

private theorem mobius_point (x : Bind) :
    ((partitions fv).map (fun bs => mobius bs *
      (if constOn bs x then (1 : Int) else 0))).sum =
        if (fv.map x).Nodup then (1 : Int) else 0 := by
  by_cases h4_5 : x 4 = x 5
  · 
    by_cases h5_1 : x 5 = x 1
    · 
      by_cases h1_2 : x 1 = x 2
      · 
        by_cases h2_3 : x 2 = x 3
        · 
          by_cases h3_0 : x 3 = x 0
          · 
            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 0 else 42
            have h4_5_symm : x 5 = x 4 := h4_5.symm
            have h5_1_symm : x 1 = x 5 := h5_1.symm
            have h1_2_symm : x 2 = x 1 := h1_2.symm
            have h2_3_symm : x 3 = x 2 := h2_3.symm
            have h3_0_symm : x 0 = x 3 := h3_0.symm
            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
              intro v w hv hw
              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              simp [xc] <;> omega
            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
              intro bs hbs
              exact constOn_eq_of_eq_on bs hbs x xc hxeq
            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
              nodup_map_eq_of_eq_on x xc hxeq
            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bs hbs
              rw [hconst bs hbs]
            refine hsum.trans ?_
            let b : Bool := (fv.map xc).Nodup
            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
            simp [hb]
            native_decide
          · 
            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 1 else 42
            have h4_5_symm : x 5 = x 4 := h4_5.symm
            have h5_1_symm : x 1 = x 5 := h5_1.symm
            have h1_2_symm : x 2 = x 1 := h1_2.symm
            have h2_3_symm : x 3 = x 2 := h2_3.symm
            have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
              intro v w hv hw
              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              simp [xc] <;> omega
            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
              intro bs hbs
              exact constOn_eq_of_eq_on bs hbs x xc hxeq
            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
              nodup_map_eq_of_eq_on x xc hxeq
            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bs hbs
              rw [hconst bs hbs]
            refine hsum.trans ?_
            let b : Bool := (fv.map xc).Nodup
            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
            simp [hb]
            native_decide
        · 
          by_cases h2_0 : x 2 = x 0
          · 
            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 0 else 42
            have h4_5_symm : x 5 = x 4 := h4_5.symm
            have h5_1_symm : x 1 = x 5 := h5_1.symm
            have h1_2_symm : x 2 = x 1 := h1_2.symm
            have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
            have h2_0_symm : x 0 = x 2 := h2_0.symm
            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
              intro v w hv hw
              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              simp [xc] <;> omega
            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
              intro bs hbs
              exact constOn_eq_of_eq_on bs hbs x xc hxeq
            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
              nodup_map_eq_of_eq_on x xc hxeq
            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bs hbs
              rw [hconst bs hbs]
            refine hsum.trans ?_
            let b : Bool := (fv.map xc).Nodup
            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
            simp [hb]
            native_decide
          · 
            by_cases h3_0 : x 3 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 1 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : x 1 = x 5 := h5_1.symm
              have h1_2_symm : x 2 = x 1 := h1_2.symm
              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
              have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
              have h3_0_symm : x 0 = x 3 := h3_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 2 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : x 1 = x 5 := h5_1.symm
              have h1_2_symm : x 2 = x 1 := h1_2.symm
              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
              have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
      · 
        by_cases h1_3 : x 1 = x 3
        · 
          by_cases h3_0 : x 3 = x 0
          · 
            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 0 else 42
            have h4_5_symm : x 5 = x 4 := h4_5.symm
            have h5_1_symm : x 1 = x 5 := h5_1.symm
            have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
            have h1_3_symm : x 3 = x 1 := h1_3.symm
            have h3_0_symm : x 0 = x 3 := h3_0.symm
            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
              intro v w hv hw
              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              simp [xc] <;> omega
            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
              intro bs hbs
              exact constOn_eq_of_eq_on bs hbs x xc hxeq
            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
              nodup_map_eq_of_eq_on x xc hxeq
            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bs hbs
              rw [hconst bs hbs]
            refine hsum.trans ?_
            let b : Bool := (fv.map xc).Nodup
            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
            simp [hb]
            native_decide
          · 
            by_cases h2_0 : x 2 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 1 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : x 1 = x 5 := h5_1.symm
              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
              have h1_3_symm : x 3 = x 1 := h1_3.symm
              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
              have h2_0_symm : x 0 = x 2 := h2_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 2 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : x 1 = x 5 := h5_1.symm
              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
              have h1_3_symm : x 3 = x 1 := h1_3.symm
              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
              have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
        · 
          by_cases h2_3 : x 2 = x 3
          · 
            by_cases h1_0 : x 1 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 0 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : x 1 = x 5 := h5_1.symm
              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
              have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
              have h2_3_symm : x 3 = x 2 := h2_3.symm
              have h1_0_symm : x 0 = x 1 := h1_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h3_0 : x 3 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 1 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h2_3_symm : x 3 = x 2 := h2_3.symm
                have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                have h3_0_symm : x 0 = x 3 := h3_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 2 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h2_3_symm : x 3 = x 2 := h2_3.symm
                have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
          · 
            by_cases h1_0 : x 1 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 0 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : x 1 = x 5 := h5_1.symm
              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
              have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
              have h1_0_symm : x 0 = x 1 := h1_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h2_0 : x 2 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 1 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                have h2_0_symm : x 0 = x 2 := h2_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 2 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 3 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
    · 
      by_cases h5_2 : x 5 = x 2
      · 
        by_cases h2_3 : x 2 = x 3
        · 
          by_cases h3_0 : x 3 = x 0
          · 
            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 0 else 42
            have h4_5_symm : x 5 = x 4 := h4_5.symm
            have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
            have h5_2_symm : x 2 = x 5 := h5_2.symm
            have h2_3_symm : x 3 = x 2 := h2_3.symm
            have h3_0_symm : x 0 = x 3 := h3_0.symm
            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
              intro v w hv hw
              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              simp [xc] <;> omega
            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
              intro bs hbs
              exact constOn_eq_of_eq_on bs hbs x xc hxeq
            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
              nodup_map_eq_of_eq_on x xc hxeq
            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bs hbs
              rw [hconst bs hbs]
            refine hsum.trans ?_
            let b : Bool := (fv.map xc).Nodup
            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
            simp [hb]
            native_decide
          · 
            by_cases h1_0 : x 1 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 1 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
              have h5_2_symm : x 2 = x 5 := h5_2.symm
              have h2_3_symm : x 3 = x 2 := h2_3.symm
              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
              have h1_0_symm : x 0 = x 1 := h1_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 2 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
              have h5_2_symm : x 2 = x 5 := h5_2.symm
              have h2_3_symm : x 3 = x 2 := h2_3.symm
              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
              have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
        · 
          by_cases h1_3 : x 1 = x 3
          · 
            by_cases h2_0 : x 2 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 0 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
              have h5_2_symm : x 2 = x 5 := h5_2.symm
              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
              have h1_3_symm : x 3 = x 1 := h1_3.symm
              have h2_0_symm : x 0 = x 2 := h2_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h3_0 : x 3 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 1 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h1_3_symm : x 3 = x 1 := h1_3.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have h3_0_symm : x 0 = x 3 := h3_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 2 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h1_3_symm : x 3 = x 1 := h1_3.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
          · 
            by_cases h2_0 : x 2 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 0 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
              have h5_2_symm : x 2 = x 5 := h5_2.symm
              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
              have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
              have h2_0_symm : x 0 = x 2 := h2_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h1_0 : x 1 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 1 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have h1_0_symm : x 0 = x 1 := h1_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 2 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : x 2 = x 5 := h5_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 3 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : x 2 = x 5 := h5_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
      · 
        by_cases h1_2 : x 1 = x 2
        · 
          by_cases h5_3 : x 5 = x 3
          · 
            by_cases h3_0 : x 3 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 0 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
              have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
              have h1_2_symm : x 2 = x 1 := h1_2.symm
              have h5_3_symm : x 3 = x 5 := h5_3.symm
              have h3_0_symm : x 0 = x 3 := h3_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h2_0 : x 2 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 1 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h5_3_symm : x 3 = x 5 := h5_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h2_0_symm : x 0 = x 2 := h2_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 2 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h5_3_symm : x 3 = x 5 := h5_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
          · 
            by_cases h2_3 : x 2 = x 3
            · 
              by_cases h5_0 : x 5 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 0 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                have h2_3_symm : x 3 = x 2 := h2_3.symm
                have h5_0_symm : x 0 = x 5 := h5_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 1 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 2 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
            · 
              by_cases h5_0 : x 5 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 0 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h5_0_symm : x 0 = x 5 := h5_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 1 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h3_0 : x 3 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 2 else 42
                    have h4_5_symm : x 5 = x 4 := h4_5.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : x 2 = x 1 := h1_2.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h3_0_symm : x 0 = x 3 := h3_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 3 else 42
                    have h4_5_symm : x 5 = x 4 := h4_5.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : x 2 = x 1 := h1_2.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
        · 
          by_cases h5_3 : x 5 = x 3
          · 
            by_cases h3_0 : x 3 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 0 else 42
              have h4_5_symm : x 5 = x 4 := h4_5.symm
              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
              have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
              have h5_3_symm : x 3 = x 5 := h5_3.symm
              have h3_0_symm : x 0 = x 3 := h3_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h1_0 : x 1 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 1 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_3_symm : x 3 = x 5 := h5_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h1_0_symm : x 0 = x 1 := h1_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 2 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_3_symm : x 3 = x 5 := h5_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 3 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_3_symm : x 3 = x 5 := h5_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
          · 
            by_cases h1_3 : x 1 = x 3
            · 
              by_cases h5_0 : x 5 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 0 else 42
                have h4_5_symm : x 5 = x 4 := h4_5.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                have h1_3_symm : x 3 = x 1 := h1_3.symm
                have h5_0_symm : x 0 = x 5 := h5_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 1 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h1_3_symm : x 3 = x 1 := h1_3.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h2_0 : x 2 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 2 else 42
                    have h4_5_symm : x 5 = x 4 := h4_5.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h1_3_symm : x 3 = x 1 := h1_3.symm
                    have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h2_0_symm : x 0 = x 2 := h2_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 3 else 42
                    have h4_5_symm : x 5 = x 4 := h4_5.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h1_3_symm : x 3 = x 1 := h1_3.symm
                    have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
            · 
              by_cases h2_3 : x 2 = x 3
              · 
                by_cases h5_0 : x 5 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 0 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h5_0_symm : x 0 = x 5 := h5_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h1_0 : x 1 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 1 else 42
                    have h4_5_symm : x 5 = x 4 := h4_5.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_3_symm : x 3 = x 2 := h2_3.symm
                    have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                    have h1_0_symm : x 0 = x 1 := h1_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h3_0 : x 3 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 2 else 42
                      have h4_5_symm : x 5 = x 4 := h4_5.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                      have h3_0_symm : x 0 = x 3 := h3_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 3 else 42
                      have h4_5_symm : x 5 = x 4 := h4_5.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
              · 
                by_cases h5_0 : x 5 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 0 else 42
                  have h4_5_symm : x 5 = x 4 := h4_5.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h5_0_symm : x 0 = x 5 := h5_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h1_0 : x 1 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 1 else 42
                    have h4_5_symm : x 5 = x 4 := h4_5.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                    have h1_0_symm : x 0 = x 1 := h1_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h2_0 : x 2 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 2 else 42
                      have h4_5_symm : x 5 = x 4 := h4_5.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                      have h2_0_symm : x 0 = x 2 := h2_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h3_0 : x 3 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 3 else 42
                        have h4_5_symm : x 5 = x 4 := h4_5.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h3_0_symm : x 0 = x 3 := h3_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 0 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 4 else 42
                        have h4_5_symm : x 5 = x 4 := h4_5.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
  · 
    by_cases h4_1 : x 4 = x 1
    · 
      by_cases h1_2 : x 1 = x 2
      · 
        by_cases h2_3 : x 2 = x 3
        · 
          by_cases h3_0 : x 3 = x 0
          · 
            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 0 else 42
            have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
            have h4_1_symm : x 1 = x 4 := h4_1.symm
            have h1_2_symm : x 2 = x 1 := h1_2.symm
            have h2_3_symm : x 3 = x 2 := h2_3.symm
            have h3_0_symm : x 0 = x 3 := h3_0.symm
            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
              intro v w hv hw
              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
              simp [xc] <;> omega
            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
              intro bs hbs
              exact constOn_eq_of_eq_on bs hbs x xc hxeq
            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
              nodup_map_eq_of_eq_on x xc hxeq
            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bs hbs
              rw [hconst bs hbs]
            refine hsum.trans ?_
            let b : Bool := (fv.map xc).Nodup
            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
            simp [hb]
            native_decide
          · 
            by_cases h5_0 : x 5 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 1 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : x 1 = x 4 := h4_1.symm
              have h1_2_symm : x 2 = x 1 := h1_2.symm
              have h2_3_symm : x 3 = x 2 := h2_3.symm
              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
              have h5_0_symm : x 0 = x 5 := h5_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 2 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : x 1 = x 4 := h4_1.symm
              have h1_2_symm : x 2 = x 1 := h1_2.symm
              have h2_3_symm : x 3 = x 2 := h2_3.symm
              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
              have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
        · 
          by_cases h5_3 : x 5 = x 3
          · 
            by_cases h2_0 : x 2 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 0 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : x 1 = x 4 := h4_1.symm
              have h1_2_symm : x 2 = x 1 := h1_2.symm
              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
              have h5_3_symm : x 3 = x 5 := h5_3.symm
              have h2_0_symm : x 0 = x 2 := h2_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h3_0 : x 3 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 1 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h5_3_symm : x 3 = x 5 := h5_3.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have h3_0_symm : x 0 = x 3 := h3_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 2 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h5_3_symm : x 3 = x 5 := h5_3.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
          · 
            by_cases h2_0 : x 2 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 0 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : x 1 = x 4 := h4_1.symm
              have h1_2_symm : x 2 = x 1 := h1_2.symm
              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
              have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
              have h2_0_symm : x 0 = x 2 := h2_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h5_0 : x 5 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 1 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have h5_0_symm : x 0 = x 5 := h5_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 2 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 3 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
      · 
        by_cases h5_2 : x 5 = x 2
        · 
          by_cases h1_3 : x 1 = x 3
          · 
            by_cases h3_0 : x 3 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 0 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : x 1 = x 4 := h4_1.symm
              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
              have h5_2_symm : x 2 = x 5 := h5_2.symm
              have h1_3_symm : x 3 = x 1 := h1_3.symm
              have h3_0_symm : x 0 = x 3 := h3_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h2_0 : x 2 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 1 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h1_3_symm : x 3 = x 1 := h1_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h2_0_symm : x 0 = x 2 := h2_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 2 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h1_3_symm : x 3 = x 1 := h1_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
          · 
            by_cases h2_3 : x 2 = x 3
            · 
              by_cases h1_0 : x 1 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h2_3_symm : x 3 = x 2 := h2_3.symm
                have h1_0_symm : x 0 = x 1 := h1_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : x 2 = x 5 := h5_2.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 2 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : x 2 = x 5 := h5_2.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
            · 
              by_cases h1_0 : x 1 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h1_0_symm : x 0 = x 1 := h1_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : x 2 = x 5 := h5_2.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h3_0 : x 3 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 2 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : x 1 = x 4 := h4_1.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_2_symm : x 2 = x 5 := h5_2.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h3_0_symm : x 0 = x 3 := h3_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 3 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : x 1 = x 4 := h4_1.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_2_symm : x 2 = x 5 := h5_2.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
        · 
          by_cases h1_3 : x 1 = x 3
          · 
            by_cases h3_0 : x 3 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 0 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : x 1 = x 4 := h4_1.symm
              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
              have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
              have h1_3_symm : x 3 = x 1 := h1_3.symm
              have h3_0_symm : x 0 = x 3 := h3_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h5_0 : x 5 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 1 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_3_symm : x 3 = x 1 := h1_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h5_0_symm : x 0 = x 5 := h5_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 2 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_3_symm : x 3 = x 1 := h1_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 3 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_3_symm : x 3 = x 1 := h1_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
          · 
            by_cases h5_3 : x 5 = x 3
            · 
              by_cases h1_0 : x 1 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : x 1 = x 4 := h4_1.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h5_3_symm : x 3 = x 5 := h5_3.symm
                have h1_0_symm : x 0 = x 1 := h1_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h5_3_symm : x 3 = x 5 := h5_3.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h2_0 : x 2 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 2 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : x 1 = x 4 := h4_1.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h5_3_symm : x 3 = x 5 := h5_3.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h2_0_symm : x 0 = x 2 := h2_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 3 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : x 1 = x 4 := h4_1.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h5_3_symm : x 3 = x 5 := h5_3.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
            · 
              by_cases h2_3 : x 2 = x 3
              · 
                by_cases h1_0 : x 1 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h1_0_symm : x 0 = x 1 := h1_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h5_0 : x 5 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : x 1 = x 4 := h4_1.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h2_3_symm : x 3 = x 2 := h2_3.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h5_0_symm : x 0 = x 5 := h5_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h3_0 : x 3 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : x 1 = x 4 := h4_1.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h3_0_symm : x 0 = x 3 := h3_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 3 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : x 1 = x 4 := h4_1.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
              · 
                by_cases h1_0 : x 1 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : x 1 = x 4 := h4_1.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_0_symm : x 0 = x 1 := h1_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h5_0 : x 5 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : x 1 = x 4 := h4_1.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h5_0_symm : x 0 = x 5 := h5_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h2_0 : x 2 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : x 1 = x 4 := h4_1.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h2_0_symm : x 0 = x 2 := h2_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h3_0 : x 3 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 3 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : x 1 = x 4 := h4_1.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h3_0_symm : x 0 = x 3 := h3_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 0 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 4 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : x 1 = x 4 := h4_1.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
    · 
      by_cases h5_1 : x 5 = x 1
      · 
        by_cases h4_2 : x 4 = x 2
        · 
          by_cases h2_3 : x 2 = x 3
          · 
            by_cases h3_0 : x 3 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 0 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
              have h5_1_symm : x 1 = x 5 := h5_1.symm
              have h4_2_symm : x 2 = x 4 := h4_2.symm
              have h2_3_symm : x 3 = x 2 := h2_3.symm
              have h3_0_symm : x 0 = x 3 := h3_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h1_0 : x 1 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 1 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h4_2_symm : x 2 = x 4 := h4_2.symm
                have h2_3_symm : x 3 = x 2 := h2_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h1_0_symm : x 0 = x 1 := h1_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 2 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h4_2_symm : x 2 = x 4 := h4_2.symm
                have h2_3_symm : x 3 = x 2 := h2_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
          · 
            by_cases h1_3 : x 1 = x 3
            · 
              by_cases h2_0 : x 2 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h4_2_symm : x 2 = x 4 := h4_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h1_3_symm : x 3 = x 1 := h1_3.symm
                have h2_0_symm : x 0 = x 2 := h2_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_3_symm : x 3 = x 1 := h1_3.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 2 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_3_symm : x 3 = x 1 := h1_3.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
            · 
              by_cases h2_0 : x 2 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h4_2_symm : x 2 = x 4 := h4_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                have h2_0_symm : x 0 = x 2 := h2_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h1_0 : x 1 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h1_0_symm : x 0 = x 1 := h1_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h3_0 : x 3 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 2 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : x 2 = x 4 := h4_2.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h3_0_symm : x 0 = x 3 := h3_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 3 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : x 2 = x 4 := h4_2.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
        · 
          by_cases h1_2 : x 1 = x 2
          · 
            by_cases h4_3 : x 4 = x 3
            · 
              by_cases h3_0 : x 3 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                have h1_2_symm : x 2 = x 1 := h1_2.symm
                have h4_3_symm : x 3 = x 4 := h4_3.symm
                have h3_0_symm : x 0 = x 3 := h3_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h4_3_symm : x 3 = x 4 := h4_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 2 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h4_3_symm : x 3 = x 4 := h4_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
            · 
              by_cases h2_3 : x 2 = x 3
              · 
                by_cases h4_0 : x 4 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h4_0_symm : x 0 = x 4 := h4_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h3_0 : x 3 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : x 2 = x 1 := h1_2.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h2_3_symm : x 3 = x 2 := h2_3.symm
                    have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                    have h3_0_symm : x 0 = x 3 := h3_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 2 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : x 2 = x 1 := h1_2.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h2_3_symm : x 3 = x 2 := h2_3.symm
                    have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
              · 
                by_cases h4_0 : x 4 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h4_0_symm : x 0 = x 4 := h4_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h2_0 : x 2 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : x 2 = x 1 := h1_2.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                    have h2_0_symm : x 0 = x 2 := h2_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h3_0 : x 3 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : x 1 = x 5 := h5_1.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h1_2_symm : x 2 = x 1 := h1_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                      have h3_0_symm : x 0 = x 3 := h3_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 3 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : x 1 = x 5 := h5_1.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h1_2_symm : x 2 = x 1 := h1_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
          · 
            by_cases h4_3 : x 4 = x 3
            · 
              by_cases h3_0 : x 3 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : x 1 = x 5 := h5_1.symm
                have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                have h4_3_symm : x 3 = x 4 := h4_3.symm
                have h3_0_symm : x 0 = x 3 := h3_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h1_0 : x 1 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h4_3_symm : x 3 = x 4 := h4_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h1_0_symm : x 0 = x 1 := h1_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h2_0 : x 2 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 2 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h4_3_symm : x 3 = x 4 := h4_3.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h2_0_symm : x 0 = x 2 := h2_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 3 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h4_3_symm : x 3 = x 4 := h4_3.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
            · 
              by_cases h1_3 : x 1 = x 3
              · 
                by_cases h4_0 : x 4 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : x 1 = x 5 := h5_1.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                  have h1_3_symm : x 3 = x 1 := h1_3.symm
                  have h4_0_symm : x 0 = x 4 := h4_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h3_0 : x 3 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h1_3_symm : x 3 = x 1 := h1_3.symm
                    have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                    have h3_0_symm : x 0 = x 3 := h3_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h2_0 : x 2 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : x 1 = x 5 := h5_1.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h1_3_symm : x 3 = x 1 := h1_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have h2_0_symm : x 0 = x 2 := h2_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 3 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : x 1 = x 5 := h5_1.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h1_3_symm : x 3 = x 1 := h1_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
              · 
                by_cases h2_3 : x 2 = x 3
                · 
                  by_cases h4_0 : x 4 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 0 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_3_symm : x 3 = x 2 := h2_3.symm
                    have h4_0_symm : x 0 = x 4 := h4_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h1_0 : x 1 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 1 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : x 1 = x 5 := h5_1.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h1_0_symm : x 0 = x 1 := h1_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h3_0 : x 3 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 2 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : x 1 = x 5 := h5_1.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_3_symm : x 3 = x 2 := h2_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h3_0_symm : x 0 = x 3 := h3_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 3 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : x 1 = x 5 := h5_1.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_3_symm : x 3 = x 2 := h2_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                · 
                  by_cases h4_0 : x 4 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 0 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : x 1 = x 5 := h5_1.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h4_0_symm : x 0 = x 4 := h4_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h1_0 : x 1 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 1 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : x 1 = x 5 := h5_1.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h1_0_symm : x 0 = x 1 := h1_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h2_0 : x 2 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 2 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : x 1 = x 5 := h5_1.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h2_0_symm : x 0 = x 2 := h2_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h3_0 : x 3 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 3 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : x 1 = x 5 := h5_1.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                          have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                          have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                          have h3_0_symm : x 0 = x 3 := h3_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 1 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 4 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : x 1 = x 5 := h5_1.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                          have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                          have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                          have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
      · 
        by_cases h4_2 : x 4 = x 2
        · 
          by_cases h2_3 : x 2 = x 3
          · 
            by_cases h3_0 : x 3 = x 0
            · 
              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 0 else 42
              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
              have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
              have h4_2_symm : x 2 = x 4 := h4_2.symm
              have h2_3_symm : x 3 = x 2 := h2_3.symm
              have h3_0_symm : x 0 = x 3 := h3_0.symm
              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                intro v w hv hw
                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                simp [xc] <;> omega
              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                intro bs hbs
                exact constOn_eq_of_eq_on bs hbs x xc hxeq
              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                nodup_map_eq_of_eq_on x xc hxeq
              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                apply congrArg List.sum
                apply List.map_congr_left
                intro bs hbs
                rw [hconst bs hbs]
              refine hsum.trans ?_
              let b : Bool := (fv.map xc).Nodup
              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
              simp [hb]
              native_decide
            · 
              by_cases h5_0 : x 5 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 1 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h4_2_symm : x 2 = x 4 := h4_2.symm
                have h2_3_symm : x 3 = x 2 := h2_3.symm
                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                have h5_0_symm : x 0 = x 5 := h5_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h1_0 : x 1 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 2 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h1_0_symm : x 0 = x 1 := h1_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 0 else if v == 0 then 3 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                  have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
          · 
            by_cases h5_3 : x 5 = x 3
            · 
              by_cases h2_0 : x 2 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h4_2_symm : x 2 = x 4 := h4_2.symm
                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                have h5_3_symm : x 3 = x 5 := h5_3.symm
                have h2_0_symm : x 0 = x 2 := h2_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h5_3_symm : x 3 = x 5 := h5_3.symm
                  have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h1_0 : x 1 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 2 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : x 2 = x 4 := h4_2.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h5_3_symm : x 3 = x 5 := h5_3.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h1_0_symm : x 0 = x 1 := h1_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 1 else if v == 0 then 3 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : x 2 = x 4 := h4_2.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h5_3_symm : x 3 = x 5 := h5_3.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
            · 
              by_cases h1_3 : x 1 = x 3
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h1_3_symm : x 3 = x 1 := h1_3.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h5_0 : x 5 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : x 2 = x 4 := h4_2.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h1_3_symm : x 3 = x 1 := h1_3.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h5_0_symm : x 0 = x 5 := h5_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h3_0 : x 3 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : x 2 = x 4 := h4_2.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h1_3_symm : x 3 = x 1 := h1_3.symm
                      have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h3_0_symm : x 0 = x 3 := h3_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 2 else if v == 0 then 3 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : x 2 = x 4 := h4_2.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h1_3_symm : x 3 = x 1 := h1_3.symm
                      have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 3 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : x 2 = x 4 := h4_2.symm
                  have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                  have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                  have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h5_0 : x 5 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 3 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : x 2 = x 4 := h4_2.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h5_0_symm : x 0 = x 5 := h5_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h1_0 : x 1 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 3 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : x 2 = x 4 := h4_2.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h1_0_symm : x 0 = x 1 := h1_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h3_0 : x 3 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 3 else if v == 0 then 3 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : x 2 = x 4 := h4_2.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h3_0_symm : x 0 = x 3 := h3_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 0 else if v == 3 then 3 else if v == 0 then 4 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : x 2 = x 4 := h4_2.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
        · 
          by_cases h5_2 : x 5 = x 2
          · 
            by_cases h4_3 : x 4 = x 3
            · 
              by_cases h3_0 : x 3 = x 0
              · 
                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 0 else 42
                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                have h5_2_symm : x 2 = x 5 := h5_2.symm
                have h4_3_symm : x 3 = x 4 := h4_3.symm
                have h3_0_symm : x 0 = x 3 := h3_0.symm
                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                  intro v w hv hw
                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                  simp [xc] <;> omega
                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                  intro bs hbs
                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                  nodup_map_eq_of_eq_on x xc hxeq
                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro bs hbs
                  rw [hconst bs hbs]
                refine hsum.trans ?_
                let b : Bool := (fv.map xc).Nodup
                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                simp [hb]
                native_decide
              · 
                by_cases h2_0 : x 2 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 1 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h5_2_symm : x 2 = x 5 := h5_2.symm
                  have h4_3_symm : x 3 = x 4 := h4_3.symm
                  have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                  have h2_0_symm : x 0 = x 2 := h2_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h1_0 : x 1 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 2 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : x 2 = x 5 := h5_2.symm
                    have h4_3_symm : x 3 = x 4 := h4_3.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h1_0_symm : x 0 = x 1 := h1_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 0 else if v == 0 then 3 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : x 2 = x 5 := h5_2.symm
                    have h4_3_symm : x 3 = x 4 := h4_3.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                    have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
            · 
              by_cases h2_3 : x 2 = x 3
              · 
                by_cases h4_0 : x 4 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h5_2_symm : x 2 = x 5 := h5_2.symm
                  have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                  have h2_3_symm : x 3 = x 2 := h2_3.symm
                  have h4_0_symm : x 0 = x 4 := h4_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h3_0 : x 3 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : x 2 = x 5 := h5_2.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h2_3_symm : x 3 = x 2 := h2_3.symm
                    have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                    have h3_0_symm : x 0 = x 3 := h3_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h1_0 : x 1 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : x 2 = x 5 := h5_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have h1_0_symm : x 0 = x 1 := h1_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 1 else if v == 0 then 3 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : x 2 = x 5 := h5_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
              · 
                by_cases h1_3 : x 1 = x 3
                · 
                  by_cases h4_0 : x 4 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 0 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : x 2 = x 5 := h5_2.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h1_3_symm : x 3 = x 1 := h1_3.symm
                    have h4_0_symm : x 0 = x 4 := h4_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h2_0 : x 2 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 1 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : x 2 = x 5 := h5_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h1_3_symm : x 3 = x 1 := h1_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h2_0_symm : x 0 = x 2 := h2_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h3_0 : x 3 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 2 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : x 2 = x 5 := h5_2.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h1_3_symm : x 3 = x 1 := h1_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h3_0_symm : x 0 = x 3 := h3_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 2 else if v == 0 then 3 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : x 2 = x 5 := h5_2.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h1_3_symm : x 3 = x 1 := h1_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                · 
                  by_cases h4_0 : x 4 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 3 else if v == 0 then 0 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : x 2 = x 5 := h5_2.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                    have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                    have h4_0_symm : x 0 = x 4 := h4_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h2_0 : x 2 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 3 else if v == 0 then 1 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : x 2 = x 5 := h5_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h2_0_symm : x 0 = x 2 := h2_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h1_0 : x 1 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 3 else if v == 0 then 2 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : x 2 = x 5 := h5_2.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have h1_0_symm : x 0 = x 1 := h1_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h3_0 : x 3 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 3 else if v == 0 then 3 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : x 2 = x 5 := h5_2.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                          have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                          have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                          have h3_0_symm : x 0 = x 3 := h3_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 1 else if v == 3 then 3 else if v == 0 then 4 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : x 2 = x 5 := h5_2.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                          have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                          have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                          have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
          · 
            by_cases h1_2 : x 1 = x 2
            · 
              by_cases h4_3 : x 4 = x 3
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : x 2 = x 1 := h1_2.symm
                  have h4_3_symm : x 3 = x 4 := h4_3.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h5_0 : x 5 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : x 2 = x 1 := h1_2.symm
                    have h4_3_symm : x 3 = x 4 := h4_3.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h5_0_symm : x 0 = x 5 := h5_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h2_0 : x 2 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : x 2 = x 1 := h1_2.symm
                      have h4_3_symm : x 3 = x 4 := h4_3.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h2_0_symm : x 0 = x 2 := h2_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 0 else if v == 0 then 3 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : x 2 = x 1 := h1_2.symm
                      have h4_3_symm : x 3 = x 4 := h4_3.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
              · 
                by_cases h5_3 : x 5 = x 3
                · 
                  by_cases h4_0 : x 4 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 0 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : x 2 = x 1 := h1_2.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h5_3_symm : x 3 = x 5 := h5_3.symm
                    have h4_0_symm : x 0 = x 4 := h4_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h3_0 : x 3 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 1 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : x 2 = x 1 := h1_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h5_3_symm : x 3 = x 5 := h5_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h3_0_symm : x 0 = x 3 := h3_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h2_0 : x 2 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 2 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : x 2 = x 1 := h1_2.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : x 3 = x 5 := h5_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have h2_0_symm : x 0 = x 2 := h2_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 1 else if v == 0 then 3 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : x 2 = x 1 := h1_2.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : x 3 = x 5 := h5_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                · 
                  by_cases h2_3 : x 2 = x 3
                  · 
                    by_cases h4_0 : x 4 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 0 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : x 2 = x 1 := h1_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h2_3_symm : x 3 = x 2 := h2_3.symm
                      have h4_0_symm : x 0 = x 4 := h4_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h5_0 : x 5 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 1 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : x 2 = x 1 := h1_2.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h2_3_symm : x 3 = x 2 := h2_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h5_0_symm : x 0 = x 5 := h5_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h3_0 : x 3 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 2 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : x 2 = x 1 := h1_2.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                          have h2_3_symm : x 3 = x 2 := h2_3.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                          have h3_0_symm : x 0 = x 3 := h3_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 2 else if v == 0 then 3 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : x 2 = x 1 := h1_2.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                          have h2_3_symm : x 3 = x 2 := h2_3.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                          have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                  · 
                    by_cases h4_0 : x 4 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 0 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : x 2 = x 1 := h1_2.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                      have h4_0_symm : x 0 = x 4 := h4_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h5_0 : x 5 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 1 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : x 2 = x 1 := h1_2.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h5_0_symm : x 0 = x 5 := h5_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h2_0 : x 2 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 2 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : x 2 = x 1 := h1_2.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                          have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                          have h2_0_symm : x 0 = x 2 := h2_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          by_cases h3_0 : x 3 = x 0
                          · 
                            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 3 else 42
                            have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                            have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                            have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                            have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                            have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                            have h1_2_symm : x 2 = x 1 := h1_2.symm
                            have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                            have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                            have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                            have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                            have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                            have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                            have h3_0_symm : x 0 = x 3 := h3_0.symm
                            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                              intro v w hv hw
                              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xc] <;> omega
                            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                              intro bs hbs
                              exact constOn_eq_of_eq_on bs hbs x xc hxeq
                            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                              nodup_map_eq_of_eq_on x xc hxeq
                            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                              apply congrArg List.sum
                              apply List.map_congr_left
                              intro bs hbs
                              rw [hconst bs hbs]
                            refine hsum.trans ?_
                            let b : Bool := (fv.map xc).Nodup
                            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                            simp [hb]
                            native_decide
                          · 
                            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 2 else if v == 3 then 3 else if v == 0 then 4 else 42
                            have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                            have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                            have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                            have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                            have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                            have h1_2_symm : x 2 = x 1 := h1_2.symm
                            have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                            have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                            have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                            have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                            have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                            have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                            have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                              intro v w hv hw
                              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xc] <;> omega
                            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                              intro bs hbs
                              exact constOn_eq_of_eq_on bs hbs x xc hxeq
                            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                              nodup_map_eq_of_eq_on x xc hxeq
                            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                              apply congrArg List.sum
                              apply List.map_congr_left
                              intro bs hbs
                              rw [hconst bs hbs]
                            refine hsum.trans ?_
                            let b : Bool := (fv.map xc).Nodup
                            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                            simp [hb]
                            native_decide
            · 
              by_cases h4_3 : x 4 = x 3
              · 
                by_cases h3_0 : x 3 = x 0
                · 
                  let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 0 else if v == 0 then 0 else 42
                  have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                  have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                  have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                  have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                  have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                  have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                  have h4_3_symm : x 3 = x 4 := h4_3.symm
                  have h3_0_symm : x 0 = x 3 := h3_0.symm
                  have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                    intro v w hv hw
                    have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                    have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                    rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                    simp [xc] <;> omega
                  have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                    intro bs hbs
                    exact constOn_eq_of_eq_on bs hbs x xc hxeq
                  have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                    nodup_map_eq_of_eq_on x xc hxeq
                  have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                      ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                    apply congrArg List.sum
                    apply List.map_congr_left
                    intro bs hbs
                    rw [hconst bs hbs]
                  refine hsum.trans ?_
                  let b : Bool := (fv.map xc).Nodup
                  have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                  simp [hb]
                  native_decide
                · 
                  by_cases h5_0 : x 5 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 0 else if v == 0 then 1 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h4_3_symm : x 3 = x 4 := h4_3.symm
                    have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                    have h5_0_symm : x 0 = x 5 := h5_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h1_0 : x 1 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 0 else if v == 0 then 2 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h4_3_symm : x 3 = x 4 := h4_3.symm
                      have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                      have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                      have h1_0_symm : x 0 = x 1 := h1_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h2_0 : x 2 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 0 else if v == 0 then 3 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : x 3 = x 4 := h4_3.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h2_0_symm : x 0 = x 2 := h2_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 0 else if v == 0 then 4 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : x 3 = x 4 := h4_3.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                        have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                        have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
              · 
                by_cases h5_3 : x 5 = x 3
                · 
                  by_cases h4_0 : x 4 = x 0
                  · 
                    let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 1 else if v == 0 then 0 else 42
                    have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                    have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                    have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                    have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                    have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                    have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                    have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                    have h5_3_symm : x 3 = x 5 := h5_3.symm
                    have h4_0_symm : x 0 = x 4 := h4_0.symm
                    have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                      intro v w hv hw
                      have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                      have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                      rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                      simp [xc] <;> omega
                    have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                      intro bs hbs
                      exact constOn_eq_of_eq_on bs hbs x xc hxeq
                    have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                      nodup_map_eq_of_eq_on x xc hxeq
                    have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                        ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                      apply congrArg List.sum
                      apply List.map_congr_left
                      intro bs hbs
                      rw [hconst bs hbs]
                    refine hsum.trans ?_
                    let b : Bool := (fv.map xc).Nodup
                    have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                    simp [hb]
                    native_decide
                  · 
                    by_cases h3_0 : x 3 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 1 else if v == 0 then 1 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h5_3_symm : x 3 = x 5 := h5_3.symm
                      have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                      have h3_0_symm : x 0 = x 3 := h3_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h1_0 : x 1 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 1 else if v == 0 then 2 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : x 3 = x 5 := h5_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                        have h1_0_symm : x 0 = x 1 := h1_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h2_0 : x 2 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 1 else if v == 0 then 3 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : x 3 = x 5 := h5_3.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                          have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                          have h2_0_symm : x 0 = x 2 := h2_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 1 else if v == 0 then 4 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : x 3 = x 5 := h5_3.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                          have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                          have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                · 
                  by_cases h1_3 : x 1 = x 3
                  · 
                    by_cases h4_0 : x 4 = x 0
                    · 
                      let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 2 else if v == 0 then 0 else 42
                      have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                      have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                      have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                      have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                      have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                      have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                      have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                      have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                      have h1_3_symm : x 3 = x 1 := h1_3.symm
                      have h4_0_symm : x 0 = x 4 := h4_0.symm
                      have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                        intro v w hv hw
                        have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                        have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                        rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                        simp [xc] <;> omega
                      have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                        intro bs hbs
                        exact constOn_eq_of_eq_on bs hbs x xc hxeq
                      have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                        nodup_map_eq_of_eq_on x xc hxeq
                      have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                          ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                        apply congrArg List.sum
                        apply List.map_congr_left
                        intro bs hbs
                        rw [hconst bs hbs]
                      refine hsum.trans ?_
                      let b : Bool := (fv.map xc).Nodup
                      have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                      simp [hb]
                      native_decide
                    · 
                      by_cases h5_0 : x 5 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 2 else if v == 0 then 1 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h1_3_symm : x 3 = x 1 := h1_3.symm
                        have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                        have h5_0_symm : x 0 = x 5 := h5_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h3_0 : x 3 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 2 else if v == 0 then 2 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                          have h1_3_symm : x 3 = x 1 := h1_3.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                          have h3_0_symm : x 0 = x 3 := h3_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          by_cases h2_0 : x 2 = x 0
                          · 
                            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 2 else if v == 0 then 3 else 42
                            have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                            have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                            have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                            have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                            have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                            have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                            have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                            have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                            have h1_3_symm : x 3 = x 1 := h1_3.symm
                            have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                            have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                            have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                            have h2_0_symm : x 0 = x 2 := h2_0.symm
                            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                              intro v w hv hw
                              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xc] <;> omega
                            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                              intro bs hbs
                              exact constOn_eq_of_eq_on bs hbs x xc hxeq
                            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                              nodup_map_eq_of_eq_on x xc hxeq
                            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                              apply congrArg List.sum
                              apply List.map_congr_left
                              intro bs hbs
                              rw [hconst bs hbs]
                            refine hsum.trans ?_
                            let b : Bool := (fv.map xc).Nodup
                            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                            simp [hb]
                            native_decide
                          · 
                            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 2 else if v == 0 then 4 else 42
                            have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                            have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                            have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                            have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                            have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                            have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                            have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                            have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                            have h1_3_symm : x 3 = x 1 := h1_3.symm
                            have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                            have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                            have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                            have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                              intro v w hv hw
                              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xc] <;> omega
                            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                              intro bs hbs
                              exact constOn_eq_of_eq_on bs hbs x xc hxeq
                            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                              nodup_map_eq_of_eq_on x xc hxeq
                            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                              apply congrArg List.sum
                              apply List.map_congr_left
                              intro bs hbs
                              rw [hconst bs hbs]
                            refine hsum.trans ?_
                            let b : Bool := (fv.map xc).Nodup
                            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                            simp [hb]
                            native_decide
                  · 
                    by_cases h2_3 : x 2 = x 3
                    · 
                      by_cases h4_0 : x 4 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 3 else if v == 0 then 0 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_3_symm : x 3 = x 2 := h2_3.symm
                        have h4_0_symm : x 0 = x 4 := h4_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h5_0 : x 5 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 3 else if v == 0 then 1 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                          have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                          have h2_3_symm : x 3 = x 2 := h2_3.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h5_0_symm : x 0 = x 5 := h5_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          by_cases h1_0 : x 1 = x 0
                          · 
                            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 3 else if v == 0 then 2 else 42
                            have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                            have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                            have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                            have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                            have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                            have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                            have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                            have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                            have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                            have h2_3_symm : x 3 = x 2 := h2_3.symm
                            have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                            have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                            have h1_0_symm : x 0 = x 1 := h1_0.symm
                            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                              intro v w hv hw
                              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xc] <;> omega
                            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                              intro bs hbs
                              exact constOn_eq_of_eq_on bs hbs x xc hxeq
                            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                              nodup_map_eq_of_eq_on x xc hxeq
                            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                              apply congrArg List.sum
                              apply List.map_congr_left
                              intro bs hbs
                              rw [hconst bs hbs]
                            refine hsum.trans ?_
                            let b : Bool := (fv.map xc).Nodup
                            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                            simp [hb]
                            native_decide
                          · 
                            by_cases h3_0 : x 3 = x 0
                            · 
                              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 3 else if v == 0 then 3 else 42
                              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                              have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                              have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                              have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                              have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                              have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                              have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                              have h2_3_symm : x 3 = x 2 := h2_3.symm
                              have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                              have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                              have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                              have h3_0_symm : x 0 = x 3 := h3_0.symm
                              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                                intro v w hv hw
                                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xc] <;> omega
                              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                                intro bs hbs
                                exact constOn_eq_of_eq_on bs hbs x xc hxeq
                              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                                nodup_map_eq_of_eq_on x xc hxeq
                              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                                apply congrArg List.sum
                                apply List.map_congr_left
                                intro bs hbs
                                rw [hconst bs hbs]
                              refine hsum.trans ?_
                              let b : Bool := (fv.map xc).Nodup
                              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                              simp [hb]
                              native_decide
                            · 
                              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 3 else if v == 0 then 4 else 42
                              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                              have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                              have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                              have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                              have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                              have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                              have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                              have h2_3_symm : x 3 = x 2 := h2_3.symm
                              have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                              have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                              have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                              have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                                intro v w hv hw
                                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xc] <;> omega
                              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                                intro bs hbs
                                exact constOn_eq_of_eq_on bs hbs x xc hxeq
                              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                                nodup_map_eq_of_eq_on x xc hxeq
                              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                                apply congrArg List.sum
                                apply List.map_congr_left
                                intro bs hbs
                                rw [hconst bs hbs]
                              refine hsum.trans ?_
                              let b : Bool := (fv.map xc).Nodup
                              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                              simp [hb]
                              native_decide
                    · 
                      by_cases h4_0 : x 4 = x 0
                      · 
                        let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 4 else if v == 0 then 0 else 42
                        have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                        have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                        have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                        have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                        have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                        have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                        have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                        have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                        have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                        have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                        have h4_0_symm : x 0 = x 4 := h4_0.symm
                        have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                          intro v w hv hw
                          have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                          have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                          rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                          simp [xc] <;> omega
                        have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                          intro bs hbs
                          exact constOn_eq_of_eq_on bs hbs x xc hxeq
                        have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                          nodup_map_eq_of_eq_on x xc hxeq
                        have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                            ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                          apply congrArg List.sum
                          apply List.map_congr_left
                          intro bs hbs
                          rw [hconst bs hbs]
                        refine hsum.trans ?_
                        let b : Bool := (fv.map xc).Nodup
                        have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                        simp [hb]
                        native_decide
                      · 
                        by_cases h5_0 : x 5 = x 0
                        · 
                          let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 4 else if v == 0 then 1 else 42
                          have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                          have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                          have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                          have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                          have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                          have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                          have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                          have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                          have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                          have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                          have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                          have h5_0_symm : x 0 = x 5 := h5_0.symm
                          have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                            intro v w hv hw
                            have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                            have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                            rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                            simp [xc] <;> omega
                          have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                            intro bs hbs
                            exact constOn_eq_of_eq_on bs hbs x xc hxeq
                          have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                            nodup_map_eq_of_eq_on x xc hxeq
                          have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                              ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                            apply congrArg List.sum
                            apply List.map_congr_left
                            intro bs hbs
                            rw [hconst bs hbs]
                          refine hsum.trans ?_
                          let b : Bool := (fv.map xc).Nodup
                          have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                          simp [hb]
                          native_decide
                        · 
                          by_cases h1_0 : x 1 = x 0
                          · 
                            let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 4 else if v == 0 then 2 else 42
                            have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                            have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                            have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                            have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                            have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                            have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                            have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                            have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                            have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                            have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                            have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                            have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                            have h1_0_symm : x 0 = x 1 := h1_0.symm
                            have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                              intro v w hv hw
                              have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                              have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                              rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                              simp [xc] <;> omega
                            have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                              intro bs hbs
                              exact constOn_eq_of_eq_on bs hbs x xc hxeq
                            have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                              nodup_map_eq_of_eq_on x xc hxeq
                            have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                              apply congrArg List.sum
                              apply List.map_congr_left
                              intro bs hbs
                              rw [hconst bs hbs]
                            refine hsum.trans ?_
                            let b : Bool := (fv.map xc).Nodup
                            have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                            simp [hb]
                            native_decide
                          · 
                            by_cases h2_0 : x 2 = x 0
                            · 
                              let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 4 else if v == 0 then 3 else 42
                              have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                              have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                              have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                              have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                              have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                              have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                              have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                              have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                              have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                              have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                              have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                              have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                              have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                              have h2_0_symm : x 0 = x 2 := h2_0.symm
                              have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                                intro v w hv hw
                                have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                                have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                                rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                simp [xc] <;> omega
                              have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                                intro bs hbs
                                exact constOn_eq_of_eq_on bs hbs x xc hxeq
                              have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                                nodup_map_eq_of_eq_on x xc hxeq
                              have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                  ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                                apply congrArg List.sum
                                apply List.map_congr_left
                                intro bs hbs
                                rw [hconst bs hbs]
                              refine hsum.trans ?_
                              let b : Bool := (fv.map xc).Nodup
                              have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                              simp [hb]
                              native_decide
                            · 
                              by_cases h3_0 : x 3 = x 0
                              · 
                                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 4 else if v == 0 then 4 else 42
                                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                                have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                                have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                                have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                                have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                                have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                                have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                                have h3_0_symm : x 0 = x 3 := h3_0.symm
                                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                                  intro v w hv hw
                                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xc] <;> omega
                                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                                  intro bs hbs
                                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                                  nodup_map_eq_of_eq_on x xc hxeq
                                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                                  apply congrArg List.sum
                                  apply List.map_congr_left
                                  intro bs hbs
                                  rw [hconst bs hbs]
                                refine hsum.trans ?_
                                let b : Bool := (fv.map xc).Nodup
                                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                                simp [hb]
                                native_decide
                              · 
                                let xc : Bind := fun v => if v == 4 then 0 else if v == 5 then 1 else if v == 1 then 2 else if v == 2 then 3 else if v == 3 then 4 else if v == 0 then 5 else 42
                                have h4_5_symm : ¬ x 5 = x 4 := by intro hh; exact h4_5 hh.symm
                                have h4_1_symm : ¬ x 1 = x 4 := by intro hh; exact h4_1 hh.symm
                                have h5_1_symm : ¬ x 1 = x 5 := by intro hh; exact h5_1 hh.symm
                                have h4_2_symm : ¬ x 2 = x 4 := by intro hh; exact h4_2 hh.symm
                                have h5_2_symm : ¬ x 2 = x 5 := by intro hh; exact h5_2 hh.symm
                                have h1_2_symm : ¬ x 2 = x 1 := by intro hh; exact h1_2 hh.symm
                                have h4_3_symm : ¬ x 3 = x 4 := by intro hh; exact h4_3 hh.symm
                                have h5_3_symm : ¬ x 3 = x 5 := by intro hh; exact h5_3 hh.symm
                                have h1_3_symm : ¬ x 3 = x 1 := by intro hh; exact h1_3 hh.symm
                                have h2_3_symm : ¬ x 3 = x 2 := by intro hh; exact h2_3 hh.symm
                                have h4_0_symm : ¬ x 0 = x 4 := by intro hh; exact h4_0 hh.symm
                                have h5_0_symm : ¬ x 0 = x 5 := by intro hh; exact h5_0 hh.symm
                                have h1_0_symm : ¬ x 0 = x 1 := by intro hh; exact h1_0 hh.symm
                                have h2_0_symm : ¬ x 0 = x 2 := by intro hh; exact h2_0 hh.symm
                                have h3_0_symm : ¬ x 0 = x 3 := by intro hh; exact h3_0 hh.symm
                                have hxeq : ∀ (v w : Nat), (v ∈ fv) → (w ∈ fv) → ((x v = x w) ↔ (xc v = xc w)) := by
                                  intro v w hv hw
                                  have hv' : v = 4 ∨ v = 5 ∨ v = 1 ∨ v = 2 ∨ v = 3 ∨ v = 0 := by simpa [fv, pattern] using hv
                                  have hw' : w = 4 ∨ w = 5 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 0 := by simpa [fv, pattern] using hw
                                  rcases hv' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  rcases hw' with rfl | rfl | rfl | rfl | rfl | rfl <;>
                                  simp [xc] <;> omega
                                have hconst : ∀ bs ∈ partitions fv, constOn bs x = constOn bs xc := by
                                  intro bs hbs
                                  exact constOn_eq_of_eq_on bs hbs x xc hxeq
                                have hnod : (fv.map x).Nodup = (fv.map xc).Nodup :=
                                  nodup_map_eq_of_eq_on x xc hxeq
                                have hsum : ((partitions fv).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
                                    ((partitions fv).map (fun bs => mobius bs * (if constOn bs xc then (1 : Int) else 0))).sum := by
                                  apply congrArg List.sum
                                  apply List.map_congr_left
                                  intro bs hbs
                                  rw [hconst bs hbs]
                                refine hsum.trans ?_
                                let b : Bool := (fv.map xc).Nodup
                                have hb : (fv.map x).Nodup = b := by simpa [b] using hnod
                                simp [hb]
                                native_decide

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles.CollisionCorrection.Goal := by
  intro n db b
  let V := fv
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v ∈ V, x v = y v) → x 0 = y 0 → F x = F y := by
    intro x y hxy _hx0
    dsimp only [F, V]
    simp [pattern, value]
    rw [hxy 0 (by native_decide), hxy 1 (by native_decide),
      hxy 2 (by native_decide), hxy 3 (by native_decide),
      hxy 4 (by native_decide), hxy 5 (by native_decide)]
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
          apply sum_congr
          intro x hx
          rw [mobius_point x]
          by_cases hN : (V.map x).Nodup <;> simp [hN, V]
    _ = ((bindings n V b).map (fun x => ((partitions V).map (fun bs =>
          mobius bs * (if constOn bs x then F x else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [show (fun bs => mobius bs * (if constOn bs x then F x else 0)) =
              (fun bs => (mobius bs * (if constOn bs x then (1 : Int) else 0)) * F x) by
            funext bs
            by_cases h : constOn bs x <;> simp [h]]
          rw [sum_mul_right]
          ac_rfl
    _ = ((partitions V).map (fun bs => mobius bs *
          ((bindings n V b).map (fun x => if constOn bs x then F x else 0)).sum)).sum := by
          rw [show (fun x => ((partitions V).map (fun bs =>
              mobius bs * (if constOn bs x then F x else 0))).sum) =
              (fun x => ((partitions V).map (fun bs =>
                (fun x => mobius bs * (if constOn bs x then F x else 0)) x)).sum) by rfl]
          rw [sum_swap]
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [sum_mul_left]
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

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.IntersectingCyclesCapsAndCollisionCorrection
