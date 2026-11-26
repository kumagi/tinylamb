import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `IntersectingCycles.FractionalCap` — `∀ n db b weights den, cover pattern weights den
  → (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den
  ≤ ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^
  w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_FractionalCap

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


theorem sound_IntersectingCycles_FractionalCap :
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

end TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_FractionalCap
