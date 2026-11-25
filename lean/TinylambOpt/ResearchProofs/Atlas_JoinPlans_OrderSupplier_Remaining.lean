import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplier.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier

/--! Finite-range sum helpers (Std only). -/

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

theorem rsum_eq_zero_of_zero (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → f i = 0) :
    rsum n f = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]
      simp

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

theorem filter_range_eq_single (n a : Nat) (ha : a < n) :
    (List.range n).filter (fun y => y == a) = [a] := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ]
      by_cases h : a == n
      · have han : a = n := Nat.eq_of_beq_eq_true h
        subst a
        have hnil : (List.range n).filter (fun y : Nat => y == n) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          have hylt : y < n := List.mem_range.mp hy
          cases h' : (y == n) <;> simp_all
        simp [hnil]
      · have hane : a ≠ n := by intro heq; subst n; simp at h
        have halt : a < n := by omega
        rw [List.filter_append, ih halt]
        have hna : n ≠ a := hane.symm
        simp [h, hna]

theorem rsum_filter_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    (((List.range n).filter (fun y => y == a)).map f).sum = f a := by
  rw [filter_range_eq_single n a ha]
  simp

private def ind (db : DB) (r x y : Nat) : Int := if db r x y != 0 then 1 else 0

private theorem ind_eq_indicator (db : DB) (r x y : Nat) :
    (if db r x y == 0 then (0 : Int) else 1) = ind db r x y := by
  unfold ind
  cases h : (db r x y == 0) <;> cases h' : (db r x y != 0) <;> simp_all

private theorem ind_nonneg (db : DB) (r x y : Nat) : 0 ≤ ind db r x y := by
  unfold ind
  split <;> omega

private theorem ind_le_one (db : DB) (r x y : Nat) : ind db r x y ≤ 1 := by
  unfold ind
  split <;> omega

private theorem reference_ind (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
      ind db 0 (b 0) x1 * (ind db 1 x1 x2 * (ind db 2 x2 x3 *
        (ind db 1 (b 0) x4 * ind db 2 x4 x3))))))) := by
  simp [reference, integrate, pattern, value, rsum, ind, put]

private theorem le_rsum_of_mem (n i : Nat) (f : Nat → Int) (hi : i < n)
    (hf : ∀ j, j < n → 0 ≤ f j) : f i ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases hin : i < n
      · have hrec := ih hin (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
        omega
      · have : i = n := by omega
        subst i
        have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        omega

private theorem eq_zero_of_rsum_eq_zero (n : Nat) (f : Nat → Int)
    (hf : ∀ i, i < n → 0 ≤ f i) (hs : rsum n f = 0) : ∀ i, i < n → f i = 0 := by
  induction n with
  | zero => intro i hi; omega
  | succ n ih =>
      rw [rsum_succ] at hs
      have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun i hi => hf i (Nat.lt_succ_of_lt hi))
      have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
      have hpre0 : rsum n f = 0 := by omega
      have hn0 : f n = 0 := by omega
      intro i hi
      by_cases hin : i < n
      · exact ih (fun j hj => hf j (Nat.lt_succ_of_lt hj)) hpre0 i hin
      · have : i = n := by omega
        subst i
        exact hn0

private theorem drop_factor (p q : Int) (hp : 0 ≤ p) (hq0 : 0 ≤ q) (hq1 : q ≤ 1) : p * q ≤ p := by
  have := Int.mul_le_mul_of_nonneg_left hq1 hp
  simpa using this

private theorem five_le_BE (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ b*e := by
  have hp1 : 0 ≤ a*b*d*e := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha0 hb0) hd0) he0
  have h1 : (a*b*d*e)*c ≤ a*b*d*e := drop_factor _ _ hp1 hc0 hc1
  have h1' : a*b*c*d*e ≤ a*b*d*e := by
    rw [show a*b*c*d*e = (a*b*d*e)*c by grind]
    exact h1
  have hp2 : 0 ≤ b*d*e := Int.mul_nonneg (Int.mul_nonneg hb0 hd0) he0
  have h2 : (b*d*e)*a ≤ b*d*e := drop_factor _ _ hp2 ha0 ha1
  have h2' : a*b*d*e ≤ b*d*e := by
    rw [show a*b*d*e = (b*d*e)*a by grind]
    exact h2
  have hp3 : 0 ≤ b*e := Int.mul_nonneg hb0 he0
  have h3 : (b*e)*d ≤ b*e := drop_factor _ _ hp3 hd0 hd1
  have h3' : b*d*e ≤ b*e := by
    rw [show b*d*e = (b*e)*d by grind]
    exact h3
  omega

private theorem five_le_ACD (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ a*c*d := by
  have hp1 : 0 ≤ a*c*d := Int.mul_nonneg (Int.mul_nonneg ha0 hc0) hd0
  have h1 : (a*c*d)*e ≤ a*c*d := drop_factor _ _ hp1 he0 he1
  have h1' : a*c*d*e ≤ a*c*d := by
    rw [show a*c*d*e = (a*c*d)*e by grind]
    exact h1
  have hp2 : 0 ≤ a*c*d := hp1
  have h2 : (a*c*d)*b ≤ a*c*d := drop_factor _ _ hp2 hb0 hb1
  have h2' : a*b*c*d ≤ a*c*d := by
    rw [show a*b*c*d = (a*c*d)*b by grind]
    exact h2
  have hfull1 : a*b*c*d*e ≤ a*c*d*e := by
    have := Int.mul_le_mul_of_nonneg_right h2' he0
    simpa [show a*c*d*e = (a*c*d)*e by grind] using this
  omega

private theorem five_le_ACE (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ a*c*e := by
  have hp1 : 0 ≤ a*c*e := Int.mul_nonneg (Int.mul_nonneg ha0 hc0) he0
  have h1 : (a*c*e)*d ≤ a*c*e := drop_factor _ _ hp1 hd0 hd1
  have h1' : a*c*d*e ≤ a*c*e := by
    rw [show a*c*d*e = (a*c*e)*d by grind]
    exact h1
  have h2 : (a*c*e)*b ≤ a*c*e := drop_factor _ _ hp1 hb0 hb1
  have h2' : a*b*c*e ≤ a*c*e := by
    rw [show a*b*c*e = (a*c*e)*b by grind]
    exact h2
  have habc0 : 0 ≤ a*b*c := Int.mul_nonneg (Int.mul_nonneg ha0 hb0) hc0
  have hddrop : (a*b*c)*d ≤ a*b*c := drop_factor _ _ habc0 hd0 hd1
  have habcd : a*b*c*d ≤ a*b*c := by
    rw [show a*b*c*d = (a*b*c)*d by grind]
    exact hddrop
  have hfull1 : a*b*c*d*e ≤ a*b*c*e := by
    have := Int.mul_le_mul_of_nonneg_right habcd he0
    simpa [show a*b*c*d*e = (a*b*c*d)*e by grind] using this
  omega

private theorem rsum_const_mul2 (n : Nat) (c : Int) (f : Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => c * f x y)) =
      c * rsum n (fun x => rsum n (fun y => f x y)) := by
  have h : (fun x => rsum n (fun y => c * f x y)) =
      (fun x => c * rsum n (fun y => f x y)) := by
    funext x
    rw [rsum_const_mul]
  rw [h, rsum_const_mul]

private theorem rsum_mul_const2 (n : Nat) (f : Nat → Nat → Int) (c : Int) :
    rsum n (fun x => rsum n (fun y => f x y * c)) =
      rsum n (fun x => rsum n (fun y => f x y)) * c := by
  have h : (fun x => rsum n (fun y => f x y * c)) =
      (fun x => rsum n (fun y => f x y) * c) := by
    funext x
    rw [rsum_mul_const]
  rw [h, rsum_mul_const]

private theorem rsum_const_mul3 (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => rsum n (fun z => c * f x y z))) =
      c * rsum n (fun x => rsum n (fun y => rsum n (fun z => f x y z))) := by
  have h : (fun x => rsum n (fun y => rsum n (fun z => c * f x y z))) =
      (fun x => c * rsum n (fun y => rsum n (fun z => f x y z))) := by
    funext x
    rw [rsum_const_mul2]
  rw [h, rsum_const_mul]

private theorem five_le_BCD (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b)
    (hc0 : 0 ≤ c) (hd0 : 0 ≤ d)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ b*c*d := by
  have hp : 0 ≤ b*c*d := Int.mul_nonneg (Int.mul_nonneg hb0 hc0) hd0
  have h1 : (b*c*d)*e ≤ b*c*d := drop_factor _ _ hp he0 he1
  have h2 : (b*c*d)*a ≤ b*c*d := drop_factor _ _ hp ha0 ha1
  have h1' : a*b*c*d*e ≤ (b*c*d)*e := by
    rw [show a*b*c*d*e = (b*c*d)*a*e by grind]
    have := Int.mul_le_mul_of_nonneg_right h2 he0
    simpa using this
  omega

private def A (db : DB) (b : Bind) (x : Nat) : Int := ind db 0 (b 0) x
private def Bf (db : DB) (x y : Nat) : Int := ind db 1 x y
private def Cf (db : DB) (x y : Nat) : Int := ind db 2 x y
private def Df (db : DB) (b : Bind) (x : Nat) : Int := ind db 1 (b 0) x
private def Ef (db : DB) (x y : Nat) : Int := ind db 2 x y

private def s0 (n : Nat) (db : DB) (b : Bind) : Int := rsum n (fun x => A db b x)
private def s1 (n : Nat) (db : DB) : Int := rsum n (fun x => rsum n (fun y => Bf db x y))
private def s2 (n : Nat) (db : DB) : Int := rsum n (fun x => rsum n (fun y => Cf db x y))
private def s3 (n : Nat) (db : DB) (b : Bind) : Int := rsum n (fun x => Df db b x)
private def s4 (n : Nat) (db : DB) : Int := rsum n (fun x => rsum n (fun y => Ef db x y))

private def Rfull (n : Nat) (db : DB) (b : Bind) : Int :=
  rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
    A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3)))))))

private theorem full_le_s1s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s1 n db * s4 n db := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            Bf db x1 x2 * Ef db x4 x3)))) := by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_BE (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2) (by dsimp [Bf]; exact ind_le_one db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Cf]; exact ind_le_one db 2 x2 x3)
                (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4) (by dsimp [Df]; exact ind_le_one db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind] using h
    _ = s1 n db * s4 n db := by
      dsimp [s1, s4]
      let K : Int := rsum n (fun x4 => rsum n (fun x3 => Ef db x4 x3))
      calc
        rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            Bf db x1 x2 * Ef db x4 x3))))
            = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 =>
                (rsum n (fun x3 => Ef db x4 x3)) * Bf db x1 x2))) := by
                  apply rsum_congr; intro x1 h1
                  apply rsum_congr; intro x2 h2
                  apply rsum_congr; intro x4 h4
                  rw [show (fun x3 => Bf db x1 x2 * Ef db x4 x3) =
                      (fun x3 => Ef db x4 x3 * Bf db x1 x2) by funext x3; ac_rfl]
                  rw [rsum_mul_const]
        _ = rsum n (fun x1 => rsum n (fun x2 => Bf db x1 x2 * K)) := by
                  dsimp [K]
                  apply rsum_congr; intro x1 h1
                  apply rsum_congr; intro x2 h2
                  rw [show (fun x4 => (rsum n (fun x3 => Ef db x4 x3)) * Bf db x1 x2) =
                      (fun x4 => Bf db x1 x2 * (rsum n (fun x3 => Ef db x4 x3))) by funext x4; ac_rfl]
                  rw [rsum_const_mul]
        _ = rsum n (fun x1 => (rsum n (fun x2 => Bf db x1 x2)) * K) := by
                  apply rsum_congr; intro x1 h1
                  rw [rsum_mul_const]
        _ = (rsum n (fun x1 => rsum n (fun x2 => Bf db x1 x2))) * K := by
                  rw [rsum_mul_const]

private theorem inner_swap_le (n : Nat) (db : DB) :
    rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3)))
      ≤ s2 n db * s4 n db := by
  dsimp [s2, s4]
  calc
    rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3)))
        = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => Cf db x2 x3 * Ef db x4 x3))) := by
            apply rsum_congr; intro x2 h2
            rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x4 => Cf db x2 x3 * Ef db x4 x3))) := by
            rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x2 => Cf db x2 x3) * rsum n (fun x4 => Ef db x4 x3)) := by
            apply rsum_congr; intro x3 h3
            rw [show (fun x2 => rsum n (fun x4 => Cf db x2 x3 * Ef db x4 x3)) =
                (fun x2 => rsum n (fun x4 => Ef db x4 x3 * Cf db x2 x3)) by
                  funext x2; congr 1; funext x4; ac_rfl]
            have hinner : (fun x2 => rsum n (fun x4 => Ef db x4 x3 * Cf db x2 x3)) =
                (fun x2 => (rsum n (fun x4 => Ef db x4 x3)) * Cf db x2 x3) := by
              funext x2
              rw [rsum_mul_const]
            rw [hinner, rsum_const_mul]
            grind
    _ ≤ rsum n (fun x3 => (rsum n (fun y => rsum n (fun z => Cf db y z))) *
            rsum n (fun x4 => Ef db x4 x3)) := by
            apply rsum_le; intro x3 h3
            have hcle : rsum n (fun x2 => Cf db x2 x3) ≤ rsum n (fun y => rsum n (fun z => Cf db y z)) := by
              apply rsum_le; intro x2 hx2
              exact le_rsum_of_mem n x3 (fun z => Cf db x2 z) h3
                (fun z hz => by dsimp [Cf]; exact ind_nonneg db 2 x2 z)
            have hEnon : 0 ≤ rsum n (fun x4 => Ef db x4 x3) :=
              rsum_nonneg n _ (fun x4 hx4 => by dsimp [Ef]; exact ind_nonneg db 2 x4 x3)
            exact Int.mul_le_mul_of_nonneg_right hcle hEnon
    _ = (rsum n (fun y => rsum n (fun z => Cf db y z))) *
          rsum n (fun x3 => rsum n (fun x4 => Ef db x4 x3)) := by
            rw [rsum_const_mul]
    _ = (rsum n (fun y => rsum n (fun z => Cf db y z))) *
          rsum n (fun x4 => rsum n (fun x3 => Ef db x4 x3)) := by
            congr 1
            rw [rsum_comm]

private theorem full_le_s0s2s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s0 n db b * s2 n db * s4 n db := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            A db b x1 * (Cf db x2 x3 * Ef db x4 x3)))) ):= by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_ACE (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2) (by dsimp [Bf]; exact ind_le_one db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Cf]; exact ind_le_one db 2 x2 x3)
                (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4) (by dsimp [Df]; exact ind_le_one db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind,
                  show A db b x1 * (Cf db x2 x3 * Ef db x4 x3) = A db b x1 * Cf db x2 x3 * Ef db x4 x3 by grind] using h
    _ = rsum n (fun x1 => A db b x1 *
          rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3)))) := by
            apply rsum_congr; intro x1 h1
            have h : (fun x2 => rsum n (fun x4 => rsum n (fun x3 => A db b x1 * (Cf db x2 x3 * Ef db x4 x3)))) =
                (fun x2 => A db b x1 * rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3))) := by
              funext x2
              rw [rsum_const_mul2]
            rw [h, rsum_const_mul]
    _ ≤ rsum n (fun x1 => A db b x1 * (s2 n db * s4 n db)) := by
            apply rsum_le; intro x1 h1
            have hA : 0 ≤ A db b x1 := by dsimp [A]; exact ind_nonneg db 0 (b 0) x1
            exact Int.mul_le_mul_of_nonneg_left (inner_swap_le n db) hA
    _ = s0 n db b * s2 n db * s4 n db := by
            dsimp [s0]
            rw [show (fun x => A db b x * (s2 n db * s4 n db)) =
                (fun x => A db b x * (s2 n db * s4 n db)) by rfl]
            rw [rsum_mul_const]
            grind

private theorem full_le_s0s2s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s0 n db b * s2 n db * s3 n db b := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            A db b x1 * (Cf db x2 x3 * Df db b x4)))) ):= by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_ACD (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2) (by dsimp [Bf]; exact ind_le_one db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Cf]; exact ind_le_one db 2 x2 x3)
                (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4) (by dsimp [Df]; exact ind_le_one db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind,
                  show A db b x1 * (Cf db x2 x3 * Df db b x4) = A db b x1 * Cf db x2 x3 * Df db b x4 by grind] using h
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            A db b x1 * (Cf db x2 x3 * s3 n db b))) ):= by
            apply rsum_congr; intro x1 h1
            apply rsum_congr; intro x2 h2
            rw [rsum_comm]
            apply rsum_congr; intro x3 h3
            have hreshape : (fun i => A db b x1 * (Cf db x2 x3 * Df db b i)) =
                (fun i => (A db b x1 * Cf db x2 x3) * Df db b i) := by
              funext i
              grind
            rw [hreshape, rsum_const_mul]
            dsimp [s3]
            grind
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            A db b x1 * (rsum n (fun x3 => Cf db x2 x3) * s3 n db b)) ):= by
            apply rsum_congr; intro x1 h1
            apply rsum_congr; intro x2 h2
            rw [rsum_const_mul]
            congr 1
            rw [rsum_mul_const]
    _ = rsum n (fun x1 => A db b x1 * (s2 n db * s3 n db b)) := by
            apply rsum_congr; intro x1 h1
            rw [rsum_const_mul]
            congr 1
            dsimp [s2]
            rw [rsum_mul_const]
    _ = s0 n db b * s2 n db * s3 n db b := by
            dsimp [s0]
            rw [rsum_mul_const]
            grind

private theorem full_le_s1s2s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s1 n db * s2 n db * s3 n db b := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            Bf db x1 x2 * (Cf db x2 x3 * Df db b x4))))) := by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_BCD (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind,
                  show Bf db x1 x2 * (Cf db x2 x3 * Df db b x4) = Bf db x1 x2 * Cf db x2 x3 * Df db b x4 by grind] using h
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            Bf db x1 x2 * (rsum n (fun x3 => Cf db x2 x3) * s3 n db b)) ):= by
            apply rsum_congr; intro x1 h1
            apply rsum_congr; intro x2 h2
            rw [rsum_comm]
            have hreshape2 : (fun j => rsum n (fun i => Bf db x1 x2 * (Cf db x2 j * Df db b i))) =
                (fun j => Bf db x1 x2 * rsum n (fun i => Cf db x2 j * Df db b i)) := by
              funext j
              rw [rsum_const_mul]
            rw [hreshape2, rsum_const_mul]
            have h3reshape : (fun j => rsum n (fun i => Cf db x2 j * Df db b i)) =
                (fun j => Cf db x2 j * s3 n db b) := by
              funext j
              rw [rsum_const_mul]
              simp [s3]
            rw [h3reshape, rsum_mul_const]
    _ ≤ rsum n (fun x1 => rsum n (fun x2 =>
            Bf db x1 x2 * (s2 n db * s3 n db b)) ):= by
            apply rsum_le; intro x1 h1
            apply rsum_le; intro x2 h2
            have hB : 0 ≤ Bf db x1 x2 := by dsimp [Bf]; exact ind_nonneg db 1 x1 x2
            have hs3 : 0 ≤ s3 n db b := rsum_nonneg n _ (fun x h => by dsimp [Df]; exact ind_nonneg db 1 (b 0) x)
            have hs2x : rsum n (fun x3 => Cf db x2 x3) ≤ s2 n db := by
              dsimp [s2]
              rw [show rsum n (fun y => rsum n (fun z => Cf db y z)) =
                  rsum n (fun z => rsum n (fun y => Cf db y z)) by rw [rsum_comm]]
              apply rsum_le; intro z hz
              exact le_rsum_of_mem n x2 (fun y => Cf db y z) h2
                (fun y hy => by dsimp [Cf]; exact ind_nonneg db 2 y z)
            exact Int.mul_le_mul_of_nonneg_left (Int.mul_le_mul_of_nonneg_right hs2x hs3) hB
    _ = s1 n db * s2 n db * s3 n db b := by
            dsimp [s1]
            have h : (fun x1 => rsum n (fun x2 => Bf db x1 x2 * (s2 n db * s3 n db b))) =
                (fun x1 => (rsum n (fun x2 => Bf db x1 x2)) * (s2 n db * s3 n db b)) := by
              funext x1
              rw [rsum_mul_const]
            rw [h, rsum_mul_const]
            grind

private theorem int_countP_eq_rsum (n : Nat) (p : Nat → Bool) :
    ((List.range n).countP p : Int) = rsum n (fun x => if p x then (1 : Int) else 0) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      have hcast : ((List.countP p (List.range n) + List.countP p [n] : Nat) : Int) =
          (List.countP p (List.range n) : Int) + (List.countP p [n] : Int) := by omega
      rw [hcast, ih]
      rw [rsum_succ]
      cases h : p n <;> simp [h]

private theorem int_list_sum_countP (l : List Nat) (n : Nat) (f : Nat → Nat → Bool) :
    ((l.map (fun x => (List.range n).countP (f x))).sum : Int) =
      (l.map (fun x => rsum n (fun y => if f x y then (1 : Int) else 0))).sum := by
  induction l with
  | nil => simp
  | cons x l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hcast : ((List.countP (f x) (List.range n) +
          (List.map (fun x => List.countP (f x) (List.range n)) l).sum : Nat) : Int) =
          (List.countP (f x) (List.range n) : Int) +
          ((List.map (fun x => List.countP (f x) (List.range n)) l).sum : Int) := by omega
      rw [hcast, int_countP_eq_rsum, ih]

private theorem support0_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨0, 0, 1⟩ : Atom) pattern b : Int) = s0 n db b := by
  simp [support, pattern, s0, A, ind, int_countP_eq_rsum]

private theorem support1_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨1, 1, 2⟩ : Atom) pattern b : Int) = s1 n db := by
  simp [support, pattern, s1, Bf]
  rw [int_list_sum_countP]
  rfl

private theorem support2_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨2, 2, 3⟩ : Atom) pattern b : Int) = s2 n db := by
  simp [support, pattern, s2, Cf]
  rw [int_list_sum_countP]
  rfl

private theorem support3_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨1, 0, 4⟩ : Atom) pattern b : Int) = s3 n db b := by
  simp [support, pattern, s3, Df, ind, int_countP_eq_rsum]

private theorem support4_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨2, 4, 3⟩ : Atom) pattern b : Int) = s4 n db := by
  simp [support, pattern, s4, Ef]
  rw [int_list_sum_countP]
  rfl

private theorem prod5_nonneg (a b c d e : Int) (ha : 0 ≤ a) (hb : 0 ≤ b) (hc : 0 ≤ c)
    (hd : 0 ≤ d) (he : 0 ≤ e) : 0 ≤ a*b*c*d*e :=
  Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha hb) hc) hd) he

private theorem Rfull_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ Rfull n db b := by
  dsimp [Rfull]
  apply rsum_nonneg; intro x1 hx1
  apply rsum_nonneg; intro x2 hx2
  apply rsum_nonneg; intro x4 hx4
  apply rsum_nonneg; intro x3 hx3
  have h := prod5_nonneg (ind db 0 (b 0) x1) (ind db 1 x1 x2) (ind db 2 x2 x3)
    (ind db 1 (b 0) x4) (ind db 2 x4 x3)
    (ind_nonneg db 0 (b 0) x1)
    (ind_nonneg db 1 x1 x2)
    (ind_nonneg db 2 x2 x3)
    (ind_nonneg db 1 (b 0) x4)
    (ind_nonneg db 2 x4 x3)
  simpa [A, Bf, Cf, Df, Ef, show ind db 0 (b 0) x1 * ind db 1 x1 x2 * ind db 2 x2 x3 *
      ind db 1 (b 0) x4 * ind db 2 x4 x3 =
      ind db 0 (b 0) x1 * (ind db 1 x1 x2 * (ind db 2 x2 x3 *
        (ind db 1 (b 0) x4 * ind db 2 x4 x3))) by grind] using h

private theorem reference_eq_Rfull (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b = Rfull n db b := by
  rw [reference_ind]
  simp [Rfull, A, Bf, Cf, Df, Ef, ind, ind_eq_indicator]

private theorem pow_mono_one (a : Int) (h1 : 1 ≤ a) : ∀ (m n : Nat), m ≤ n → a^m ≤ a^n
  | m, 0, hmn => by
      have : m = 0 := by omega
      subst m
      simp
  | m, n+1, hmn => by
      by_cases hm : m ≤ n
      · have ih := pow_mono_one a h1 m n hm
        have hnon : 0 ≤ a ^ n := Int.pow_nonneg (by omega)
        have hstep : a^n ≤ a^n * a := by
          have := Int.mul_le_mul_of_nonneg_left h1 hnon
          simpa using this
        rw [Int.pow_succ]
        omega
      · have : m = n+1 := by omega
        subst m
        simp

private theorem Rfull_zero_of_s1 (n : Nat) (db : DB) (b : Bind) (h : s1 n db = 0) :
    Rfull n db b = 0 := by
  have hB : ∀ x y, x < n → y < n → Bf db x y = 0 := by
    intro x y hx hy
    have houter : ∀ i, i < n → 0 ≤ rsum n (fun j => Bf db i j) := by
      intro i hi
      exact rsum_nonneg n _ (fun j hj => ind_nonneg db 1 i j)
    have hz : rsum n (fun i => rsum n (fun j => Bf db i j)) = 0 := by simpa [s1] using h
    have hrow : rsum n (fun y => Bf db x y) = 0 :=
      eq_zero_of_rsum_eq_zero n (fun i => rsum n (fun j => Bf db i j)) houter hz x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Bf db x y)
      (fun j hj => ind_nonneg db 1 x j) hrow y hy
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hB x1 x2 hx1 hx2]

private theorem Rfull_zero_of_s2 (n : Nat) (db : DB) (b : Bind) (h : s2 n db = 0) :
    Rfull n db b = 0 := by
  have hC : ∀ x y, x < n → y < n → Cf db x y = 0 := by
    intro x y hx hy
    have houter : ∀ i, i < n → 0 ≤ rsum n (fun j => Cf db i j) := by
      intro i hi
      exact rsum_nonneg n _ (fun j hj => ind_nonneg db 2 i j)
    have hz : rsum n (fun i => rsum n (fun j => Cf db i j)) = 0 := by simpa [s2] using h
    have hrow : rsum n (fun y => Cf db x y) = 0 :=
      eq_zero_of_rsum_eq_zero n (fun i => rsum n (fun j => Cf db i j)) houter hz x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Cf db x y)
      (fun j hj => ind_nonneg db 2 x j) hrow y hy
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hC x2 x3 hx2 hx3]

private theorem Rfull_zero_of_s4 (n : Nat) (db : DB) (b : Bind) (h : s4 n db = 0) :
    Rfull n db b = 0 := by
  have hE : ∀ x y, x < n → y < n → Ef db x y = 0 := by
    intro x y hx hy
    have houter : ∀ i, i < n → 0 ≤ rsum n (fun j => Ef db i j) := by
      intro i hi
      exact rsum_nonneg n _ (fun j hj => ind_nonneg db 2 i j)
    have hz : rsum n (fun i => rsum n (fun j => Ef db i j)) = 0 := by simpa [s4] using h
    have hrow : rsum n (fun y => Ef db x y) = 0 :=
      eq_zero_of_rsum_eq_zero n (fun i => rsum n (fun j => Ef db i j)) houter hz x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Ef db x y)
      (fun j hj => ind_nonneg db 2 x j) hrow y hy
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hE x4 x3 hx4 hx3]

private theorem Rfull_zero_of_s0 (n : Nat) (db : DB) (b : Bind) (h : s0 n db b = 0) :
    Rfull n db b = 0 := by
  have hA : ∀ x, x < n → A db b x = 0 := by
    intro x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => A db b y)
      (fun j hj => ind_nonneg db 0 (b 0) j) (by simpa [s0] using h) x hx
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hA x1 hx1]

private theorem Rfull_zero_of_s3 (n : Nat) (db : DB) (b : Bind) (h : s3 n db b = 0) :
    Rfull n db b = 0 := by
  have hD : ∀ x, x < n → Df db b x = 0 := by
    intro x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Df db b y)
      (fun j hj => ind_nonneg db 1 (b 0) j) (by simpa [s3] using h) x hx
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hD x4 hx4]

private theorem pow_le_pow_of_le {a c : Int} (h : a ≤ c) (ha : 0 ≤ a) :
    ∀ k : Nat, a^k ≤ c^k
  | 0 => by simp
  | k+1 => by
      rw [Int.pow_succ, Int.pow_succ]
      have hc : 0 ≤ c := by omega
      have hak : 0 ≤ a^k := Int.pow_nonneg ha
      exact Int.mul_le_mul (pow_le_pow_of_le h ha k) h ha (Int.pow_nonneg hc)

private theorem mul5_le {a b c d e A B C D E : Int}
    (ha : a ≤ A) (hb : b ≤ B) (hc : c ≤ C) (hd : d ≤ D) (he : e ≤ E)
    (ha0 : 0 ≤ a) (hb0 : 0 ≤ b) (hc0 : 0 ≤ c) (hd0 : 0 ≤ d) (he0 : 0 ≤ e) :
    a*b*c*d*e ≤ A*B*C*D*E := by
  have hA0 : 0 ≤ A := by omega
  have hB0 : 0 ≤ B := by omega
  have hC0 : 0 ≤ C := by omega
  have hD0 : 0 ≤ D := by omega
  have h12 : a*b ≤ A*B := Int.mul_le_mul ha hb hb0 hA0
  have hAB0 : 0 ≤ A*B := Int.mul_nonneg hA0 hB0
  have h123 : a*b*c ≤ A*B*C := Int.mul_le_mul h12 hc hc0 hAB0
  have hABC0 : 0 ≤ A*B*C := Int.mul_nonneg hAB0 hC0
  have h1234 : a*b*c*d ≤ A*B*C*D := Int.mul_le_mul h123 hd hd0 hABC0
  have hABCD0 : 0 ≤ A*B*C*D := Int.mul_nonneg hABC0 hD0
  exact Int.mul_le_mul h1234 he he0 hABCD0

private theorem s0_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ s0 n db b :=
  rsum_nonneg n _ (fun x hx => ind_nonneg db 0 (b 0) x)
private theorem s1_nonneg (n : Nat) (db : DB) : 0 ≤ s1 n db :=
  rsum_nonneg n _ (fun x hx => rsum_nonneg n _ (fun y hy => ind_nonneg db 1 x y))
private theorem s2_nonneg (n : Nat) (db : DB) : 0 ≤ s2 n db :=
  rsum_nonneg n _ (fun x hx => rsum_nonneg n _ (fun y hy => ind_nonneg db 2 x y))
private theorem s3_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ s3 n db b :=
  rsum_nonneg n _ (fun x hx => ind_nonneg db 1 (b 0) x)
private theorem s4_nonneg (n : Nat) (db : DB) : 0 ≤ s4 n db :=
  rsum_nonneg n _ (fun x hx => rsum_nonneg n _ (fun y hy => ind_nonneg db 2 x y))

theorem sound_FractionalCap : FractionalCap.Goal := by
  intro n db b weights den hcov
  have hlen : weights.length = 5 := by
    simpa [cover, pattern] using hcov.2.1
  rcases weights with _ | ⟨w0, _ | ⟨w1, _ | ⟨w2, _ | ⟨w3, _ | ⟨w4, rest⟩⟩⟩⟩⟩
  · simp at hlen
  · simp at hlen
  · simp at hlen
  · simp at hlen
  · simp at hlen
  · have hrest : rest = [] := by simpa using hlen
    subst rest
    unfold cover at hcov
    have hdenpos : 0 < den := hcov.1
    have hc1 : den ≤ w0 + w1 := by
      have := hcov.2.2 1 (by simp [pattern])
      simpa [pattern] using this
    have hc2 : den ≤ w1 + w2 := by
      have := hcov.2.2 2 (by simp [pattern])
      simpa [pattern] using this
    have hc3 : den ≤ w3 + w4 := by
      have := hcov.2.2 4 (by simp [pattern])
      simpa [pattern] using this
    have hc4 : den ≤ w2 + w4 := by
      have := hcov.2.2 3 (by simp [pattern])
      simpa [pattern] using this
    by_cases hz : s0 n db b = 0 ∨ s1 n db = 0 ∨ s2 n db = 0 ∨
        s3 n db b = 0 ∨ s4 n db = 0
    · have hR : Rfull n db b = 0 := by
        rcases hz with h | h | h | h | h
        · exact Rfull_zero_of_s0 n db b h
        · exact Rfull_zero_of_s1 n db b h
        · exact Rfull_zero_of_s2 n db b h
        · exact Rfull_zero_of_s3 n db b h
        · exact Rfull_zero_of_s4 n db b h
      have href0 : reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b = 0 := by
        rw [reference_eq_Rfull, hR]
      have href : reference pattern n (fun r x y => if db r x y = 0 then (0 : Int) else 1) b = 0 := by
        simpa using href0
      have hzeroPow : (0 : Nat)^den = 0 := by
        cases den with
        | zero => omega
        | succ k => simp
      have hrefb : reference pattern n (fun r x y => if (db r x y == 0) = true then 0 else 1) b = 0 := by
        simpa using href0
      rw [hrefb]
      simp [hzeroPow]
    · have hs0 : 1 ≤ s0 n db b := by
        have := s0_nonneg n db b
        omega
      have hs1 : 1 ≤ s1 n db := by
        have := s1_nonneg n db
        omega
      have hs2 : 1 ≤ s2 n db := by
        have := s2_nonneg n db
        omega
      have hs3 : 1 ≤ s3 n db b := by
        have := s3_nonneg n db b
        omega
      have hs4 : 1 ≤ s4 n db := by
        have := s4_nonneg n db
        omega
      let p := min w1 den
      let q := min w4 den
      have hpden : p ≤ den := Nat.min_le_right _ _
      have hqden : q ≤ den := Nat.min_le_right _ _
      have hw0' : den - p ≤ w0 := by
        dsimp [p]
        by_cases h : w1 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w1 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hw1' : p ≤ w1 := Nat.min_le_left _ _
      have hdp2 : den - p ≤ w2 := by
        dsimp [p]
        by_cases h : w1 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w1 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hdq2 : den - q ≤ w2 := by
        dsimp [q]
        by_cases h : w4 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w4 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hw2' : den - min p q ≤ w2 := by
        by_cases hpq : p ≤ q
        · rw [Nat.min_eq_left hpq]
          exact hdp2
        · have hpq' : q ≤ p := by omega
          rw [Nat.min_eq_right hpq']
          exact hdq2
      have hw3' : den - q ≤ w3 := by
        dsimp [q]
        by_cases h : w4 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w4 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hw4' : q ≤ w4 := Nat.min_le_left _ _
      have hmain : (Rfull n db b) ^ den ≤
          (s0 n db b) ^ w0 * (s1 n db) ^ w1 * (s2 n db) ^ w2 *
            (s3 n db b) ^ w3 * (s4 n db) ^ w4 := by
        by_cases hpq : p ≤ q
        · have hdecomp : den = (den - q) + (q - p) + p := by omega
          have h1 := pow_le_pow_of_le (full_le_s0s2s3 n db b) (Rfull_nonneg n db b) (den - q)
          have h2 := pow_le_pow_of_le (full_le_s0s2s4 n db b) (Rfull_nonneg n db b) (q - p)
          have h3 := pow_le_pow_of_le (full_le_s1s4 n db b) (Rfull_nonneg n db b) p
          have hprod : (Rfull n db b)^(den-q) * (Rfull n db b)^(q-p) * (Rfull n db b)^p ≤
              ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) *
                  ((s1 n db)*(s4 n db))^p := by
            have hR0 : 0 ≤ Rfull n db b := Rfull_nonneg n db b
            have hP1 : 0 ≤ (s0 n db b)*(s2 n db)*(s3 n db b) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP2 : 0 ≤ (s0 n db b)*(s2 n db)*(s4 n db) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP3 : 0 ≤ (s1 n db)*(s4 n db) := Int.mul_nonneg (by omega) (by omega)
            have hbc := Int.mul_le_mul h1 h2 (Int.pow_nonneg hR0) (Int.pow_nonneg hP1)
            have hcd0 : 0 ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) :=
              Int.mul_nonneg (Int.pow_nonneg hP1) (Int.pow_nonneg hP2)
            exact Int.mul_le_mul hbc h3 (Int.pow_nonneg hR0) hcd0
          have hexpand : ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) *
                  ((s1 n db)*(s4 n db))^p =
              (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-p) *
                (s3 n db b)^(den-q) * (s4 n db)^q := by
            repeat rw [Int.mul_pow]
            have he0 : (den - q) + (q - p) = den - p := by omega
            have he4 : (q - p) + p = q := by omega
            have hrearr : (s0 n db b)^(den-q) * (s2 n db)^(den-q) * (s3 n db b)^(den-q) *
                 ((s0 n db b)^(q-p) * (s2 n db)^(q-p) * (s4 n db)^(q-p)) *
                   ((s1 n db)^p * (s4 n db)^p) =
               ((s0 n db b)^(den-q) * (s0 n db b)^(q-p)) *
                 ((s2 n db)^(den-q) * (s2 n db)^(q-p)) * (s3 n db b)^(den-q) *
                   ((s4 n db)^(q-p) * (s4 n db)^p) * (s1 n db)^p := by grind
            rw [hrearr]
            rw [← Int.pow_add (s0 n db b) (den-q) (q-p)]
            rw [← Int.pow_add (s2 n db) (den-q) (q-p)]
            rw [← Int.pow_add (s4 n db) (q-p) p]
            rw [he0, he4]
            grind
          have hm0 := pow_mono_one (s0 n db b) hs0 (den-p) w0 hw0'
          have hm1 := pow_mono_one (s1 n db) hs1 p w1 hw1'
          have hm2 := pow_mono_one (s2 n db) hs2 (den-p) w2 (by omega)
          have hm3 := pow_mono_one (s3 n db b) hs3 (den-q) w3 hw3'
          have hm4 := pow_mono_one (s4 n db) hs4 q w4 hw4'
          have hmono : (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-p) *
                (s3 n db b)^(den-q) * (s4 n db)^q ≤
              (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                (s3 n db b)^w3 * (s4 n db)^w4 := by
            apply mul5_le hm0 hm1 hm2 hm3 hm4
            all_goals apply Int.pow_nonneg; omega
          calc
            (Rfull n db b)^den = (Rfull n db b)^((den-q)+(q-p)+p) := by
              rw [← hdecomp]
            _ = (Rfull n db b)^(den-q) * (Rfull n db b)^(q-p) * (Rfull n db b)^p := by
                  rw [Int.pow_add, Int.pow_add]
            _ ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                  ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) *
                    ((s1 n db)*(s4 n db))^p := hprod
            _ = (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-p) *
                  (s3 n db b)^(den-q) * (s4 n db)^q := hexpand
            _ ≤ (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                  (s3 n db b)^w3 * (s4 n db)^w4 := hmono
        · have hpq' : q ≤ p := by omega
          have hdecomp : den = (den - p) + (p - q) + q := by omega
          have h1 := pow_le_pow_of_le (full_le_s0s2s3 n db b) (Rfull_nonneg n db b) (den - p)
          have h2 := pow_le_pow_of_le (full_le_s1s2s3 n db b) (Rfull_nonneg n db b) (p - q)
          have h3 := pow_le_pow_of_le (full_le_s1s4 n db b) (Rfull_nonneg n db b) q
          have hprod : (Rfull n db b)^(den-p) * (Rfull n db b)^(p-q) * (Rfull n db b)^q ≤
              ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) *
                  ((s1 n db)*(s4 n db))^q := by
            have hR0 : 0 ≤ Rfull n db b := Rfull_nonneg n db b
            have hP1 : 0 ≤ (s0 n db b)*(s2 n db)*(s3 n db b) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP2 : 0 ≤ (s1 n db)*(s2 n db)*(s3 n db b) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP3 : 0 ≤ (s1 n db)*(s4 n db) := Int.mul_nonneg (by omega) (by omega)
            have hbc := Int.mul_le_mul h1 h2 (Int.pow_nonneg hR0) (Int.pow_nonneg hP1)
            have hcd0 : 0 ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) :=
              Int.mul_nonneg (Int.pow_nonneg hP1) (Int.pow_nonneg hP2)
            exact Int.mul_le_mul hbc h3 (Int.pow_nonneg hR0) hcd0
          have hexpand : ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) *
                  ((s1 n db)*(s4 n db))^q =
              (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-q) *
                (s3 n db b)^(den-q) * (s4 n db)^q := by
            repeat rw [Int.mul_pow]
            have he1 : (p - q) + q = p := by omega
            have he2 : (den - p) + (p - q) = den - q := by omega
            have hrearr : (s0 n db b)^(den-p) * (s2 n db)^(den-p) * (s3 n db b)^(den-p) *
                 ((s1 n db)^(p-q) * (s2 n db)^(p-q) * (s3 n db b)^(p-q)) *
                   ((s1 n db)^q * (s4 n db)^q) =
               (s0 n db b)^(den-p) *
                 ((s1 n db)^(p-q) * (s1 n db)^q) *
                   ((s2 n db)^(den-p) * (s2 n db)^(p-q)) *
                     ((s3 n db b)^(den-p) * (s3 n db b)^(p-q)) * (s4 n db)^q := by grind
            rw [hrearr]
            rw [← Int.pow_add (s1 n db) (p-q) q]
            rw [← Int.pow_add (s2 n db) (den-p) (p-q)]
            rw [← Int.pow_add (s3 n db b) (den-p) (p-q)]
            rw [he1, he2]
          have hm0 := pow_mono_one (s0 n db b) hs0 (den-p) w0 hw0'
          have hm1 := pow_mono_one (s1 n db) hs1 p w1 hw1'
          have hm2 := pow_mono_one (s2 n db) hs2 (den-q) w2 (by omega)
          have hm3 := pow_mono_one (s3 n db b) hs3 (den-q) w3 hw3'
          have hm4 := pow_mono_one (s4 n db) hs4 q w4 hw4'
          have hmono : (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-q) *
                (s3 n db b)^(den-q) * (s4 n db)^q ≤
              (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                (s3 n db b)^w3 * (s4 n db)^w4 := by
            apply mul5_le hm0 hm1 hm2 hm3 hm4
            all_goals apply Int.pow_nonneg; omega
          calc
            (Rfull n db b)^den = (Rfull n db b)^((den-p)+(p-q)+q) := by
              rw [← hdecomp]
            _ = (Rfull n db b)^(den-p) * (Rfull n db b)^(p-q) * (Rfull n db b)^q := by
                  rw [Int.pow_add, Int.pow_add]
            _ ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                  ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) *
                    ((s1 n db)*(s4 n db))^q := hprod
            _ = (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-q) *
                  (s3 n db b)^(den-q) * (s4 n db)^q := hexpand
            _ ≤ (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                  (s3 n db b)^w3 * (s4 n db)^w4 := hmono
      apply Int.ofNat_le.mp
      rw [show (((reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den : Nat) : Int) =
          ((reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat : Int) ^ den by simp]
      rw [Int.toNat_of_nonneg (by rw [reference_eq_Rfull]; exact Rfull_nonneg n db b)]
      rw [reference_eq_Rfull]
      simp [pattern]
      have h0 := support0_eq n db b
      have h1 := support1_eq n db b
      have h2 := support2_eq n db b
      have h3 := support3_eq n db b
      have h4 := support4_eq n db b
      simp only [pattern] at h0 h1 h2 h3 h4 ⊢
      rw [h0, h1, h2, h3, h4]
      simpa only [Int.mul_assoc] using hmain


end TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplier.Remaining
