import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.PostDiamond.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PostDiamond

/-! Small finite-sum and integration helpers. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero (n : Nat) : rsum n (fun _ => (0 : Int)) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih => rw [rsum_succ, ih]; simp

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]

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

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_le_total (n d : Nat) (f : Nat → Int) (hd : d < n)
    (hf : ∀ i, i < n → 0 ≤ f i) : f d ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases hdn : d < n
      · have hi := ih hdn (fun i hi => hf i (Nat.lt_succ_of_lt hi))
        have hn := hf n (Nat.lt_succ_self n)
        omega
      · have hdeq : d = n := by omega
        subst d
        have hi := rsum_nonneg n f (fun i hi => hf i (Nat.lt_succ_of_lt hi))
        omega

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        simp [rsum]]
      exact (rsum_zero m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem ind_nonneg (z : Int) : 0 ≤ (if z = 0 then 0 else 1 : Int) := by
  split <;> omega

private theorem ind_le_one (z : Int) : (if z = 0 then 0 else 1 : Int) ≤ 1 := by
  split <;> omega

private theorem countP_row_eq_rsum (n : Nat) (db : DB) (x : Nat) (e : Nat → Nat → Int)
    (he : ∀ y, e x y = (if db 0 x y = 0 then 0 else 1)) :
    (List.countP (fun y => db 0 x y != 0) (List.range n) : Int) = rsum n (fun y => e x y) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [show n + 1 = Nat.succ n by omega, rsum_succ]
      change ↑(List.countP (fun y => db 0 x y != 0) (List.range (Nat.succ n))) = rsum n (fun y => e x y) + e x n
      rw [List.range_succ, List.countP_append, List.countP_singleton]
      rw [Int.natCast_add, ih]
      have hxy : e x n = (if db 0 x n = 0 then 0 else 1) := he n
      rw [hxy]
      by_cases h : db 0 x n = 0
      · simp [h]
      · have hb : (db 0 x n != 0) = true := by
          by_cases h' : db 0 x n != 0 <;> simp_all
        simp [h, hb]

private theorem sum_countP_eq_rsum (n m : Nat) (db : DB) (e : Nat → Nat → Int)
    (he : ∀ x y, e x y = (if db 0 x y = 0 then 0 else 1)) :
    ((List.map (fun x => List.countP (fun y => db 0 x y != 0) (List.range m)) (List.range n)).sum : Int) =
      rsum n (fun x => rsum m (fun y => e x y)) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, List.map_singleton, List.sum_singleton]
      rw [Int.natCast_add, rsum_succ, ih]
      rw [countP_row_eq_rsum m db n e (fun y => he n y)]

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

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

private theorem rsum_ite_eq (n a : Nat) (f : Nat → Int) (ha : a < n) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases han : a < n
      · rw [ih han]
        have hne : n ≠ a := by omega
        simp [hne]
      · have haeq : a = n := by omega
        subst a
        rw [show rsum n (fun x => if x = n then f x else 0) =
            rsum n (fun _ => (0 : Int)) by
          apply rsum_congr
          intro i hi
          have hne : i ≠ n := by omega
          simp [hne]]
        rw [rsum_zero]
        simp

private theorem integrate_cons (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = rsum n (fun x => integrate n vs (put b v x) f) := by
  rfl

private theorem integrate_nil (n : Nat) (b : Bind) (f : Bind → Int) :
    integrate n [] b f = f b := by
  rfl

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      rw [integrate_cons, integrate_cons]
      apply rsum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      rw [integrate_cons, integrate_cons, integrate_cons]
      rw [← rsum_add]
      apply rsum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_mul_const (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      rw [integrate_cons, integrate_cons]
      rw [← rsum_const_mul]
      apply rsum_congr
      intro x hx
      exact ih (put b v x)

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_other (b : Bind) (v w x : Nat) (h : w ≠ v) : put b v x w = b w := by
  simp [put, h]

theorem sound_FractionalCap : FractionalCap.Goal := by
  intro n db b weights den hcov
  have hwlen : weights.length = 4 := by simpa [cover, pattern] using hcov.2.1
  obtain ⟨w1, w2, w3, w4, hw⟩ : ∃ w1 w2 w3 w4, weights = [w1, w2, w3, w4] := by
    cases weights with
    | nil => simp at hwlen
    | cons a as =>
      cases as with
      | nil => simp at hwlen
      | cons b bs =>
        cases bs with
        | nil => simp at hwlen
        | cons c cs =>
          cases cs with
          | nil => simp at hwlen
          | cons d ds =>
            cases ds with
            | nil => exact ⟨a, b, c, d, rfl⟩
            | cons e es => simp at hwlen
  subst weights
  simp [cover, pattern] at hcov
  simp [support, pattern, reference, integrate, value, put] at *
  let e : Nat → Nat → Int := fun x y => if db 0 x y = 0 then 0 else 1
  let A : Nat := List.countP (fun y => db 0 (b 0) y != 0) (List.range n)
  let B : Nat := (List.map (fun x => List.countP (fun y => db 0 x y != 0) (List.range n)) (List.range n)).sum
  let S : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d =>
    e (b 0) a * (e (b 0) c * (e a d * e c d)))))
  change S.toNat ^ den ≤ A ^ w1 * (A ^ w2 * (B ^ w3 * B ^ w4))
  have he : ∀ x y, e x y = (if db 0 x y = 0 then 0 else 1) := by intro x y; rfl
  have hAeq : (A : Int) = rsum n (fun a => e (b 0) a) := by
    dsimp [A]
    exact countP_row_eq_rsum n db (b 0) e (fun y => he (b 0) y)
  have hBeq : (B : Int) = rsum n (fun x => rsum n (fun y => e x y)) := by
    dsimp [B]
    exact sum_countP_eq_rsum n n db e he
  have hSle0 : S ≤ rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      e (b 0) a * e c d))) := by
    dsimp [S]
    apply rsum_le
    intro a ha
    apply rsum_le
    intro c hc
    apply rsum_le
    intro d hd
    have hbc0 : 0 ≤ e (b 0) c := by rw [he]; exact ind_nonneg _
    have hbc1 : e (b 0) c ≤ 1 := by rw [he]; exact ind_le_one _
    have had0 : 0 ≤ e a d := by rw [he]; exact ind_nonneg _
    have had1 : e a d ≤ 1 := by rw [he]; exact ind_le_one _
    have hba0 : 0 ≤ e (b 0) a := by rw [he]; exact ind_nonneg _
    have hcd0 : 0 ≤ e c d := by rw [he]; exact ind_nonneg _
    have hmul : e (b 0) c * e a d ≤ 1 := by
      have hh := Int.mul_le_mul hbc1 had1 had0 (by omega : (0 : Int) ≤ 1)
      simpa using hh
    have hh := Int.mul_le_mul (Int.le_refl (e (b 0) a * e c d)) hmul
      (Int.mul_nonneg hbc0 had0) (Int.mul_nonneg hba0 hcd0)
    calc
      e (b 0) a * (e (b 0) c * (e a d * e c d))
          = (e (b 0) a * e c d) * (e (b 0) c * e a d) := by ac_rfl
      _ ≤ (e (b 0) a * e c d) * 1 := hh
      _ = e (b 0) a * e c d := by rw [Int.mul_one]
  have htripleAB : rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      e (b 0) a * e c d))) = (A : Int) * (B : Int) := by
    calc
      rsum n (fun a => rsum n (fun c => rsum n (fun d => e (b 0) a * e c d)))
          = rsum n (fun a => rsum n (fun c => e (b 0) a * rsum n (fun d => e c d))) := by
              apply rsum_congr
              intro a ha
              apply rsum_congr
              intro c hc
              rw [rsum_const_mul]
      _ = rsum n (fun a => e (b 0) a * rsum n (fun c => rsum n (fun d => e c d))) := by
              apply rsum_congr
              intro a ha
              rw [rsum_const_mul]
      _ = (rsum n (fun a => e (b 0) a)) * (rsum n (fun c => rsum n (fun d => e c d))) := by
              rw [rsum_mul_const]
      _ = (A : Int) * (B : Int) := by rw [← hAeq, ← hBeq]
  have hSleAB : S ≤ (A : Int) * (B : Int) := by
    rw [← htripleAB]
    exact hSle0
  have hSle0B : S ≤ rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      e a d * e c d))) := by
    dsimp [S]
    apply rsum_le
    intro a ha
    apply rsum_le
    intro c hc
    apply rsum_le
    intro d hd
    have hba0 : 0 ≤ e (b 0) a := by rw [he]; exact ind_nonneg _
    have hbc0 : 0 ≤ e (b 0) c := by rw [he]; exact ind_nonneg _
    have hba1 : e (b 0) a ≤ 1 := by rw [he]; exact ind_le_one _
    have hbc1 : e (b 0) c ≤ 1 := by rw [he]; exact ind_le_one _
    have had0 : 0 ≤ e a d := by rw [he]; exact ind_nonneg _
    have hcd0 : 0 ≤ e c d := by rw [he]; exact ind_nonneg _
    have hmul : e (b 0) a * e (b 0) c ≤ 1 := by
      have hh := Int.mul_le_mul hba1 hbc1 hbc0 (by omega : (0 : Int) ≤ 1)
      simpa using hh
    have hh := Int.mul_le_mul (Int.le_refl (e a d * e c d)) hmul
      (Int.mul_nonneg hba0 hbc0) (Int.mul_nonneg had0 hcd0)
    calc
      e (b 0) a * (e (b 0) c * (e a d * e c d))
          = (e a d * e c d) * (e (b 0) a * e (b 0) c) := by ac_rfl
      _ ≤ (e a d * e c d) * 1 := hh
      _ = e a d * e c d := by rw [Int.mul_one]
  have hcol_nonneg : ∀ d, 0 ≤ rsum n (fun c => e c d) := by
    intro d
    apply rsum_nonneg
    intro c hc
    rw [he]
    exact ind_nonneg _
  have hcol_total : rsum n (fun d => rsum n (fun c => e c d)) = (B : Int) := by
    rw [rsum_comm, hBeq]
  have hcol_le_B : ∀ d, d < n → rsum n (fun c => e c d) ≤ (B : Int) := by
    intro d hd
    calc
      rsum n (fun c => e c d) ≤ rsum n (fun c => rsum n (fun y => e c y)) := by
          apply rsum_le
          intro c hc
          apply rsum_le_total n d
          · exact hd
          · intro y hy
            rw [he]
            exact ind_nonneg _
      _ = (B : Int) := hBeq.symm
  have htripleB : rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      e a d * e c d))) ≤ (B : Int) * (B : Int) := by
    have hrearr : rsum n (fun a => rsum n (fun c => rsum n (fun d => e a d * e c d))) =
        rsum n (fun d => (rsum n (fun a => e a d)) * (rsum n (fun c => e c d))) := by
      calc
        rsum n (fun a => rsum n (fun c => rsum n (fun d => e a d * e c d)))
            = rsum n (fun a => rsum n (fun d => rsum n (fun c => e a d * e c d))) := by
                apply rsum_congr
                intro a ha
                rw [rsum_comm]
        _ = rsum n (fun a => rsum n (fun d => e a d * rsum n (fun c => e c d))) := by
                apply rsum_congr
                intro a ha
                apply rsum_congr
                intro z hz
                rw [rsum_const_mul]
        _ = rsum n (fun d => rsum n (fun a => e a d * rsum n (fun c => e c d))) := by
                rw [rsum_comm]
        _ = rsum n (fun d => (rsum n (fun a => e a d)) * (rsum n (fun c => e c d))) := by
                apply rsum_congr
                intro z hz
                rw [rsum_mul_const]
    rw [hrearr]
    calc
      rsum n (fun d => (rsum n (fun a => e a d)) * (rsum n (fun c => e c d)))
          ≤ rsum n (fun d => (rsum n (fun a => e a d)) * (B : Int)) := by
              apply rsum_le
              intro d hd
              exact Int.mul_le_mul (Int.le_refl _) (hcol_le_B d hd)
                (hcol_nonneg d) (hcol_nonneg d)
      _ = (rsum n (fun d => rsum n (fun a => e a d))) * (B : Int) := by
              rw [rsum_mul_const]
      _ = (B : Int) * (B : Int) := by rw [hcol_total]
  have hSleB2 : S ≤ (B : Int) * (B : Int) := Int.le_trans hSle0B htripleB
  have hSleABnat : S.toNat ≤ A * B := Int.toNat_le.mpr hSleAB
  have hSleB2nat : S.toNat ≤ B ^ 2 := by
    have hb2sq : B ^ 2 = B * B := by
      rw [show 2 = Nat.succ 1 by omega, Nat.pow_succ, Nat.pow_one]
    have hb2 : (B : Int) * (B : Int) = ((B ^ 2 : Nat) : Int) := by
      rw [hb2sq, Int.natCast_mul]
    rw [hb2] at hSleB2
    exact Int.toNat_le.mpr hSleB2
  rcases hcov with ⟨hdenpos, h13, h24, h34⟩
  by_cases hA0 : A = 0
  · have hS0 : S.toNat = 0 := by
      apply Nat.eq_zero_of_le_zero
      simpa [hA0] using hSleABnat
    simp [hS0, Nat.zero_pow hdenpos]
  by_cases hB0 : B = 0
  · have hS0 : S.toNat = 0 := by
      apply Nat.eq_zero_of_le_zero
      simpa [hB0] using hSleB2nat
    simp [hS0, Nat.zero_pow hdenpos]
  have hApos : 0 < A := by omega
  have hBpos : 0 < B := by omega
  have hxy : 2 * den ≤ (w1 + w2) + (w3 + w4) := by omega
  have hpow : S.toNat ^ den ≤ A ^ (w1 + w2) * B ^ (w3 + w4) := by
    by_cases hxd : den ≤ w1 + w2
    · have h1 : S.toNat ^ den ≤ (A * B) ^ den := Nat.pow_le_pow_left hSleABnat den
      rw [Nat.mul_pow] at h1
      exact Nat.le_trans h1 (Nat.mul_le_mul (Nat.pow_le_pow_right hApos hxd)
        (Nat.pow_le_pow_right hBpos h34))
    · have hxlt : w1 + w2 < den := by omega
      let d : Nat := den - (w1 + w2)
      have hdpos : 0 < d := by dsimp [d]; omega
      have hdecomp : den = (w1 + w2) + d := by dsimp [d]; omega
      rw [hdecomp, Nat.pow_add]
      have h1 : S.toNat ^ (w1 + w2) ≤ (A * B) ^ (w1 + w2) :=
        Nat.pow_le_pow_left hSleABnat (w1 + w2)
      have h2 : S.toNat ^ d ≤ (B ^ 2) ^ d := Nat.pow_le_pow_left hSleB2nat d
      have hprod := Nat.mul_le_mul h1 h2
      rw [Nat.mul_pow, ← Nat.pow_mul] at hprod
      have hreshape : (A ^ (w1 + w2) * B ^ (w1 + w2)) * B ^ (2 * d) =
          A ^ (w1 + w2) * B ^ ((w1 + w2) + 2 * d) := by
        calc
          (A ^ (w1 + w2) * B ^ (w1 + w2)) * B ^ (2 * d)
              = A ^ (w1 + w2) * (B ^ (w1 + w2) * B ^ (2 * d)) := by ac_rfl
          _ = A ^ (w1 + w2) * B ^ ((w1 + w2) + 2 * d) := by
                rw [← Nat.pow_add B (w1 + w2) (2 * d)]
      rw [hreshape] at hprod
      have hexp : (w1 + w2) + 2 * d ≤ w3 + w4 := by dsimp [d]; omega
      have hlast : A ^ (w1 + w2) * B ^ ((w1 + w2) + 2 * d) ≤
          A ^ (w1 + w2) * B ^ (w3 + w4) :=
        Nat.mul_le_mul_left _ (Nat.pow_le_pow_right hBpos hexp)
      exact Nat.le_trans hprod hlast
  calc
    S.toNat ^ den ≤ A ^ (w1 + w2) * B ^ (w3 + w4) := hpow
    _ = A ^ w1 * (A ^ w2 * (B ^ w3 * B ^ w4)) := by
        rw [Nat.pow_add, Nat.pow_add]
        ac_rfl



private theorem rsum_add4 (n : Nat) (f1 f2 f3 f4 : Nat → Int) :
    rsum n (fun i => f1 i + f2 i + f3 i + f4 i) =
      rsum n f1 + rsum n f2 + rsum n f3 + rsum n f4 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ, rsum_succ, rsum_succ]
      rw [ih]
      omega

private theorem rsum2_add4 (n : Nat) (f1 f2 f3 f4 : Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun c => f1 a c + f2 a c + f3 a c + f4 a c)) =
      rsum n (fun a => rsum n (fun c => f1 a c)) +
      rsum n (fun a => rsum n (fun c => f2 a c)) +
      rsum n (fun a => rsum n (fun c => f3 a c)) +
      rsum n (fun a => rsum n (fun c => f4 a c)) := by
  rw [show (fun a => rsum n (fun c => f1 a c + f2 a c + f3 a c + f4 a c)) =
      (fun a => rsum n (fun c => f1 a c) + rsum n (fun c => f2 a c) +
        rsum n (fun c => f3 a c) + rsum n (fun c => f4 a c)) by
        funext a
        exact rsum_add4 n _ _ _ _]
  rw [rsum_add4]

private theorem rsum3_add4 (n : Nat) (f1 f2 f3 f4 : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      f1 a c d + f2 a c d + f3 a c d + f4 a c d))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => f1 a c d))) +
      rsum n (fun a => rsum n (fun c => rsum n (fun d => f2 a c d))) +
      rsum n (fun a => rsum n (fun c => rsum n (fun d => f3 a c d))) +
      rsum n (fun a => rsum n (fun c => rsum n (fun d => f4 a c d))) := by
  rw [show (fun a => rsum n (fun c => rsum n (fun d =>
      f1 a c d + f2 a c d + f3 a c d + f4 a c d))) =
      (fun a => rsum n (fun c => rsum n (fun d => f1 a c d)) +
        rsum n (fun c => rsum n (fun d => f2 a c d)) +
        rsum n (fun c => rsum n (fun d => f3 a c d)) +
        rsum n (fun c => rsum n (fun d => f4 a c d))) by
        funext a
        exact rsum2_add4 n _ _ _ _]
  rw [rsum_add4]

private theorem rsum3_add2 (n : Nat) (f g : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun c => rsum n (fun d => f a c d + g a c d))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => f a c d))) +
      rsum n (fun a => rsum n (fun c => rsum n (fun d => g a c d))) := by
  rw [show (fun a => rsum n (fun c => rsum n (fun d => f a c d + g a c d))) =
      (fun a => rsum n (fun c => rsum n (fun d => f a c d)) +
        rsum n (fun c => rsum n (fun d => g a c d))) by
        funext a
        rw [show (fun c => rsum n (fun d => f a c d + g a c d)) =
            (fun c => rsum n (fun d => f a c d) + rsum n (fun d => g a c d)) by
              funext c
              exact rsum_add n _ _]
        rw [rsum_add]]
  rw [rsum_add]

private theorem rsum3_const_mul (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun d => c * f a b d))) =
      c * rsum n (fun a => rsum n (fun b => rsum n (fun d => f a b d))) := by
  rw [show (fun a => rsum n (fun b => rsum n (fun d => c * f a b d))) =
      (fun a => c * rsum n (fun b => rsum n (fun d => f a b d))) by
        funext a
        rw [show (fun b => rsum n (fun d => c * f a b d)) =
            (fun b => c * rsum n (fun d => f a b d)) by
              funext b
              exact rsum_const_mul n c _]
        rw [rsum_const_mul]]
  rw [rsum_const_mul]

private theorem triple_ite_left (n a : Nat) (ha : a < n) (f : Nat → Nat → Int) :
    rsum n (fun c => rsum n (fun d => if c = a then f c d else 0)) =
      rsum n (fun d => f a d) := by
  calc
    rsum n (fun c => rsum n (fun d => if c = a then f c d else 0))
        = rsum n (fun c => if c = a then rsum n (fun d => f c d) else 0) := by
            apply rsum_congr
            intro c hc
            by_cases h : c = a
            · simp [h]
            · have hz : rsum n (fun d => if c = a then f c d else 0) = 0 := by
                rw [show (fun d => if c = a then f c d else 0) = (fun _ => (0 : Int)) by
                  funext d
                  simp [h]]
                exact rsum_zero n
              rw [hz]
              simp [h]
    _ = rsum n (fun d => f a d) := rsum_ite_eq n a (fun c => rsum n (fun d => f c d)) ha

private theorem triple_ite_right (n a : Nat) (ha : a < n) (f : Nat → Nat → Int) :
    rsum n (fun c => rsum n (fun d => if d = a then f c d else 0)) =
      rsum n (fun c => f c a) := by
  apply rsum_congr
  intro c hc
  exact rsum_ite_eq n a (fun d => f c d) ha

private theorem triple_ite_both (n a : Nat) (ha : a < n) (f : Nat → Nat → Int) :
    rsum n (fun c => rsum n (fun d => if c = a ∧ d = a then f c d else 0)) = f a a := by
  calc
    rsum n (fun c => rsum n (fun d => if c = a ∧ d = a then f c d else 0))
        = rsum n (fun c => if c = a then rsum n (fun d => if d = a then f c d else 0) else 0) := by
            apply rsum_congr
            intro c hc
            by_cases h : c = a
            · simp [h]
            · have hz : rsum n (fun d => if c = a ∧ d = a then f c d else 0) = 0 := by
                rw [show (fun d => if c = a ∧ d = a then f c d else 0) = (fun _ => (0 : Int)) by
                  funext d
                  simp [h]]
                exact rsum_zero n
              rw [hz]
              simp [h]
    _ = rsum n (fun d => if d = a then f a d else 0) :=
          rsum_ite_eq n a (fun c => rsum n (fun d => if d = a then f c d else 0)) ha
    _ = f a a := rsum_ite_eq n a (fun d => f a d) ha

private theorem ie_point (P : Nat → Nat → Nat → Int) (a c d : Nat) :
    (if (a ≠ c ∧ a ≠ d) ∧ c ≠ d then P a c d else 0) +
      (if c = a then P a c d else 0) +
      (if d = a then P a c d else 0) +
      (if d = c then P a c d else 0) =
    P a c d + 2 * (if c = a ∧ d = a then P a c d else 0) := by
  by_cases hac : a = c <;> by_cases had : a = d <;> by_cases hcd : c = d <;>
    simp_all <;> omega

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  simp [injective, partitions, mobius, collapsed, pattern, value, put, integrate, factorial]
  let F : Nat → Nat → Int := fun x y => db 0 x y
  let P : Nat → Nat → Nat → Int := fun a c d => F (b 0) a * (F (b 0) c * (F a d * F c d))
  let Q1 : Nat → Nat → Int := fun a d => F (b 0) a * (F (b 0) a * (F a d * F a d))
  let Q2 : Nat → Nat → Int := fun a c => F (b 0) c * (F (b 0) a * (F c c * F a c))
  let Q3 : Nat → Nat → Int := fun a c => F (b 0) a * (F (b 0) c * (F a c * F c c))
  let Q4 : Nat → Int := fun a => F (b 0) a * (F (b 0) a * (F a a * F a a))
  change (rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      if (a ≠ c ∧ a ≠ d) ∧ c ≠ d then P a c d else 0)))) =
    rsum n (fun a => rsum n (fun c => rsum n (fun d => P a c d))) +
      (- rsum n (fun a => rsum n (fun d => Q1 a d)) +
       (- rsum n (fun a => rsum n (fun c => Q2 a c)) +
        (- rsum n (fun a => rsum n (fun c => Q3 a c)) +
         2 * rsum n (fun a => Q4 a))))
  have hQ1 : ∀ a d, Q1 a d = P a a d := by intro a d; rfl
  have hQ2 : ∀ a c, Q2 a c = P c a c := by intro a c; rfl
  have hQ3 : ∀ a c, Q3 a c = P a c c := by intro a c; rfl
  have hQ4 : ∀ a, Q4 a = P a a a := by intro a; rfl
  have hQ1s : rsum n (fun a => rsum n (fun d => Q1 a d)) =
      rsum n (fun a => rsum n (fun d => P a a d)) := by
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro d hd
    exact hQ1 a d
  have hQ2s : rsum n (fun a => rsum n (fun c => Q2 a c)) =
      rsum n (fun a => rsum n (fun c => P a c a)) := by
    calc
      rsum n (fun a => rsum n (fun c => Q2 a c))
          = rsum n (fun a => rsum n (fun c => P c a c)) := by
              apply rsum_congr
              intro a ha
              apply rsum_congr
              intro c hc
              exact hQ2 a c
      _ = rsum n (fun c => rsum n (fun a => P c a c)) := rsum_comm n n (fun x y => P y x y)
  have hQ3s : rsum n (fun a => rsum n (fun c => Q3 a c)) =
      rsum n (fun a => rsum n (fun c => P a c c)) := by
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    exact hQ3 a c
  have hQ4s : rsum n (fun a => Q4 a) = rsum n (fun a => P a a a) := by
    apply rsum_congr
    intro a ha
    exact hQ4 a
  rw [hQ1s, hQ2s, hQ3s, hQ4s]
  let TI : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      if (a ≠ c ∧ a ≠ d) ∧ c ≠ d then P a c d else 0)))
  let T0 : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d => P a c d)))
  let T12 : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d => if c = a then P a c d else 0)))
  let T13 : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d => if d = a then P a c d else 0)))
  let T23 : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d => if d = c then P a c d else 0)))
  let T123 : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d => if c = a ∧ d = a then P a c d else 0)))
  change TI = T0 + (- rsum n (fun a => rsum n (fun d => P a a d)) +
       (- rsum n (fun a => rsum n (fun c => P a c a)) +
        (- rsum n (fun a => rsum n (fun c => P a c c)) +
         2 * rsum n (fun a => P a a a))))
  have hT12 : T12 = rsum n (fun a => rsum n (fun d => P a a d)) := by
    dsimp [T12]
    apply rsum_congr
    intro a ha
    exact triple_ite_left n a ha (fun c d => P a c d)
  have hT13 : T13 = rsum n (fun a => rsum n (fun c => P a c a)) := by
    dsimp [T13]
    apply rsum_congr
    intro a ha
    exact triple_ite_right n a ha (fun c d => P a c d)
  have hT23 : T23 = rsum n (fun a => rsum n (fun c => P a c c)) := by
    dsimp [T23]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    exact rsum_ite_eq n c (fun d => P a c d) hc
  have hT123 : T123 = rsum n (fun a => P a a a) := by
    dsimp [T123]
    apply rsum_congr
    intro a ha
    exact triple_ite_both n a ha (fun c d => P a c d)
  have hpoint : rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      (if (a ≠ c ∧ a ≠ d) ∧ c ≠ d then P a c d else 0) +
        (if c = a then P a c d else 0) +
        (if d = a then P a c d else 0) +
        (if d = c then P a c d else 0)))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d =>
        P a c d + 2 * (if c = a ∧ d = a then P a c d else 0)))) := by
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro d hd
    exact ie_point P a c d
  rw [rsum3_add4] at hpoint
  rw [rsum3_add2] at hpoint
  rw [rsum3_const_mul] at hpoint
  dsimp [TI, T0, T12, T13, T23, T123] at hpoint ⊢
  omega

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.PostDiamond.Remaining

