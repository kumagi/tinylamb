import TinylambOpt.ResearchGoals.ReciprocalDivMod

namespace TinylambOpt.ResearchProofs.ReciprocalDivMod

open TinylambOpt.ResearchGoals.ReciprocalDivMod

/-! ## Arithmetic of the 64-bit reciprocal -/

def recip (m : Nat) : Nat := (2^64 + m - 1) / m

theorem recip_mul_lower (m : Nat) (hm : 0 < m) : 2^64 ≤ recip m * m := by
  unfold recip
  have hN : 2^64 + m - 1 = 2^64 + (m - 1) := by omega
  rw [hN]
  have hdiv := Nat.div_add_mod (2^64+(m-1)) m
  rw [Nat.mul_comm m ((2^64+(m-1))/m)] at hdiv
  have hmod := Nat.mod_lt (2^64+(m-1)) hm
  omega

theorem recip_mul_upper (m : Nat) (hm : 0 < m) : recip m * m < 2^64 + m := by
  unfold recip
  have hN : 2^64 + m - 1 = 2^64 + (m - 1) := by omega
  rw [hN]
  have hdiv := Nat.div_add_mod (2^64+(m-1)) m
  rw [Nat.mul_comm m ((2^64+(m-1))/m)] at hdiv
  omega

theorem recip_le (m : Nat) (hm2 : 2 ≤ m) : recip m ≤ 2^63 := by
  unfold recip
  rw [Nat.div_le_iff_le_mul (by omega : 0 < m)]
  have hN : 2^64 + m - 1 = 2^64 + (m - 1) := by omega
  rw [hN]
  have hmul := Nat.mul_le_mul_left (2^63) hm2
  omega

theorem recip_pos (m : Nat) (hm : 0 < m) : 0 < recip m := by
  unfold recip
  apply Nat.div_pos
  · omega
  · exact hm

/-- The high 64 bits of the 128-bit product are the true quotient or one more. -/
theorem est_bounds (a m : Nat) (hm2 : 2 ≤ m) (hmle : m ≤ 2^63) (ha : a ≤ 2^63) :
    let c := recip m
    let e := (a*c) / 2^64
    a/m ≤ e ∧ e ≤ a/m + 1 := by
  dsimp
  let q := a / m
  let c := recip m
  let e := (a*c) / 2^64
  change q ≤ e ∧ e ≤ q + 1
  have hmpos : 0 < m := by omega
  have hc_lower : 2^64 ≤ c * m := by dsimp [c]; exact recip_mul_lower m hmpos
  have hc_upper : c * m < 2^64 + m := by dsimp [c]; exact recip_mul_upper m hmpos
  have hdm0 := Nat.div_add_mod a m
  rw [Nat.mul_comm m (a/m)] at hdm0
  have hmod := Nat.mod_lt a hmpos
  have hqm : q * m ≤ a := by dsimp [q]; exact Nat.div_mul_le_self a m
  have hqm' : a < (q+1)*m := by
    calc a = q*m + a%m := by simpa [q] using hdm0.symm
         _ < q*m + m := Nat.add_lt_add_left hmod (q*m)
         _ = (q+1)*m := by rw [Nat.add_mul]; simp
  have hcpos : 0 < c := by dsimp [c]; exact recip_pos m hmpos
  have lower_prod : q * 2^64 ≤ a * c := by
    calc q * 2^64 ≤ q * (c*m) := Nat.mul_le_mul_left q hc_lower
         _ = (q*m)*c := by ac_rfl
         _ ≤ a*c := Nat.mul_le_mul_right c hqm
  have upper_prod : a * c < (q+2) * 2^64 := by
    have h1 : a*c < ((q+1)*m)*c := Nat.mul_lt_mul_of_pos_right hqm' hcpos
    have h2 : ((q+1)*m)*c = (q+1)*(c*m) := by ac_rfl
    have h1' : a*c < (q+1)*(c*m) := by rwa [h2] at h1
    have h3 : (q+1)*(c*m) < (q+1)*(2^64+m) := Nat.mul_lt_mul_of_pos_left hc_upper (by omega)
    have h4 : (q+1)*(2^64+m) = (q+1)*2^64 + (q+1)*m := by rw [Nat.mul_add]
    have h3' : (q+1)*(c*m) < (q+1)*2^64 + (q+1)*m := by rwa [h4] at h3
    have hqm_up : (q+1)*m ≤ 2^64 := by
      have heq : (q+1)*m = q*m + m := by rw [Nat.add_mul]; simp
      rw [heq]
      omega
    have hq2 : q+2 = (q+1)+1 := by omega
    have h6 : (q+2)*2^64 = (q+1)*2^64 + 2^64 := by
      rw [hq2, Nat.add_mul]
    have h5 : (q+1)*2^64 + (q+1)*m ≤ (q+2)*2^64 := by
      rw [h6]
      exact Nat.add_le_add_left hqm_up ((q+1)*2^64)
    exact Nat.lt_trans h1' (Nat.lt_of_lt_of_le h3' h5)
  constructor
  · rw [Nat.le_div_iff_mul_le (by omega : 0 < 2^64)]
    exact lower_prod
  · rw [Nat.div_le_iff_le_mul (by omega : 0 < 2^64)]
    omega

/-- The 128-bit correction subtracts the spurious unit, leaving the exact quotient. -/
theorem correction_nat (a m : Nat) (hm2 : 2 ≤ m) (hmle : m ≤ 2^63) (ha : a ≤ 2^63) :
    let c := recip m
    let e := (a*c) / 2^64
    (if e*m > a then e-1 else e) = a/m := by
  dsimp
  let q := a/m
  let c := recip m
  let e := (a*c)/2^64
  change (if e*m > a then e-1 else e) = q
  have hb := est_bounds a m hm2 hmle ha
  dsimp [c] at hb
  change q ≤ e ∧ e ≤ q+1 at hb
  have hmpos : 0 < m := by omega
  have hqm : q*m ≤ a := by dsimp [q]; exact Nat.div_mul_le_self a m
  by_cases h : e*m > a
  · have hgt : q < e := by
      by_cases hle : e ≤ q
      · have hem : e*m ≤ q*m := Nat.mul_le_mul_right m hle
        have : e*m ≤ a := Nat.le_trans hem hqm
        exact False.elim ((Nat.not_le_of_gt h) this)
      · have hne : ¬ q = e := by
          intro hqe
          apply hle
          rw [hqe]
          exact Nat.le_refl e
        exact Nat.lt_of_le_of_ne hb.1 hne
    have heq : e = q+1 := Nat.le_antisymm hb.2 (Nat.succ_le_of_lt hgt)
    rw [if_pos h, heq]
    simp
  · have he_le : e*m ≤ a := Nat.le_of_not_gt h
    have hdm0 := Nat.div_add_mod a m
    rw [Nat.mul_comm m (a/m)] at hdm0
    have hmod := Nat.mod_lt a hmpos
    have hqm' : a < (q+1)*m := by
      calc a = q*m + a%m := by simpa [q] using hdm0.symm
           _ < q*m + m := Nat.add_lt_add_left hmod (q*m)
           _ = (q+1)*m := by rw [Nat.add_mul]; simp
    have hlt : e < q+1 := by
      by_cases hge : q+1 ≤ e
      · have hmul : (q+1)*m ≤ e*m := Nat.mul_le_mul_right m hge
        have hcontra : (q+1)*m ≤ a := Nat.le_trans hmul he_le
        exact False.elim (Nat.not_lt_of_ge hcontra hqm')
      · omega
    rw [if_neg h]
    omega

/-- The actual bit-vector pipeline computes the exact quotient of the magnitudes. -/
theorem bitvec_magnitude (a m : Nat) (hm2 : 2 ≤ m) (hmle : m ≤ 2^63) (ha : a ≤ 2^63) :
    let dividend : BitVec 128 := BitVec.ofNat 128 a
    let multiplier : BitVec 64 := BitVec.ofNat 64 ((2^64 + m - 1)/m)
    let wide := dividend * multiplier.zeroExtend 128
    let estimate := wide.extractLsb' 64 64
    let product := estimate.zeroExtend 128 * BitVec.ofNat 128 m
    let corrected := if product > dividend then estimate - 1 else estimate
    corrected.toNat = a/m := by
  dsimp
  let c := recip m
  let q := a/m
  let dividend : BitVec 128 := BitVec.ofNat 128 a
  let multiplier : BitVec 64 := BitVec.ofNat 64 c
  let wide : BitVec 128 := dividend * multiplier.zeroExtend 128
  let estimate : BitVec 64 := wide.extractLsb' 64 64
  let product : BitVec 128 := estimate.zeroExtend 128 * BitVec.ofNat 128 m
  change (if product > dividend then estimate - 1 else estimate).toNat = q
  have hmpos : 0 < m := by omega
  have hc_le : c ≤ 2^63 := by dsimp [c]; exact recip_le m hm2
  have hc_lt64 : c < 2^64 := by omega
  have ha_lt128 : a < 2^128 := by omega
  have hm_lt128 : m < 2^128 := by omega
  have hac_lt128 : a*c < 2^128 := by
    have := Nat.mul_le_mul ha hc_le
    omega
  have hdiv_toNat : dividend.toNat = a := by
    dsimp [dividend]
    rw [BitVec.toNat_ofNat, Nat.mod_eq_of_lt ha_lt128]
  have hmul_toNat : multiplier.toNat = c := by
    dsimp [multiplier]
    rw [BitVec.toNat_ofNat, Nat.mod_eq_of_lt hc_lt64]
  have hwide_toNat : wide.toNat = a*c := by
    dsimp [wide]
    rw [hdiv_toNat]
    have hext : (multiplier.zeroExtend 128).toNat = c := by
      rw [BitVec.toNat_setWidth, hmul_toNat, Nat.mod_eq_of_lt]
      omega
    rw [hext]
    rw [Nat.mod_eq_of_lt hac_lt128]
  have hest_toNat : estimate.toNat = (a*c)/2^64 := by
    dsimp [estimate]
    rw [Nat.shiftRight_eq_div_pow, hwide_toNat]
    have hac_div_lt : (a*c)/2^64 < 2^64 := by
      apply Nat.div_lt_of_lt_mul
      have hp : 2^64 * 2^64 = 2^128 := by omega
      rwa [hp]
    rw [Nat.mod_eq_of_lt hac_div_lt]
  have hb := est_bounds a m hm2 hmle ha
  dsimp [c] at hb
  change q ≤ (a*c)/2^64 ∧ (a*c)/2^64 ≤ q+1 at hb
  have hest_bound : estimate.toNat ≤ q+1 := by omega
  have hest_mul_bound : estimate.toNat * m ≤ 2^64 := by
    have hqm_up : (q+1)*m ≤ 2^64 := by
      have hqm : q*m ≤ a := Nat.div_mul_le_self a m
      have heq : (q+1)*m = q*m + m := by rw [Nat.add_mul]; simp
      rw [heq]
      omega
    have : estimate.toNat * m ≤ (q+1)*m := Nat.mul_le_mul_right m hest_bound
    exact Nat.le_trans this hqm_up
  have hest_mul_lt128 : estimate.toNat * m < 2^128 := by omega
  have hprod_toNat : product.toNat = estimate.toNat * m := by
    dsimp [product]
    have hext : (estimate.zeroExtend 128).toNat = estimate.toNat := by
      rw [BitVec.toNat_setWidth, Nat.mod_eq_of_lt]
      omega
    rw [hext]
    have hm_toNat : (BitVec.ofNat 128 m).toNat = m := by
      rw [BitVec.toNat_ofNat, Nat.mod_eq_of_lt hm_lt128]
    rw [hm_toNat, Nat.mod_eq_of_lt hest_mul_lt128]
  have hcond : (product > dividend) ↔ (estimate.toNat * m > a) := by
    simp [BitVec.lt_def, hprod_toNat, hdiv_toNat]
  by_cases h : product > dividend
  · have hn : estimate.toNat * m > a := hcond.mp h
    have hest_pos : 0 < estimate.toNat := by
      by_cases hz : estimate.toNat = 0
      · rw [hz] at hn
        simp at hn
      · omega
    rw [if_pos h]
    rw [BitVec.toNat_sub]
    have hone : (1 : BitVec 64).toNat = 1 := by decide
    rw [hone]
    rw [hest_toNat]
    have hmod : (2^64 - 1 + (a*c)/2^64) % 2^64 = (a*c)/2^64 - 1 := by
      have hlt : (a*c)/2^64 < 2^64 := by omega
      omega
    rw [hmod]
    have hc := correction_nat a m hm2 hmle ha
    dsimp [c] at hc
    change (if (a*c)/2^64 * m > a then (a*c)/2^64 - 1 else (a*c)/2^64) = q at hc
    have hn' : (a*c)/2^64 * m > a := by rwa [hest_toNat] at hn
    rw [if_pos hn'] at hc
    exact hc
  · have hn : ¬ estimate.toNat * m > a := by intro hh; exact h (hcond.mpr hh)
    rw [if_neg h]
    rw [hest_toNat]
    have hc := correction_nat a m hm2 hmle ha
    dsimp [c] at hc
    change (if (a*c)/2^64 * m > a then (a*c)/2^64 - 1 else (a*c)/2^64) = q at hc
    have hn' : ¬ (a*c)/2^64 * m > a := by
      intro hh
      apply hn
      rwa [hest_toNat]
    rw [if_neg hn'] at hc
    exact hc

/-! ## Bounds and signed division -/

theorem int64_abs_le (a : Int64) : a.toInt.natAbs ≤ 2^63 := by
  have h1 := BitVec.le_toInt a.toBitVec
  have h2 := BitVec.toInt_le (x := a.toBitVec)
  change -(2^63 : Int) ≤ a.toInt at h1
  change a.toInt ≤ (2:Int)^63 - 1 at h2
  by_cases hneg : a.toInt < 0
  · have hc : ((-a.toInt).natAbs : Int) = -a.toInt := Int.ofNat_natAbs_of_nonneg (by omega)
    have hc2 : ((-a.toInt).natAbs : Int) ≤ (2^63 : Nat) := by omega
    have hn : (-a.toInt).natAbs ≤ 2^63 := by exact_mod_cast hc2
    simpa using hn
  · have hc : (a.toInt.natAbs : Int) = a.toInt := Int.ofNat_natAbs_of_nonneg (by omega)
    have hc2 : (a.toInt.natAbs : Int) ≤ (2^63 : Nat) := by omega
    exact_mod_cast hc2

theorem rem_pos_pos (A M : Nat) : (Int.ofNat A) - (Int.ofNat (A/M)) * (Int.ofNat M) = Int.ofNat (A%M) := by
  have h := Nat.div_add_mod A M
  rw [Nat.mul_comm M (A/M)] at h
  have hle : (A/M)*M ≤ A := Nat.div_mul_le_self A M
  have hnat : A - (A/M)*M = A%M := by omega
  rw [← hnat]
  simp only [Int.ofNat_eq_natCast]
  rw [Int.ofNat_sub hle, Int.natCast_mul]

theorem rem_pos_neg (A M : Nat) : (Int.ofNat A) - (-(Int.ofNat (A/M))) * (-(Int.ofNat M)) = Int.ofNat (A%M) := by
  rw [Int.neg_mul_neg]
  exact rem_pos_pos A M

theorem rem_neg_pos (A M : Nat) : (-(Int.ofNat A)) - (-(Int.ofNat (A/M))) * (Int.ofNat M) = -Int.ofNat (A%M) := by
  have hp := rem_pos_pos A M
  rw [Int.neg_mul]
  simp only [Int.ofNat_eq_natCast] at hp ⊢
  omega

theorem rem_neg_neg (A M : Nat) : (-(Int.ofNat A)) - (Int.ofNat (A/M)) * (-(Int.ofNat M)) = -Int.ofNat (A%M) := by
  have hp := rem_pos_pos A M
  rw [Int.mul_neg]
  simp only [Int.ofNat_eq_natCast] at hp ⊢
  omega

/-- Truncated division/remainder agree with the sign-restored reciprocal result. -/
theorem signed_divmod (ai bi : Int) (Q : Nat)
    (hQ : Q = ai.natAbs / bi.natAbs) (hM : bi.natAbs ≠ 0) :
    (if (ai < 0) != (bi < 0) then -(Q:Int) else (Q:Int)) = ai.tdiv bi ∧
    ai - (if (ai < 0) != (bi < 0) then -(Q:Int) else (Q:Int)) * bi = ai.tmod bi := by
  cases ai with
  | ofNat A =>
    cases bi with
    | ofNat M =>
      have hA : ¬ ((A : Int) < 0) := by have := Int.natCast_nonneg A; omega
      have hM' : ¬ ((M : Int) < 0) := by have := Int.natCast_nonneg M; omega
      have hcond : (((Int.ofNat A : Int) < 0) != ((Int.ofNat M : Int) < 0)) = false := by simp [hA, hM']
      have hQ' : Q = A / M := by simpa using hQ
      rw [hcond, hQ']
      constructor
      · rfl
      · exact rem_pos_pos A M
    | negSucc M =>
      have hA : ¬ ((A : Int) < 0) := by have := Int.natCast_nonneg A; omega
      have hM' : (Int.negSucc M : Int) < 0 := by simp
      have hcond : (((Int.ofNat A : Int) < 0) != ((Int.negSucc M : Int) < 0)) = true := by simp [hA, hM']
      have hQ' : Q = A / M.succ := by simpa using hQ
      rw [hcond, hQ']
      constructor
      · rfl
      · change (Int.ofNat A) - (-(Int.ofNat (A / M.succ))) * (-(Int.ofNat M.succ)) = Int.ofNat (A % M.succ)
        exact rem_pos_neg A M.succ
  | negSucc A =>
    cases bi with
    | ofNat M =>
      have hA : (Int.negSucc A : Int) < 0 := by simp
      have hM' : ¬ ((M : Int) < 0) := by have := Int.natCast_nonneg M; omega
      have hcond : (((Int.negSucc A : Int) < 0) != ((Int.ofNat M : Int) < 0)) = true := by simp [hA, hM']
      have hQ' : Q = A.succ / M := by simpa using hQ
      rw [hcond, hQ']
      constructor
      · rfl
      · change (-(Int.ofNat A.succ)) - (-(Int.ofNat (A.succ / M))) * (Int.ofNat M) = -Int.ofNat (A.succ % M)
        exact rem_neg_pos A.succ M
    | negSucc M =>
      have hA : (Int.negSucc A : Int) < 0 := by simp
      have hM' : (Int.negSucc M : Int) < 0 := by simp
      have hcond : (((Int.negSucc A : Int) < 0) != ((Int.negSucc M : Int) < 0)) = false := by simp [hA, hM']
      have hQ' : Q = A.succ / M.succ := by simpa using hQ
      rw [hcond, hQ']
      constructor
      · rfl
      · change (-(Int.ofNat A.succ)) - (Int.ofNat (A.succ / M.succ)) * (-(Int.ofNat M.succ)) = -Int.ofNat (A.succ % M.succ)
        exact rem_neg_neg A.succ M.succ

/-! ## The final result -/

theorem multiply_correct (a b : Int64) (hm2 : 2 ≤ b.toInt.natAbs) :
    run (.multiply b (BitVec.ofNat 64 ((2^64 - 1 + b.toInt.natAbs) / b.toInt.natAbs))) (some a)
      = Result.pair (a.toInt.tdiv b.toInt) (a.toInt.tmod b.toInt) := by
  let A := a.toInt.natAbs
  let m := b.toInt.natAbs
  have hrecip_eq : 2^64 - 1 + m = 2^64 + m - 1 := by omega
  have hmle : m ≤ 2^63 := by dsimp [m]; exact int64_abs_le b
  have hale : A ≤ 2^63 := by dsimp [A]; exact int64_abs_le a
  have hmag := bitvec_magnitude A m hm2 hmle hale
  let dividend : BitVec 128 := BitVec.ofNat 128 A
  let multiplier : BitVec 64 := BitVec.ofNat 64 ((2^64 + m - 1)/m)
  let wide : BitVec 128 := dividend * multiplier.zeroExtend 128
  let estimate : BitVec 64 := wide.extractLsb' 64 64
  let product : BitVec 128 := estimate.zeroExtend 128 * BitVec.ofNat 128 m
  let corrected : BitVec 64 := if product > dividend then estimate - 1 else estimate
  have hmag' : corrected.toNat = A / m := by
    simpa [corrected, dividend, multiplier, wide, estimate, product, A, m] using hmag
  let Q := corrected.toNat
  have hQ : Q = a.toInt.natAbs / b.toInt.natAbs := by simpa [Q, A, m] using hmag'
  have hne : b.toInt.natAbs ≠ 0 := by omega
  have hs := signed_divmod a.toInt b.toInt Q hQ hne
  dsimp [run, A, m]
  rw [hrecip_eq]
  change Result.pair (if (a.toInt < 0) != (b.toInt < 0) then -(Q : Int) else (Q : Int))
      (a.toInt - (if (a.toInt < 0) != (b.toInt < 0) then -(Q : Int) else (Q : Int)) * b.toInt)
      = Result.pair (a.toInt.tdiv b.toInt) (a.toInt.tmod b.toInt)
  rcases hs with ⟨hq, hr⟩
  let qexpr : Int := if (a.toInt < 0) != (b.toInt < 0) then -(Q : Int) else (Q : Int)
  have h1 : Result.pair qexpr (a.toInt - qexpr * b.toInt) =
      Result.pair (a.toInt.tdiv b.toInt) (a.toInt - qexpr * b.toInt) :=
    congrArg (fun z : Int => Result.pair z (a.toInt - qexpr * b.toInt)) hq
  have h2 : Result.pair (a.toInt.tdiv b.toInt) (a.toInt - qexpr * b.toInt) =
      Result.pair (a.toInt.tdiv b.toInt) (a.toInt.tmod b.toInt) :=
    congrArg (fun z : Int => Result.pair (a.toInt.tdiv b.toInt) z) hr
  exact h1.trans h2

theorem sound : TinylambOpt.ResearchGoals.ReciprocalDivMod.Goal := by
  intro x d
  cases x with
  | none =>
      cases d <;> rfl
  | some a =>
      cases d with
      | none => rfl
      | some b =>
        by_cases hm0 : b.toInt.natAbs = 0
        · have hb0 : b.toInt = 0 := by simpa using (Int.natAbs_eq_zero.mp hm0)
          simp [prepare, run, reference, hm0, hb0]
        · by_cases hm1 : b.toInt.natAbs = 1
          · rcases Int.natAbs_eq_iff.mp hm1 with hb | hb
            · simp [prepare, run, reference, hm0, hm1, hb]
            · have hbneg : b.toInt < 0 := by rw [hb]; simp
              by_cases hmin : a.toInt = -(2^63)
              · simp [prepare, run, reference, hm0, hm1, hb, hmin]
              · have hbeq : (a.toInt == -(2^63)) = false := by
                  -- `==` is boolean equality on Int
                  exact beq_eq_false_iff_ne.mpr hmin
                simp [prepare, run, reference, hm0, hm1, hb, hmin, hbeq, Int.tdiv_neg, Int.tmod_neg]
          · have hm2 : 2 ≤ b.toInt.natAbs := by omega
            have hb0 : b.toInt ≠ 0 := by
              intro hz
              apply hm0
              simpa [hz]
            have hbne1 : b.toInt ≠ -1 := by
              intro hz
              have : b.toInt.natAbs = 1 := by rw [hz]; rfl
              omega
            have hrecip : 2^64 + b.toInt.natAbs - 1 = 2^64 - 1 + b.toInt.natAbs := by omega
            rw [prepare, if_neg hm0, if_neg hm1, hrecip]
            change run (.multiply b (BitVec.ofNat 64 ((2^64 - 1 + b.toInt.natAbs) / b.toInt.natAbs))) (some a) =
              (if b.toInt = 0 then Result.zeroDivisor
               else if a.toInt = -(2^63) ∧ b.toInt = -1 then Result.overflow
               else Result.pair (a.toInt.tdiv b.toInt) (a.toInt.tmod b.toInt))
            rw [if_neg hb0]
            rw [if_neg (by intro h; exact hbne1 h.2)]
            exact multiply_correct a b hm2

end TinylambOpt.ResearchProofs.ReciprocalDivMod
