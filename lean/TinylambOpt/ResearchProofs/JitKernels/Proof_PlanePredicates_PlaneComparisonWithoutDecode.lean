import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
Unsigned less-than is decided directly on the encoded bit planes: scanning
planes from least to most significant keeps the first differing plane's
threshold bit as the result. No lane needs to be reconstructed, so a ballot
over the planes can be generated for all rows at once.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PlanePredicates_PlaneComparisonWithoutDecode
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

private theorem foldBits_succ (x c w : Nat) :
    foldBits x c (w + 1) =
      (if bit x w == bit c w then foldBits x c w else bit c w) := by
  unfold foldBits
  rw [List.range_succ, List.foldl_append]
  rfl

private theorem foldBits_congr {x x' c c' w : Nat}
    (hx : ∀ j, j < w → bit x j = bit x' j)
    (hc : ∀ j, j < w → bit c j = bit c' j) :
    foldBits x c w = foldBits x' c' w := by
  induction w with
  | zero => simp [foldBits]
  | succ w ih =>
      rw [foldBits_succ, foldBits_succ]
      have ih' := ih (fun j hj => hx j (by omega)) (fun j hj => hc j (by omega))
      rw [ih', hx w (by omega), hc w (by omega)]

private theorem foldBits_mod_pow (x c w k : Nat) (hk : k ≤ w) :
    foldBits x c k = foldBits (x % 2^w) (c % 2^w) k := by
  apply foldBits_congr
  · intro j hj
    unfold bit
    rw [Nat.testBit_mod_two_pow]
    simp [show j < w by omega]
  · intro j hj
    unfold bit
    rw [Nat.testBit_mod_two_pow]
    simp [show j < w by omega]

theorem sound : PlaneComparisonWithoutDecode.Goal := by
  intro x c w hx hc
  induction w generalizing x c with
  | zero =>
      have hx0 : x = 0 := by omega
      have hc0 : c = 0 := by omega
      subst x
      subst c
      simp [foldBits]
  | succ w ih =>
      rw [foldBits_succ]
      have hmodx : x % 2^w < 2^w := Nat.mod_lt _ (Nat.two_pow_pos w)
      have hmodc : c % 2^w < 2^w := Nat.mod_lt _ (Nat.two_pow_pos w)
      have ih' := ih (x % 2^w) (c % 2^w) hmodx hmodc
      rw [foldBits_mod_pow x c w w (by omega), ih']
      have hqxlt : x / 2^w < 2 := by
        rw [Nat.div_lt_iff_lt_mul (Nat.two_pow_pos w)]
        simpa [Nat.pow_succ, Nat.mul_comm] using hx
      have hqclt : c / 2^w < 2 := by
        rw [Nat.div_lt_iff_lt_mul (Nat.two_pow_pos w)]
        simpa [Nat.pow_succ, Nat.mul_comm] using hc
      have hxqx : x = 2^w * (x / 2^w) + x % 2^w := (Nat.div_add_mod x (2^w)).symm
      have hxqc : c = 2^w * (c / 2^w) + c % 2^w := (Nat.div_add_mod c (2^w)).symm
      have hxbits : bit x w = (x / 2^w).testBit 0 := by
        unfold bit
        simpa using (Nat.testBit_add x 0 w)
      have hcbits : bit c w = (c / 2^w).testBit 0 := by
        unfold bit
        simpa using (Nat.testBit_add c 0 w)
      have hqx01 : x / 2^w = 0 ∨ x / 2^w = 1 :=
        Nat.le_one_iff_eq_zero_or_eq_one.mp (by omega)
      have hqc01 : c / 2^w = 0 ∨ c / 2^w = 1 :=
        Nat.le_one_iff_eq_zero_or_eq_one.mp (by omega)
      rcases hqx01 with hqx | hqx
      · rcases hqc01 with hqc | hqc
        · have hxv : x = x % 2^w := by rw [hxqx, hqx]; simp
          have hcv : c = c % 2^w := by rw [hxqc, hqc]; simp
          have hxLtC : decide (x < c) = decide (x % 2^w < c % 2^w) := by
            rw [hxv, hcv]
            simp [Nat.mod_eq_of_lt hmodx, Nat.mod_eq_of_lt hmodc]
          rw [hxLtC]
          simp [hqx, hqc, hxbits, hcbits]
        · have hxv : x = x % 2^w := by rw [hxqx, hqx]; simp
          have hcv : c = 2^w + c % 2^w := by rw [hxqc, hqc]; simp [Nat.mul_comm]
          have hxLtC : decide (x < c) = true := by
            rw [hxv, hcv]
            simp [show x % 2^w < 2^w + c % 2^w by omega]
          rw [hxLtC]
          simp [hqx, hqc, hxbits, hcbits]
      · rcases hqc01 with hqc | hqc
        · have hxv : x = 2^w + x % 2^w := by rw [hxqx, hqx]; simp [Nat.mul_comm]
          have hcv : c = c % 2^w := by rw [hxqc, hqc]; simp
          have hxLtC : decide (x < c) = false := by
            rw [hxv, hcv]
            simp [show ¬ 2^w + x % 2^w < c % 2^w by omega]
          rw [hxLtC]
          simp [hqx, hqc, hxbits, hcbits]
        · have hxv : x = 2^w + x % 2^w := by rw [hxqx, hqx]; simp [Nat.mul_comm]
          have hcv : c = 2^w + c % 2^w := by rw [hxqc, hqc]; simp [Nat.mul_comm]
          have hxLtC : decide (x < c) = decide (x % 2^w < c % 2^w) := by
            rw [hxv, hcv]
            simp [Nat.add_lt_add_iff_left]
          rw [hxLtC]
          simp [hqx, hqc, hxbits, hcbits]

end Nov26.Proof_PlanePredicates_PlaneComparisonWithoutDecode
