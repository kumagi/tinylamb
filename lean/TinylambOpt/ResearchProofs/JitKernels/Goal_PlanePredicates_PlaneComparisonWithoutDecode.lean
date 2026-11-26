import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-! Unsigned less-than is decided by the most significant differing bit plane:
    the least-to-most-significant fold returns exactly that plane's threshold
    bit. SQL predicates over in-frame values can therefore be generated from
    encoded bit planes without decoding any residual. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_PlaneComparisonWithoutDecode

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

theorem sound : TinylambOpt.ResearchGoals.JitKernels.PlanePredicates.PlaneComparisonWithoutDecode.Goal := by
  have foldBits_succ : ∀ (x c w : Nat),
      foldBits x c (w + 1) = if bit x w == bit c w then foldBits x c w else bit c w := by
    intro x c w
    simp [foldBits, List.range_succ]
  have foldBits_congr : ∀ {x c x' c' : Nat} {w : Nat},
      (∀ j < w, bit x j = bit x' j) →
      (∀ j < w, bit c j = bit c' j) →
      foldBits x c w = foldBits x' c' w := by
    intro x c x' c' w hx hc
    induction w with
    | zero => simp [foldBits]
    | succ w ih =>
        rw [foldBits_succ, foldBits_succ]
        have hxw := hx w (by omega)
        have hcw := hc w (by omega)
        rw [hxw, hcw]
        by_cases hcond : bit x' w == bit c' w
        · simp [hcond, ih (fun j hj => hx j (by omega)) (fun j hj => hc j (by omega))]
        · simp [hcond]
  have foldBits_mod : ∀ (x c w : Nat),
      foldBits x c w = foldBits (x % 2^w) (c % 2^w) w := by
    intro x c w
    apply foldBits_congr <;> intro j hj
    · simp [bit, Nat.testBit_mod_two_pow, hj]
    · simp [bit, Nat.testBit_mod_two_pow, hj]
  have bit_top_iff : ∀ (x w : Nat) (hx : x < 2^(w + 1)),
      bit x w = true ↔ 2^w ≤ x := by
    intro x w hx
    constructor
    · intro hb
      by_cases hlt : x < 2^w
      · have hfalse : bit x w = false := Nat.testBit_lt_two_pow hlt
        simp [hb] at hfalse
      · omega
    · intro hle
      exact Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hle (by omega)
  have bit_false_lt : ∀ (x w : Nat) (hx : x < 2^(w + 1)),
      bit x w = false → x < 2^w := by
    intro x w hx hb
    by_cases hle : 2^w ≤ x
    · have ht := (bit_top_iff x w hx).2 hle
      simp [hb] at ht
    · omega
  intro x c w hx hc
  induction w generalizing x c with
  | zero =>
      have hx0 : x = 0 := by omega
      have hc0 : c = 0 := by omega
      rw [hx0, hc0]
      simp [foldBits]
  | succ w ih =>
      rw [foldBits_succ]
      cases hxb : bit x w <;> cases hcb : bit c w
      · have hxlo := bit_false_lt x w hx hxb
        have hclo := bit_false_lt c w hc hcb
        simp [hxb, hcb]
        rw [foldBits_mod, Nat.mod_eq_of_lt hxlo, Nat.mod_eq_of_lt hclo]
        exact ih x c hxlo hclo
      · have hxlo := bit_false_lt x w hx hxb
        have hchi : 2^w ≤ c := (bit_top_iff c w hc).1 (by simp [hcb])
        simp [hxb, hcb]
        by_cases hlt : x < c <;> simp [hlt] <;> omega
      · have hxhi : 2^w ≤ x := (bit_top_iff x w hx).1 (by simp [hxb])
        have hclo := bit_false_lt c w hc hcb
        simp [hxb, hcb]
        omega
      · have hxhi : 2^w ≤ x := (bit_top_iff x w hx).1 (by simp [hxb])
        have hchi : 2^w ≤ c := (bit_top_iff c w hc).1 (by simp [hcb])
        simp [hxb, hcb]
        rw [foldBits_mod]
        have hxmod : x % 2^w = x - 2^w := by
          rw [Nat.mod_eq_sub_mod hxhi, Nat.mod_eq_of_lt (by omega : x - 2^w < 2^w)]
        have hcmod : c % 2^w = c - 2^w := by
          rw [Nat.mod_eq_sub_mod hchi, Nat.mod_eq_of_lt (by omega : c - 2^w < 2^w)]
        rw [hxmod, hcmod]
        have hxl : x - 2^w < 2^w := by omega
        have hcl : c - 2^w < 2^w := by omega
        rw [ih (x - 2^w) (c - 2^w) hxl hcl]
        by_cases hlt : x - 2^w < c - 2^w <;> by_cases hlt' : x < c <;> simp [hlt, hlt'] <;> omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_PlaneComparisonWithoutDecode
