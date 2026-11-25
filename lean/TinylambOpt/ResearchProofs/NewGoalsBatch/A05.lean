import TinylambOpt.ResearchGoals.Compression.BitPatterns

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A05

open TinylambOpt.ResearchGoals.Compression.BitPatterns
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Per-goal notes:
  * XorChain: induction on the list; XOR is involutive, so `prev ^^^ (prev ^^^ x) = x`.
  * SignificantWindow: `tz` is the least set bit, so the low `tz` bits vanish and
    the remaining quotient fits in `activeBits`.
  * CommonHighBits: `Nat.xor_div_two_pow` sends the common quotient to `q ^^^ q = 0`,
    hence the XOR is below `2^k`.
  * PreviousWindow: decompose `x = (x / 2^lo) * 2^lo + x % 2^lo` and cancel.
  * AmortizedWindow: `oldWidth = newWidth + (oldWidth - newWidth)`, then linear arithmetic.
-/

theorem XorChain_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.XorChain.Goal := by
  intro prev xs
  induction xs generalizing prev with
  | nil => rfl
  | cons x xs ih =>
      have hx : prev ^^^ (prev ^^^ x) = x := by
        rw [← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]
      simp [TinylambOpt.ResearchGoals.Compression.BitPatterns.delta,
        TinylambOpt.ResearchGoals.Compression.BitPatterns.undo, hx, ih]

theorem SignificantWindow_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.SignificantWindow.Goal := by
  intro x hx
  by_cases hx0 : x = 0
  · subst x
    simp [TinylambOpt.ResearchGoals.Compression.BitPatterns.tz,
      TinylambOpt.ResearchGoals.Compression.BitPatterns.activeBits,
      TinylambOpt.ResearchGoals.Compression.BitPatterns.significant, width]
  · let t := TinylambOpt.ResearchGoals.Compression.BitPatterns.tz x
    have hloglt : x.log2 < 64 := (Nat.log2_lt hx0).2 hx
    have hlogbit : x.testBit x.log2 = true := Nat.testBit_log2 hx0
    have hmemlog : x.log2 ∈ (List.range 64 : List Nat) := by simp [hloglt]
    have hex : ∃ y ∈ (List.range 64 : List Nat), x.testBit y = true :=
      ⟨x.log2, hmemlog, hlogbit⟩
    have htlt : t < 64 := by
      dsimp [t, TinylambOpt.ResearchGoals.Compression.BitPatterns.tz]
      exact List.findIdx_lt_length_of_exists hex
    have htfind :
        (List.range 64 : List Nat).findIdx (fun i => x.testBit i) = t := by rfl
    have hfind := (List.findIdx_eq (xs := (List.range 64 : List Nat))
      (p := fun i => x.testBit i) (i := t) (by simpa using htlt)).1 htfind
    have htbit : x.testBit t = true := by
      simpa using hfind.1
    have hlow : ∀ i, i < t → x.testBit i = false := by
      intro i hi
      simpa using hfind.2 i hi
    have hmod : x % 2^t = 0 := by
      apply Nat.eq_of_testBit_eq
      intro i
      rw [Nat.testBit_mod_two_pow]
      by_cases hi : i < t
      · simp [hi, hlow i hi]
      · simp [hi]
    have hpowle : 2^t ≤ x := Nat.ge_two_pow_of_testBit htbit
    have htlog : t ≤ x.log2 := (Nat.le_log2 hx0).2 hpowle
    have hwidth : width x = x.log2 + 1 := by simp [width, hx0]
    have hab : TinylambOpt.ResearchGoals.Compression.BitPatterns.activeBits x =
        x.log2 + 1 - t := by
      simp [TinylambOpt.ResearchGoals.Compression.BitPatterns.activeBits, hwidth, t]
    have hltpow : x / 2^t < 2^(x.log2 + 1 - t) := by
      rw [Nat.div_lt_iff_lt_mul (Nat.two_pow_pos t)]
      calc
        x < 2^(x.log2 + 1) := Nat.lt_log2_self (n := x)
        _ = 2^(x.log2 + 1 - t) * 2^t := by
          rw [← Nat.pow_add]
          congr 1
          omega
    have hsiglt : TinylambOpt.ResearchGoals.Compression.BitPatterns.significant x <
        2^TinylambOpt.ResearchGoals.Compression.BitPatterns.activeBits x := by
      change x / 2^t <
        2^TinylambOpt.ResearchGoals.Compression.BitPatterns.activeBits x
      rw [hab]
      exact hltpow
    calc
      (TinylambOpt.ResearchGoals.Compression.BitPatterns.significant x %
          2^TinylambOpt.ResearchGoals.Compression.BitPatterns.activeBits x) *
          2^TinylambOpt.ResearchGoals.Compression.BitPatterns.tz x
          = TinylambOpt.ResearchGoals.Compression.BitPatterns.significant x * 2^t := by
              rw [Nat.mod_eq_of_lt hsiglt]
      _ = x := by
            change (x / 2^t) * 2^t = x
            exact Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero hmod)

theorem CommonHighBits_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.CommonHighBits.Goal := by
  intro a b k hab
  have hdiv : (a ^^^ b) / 2^k = 0 := by
    rw [Nat.xor_div_two_pow, hab, Nat.xor_self]
  exact (Nat.div_eq_zero_iff_lt (Nat.two_pow_pos k)).1 hdiv

theorem PreviousWindow_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.PreviousWindow.Goal := by
  intro lo w x
  constructor
  · intro h
    simp [TinylambOpt.ResearchGoals.Compression.BitPatterns.fitsWindow] at h
    rcases h with ⟨hmod, hlt⟩
    have hdecomp : 2^lo * (x / 2^lo) = x := by
      have h0 := Nat.mod_add_div x (2^lo)
      omega
    calc
      ((x / 2^lo) % 2^w) * 2^lo = (x / 2^lo) * 2^lo := by
        rw [Nat.mod_eq_of_lt hlt]
      _ = 2^lo * (x / 2^lo) := Nat.mul_comm _ _
      _ = x := hdecomp
  · intro h
    simp [TinylambOpt.ResearchGoals.Compression.BitPatterns.fitsWindow]
    have hle : (x / 2^lo % 2^w) * 2^lo ≤ (x / 2^lo) * 2^lo :=
      Nat.mul_le_mul_right (2^lo) (Nat.mod_le _ _)
    have hdecomp : x % 2^lo + (x / 2^lo) * 2^lo = x := by
      have h0 := Nat.mod_add_div x (2^lo)
      rw [Nat.mul_comm (2^lo) (x / 2^lo)] at h0
      exact h0
    have hr0 : x % 2^lo = 0 := by omega
    constructor
    · exact hr0
    · have hqmul : (x / 2^lo % 2^w) * 2^lo = (x / 2^lo) * 2^lo := by omega
      have hq : x / 2^lo % 2^w = x / 2^lo :=
        Nat.eq_of_mul_eq_mul_right (Nat.two_pow_pos lo) hqmul
      rw [← hq]
      exact Nat.mod_lt _ (Nat.two_pow_pos w)

theorem AmortizedWindow_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.AmortizedWindow.Goal := by
  intro n oldWidth newWidth header hle
  have hold : oldWidth = newWidth + (oldWidth - newWidth) := by omega
  rw [hold]
  have hd : newWidth + (oldWidth - newWidth) - newWidth = oldWidth - newWidth := by
    omega
  rw [hd]
  rw [Nat.mul_add]
  omega

end TinylambOpt.ResearchProofs.NewGoalsBatch.A05
