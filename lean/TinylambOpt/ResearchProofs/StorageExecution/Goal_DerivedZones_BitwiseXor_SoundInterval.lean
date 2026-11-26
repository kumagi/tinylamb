import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `BitwiseXor.SoundInterval` — Bound a computed group or predicate key from independent
  column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SoundInterval

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

private theorem width_lt_pow (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · simp [width, hn]
    exact (Nat.log2_eq_iff hn).mp rfl |>.2

private theorem xor_eq_zero_eq {a b : Nat} (h : a ^^^ b = 0) : a = b := by
  calc
    a = (a ^^^ b) ^^^ b := by rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]
    _ = 0 ^^^ b := by rw [h]
    _ = b := Nat.zero_xor b

private theorem div_eq_of_interval (lo hi x w : Nat) (hlo : lo ≤ x) (hhi : x ≤ hi)
    (hw : width (lo ^^^ hi) ≤ w) : x / 2 ^ w = lo / 2 ^ w := by
  have hlt : lo ^^^ hi < 2 ^ w := by
    calc
      lo ^^^ hi < 2 ^ width (lo ^^^ hi) := width_lt_pow _
      _ ≤ 2 ^ w := Nat.pow_le_pow_right (by decide) hw
  have hdiv : (lo ^^^ hi) / 2 ^ w = 0 := Nat.div_eq_of_lt hlt
  have hq : lo / 2 ^ w = hi / 2 ^ w := by
    apply xor_eq_zero_eq
    rw [← Nat.xor_div_two_pow]
    exact hdiv
  apply Nat.le_antisymm
  · rw [hq]
    exact Nat.div_le_div_right hhi
  · exact Nat.div_le_div_right hlo

theorem sound_SoundInterval : BitwiseXor.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  let w := max (width (a.lo ^^^ a.hi)) (width (b.lo ^^^ b.hi))
  let base := ((a.lo ^^^ b.lo) / 2 ^ w) * 2 ^ w
  change base ≤ x ^^^ y ∧ x ^^^ y ≤ base + 2 ^ w - 1
  have hqa : x / 2 ^ w = a.lo / 2 ^ w := by
    apply div_eq_of_interval a.lo a.hi x w ha1 ha2
    dsimp [w]
    exact Nat.le_max_left _ _
  have hqb : y / 2 ^ w = b.lo / 2 ^ w := by
    apply div_eq_of_interval b.lo b.hi y w hb1 hb2
    dsimp [w]
    exact Nat.le_max_right _ _
  have hq : (x ^^^ y) / 2 ^ w = (a.lo ^^^ b.lo) / 2 ^ w := by
    rw [Nat.xor_div_two_pow, hqa, hqb, ← Nat.xor_div_two_pow]
  have hp : 0 < 2 ^ w := Nat.two_pow_pos w
  constructor
  · dsimp [base]
    rw [← hq]
    exact Nat.div_mul_le_self (x ^^^ y) (2 ^ w)
  · dsimp [base]
    rw [← hq]
    have hlt' : x ^^^ y < 2 ^ w * ((x ^^^ y) / 2 ^ w + 1) :=
      Nat.lt_mul_div_succ (x ^^^ y) hp
    have hlt'' : x ^^^ y < ((x ^^^ y) / 2 ^ w) * 2 ^ w + 2 ^ w := by
      calc
        x ^^^ y < 2 ^ w * ((x ^^^ y) / 2 ^ w + 1) := hlt'
        _ = ((x ^^^ y) / 2 ^ w) * 2 ^ w + 2 ^ w := by
          rw [Nat.mul_add, Nat.mul_one, Nat.mul_comm (2 ^ w)]
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SoundInterval
