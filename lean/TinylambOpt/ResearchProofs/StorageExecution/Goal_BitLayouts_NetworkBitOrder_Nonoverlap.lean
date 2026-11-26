import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `NetworkBitOrder.Nonoverlap` — A writer may fill distinct bit cells concurrently.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder

/-- Reversing the bit order inside every byte is an involution. -/
private def rev8 (p : Nat) : Nat := p / 8 * 8 + (7 - p % 8)

private theorem rev8_rev8 (p : Nat) : rev8 (rev8 p) = p := by
  unfold rev8
  have hlt : p % 8 < 8 := Nat.mod_lt p (by decide)
  omega

theorem sound_Nonoverlap : NetworkBitOrder.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    change rev8 (i * w + a) = rev8 (j * w + b) at h
    have hpos : 0 < w := by omega
    have hlin : i * w + a = j * w + b := by
      have h' := congrArg rev8 h
      simpa [rev8_rev8] using h'
    have hab : a = b := by
      have hmod := congrArg (fun x : Nat => x % w) hlin
      have hl : (i * w + a) % w = a := by
        simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt ha]
      have hr : (j * w + b) % w = b := by
        simp [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hb]
      rw [hl, hr] at hmod
      exact hmod
    subst b
    have hij : i = j := Nat.mul_right_cancel hpos (Nat.add_right_cancel hlin)
    exact ⟨hij, rfl⟩
  · intro h
    rcases h with ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_Nonoverlap
