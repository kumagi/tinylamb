import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `ByteAlignedRow.DisjointRowWrites` — `∀ n w i j, i≠j → ∀ b ∈ footprint layout n w i,
  b ∉ footprint layout n w j`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_DisjointRowWrites

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

theorem sound_DisjointRowWrites : ByteAlignedRow.DisjointRowWrites.Goal := by
  intro n w i j hij b hb hbj
  simp only [footprint, layout, lanes] at hb hbj
  rw [List.mem_eraseDups] at hb hbj
  rcases List.mem_map.mp hb with ⟨bi, hbi, rfl⟩
  rcases List.mem_map.mp hbj with ⟨bj, hbj, eqbj⟩
  simp only [List.mem_range] at hbi hbj
  let s := (w + 7) / 8
  have halign : align 8 w = 8 * s := by
    simp [s, align, Nat.mul_comm]
  have hbiBound : bi / 8 < s := by
    dsimp [s]
    omega
  have hbjBound : bj / 8 < s := by
    dsimp [s]
    omega
  have hmul_i : i * align 8 w = 8 * (i * s) := by
    rw [halign]
    ac_rfl
  have hmul_j : j * align 8 w = 8 * (j * s) := by
    rw [halign]
    ac_rfl
  have hleft : (i * align 8 w + bi) / 8 = i * s + bi / 8 := by
    rw [hmul_i]
    omega
  have hright : (j * align 8 w + bj) / 8 = j * s + bj / 8 := by
    rw [hmul_j]
    omega
  have hEq : i * s + bi / 8 = j * s + bj / 8 := by
    rw [← hleft, ← hright, eqbj]
  by_cases hijlt : i < j
  · have hle0 : (i + 1) * s ≤ j * s := Nat.mul_le_mul_right s (by omega)
    have hle : i * s + s ≤ j * s := by
      simpa [Nat.succ_mul] using hle0
    omega
  · by_cases hjilt : j < i
    · have hle0 : (j + 1) * s ≤ i * s := Nat.mul_le_mul_right s (by omega)
      have hle : j * s + s ≤ i * s := by
        simpa [Nat.succ_mul] using hle0
      omega
    · omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_DisjointRowWrites
