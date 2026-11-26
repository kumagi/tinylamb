import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_AppendWithinPad

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane

-- Appending one row changes the byte-aligned plane stride only when `n` is a
-- multiple of eight. For any other residue both `align 8 n` and
-- `align 8 (n+1)` round up to the very same multiple of eight.
private theorem align8_succ_of_mod_ne_zero (n : Nat) (h : n % 8 ≠ 0) :
    align 8 (n + 1) = align 8 n := by
  unfold align
  have hdiv := Nat.div_add_mod n 8
  have hmodlt := Nat.mod_lt n (by decide : 0 < 8)
  omega

theorem sound_AppendWithinPad : ByteAlignedPlane.AppendWithinPad.Goal := by
  intro n w i b _hn hmod
  dsimp [layout]
  rw [align8_succ_of_mod_ne_zero n hmod]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_AppendWithinPad
