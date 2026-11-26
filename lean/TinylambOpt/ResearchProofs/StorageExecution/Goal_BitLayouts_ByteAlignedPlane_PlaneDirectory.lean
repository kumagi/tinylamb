import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `ByteAlignedPlane.PlaneDirectory` — `∀ n w, byteCount (layout.capacity n w) =
  w*byteCount n`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_PlaneDirectory

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane

-- Sanity checks on small inputs (the goal has no hypotheses).
#eval (List.range 20).all fun n =>
  (List.range 20).all fun w =>
    decide (byteCount (ByteAlignedPlane.layout.capacity n w) = w * byteCount n)

theorem sound_PlaneDirectory : ByteAlignedPlane.PlaneDirectory.Goal := by
  intro n w
  change byteCount (w * align 8 n) = w * byteCount n
  have halign : align 8 n = 8 * byteCount n := by
    simp [align, byteCount, Nat.mul_comm]
  rw [halign]
  have hmul : w * (8 * byteCount n) = 8 * (w * byteCount n) := by
    ac_rfl
  rw [hmul]
  unfold byteCount
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedPlane_PlaneDirectory
