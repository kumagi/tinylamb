import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_JumpUnprofitable

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

-- Truth was first checked exhaustively on z, fee, x, y < 12 via #eval:
-- no input satisfying the hypotheses violates the strict inequality.

theorem sound_JumpUnprofitable : MirrorZones.JumpUnprofitable.Goal := by
  intro z fee x y hxy hfee hzone
  simp only [MirrorZones.price, span]
  have hk : 1 ≤ y / (z + 1) - x / (z + 1) := by omega
  have hspan : 1 ≤ y - x := by omega
  have hmul : 2 ≤ fee * (y / (z + 1) - x / (z + 1)) := by
    have h2 : 2 ≤ fee := by omega
    calc
      2 = 2 * 1 := by omega
      _ ≤ fee * (y / (z + 1) - x / (z + 1)) := Nat.mul_le_mul h2 hk
  have hx : x / (z + 1) - x / (z + 1) = 0 := by omega
  have hy : y / (z + 1) - y / (z + 1) = 0 := by omega
  simp only [hx, hy]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_JumpUnprofitable
