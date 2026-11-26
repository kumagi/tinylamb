import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_TileCharge

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose

/-- Within an eight-row tile, each significance occupies one distinct byte. -/
theorem sound_TileCharge : TileTranspose.TileCharge.Goal := by
  intro n w tile b i hi
  simp only [layout]
  have hdiv : (tile * 8 + i) / 8 = tile := by omega
  have hmod : (tile * 8 + i) % 8 = i := by omega
  simp only [hdiv, hmod]
  have hmul : tile * 8 * w + b * 8 + i = 8 * (tile * w + b) + i := by
    have h1 : tile * 8 * w = 8 * (tile * w) := by
      rw [← Nat.mul_assoc, Nat.mul_comm tile 8, Nat.mul_assoc]
    have h2 : b * 8 = 8 * b := Nat.mul_comm _ _
    omega
  rw [hmul, Nat.add_comm (8 * (tile * w + b)) i,
    Nat.add_mul_div_left i (tile * w + b) (by decide : 0 < 8),
    Nat.div_eq_of_lt hi]
  simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_TileCharge
