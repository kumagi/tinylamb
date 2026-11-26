import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_DoubleStride

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow

-- Sanity check on small inputs: alternating parity is preserved by a stride of two.
private def check (N W : Nat) : Bool :=
  (List.range N).all fun n => (List.range W).all fun w =>
    (List.range (3 * W + 3)).all fun i =>
      (List.range W).all fun b =>
        decide (layout.bit n w (i + 2) b = layout.bit n w i b + 2 * w)

#eval check 8 8

theorem sound_DoubleStride : SerpentineRow.DoubleStride.Goal := by
  intro n w i b
  dsimp [SerpentineRow.layout, layout]
  have hparity : (i + 2) % 2 = i % 2 := by omega
  rw [hparity, Nat.add_mul]
  ac_rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentineRow_DoubleStride
