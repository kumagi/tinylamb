import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_ByteFootprint

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder

-- Sanity checks on small inputs.
private def check (N W : Nat) : Bool :=
  (List.range N).all fun n => (List.range W).all fun w =>
    (List.range (2 * W + 3)).all fun i =>
      decide (footprint layout n w i = footprint RowMajor.layout n w i)

#eval check 5 8

theorem sound_ByteFootprint : NetworkBitOrder.ByteFootprint.Goal := by
  intro n w i
  unfold footprint lanes
  apply congrArg List.eraseDups
  apply List.map_congr_left
  intro b hb
  simp only [layout, RowMajor.layout]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_ByteFootprint
