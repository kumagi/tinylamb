import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_XorClass_DecodeRight

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.XorClass

/-! `XorClass.key (x,y) = (x ^^^ y, x)`, so decoding the right component is
    the XOR-cancellation identity `(x ^^^ y) ^^^ x = y`.  XOR on `Nat` is
    associative and commutative with `x ^^^ x = 0` and `0 ^^^ y = y`, both of
    which `simp` knows. -/

theorem sound_DecodeRight : XorClass.DecodeRight.Goal := by
  intro x y
  change (x ^^^ y) ^^^ x = y
  calc
    (x ^^^ y) ^^^ x = (x ^^^ x) ^^^ y := by ac_rfl
    _ = y := by simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_XorClass_DecodeRight
