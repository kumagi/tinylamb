import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_DoubleFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : BitParity.DoubleFrame.Goal := by
  intro x xs
  have hne : (x :: xs) ≠ [] := by simp
  have his : ((x :: xs) ++ (x :: xs)).isEmpty = false :=
    isEmpty_false_of_ne_nil (by simp : (x :: xs) ++ (x :: xs) ≠ [])
  simp only [value, his, Bool.false_eq_true, ite_false, Option.some.injEq]
  change ((x :: xs) ++ (x :: xs)).foldl (fun a b => a ^^^ b) 0 = 0
  rw [foldl_xor_append]
  simp only [List.foldl_cons, Nat.zero_xor]
  rw [Nat.xor_self]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitParity_DoubleFrame
